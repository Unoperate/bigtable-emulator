// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "column_family.h"
#include "google/cloud/internal/big_endian.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_format.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "bigtable_limits.h"
#include "cell_view.h"
#include "filter.h"
#include "filtered_map.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/util/time_util.h>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

// See comment next to `static_assert(kMaxGCRuleSize ==` for the proof of
// safety of this function despite the recursive calls.
// NOLINTNEXTLINE(misc-no-recursion)
Status CheckGCRuleTreeNodeHasValidFields(
    google::bigtable::admin::v2::GcRule const& rule) {
  switch (rule.rule_case()) {
    case google::bigtable::admin::v2::GcRule::kMaxAge: {
      if (!rule.has_max_age()) {
        return InvalidArgumentError(
            "max_age must be set",
            GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
      }
      if (protobuf::util::TimeUtil::DurationToMilliseconds(rule.max_age()) <
          1) {
        return InvalidArgumentError(
            "max_age must be at least 1ms",
            GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
      }
      return Status();
    }
    case google::bigtable::admin::v2::GcRule::kMaxNumVersions: {
      if (!rule.has_max_num_versions()) {
        return InvalidArgumentError(
            "max_num_version must be set",
            GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
      }
      if (rule.max_num_versions() < 1) {
        return InvalidArgumentError(
            "max_num_versions must be positive",
            GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
      }
      return Status();
    }
    // FIXME: see how the real Cloud Bigtable validates intersection's rules.
    case google::bigtable::admin::v2::GcRule::kIntersection:
    // FIXME: see how the real Cloud Bigtable validates union's rules.
    case google::bigtable::admin::v2::GcRule::kUnion:
    // It's a widely used value meaning "no GC".
    // Cloud Bigtable *clients* fold client constructs such as `Union[]` to it.
    // FIXME: Send raw gRPC requests with unset or empty lists in union and
    // intersection and see whether we need to copy the clients' logic for our
    // handling of the requests to behave like Cloud Bigtable. For now, we avoid
    // folding because the result is identical, we might just do some extra work
    // and/or return some GC rule when Bigtable would possibly return none. If
    // we decide to also fold, we will also need to modify handling schema in
    // table admin operations accordingly.
    case google::bigtable::admin::v2::GcRule::RULE_NOT_SET: {
      return Status();
    }
    default: {
      return InvalidArgumentError(
          "unknown GCRule",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }
  }
}

// See comment next to `static_assert(kMaxGCRuleSize ==` for the proof of
// safety of this function despite the recursive calls.
// NOLINTNEXTLINE(misc-no-recursion)
Status CheckGCRuleTreeHasValidFields(
    google::bigtable::admin::v2::GcRule const& rule) {
  Status rule_validation = CheckGCRuleTreeNodeHasValidFields(rule);
  if (!rule_validation.ok()) {
    return rule_validation;
  }
  switch (rule.rule_case()) {
    case google::bigtable::admin::v2::GcRule::kIntersection: {
      for (auto const& r : rule.intersection().rules()) {
        Status subrule_validation = CheckGCRuleTreeHasValidFields(r);
        if (!subrule_validation.ok()) {
          return subrule_validation;
        }
      }
      return Status();
    }
    case google::bigtable::admin::v2::GcRule::kUnion: {
      for (auto const& r : rule.union_().rules()) {
        Status subrule_validation = CheckGCRuleTreeHasValidFields(r);
        if (!subrule_validation.ok()) {
          return subrule_validation;
        }
      }
      return Status();
    }
    default:
      return Status();
  }
}

Status CheckGCRuleSizeIsBelowLimit(
    google::bigtable::admin::v2::GcRule const& rule) {
  // As per the spec, limit the size of the serialized gc_rule to
  // kMaxGCRuleSize bytes. This is important for controlling the depth of
  // recursion in the GC thread later on (and therefore protecting
  // from a stack overflow).
  std::size_t gc_rule_size = rule.ByteSizeLong();
  if (gc_rule_size > kMaxGCRuleSize) {
    return InvalidArgumentError(
        absl::StrFormat("Supplied GcRule is too large: It must not exceed %u "
                        "bytes (when serialized)",
                        kMaxGCRuleSize),
        GCP_ERROR_INFO().WithMetadata("GcRule", rule.DebugString()));
  }

  return Status();
}
}  // namespace

StatusOr<ReadModifyWriteCellResult> ColumnRow::ReadModifyWrite(
    std::int64_t inc_value) {
  auto system_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());

  if (cells_.empty()) {
    std::string value = google::cloud::internal::EncodeBigEndian(inc_value);
    cells_[system_ms] = value;

    return ReadModifyWriteCellResult{system_ms, std::move(value),
                                     absl::nullopt};
  }

  auto latest_it = cells_.begin();

  auto maybe_old_value =
      google::cloud::internal::DecodeBigEndian<std::int64_t>(latest_it->second);
  if (!maybe_old_value) {
    return maybe_old_value.status();
  }

  auto value = google::cloud::internal::EncodeBigEndian(
      inc_value + maybe_old_value.value());

  if (latest_it->first < system_ms) {
    // We need to add a cell with the current system timestamp
    cells_[system_ms] = value;

    return ReadModifyWriteCellResult{system_ms, std::move(value),
                                     absl::nullopt};
  }

  // Latest timestamp is >= system time. Overwrite latest timestamp
  auto old_value = std::move(latest_it->second);
  latest_it->second = value;

  return ReadModifyWriteCellResult{latest_it->first, std::move(value),
                                   std::move(old_value)};
}

ReadModifyWriteCellResult ColumnRow::ReadModifyWrite(
    std::string const& append_value) {
  auto system_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  if (cells_.empty()) {
    cells_[system_ms] = append_value;

    return ReadModifyWriteCellResult{system_ms, std::move(append_value),
                                     absl::nullopt};
  }

  auto latest_it = cells_.begin();

  auto value = latest_it->second + append_value;

  if (latest_it->first < system_ms) {
    // We need to add a cell with the current system timestamp
    cells_[system_ms] = value;

    return ReadModifyWriteCellResult{system_ms, std::move(value),
                                     absl::nullopt};
  }

  // Latest timestamp is >= system time. Overwrite latest timestamp
  auto old_value = std::move(latest_it->second);
  latest_it->second = value;

  return ReadModifyWriteCellResult{latest_it->first, value,
                                   std::move(old_value)};
}

absl::optional<std::string> ColumnRow::SetCell(
    std::chrono::milliseconds timestamp, std::string const& value) {
  absl::optional<std::string> ret = absl::nullopt;

  auto cell_it = cells_.find(timestamp);
  if (!(cell_it == cells_.end())) {
    ret = std::move(cell_it->second);
  }

  cells_[timestamp] = value;

  return ret;
}

StatusOr<absl::optional<std::string>> ColumnRow::UpdateCell(
    std::chrono::milliseconds timestamp, std::string& value,
    std::function<StatusOr<std::string>(std::string const&,
                                        std::string&&)> const& update_fn) {
  absl::optional<std::string> ret = absl::nullopt;

  auto cell_it = cells_.find(timestamp);
  if (!(cell_it == cells_.end())) {
    auto maybe_update_value = update_fn(cell_it->second, std::move(value));
    if (!maybe_update_value) {
      return maybe_update_value.status();
    }
    ret = std::move(cell_it->second);
    maybe_update_value.value().swap(cell_it->second);
    return ret;
  }

  cells_[timestamp] = value;

  return ret;
}

std::vector<Cell> ColumnRow::DeleteTimeRange(
    ::google::bigtable::v2::TimestampRange const& time_range) {
  std::vector<Cell> deleted_cells;
  absl::optional<std::int64_t> maybe_end_micros =
      time_range.end_timestamp_micros();
  if (maybe_end_micros.value_or(0) == 0) {
    maybe_end_micros.reset();
  }
  for (auto cell_it =
           maybe_end_micros
               ? upper_bound(
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::microseconds(*maybe_end_micros)))
               : begin();
       cell_it != cells_.end() &&
       cell_it->first >= std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::microseconds(
                                 time_range.start_timestamp_micros()));) {
    Cell cell = {std::move(cell_it->first), std::move(cell_it->second)};
    deleted_cells.emplace_back(std::move(cell));
    cells_.erase(cell_it++);
  }
  return deleted_cells;
}

absl::optional<Cell> ColumnRow::DeleteTimeStamp(
    std::chrono::milliseconds timestamp) {
  absl::optional<Cell> ret = absl::nullopt;

  auto cell_it = cells_.find(timestamp);
  if (cell_it != cells_.end()) {
    Cell cell = {std::move(cell_it->first), std::move(cell_it->second)};
    ret.emplace(std::move(cell));
    cells_.erase(cell_it);
  }

  return ret;
}

void ColumnRow::RunGC(google::bigtable::admin::v2::GcRule const& gc_rule) {
  assert(CheckGCRuleIsValid(gc_rule).ok());
  switch (gc_rule.rule_case()) {
    case google::bigtable::admin::v2::GcRule::kMaxAge: {
      ApplyGCRuleMaxAge(gc_rule.max_age());
      break;
    }
    case google::bigtable::admin::v2::GcRule::kMaxNumVersions: {
      ApplyGCRuleMaxNumVersions(
          static_cast<std::size_t>(gc_rule.max_num_versions()));
      break;
    }
    case google::bigtable::admin::v2::GcRule::kIntersection:
    case google::bigtable::admin::v2::GcRule::kUnion: {
      ApplyGCRuleVerdict(gc_rule);
      break;
    }
    case google::bigtable::admin::v2::GcRule::RULE_NOT_SET:
    default: {
      break;
    }
  }
}

void ColumnRow::ApplyGCRuleMaxNumVersions(std::size_t n) {
  if (n >= cells_.size()) {
    return;
  }

  auto it = cells_.begin();
  std::advance(it, n);
  cells_.erase(it, cells_.end());
}

std::chrono::milliseconds GetMaxAgeCutOffTimestamp(
    protobuf::Duration const& max_age) {
  std::chrono::milliseconds max_age_ms(
      protobuf::util::TimeUtil::DurationToMilliseconds(max_age));
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch()) -
         max_age_ms;
}

void ColumnRow::ApplyGCRuleMaxAge(protobuf::Duration const& max_age) {
  std::chrono::milliseconds max_age_ms(
      protobuf::util::TimeUtil::DurationToMilliseconds(max_age));
  auto cut_off_ms = GetMaxAgeCutOffTimestamp(max_age);
  auto newest_to_delete = cells_.upper_bound(cut_off_ms);
  cells_.erase(newest_to_delete, cells_.end());
}

// See comment next to `static_assert(kMaxGCRuleSize ==` for the proof of
// safety of this function despite the recursive calls.
// NOLINTBEGIN(misc-no-recursion)
bool ColumnRow::GCRuleEraseVerdict(
    google::bigtable::admin::v2::GcRule const& rule,
    std::map<std::chrono::milliseconds, std::string,
             std::greater<>>::const_iterator it,
    int32_t const version_rank) {
  assert(CheckGCRuleIsValid(rule).ok());
  switch (rule.rule_case()) {
    case google::bigtable::admin::v2::GcRule::kMaxAge: {
      return it->first < GetMaxAgeCutOffTimestamp(rule.max_age());
    }
    case google::bigtable::admin::v2::GcRule::kMaxNumVersions: {
      return version_rank >= rule.max_num_versions();
    }
    case google::bigtable::admin::v2::GcRule::kIntersection: {
      auto rules = rule.intersection().rules();
      return !rules.empty() &&
             std::all_of(rules.begin(), rules.end(),
                         [&](google::bigtable::admin::v2::GcRule const& r) {
                           return GCRuleEraseVerdict(r, it, version_rank);
                         });
    }
    case google::bigtable::admin::v2::GcRule::kUnion: {
      auto rules = rule.union_().rules();
      return !rules.empty() &&
             std::any_of(rules.begin(), rules.end(),
                         [&](google::bigtable::admin::v2::GcRule const& r) {
                           return GCRuleEraseVerdict(r, it, version_rank);
                         });
    }
    case google::bigtable::admin::v2::GcRule::RULE_NOT_SET:
    default: {
      return false;
    }
  }
}
// NOLINTEND(misc-no-recursion)

void ColumnRow::ApplyGCRuleVerdict(
    google::bigtable::admin::v2::GcRule const& rule) {
  int32_t version_rank = 0;
  for (auto it = cells_.begin(); it != cells_.end();) {
    if (GCRuleEraseVerdict(rule, it, version_rank)) {
      it = cells_.erase(it);
    } else {
      it++;
      version_rank++;
    }
  }
}

absl::optional<std::string> ColumnFamilyRow::SetCell(
    std::string const& column_qualifier, std::chrono::milliseconds timestamp,
    std::string const& value) {
  return columns_[column_qualifier].SetCell(timestamp, value);
}

StatusOr<absl::optional<std::string>> ColumnFamilyRow::UpdateCell(
    std::string const& column_qualifier, std::chrono::milliseconds timestamp,
    std::string& value,
    std::function<StatusOr<std::string>(std::string const&,
                                        std::string&&)> const& update_fn) {
  return columns_[column_qualifier].UpdateCell(timestamp, value,
                                               std::move(update_fn));
}

std::vector<Cell> ColumnFamilyRow::DeleteColumn(
    std::string const& column_qualifier,
    ::google::bigtable::v2::TimestampRange const& time_range) {
  auto column_it = columns_.find(column_qualifier);
  if (column_it == columns_.end()) {
    return {};
  }
  auto res = column_it->second.DeleteTimeRange(time_range);
  if (!column_it->second.HasCells()) {
    columns_.erase(column_it);
  }
  return res;
}

absl::optional<Cell> ColumnFamilyRow::DeleteTimeStamp(
    std::string const& column_qualifier, std::chrono::milliseconds timestamp) {
  auto column_it = columns_.find(column_qualifier);
  if (column_it == columns_.end()) {
    return absl::nullopt;
  }

  auto ret = column_it->second.DeleteTimeStamp(timestamp);
  if (!column_it->second.HasCells()) {
    columns_.erase(column_it);
  }

  return ret;
}

void ColumnFamilyRow::RunGC(
    google::bigtable::admin::v2::GcRule const& gc_rule) {
  assert(CheckGCRuleIsValid(gc_rule).ok());
  for (auto it = columns_.begin(); it != columns_.end();) {
    it->second.RunGC(gc_rule);
    if (!it->second.HasCells()) {
      it = erase(it);
    } else {
      it++;
    }
  }
}

absl::optional<std::string> ColumnFamily::SetCell(
    std::string const& row_key, std::string const& column_qualifier,
    std::chrono::milliseconds timestamp, std::string const& value) {
  return rows_[row_key].SetCell(column_qualifier, timestamp, value);
}

StatusOr<absl::optional<std::string>> ColumnFamily::UpdateCell(
    std::string const& row_key, std::string const& column_qualifier,
    std::chrono::milliseconds timestamp, std::string& value) {
  return rows_[row_key].UpdateCell(column_qualifier, timestamp, value,
                                   update_cell_);
}

std::map<std::string, std::vector<Cell>> ColumnFamily::DeleteRow(
    std::string const& row_key) {
  std::map<std::string, std::vector<Cell>> res;

  auto row_it = rows_.find(row_key);
  if (row_it == rows_.end()) {
    return {};
  }

  for (auto& column : row_it->second.columns_) {
    // Not setting start and end timestamps will select all cells for deletion
    ::google::bigtable::v2::TimestampRange time_range;
    auto deleted_cells = column.second.DeleteTimeRange(time_range);
    if (!deleted_cells.empty()) {
      res[column.first] = std::move(deleted_cells);
    }
  }

  rows_.erase(row_it);

  return res;
}

std::vector<Cell> ColumnFamily::DeleteColumn(
    std::string const& row_key, std::string const& column_qualifier,
    ::google::bigtable::v2::TimestampRange const& time_range) {
  auto row_it = rows_.find(row_key);

  return DeleteColumn(row_it, column_qualifier, time_range);
}

std::vector<Cell> ColumnFamily::DeleteColumn(
    std::map<std::string, ColumnFamilyRow>::iterator row_it,
    std::string const& column_qualifier,
    ::google::bigtable::v2::TimestampRange const& time_range) {
  if (row_it != rows_.end()) {
    auto erased_cells =
        row_it->second.DeleteColumn(column_qualifier, time_range);
    if (!row_it->second.HasColumns()) {
      rows_.erase(row_it);
    }
    return erased_cells;
  }
  return {};
}

absl::optional<Cell> ColumnFamily::DeleteTimeStamp(
    std::string const& row_key, std::string const& column_qualifier,
    std::chrono::milliseconds timestamp) {
  auto row_it = rows_.find(row_key);
  if (row_it == rows_.end()) {
    return absl::nullopt;
  }

  auto ret = row_it->second.DeleteTimeStamp(column_qualifier, timestamp);
  if (!row_it->second.HasColumns()) {
    rows_.erase(row_it);
  }

  return ret;
}

void ColumnFamily::RunGC() {
  if (gc_rule_.has_value()) {
    assert(CheckGCRuleIsValid(gc_rule_.value()).ok());
    for (auto it = rows_.begin(); it != rows_.end();) {
      it->second.RunGC(gc_rule_.value());
      if (!it->second.HasColumns()) {
        it = erase(it);
      } else {
        it++;
      }
    }
  }
}

class FilteredColumnFamilyStream::FilterApply {
 public:
  explicit FilterApply(FilteredColumnFamilyStream& parent) : parent_(parent) {}

  bool operator()(ColumnRange const& column_range) {
    if (column_range.column_family == parent_.column_family_name_) {
      parent_.column_ranges_.Intersect(column_range.range);
    }
    return true;
  }

  bool operator()(TimestampRange const& timestamp_range) {
    parent_.timestamp_ranges_.Intersect(timestamp_range.range);
    return true;
  }

  bool operator()(RowKeyRegex const& row_key_regex) {
    parent_.row_regexes_.emplace_back(row_key_regex.regex);
    return true;
  }

  bool operator()(FamilyNameRegex const&) { return false; }

  bool operator()(ColumnRegex const& column_regex) {
    parent_.column_regexes_.emplace_back(column_regex.regex);
    return true;
  }

 private:
  FilteredColumnFamilyStream& parent_;
};

FilteredColumnFamilyStream::FilteredColumnFamilyStream(
    ColumnFamily const& column_family, std::string column_family_name,
    std::shared_ptr<StringRangeSet const> row_set)
    : column_family_name_(std::move(column_family_name)),
      row_ranges_(std::move(row_set)),
      column_ranges_(StringRangeSet::All()),
      timestamp_ranges_(TimestampRangeSet::All()),
      rows_(
          StringRangeFilteredMapView<ColumnFamily>(column_family, *row_ranges_),
          std::cref(row_regexes_)) {}

bool FilteredColumnFamilyStream::ApplyFilter(
    InternalFilter const& internal_filter) {
  assert(!initialized_);
  return absl::visit(FilterApply(*this), internal_filter);
}

bool FilteredColumnFamilyStream::HasValue() const {
  InitializeIfNeeded();
  return *row_it_ != rows_.end();
}
CellView const& FilteredColumnFamilyStream::Value() const {
  InitializeIfNeeded();
  if (!cur_value_) {
    cur_value_ = CellView((*row_it_)->first, column_family_name_,
                          column_it_.value()->first, cell_it_.value()->first,
                          cell_it_.value()->second);
  }
  return cur_value_.value();
}

bool FilteredColumnFamilyStream::Next(NextMode mode) {
  InitializeIfNeeded();
  cur_value_.reset();
  assert(*row_it_ != rows_.end());
  assert(column_it_.value() != columns_.value().end());
  assert(cell_it_.value() != cells_.value().end());

  if (mode == NextMode::kCell) {
    ++(cell_it_.value());
    if (cell_it_.value() != cells_.value().end()) {
      return true;
    }
  }
  if (mode == NextMode::kCell || mode == NextMode::kColumn) {
    ++(column_it_.value());
    if (PointToFirstCellAfterColumnChange()) {
      return true;
    }
  }
  ++(*row_it_);
  PointToFirstCellAfterRowChange();
  return true;
}

void FilteredColumnFamilyStream::InitializeIfNeeded() const {
  if (!initialized_) {
    row_it_ = rows_.begin();
    PointToFirstCellAfterRowChange();
    initialized_ = true;
  }
}

bool FilteredColumnFamilyStream::PointToFirstCellAfterColumnChange() const {
  for (; column_it_.value() != columns_.value().end(); ++(column_it_.value())) {
    cells_ = TimestampRangeFilteredMapView<ColumnRow>(
        column_it_.value()->second, timestamp_ranges_);
    cell_it_ = cells_.value().begin();
    if (cell_it_.value() != cells_.value().end()) {
      return true;
    }
  }
  return false;
}

bool FilteredColumnFamilyStream::PointToFirstCellAfterRowChange() const {
  for (; (*row_it_) != rows_.end(); ++(*row_it_)) {
    columns_ = RegexFiteredMapView<StringRangeFilteredMapView<ColumnFamilyRow>>(
        StringRangeFilteredMapView<ColumnFamilyRow>((*row_it_)->second,
                                                    column_ranges_),
        column_regexes_);
    column_it_ = columns_.value().begin();
    if (PointToFirstCellAfterColumnChange()) {
      return true;
    }
  }
  return false;
}

StatusOr<std::shared_ptr<ColumnFamily>> ColumnFamily::ConstructColumnFamily(
    absl::optional<google::bigtable::admin::v2::Type> maybe_value_type,
    absl::optional<google::bigtable::admin::v2::GcRule> maybe_gc_rule) {
  auto cf = std::make_shared<ColumnFamily>();

  google::bigtable::admin::v2::Type value_type;

  if (maybe_value_type.has_value()) {
    auto& value_type = maybe_value_type.value();

    if (value_type.has_aggregate_type()) {
      auto const& aggregate_type = value_type.aggregate_type();
      switch (aggregate_type.aggregator_case()) {
        case google::bigtable::admin::v2::Type::Aggregate::kSum:
          cf->update_cell_ = cf->SumUpdateCellBEInt64;
          break;
        case google::bigtable::admin::v2::Type::Aggregate::kMin:
          cf->update_cell_ = cf->MinUpdateCellBEInt64;
          break;
        case google::bigtable::admin::v2::Type::Aggregate::kMax:
          cf->update_cell_ = cf->MaxUpdateCellBEInt64;
          break;
        default:
          return InvalidArgumentError(
              "unsupported aggregation type",
              GCP_ERROR_INFO().WithMetadata(
                  "aggregation case",
                  absl::StrFormat("%d", aggregate_type.aggregator_case())));
      }

      cf->value_type_ = std::move(value_type);
    } else {
      return InvalidArgumentError(
          "no aggregate type set in the supplied value_type",
          GCP_ERROR_INFO().WithMetadata("supplied value type",
                                        value_type.DebugString()));
    }
  }

  if (maybe_gc_rule.has_value()) {
    auto& gc_rule = maybe_gc_rule.value();

    auto status = CheckGCRuleIsValid(gc_rule);
    if (!status.ok()) {
      return status;
    }

    cf->gc_rule_ = std::move(gc_rule);
  }

  return cf;
}

Status CheckGCRuleIsValid(google::bigtable::admin::v2::GcRule const& rule) {
  Status size_check = CheckGCRuleSizeIsBelowLimit(rule);
  if (!size_check.ok()) {
    return size_check;
  }
  return CheckGCRuleTreeHasValidFields(rule);
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
