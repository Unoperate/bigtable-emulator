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

Status ColumnRow::RunGC(google::bigtable::admin::v2::GcRule const& gc_rule) {
  switch (gc_rule.rule_case()) {
    // FIXME: How should we be validating max_age?
    case google::bigtable::admin::v2::GcRule::kMaxAge: {
      ApplyGCRuleMaxAge(gc_rule.max_age());
      return Status();
    }
    case google::bigtable::admin::v2::GcRule::kMaxNumVersions: {
      auto max_num_versions = gc_rule.max_num_versions();
      if (max_num_versions < 0) {
        return InvalidArgumentError(
            "max_num_versions cannot be negative",
            GCP_ERROR_INFO().WithMetadata("rule", gc_rule.DebugString()));
      }

      auto n = static_cast<std::size_t>(max_num_versions);
      ApplyGCRuleMaxNumVersions(n);
      return Status();
    }
    case google::bigtable::admin::v2::GcRule::kIntersection: {
      auto status = ApplyGCRuleIntersection(gc_rule.intersection().rules());
      if (!status.ok()) {
        return status;
      }
      break;
    }
    case google::bigtable::admin::v2::GcRule::kUnion: {
      auto status = ApplyGCRuleUnion(gc_rule.union_().rules());
      if (!status.ok()) {
        return status;
      }
      break;
    }
    default: {
      return InvalidArgumentError(
          "unset or unknown GCRule",
          GCP_ERROR_INFO().WithMetadata("rule", gc_rule.DebugString()));
    }
  }

  return Status();
}

void ColumnRow::ApplyGCRuleMaxNumVersions(std::size_t n) {
  if (n >= cells_.size()) {
    return;
  }

  auto it = cells_.begin();
  std::advance(it, n);
  cells_.erase(it, cells_.end());
}

bool TimestampIsOlderThan(std::chrono::milliseconds timestamp,
                          protobuf::Duration const& max_age) {
  std::chrono::milliseconds max_age_ms(
      protobuf::util::TimeUtil::DurationToMilliseconds(max_age));

  auto cut_off_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()) -
                    max_age_ms;

  return timestamp < cut_off_ms;
}

void ColumnRow::ApplyGCRuleMaxAge(protobuf::Duration const& max_age) {
  std::chrono::milliseconds max_age_ms(
      protobuf::util::TimeUtil::DurationToMilliseconds(max_age));

  auto cut_off_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()) -
                    max_age_ms;
  auto newest_to_delete = cells_.upper_bound(cut_off_ms);

  cells_.erase(newest_to_delete, cells_.end());
}

// GCRuleEraseVerdict returns true if the cell pointed to by the
// iterator `it' should be erased according to the GcRule
// rule. Otherwise it returns false or an error status if the rule or
// any other rule it transitively contains fails to meet some
// invariant.
//
// Note that since a column family GCRule configuration must serialize
// to at most 500 bytes
// (https://github.com/googleapis/googleapis/blob/6d3a7f1b08c60a00926f7b15a1db69ec71bf501a/google/bigtable/admin/v2/table.proto#L343)
// and (in the case of a GCRule containing only a small
// max_num_versions) the minimum size of a GCRule is >= 2 bytes, a
// GCRule within size limits can embed at most 250 GCRules, which is
// also the maximum depth of recursion for this function.
//
// So we can expect that the maximum size of the stack used in
// recursion will be < 250KB, assuming each recursive call takes up
// less than 1KB of stack size (at most 500B for the rule and well
// less than 500B for the rest of the automatic variables -- which are
// all integers or pointers and would need to be > 60 in number for
// any call to exceed 500B).
//
// Therefore, since we enforce the size limit for a column family
// GCRule configuration before we store or modify it, it is safe to
// use recursion here (MacOS X has the lowest default stack size of
// 512KiB).
//
// NOLINTBEGIN(misc-no-recursion)
StatusOr<bool> ColumnRow::GCRuleEraseVerdict(
    google::bigtable::admin::v2::GcRule const& rule,
    std::map<std::chrono::milliseconds, std::string,
             std::greater<>>::const_iterator it) {
  switch (rule.rule_case()) {
    case google::bigtable::admin::v2::GcRule::kMaxAge: {
      return TimestampIsOlderThan(it->first, rule.max_age());
    }
    case google::bigtable::admin::v2::GcRule::kMaxNumVersions: {
      auto timestamp = it->first;

      auto max_num_versions = rule.max_num_versions();
      if (max_num_versions < 0) {
        return InvalidArgumentError(
            "max_num_versions cannot be negative",
            GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
      }

      auto n = static_cast<std::size_t>(max_num_versions);

      if (n >= cells_.size()) {
        return false;
      }

      auto first_deleted_it = cells_.begin();
      // Locate the first cell we would delete. We should delete
      // the cell the iterator `it' will point and all older cells,
      // which are just the cells that have a timestamp less than
      // the timestamp of the cell pointed to by `it', since cells
      // are in the reverse order of timestamps.
      //
      // FIXME: This operation is linear in the number of cells in the
      // column, meaning that in the worst case, if we iterate over
      // all the cells in the column to get a verdict, the total time
      // is quadratic in the number of cells in the column.
      std::advance(first_deleted_it, n);

      // Now it_2 points to the first cell that should be
      // deleted. If a cell has the timestamp of it_2 or younger, it
      // should also be erased.
      return timestamp <= first_deleted_it->first;
    }
    case google::bigtable::admin::v2::GcRule::kIntersection: {
      auto rules = rule.intersection().rules();

      // An empty rules vector should not cause any cell to be deleted.
      if (rules.empty()) {
        return false;
      }

      for (auto const& r : rules) {
        auto maybe_verdict = GCRuleEraseVerdict(r, it);
        if (!maybe_verdict) {
          return maybe_verdict.status();
        }

        auto verdict = maybe_verdict.value();
        if (!verdict) {
          return false;
        }
      }

      return true;
    }
    case google::bigtable::admin::v2::GcRule::kUnion: {
      auto rules = rule.union_().rules();

      // An empty rules vector should not cause any cell to be deleted.
      if (rules.empty()) {
        return false;
      }

      for (auto const& r : rules) {
        auto maybe_verdict = GCRuleEraseVerdict(r, it);
        if (!maybe_verdict) {
          return maybe_verdict.status();
        }

        auto verdict = maybe_verdict.value();
        if (verdict) {
          return true;
        }
      }

      return false;
    }
    default: {
      return InvalidArgumentError(
          "unset or unknown GCRule",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }
  }
}
// NOLINTEND(misc-no-recursion)

Status ColumnRow::ApplyGCRuleIntersection(
    protobuf::RepeatedPtrField<google::bigtable::admin::v2::GcRule> const&
        rules) {
  // Don't remove all the cells if in fact the list of rules to
  // intersect is empty.
  if (rules.empty()) {
    return Status();
  }

  for (auto it = cells_.begin(); it != cells_.end();) {
    auto delete_it = true;
    for (auto const& rule : rules) {
      auto maybe_verdict = GCRuleEraseVerdict(rule, it);
      if (!maybe_verdict) {
        return maybe_verdict.status();
      }
      // Should we erase?
      auto verdict = maybe_verdict.value();
      if (!verdict) {
        delete_it = false;
        break;
      }
    }

    if (!delete_it) {
      it++;
      continue;
    }

    it = cells_.erase(it);
  }

  return Status();
}

Status ColumnRow::ApplyGCRuleUnion(
    protobuf::RepeatedPtrField<google::bigtable::admin::v2::GcRule> const&
        rules) {
  // Don't consider any cells for deletion if in fact the list of
  // rules to union is empty.
  if (rules.empty()) {
    return Status();
  }

  for (auto it = cells_.begin(); it != cells_.end();) {
    auto delete_it = false;
    for (auto const& rule : rules) {
      auto maybe_verdict = GCRuleEraseVerdict(rule, it);
      if (!maybe_verdict) {
        return maybe_verdict.status();
      }
      // Should we erase?
      auto verdict = maybe_verdict.value();
      if (verdict) {
        delete_it = true;
        break;
      }
    }

    if (!delete_it) {
      it++;
      continue;
    }

    it = cells_.erase(it);
  }

  return Status();
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

    auto status = CheckGCRuleSizeIsBelowLimit(gc_rule);
    if (!status.ok()) {
      return status;
    }

    cf->gc_rule_ = std::move(gc_rule);
  }

  return cf;
}

Status CheckGCRuleSizeIsBelowLimit(
    google::bigtable::admin::v2::GcRule const& rule) {
  // As per the spec, limit the size of the serialized gc_rule to
  // 500 bytes. This is important for controlling the depth of
  // recursion in the GC thread later on (and therefore protecting
  // from a stack overflow).
  std::size_t gc_rule_size = rule.ByteSizeLong();
  if (gc_rule_size > kMaxGCRuleSize) {
    return InvalidArgumentError(
        "Supplied GcRule is too large: It must not exceed 500 bytes (when "
        "serialized)",
        GCP_ERROR_INFO().WithMetadata("GcRule", rule.DebugString()));
  }

  return Status();
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
