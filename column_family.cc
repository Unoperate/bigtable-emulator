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
#include "absl/strings/str_split.h"
#include "cell_view.h"
#include "filter.h"
#include "filtered_map.h"
#include "key_coder.h"
#include "range_set.h"
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <cassert>
#include <chrono>
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
std::string const kUpperBound =
    std::string({KeyCoder::kSeparator, KeyCoder::kExistingEsc});
}  // anonymous namespace

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

Status ColumnFamily::ChooseCellUpdateFunction(
    google::bigtable::admin::v2::Type value_type,
    std::shared_ptr<ColumnFamily> const& cf) {
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
    return Status();
  }

  return InvalidArgumentError(
      "no aggregate type set in the supplied value_type",
      GCP_ERROR_INFO().WithMetadata("supplied value type",
                                    value_type.DebugString()));
}

absl::optional<std::string> InMemoryColumnFamily::SetCell(
    std::string const& row_key, std::string const& column_qualifier,
    std::chrono::milliseconds timestamp, std::string const& value) {
  return rows_[row_key].SetCell(column_qualifier, timestamp, value);
}

StatusOr<absl::optional<std::string>> InMemoryColumnFamily::UpdateCell(
    std::string const& row_key, std::string const& column_qualifier,
    std::chrono::milliseconds timestamp, std::string& value) {
  return rows_[row_key].UpdateCell(column_qualifier, timestamp, value,
                                   update_cell_);
}

std::map<std::string, std::vector<Cell>> InMemoryColumnFamily::DeleteRow(
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

std::vector<Cell> InMemoryColumnFamily::DeleteColumn(
    std::string const& row_key, std::string const& column_qualifier,
    ::google::bigtable::v2::TimestampRange const& time_range) {
  auto row_it = rows_.find(row_key);

  return DeleteColumn(row_it, column_qualifier, time_range);
}

std::vector<Cell> InMemoryColumnFamily::DeleteColumn(
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

absl::optional<Cell> InMemoryColumnFamily::DeleteTimeStamp(
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

std::unique_ptr<AbstractCellStreamImpl>
InMemoryColumnFamily::GetFilteredColumnFamilyStream(
    std::shared_ptr<StringRangeSet const> row_set,
    std::string column_family_name) {
  return std::make_unique<FilteredInMemoryColumnFamilyStream>(
      *this, column_family_name, row_set);
}

StatusOr<std::shared_ptr<PersistentColumnFamily>>
PersistentColumnFamily::Create(std::shared_ptr<rocksdb::TransactionDB> db,
                               rocksdb::ColumnFamilyOptions const& opts,
                               std::string const& name) {
  PersistentColumnFamily pcf;
  pcf.db_ = std::move(db);

  rocksdb::ColumnFamilyHandle* raw = nullptr;
  auto status = pcf.db_->CreateColumnFamily(opts, name, &raw);
  if (!status.ok()) {
    return InternalError("Failed to create Column Family: " + status.ToString(),
                         GCP_ERROR_INFO().WithMetadata("cf name", name));
  }

  auto db_keepalive = pcf.db_;
  pcf.handle_ = std::shared_ptr<rocksdb::ColumnFamilyHandle>(
      raw, [db_keepalive](rocksdb::ColumnFamilyHandle* h) {
        if (!h) return;
        (void)db_keepalive->DestroyColumnFamilyHandle(h);
      });
  return std::make_shared<PersistentColumnFamily>(std::move(pcf));
}

std::unique_ptr<AbstractCellStreamImpl>
PersistentColumnFamily::GetFilteredColumnFamilyStream(
    std::shared_ptr<StringRangeSet const> row_set,
    std::string column_family_name) {
  return std::make_unique<FilteredPersistentColumnFamilyStream>(
      handle_, column_family_name, db_, row_set);
}

class FilteredInMemoryColumnFamilyStream::FilterApply {
 public:
  explicit FilterApply(FilteredInMemoryColumnFamilyStream& parent)
      : parent_(parent) {}

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
  FilteredInMemoryColumnFamilyStream& parent_;
};

FilteredInMemoryColumnFamilyStream::FilteredInMemoryColumnFamilyStream(
    InMemoryColumnFamily const& column_family, std::string column_family_name,
    std::shared_ptr<StringRangeSet const> row_set)
    : column_family_name_(std::move(column_family_name)),
      row_ranges_(std::move(row_set)),
      column_ranges_(StringRangeSet::All()),
      timestamp_ranges_(TimestampRangeSet::All()),
      rows_(StringRangeFilteredMapView<InMemoryColumnFamily>(column_family,
                                                             *row_ranges_),
            std::cref(row_regexes_)) {}

bool FilteredInMemoryColumnFamilyStream::ApplyFilter(
    InternalFilter const& internal_filter) {
  assert(!initialized_);
  return absl::visit(FilterApply(*this), internal_filter);
}

bool FilteredInMemoryColumnFamilyStream::HasValue() const {
  InitializeIfNeeded();
  return *row_it_ != rows_.end();
}
CellView const& FilteredInMemoryColumnFamilyStream::Value() const {
  InitializeIfNeeded();
  if (!cur_value_) {
    cur_value_ = CellView((*row_it_)->first, column_family_name_,
                          column_it_.value()->first, cell_it_.value()->first,
                          cell_it_.value()->second);
  }
  return cur_value_.value();
}

bool FilteredInMemoryColumnFamilyStream::Next(NextMode mode) {
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

void FilteredInMemoryColumnFamilyStream::InitializeIfNeeded() const {
  if (!initialized_) {
    row_it_ = rows_.begin();
    PointToFirstCellAfterRowChange();
    initialized_ = true;
  }
}

bool FilteredInMemoryColumnFamilyStream::PointToFirstCellAfterColumnChange()
    const {
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

bool FilteredInMemoryColumnFamilyStream::PointToFirstCellAfterRowChange()
    const {
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

class FilteredPersistentColumnFamilyStream::FilterApply {
 public:
  explicit FilterApply(FilteredPersistentColumnFamilyStream& parent)
      : parent_(parent) {}

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
  FilteredPersistentColumnFamilyStream& parent_;
};

FilteredPersistentColumnFamilyStream::FilteredPersistentColumnFamilyStream(
    std::shared_ptr<rocksdb::ColumnFamilyHandle> handle,
    std::string column_family_name, std::shared_ptr<rocksdb::TransactionDB> db,
    std::shared_ptr<StringRangeSet const> row_set)
    : column_family_name_(std::move(column_family_name)),
      handle_(std::move(handle)),
      db_(std::move(db)),
      row_ranges_(std::move(row_set)),
      timestamp_ranges_(TimestampRangeSet::All()),
      column_ranges_(StringRangeSet::All()),
      curr_decoded_key_() {}

bool FilteredPersistentColumnFamilyStream::ApplyFilter(
    InternalFilter const& internal_filter) {
  assert(!initialized_);
  return absl::visit(FilterApply(*this), internal_filter);
}

bool FilteredPersistentColumnFamilyStream::HasValue() const {
  InitializeIfNeeded();
  return it_->Valid();
}

CellView const& FilteredPersistentColumnFamilyStream::Value() const {
  InitializeIfNeeded();
  if (!cur_value_) {
    cur_value_ = CellView(
        curr_decoded_key_.row, column_family_name_, curr_decoded_key_.col,
        std::chrono::milliseconds(curr_decoded_key_.timestamp),
        curr_value_string_);
  }
  return cur_value_.value();
}

bool FilteredPersistentColumnFamilyStream::Next(NextMode mode) {
  InitializeIfNeeded();
  cur_value_.reset();

  if (!it_->Valid()) return false;

  if (mode == NextMode::kCell) {
    it_->Next();
  } else if (mode == NextMode::kColumn) {
    it_->Seek(
        KeyCoder::PartialEncode(curr_decoded_key_.row, curr_decoded_key_.col) +
        kUpperBound);
  } else if (mode == NextMode::kRow) {
    it_->Seek(curr_decoded_key_.row + kUpperBound);
  } else {
    return false;
  }

  return JumpToNextValid();
}

bool FilteredPersistentColumnFamilyStream::MatchesAll(
    absl::string_view value,
    std::vector<std::shared_ptr<re2::RE2 const>> const& regexes) const {
  if (regexes.empty()) return true;
  if (std::all_of(regexes.begin(), regexes.end(),
                  [value](std::shared_ptr<RE2 const> const& re) {
                    return re2::RE2::PartialMatch(value, *re);
                  })) {
    return true;
  }
  return false;
}

bool FilteredPersistentColumnFamilyStream::JumpToNextValid() {
  auto const& r_ranges = row_ranges_->disjoint_ranges();
  auto const& c_ranges = column_ranges_.disjoint_ranges();
  auto const& t_ranges = timestamp_ranges_.disjoint_ranges();

  while (it_->Valid()) {
    auto decoded = KeyCoder::Decode(it_->key().ToString()).value();

    if (row_filter_pos_ == r_ranges.end()) return false;

    if (row_filter_pos_->IsAboveEnd(decoded.row)) {
      ++row_filter_pos_;
      if (row_filter_pos_ == r_ranges.end()) return false;
      it_->Seek(row_filter_pos_->start_finite());
      continue;
    }

    if (row_filter_pos_->IsBelowStart(decoded.row)) {
      it_->Seek(row_filter_pos_->start_finite());

      if (it_->Valid()) {
        auto new_decoded = KeyCoder::Decode(it_->key().ToString()).value();
        if (row_filter_pos_->IsBelowStart(new_decoded.row)) {
          it_->Next();
        }
      }
      continue;
    }

    if (decoded.row != current_row_key_tracker_) {
      if (!MatchesAll(decoded.row, row_regexes_)) {
        it_->Seek(decoded.row + kUpperBound);
        continue;
      }

      current_row_key_tracker_ = decoded.row;
      col_filter_pos_ = c_ranges.begin();
      current_col_key_tracker_ = "";
      ts_filter_pos_ = t_ranges.rbegin();
    }

    if (col_filter_pos_ == c_ranges.end()) {
      it_->Seek(decoded.row + kUpperBound);
      continue;
    }

    if (col_filter_pos_->IsAboveEnd(decoded.col)) {
      ++col_filter_pos_;
      if (col_filter_pos_ == c_ranges.end()) {
        it_->Seek(decoded.row + kUpperBound);
      } else {
        it_->Seek(KeyCoder::PartialEncode(decoded.row,
                                          col_filter_pos_->start_finite()));
      }
      continue;
    }

    if (col_filter_pos_->IsBelowStart(decoded.col)) {
      it_->Seek(KeyCoder::PartialEncode(decoded.row,
                                        col_filter_pos_->start_finite()));
      continue;
    }

    if (decoded.col != current_col_key_tracker_) {
      if (!MatchesAll(decoded.col, column_regexes_)) {
        it_->Seek(KeyCoder::PartialEncode(decoded.row, decoded.col) +
                  kUpperBound);
        continue;
      }

      current_col_key_tracker_ = decoded.col;
      ts_filter_pos_ = t_ranges.rbegin();
    }

    if (ts_filter_pos_ == t_ranges.rend()) {
      it_->Seek(KeyCoder::PartialEncode(decoded.row, decoded.col) +
                kUpperBound);
      continue;
    }

    using TimestampValue = TimestampRangeSet::Range::Value;

    if (ts_filter_pos_->IsAboveEnd(TimestampValue(decoded.timestamp))) {
      it_->Seek(KeyCoder::Encode(decoded.row, decoded.col,
                                 ts_filter_pos_->end().count()));

      if (it_->Valid()) {
        auto new_decoded = KeyCoder::Decode(it_->key().ToString()).value();

        if (new_decoded.row == decoded.row && new_decoded.col == decoded.col) {
          if (ts_filter_pos_->IsAboveEnd(
                  TimestampValue(new_decoded.timestamp))) {
            it_->Next();
          }
        }
      }
      continue;
    }

    if (ts_filter_pos_->IsBelowStart(TimestampValue(decoded.timestamp))) {
      ++ts_filter_pos_;

      if (ts_filter_pos_ != t_ranges.rend()) {
        it_->Seek(KeyCoder::Encode(decoded.row, decoded.col,
                                   ts_filter_pos_->end().count()));
      } else {
        auto new_decoded = KeyCoder::Decode(it_->key().ToString()).value();
        it_->Seek(KeyCoder::PartialEncode(decoded.row, decoded.col) +
                  kUpperBound);
      }
      continue;
    }

    curr_decoded_key_ = decoded;
    curr_value_string_ = it_->value().ToString();

    cur_value_.reset();
    return true;
  }
  return true;
}

void FilteredPersistentColumnFamilyStream::InitializeIfNeeded() const {
  if (initialized_) return;
  initialized_ = true;

  rocksdb::ReadOptions opts;
  it_.reset(db_->NewIterator(opts, handle_.get()));

  auto const& ranges = row_ranges_->disjoint_ranges();
  row_filter_pos_ = ranges.begin();

  if (row_filter_pos_ == ranges.end()) {
    return;
  }

  it_->Seek(row_filter_pos_->start_finite());
  const_cast<FilteredPersistentColumnFamilyStream*>(this)->JumpToNextValid();
}

StatusOr<std::shared_ptr<InMemoryColumnFamily>>
InMemoryColumnFamily::ConstructAggregateColumnFamily(
    google::bigtable::admin::v2::Type value_type) {
  auto cf = std::make_shared<InMemoryColumnFamily>();
  auto status = ChooseCellUpdateFunction(std::move(value_type), cf);
  if (!status.ok()) {
    return status;
  }
  return cf;
}

StatusOr<std::shared_ptr<PersistentColumnFamily>>
PersistentColumnFamily::ConstructAggregateColumnFamily(
    google::bigtable::admin::v2::Type const& value_type,
    std::shared_ptr<rocksdb::TransactionDB> db_, std::string const& name) {
  rocksdb::ColumnFamilyOptions opts;
  auto maybe_cf = Create(std::move(db_), opts, name);
  if (!maybe_cf) {
    return InternalError(
        "Failed to create aggregate persistent column family: " +
            maybe_cf.status().message(),
        GCP_ERROR_INFO().WithMetadata("supplied value type",
                                      value_type.DebugString()));
  }

  auto cf = maybe_cf.value();
  Status status = ChooseCellUpdateFunction(value_type, cf);
  if (!status.ok()) {
    return status;
  }
  return cf;
}

google::cloud::Status PersistentColumnFamily::ConfigureFromValueType(
    absl::optional<google::bigtable::admin::v2::Type> const& value_type) {
  value_type_ = value_type;
  update_cell_ = DefaultUpdateCell;

  if (!value_type.has_value()) return google::cloud::Status();
  if (!value_type->has_aggregate_type()) return google::cloud::Status();

  auto const& aggregate = value_type->aggregate_type();
  switch (aggregate.aggregator_case()) {
    case google::bigtable::admin::v2::Type::Aggregate::kSum:
      update_cell_ = SumUpdateCellBEInt64;
      return google::cloud::Status();
    case google::bigtable::admin::v2::Type::Aggregate::kMin:
      update_cell_ = MinUpdateCellBEInt64;
      return google::cloud::Status();
    case google::bigtable::admin::v2::Type::Aggregate::kMax:
      update_cell_ = MaxUpdateCellBEInt64;
      return google::cloud::Status();
    default:
      return InvalidArgumentError(
          "unsupported aggregation type",
          GCP_ERROR_INFO().WithMetadata(
              "aggregation_case",
              absl::StrFormat("%d", aggregate.aggregator_case())));
  }
}

StatusOr<std::shared_ptr<PersistentColumnFamily>>
PersistentColumnFamily::OpenExisting(
    std::shared_ptr<rocksdb::TransactionDB> db,
    std::shared_ptr<rocksdb::ColumnFamilyHandle> handle,
    absl::optional<google::bigtable::admin::v2::Type> const& value_type) {
  auto cf = std::make_shared<PersistentColumnFamily>();
  cf->db_ = std::move(db);
  cf->handle_ = std::move(handle);

  auto st = cf->ConfigureFromValueType(value_type);
  if (!st.ok()) return st;
  return cf;
}

bool PersistentColumnFamily::RowKeyExists(std::string const& row_key) {
  auto it = std::unique_ptr<rocksdb::Iterator>(
      db_->NewIterator(rocksdb::ReadOptions(), handle_.get()));

  it->Seek(row_key);
  if (!it->Valid()) {
    return false;
  }

  auto key = it->key();
  auto res = KeyCoder::Decode(key.ToString());
  if (!res.ok()) {
    return false;
  }

  if (res.value().row == row_key) {
    return true;
  }
  return false;
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
