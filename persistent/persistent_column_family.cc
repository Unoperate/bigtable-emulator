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

#include "persistent/persistent_column_family.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_format.h"
#include "cell_view.h"
#include "column_family.h"
#include "filter.h"
#include "persistent/key_coder.h"
#include "range_set.h"
#include "re2/re2.h"
#include "rocksdb/utilities/transaction_db.h"
#include <google/bigtable/admin/v2/types.pb.h>
#include <algorithm>
#include <cassert>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

namespace {

bool MatchesAll(std::string_view value,
                std::vector<std::shared_ptr<re2::RE2 const>> const& regexes) {
  return std::all_of(regexes.begin(), regexes.end(),
                     [value](std::shared_ptr<re2::RE2 const> const& re) {
                       return re2::RE2::PartialMatch(value, *re);
                     });
}

}  // namespace

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
        if (!h) {
          return;
        }
        (void)db_keepalive->DestroyColumnFamilyHandle(h);
      });
  return std::make_shared<PersistentColumnFamily>(std::move(pcf));
}

std::unique_ptr<FilteredColumnFamilyStream>
PersistentColumnFamily::GetFilteredColumnFamilyStream(
    std::shared_ptr<StringRangeSet const> row_set,
    std::string column_family_name,
    std::shared_ptr<rocksdb::ManagedSnapshot> snapshot) {
  return std::make_unique<FilteredPersistentColumnFamilyStream>(
      handle_, std::move(column_family_name), db_, std::move(row_set),
      std::move(snapshot));
}

FilteredPersistentColumnFamilyStream::FilteredPersistentColumnFamilyStream(
    std::shared_ptr<rocksdb::ColumnFamilyHandle> handle,
    std::string column_family_name, std::shared_ptr<rocksdb::TransactionDB> db,
    std::shared_ptr<StringRangeSet const> row_set,
    std::shared_ptr<rocksdb::ManagedSnapshot> snapshot)
    : FilteredColumnFamilyStream(std::move(column_family_name),
                                 std::move(row_set)),
      handle_(std::move(handle)),
      db_(std::move(db)),
      snapshot_(std::move(snapshot)) {
  rocksdb::ReadOptions opts;
  opts.snapshot = snapshot_->snapshot();
  it_.reset(db_->NewIterator(opts, handle_.get()));
}

bool FilteredPersistentColumnFamilyStream::HasValue() const {
  InitializeIfNeeded();
  return curr_decoded_key_.has_value();
}

CellView const& FilteredPersistentColumnFamilyStream::Value() const {
  InitializeIfNeeded();
  auto const& curr_key = *curr_decoded_key_;
  if (!cur_value_) {
    cur_value_ = CellView(curr_key.row, column_family_name_, curr_key.col,
                          std::chrono::microseconds(curr_key.timestamp),
                          curr_value_string_);
  }
  return cur_value_.value();
}

bool FilteredPersistentColumnFamilyStream::Next(NextMode mode) {
  InitializeIfNeeded();
  cur_value_.reset();

  // API specifies that false is returned only for an unsupported op.
  if (!curr_decoded_key_.has_value()) {
    return true;
  }

  auto const& curr_key_value = *curr_decoded_key_;
  if (mode == NextMode::kCell) {
    it_->Next();
  } else if (mode == NextMode::kColumn) {
    it_->Seek(KeyCoder::NextColumn(curr_key_value.row, curr_key_value.col));
  } else if (mode == NextMode::kRow) {
    it_->Seek(KeyCoder::NextRow(curr_key_value.row));
  } else {
    return false;
  }

  JumpToNextValid();
  return true;
}

void FilteredPersistentColumnFamilyStream::JumpToNextValid() const {
  auto const& r_ranges = row_ranges_->disjoint_ranges();
  auto const& c_ranges = column_ranges_.disjoint_ranges();
  auto const& t_ranges = timestamp_ranges_.disjoint_ranges();

  while (it_->Valid()) {
    auto decoded = KeyCoder::Decode(it_->key().ToStringView()).value();

    // 1. Check if row filters need to be advanced.
    if (row_filter_pos_ == r_ranges.end()) {
      break;
    }

    if (row_filter_pos_->IsAboveEnd(decoded.row)) {
      ++row_filter_pos_;
      continue;
    }

    if (row_filter_pos_->IsBelowStart(decoded.row)) {
      it_->Seek(KeyCoder::RowRangeStartKey(*row_filter_pos_));
      continue;
    }

    if (decoded.row != current_row_key_tracker_) {
      if (!MatchesAll(decoded.row, row_regexes_)) {
        it_->Seek(KeyCoder::NextRow(decoded.row));
        continue;
      }

      current_row_key_tracker_ = decoded.row;
      col_filter_pos_ = c_ranges.begin();
      current_col_key_tracker_.reset();
      ts_filter_pos_ = t_ranges.rbegin();
    }

    // 2. Check if column filters need to be advanced.
    if (col_filter_pos_ == c_ranges.end()) {
      it_->Seek(KeyCoder::NextRow(decoded.row));
      continue;
    }

    if (col_filter_pos_->IsAboveEnd(decoded.col)) {
      ++col_filter_pos_;
      continue;
    }

    if (col_filter_pos_->IsBelowStart(decoded.col)) {
      it_->Seek(KeyCoder::ColumnRangeStartKey(decoded.row, *col_filter_pos_));
      continue;
    }

    if (decoded.col != current_col_key_tracker_) {
      if (!MatchesAll(decoded.col, column_regexes_)) {
        it_->Seek(KeyCoder::NextColumn(decoded.row, decoded.col));
        continue;
      }

      current_col_key_tracker_ = decoded.col;
      ts_filter_pos_ = t_ranges.rbegin();
    }

    if (ts_filter_pos_ == t_ranges.rend()) {
      it_->Seek(KeyCoder::NextColumn(decoded.row, decoded.col));
      continue;
    }

    // 3. Check if timestamp filters need to be advanced.
    using TimestampValue = TimestampRangeSet::Range::Value;

    if (ts_filter_pos_->IsAboveEnd(TimestampValue(decoded.timestamp))) {
      it_->Seek(KeyCoder::Encode(
          decoded.row, decoded.col,
          static_cast<std::uint64_t>(ts_filter_pos_->end().count() - 1)));
      continue;
    }

    if (ts_filter_pos_->IsBelowStart(TimestampValue(decoded.timestamp))) {
      ++ts_filter_pos_;

      if (ts_filter_pos_ != t_ranges.rend()) {
        it_->Seek(KeyCoder::Encode(
            decoded.row, decoded.col,
            static_cast<std::uint64_t>(ts_filter_pos_->end().count() - 1)));
      } else {
        it_->Seek(KeyCoder::NextColumn(decoded.row, decoded.col));
      }
      continue;
    }

    curr_decoded_key_ = std::move(decoded);
    curr_value_string_ = it_->value().ToString();

    cur_value_.reset();
    return;
  }
  if (!it_->status().ok()) {
    google::cloud::internal::ThrowStatus(InternalError(
        "Failed to read column family: " + it_->status().ToString(),
        GCP_ERROR_INFO().WithMetadata("column family", column_family_name_)));
  }
  curr_decoded_key_ = absl::nullopt;
}

void FilteredPersistentColumnFamilyStream::InitializeIfNeeded() const {
  if (initialized_) {
    return;
  }
  initialized_ = true;

  auto const& ranges = row_ranges_->disjoint_ranges();
  row_filter_pos_ = ranges.begin();

  if (row_filter_pos_ == ranges.end()) {
    return;
  }

  it_->Seek(KeyCoder::RowRangeStartKey(*row_filter_pos_));
  JumpToNextValid();
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
  Status status = cf->ChooseCellUpdateFunction(value_type);
  if (!status.ok()) {
    return status;
  }
  return cf;
}

google::cloud::Status PersistentColumnFamily::ConfigureFromValueType(
    absl::optional<google::bigtable::admin::v2::Type> const& value_type) {
  value_type_ = value_type;
  update_cell_ = DefaultUpdateCell;

  if (!value_type.has_value()) {
    return google::cloud::Status();
  }
  if (!value_type->has_aggregate_type()) {
    return google::cloud::Status();
  }

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
  if (!st.ok()) {
    return st;
  }
  return cf;
}

bool PersistentColumnFamily::RowKeyExists(std::string const& row_key) {
  auto it = std::unique_ptr<rocksdb::Iterator>(
      db_->NewIterator(rocksdb::ReadOptions(), handle_.get()));

  auto const row_prefix = KeyCoder::RowPrefix(row_key);
  it->Seek(row_prefix);
  return it->Valid() && it->key().starts_with(row_prefix);
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
