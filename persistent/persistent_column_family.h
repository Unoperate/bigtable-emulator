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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSISTENT_PERSISTENT_COLUMN_FAMILY_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSISTENT_PERSISTENT_COLUMN_FAMILY_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/types/optional.h"
#include "cell_view.h"
#include "column_family.h"
#include "filter.h"
#include "persistent/key_coder.h"
#include "range_set.h"
#include "rocksdb/utilities/transaction_db.h"
#include <google/bigtable/admin/v2/types.pb.h>
#include <memory>
#include <set>
#include <string>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/// The RocksDB column family storing the Bigtable family `column_family_name`.
/// The prefix keeps a family named "default" away from RocksDB's own.
std::string RocksDbColumnFamilyName(std::string const& column_family_name);

/**
 * Convenience wrapper around ColumnFamilyHandle
 */
class PersistentColumnFamily : public ColumnFamily {
 public:
  PersistentColumnFamily() = default;
  static StatusOr<std::shared_ptr<PersistentColumnFamily>> Create(
      std::shared_ptr<rocksdb::TransactionDB> db,
      rocksdb::ColumnFamilyOptions const& opts, std::string const& name);

  static StatusOr<std::shared_ptr<PersistentColumnFamily>> OpenExisting(
      std::shared_ptr<rocksdb::TransactionDB> db,
      std::shared_ptr<rocksdb::ColumnFamilyHandle> handle,
      absl::optional<google::bigtable::admin::v2::Type> const& value_type);

  PersistentColumnFamily(PersistentColumnFamily&& other) noexcept {
    db_ = std::move(other.db_);
    handle_ = std::move(other.handle_);
    value_type_ = std::move(other.value_type_);
  }

  bool RowKeyExists(std::string const& row_key) override;

  rocksdb::ColumnFamilyHandle* GetRaw() const { return handle_.get(); }

  std::unique_ptr<FilteredColumnFamilyStream> GetFilteredColumnFamilyStream(
      std::shared_ptr<StringRangeSet const> row_set,
      std::string column_family_name,
      std::shared_ptr<rocksdb::ManagedSnapshot> snapshot);

  void ResetHandle(std::shared_ptr<rocksdb::ColumnFamilyHandle> h) {
    handle_ = std::move(h);
  }

  rocksdb::ColumnFamilyHandle* ToRawPtr() const { return handle_.get(); }
  std::shared_ptr<rocksdb::ColumnFamilyHandle> GetHandle() { return handle_; };
  static StatusOr<std::shared_ptr<PersistentColumnFamily>>
  ConstructAggregateColumnFamily(
      google::bigtable::admin::v2::Type const& value_type,
      std::shared_ptr<rocksdb::TransactionDB> db_,
      rocksdb::ColumnFamilyOptions const& opts, std::string const& name);

 private:
  // Owning  table
  std::shared_ptr<rocksdb::TransactionDB> db_;
  std::shared_ptr<rocksdb::ColumnFamilyHandle> handle_;
  google::cloud::Status ConfigureFromValueType(
      absl::optional<google::bigtable::admin::v2::Type> const& value_type);
};

class FilteredPersistentColumnFamilyStream : public FilteredColumnFamilyStream {
 public:
  FilteredPersistentColumnFamilyStream(
      std::shared_ptr<rocksdb::ColumnFamilyHandle> handle,
      std::string column_family_name,
      std::shared_ptr<rocksdb::TransactionDB> db,
      std::shared_ptr<StringRangeSet const> row_set,
      std::shared_ptr<rocksdb::ManagedSnapshot> snapshot);
  bool HasValue() const override;
  CellView const& Value() const override;
  bool Next(NextMode mode) override;

 private:
  void InitializeIfNeeded() const;

  void JumpToNextValid() const;

  std::shared_ptr<rocksdb::ColumnFamilyHandle> handle_;
  std::shared_ptr<rocksdb::TransactionDB> db_;
  std::shared_ptr<rocksdb::ManagedSnapshot> snapshot_;

  mutable std::set<StringRangeSet::Range,
                   StringRangeSet::Range::StartLess>::const_iterator
      col_filter_pos_;
  mutable std::set<TimestampRangeSet::Range,
                   TimestampRangeSet::Range::StartLess>::const_reverse_iterator
      ts_filter_pos_;
  mutable absl::optional<std::string> current_row_key_tracker_;
  mutable absl::optional<std::string> current_col_key_tracker_;

  mutable std::unique_ptr<rocksdb::Iterator> it_;
  mutable std::string curr_value_string_;
  mutable absl::optional<CellView> cur_value_;
  mutable std::set<StringRangeSet::Range,
                   StringRangeSet::Range::StartLess>::const_iterator
      row_filter_pos_;
  mutable absl::optional<DecodeResult> curr_decoded_key_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSISTENT_PERSISTENT_COLUMN_FAMILY_H
