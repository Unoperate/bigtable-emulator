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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSISTENT_PERSISTENT_BACKEND_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSISTENT_PERSISTENT_BACKEND_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/types/optional.h"
#include "backend.h"
#include "column_family.h"
#include "filter.h"
#include "persistent/persistent_column_family.h"
#include "range_set.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table_operations.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class PersistentRowTransaction;

/// Keeps each table in its own RocksDB database at `<data_dir>/<table name>`.
class PersistentBackend final : public Backend {
 public:
  static StatusOr<std::unique_ptr<PersistentBackend>> Open(
      BackendOptions const& options);

  /// Opens every database found under `data_dir`. A table that fails to
  /// open is logged and skipped.
  StatusOr<std::vector<RestoredTable>> Restore() override;
  StatusOr<std::unique_ptr<TableOperations>> CreateTable(
      google::bigtable::admin::v2::Table const& schema) override;
  /// Removes the database synchronously; failures are logged.
  void DestroyTable(std::unique_ptr<TableOperations> operations) override;
  void StopBackgroundWork() override {}
  Status Shutdown() override { return Status(); }

 private:
  explicit PersistentBackend(BackendOptions options)
      : options_(std::move(options)) {}

  BackendOptions const options_;
};

class PersistentTableOperations
    : public TableOperationsBase<PersistentColumnFamily> {
 public:
  static StatusOr<std::unique_ptr<TableOperations>> CreateNew(
      std::string const& data_root, google::bigtable::admin::v2::Table& schema);

  static StatusOr<std::unique_ptr<TableOperations>> OpenExisting(
      std::string const& data_root, google::bigtable::admin::v2::Table& schema);

  /// `deadline` is not used yet.
  StatusOr<std::unique_ptr<RowTransaction>> NewRowTransaction(
      std::string const& row_key, Deadline deadline) override;

  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const override;

  StatusOr<std::vector<RowKeySample>> SampleRowKeys() override;

  void RunPeriodicMaintenance() override {}

  Status RemoveAllDataFromColumnFamilies() override;

  Status DropRowRange(std::string const& row_key_prefix) override;

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
      google::bigtable::admin::v2::Table schema) override;

  Status UpdateTable(
      google::bigtable::admin::v2::Table const& new_schema) override {
    return PersistSchema(new_schema);
  }

  Status CommitDeletion() override { return Status(); }

  absl::optional<std::chrono::milliseconds> MaintenanceInterval()
      const override {
    return absl::nullopt;
  }

  Status BackgroundError() const override { return Status(); }

  Status Close() override {
    column_families_.clear();
    db_.reset();
    return Status();
  }

  std::string const& table_name() const { return table_name_; }

  StatusOr<google::bigtable::admin::v2::Table> LoadSchema() const;

  std::unique_ptr<rocksdb::Iterator> GetIterator(
      std::shared_ptr<PersistentColumnFamily> const& cf) const {
    return std::unique_ptr<rocksdb::Iterator>(
        db_->NewIterator(rocksdb::ReadOptions(), cf->GetRaw()));
  }

  bool AllowsConcurrentDataOperations() const override { return true; }

  friend PersistentRowTransaction;

 private:
  Status PersistSchema(google::bigtable::admin::v2::Table const& schema);

  StatusOr<std::size_t> GetRowCountEstimate() const;

  Status ReconcileColumnFamiliesToTarget(
      ::google::bigtable::admin::v2::Table const& target_schema);

  std::string table_name_;
  /**
   * To close a rocksdb database, in the simplest case it is enough to delete a
   * pointer to it
   * (https://github.com/facebook/rocksdb/wiki/basic-operations#closing-a-database,
   * https://github.com/facebook/rocksdb/wiki/basic-operations#concurrency).
   * We also don't need to care about locking here, as rocksdb has internal
   * synchronization.
   */
  std::shared_ptr<rocksdb::TransactionDB> db_;
};

/// Reached only while the Table holds its lock, which outlives this object, so
/// a reference to the operations is enough.
class PersistentRowTransaction : public RowTransaction {
 public:
  explicit PersistentRowTransaction(PersistentTableOperations& utilities,
                                    std::string const& row_key,
                                    rocksdb::TransactionDB* db)
      : RowTransaction(row_key), utilities_(utilities) {
    txn_ = std::unique_ptr<rocksdb::Transaction>(
        db->BeginTransaction(rocksdb::WriteOptions()));
  }

  Status Commit() override;
  Status AddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      absl::optional<std::chrono::microseconds> timestamp_override) override;
  Status SetCell(
      ::google::bigtable::v2::Mutation_SetCell const& set_cell,
      absl::optional<std::chrono::microseconds> timestamp_override) override;
  Status MergeToCell(::google::bigtable::v2::Mutation_MergeToCell const&
                         merge_to_cell) override;
  Status DeleteFromColumn(
      ::google::bigtable::v2::Mutation_DeleteFromColumn const&
          delete_from_column) override;
  Status DeleteFromFamily(
      ::google::bigtable::v2::Mutation_DeleteFromFamily const&
          delete_from_family) override;
  Status DeleteFromRow() override;

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request) override;

  /// Reads the latest committed state through a snapshot; does not see this
  /// transaction's own writes yet.
  StatusOr<bool> RowMatches(
      absl::optional<btproto::RowFilter> const& filter) override;

 private:
  std::unique_ptr<rocksdb::Transaction> txn_;
  PersistentTableOperations& utilities_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSISTENT_PERSISTENT_BACKEND_H
