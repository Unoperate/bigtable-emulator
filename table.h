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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/match.h"
#include "column_family.h"
#include "filter.h"
#include "range_set.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "row_streamer.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stack>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class Table;
class RowTransaction;
class InMemoryRowTransaction;
class PersistentRowTransaction;

struct RestoreValue {
  InMemoryColumnFamily& column_family;
  std::string column_qualifier;
  std::chrono::milliseconds timestamp;
  std::string value;
};

struct DeleteValue {
  InMemoryColumnFamily& column_family;
  std::string column_qualifier;
  std::chrono::milliseconds timestamp;
};

google::bigtable::v2::ReadModifyWriteRowResponse
FamiliesToReadModifyWriteResponse(
    std::string const& row_key,
    std::map<std::string, InMemoryColumnFamily> const& families);

/**
 * This class encapsulates implementation-specific details of a table,
 * such as creating cell streams, locking etc.
 */
class TableOperations {
 public:
  virtual ~TableOperations() = default;

  virtual std::unique_ptr<RowTransaction> NewRowTransaction(
      std::string const& row_key) = 0;

  virtual StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const = 0;

  virtual StatusOr<std::size_t> GetRowCountEstimate() = 0;

  virtual Status RemoveAllDataFromColumnFamilies() = 0;

  virtual Status DropRowRange(std::string const& row_key_prefix) = 0;

  virtual StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
      google::bigtable::admin::v2::Table schema) = 0;

  virtual void Cleanup() = 0;

  virtual Status PersistSchema(google::bigtable::admin::v2::Table const&) = 0;

  /**
   * Table object would like to use locking, but depending on the type
   * (InMemory, Persistent), it would like to lock a different lock (mutex, or
   * shared). ScopedLock abstract this.
   */
  class ScopedLock {
   public:
    explicit ScopedLock(TableOperations* parent, bool modify_cfs)
        : parent_(parent), modify_cfs_(modify_cfs) {
      parent_->LockScopeImpl(modify_cfs_);
    }

    ~ScopedLock() { parent_->UnlockScopeImpl(modify_cfs_); }

   private:
    TableOperations* parent_;
    bool modify_cfs_;
  };

  ScopedLock LockScope(bool modify_cfs) { return ScopedLock(this, modify_cfs); }

 protected:
  virtual void LockScopeImpl(bool modify_cfs) const = 0;
  virtual void UnlockScopeImpl(bool modify_cfs) const = 0;
};

class InMemoryTableOperations
    : public TableOperations,
      public std::enable_shared_from_this<InMemoryTableOperations> {
 public:
  static StatusOr<std::shared_ptr<TableOperations>> Create(
      google::bigtable::admin::v2::Table const& schema);

  std::unique_ptr<RowTransaction> NewRowTransaction(
      std::string const& row_key) override;

  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const override;

  StatusOr<std::size_t> GetRowCountEstimate() override {
    std::size_t row_count_estimate = 0;
    for (auto const& cf : column_families_) {
      auto ccf = std::static_pointer_cast<InMemoryColumnFamily>(cf.second);
      row_count_estimate = std::max(ccf->size(), row_count_estimate);
    }

    return row_count_estimate;
  };

  Status RemoveAllDataFromColumnFamilies() override {
    for (auto& column_family : column_families_) {
      column_family.second->RemoveAllDataFromColumnFamily();
    }
    return Status();
  }

  Status DropRowRange(std::string const& row_key_prefix) override {
    for (auto& cf : column_families_) {
      auto ccf = std::static_pointer_cast<InMemoryColumnFamily>(cf.second);
      for (auto row_it = ccf->lower_bound(row_key_prefix);
           row_it != ccf->end();) {
        if (absl::StartsWith(row_it->first, row_key_prefix)) {
          row_it = ccf->erase(row_it);
        } else {
          break;
        }
      }
    }
    return Status();
  }

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
      google::bigtable::admin::v2::Table schema) override;

  Status PersistSchema(
      google::bigtable::admin::v2::Table const& /*schema*/) override {
    return Status();
  }

  void Cleanup() override { column_families_.clear(); }

  std::shared_ptr<InMemoryTableOperations> get() { return shared_from_this(); }

  template <typename MESSAGE>
  StatusOr<std::shared_ptr<InMemoryColumnFamily>> FindColumnFamily(
      MESSAGE const& message) const;

  std::map<std::string, std::shared_ptr<InMemoryColumnFamily>>::iterator
  begin() {
    return column_families_.begin();
  }
  std::map<std::string, std::shared_ptr<InMemoryColumnFamily>>::iterator end() {
    return column_families_.end();
  }
  std::map<std::string, std::shared_ptr<InMemoryColumnFamily>>::iterator find(
      std::string const& column_family) {
    return column_families_.find(column_family);
  }

 protected:
  void LockScopeImpl(bool /*modify_cfs*/) const override { mu_.lock(); }

  void UnlockScopeImpl(bool /*modify_cfs*/) const override { mu_.unlock(); }

 private:
  std::map<std::string, std::shared_ptr<InMemoryColumnFamily>> column_families_;
  mutable std::mutex mu_;
};

class PersistentTableOperations
    : public TableOperations,
      public std::enable_shared_from_this<PersistentTableOperations> {
 public:
  static StatusOr<std::shared_ptr<TableOperations>> CreateNew(
      std::string const& data_root, google::bigtable::admin::v2::Table& schema);

  static StatusOr<std::shared_ptr<TableOperations>> OpenExisting(
      std::string const& data_root, google::bigtable::admin::v2::Table& schema);

  std::unique_ptr<RowTransaction> NewRowTransaction(
      std::string const& row_key) override;

  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const override;

  StatusOr<std::size_t> GetRowCountEstimate() override;
  Status RemoveAllDataFromColumnFamilies() override;

  Status DropRowRange(std::string const& row_key_prefix) override;

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
      google::bigtable::admin::v2::Table schema) override;

  Status PersistSchema(
      google::bigtable::admin::v2::Table const& schema) override;

  void Cleanup() override {
    /**
     * We need to clear these because both column families and db_ hold
     * a shared ptr to the owning Table. If we didn't do this, the Table's
     * destructor would never be called, and so we would have a memory leak.
     */
    column_families_.clear();
    db_.reset();
  }

  StatusOr<google::bigtable::admin::v2::Table> LoadSchema() const;

  std::shared_ptr<PersistentTableOperations> get() {
    return shared_from_this();
  }

  template <typename MESSAGE>
  StatusOr<std::shared_ptr<PersistentColumnFamily>> FindColumnFamily(
      MESSAGE const& message) const;

  std::map<std::string, std::shared_ptr<PersistentColumnFamily>>::iterator
  begin() {
    return column_families_.begin();
  }
  std::map<std::string, std::shared_ptr<PersistentColumnFamily>>::iterator
  end() {
    return column_families_.end();
  }
  std::map<std::string, std::shared_ptr<PersistentColumnFamily>>::iterator find(
      std::string const& column_family) {
    return column_families_.find(column_family);
  }

  std::unique_ptr<rocksdb::Iterator> GetIterator(
      std::shared_ptr<PersistentColumnFamily> const& cf) const {
    return std::unique_ptr<rocksdb::Iterator>(
        db_->NewIterator(rocksdb::ReadOptions(), cf->GetRaw()));
  }

  friend PersistentRowTransaction;

 protected:
  /**
   * Persistent table gets the shared lock when it doesn't modify schema,
   * and exclusive otherwise.
   *
   * @param modify_cfs
   * Controls whether to get exclusive or shared lock
   */
  void LockScopeImpl(bool modify_cfs) const override {
    if (modify_cfs) {
      mu_.lock();
    } else {
      mu_.lock_shared();
    }
  }

  /**
   * Persistent table gets the shared lock when it doesn't modify schema,
   * and exclusive otherwise.
   *
   * @param modify_cfs
   * Controls whether to get exclusive or shared lock
   */
  void UnlockScopeImpl(bool modify_cfs) const override {
    if (modify_cfs) {
      mu_.unlock();
    } else {
      mu_.unlock_shared();
    }
  }

 private:
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
  std::map<std::string, std::shared_ptr<PersistentColumnFamily>>
      column_families_;
  mutable std::shared_mutex mu_;
};

/// Objects of this class represent Bigtable tables.
class Table : public std::enable_shared_from_this<Table> {
 public:
  static StatusOr<std::shared_ptr<Table>> Create(
      google::bigtable::admin::v2::Table schema, bool should_persist,
      std::string const& data_root = "/tmp/");

  static StatusOr<std::shared_ptr<Table>> Load(
      std::string const& table_name, std::string const& data_root = "/tmp/");

  std::shared_ptr<Table> get() { return shared_from_this(); }

  google::bigtable::admin::v2::Table GetSchema() const;

  Status Update(google::bigtable::admin::v2::Table const& new_schema,
                google::protobuf::FieldMask const& to_update);

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request);

  bool IsDeleteProtected() const;

  StatusOr<google::bigtable::v2::CheckAndMutateRowResponse> CheckAndMutateRow(
      google::bigtable::v2::CheckAndMutateRowRequest const& request);

  Status MutateRow(google::bigtable::v2::MutateRowRequest const& request);

  Status DoMutationsWithPossibleRollbackLocked(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations) {
    auto lock_scope = utilities_->LockScope(false);
    return DoMutationsWithPossibleRollback(row_key, mutations);
  }

  Status ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                  RowStreamer& row_streamer) const;

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request);

  Status SampleRowKeys(
      double pass_probability,
      grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer)
      const;

  Status DropRowRange(
      ::google::bigtable::admin::v2::DropRowRangeRequest const& request);

  void MarkForDeletion() {
    auto scoped_lock = utilities_->LockScope(true);
    utilities_->Cleanup();
  }

  // For testing only
  std::shared_ptr<TableOperations> GetUtilities() { return utilities_; }

  ~Table() = default;

 private:
  friend class RowTransaction;
  friend class InMemoryRowTransaction;
  friend class PersistentRowTransaction;
  enum class OpenMode { kCreateNew, kOpenExisting };

  Status PrepareSchema();

  google::bigtable::admin::v2::Table schema_;

  Status Construct(google::bigtable::admin::v2::Table schema,
                   bool should_persist, std::string const& data_root,
                   OpenMode mode);

  bool IsDeleteProtectedNoLock() const;

  Status DoMutationsWithPossibleRollback(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations);

  std::shared_ptr<TableOperations> utilities_;
};

class RowTransaction {
 public:
  explicit RowTransaction(std::string const& row_key) : row_key_(row_key) {}
  virtual ~RowTransaction() = default;

  virtual Status commit() = 0;

  // timestamp_override, if provided, will be used instead of
  // set_cell.timestamp. The override is used to set the timestamp to
  // the server time in case a timestamp <= 0 is provided.
  virtual Status SetCell(
      ::google::bigtable::v2::Mutation_SetCell const& set_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override) = 0;
  virtual Status AddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override) = 0;
  ;
  Status MergeToCell(
      ::google::bigtable::v2::Mutation_MergeToCell const& merge_to_cell);
  virtual Status DeleteFromColumn(
      ::google::bigtable::v2::Mutation_DeleteFromColumn const&
          delete_from_column) = 0;
  virtual Status DeleteFromFamily(
      ::google::bigtable::v2::Mutation_DeleteFromFamily const&
          delete_from_family) = 0;
  virtual Status DeleteFromRow() = 0;

  virtual StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request) = 0;

  // row_key_ is initialized from the request proto, and therefore it
  // is safe to access it while the mutation request is ongoing. We
  // store a reference to it to avoid copying a potentially very large
  // (up to 4KB) value.
  std::string const& row_key_;
};

class InMemoryRowTransaction : public RowTransaction {
 public:
  explicit InMemoryRowTransaction(
      std::shared_ptr<InMemoryTableOperations> utilities,
      std::string const& row_key)
      : RowTransaction(row_key), utilities_(std::move(utilities)) {}

  ~InMemoryRowTransaction() override {
    if (!committed_) {
      Undo();
    }
  };

  Status commit() override {
    committed_ = true;
    return Status();
  }

  Status AddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override) override;

  // timestamp_override, if provided, will be used instead of
  // set_cell.timestamp. The override is used to set the timestamp to
  // the server time in case a timestamp <= 0 is provided.
  Status SetCell(
      ::google::bigtable::v2::Mutation_SetCell const& set_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override) override;
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

 private:
  void Undo();
  std::shared_ptr<InMemoryTableOperations> utilities_;
  bool committed_ = false;
  std::stack<absl::variant<DeleteValue, RestoreValue>> undo_;
};

class PersistentRowTransaction : public RowTransaction {
 public:
  explicit PersistentRowTransaction(
      std::shared_ptr<PersistentTableOperations> utilities,
      std::string const& row_key, rocksdb::TransactionDB* db)
      : RowTransaction(row_key), utilities_(std::move(utilities)) {
    txn_ = std::unique_ptr<rocksdb::Transaction>(
        db->BeginTransaction(rocksdb::WriteOptions()));
  }

  Status commit() override;
  Status AddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override) override;
  Status SetCell(
      ::google::bigtable::v2::Mutation_SetCell const& set_cell,
      absl::optional<std::chrono::milliseconds> timestamp_override) override;
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

 private:
  std::unique_ptr<rocksdb::Transaction> txn_;
  std::shared_ptr<PersistentTableOperations> utilities_;
};

class InMemoryTable : public Table {
 private:
  InMemoryTable() = default;
  friend class RowSetIterator;
};

/**
 * A `AbstractCellStreamImpl` which streams filtered contents of the table.
 *
 * Underneath is essentially a collection of `FilteredColumnFamilyStream`s.
 * All filters applied to `FilteredColumnFamilyStream` are propagated to the
 * underlying `FilteredColumnFamilyStream`, except for `FamilyNameRegex`, which
 * is handled by this subclass.
 *
 * This class is public only to enable testing.
 */
class FilteredInMemoryTableStream : public MergeCellStreams {
 public:
  explicit FilteredInMemoryTableStream(
      std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  bool ApplyFilter(InternalFilter const& internal_filter) override;

 private:
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams);
};

class FilteredPersistentTableStream : public MergeCellStreams {
 public:
  explicit FilteredPersistentTableStream(
      std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  bool ApplyFilter(InternalFilter const& internal_filter) override;

 private:
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H
