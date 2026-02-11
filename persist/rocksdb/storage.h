/**
 * @file rocksdb_storage.h
 * @brief RocksDB-backed storage implementation for the Bigtable emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_ROCKSDB_STORAGE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_ROCKSDB_STORAGE_H

#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "filter.h"
#include "persist/utils/logging.h"
#include "persist/metadata_view.h"
#include "persist/proto/storage.pb.h"
#include "persist/rocksdb/column_family_stream.h"
#include "persist/rocksdb/filtered_table_stream.h"
#include "persist/rocksdb/key_encoding.h"
#include "persist/rocksdb/storage_row_tx.h"
#include "persist/storage.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <algorithm>
#include <chrono>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
 * RocksDB-backed storage implementation for the Bigtable emulator.
 */
class RocksDBStorage : public Storage {
  friend class RocksDBStorageRowTX;
  friend class RocksDBColumnFamilyStream;

 private:
  /** Creates an iterator over the metadata column family (table keys). */
  inline rocksdb::Iterator* createTablesIterator() const {
    return db->NewIterator(roptions, metaHandle);
  }

 public:
  /** Column family handle type. */
  using CFHandle = rocksdb::ColumnFamilyHandle*;

  /** Constructs storage from a RocksDB config. */
  explicit RocksDBStorage(
      storage::StorageRocksDBConfig const& arg_storage_config) {
    storage_config = arg_storage_config;
  }

  /** Constructs storage using FLAGS_storage_path and default meta CF name. */
  explicit RocksDBStorage() {
    storage_config = storage::StorageRocksDBConfig();
    storage_config.set_db_path(absl::GetFlag(FLAGS_storage_path));
    storage_config.set_meta_column_family("bte_metadata");
  }

  /** Destroys the storage and closes the database. */
  virtual ~RocksDBStorage() { Close(); }

  /**
   * Parses a RocksDB key into (table_name, row_key, column_qualifier).
   */
  inline std::tuple<std::string, std::string, std::string> RawDataParseRowKey(
      rocksdb::Iterator* iter) const {
    return key_encoding_.ParseRowKey(iter->key().ToString());
  }

  /**
   * Seeks the iterator to the first key for (table_name, row_prefix).
   * RawData* methods implement lowest-level row storage; this seeks so we
   * iterate a superset of the prefix.
   */
  inline void RawDataSeekPrefixed(rocksdb::Iterator* iter,
                                  std::string const& table_name,
                                  std::string const& row_prefix) {
    iter->Seek(rocksdb::Slice(key_encoding_.StorageKeyPartial(table_name, row_prefix)));
  }

  /**
   * Puts serialized row data for (table, row, column) into RocksDB.
   */
  inline Status RawDataPut(rocksdb::Transaction* txn,
                           std::string const& table_name,
                           std::string const& column_family,
                           std::string const& row_key,
                           std::string const& column_qualifier,
                           std::string&& data) {
    auto const& cf = column_families_handles_map[column_family];
    return GetStatus(txn->Put(cf,
                              rocksdb::Slice(key_encoding_.StorageKey(table_name, row_key,
                                                        column_qualifier)),
                              rocksdb::Slice(std::move(data))),
                     "Update commit row");
  }

  /**
   * Gets serialized row data for (table, row, column) from RocksDB.
   */
  inline Status RawDataGet(rocksdb::Transaction* txn,
                           std::string const& table_name,
                           std::string const& column_family,
                           std::string const& row_key,
                           std::string const& column_qualifier,
                           std::string* out) {
    rocksdb::ReadOptions roptions;
    auto const& cf = column_families_handles_map[column_family];
    return GetStatus(txn->Get(roptions, cf,
                              rocksdb::Slice(key_encoding_.StorageKey(table_name, row_key,
                                                        column_qualifier)),
                              out),
                     "Load commit row", Status());
  }

  /**
   * Deletes the underlying data for the given row's column.
   */
  inline Status RawDataDeleteColumn(rocksdb::Transaction* txn,
                                    std::string const& table_name,
                                    std::string const& column_family,
                                    std::string const& row_key,
                                    std::string const& column_qualifier) {
    auto const& cf = column_families_handles_map[column_family];
    return GetStatus(
        txn->Delete(cf, rocksdb::Slice(
          key_encoding_.StorageKey(table_name, row_key, column_qualifier))),
        "Delete row column");
  }

  /**
   * Deletes all columns for the given row in the column family (may be more
   * expensive than RawDataDeleteColumn).
   */
  inline Status RawDataDelete(rocksdb::Transaction* txn,
                              std::string const& table_name,
                              std::string const& column_family,
                              std::string const& row_key) {
    auto const& cf = column_families_handles_map[column_family];
    // Need to iterate through the columns
    auto it = txn->GetIterator(roptions, cf);
    for (it->Seek(rocksdb::Slice(key_encoding_.StorageKeyPartial(table_name, row_key)));
         it->Valid(); it->Next()) {
      auto const& [k_table_name, k_row_key, _] = RawDataParseRowKey(it);
      if (k_table_name != table_name || k_row_key != row_key) {
        break;
      }
      auto status = GetStatus(txn->Delete(cf, it->key()), "Delete row");
      if (!status.ok()) {
        delete it;
        return status;
      }
    }
    delete it;
    return Status();
  }

  /** @copydoc Storage::RowTransaction */
  virtual StatusOr<std::unique_ptr<StorageRowTX>> RowTransaction(
      std::string const& table_name, std::string const& row_key) {
    return StatusOr<std::unique_ptr<StorageRowTX>>(std::unique_ptr<RocksDBStorageRowTX>(new RocksDBStorageRowTX(
        table_name, row_key, StartRocksTransaction(), this)));
  }

  /** @copydoc Storage::Close */
  virtual Status UncheckedClose() {
    if (db == nullptr) {
      return Status();
    }

    DBG("[RocksDBStorage][UncheckedClose] killing meta handle");
    for (auto& pair : column_families_handles_map) {
      delete pair.second;
    }
    column_families_handles_map.clear();

    rocksdb::WaitForCompactOptions opts;
    opts.close_db = true;
    DBG("[RocksDBStorage][UncheckedClose] waiting for compact");
    auto status = GetStatus(db->WaitForCompact(opts), "Wait for compact");
    if (!status.ok()) {
      DBG("[RocksDBStorage][UncheckedClose] error: {}", status.message());
      return status;
    }
    DBG("[RocksDBStorage][UncheckedClose] deleting DB");
    db = nullptr;
    DBG("[RocksDBStorage][UncheckedClose] deleted DB");
    metaHandle = nullptr;
    DBG("[RocksDBStorage][UncheckedClose] exit");
    return Status();
  }

  /** @copydoc Storage::Open */
  virtual Status UncheckedOpen(
      std::vector<std::string> additional_cf_names = {}) {
    DBG("[RocksDBStorage][UncheckedOpen] call additional_cf_names.size()={}",
        additional_cf_names.size());
    options = rocksdb::Options();
    txn_options = rocksdb::TransactionDBOptions();
    woptions = rocksdb::WriteOptions();
    roptions = rocksdb::ReadOptions();

    options.create_if_missing = true;

    std::vector<std::string> existing_cf_names;
    auto status = rocksdb::TransactionDB::ListColumnFamilies(
        options, storage_config.db_path(), &existing_cf_names);
    bool is_new_database = !status.ok();
    if (is_new_database) {
      DBG("[RocksDBStorage][UncheckedOpen] no existing DB path={}",
          storage_config.db_path());
      existing_cf_names.clear();
      // New database - start with just default CF
      existing_cf_names.push_back(rocksdb::kDefaultColumnFamilyName);
    }

    // Build set of all column families we need
    std::set<std::string> all_cf_set;
    for (auto const& cf_name : existing_cf_names) {
      all_cf_set.insert(cf_name);
    }

    // Add additional CFs from parameter
    for (auto const& cf_name : additional_cf_names) {
      all_cf_set.insert(cf_name);
    }

    // Always need meta CF
    bool need_meta_cf = all_cf_set.find(storage_config.meta_column_family()) ==
                        all_cf_set.end();
    if (need_meta_cf) {
      all_cf_set.insert(storage_config.meta_column_family());
    }

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    for (auto const& cf_name : all_cf_set) {
      column_families.push_back(rocksdb::ColumnFamilyDescriptor(
          cf_name, rocksdb::ColumnFamilyOptions()));
    }

    DBG("[RocksDBStorage][UncheckedOpen] first open path={} cf_count={}",
        storage_config.db_path(), column_families.size());
    std::vector<CFHandle> handles;
    auto open_status = OpenDBWithRetry(options, column_families, &handles);

    // If we're creating a new DB and need meta CF, we need to do a two-step
    // process
    if (is_new_database && need_meta_cf && !open_status.ok()) {
      std::vector<rocksdb::ColumnFamilyDescriptor> minimal_cfs;
      minimal_cfs.push_back(rocksdb::ColumnFamilyDescriptor(
          rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));

      DBG("[RocksDBStorage][UncheckedOpen] after failed init path={}",
          storage_config.db_path());
      open_status = OpenDBWithRetry(options, minimal_cfs, &handles);
      if (!open_status.ok()) {
        DBG("[RocksDBStorage][UncheckedOpen] failed to create new DB path={} "
            "error={}",
            storage_config.db_path(), open_status.message());
        return open_status;
      }

      // Step 2: Add meta column family
      CFHandle meta_cf_handle;
      auto create_cf_status = db->CreateColumnFamily(
          rocksdb::ColumnFamilyOptions(), storage_config.meta_column_family(),
          &meta_cf_handle);

      if (!create_cf_status.ok()) {
        DBG("[RocksDBStorage][UncheckedOpen] failed to create meta CF path={} "
            "error={}",
            storage_config.db_path(), create_cf_status.ToString());
        // Clean up
        LERROR(
            "[RocksDBStorage][UncheckedOpen] Failed to create meta column "
            "family path={} status={}",
            storage_config.db_path(), create_cf_status.ToString());
        for (auto h : handles) delete h;
        delete db;
        db = nullptr;
        return StorageError("Failed to create meta column family");
      }

      handles.push_back(meta_cf_handle);
    } else if (!open_status.ok()) {
      DBG("[RocksDBStorage][UncheckedOpen] failed path={} error={}",
          storage_config.db_path(), open_status.message());
      return open_status;
    }

    metaHandle = nullptr;
    for (auto const& handle : handles) {
      column_families_handles_map[handle->GetName()] = handle;
      if (handle->GetName() == storage_config.meta_column_family()) {
        metaHandle = handle;
      }
    }

    if (metaHandle == nullptr) {
      LERROR(
          "[RocksDBStorage][UncheckedOpen] Meta column family not found "
          "path={} meta_cf_name={}",
          storage_config.db_path(), storage_config.meta_column_family());
      return StorageError("Meta column family not found after opening");
    }

    return Status();
  }

  /** @copydoc Storage::CreateTable */
  virtual Status CreateTable(google::bigtable::admin::v2::Table& schema) {
    auto txn = StartRocksTransaction();

    auto key = rocksdb::Slice(schema.name());
    if (KeyExists(txn, metaHandle, key)) {
      LERROR("[RocksDBStorage][CreateTable] Table already exists table_name={}",
             schema.name());
      return AlreadyExistsError(
          "Table already exists.",
          GCP_ERROR_INFO().WithMetadata("table_name", schema.name()));
    }

    storage::TableMeta meta;
    *meta.mutable_table() = schema;

    std::vector<std::string> missing_cfs;
    for (auto& cf : meta.table().column_families()) {
      auto iter = column_families_handles_map.find(cf.first);
      if (iter == column_families_handles_map.end()) {
        missing_cfs.push_back(cf.first);
      }
    }

    if (!missing_cfs.empty()) {
      txn->Rollback();
      delete txn;

      // Close current database
      UncheckedClose();

      // Open regular RocksDB (not TransactionDB) to add column families
      rocksdb::DB* regular_db;
      std::vector<rocksdb::ColumnFamilyDescriptor> existing_cfs;
      std::vector<std::string> cf_names;
      auto list_status = rocksdb::DB::ListColumnFamilies(
          options, storage_config.db_path(), &cf_names);
      if (!list_status.ok()) {
        LERROR(
            "[RocksDBStorage][CreateTable] Failed to list CFs path={} "
            "status={}",
            storage_config.db_path(), list_status.ToString());
        return StorageError("Failed to list CFs during CF addition");
      }

      std::vector<CFHandle> cf_handles;
      for (auto const& name : cf_names) {
        existing_cfs.push_back(rocksdb::ColumnFamilyDescriptor(
            name, rocksdb::ColumnFamilyOptions()));
      }

      auto db_status =
          rocksdb::DB::Open(options, storage_config.db_path(), existing_cfs,
                            &cf_handles, &regular_db);
      if (!db_status.ok()) {
        LERROR(
            "[RocksDBStorage][CreateTable] Failed to open regular DB path={} "
            "status={}",
            storage_config.db_path(), db_status.ToString());
        return StorageError("Failed to open regular DB for CF addition: " +
                            db_status.ToString());
      }

      for (auto const& cf_name : missing_cfs) {
        CFHandle new_handle;
        auto create_status = regular_db->CreateColumnFamily(
            rocksdb::ColumnFamilyOptions(), cf_name, &new_handle);
        if (!create_status.ok()) {
          LERROR(
              "[RocksDBStorage][CreateTable] Failed to create column family "
              "path={} cf_name={} status={}",
              storage_config.db_path(), cf_name, create_status.ToString());
          for (auto h : cf_handles) delete h;
          delete regular_db;
          return StorageError("Failed to create column family " + cf_name +
                              ": " + create_status.ToString());
        }
        cf_handles.push_back(new_handle);
      }

      for (auto h : cf_handles) delete h;
      delete regular_db;

      DBG("[RocksDBStorage][CreateTable] reopen table_name={}",
          schema.name());
      auto reopen_status = UncheckedOpen();
      if (!reopen_status.ok()) {
        return reopen_status;
      }

      txn = StartRocksTransaction();

      missing_cfs.clear();
      for (auto& cf : meta.table().column_families()) {
        auto iter = column_families_handles_map.find(cf.first);
        if (iter == column_families_handles_map.end()) {
          missing_cfs.push_back(cf.first);
        }
      }
    }

    auto status = GetStatus(
        txn->Put(metaHandle, key, rocksdb::Slice(SerializeTableMeta(meta))),
        "Put table key to metadata cf");

    status = Commit(txn);
    if (!status.ok()) {
      return status;
    }

    // User needs to restart emulator with pre-created column families
    if (!missing_cfs.empty()) {
      std::string missing_list;
      for (auto const& cf : missing_cfs) {
        if (!missing_list.empty()) missing_list += ", ";
        missing_list += cf;
      }
      LERROR(
          "[RocksDBStorage][CreateTable] Column families do not exist "
          "table_name={} missing_cfs={}",
          schema.name(), missing_list);
      return InvalidArgumentError(
          "Column families do not exist. Please restart the emulator to create "
          "them.",
          GCP_ERROR_INFO()
              .WithMetadata("missing_column_families", missing_list)
              .WithMetadata("table_name", schema.name()));
    }

    // TODO: Implement garbage collection for old cell versions based on gc_rule
    return Status();
  }

  /** @copydoc Storage::DeleteTable */
  virtual Status DeleteTable(
      std::string table_name,
      std::function<Status(std::string, storage::TableMeta)>&&
          precondition_fn) {
    auto txn = StartRocksTransaction();

    std::string out;
    auto status = GetStatus(
        txn->Get(roptions, metaHandle, rocksdb::Slice(table_name), &out),
        "Get table",
        NotFoundError("No such table.",
                      GCP_ERROR_INFO().WithMetadata("table_name", table_name)));
    if (!status.ok()) {
      LERROR("[RocksDBStorage][GetTableMeta] Get table failed table_name={} "
             "status={}",
             table_name, status.message());
      return Rollback(txn, status);
    }
    auto const meta = DeserializeTableMeta(std::move(out));
    if (!meta.ok()) {
      LERROR(
          "[RocksDBStorage][DeleteTableMeta] DeserializeTableMeta failed "
          "table_name={} status={}",
          table_name, meta.status().message());
      return Rollback(txn, meta.status());
    }
    auto const precondition_status = precondition_fn(table_name, meta.value());
    if (!precondition_status.ok()) {
      LERROR(
          "[RocksDBStorage][DeleteTableMeta] precondition failed table_name={} "
          "status={}",
          table_name, precondition_status.message());
      return Rollback(txn, precondition_status);
    }

    auto const delete_status = GetStatus(
        txn->Delete(metaHandle, rocksdb::Slice(table_name)), "Delete table",
        NotFoundError("No such table.",
                      GCP_ERROR_INFO().WithMetadata("table_name", table_name)));

    if (!delete_status.ok()) {
      LERROR(
          "[RocksDBStorage][DeleteTableMeta] Delete table failed table_name={} "
          "status={}",
          table_name, delete_status.message());
      return Rollback(txn, delete_status);
    }
    return Commit(txn);
  }

  /** @copydoc Storage::HasTable */
  virtual bool HasTable(std::string table_name) const {
    std::string _;
    return db->KeyMayExist(roptions, metaHandle, rocksdb::Slice(table_name),
                           &_) &&
           db->Get(roptions, metaHandle, rocksdb::Slice(table_name), &_).ok();
  }

  /** @copydoc Storage::GetTable */
  virtual StatusOr<storage::TableMeta> GetTable(std::string table_name) const {
    std::string out;
    auto status = GetStatus(
        db->Get(roptions, metaHandle, rocksdb::Slice(table_name), &out),
        "Get table",
        NotFoundError("No such table.",
                      GCP_ERROR_INFO().WithMetadata("table_name", table_name)));
    if (!status.ok()) {
      LERROR("[RocksDBStorage][GetTable] No such table table_name={} status={}",
             table_name, status.message());
      return status;
    }
    return DeserializeTableMeta(std::move(out));
  }

  /** @copydoc Storage::UpdateTableMetadata */
  virtual Status UpdateTableMetadata(std::string table_name,
                                     storage::TableMeta const& meta) {
    auto txn = StartRocksTransaction();
    auto key = rocksdb::Slice(table_name);
    auto serialized = SerializeTableMeta(meta);

    auto status =
        GetStatus(txn->Put(metaHandle, key, rocksdb::Slice(serialized)),
                  "Update table metadata");

    if (!status.ok()) {
      txn->Rollback();
      delete txn;
      return status;
    }

    return Commit(txn);
  }

  /** @copydoc Storage::EnsureColumnFamiliesExist */
  virtual Status EnsureColumnFamiliesExist(
      std::vector<std::string> const& cf_names) {
    std::vector<std::string> missing_cfs;
    for (auto const& cf_name : cf_names) {
      auto iter = column_families_handles_map.find(cf_name);
      if (iter == column_families_handles_map.end()) {
        missing_cfs.push_back(cf_name);
      }
    }

    if (missing_cfs.empty()) {
      return Status();
    }

    for (auto& pair : column_families_handles_map) {
      delete pair.second;
    }
    column_families_handles_map.clear();
    delete db;
    db = nullptr;

    rocksdb::DB* regular_db;
    std::vector<rocksdb::ColumnFamilyDescriptor> existing_cfs;
    std::vector<std::string> existing_cf_names;
    auto list_status = rocksdb::DB::ListColumnFamilies(
        options, storage_config.db_path(), &existing_cf_names);
    if (!list_status.ok()) {
      LERROR(
          "[RocksDBStorage][EnsureColumnFamiliesExist] Failed to list CFs "
          "path={} status={}",
          storage_config.db_path(), list_status.ToString());
      return StorageError("Failed to list CFs during CF addition");
    }

    std::vector<CFHandle> cf_handles;
    for (auto const& name : existing_cf_names) {
      existing_cfs.push_back(rocksdb::ColumnFamilyDescriptor(
          name, rocksdb::ColumnFamilyOptions()));
    }

    auto db_status = rocksdb::DB::Open(options, storage_config.db_path(),
                                       existing_cfs, &cf_handles, &regular_db);
    if (!db_status.ok()) {
      LERROR(
          "[RocksDBStorage][EnsureColumnFamiliesExist] Failed to open DB "
          "path={} status={}",
          storage_config.db_path(), db_status.ToString());
      return StorageError("Failed to open regular DB for CF addition: " +
                          db_status.ToString());
    }

    for (auto const& cf_name : missing_cfs) {
      CFHandle new_handle;
      auto create_status = regular_db->CreateColumnFamily(
          rocksdb::ColumnFamilyOptions(), cf_name, &new_handle);
      if (!create_status.ok()) {
        LERROR(
            "[RocksDBStorage][EnsureColumnFamiliesExist] Failed to create CF "
            "path={} cf_name={} status={}",
            storage_config.db_path(), cf_name, create_status.ToString());
        for (auto h : cf_handles) delete h;
        delete regular_db;
        return StorageError("Failed to create column family " + cf_name + ": " +
                            create_status.ToString());
      }
      cf_handles.push_back(new_handle);
    }

    for (auto h : cf_handles) delete h;
    delete regular_db;

    DBG("[RocksDBStorage][CreateTable] open on ensure column families exist "
        "missing_cfs_count={}",
        missing_cfs.size());
    return UncheckedOpen();
  }

  template <bool with_prefix, typename T>
  class TablesMetadataView;

  /**
   * Forward iterator over table metadata (table name, TableMeta) in RocksDB
   * metadata CF.
   * @tparam with_prefix If true, only keys with the given prefix are iterated.
   */
  template <bool with_prefix>
  class TablesMetadataIterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::tuple<std::string, storage::TableMeta>;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type;
    using reference = value_type;

   private:
    template <bool p_with_prefix, typename T>
    friend class TablesMetadataView;

    /** Underlying RocksDB iterator. */
    rocksdb::Iterator* iter;
    /** Cached (key, TableMeta) for current position. */
    absl::optional<value_type> loaded_table_data;
    /** Error from last DeserializeTableMeta. */
    Status error;

    /** When with_prefix is true, the key prefix to filter by. */
    std::conditional_t<with_prefix == true, std::string, std::monostate> prefix;

    /** Returns true if the iterator is valid (and optionally matches prefix).
     */
    inline bool isValid() const {
      if constexpr (with_prefix) {
        return iter != nullptr && error.ok() && iter->Valid() &&
               iter->key().starts_with(prefix);
      } else {
        return iter != nullptr && error.ok() && iter->Valid();
      }
    }

    /** Loads (key, TableMeta) from current iterator position. */
    inline void loadTableData() {
      if (!isValid()) {
        return;
      }
      auto meta = DeserializeTableMeta(iter->value().ToString());
      if (!meta.ok()) {
        error = meta.status();
        return;
      }
      loaded_table_data = std::make_pair(iter->key().ToString(), meta.value());
    }

    template <bool B = with_prefix, typename std::enable_if<B, int>::type = 0>
    explicit TablesMetadataIterator(rocksdb::Iterator*&& rocks_iterator,
                                    std::string const& search_prefix)
        : iter(std::move(rocks_iterator)),
          loaded_table_data(absl::nullopt),
          prefix(search_prefix) {
      iter->Seek(rocksdb::Slice(prefix));
      loadTableData();
    };

    template <bool B = !with_prefix, typename std::enable_if<B, int>::type = 0>
    explicit TablesMetadataIterator(rocksdb::Iterator*&& rocks_iterator)
        : iter(std::move(rocks_iterator)), loaded_table_data(absl::nullopt) {
      iter->SeekToFirst();
      loadTableData();
    };

    explicit TablesMetadataIterator(std::monostate _) : iter(nullptr) {}

   public:
    /** Destroys the iterator and deletes the RocksDB iterator. */
    ~TablesMetadataIterator() {
      if (iter != nullptr) {
        delete iter;
      }
    }

    reference operator*() const { return loaded_table_data.value(); }
    pointer operator->() { return loaded_table_data.value(); }

    /** Prefix increment. */
    TablesMetadataIterator& operator++() {
      if (!isValid()) {
        return *this;
      }
      iter->Next();
      loadTableData();
      return *this;
    }

    /** Postfix increment. */
    TablesMetadataIterator operator++(int) {
      TablesMetadataIterator tmp = *this;
      ++(*this);
      return tmp;
    }

    friend bool operator==(TablesMetadataIterator const& a,
                           TablesMetadataIterator const& b) {
      return a.iter == b.iter || (!a.isValid() && !b.isValid());
    };
    friend bool operator!=(TablesMetadataIterator const& a,
                           TablesMetadataIterator const& b) {
      return a.iter != b.iter && (a.isValid() || b.isValid());
    };

    /** Returns error from last load. */
    Status status() const { return error; }
  };

  /**
   * Range/view over table metadata (optionally filtered by key prefix).
   * @tparam with_prefix If true, only tables with the given key prefix are
   * included.
   * @tparam T Storage type (e.g. RocksDBStorage) that provides
   * createTablesIterator().
   */
  template <bool with_prefix, typename T>
  class TablesMetadataView {
   private:
    /** When with_prefix, the key prefix. */
    std::conditional_t<with_prefix, std::string, std::monostate> const prefix;
    /** Pointer to the storage instance. */
    T const* storage;

    template <bool B = with_prefix, typename std::enable_if<B, int>::type = 0>
    explicit TablesMetadataView(T const* storage_ptr, std::string const prefix)
        : storage(storage_ptr), prefix(std::move(prefix)){};

    template <bool B = !with_prefix, typename std::enable_if<B, int>::type = 0>
    explicit TablesMetadataView(T const* storage_ptr) : storage(storage_ptr){};

    friend T;

   public:
    /** @cond */
    template <bool B = with_prefix, typename std::enable_if<B, int>::type = 0>
    inline TablesMetadataIterator<true> begin() const {
      return TablesMetadataIterator<true>(
          std::move(storage->createTablesIterator()), prefix);
    }

    template <bool B = !with_prefix, typename std::enable_if<B, int>::type = 0>
    inline TablesMetadataIterator<false> begin() const {
      return TablesMetadataIterator<false>(
          std::move(storage->createTablesIterator()));
    }
    /** @endcond */

    /** Returns past-the-end iterator. */
    inline TablesMetadataIterator<with_prefix> end() const {
      return TablesMetadataIterator<with_prefix>(std::monostate{});
    }
  };

  /** Returns a view of all table metadata. */
  virtual CachedTablesMetadataView Tables() const override {
    return CachedTablesMetadataView(
        TablesMetadataView<false, RocksDBStorage>(this));
  }

  /** Returns a view of table metadata with keys starting with prefix. */
  virtual CachedTablesMetadataView Tables(
      std::string const& prefix) const override {
    return CachedTablesMetadataView(
        TablesMetadataView<true, RocksDBStorage>(this, prefix));
  }

  /** @copydoc Storage::DeleteColumnFamily */
  virtual Status DeleteColumnFamily(std::string const& cf_name) override {
    auto it = column_families_handles_map.find(cf_name);
    if (it == column_families_handles_map.end()) {
      return Status();  // Already deleted or doesn't exist
    }
    auto* handle = it->second;

    // Drop from RocksDB (records drop in MANIFEST)
    auto status = db->DropColumnFamily(handle);
    if (!status.ok()) {
      return GetStatus(status, "DropColumnFamily");
    }

    // Destroy the handle to free memory
    status = db->DestroyColumnFamilyHandle(handle);
    if (!status.ok()) {
      return GetStatus(status, "DestroyColumnFamilyHandle");
    }

    // Remove from internal map so Destructor doesn't double-free
    column_families_handles_map.erase(it);

    return Status();
  }

  /**
   * Returns a cell stream over the table for the given row set.
   * @param table_name Table name.
   * @param range_set Row keys to include.
   * @param prefetch_all_columns If true, prefetch all columns per row.
   */
  virtual StatusOr<CellStream> StreamTable(
      std::string const& table_name, std::shared_ptr<StringRangeSet> range_set,
      bool prefetch_all_columns) override {
    std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> iters;
    auto table = GetTable(table_name);
    auto const& m = table.value().table().column_families();
    for (auto const& it : m) {
      auto const x = column_families_handles_map[it.first];
      if (x == nullptr) {
        continue;
      }
      std::unique_ptr<AbstractFamilyColumnStreamImpl> c =
          std::make_unique<RocksDBColumnFamilyStream>(
              it.first, x, range_set, this, table_name, prefetch_all_columns);
      iters.push_back(std::move(c));
    }
    return CellStream(
        std::make_unique<StorageFitleredTableStream>(std::move(iters)));
  }

 private:
  /** RocksDB configuration. */
  storage::StorageRocksDBConfig storage_config;

  /** Key encoding for storage keys (delimiter='|' by default, backslash for esacapes). */
  const KeyEncoding<'|', '\\', false> key_encoding_;

  /** RocksDB options. */
  rocksdb::Options options;
  /** Transaction DB options. */
  rocksdb::TransactionDBOptions txn_options;
  /** Write options. */
  rocksdb::WriteOptions woptions;
  /** Read options. */
  rocksdb::ReadOptions roptions;
  /** Transaction DB instance. */
  rocksdb::TransactionDB* db;
  /** Handle for the metadata column family. */
  CFHandle metaHandle = nullptr;
  /** Map from column family name to handle. */
  std::map<std::string, CFHandle> column_families_handles_map;

  /** Starts a new RocksDB transaction. */
  inline rocksdb::Transaction* StartRocksTransaction() const {
    return db->BeginTransaction(woptions);
  }

  /** Deserializes RowData from string. */
  inline StatusOr<storage::RowData> DeserializeRow(std::string&& data) const {
    storage::RowData row;
    if (!row.ParseFromString(data)) {
      LERROR("[RocksDBStorage][DeserializeRow] ParseFromString failed data_len={}",
             data.size());
      return StorageError("DeserializeRow()");
    }
    return row;
  }

  /** Serializes RowData to string. */
  inline std::string SerializeRow(storage::RowData row) const {
    std::string out;
    row.SerializeToString(&out);
    return out;
  }

  /** Deserializes TableMeta from string. */
  static inline StatusOr<storage::TableMeta> DeserializeTableMeta(
      std::string&& data) {
    storage::TableMeta meta;
    if (!meta.ParseFromString(data)) {
      LERROR("[RocksDBStorage][DeserializeTableMeta] ParseFromString failed "
             "data_len={}",
             data.size());
      return StorageError("DeserializeTableMeta()");
    }
    assert(meta.has_table());
    return meta;
  }

  /** Serializes TableMeta to string. */
  inline std::string SerializeTableMeta(storage::TableMeta meta) const {
    std::string out;
    meta.SerializeToString(&out);
    return out;
  }

  /** Returns true if the key exists in the column family. */
  inline bool KeyExists(rocksdb::Transaction* txn,
                        rocksdb::ColumnFamilyHandle* column_family,
                        rocksdb::Slice const& key) const {
    std::string _;
    return txn->Get(roptions, column_family, key, &_).ok();
  }

  /** Rolls back the transaction and returns the given status. */
  inline Status Rollback(rocksdb::Transaction* txn, Status status) const {
    auto const txn_status = txn->Rollback();
    assert(txn_status.ok());
    delete txn;
    if (!status.ok()) {
      return status;
    }
    return Status();
  }

  /** Commits the transaction. */
  inline Status Commit(rocksdb::Transaction* txn) const {
    auto const status = txn->Commit();
    assert(status.ok());
    delete txn;
    return Status();
  }

  /** Opens the DB with retries on IO error. */
  inline Status OpenDBWithRetry(
      rocksdb::Options options,
      std::vector<rocksdb::ColumnFamilyDescriptor> column_families,
      std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
    auto status = rocksdb::Status();
    for (auto i = 0; i < 5; ++i) {
      status = rocksdb::TransactionDB::Open(options, txn_options,
                                            storage_config.db_path(),
                                            column_families, handles, &db);
      if (!status.ok()) {
        LWARN("[OpenDBWithRetry] failed DB open with retry path={} code={}", storage_config.db_path(),
            (uint64_t)status.code());
      }
      if (status.IsIOError()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        continue;
      } else {
        break;
      }
    }
    return GetStatus(status, "Open database");
  }

  /** Builds a storage error status. */
  static inline Status StorageError(std::string&& operation) {
    LERROR("[RocksDBStorage][StorageError] operation={}", operation);
    return InternalError("Storage error",
                         GCP_ERROR_INFO().WithMetadata("operation", operation));
  }

  /** Converts RocksDB status; uses not_found_status when status is NotFound. */
  inline Status GetStatus(rocksdb::Status status, std::string&& operation,
                          Status&& not_found_status) const {
    if (status.IsNotFound()) {
      LERROR("[RocksDBStorage][GetStatus] not found operation={}", operation);
      return not_found_status;
    }
    if (!status.ok()) {
      return StorageError(std::move(operation));
    }
    return Status();
  }

  /** Converts RocksDB status to Storage Status. */
  inline Status GetStatus(rocksdb::Status status,
                          std::string&& operation) const {
    if (!status.ok()) {
      return StorageError(std::move(operation));
    }
    return Status();
  }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_ROCKSDB_STORAGE_H
