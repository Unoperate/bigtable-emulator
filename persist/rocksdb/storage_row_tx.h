/**
 * @file storage_row_tx.h
 * @brief RocksDB implementation of row-scoped storage transaction.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_STORAGE_ROW_TX_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_STORAGE_ROW_TX_H

#include "persist/proto/storage.pb.h"
#include "persist/storage_row_tx.h"
#include "rocksdb/utilities/transaction.h"
#include <map>
#include <string>
#include <tuple>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class RocksDBStorage;

/**
 * RocksDB implementation of a row-scoped storage transaction.
 *
 * Uses RocksDB transactions for atomicity. Row data is loaded lazily into
 * lazy_row_data_ when needed for reads or writes; mutations are applied
 * within the RocksDB transaction scope and flushed on Commit().
 */
class RocksDBStorageRowTX : public StorageRowTX {
  friend class RocksDBStorage;

 public:
  /** Destroys the transaction (rolls back if not committed). */
  virtual ~RocksDBStorageRowTX();

  /** Commits the transaction (writes lazy row data to RocksDB). */
  virtual Status Commit() override;

  /** Rolls back the transaction (abandons RocksDB transaction). */
  virtual Status Rollback(Status status) override;

  /** @copydoc StorageRowTX::SetCell */
  virtual Status SetCell(std::string const& column_family,
                         std::string const& column_qualifier,
                         std::chrono::milliseconds timestamp,
                         std::string const& value) override;

  /** @copydoc StorageRowTX::UpdateCell */
  virtual StatusOr<absl::optional<std::string>> UpdateCell(
      std::string const& column_family, std::string const& column_qualifier,
      std::chrono::milliseconds timestamp, std::string& value,
      std::function<StatusOr<std::string>(
          std::string const&, std::string&&)> const& update_fn) override;

  /** @copydoc StorageRowTX::DeleteRowColumn */
  virtual Status DeleteRowColumn(
      std::string const& column_family, std::string const& column_qualifier,
      ::google::bigtable::v2::TimestampRange const& time_range) override;

  /** @copydoc StorageRowTX::DeleteRowFromAllColumnFamilies */
  virtual Status DeleteRowFromAllColumnFamilies() override;

  /** @copydoc StorageRowTX::DeleteRowFromColumnFamily */
  virtual Status DeleteRowFromColumnFamily(
      std::string const& column_family) override;

  /**
   * Loads row data for the given column into local transaction state.
   * @param column_family Column family name.
   * @param column_qualifier Column qualifier.
   * @return Status of the load operation.
   */
  Status LoadRow(std::string const& column_family,
                 std::string const& column_qualifier);

 private:
  rocksdb::Transaction* txn_;   /**< RocksDB transaction for this row. */
  rocksdb::ReadOptions roptions_; /**< Read options for snapshot/consistency. */
  std::string const row_key_;   /**< Row key for this transaction. */
  std::string const table_name_; /**< Table name for this transaction. */
  RocksDBStorage* db_;           /**< Backing RocksDB storage. */

  /** Key used to fetch row data: [column_family, column_qualifier]. */
  using row_data_key_t = std::tuple<std::string, std::string>;
  /**
   * Cache of row data keyed by (column_family, column_qualifier).
   * Loaded on demand when reading or writing; written back to RocksDB on
   * Commit(). Acts as a simple ORM over the transaction.
   */
  std::map<row_data_key_t, storage::RowData> lazy_row_data_;

  /**
   * Removes all cached row data entries for the given column family.
   * Used when deleting a row from a column family.
   * @param column_family Column family name to clear from the cache.
   */
  void lazyRowDataRemoveColumnFamily(std::string const& column_family);

  /**
   * Returns whether row data for the given column is already in the cache.
   * @param column_family Column family name.
   * @param column_qualifier Column qualifier.
   * @return true if lazy_row_data_ contains an entry for this column.
   */
  bool hasLazyRowData(std::string const& column_family,
                      std::string const& column_qualifier);

  /**
   * Returns a reference to cached row data for the column; loads from RocksDB
   * if not present. Used when reading or mutating cell data.
   * @param column_family Column family name.
   * @param column_qualifier Column qualifier.
   * @return Mutable reference to the RowData in lazy_row_data_.
   */
  storage::RowData& lazyRowDataRef(std::string const& column_family,
                                   std::string const& column_qualifier);

  /**
   * Removes row data for the column from the cache and deletes the column
   * from RocksDB within the transaction.
   * @param column_family Column family name.
   * @param column_qualifier Column qualifier.
   * @return Status of the delete operation.
   */
  Status lazyRowDataDelete(std::string const& column_family,
                           std::string const& column_qualifier);

  /**
   * Private constructor; use RocksDBStorage::RowTransaction() to create.
   * @param table_name Table name.
   * @param row_key Row key.
   * @param txn RocksDB transaction.
   * @param db Pointer to backing RocksDB storage.
   */
  explicit RocksDBStorageRowTX(std::string const table_name,
                               std::string const row_key,
                               rocksdb::Transaction* txn, RocksDBStorage* db);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_STORAGE_ROW_TX_H
