/**
 * @file storage_row_tx.h
 * @brief In-memory implementation of row-scoped storage transaction.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_ROW_TX_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_ROW_TX_H

#include "persist/proto/storage.pb.h"
#include "persist/storage_row_tx.h"
#include "table.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class MemoryStorage;

/**
 * In-memory implementation of a row-scoped storage transaction.
 *
 * Uses an undo stack to support rollback; all mutations are applied to
 * the in-memory Table and reverted on rollback or destructor.
 */
class MemoryStorageRowTX : public StorageRowTX {
  friend class MemoryStorage;

 public:
  /** Destroys the transaction (rolls back if not committed). */
  virtual ~MemoryStorageRowTX();

  /** Commits the transaction (marks as committed; no further rollback). */
  virtual Status Commit() override;

  /** Rolls back the transaction with the given status (replays undo stack). */
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

 private:
  std::string const row_key_;    /**< Row key for this transaction. */
  std::string const table_name_; /**< Table name for this transaction. */
  MemoryStorage* db_;            /**< Backing in-memory storage. */
  std::shared_ptr<Table> table_; /**< Table instance being mutated. */

  bool committed_; /**< True after Commit(); used to skip undo on destruct. */
  std::stack<absl::variant<DeleteValue, RestoreValue>>
      undo_; /**< Undo log for rollback. */

  /** Replays the undo stack to restore state (used on rollback). */
  void Undo();

  /**
   * Private constructor; use MemoryStorage::RowTransaction() to create.
   * @param table_name Table name.
   * @param row_key Row key.
   * @param db Pointer to backing MemoryStorage.
   * @param table Shared pointer to the Table instance.
   */
  explicit MemoryStorageRowTX(std::string const table_name,
                              std::string const row_key, MemoryStorage* db,
                              std::shared_ptr<Table> table);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_ROW_TX_H
