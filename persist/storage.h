/**
 * @file storage.h
 * @brief Abstract storage interface and helpers for the Bigtable emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/strings/str_cat.h"
#include "filter.h"
#include "persist/utils/logging.h"
#include "persist/metadata_view.h"
#include "persist/proto/storage.pb.h"
#include "persist/storage_row_tx.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

ABSL_DECLARE_FLAG(std::string, storage_path);

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
 * Ensures a cell value is set in the row data.
 * @param row The row data to update.
 * @param column_qualifier The column qualifier for the cell.
 * @param timestamp The cell timestamp.
 * @param value The cell value.
 * @todo Maybe rename and move to a better place.
 */
static inline void RowDataEnsure(storage::RowData& row,
                                 std::string const& column_qualifier,
                                 std::chrono::milliseconds const& timestamp,
                                 std::string const& value) {
  DBG("[RowDataEnsure] setting cell timestamp {} (column_qualifier={})",
      timestamp.count(), column_qualifier);
  (*(*row.mutable_column()).mutable_cells())[timestamp.count()] = value;
}

/**
 * Creates a dummy table schema proto with the given name.
 * @param table_name The name for the table in the schema.
 * @return A Table proto with only the name set.
 */
static inline google::bigtable::admin::v2::Table FakeSchema(
    std::string const& table_name) {
  google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  return schema;
}

/**
 * Abstract storage interface for the Bigtable emulator.
 */
class Storage {
 private:
  bool is_open_ = false;

 protected:
  /** Concrete implementation should implement this to setup the storage engine.
   * This function is guaranteed not to be double-invoked i.e it's eiter first
   * call to this object or Close() was called before. The only argument is the
   * list of extra column families that need to be loaded. */
  virtual Status UncheckedOpen(
      std::vector<std::string> additional_cf_names) = 0;
  /** Concreate implementation should implement this to tear down the storage
   * engine. Offers similar guarantee to UncheckedOpen(), you don't have to
   * check if the double-close occurred in your implementation. */
  virtual Status UncheckedClose() = 0;

 public:
  /** Default constructor. */
  Storage() {}
  /** Destroys the storage. */
  virtual ~Storage() = default;

  /** Starts a row-scoped transaction. */
  virtual StatusOr<std::unique_ptr<StorageRowTX>> RowTransaction(
      std::string const& table_name, std::string const& row_key) = 0;

  /** Closes the storage and releases resources. */
  virtual Status Close() {
    if (!is_open_) {
      return Status();
    }
    is_open_ = false;
    return this->UncheckedClose();
  };

  /** Opens the storage; optionally creates additional column families. */
  virtual Status Open(std::vector<std::string> additional_cf_names = {}) {
    if (is_open_) {
      return Status();
    }
    is_open_ = true;
    return this->UncheckedOpen(additional_cf_names);
  };

  /** Creates a table with the given schema. */
  virtual Status CreateTable(google::bigtable::admin::v2::Table& schema) = 0;

  /** Deletes a table after running the precondition. */
  virtual Status DeleteTable(
      std::string table_name,
      std::function<Status(std::string, storage::TableMeta)>&&
          precondition_fn) = 0;
  /** Returns table metadata. */
  virtual StatusOr<storage::TableMeta> GetTable(
      std::string table_name) const = 0;
  /** Updates table metadata (e.g. after CreateTable()). */
  virtual Status UpdateTableMetadata(std::string table_name,
                                     storage::TableMeta const& meta) = 0;
  /** Ensures the given column families exist. */
  virtual Status EnsureColumnFamiliesExist(
      std::vector<std::string> const& cf_names) = 0;
  /** Returns true if the table exists. */
  virtual bool HasTable(std::string table_name) const = 0;
  /** Deletes a column family (may be expensive depending on implementation). */
  virtual Status DeleteColumnFamily(std::string const& cf_name) = 0;

  /**
  *  Returns a view of all table metadata.
  *  Guarantees:
  *   - Random const access iterator (specifically std continous range)
  *   - Provides snapshot of tables when call to Tables() happened, even if mutations happen in meantime
  */
  virtual CachedTablesMetadataView Tables() const { return Tables(""); }

  /** Returns a view of table metadata with keys starting with prefix. 
  *  Guarantees:
  *   - Random const access iterator (specifically std continous range)
  *   - Provides snapshot of tables when call to Tables() happened, even if mutations happen in meantime
  */
  virtual CachedTablesMetadataView Tables(std::string const& prefix) const = 0;

  /** Streams all rows */
  virtual StatusOr<CellStream> StreamTableFull(std::string const& table_name) {
    return this->StreamTable(table_name, false);
  }

  /** Streams all rows  with optional prefetch settings */
  virtual StatusOr<CellStream> StreamTable(std::string const& table_name,
                                           bool prefetch_all_columns) {
    auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
    return this->StreamTable(table_name, all_rows_set, prefetch_all_columns);
  }

  /**
   * Returns a cell stream over the table for the given row set.
   * Note: prefetch_all_columns should fetch all columns for row in bulk.
   *       This can be useful if your column sizes are small.
   *       Usually false by default.
   * Note: Specific implementation doesn't have to support prefetch_all_columns
   *
   * @param table_name Table name.
   * @param range_set Row keys to include.
   * @param prefetch_all_columns If true, prefetch all columns per row.
   */
  virtual StatusOr<CellStream> StreamTable(
      std::string const& table_name, std::shared_ptr<StringRangeSet> range_set,
      bool prefetch_all_columns) = 0;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_H
