/**
 * @file storage_row_tx.h
 * @brief Abstract row transaction interface for the Bigtable emulator storage.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_ROW_TX_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_ROW_TX_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/types/optional.h"
#include <google/bigtable/v2/data.pb.h>
#include <chrono>
#include <functional>
#include <memory>
#include <string>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
 * Represents a storage row transaction.
 * Transactions in Bigtable are row-scoped and atomic; most operations here
 * refer to row operations.
 */
class StorageRowTX {
 public:
  /** Destroys the transaction. */
  virtual ~StorageRowTX() = default;
  /** Commits the transaction. */
  virtual Status Commit() = 0;
  /** Rolls back the transaction with the given status. */
  virtual Status Rollback(Status s) = 0;

  /**
   * Sets a specific cell value.
   * @param column_family The column family name.
   * @param column_qualifier The column qualifier.
   * @param timestamp The cell timestamp.
   * @param value The cell value.
   */
  virtual Status SetCell(std::string const& column_family,
                         std::string const& column_qualifier,
                         std::chrono::milliseconds timestamp,
                         std::string const& value) {
    return Status();
  }

  /**
   * Updates a specific cell with the given functor and returns the old value.
   * @param column_family The column family name.
   * @param column_qualifier The column qualifier.
   * @param timestamp The cell timestamp.
   * @param value The value passed to the update functor.
   * @param update_fn Functor that takes (column_qualifier, value) and returns
   * new value or error.
   */
  virtual StatusOr<absl::optional<std::string>> UpdateCell(
      std::string const& column_family, std::string const& column_qualifier,
      std::chrono::milliseconds timestamp, std::string& value,
      std::function<StatusOr<std::string>(std::string const&,
                                          std::string&&)> const& update_fn) {
    return Status();
  }

  /**
   * Deletes cells in the given time range (inclusive start, exclusive end:
   * start <= timestamp < end).
   * @param column_family The column family name.
   * @param column_qualifier The column qualifier.
   * @param time_range The timestamp range to delete.
   */
  virtual Status DeleteRowColumn(
      std::string const& column_family, std::string const& column_qualifier,
      ::google::bigtable::v2::TimestampRange const& time_range) {
    return Status();
  }

  /**
   * Deletes cells in the given time range (inclusive start, exclusive end).
   * @param column_family The column family name.
   * @param column_qualifier The column qualifier.
   * @param start Start of range (inclusive).
   * @param end End of range (exclusive).
   */
  virtual Status DeleteRowColumn(std::string const& column_family,
                         std::string const& column_qualifier,
                         std::chrono::milliseconds& start,
                         std::chrono::milliseconds& end) {
    ::google::bigtable::v2::TimestampRange t_range;
    t_range.set_start_timestamp_micros(
        std::chrono::duration_cast<std::chrono::microseconds>(start).count());
    t_range.set_end_timestamp_micros(
        std::chrono::duration_cast<std::chrono::microseconds>(end).count());
    return this->DeleteRowColumn(column_family, column_qualifier, t_range);
  }

  /**
   * Deletes the cell at the exact timestamp (removes values where start <=
   * timestamp < start+1ms).
   * @param column_family The column family name.
   * @param column_qualifier The column qualifier.
   * @param value The timestamp of the cell to delete.
   */
  virtual Status DeleteRowColumn(std::string const& column_family,
                         std::string const& column_qualifier,
                         std::chrono::milliseconds& value) {
    std::chrono::milliseconds value_end = value;
    ++value_end;
    return this->DeleteRowColumn(column_family, column_qualifier, value,
                                 value_end);
  }

  /**
   * Deletes the row from the given column family.
   * @param column_family The column family name.
   */
  virtual Status DeleteRowFromColumnFamily(std::string const& column_family) {
    return Status();
  }

  /**
   * Deletes the row from all column families (potentially expensive).
   */
  virtual Status DeleteRowFromAllColumnFamilies() { return Status(); }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_STORAGE_ROW_TX_H
