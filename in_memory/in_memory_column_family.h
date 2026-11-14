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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_IN_MEMORY_IN_MEMORY_COLUMN_FAMILY_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_IN_MEMORY_IN_MEMORY_COLUMN_FAMILY_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/types/optional.h"
#include "cell_view.h"
#include "column_family.h"
#include "filter.h"
#include "filtered_map.h"
#include "range_set.h"
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

struct Cell {
  std::chrono::microseconds timestamp;
  std::string value;
};

// ReadModifyWriteCellResult supports undo and return value
// construction for the ReadModifyWrite RPC.
//
// The timestamp and value written are always returned in timestamp
// and value and will be used to construct the Row returned by the
// RPC.
//
// If maybe_old_value has a value, then a timestamp was overwritten
// and the ReadModifyWriteCellResult will be used to create a
// RestoreValue for undo log. Otherwise, a new cell was added and the
// ReadmodifyWriteCellResult will be used to create a DeleteValue for
// the undo log.
struct ReadModifyWriteCellResult {
  std::chrono::microseconds timestamp;
  std::string value;
  absl::optional<std::string> maybe_old_value;
};

/**
 * Objects of this class hold contents of a specific column in a specific row.
 *
 * This is essentially a blessed map from timestamps to values.
 */
class ColumnRow {
 public:
  ColumnRow() = default;
  // Disable copying.
  ColumnRow(ColumnRow const&) = delete;
  ColumnRow& operator=(ColumnRow const&) = delete;

  StatusOr<ReadModifyWriteCellResult> ReadModifyWrite(std::int64_t inc_value);

  ReadModifyWriteCellResult ReadModifyWrite(std::string const& append_value);

  /**
   * Insert or update and existing cell at a given timestamp.
   *
   * @param timestamp the time stamp at which the value will be inserted or
   *     updated.
   * @param value the value to insert/update.
   *
   * @return no value if the timestamp had no value before, otherwise
   * the previous value of the timestamp.
   */
  absl::optional<std::string> SetCell(std::chrono::microseconds timestamp,
                                      std::string const& value);

  StatusOr<absl::optional<std::string>> UpdateCell(
      std::chrono::microseconds timestamp, std::string& value,
      std::function<StatusOr<std::string>(std::string const&,
                                          std::string&&)> const& update_fn);

  /**
   * Delete cells falling into a given timestamp range.
   *
   * @param time_range the timestamp range dictating which values to delete.
   * @return vector of deleted cells.
   */
  std::vector<Cell> DeleteTimeRange(
      ::google::bigtable::v2::TimestampRange const& time_range);

  /**
   * Delete a cell with the given timestamp.
   *
   * @param timestamp the std::chrono::microseconds timestamp of the
   *     cell to delete.
   *
   * @return Cell representing deleted cell, if there
   *     was a cell with that timestamp, otherwise absl::nullopt.
   */
  absl::optional<Cell> DeleteTimeStamp(std::chrono::microseconds timestamp);

  bool HasCells() const { return !cells_.empty(); }

  using const_iterator = std::map<std::chrono::microseconds, std::string,
                                  std::greater<>>::const_iterator;

  const_iterator begin() const { return cells_.begin(); }
  const_iterator end() const { return cells_.end(); }
  const_iterator lower_bound(std::chrono::microseconds timestamp) const {
    return cells_.lower_bound(timestamp);
  }
  const_iterator upper_bound(std::chrono::microseconds timestamp) const {
    return cells_.upper_bound(timestamp);
  }

  const_iterator find(std::chrono::microseconds const& timestamp) {
    return cells_.find(timestamp);
  }

  void erase(const_iterator timestamp_it) { cells_.erase(timestamp_it); }

 private:
  // Note the order - the iterator return the freshest cells first.
  std::map<std::chrono::microseconds, std::string, std::greater<>> cells_;
};

/**
 * Objects of this class hold contents of a specific row in a column family.
 *
 * The users of this class may access the columns for a given row via
 * references to `ColumnRow`.
 *
 * It is guaranteed that every returned `ColumnRow` contains at least one cell.
 */
class ColumnFamilyRow {
 public:
  StatusOr<ReadModifyWriteCellResult> ReadModifyWrite(
      std::string const& column_qualifier, std::int64_t inc_value) {
    return columns_[column_qualifier].ReadModifyWrite(inc_value);
  };

  ReadModifyWriteCellResult ReadModifyWrite(std::string const& column_qualifier,
                                            std::string const& append_value) {
    return columns_[column_qualifier].ReadModifyWrite(append_value);
  }

  /**
   * Insert or update and existing cell at a given column and timestamp.
   *
   * @param column_qualifier the column qualifier at which to update the value.
   * @param timestamp the time stamp at which the value will be inserted or
   *     updated.
   * @param value the value to insert/update.
   *
   * @return no value if the timestamp had no value before, otherwise
   * the previous value of the timestamp.
   *
   */
  absl::optional<std::string> SetCell(std::string const& column_qualifier,
                                      std::chrono::microseconds timestamp,
                                      std::string const& value);

  StatusOr<absl::optional<std::string>> UpdateCell(
      std::string const& column_qualifier, std::chrono::microseconds timestamp,
      std::string& value,
      std::function<StatusOr<std::string>(std::string const&,
                                          std::string&&)> const& update_fn);

  /**
   * Delete cells falling into a given timestamp range in one column.
   *
   * @param column_qualifier the column qualifier from which to delete the
   *     values.
   * @param time_range the timestamp range dictating which values to delete.
   * @return vector of deleted cells.
   */
  std::vector<Cell> DeleteColumn(
      std::string const& column_qualifier,
      ::google::bigtable::v2::TimestampRange const& time_range);
  /**
   * Delete a cell with the given timestamp from the column given by
   *     the given column qualifier.
   *
   * @param column_qualifier the column from which to delete the cell.
   *
   * @param timestamp the std::chrono::microseconds timestamp of the
   *     cell to delete.
   *
   * @return Cell representing deleted cell, if there was a cell with
   *     that timestamp in then given column,  otherwise absl::nullopt.
   */
  absl::optional<Cell> DeleteTimeStamp(std::string const& column_qualifier,
                                       std::chrono::microseconds timestamp);

  bool HasColumns() { return !columns_.empty(); }
  using const_iterator = std::map<std::string, ColumnRow>::const_iterator;
  const_iterator begin() const { return columns_.begin(); }
  const_iterator end() const { return columns_.end(); }
  const_iterator lower_bound(std::string const& column_qualifier) const {
    return columns_.lower_bound(column_qualifier);
  }
  const_iterator upper_bound(std::string const& column_qualifier) const {
    return columns_.upper_bound(column_qualifier);
  }

  std::map<std::string, ColumnRow>::iterator find(
      std::string const& column_qualifier) {
    return columns_.find(column_qualifier);
  }

  void erase(std::map<std::string, ColumnRow>::iterator column_it) {
    columns_.erase(column_it);
  }

 private:
  friend class InMemoryColumnFamily;

  std::map<std::string, ColumnRow> columns_;
};

class InMemoryColumnFamily : public ColumnFamily {
 public:
  InMemoryColumnFamily() = default;
  // ConstructAggregateColumnFamily can be used to return an aggregate
  // ColumnFamily that can support AddToCell or MergeToCell and
  // similar aggregate complex types. To construct an ordinary
  // ColumnFamily, use the default constructor ColumnFamily().
  static StatusOr<std::shared_ptr<InMemoryColumnFamily>>
  ConstructAggregateColumnFamily(google::bigtable::admin::v2::Type value_type);

  // Disable copying.
  InMemoryColumnFamily(InMemoryColumnFamily const&) = delete;
  InMemoryColumnFamily& operator=(InMemoryColumnFamily const&) = delete;

  using const_iterator = std::map<std::string, ColumnFamilyRow>::const_iterator;
  using iterator = std::map<std::string, ColumnFamilyRow>::iterator;

  StatusOr<ReadModifyWriteCellResult> ReadModifyWrite(
      std::string const& row_key, std::string const& column_qualifier,
      std::int64_t inc_value) {
    return rows_[row_key].ReadModifyWrite(column_qualifier, inc_value);
  };

  ReadModifyWriteCellResult ReadModifyWrite(std::string const& row_key,
                                            std::string const& column_qualifier,
                                            std::string const& append_value) {
    return rows_[row_key].ReadModifyWrite(column_qualifier, append_value);
  };

  /**
   * Insert or update and existing cell at a given row, column and timestamp.
   *
   * @param row_key the row key at which to update the value.
   * @param column_qualifier the column qualifier at which to update the value.
   * @param timestamp the time stamp at which the value will be inserted or
   *     updated.
   * @param value the value to insert/update.
   *
   * @return no value if the timestamp had no value before, otherwise
   *     the previous value of the timestamp.
   *
   */
  absl::optional<std::string> SetCell(std::string const& row_key,
                                      std::string const& column_qualifier,
                                      std::chrono::microseconds timestamp,
                                      std::string const& value);

  /**
   * UpdateCell is like SetCell except that, when a cell exists with
   * the same timestamp, an update function (that depends on the column
   * family type) is called to derive a new value from the new and
   * existing value, and that is the value that is written.
   *
   * Simple (non-aggregate) column families have a default update
   * function that just returns the new value.
   *
   */
  StatusOr<absl::optional<std::string>> UpdateCell(
      std::string const& row_key, std::string const& column_qualifier,
      std::chrono::microseconds timestamp, std::string& value);

  /**
   * Delete the whole row from this column family.
   *
   * @param row_key the row key to remove.
   * @return map from deleted column qualifiers to deleted cells.
   */
  std::map<std::string, std::vector<Cell>> DeleteRow(
      std::string const& row_key);
  /**
   * Delete cells from a row falling into a given timestamp range in one column.
   *
   * @param row_key the row key to remove the cells from (or the
   *     iterator to the row - row_it - in the 2nd overloaded form of the
   *     function).

   * @param column_qualifier the column qualifier from which to delete
   *     the values.
   *
   * @param time_range the timestamp range dictating which values to
   *     delete.
   * @return vector of deleted cells.
   */
  std::vector<Cell> DeleteColumn(
      std::string const& row_key, std::string const& column_qualifier,
      ::google::bigtable::v2::TimestampRange const& time_range);

  std::vector<Cell> DeleteColumn(
      std::map<std::string, ColumnFamilyRow>::iterator row_it,
      std::string const& column_qualifier,
      ::google::bigtable::v2::TimestampRange const& time_range);

  /**
   * Delete a cell with the given timestamp from the column given by
   *     the given column qualifier from the row given by row_key.
   *
   * @param row_key the row from which to delete the cell
   *
   * @param column_qualifier the column from which to delete the cell.
   *
   * @param timestamp the std::chrono::microseconds timestamp of the
   *     cell to delete.
   *
   * @return Cell representing deleted cell, if there was a cell with
   *     that timestamp in then given column in the given row,
   *     otherwise absl::nullopt.
   */
  absl::optional<Cell> DeleteTimeStamp(std::string const& row_key,
                                       std::string const& column_qualifier,
                                       std::chrono::microseconds timestamp);

  const_iterator begin() const { return rows_.begin(); }
  iterator begin() { return rows_.begin(); }
  const_iterator end() const { return rows_.end(); }
  iterator end() { return rows_.end(); }
  const_iterator lower_bound(std::string const& row_key) const {
    return rows_.lower_bound(row_key);
  }
  iterator lower_bound(std::string const& row_key) {
    return rows_.lower_bound(row_key);
  }
  const_iterator upper_bound(std::string const& row_key) const {
    return rows_.upper_bound(row_key);
  }
  iterator upper_bound(std::string const& row_key) {
    return rows_.upper_bound(row_key);
  }

  std::size_t size() { return rows_.size(); }

  std::map<std::string, ColumnFamilyRow>::iterator find(
      std::string const& row_key) {
    return rows_.find(row_key);
  }

  iterator erase(std::map<std::string, ColumnFamilyRow>::iterator row_it) {
    return rows_.erase(row_it);
  }

  void RemoveAllDataFromColumnFamily() { rows_.clear(); };

  std::unique_ptr<FilteredColumnFamilyStream> GetFilteredColumnFamilyStream(
      std::shared_ptr<StringRangeSet const> row_set,
      std::string column_family_name);

  bool RowKeyExists(std::string const& row_key) override {
    return this->find(row_key) != this->end();
  }

 private:
  std::map<std::string, ColumnFamilyRow> rows_;
};

class FilteredInMemoryColumnFamilyStream : public FilteredColumnFamilyStream {
 public:
  /**
   * Construct a new object.
   *
   * @column_family the family to iterate over. It should not change over this
   *     objects lifetime.
   * @column_family_name the name of this column family. It will be used to
   *     populate the returned `CellView`s.
   * @row_set the row set indicating which row keys include in the returned
   *     values.
   */
  FilteredInMemoryColumnFamilyStream(
      InMemoryColumnFamily const& column_family, std::string column_family_name,
      std::shared_ptr<StringRangeSet const> row_set);
  bool HasValue() const override;
  CellView const& Value() const override;
  bool Next(NextMode mode) override;

 private:
  void InitializeIfNeeded() const;
  /**
   * Adjust the internal iterators after `column_it_` advanced.
   *
   * We need to make sure that either we reach the end of the column family or:
   * * `column_it_` doesn't point to `end()`
   * * `cell_it` points to a cell in the column family pointed to by
   *     `column_it_`
   *
   * @return whether we've managed to find another cell in currently pointed
   *     row.
   */
  bool PointToFirstCellAfterColumnChange() const;
  /**
   * Adjust the internal iterators after `row_it_` advanced.
   *
   * Similarly to `PointToFirstCellAfterColumnChange()` it ensures that all
   * internal iterators are valid (or we've reached `end()`).
   *
   * @return whether we've managed to find another cell
   */
  bool PointToFirstCellAfterRowChange() const;

  RegexFiteredMapView<StringRangeFilteredMapView<InMemoryColumnFamily>> rows_;
  mutable absl::optional<
      RegexFiteredMapView<StringRangeFilteredMapView<ColumnFamilyRow>>>
      columns_;
  mutable absl::optional<TimestampRangeFilteredMapView<ColumnRow>> cells_;

  // If row_it_ == rows_.end() we've reached the end.
  // We maintain the following invariant:
  //   if (row_it_ != rows_.end()) then
  //   cell_it_ != cells.end() && column_it_ != columns_.end().
  mutable absl::optional<RegexFiteredMapView<
      StringRangeFilteredMapView<InMemoryColumnFamily>>::const_iterator>
      row_it_;
  mutable absl::optional<RegexFiteredMapView<
      StringRangeFilteredMapView<ColumnFamilyRow>>::const_iterator>
      column_it_;
  mutable absl::optional<
      TimestampRangeFilteredMapView<ColumnRow>::const_iterator>
      cell_it_;
  mutable absl::optional<CellView> cur_value_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_IN_MEMORY_IN_MEMORY_COLUMN_FAMILY_H
