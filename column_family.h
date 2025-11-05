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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_COLUMN_FAMILY_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_COLUMN_FAMILY_H

#include "google/cloud/internal/big_endian.h"
#include "google/cloud/status_or.h"
#include "absl/types/optional.h"
#include "bigtable_limits.h"
#include "cell_view.h"
#include "filter.h"
#include "filtered_map.h"
#include "range_set.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/stubs/mutex.h>
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

// Many of the GC-related functions are recursive. Below we prove
// this approach's safety.
//
// Note that since a column family GCRule configuration must serialize
// to at most kMaxGCRuleSize (500) bytes and (in the case of a GCRule
// containing only a small max_num_versions) the minimum size of a GCRule
// is >= 2 bytes, a GCRule within size limits can embed at most 250 GCRules,
// which is also the maximum depth of recursion for this function.
//
// So we can expect that the maximum size of the stack used in
// recursion will be < 250KB, assuming each recursive call takes up
// less than 1KB of stack size (at most 500B for the rule and well
// less than 500B for the rest of the automatic variables -- which are
// all integers or pointers and would need to be > 60 in number for
// any call to exceed 500B).
//
// Therefore, since we enforce the size limit for a column family
// GCRule configuration before we store or modify it, it is safe to
// use recursion on the validated GCRules (MacOS X has the lowest default
// stack size of 512KiB).
static_assert(kMaxGCRuleSize == 500,
              "Max GC rule size changed. Recheck the logic of proof above.");

/**
 * Validates a GcRule before further processing.
 *
 * This function MUST be called on every GcRule object, there are multiple
 * places where it is assumed by the code. Objects that fail the validation
 * MUST not be passed as arguments to GC-related functions.
 */
Status CheckGCRuleIsValid(google::bigtable::admin::v2::GcRule const& rule);

struct Cell {
  std::chrono::milliseconds timestamp;
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
  std::chrono::milliseconds timestamp;
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
  absl::optional<std::string> SetCell(std::chrono::milliseconds timestamp,
                                      std::string const& value);

  StatusOr<absl::optional<std::string>> UpdateCell(
      std::chrono::milliseconds timestamp, std::string& value,
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
   * @param timestamp the std::chrono::milliseconds timestamp of the
   *     cell to delete.
   *
   * @return Cell representing deleted cell, if there
   *     was a cell with that timestamp, otherwise absl::nullopt.
   */
  absl::optional<Cell> DeleteTimeStamp(std::chrono::milliseconds timestamp);

  bool HasCells() const { return !cells_.empty(); }
  std::size_t size() const { return cells_.size(); }

  using const_iterator = std::map<std::chrono::milliseconds, std::string,
                                  std::greater<>>::const_iterator;
  using iterator = std::map<std::chrono::milliseconds, std::string,
                            std::greater<>>::iterator;

  const_iterator begin() const { return cells_.begin(); }
  const_iterator end() const { return cells_.end(); }
  const_iterator lower_bound(std::chrono::milliseconds timestamp) const {
    return cells_.lower_bound(timestamp);
  }
  const_iterator upper_bound(std::chrono::milliseconds timestamp) const {
    return cells_.upper_bound(timestamp);
  }

  const_iterator find(std::chrono::milliseconds const& timestamp) {
    return cells_.find(timestamp);
  }

  iterator erase(const_iterator timestamp_it) {
    return cells_.erase(timestamp_it);
  }

  /**
   * Runs garbage collection as defined by the passed GC rule.
   *
   * @param gc_rule The definition of garbage collection to be performed.
   * Note that it is assumed to be valid. That assumption is guarded using
   * assertions in debug build, but there are no guardrails in the release
   * build.
   */
  void RunGC(google::bigtable::admin::v2::GcRule const& gc_rule);

 private:
  // Note the order - the iterator return the freshest cells first.
  std::map<std::chrono::milliseconds, std::string, std::greater<>> cells_;

  // GCRuleEraseVerdict returns true if the cell pointed to by the
  // iterator `it` should be erased according to the GcRule `rule`.
  // Otherwise, it returns false.
  bool GCRuleEraseVerdict(google::bigtable::admin::v2::GcRule const& rule,
                          std::map<std::chrono::milliseconds, std::string,
                                   std::greater<>>::const_iterator it,
                          int32_t version_rank);
  // The following methods implement support for column family level
  // garbage collection.
  void ApplyGCRuleMaxNumVersions(std::size_t n);
  void ApplyGCRuleMaxAge(protobuf::Duration const& max_age);
  void ApplyGCRuleVerdict(google::bigtable::admin::v2::GcRule const& gc_rule);
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
                                      std::chrono::milliseconds timestamp,
                                      std::string const& value);

  StatusOr<absl::optional<std::string>> UpdateCell(
      std::string const& column_qualifier, std::chrono::milliseconds timestamp,
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
   * @param timestamp the std::chrono::milliseconds timestamp of the
   *     cell to delete.
   *
   * @return Cell representing deleted cell, if there was a cell with
   *     that timestamp in then given column,  otherwise absl::nullopt.
   */
  absl::optional<Cell> DeleteTimeStamp(std::string const& column_qualifier,
                                       std::chrono::milliseconds timestamp);

  bool HasColumns() { return !columns_.empty(); }
  using const_iterator = std::map<std::string, ColumnRow>::const_iterator;
  using iterator = std::map<std::string, ColumnRow>::iterator;
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

  iterator erase(std::map<std::string, ColumnRow>::iterator column_it) {
    return columns_.erase(column_it);
  }

  void RunGC(google::bigtable::admin::v2::GcRule const& gc_rule);

 private:
  friend class ColumnFamily;

  std::map<std::string, ColumnRow> columns_;
};

/**
 * Objects of this class hold contents of a column family indexed by rows.
 *
 * The users of this class may access individual rows via references to
 * `ColumnFamilyRow`.
 *
 * It is guaranteed that every returned `ColumnFamilyRow` contains at least one
 * `ColumnRow`.
 */
class ColumnFamily {
 public:
  ColumnFamily() = default;
  // ConstructColumnFamily can be used to return a ColumnFamily with
  // non-zero/non-default values for the GC policy and/or the Value
  // Type (the latter for aggregate column families which support
  // AddToCell and the like).  To construct an ordinary ColumnFamily
  // without GC or support for complex aggregation, use the default
  // constructor ColumnFamily() or call ConstructColumnfamily()
  // without any options.
  static StatusOr<std::shared_ptr<ColumnFamily>> ConstructColumnFamily(
      absl::optional<google::bigtable::admin::v2::Type> maybe_value_type =
          absl::nullopt,
      absl::optional<google::bigtable::admin::v2::GcRule> maybe_gc_rule =
          absl::nullopt);

  // Disable copying.
  ColumnFamily(ColumnFamily const&) = delete;
  ColumnFamily& operator=(ColumnFamily const&) = delete;

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
                                      std::chrono::milliseconds timestamp,
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
      std::chrono::milliseconds timestamp, std::string& value);

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
   * @param timestamp the std::chrono::milliseconds timestamp of the
   *     cell to delete.
   *
   * @return Cell representing deleted cell, if there was a cell with
   *     that timestamp in then given column in the given row,
   *     otherwise absl::nullopt.
   */
  absl::optional<Cell> DeleteTimeStamp(std::string const& row_key,
                                       std::string const& column_qualifier,
                                       std::chrono::milliseconds timestamp);

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

  void clear() { rows_.clear(); }
  absl::optional<google::bigtable::admin::v2::Type> GetValueType() {
    return value_type_;
  };

  void RunGC();

  void SetGCRule(google::bigtable::admin::v2::GcRule const& gc_rule) {
    gc_rule_ = gc_rule;
  }

 private:
  std::map<std::string, ColumnFamilyRow> rows_;

  // Support for aggregate and other complex types.
  absl::optional<google::bigtable::admin::v2::Type> value_type_ = absl::nullopt;

  // Support for garbage collection (GcRule)
  absl::optional<google::bigtable::admin::v2::GcRule> gc_rule_ = absl::nullopt;

  static StatusOr<std::string> DefaultUpdateCell(
      std::string const& /*existing_value*/, std::string&& new_value) {
    return new_value;
  };

  static StatusOr<std::string> SumUpdateCellBEInt64(
      std::string const& existing_value, std::string&& new_value) {
    auto existing_value_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(existing_value);
    if (!existing_value_int) {
      return existing_value_int.status();
    }

    auto new_value_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(new_value);
    if (!new_value_int) {
      return new_value_int.status();
    }

    return google::cloud::internal::EncodeBigEndian(existing_value_int.value() +
                                                    new_value_int.value());
  };

  static StatusOr<std::string> MaxUpdateCellBEInt64(
      std::string const& existing_value, std::string&& new_value) {
    auto existing_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(existing_value);
    if (!existing_int) {
      return existing_int.status();
    }
    auto new_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(new_value);
    if (!new_int) {
      return new_int.status();
    }

    if (existing_int.value() > new_int.value()) {
      return existing_value;
    }

    return new_value;
  };

  static StatusOr<std::string> MinUpdateCellBEInt64(
      std::string const& existing_value, std::string&& new_value) {
    auto existing_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(existing_value);
    if (!existing_int) {
      return existing_int.status();
    }
    auto new_int =
        google::cloud::internal::DecodeBigEndian<std::int64_t>(new_value);
    if (!new_int) {
      return new_int.status();
    }

    if (existing_int.value() < new_int.value()) {
      return existing_value;
    }

    return new_value;
  };

  std::function<StatusOr<std::string>(std::string const&, std::string&&)>
      update_cell_ = DefaultUpdateCell;
};

/**
 * A stream of cells which allows for filtering unwanted ones.
 *
 * In absence of any filters, objects of this class stream the contents of a
 * whole `ColumnFamily` just like true `Bigtable` would.
 *
 * The users can apply the following filters:
 * * row sets - to only stream cells for relevant rows
 * * row regexes - ditto
 * * column ranges - to only stream cells with given column qualifiers
 * * column regexes - ditto
 * * timestamp ranges - to only stream cells with timestamps in given ranges
 *
 * Objects of this class are not thread safe. Their users need to ensure that
 * underlying `ColumnFamily` object tree doesn't change.
 */
class FilteredColumnFamilyStream : public AbstractCellStreamImpl {
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
  FilteredColumnFamilyStream(ColumnFamily const& column_family,
                             std::string column_family_name,
                             std::shared_ptr<StringRangeSet const> row_set);
  bool ApplyFilter(InternalFilter const& internal_filter) override;
  bool HasValue() const override;
  CellView const& Value() const override;
  bool Next(NextMode mode) override;
  std::string const& column_family_name() const { return column_family_name_; }

 private:
  class FilterApply;

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

  std::string column_family_name_;

  std::shared_ptr<StringRangeSet const> row_ranges_;
  std::vector<std::shared_ptr<re2::RE2 const>> row_regexes_;
  mutable StringRangeSet column_ranges_;
  std::vector<std::shared_ptr<re2::RE2 const>> column_regexes_;
  mutable TimestampRangeSet timestamp_ranges_;

  RegexFiteredMapView<StringRangeFilteredMapView<ColumnFamily>> rows_;
  mutable absl::optional<
      RegexFiteredMapView<StringRangeFilteredMapView<ColumnFamilyRow>>>
      columns_;
  mutable absl::optional<TimestampRangeFilteredMapView<ColumnRow>> cells_;

  // If row_it_ == rows_.end() we've reached the end.
  // We maintain the following invariant:
  //   if (row_it_ != rows_.end()) then
  //   cell_it_ != cells.end() && column_it_ != columns_.end().
  mutable absl::optional<RegexFiteredMapView<
      StringRangeFilteredMapView<ColumnFamily>>::const_iterator>
      row_it_;
  mutable absl::optional<RegexFiteredMapView<
      StringRangeFilteredMapView<ColumnFamilyRow>>::const_iterator>
      column_it_;
  mutable absl::optional<
      TimestampRangeFilteredMapView<ColumnRow>::const_iterator>
      cell_it_;
  mutable absl::optional<CellView> cur_value_;
  mutable bool initialized_{false};
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_COLUMN_FAMILY_H
