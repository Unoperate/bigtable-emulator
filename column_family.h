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
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/types/optional.h"
#include "cell_view.h"
#include "filter.h"
#include "range_set.h"
#include "re2/re2.h"
#include <google/bigtable/admin/v2/types.pb.h>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

inline std::chrono::microseconds NowTruncatedToMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
}

class FilteredColumnFamilyStream;

/// Fails unless `value_type` is an aggregate the emulator supports: Sum, Min
/// or Max.
Status ValidateValueType(google::bigtable::admin::v2::Type const& value_type);

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
  virtual ~ColumnFamily() = default;
  // ConstructAggregateColumnFamily can be used to return an aggregate
  // ColumnFamily that can support AddToCell or MergeToCell and
  // similar aggregate complex types. To construct an ordinary
  // ColumnFamily, use the default constructor ColumnFamily().
  // static StatusOr<std::shared_ptr<ColumnFamily>>
  // ConstructAggregateColumnFamily( google::bigtable::admin::v2::Type
  // value_type);

  virtual bool RowKeyExists(std::string const& row_key) = 0;

  absl::optional<google::bigtable::admin::v2::Type> GetValueType() {
    return value_type_;
  };

  std::function<StatusOr<std::string>(std::string const&, std::string&&)>
      update_cell_ = DefaultUpdateCell;

 protected:
  // Support for aggregate and other complex types.
  absl::optional<google::bigtable::admin::v2::Type> value_type_ = absl::nullopt;

  Status ChooseCellUpdateFunction(google::bigtable::admin::v2::Type value_type);

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
  bool ApplyFilter(InternalFilter const& internal_filter) final {
    assert(!initialized_);
    return absl::visit(FilterApply(*this), internal_filter);
  }
  std::string const& column_family_name() const { return column_family_name_; }

 protected:
  FilteredColumnFamilyStream(std::string column_family_name,
                             std::shared_ptr<StringRangeSet const> row_set)
      : column_family_name_(std::move(column_family_name)),
        row_ranges_(std::move(row_set)),
        column_ranges_(StringRangeSet::All()),
        timestamp_ranges_(TimestampRangeSet::All()) {
    assert(row_ranges_ != nullptr);
  }

  class FilterApply {
   public:
    explicit FilterApply(FilteredColumnFamilyStream& parent)
        : parent_(parent) {}

    bool operator()(ColumnRange const& column_range) {
      if (column_range.column_family == parent_.column_family_name_) {
        parent_.column_ranges_.Intersect(column_range.range);
      }
      return true;
    }

    bool operator()(TimestampRange const& timestamp_range) {
      parent_.timestamp_ranges_.Intersect(timestamp_range.range);
      return true;
    }

    bool operator()(RowKeyRegex const& row_key_regex) {
      parent_.row_regexes_.emplace_back(row_key_regex.regex);
      return true;
    }

    bool operator()(FamilyNameRegex const&) { return false; }

    bool operator()(ColumnRegex const& column_regex) {
      parent_.column_regexes_.emplace_back(column_regex.regex);
      return true;
    }

   private:
    FilteredColumnFamilyStream& parent_;
  };

  std::string column_family_name_;
  std::shared_ptr<StringRangeSet const> row_ranges_;
  std::vector<std::shared_ptr<re2::RE2 const>> row_regexes_;
  StringRangeSet column_ranges_;
  std::vector<std::shared_ptr<re2::RE2 const>> column_regexes_;
  TimestampRangeSet timestamp_ranges_;
  mutable bool initialized_{false};
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_COLUMN_FAMILY_H
