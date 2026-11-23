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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_OPERATIONS_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_OPERATIONS_H

#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/types/optional.h"
#include "column_family.h"
#include "filter.h"
#include "in_memory/in_memory_column_family.h"
#include "range_set.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
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

namespace btproto = ::google::bigtable::v2;
using Deadline = std::chrono::system_clock::time_point;

Status ValidateAddToCellTransaction(
    ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
    absl::optional<::google::bigtable::admin::v2::Type> const& cf_value_type);

google::bigtable::v2::ReadModifyWriteRowResponse
FamiliesToReadModifyWriteResponse(
    std::string const& row_key,
    std::map<std::string, InMemoryColumnFamily> const& families);

/// One row operation, rolled back if destroyed without Commit(). Lives only
/// while the Table holds its data lock.
class RowTransaction {
 public:
  explicit RowTransaction(std::string const& row_key) : row_key_(row_key) {}
  virtual ~RowTransaction() = default;

  // timestamp_override, if provided, will be used instead of
  // set_cell.timestamp. The override is used to set the timestamp to
  // the server time in case a timestamp <= 0 is provided.
  virtual Status SetCell(
      ::google::bigtable::v2::Mutation_SetCell const& set_cell,
      absl::optional<std::chrono::microseconds> timestamp_override) = 0;
  virtual Status AddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      absl::optional<std::chrono::microseconds> timestamp_override) = 0;
  /// Returns UNIMPLEMENTED for now.
  virtual Status MergeToCell(
      ::google::bigtable::v2::Mutation_MergeToCell const& merge_to_cell) = 0;
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

  /// Whether `filter` leaves any cell of this row.
  virtual StatusOr<bool> RowMatches(
      absl::optional<btproto::RowFilter> const& filter) = 0;

  virtual Status Commit() = 0;

 protected:
  // row_key_ is initialized from the request proto, and therefore it
  // is safe to access it while the mutation request is ongoing. We
  // store a reference to it to avoid copying a potentially very large
  // (up to 4KB) value.
  std::string const& row_key_;
};

struct RowKeySample {
  std::string row_key;
  std::int64_t offset_bytes;
};

/// Reached only under Table's data lock.
class DataOperations {
 public:
  /// `deadline` is ignored for now.
  virtual StatusOr<std::unique_ptr<RowTransaction>> NewRowTransaction(
      std::string const& row_key, Deadline deadline) = 0;

  virtual StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const = 0;

  /// Ends with the empty key.
  virtual StatusOr<std::vector<RowKeySample>> SampleRowKeys() = 0;

  /// Called every MaintenanceInterval().
  virtual void RunPeriodicMaintenance() = 0;

 protected:
  ~DataOperations() = default;
};

/// Reached only under Table's exclusive lock.
class AdminOperations {
 public:
  /// Returns the new schema.
  virtual StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
      google::bigtable::admin::v2::Table schema) = 0;

  /// Stores schema changes that do not affect data.
  virtual Status UpdateTable(
      google::bigtable::admin::v2::Table const& new_schema) = 0;

  virtual Status DropRowRange(std::string const& row_key_prefix) = 0;

  /// delete_all_data_from_table.
  virtual Status RemoveAllDataFromColumnFamilies() = 0;

  /// The point after which DeleteTable cannot be undone.
  virtual Status CommitDeletion() = 0;

 protected:
  ~AdminOperations() = default;
};

/// One object per table: one RocksDB database, or one set of maps.
class TableOperations : public DataOperations, public AdminOperations {
 public:
  virtual ~TableOperations() = default;

  /// If false, Table takes its lock exclusively for data operations too.
  virtual bool AllowsConcurrentDataOperations() const = 0;

  /// How often to call RunPeriodicMaintenance(); nullopt means never.
  virtual absl::optional<std::chrono::milliseconds> MaintenanceInterval()
      const = 0;

  /// A non-OK status makes every request on the table fail.
  virtual Status BackgroundError() const = 0;

  /// Closes the storage at shutdown, under the exclusive lock.
  virtual Status Close() = 0;
};

template <class CF>
class TableOperationsBase : public TableOperations {
 public:
  typename std::map<std::string, std::shared_ptr<CF>>::iterator begin() {
    return column_families_.begin();
  }
  typename std::map<std::string, std::shared_ptr<CF>>::iterator end() {
    return column_families_.end();
  }
  typename std::map<std::string, std::shared_ptr<CF>>::iterator find(
      std::string const& column_family) {
    return column_families_.find(column_family);
  }

  template <typename MESSAGE>
  StatusOr<std::shared_ptr<CF>> FindColumnFamily(MESSAGE const& message) const {
    auto column_family_it = column_families_.find(message.family_name());
    if (column_family_it == column_families_.end()) {
      return google::cloud::internal::NotFoundError(
          "No such column family.",
          GCP_ERROR_INFO().WithMetadata("column family",
                                        message.family_name()));
    }
    return column_family_it->second;
  }

 protected:
  std::map<std::string, std::shared_ptr<CF>> column_families_;
};

/// Fails unless `column_family_id` is 1-64 characters matching
/// `[_a-zA-Z0-9][-_.a-zA-Z0-9]*`.
Status ValidateColumnFamilyId(std::string const& column_family_id);

/// Checks the family limit and every family's ID and value type.
Status ValidateSchema(google::bigtable::admin::v2::Table const& schema);

/// Validates `request` against `schema` and returns the new schema.
StatusOr<google::bigtable::admin::v2::Table>
ApplyModifyColumnFamiliesToSchemaOnly(
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
    google::bigtable::admin::v2::Table schema);

/// Merges the per-family streams into one and applies `filter`. May call
/// `create_column_family_streams` several times; each call must return new
/// streams.
StatusOr<CellStream> CreateFilteredTableStream(
    std::function<std::vector<std::unique_ptr<FilteredColumnFamilyStream>>()>
        create_column_family_streams,
    absl::optional<btproto::RowFilter> const& filter);

/// Whether `filter` leaves any cell of `row_key`.
StatusOr<bool> RowHasMatchingCells(
    DataOperations const& operations, std::string const& row_key,
    absl::optional<btproto::RowFilter> const& filter);

// TODO(prawilny): make it return Optional and put function outside of the call.
/// Samples rows by scanning the table. `estimate_row_count` is called only if
/// no row was sampled.
StatusOr<std::vector<RowKeySample>> SampleRowKeysByScanning(
    DataOperations const& operations,
    std::function<StatusOr<std::size_t>()> const& estimate_row_count);

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_OPERATIONS_H
