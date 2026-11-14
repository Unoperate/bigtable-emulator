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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_IN_MEMORY_IN_MEMORY_BACKEND_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_IN_MEMORY_IN_MEMORY_BACKEND_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/match.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "backend.h"
#include "column_family.h"
#include "filter.h"
#include "in_memory/in_memory_column_family.h"
#include "range_set.h"
#include "table_operations.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <map>
#include <memory>
#include <stack>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class InMemoryBackend final : public Backend {
 public:
  StatusOr<std::vector<RestoredTable>> Restore() override {
    return std::vector<RestoredTable>{};
  }
  StatusOr<std::unique_ptr<TableOperations>> CreateTable(
      google::bigtable::admin::v2::Table const& schema) override;
  /// Destroying `operations` frees the maps.
  void DestroyTable(std::unique_ptr<TableOperations> /*operations*/) override {}
  void StopBackgroundWork() override {}
  Status Shutdown() override { return Status(); }
};

struct RestoreValue {
  InMemoryColumnFamily& column_family;
  std::string column_qualifier;
  std::chrono::microseconds timestamp;
  std::string value;
};

struct DeleteValue {
  InMemoryColumnFamily& column_family;
  std::string column_qualifier;
  std::chrono::microseconds timestamp;
};

class InMemoryTableOperations
    : public TableOperationsBase<InMemoryColumnFamily> {
 public:
  static StatusOr<std::unique_ptr<TableOperations>> Create(
      google::bigtable::admin::v2::Table const& schema);

  /// `deadline` is ignored: the table lock is already exclusive.
  StatusOr<std::unique_ptr<RowTransaction>> NewRowTransaction(
      std::string const& row_key, Deadline deadline) override;

  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter>) const override;

  StatusOr<std::vector<RowKeySample>> SampleRowKeys() override;

  void RunPeriodicMaintenance() override {}

  Status RemoveAllDataFromColumnFamilies() override {
    for (auto& column_family : column_families_) {
      column_family.second->RemoveAllDataFromColumnFamily();
    }
    return Status();
  }

  Status DropRowRange(std::string const& row_key_prefix) override {
    for (auto& cf : column_families_) {
      for (auto row_it = cf.second->lower_bound(row_key_prefix);
           row_it != cf.second->end();) {
        if (absl::StartsWith(row_it->first, row_key_prefix)) {
          row_it = cf.second->erase(row_it);
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

  /// The schema lives in Table.
  Status UpdateTable(
      google::bigtable::admin::v2::Table const& /*new_schema*/) override {
    return Status();
  }

  Status CommitDeletion() override { return Status(); }

  bool AllowsConcurrentDataOperations() const override { return false; }

  absl::optional<std::chrono::milliseconds> MaintenanceInterval()
      const override {
    return absl::nullopt;
  }

  Status BackgroundError() const override { return Status(); }

  Status Close() override { return Status(); }

 private:
  StatusOr<std::size_t> GetRowCountEstimate() const {
    std::size_t row_count_estimate = 0;
    for (auto const& cf : column_families_) {
      row_count_estimate = std::max(cf.second->size(), row_count_estimate);
    }

    return row_count_estimate;
  };
};

/// Reached only while the Table holds its lock, which outlives this object, so
/// a reference to the operations is enough.
class InMemoryRowTransaction : public RowTransaction {
 public:
  explicit InMemoryRowTransaction(InMemoryTableOperations& utilities,
                                  std::string const& row_key)
      : RowTransaction(row_key), utilities_(utilities) {}

  ~InMemoryRowTransaction() override {
    if (!committed_) {
      Undo();
    }
  };

  Status Commit() override {
    committed_ = true;
    return Status();
  }

  Status AddToCell(
      ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
      absl::optional<std::chrono::microseconds> timestamp_override) override;

  // timestamp_override, if provided, will be used instead of
  // set_cell.timestamp. The override is used to set the timestamp to
  // the server time in case a timestamp <= 0 is provided.
  Status SetCell(
      ::google::bigtable::v2::Mutation_SetCell const& set_cell,
      absl::optional<std::chrono::microseconds> timestamp_override) override;
  Status MergeToCell(::google::bigtable::v2::Mutation_MergeToCell const&
                         merge_to_cell) override;
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

  /// Writes are applied in place, so this sees the transaction's own writes.
  StatusOr<bool> RowMatches(
      absl::optional<btproto::RowFilter> const& filter) override;

 private:
  void Undo();
  InMemoryTableOperations& utilities_;
  bool committed_ = false;
  std::stack<absl::variant<DeleteValue, RestoreValue>> undo_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_IN_MEMORY_IN_MEMORY_BACKEND_H
