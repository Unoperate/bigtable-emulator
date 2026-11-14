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

#include "in_memory/in_memory_backend.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_format.h"
#include "absl/types/variant.h"
#include "column_family.h"
#include "filter.h"
#include "google/protobuf/util/field_mask_util.h"
#include "in_memory/in_memory_column_family.h"
#include "table_operations.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <chrono>
#include <cstdlib>
#include <map>
#include <memory>
#include <stack>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

namespace {
// ProcessReadModifyWriteRuleResult records the result of a
// ReadModifyWriteRule computation for possible undo in the undo log
// and also updates the tmp_families temporary table (containing only
// one row) with the modified cell for later return.
void ProcessReadModifyWriteResult(
    std::shared_ptr<InMemoryColumnFamily>& column_family,
    std::string const& row_key,
    std::stack<absl::variant<DeleteValue, RestoreValue>>& undo,
    google::bigtable::v2::ReadModifyWriteRule const& rule,
    ReadModifyWriteCellResult& result,
    std::map<std::string, InMemoryColumnFamily>& tmp_families) {
  if (result.maybe_old_value.has_value()) {
    // We overwrote a cell, we need to record a RestoreValue in the undo log
    RestoreValue restore_value{*column_family, rule.column_qualifier(),
                               result.timestamp,
                               std::move(result.maybe_old_value.value())};
    undo.emplace(std::move(restore_value));
  } else {
    // We created a new cell -- we would need to delete it in any rollback
    DeleteValue delete_value{*column_family, rule.column_qualifier(),
                             result.timestamp};
    undo.emplace(std::move(delete_value));
  }

  // Record the cell in our local mini table here to use in
  // assembling a row of changed cells for return.
  tmp_families[rule.family_name()].SetCell(row_key, rule.column_qualifier(),
                                           result.timestamp,
                                           std::move(result.value));
}
}  // anonymous namespace

StatusOr<std::unique_ptr<TableOperations>> InMemoryBackend::CreateTable(
    google::bigtable::admin::v2::Table const& schema) {
  return InMemoryTableOperations::Create(schema);
}

StatusOr<std::unique_ptr<TableOperations>> InMemoryTableOperations::Create(
    google::bigtable::admin::v2::Table const& schema) {
  auto res = std::make_unique<InMemoryTableOperations>();
  for (auto const& column_family_def : schema.column_families()) {
    absl::optional<google::bigtable::admin::v2::Type> opt_value_type =
        absl::nullopt;

    // Support for complex types (AddToCell aggregations, e.t.c.).
    if (column_family_def.second.has_value_type()) {
      opt_value_type = column_family_def.second.value_type();
    }

    if (opt_value_type.has_value()) {
      auto cf = InMemoryColumnFamily::ConstructAggregateColumnFamily(
          opt_value_type.value());
      if (!cf) {
        return cf.status();
      }
      res->column_families_.emplace(column_family_def.first, cf.value());
    } else {
      res->column_families_.emplace(column_family_def.first,
                                    std::make_shared<InMemoryColumnFamily>());
    }
  }
  return std::unique_ptr<TableOperations>(std::move(res));
}

StatusOr<std::unique_ptr<RowTransaction>>
InMemoryTableOperations::NewRowTransaction(std::string const& row_key,
                                           Deadline /*deadline*/) {
  return std::unique_ptr<RowTransaction>(
      std::make_unique<InMemoryRowTransaction>(*this, row_key));
}

StatusOr<std::vector<RowKeySample>> InMemoryTableOperations::SampleRowKeys() {
  return SampleRowKeysByScanning(*this,
                                 [this] { return GetRowCountEstimate(); });
}

StatusOr<CellStream> InMemoryTableOperations::CreateCellStream(
    std::shared_ptr<StringRangeSet> range_set,
    absl::optional<google::bigtable::v2::RowFilter> maybe_row_filter) const {
  return CreateFilteredTableStream(
      [range_set = std::move(range_set), this] {
        std::vector<std::unique_ptr<FilteredColumnFamilyStream>> per_cf_streams;
        per_cf_streams.reserve(column_families_.size());
        for (auto const& column_family : column_families_) {
          auto stream = column_family.second->GetFilteredColumnFamilyStream(
              range_set, column_family.first);
          per_cf_streams.emplace_back(std::move(stream));
        }
        return per_cf_streams;
      },
      maybe_row_filter);
}

StatusOr<google::bigtable::admin::v2::Table>
InMemoryTableOperations::ModifyColumnFamilies(
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
    google::bigtable::admin::v2::Table schema) {
  auto maybe_new_schema =
      ApplyModifyColumnFamiliesToSchemaOnly(request, std::move(schema));
  if (!maybe_new_schema) {
    return maybe_new_schema.status();
  }

  // The request is valid; replay it on a copy of the column families, in
  // order, so that a family dropped and created again starts empty. Updates
  // change only the schema.
  auto new_column_families = column_families_;
  for (auto const& modification : request.modifications()) {
    if (modification.drop()) {
      new_column_families.erase(modification.id());
    } else if (modification.has_create()) {
      std::shared_ptr<InMemoryColumnFamily> cf;
      // Have we been asked to create an aggregate column family?
      if (modification.create().has_value_type()) {
        auto value_type = modification.create().value_type();
        auto maybe_cf =
            InMemoryColumnFamily::ConstructAggregateColumnFamily(value_type);
        if (!maybe_cf) {
          return maybe_cf.status();
        }
        cf = std::move(maybe_cf.value());
      } else {
        cf = std::make_shared<InMemoryColumnFamily>();
      }
      new_column_families.emplace(modification.id(), std::move(cf));
    }
  }
  // Defer destroying potentially large objects to after releasing the lock.
  column_families_.swap(new_column_families);
  return maybe_new_schema;
}

Status InMemoryRowTransaction::AddToCell(
    ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
    absl::optional<std::chrono::microseconds> timestamp_override) {
  auto status = utilities_.FindColumnFamily(add_to_cell);
  if (!status.ok()) {
    return status.status();
  }

  auto const& cf = status.value();
  auto cf_value_type = cf->GetValueType();

  auto validation_res =
      ValidateAddToCellTransaction(add_to_cell, cf_value_type);
  if (!validation_res.ok()) {
    return validation_res;
  }

  auto int64_input = add_to_cell.input().int_value();

  auto value = google::cloud::internal::EncodeBigEndian(int64_input);

  auto const timestamp = timestamp_override.value_or(std::chrono::microseconds(
      add_to_cell.timestamp().raw_timestamp_micros()));

  auto column_qualifier = add_to_cell.column_qualifier().raw_value();
  auto maybe_old_value =
      cf->UpdateCell(row_key_, column_qualifier, timestamp, value);
  if (!maybe_old_value) {
    return maybe_old_value.status();
  }

  if (!maybe_old_value.value()) {
    DeleteValue delete_value{*cf, std::move(column_qualifier), timestamp};
    undo_.emplace(std::move(delete_value));
  } else {
    RestoreValue restore_value{*cf, std::move(column_qualifier), timestamp,
                               std::move(maybe_old_value.value().value())};
    undo_.emplace(std::move(restore_value));
  }
  return Status();
}

Status InMemoryRowTransaction::DeleteFromColumn(
    ::google::bigtable::v2::Mutation_DeleteFromColumn const&
        delete_from_column) {
  auto maybe_column_family = utilities_.FindColumnFamily(delete_from_column);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  if (delete_from_column.has_time_range()) {
    auto const start = std::chrono::microseconds(
        delete_from_column.time_range().start_timestamp_micros());
    auto const end = std::chrono::microseconds(
        delete_from_column.time_range().end_timestamp_micros());

    // An end timestamp micros of 0 is to be interpreted as infinity,
    // so we allow that.
    if (end <= start &&
        delete_from_column.time_range().end_timestamp_micros() != 0) {
      return InvalidArgumentError(
          "empty or reversed time range: the end timestamp must be more than "
          "the start timestamp",
          GCP_ERROR_INFO().WithMetadata("delete_from_column proto",
                                        delete_from_column.DebugString()));
    }
  }

  auto const& column_family = maybe_column_family.value();

  auto deleted_cells = column_family->DeleteColumn(
      row_key_, delete_from_column.column_qualifier(),
      delete_from_column.time_range());

  for (auto& cell : deleted_cells) {
    RestoreValue restore_value{
        *column_family, delete_from_column.column_qualifier(),
        std::move(cell.timestamp), std::move(cell.value)};
    undo_.emplace(std::move(restore_value));
  }

  return Status();
}

Status InMemoryRowTransaction::DeleteFromRow() {
  bool row_existed = false;
  for (auto& column_family : utilities_) {
    auto deleted_columns = column_family.second->DeleteRow(row_key_);

    for (auto& column : deleted_columns) {
      for (auto& cell : column.second) {
        RestoreValue restore_value = {*column_family.second,
                                      std::move(column.first), cell.timestamp,
                                      std::move(cell.value)};
        undo_.emplace(std::move(restore_value));
        row_existed = true;
      }
    }
  }

  if (row_existed) {
    return Status();
  }

  return NotFoundError("row not found in table",
                       GCP_ERROR_INFO().WithMetadata("row", row_key_));
}

Status InMemoryRowTransaction::DeleteFromFamily(
    ::google::bigtable::v2::Mutation_DeleteFromFamily const&
        delete_from_family) {
  // If the request references an incorrect schema (non-existent
  // column family) then return a failure status error immediately.
  auto maybe_column_family = utilities_.FindColumnFamily(delete_from_family);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  auto column_family_it = utilities_.find(delete_from_family.family_name());
  if (column_family_it == utilities_.end()) {
    return NotFoundError(
        "column family not found in table",
        GCP_ERROR_INFO().WithMetadata("column family",
                                      delete_from_family.family_name()));
  }

  auto ccf = column_family_it->second;
  if (!ccf->RowKeyExists(row_key_)) {
    // The row does not exist
    return NotFoundError(
        "row key is not found in column family",
        GCP_ERROR_INFO()
            .WithMetadata("row key", row_key_)
            .WithMetadata("column family", column_family_it->first));
  }

  auto deleted = column_family_it->second->DeleteRow(row_key_);
  for (auto const& column : deleted) {
    for (auto const& cell : column.second) {
      RestoreValue restore_value{*column_family_it->second,
                                 std::move(column.first), cell.timestamp,
                                 std::move(cell.value)};
      undo_.emplace(std::move(restore_value));
    }
  }

  return Status();
}

// timestamp_override, if provided, will be used instead of
// set_cell.timestamp. The override is used to set the timestamp to
// the server time in case a timestamp <= 0 is provided.
Status InMemoryRowTransaction::SetCell(
    ::google::bigtable::v2::Mutation_SetCell const& set_cell,
    absl::optional<std::chrono::microseconds> timestamp_override) {
  auto maybe_column_family = utilities_.FindColumnFamily(set_cell);
  if (!maybe_column_family) {
    return maybe_column_family.status();
  }

  auto const& column_family = maybe_column_family.value();

  auto timestamp = std::chrono::microseconds(set_cell.timestamp_micros());

  if (timestamp_override.has_value()) {
    timestamp = timestamp_override.value();
  }

  auto maybe_old_value = column_family->SetCell(
      row_key_, set_cell.column_qualifier(), timestamp, set_cell.value());

  if (!maybe_old_value) {
    DeleteValue delete_value{*column_family,
                             std::move(set_cell.column_qualifier()), timestamp};
    undo_.emplace(std::move(delete_value));
  } else {
    RestoreValue restore_value{*column_family,
                               std::move(set_cell.column_qualifier()),
                               timestamp, std::move(maybe_old_value.value())};
    undo_.emplace(std::move(restore_value));
  }

  return Status();
}

Status InMemoryRowTransaction::MergeToCell(
    ::google::bigtable::v2::Mutation_MergeToCell const& merge_to_cell) {
  return UnimplementedError(
      "Unsupported mutation type.",
      GCP_ERROR_INFO().WithMetadata("mutation", merge_to_cell.DebugString()));
}

StatusOr<bool> InMemoryRowTransaction::RowMatches(
    absl::optional<btproto::RowFilter> const& filter) {
  return RowHasMatchingCells(utilities_, row_key_, filter);
}

StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
InMemoryRowTransaction::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request) {
  if (row_key_.empty()) {
    return InvalidArgumentError(
        "row key not set",
        GCP_ERROR_INFO().WithMetadata("request", request.DebugString()));
  }

  // tmp_families is a small one row mini table used to accumulate
  // changed cells efficiently for later return in the row returned by
  // the RPC.
  std::map<std::string, InMemoryColumnFamily> tmp_families;

  for (auto const& rule : request.rules()) {
    auto maybe_column_family = utilities_.FindColumnFamily(rule);
    if (!maybe_column_family) {
      return maybe_column_family.status();
    }

    auto column_family = maybe_column_family.value();

    if (rule.has_append_value()) {
      auto result = column_family->ReadModifyWrite(
          row_key_, rule.column_qualifier(), rule.append_value());

      ProcessReadModifyWriteResult(column_family, row_key_, undo_, rule, result,
                                   tmp_families);

    } else if (rule.has_increment_amount()) {
      auto maybe_result = column_family->ReadModifyWrite(
          row_key_, rule.column_qualifier(), rule.increment_amount());
      if (!maybe_result) {
        return maybe_result.status();
      }

      auto& result = maybe_result.value();

      ProcessReadModifyWriteResult(column_family, row_key_, undo_, rule, result,
                                   tmp_families);

    } else {
      return InvalidArgumentError(
          "either append value or increment amount must be set",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }
  }

  // Now assemble the returned value.
  return FamiliesToReadModifyWriteResponse(row_key_, tmp_families);
}

void InMemoryRowTransaction::Undo() {
  auto row_key = row_key_;

  while (!undo_.empty()) {
    auto op = undo_.top();
    undo_.pop();

    auto* restore_value = absl::get_if<RestoreValue>(&op);
    if (restore_value) {
      restore_value->column_family.SetCell(
          row_key, std::move(restore_value->column_qualifier),
          restore_value->timestamp, std::move(restore_value->value));
      continue;
    }

    auto* delete_value = absl::get_if<DeleteValue>(&op);
    if (delete_value) {
      delete_value->column_family.DeleteTimeStamp(
          row_key, std::move(delete_value->column_qualifier),
          delete_value->timestamp);
      continue;
    }

    // If we get here, there is a type of undo log that has not been
    // implemented!
    std::abort();
  }
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
