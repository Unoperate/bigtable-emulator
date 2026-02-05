// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file storage_row_tx.cc
 * @brief In-memory row-scoped storage transaction implementation.
 */

#include "absl/strings/str_cat.h"
#include "persist/utils/logging.h"
#include "persist/memory/storage.h"
#include "persist/storage.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

MemoryStorageRowTX::~MemoryStorageRowTX() {
  DBG("[MemoryStorageRowTX][~MemoryStorageRowTX] destructor table={} row={}",
      table_name_, row_key_);
}

Status MemoryStorageRowTX::Rollback(Status status) {
  DBG("[MemoryStorageRowTX][Rollback] table={} row={} committed={} status={}",
      table_name_, row_key_, committed_, status.message());
  if (!committed_) {
    Undo();
  }
  return status;
}

MemoryStorageRowTX::MemoryStorageRowTX(std::string const table_name,
                                       std::string const row_key,
                                       MemoryStorage* db,
                                       std::shared_ptr<Table> table)
    : table_name_(std::move(table_name)),
      row_key_(std::move(row_key)),
      db_(db),
      table_(std::move(table)) {
  DBG("[MemoryStorageRowTX][MemoryStorageRowTX] table={} row={}", table_name_,
      row_key_);
}

Status MemoryStorageRowTX::DeleteRowFromColumnFamily(
    std::string const& column_family) {
  DBG("[MemoryStorageRowTX][DeleteRowFromColumnFamily] table={} row={} cf={}",
      table_name_, row_key_, column_family);
  auto maybe_column_family = table_->FindColumnFamily(column_family);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  auto column_family_it = table_->find(column_family);
  if (column_family_it == table_->end()) {
    LERROR(
        "[MemoryStorageRowTX][DeleteRowFromColumnFamily] column family not "
        "found table={} row={} cf={}",
        table_name_, row_key_, column_family);
    return NotFoundError(
        "column family not found in table",
        GCP_ERROR_INFO().WithMetadata("column family", column_family));
  }

  std::map<std::string, ColumnFamilyRow>::iterator column_family_row_it;
  if (column_family_it->second->find(row_key_) ==
      column_family_it->second->end()) {
    // The row does not exist
    LERROR(
        "[MemoryStorageRowTX][DeleteRowFromColumnFamily] row key not found "
        "table={} row={} cf={}",
        table_name_, row_key_, column_family_it->first);
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

Status MemoryStorageRowTX::Commit() {
  DBG("[MemoryStorageRowTX][Commit] table={} row={}", table_name_, row_key_);
  committed_ = true;
  return Status();
}

Status MemoryStorageRowTX::SetCell(std::string const& column_family,
                                   std::string const& column_qualifier,
                                   std::chrono::milliseconds timestamp,
                                   std::string const& value) {
  DBG("[MemoryStorageRowTX][SetCell] table={} row={} cf={} cq={} ts={}",
      table_name_, row_key_, column_family, column_qualifier, timestamp.count());
  auto status = table_->FindColumnFamily(column_family);
  if (!status.ok()) {
    return status.status();
  }

  auto& cf = status->get();
  std::string val = value;
  auto maybe_old_value =
      cf.UpdateCell(row_key_, column_qualifier, timestamp, val);
  if (!maybe_old_value) {
    return maybe_old_value.status();
  }

  if (!maybe_old_value.value()) {
    DeleteValue delete_value{cf, std::move(column_qualifier), timestamp};
    undo_.emplace(std::move(delete_value));
  } else {
    RestoreValue restore_value{cf, std::move(column_qualifier), timestamp,
                               std::move(maybe_old_value.value().value())};
    undo_.emplace(std::move(restore_value));
  }
  return Status();
}

StatusOr<absl::optional<std::string>> MemoryStorageRowTX::UpdateCell(
    std::string const& column_family, std::string const& column_qualifier,
    std::chrono::milliseconds timestamp, std::string& value,
    std::function<StatusOr<std::string>(std::string const&,
                                        std::string&&)> const& update_fn) {
  DBG("[MemoryStorageRowTX][UpdateCell] table={} row={} cf={} cq={} ts={}",
      table_name_, row_key_, column_family, column_qualifier, timestamp.count());
  auto status = table_->FindColumnFamily(column_family);
  if (!status.ok()) {
    return status.status();
  }

  auto& cf = status->get();
  auto maybe_old_value =
      cf.UpdateCell(row_key_, column_qualifier, timestamp, value);
  if (!maybe_old_value) {
    return maybe_old_value.status();
  }

  if (!maybe_old_value.value()) {
    DeleteValue delete_value{cf, std::move(column_qualifier), timestamp};
    undo_.emplace(std::move(delete_value));
  } else {
    RestoreValue restore_value{cf, std::move(column_qualifier), timestamp,
                               std::move(maybe_old_value.value().value())};
    undo_.emplace(std::move(restore_value));
  }
  DBG("[MemoryStorageRowTX][UpdateCell] exit table={} row={}", table_name_,
      row_key_);
  return Status();
}

Status MemoryStorageRowTX::DeleteRowColumn(
    std::string const& column_family, std::string const& column_qualifier,
    ::google::bigtable::v2::TimestampRange const& time_range) {
  DBG("[MemoryStorageRowTX][DeleteRowColumn] table={} row={} cf={} cq={} "
      "start_micros={} end_micros={}",
      table_name_, row_key_, column_family, column_qualifier,
      time_range.start_timestamp_micros(), time_range.end_timestamp_micros());
  auto maybe_column_family = table_->FindColumnFamily(column_family);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }
  auto& cf = maybe_column_family->get();

  auto deleted_cells = cf.DeleteColumn(row_key_, column_qualifier, time_range);

  for (auto& cell : deleted_cells) {
    RestoreValue restore_value{cf, column_qualifier, std::move(cell.timestamp),
                               std::move(cell.value)};
    undo_.emplace(std::move(restore_value));
  }
  DBG("[MemoryStorageRowTX][DeleteRowColumn] exit table={} row={} deleted_cells={}",
      table_name_, row_key_, deleted_cells.size());
  return Status();
}

Status MemoryStorageRowTX::DeleteRowFromAllColumnFamilies() {
  DBG("[MemoryStorageRowTX][DeleteRowFromAllColumnFamilies] table={} row={}",
      table_name_, row_key_);
  bool row_existed = false;
  for (auto& column_family : table_->column_families_) {
    auto deleted_columns = column_family.second->DeleteRow(row_key_);

    for (auto& column : deleted_columns) {
      for (auto& cell : column.second) {
        RestoreValue restrore_value = {*column_family.second,
                                       std::move(column.first), cell.timestamp,
                                       std::move(cell.value)};
        undo_.emplace(std::move(restrore_value));
        row_existed = true;
      }
    }
  }

  DBG("[MemoryStorageRowTX][DeleteRowFromAllColumnFamilies] exit row_existed={}",
      row_existed);
  if (row_existed) {
    return Status();
  }
  LERROR(
      "[MemoryStorageRowTX][DeleteRowFromAllColumnFamilies] row not found "
      "table={} row={}",
      table_name_, row_key_);
  return NotFoundError("row not found in table",
                       GCP_ERROR_INFO().WithMetadata("row", row_key_));
}

void MemoryStorageRowTX::Undo() {
  DBG("[MemoryStorageRowTX][Undo] table={} row={} undo_stack_size={}",
      table_name_, row_key_, undo_.size());
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

    // If we get here, there is an type of undo log that has not been
    // implemented!
    std::abort();
  }
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
