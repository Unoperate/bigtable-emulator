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

#include "persist/rocksdb/storage_row_tx.h"
#include "absl/strings/str_cat.h"
#include "persist/rocksdb/storage.h"
#include "persist/storage.h"
#include <cassert>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

RocksDBStorageRowTX::~RocksDBStorageRowTX() {
  DBG("[RocksDBStorageRowTX][~RocksDBStorageRowTX] destructor table={} "
      "row={} txn_null={}",
      table_name_, row_key_, (txn_ == nullptr));
  if (txn_ != nullptr) {
    DBG("[RocksDBStorageRowTX][~RocksDBStorageRowTX] performing rollback "
        "table={} row={}",
        table_name_, row_key_);
    Rollback(Status());
  }
  DBG("[RocksDBStorageRowTX][~RocksDBStorageRowTX] exit");
}

Status RocksDBStorageRowTX::Rollback(Status status) {
  auto const txn_status = txn_->Rollback();
  assert(txn_status.ok());
  delete txn_;
  txn_ = nullptr;
  if (!status.ok()) {
    return status;
  }
  return Status();
}

RocksDBStorageRowTX::RocksDBStorageRowTX(std::string const table_name,
                                         std::string const row_key,
                                         rocksdb::Transaction* txn,
                                         RocksDBStorage* db)
    : table_name_(std::move(table_name)),
      row_key_(std::move(row_key)),
      txn_(txn),
      roptions_(),
      db_(db) {}

void RocksDBStorageRowTX::lazyRowDataRemoveColumnFamily(
    std::string const& column_family) {
  for (auto it = lazy_row_data_.begin(); it != lazy_row_data_.end();) {
    (std::get<0>(it->first) == column_family) ? lazy_row_data_.erase(it++)
                                              : (++it);
  }
}

bool RocksDBStorageRowTX::hasLazyRowData(std::string const& column_family,
                                         std::string const& column_qualifier) {
  return lazy_row_data_.find(std::make_tuple(
             column_family, column_qualifier)) != lazy_row_data_.end();
}

storage::RowData& RocksDBStorageRowTX::lazyRowDataRef(
    std::string const& column_family, std::string const& column_qualifier) {
  return lazy_row_data_[std::make_tuple(column_family, column_qualifier)];
}

Status RocksDBStorageRowTX::lazyRowDataDelete(
    std::string const& column_family, std::string const& column_qualifier) {
  lazy_row_data_.erase(
      lazy_row_data_.find(std::make_tuple(column_family, column_qualifier)));
  return db_->RawDataDeleteColumn(txn_, table_name_, column_family, row_key_,
                                  column_qualifier);
}

Status RocksDBStorageRowTX::DeleteRowFromColumnFamily(
    std::string const& column_family) {
  lazyRowDataRemoveColumnFamily(column_family);
  return db_->RawDataDelete(txn_, table_name_, column_family, row_key_);
}

Status RocksDBStorageRowTX::Commit() {
  for (auto const& row_entry : lazy_row_data_) {
    auto out = db_->SerializeRow(row_entry.second);
    auto status =
        db_->RawDataPut(txn_, table_name_, std::get<0>(row_entry.first),
                        row_key_, std::get<1>(row_entry.first), std::move(out));
    if (!status.ok()) {
      return Rollback(status);
    }
  }
  auto const status = txn_->Commit();
  assert(status.ok());
  delete txn_;
  txn_ = nullptr;
  return Status();
}

Status RocksDBStorageRowTX::LoadRow(std::string const& column_family,
                                    std::string const& column_qualifier) {
  if (!hasLazyRowData(column_family, column_qualifier)) {
    std::string out;
    auto get_status = db_->RawDataGet(txn_, table_name_, column_family,
                                      row_key_, column_qualifier, &out);
    if (!get_status.ok()) {
      return get_status;
    }
    if (out.size() == 0) {
      lazyRowDataRef(column_family, column_qualifier) = storage::RowData();
    } else {
      auto row_data = db_->DeserializeRow(std::move(out));
      if (!row_data.ok()) {
        return row_data.status();
      }
      lazyRowDataRef(column_family, column_qualifier) = row_data.value();
    }
  }
  return Status();
}

Status RocksDBStorageRowTX::SetCell(std::string const& column_family,
                                    std::string const& column_qualifier,
                                    std::chrono::milliseconds timestamp,
                                    std::string const& value) {
  DBG("[RocksDBStorageRowTX][SetCell] table={} row={} cf={} cq={} ts={}",
      table_name_, row_key_, column_family, column_qualifier, timestamp.count());
  auto status = LoadRow(column_family, column_qualifier);
  if (!status.ok()) {
    return status;
  }
  RowDataEnsure(lazyRowDataRef(column_family, column_qualifier),
                column_qualifier, timestamp, value);
  DBG("[RocksDBStorageRowTX][SetCell] exit table={} row={}", table_name_,
      row_key_);
  return Status();
}

StatusOr<absl::optional<std::string>> RocksDBStorageRowTX::UpdateCell(
    std::string const& column_family, std::string const& column_qualifier,
    std::chrono::milliseconds timestamp, std::string& value,
    std::function<StatusOr<std::string>(std::string const&,
                                        std::string&&)> const& update_fn) {
  DBG("[RocksDBStorageRowTX][UpdateCell] table={} row={} cf={} cq={} ts={}",
      table_name_, row_key_, column_family, column_qualifier, timestamp.count());
  auto status = LoadRow(column_family, column_qualifier);
  if (!status.ok()) {
    return status;
  }
  auto& row_data = lazyRowDataRef(column_family, column_qualifier);
  absl::optional<std::string> old_value = absl::nullopt;
  auto const& cells = (row_data.mutable_column()->mutable_cells());
  auto cell_it = cells->find(timestamp.count());
  if (cell_it != cells->end()) {
    old_value = cell_it->second;
  }
  auto maybe_new_value = update_fn(column_qualifier, std::move(value));
  if (!maybe_new_value.ok()) {
    return maybe_new_value.status();
  }
  RowDataEnsure(row_data, column_qualifier, timestamp, maybe_new_value.value());
  DBG("[RocksDBStorageRowTX][UpdateCell] exit table={} row={}", table_name_,
      row_key_);
  return old_value;
}

Status RocksDBStorageRowTX::DeleteRowColumn(
    std::string const& column_family, std::string const& column_qualifier,
    ::google::bigtable::v2::TimestampRange const& time_range) {
  DBG("[RocksDBStorageRowTX][DeleteRowColumn] table={} row={} cf={} cq={} "
      "start_micros={} end_micros={}",
      table_name_, row_key_, column_family, column_qualifier,
      time_range.start_timestamp_micros(), time_range.end_timestamp_micros());
  auto status = LoadRow(column_family, column_qualifier);
  if (!status.ok()) {
    return status;
  }
  auto& row_data = lazyRowDataRef(column_family, column_qualifier);
  auto& cells = (*row_data.mutable_column()->mutable_cells());
  int64_t start_micros = time_range.start_timestamp_micros();
  absl::optional<int64_t> maybe_end_micros =
      time_range.end_timestamp_micros() == 0
          ? absl::nullopt
          : absl::optional<int64_t>(time_range.end_timestamp_micros());
  auto start_millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::microseconds(start_micros))
                          .count();
  for (auto cell_it = cells.begin(); cell_it != cells.end();) {
    int64_t cell_timestamp = cell_it->first;
    bool in_range = cell_timestamp >= start_millis;
    if (maybe_end_micros.has_value()) {
      auto end_millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::microseconds(*maybe_end_micros))
                            .count();
      in_range = in_range && cell_timestamp < end_millis;
    }
    if (in_range) {
      DBG("[RocksDBStorageRowTX][DeleteRowColumn] erased cell table={} row={} "
          "cf={} cq={} cell_ts={}",
          table_name_, row_key_, column_family, column_qualifier, cell_timestamp);
      cell_it = cells.erase(cell_it);
    } else {
      DBG("[RocksDBStorageRowTX][DeleteRowColumn] preserved cell table={} "
          "row={} cell_ts={} start_millis={}",
          table_name_, row_key_, cell_timestamp, start_millis);
      ++cell_it;
    }
  }
  DBG("[RocksDBStorageRowTX][DeleteRowColumn] end iteration table={} row={} "
      "cf={} cq={}",
      table_name_, row_key_, column_family, column_qualifier);
  if (cells.empty()) {
    DBG("[RocksDBStorageRowTX][DeleteRowColumn] empty column deleting table={} "
        "row={} cf={} cq={}",
        table_name_, row_key_, column_family, column_qualifier);
    auto del_status = lazyRowDataDelete(column_family, column_qualifier);
    if (!del_status.ok()) {
      return del_status;
    }
  }
  DBG("[RocksDBStorageRowTX][DeleteRowColumn] exit table={} row={}",
      table_name_, row_key_);
  return Status();
}

Status RocksDBStorageRowTX::DeleteRowFromAllColumnFamilies() {
  DBG("[RocksDBStorageRowTX][DeleteRowFromAllColumnFamilies] table={} row={} "
      "cf_count={}",
      table_name_, row_key_, db_->column_families_handles_map.size());
  for (auto const& cf_entry : db_->column_families_handles_map) {
    if (cf_entry.first == db_->storage_config.meta_column_family()) {
      continue;
    }
    auto status = DeleteRowFromColumnFamily(cf_entry.first);
    if (!status.ok()) {
      return status;
    }
  }
  DBG("[RocksDBStorageRowTX][DeleteRowFromAllColumnFamilies] exit table={} "
      "row={}",
      table_name_, row_key_);
  return Status();
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
