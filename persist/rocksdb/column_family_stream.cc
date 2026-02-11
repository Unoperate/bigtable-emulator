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

#include "persist/rocksdb/column_family_stream.h"
#include "absl/strings/str_cat.h"
#include "persist/rocksdb/storage.h"
#include <cassert>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

RocksDBColumnFamilyStream::~RocksDBColumnFamilyStream() { delete row_iter_; }

RocksDBColumnFamilyStream::RocksDBColumnFamilyStream(
    std::string const& column_family_name, rocksdb::ColumnFamilyHandle* handle,
    std::shared_ptr<StringRangeSet const> row_set, RocksDBStorage* db,
    std::string const table_name, bool const prefetch_all_columns)
    : column_family_name_(column_family_name),
      handle_(handle),
      row_ranges_(std::move(row_set)),
      column_ranges_(StringRangeSet::All()),
      timestamp_ranges_(TimestampRangeSet::All()),
      db_(db),
      initialized_(false),
      row_iter_(nullptr),
      table_name_(std::move(table_name)),
      prefetch_all_columns_(prefetch_all_columns) {}

bool RocksDBColumnFamilyStream::HasValue() const {
  DBG("[RocksDBColumnFamilyStream][HasValue] table={} cf={}", table_name_,
      column_family_name_);
  InitializeIfNeeded();
  DBG("[RocksDBColumnFamilyStream][HasValue] exit has_value={}",
      row_data_.has_value());
  return row_data_.has_value();
}

CellView const& RocksDBColumnFamilyStream::Value() const {
  InitializeIfNeeded();
  if (!cur_value_) {
    cur_value_ =
        CellView(row_key_, column_family_name_, column_it_.value()->first,
                 cell_it_.value()->first, cell_it_.value()->second);
  }
  return cur_value_.value();
}

void RocksDBColumnFamilyStream::NextRow() const {
  DBG("[RocksDBColumnFamilyStream][NextRow] table={} cf={}", table_name_,
      column_family_name_);
  if (!initialized_) {
    auto const& ranges = row_ranges_->disjoint_ranges();
    if (!ranges.empty()) {
      auto const& first_range = *ranges.begin();
      if (absl::holds_alternative<std::string>(first_range.start())) {
        db_->RawDataSeekPrefixed(row_iter_, table_name_,
                                 absl::get<std::string>(first_range.start()));
      } else {
        row_iter_->SeekToFirst();
      }
    } else {
      row_iter_->SeekToFirst();
    }
  }

  DBG("[RocksDBColumnFamilyStream][NextRow] while loop table={}", table_name_);
  while (row_iter_->Valid()) {
    auto const [table_name, row_key, column_qualifier] =
        db_->RawDataParseRowKey(row_iter_);
    DBG("[RocksDBColumnFamilyStream][NextRow] parsed table_name={} row_key={}",
        table_name, row_key);
    if (table_name != table_name_) {
      break;
    }

    bool in_range = false;
    for (auto const& range : row_ranges_->disjoint_ranges()) {
      if (range.IsWithin(row_key)) {
        in_range = true;
        break;
      }
    }

    if (in_range) {
      auto data = TColumnFamilyRow();
      DBG("[RocksDBColumnFamilyStream][NextRow] collecting columns table={} "
          "row_key={}",
          table_name_, row_key);
      while (row_iter_->Valid()) {
        auto const [c_table_name, c_row_key, c_column_qualifier] =
            db_->RawDataParseRowKey(row_iter_);
        DBG("[RocksDBColumnFamilyStream][NextRow] column table={} row={} cq={}",
            table_name_, row_key, c_column_qualifier);
        if (c_table_name != table_name_ || c_row_key != row_key_) {
          break;
        }
        auto maybe_row = db_->DeserializeRow(row_iter_->value().ToString());
        if (!maybe_row.ok()) {
          row_data_ = absl::nullopt;
          return;
        }
        for (auto const& i : maybe_row.value().column().cells()) {
          data[c_column_qualifier][std::chrono::milliseconds(i.first)] =
              i.second;
        }
        row_iter_->Next();
        if (!prefetch_all_columns_) {
          break;
        }
      }
      DBG("[RocksDBColumnFamilyStream][NextRow] done collection table={} "
          "row_key={} columns_count={}",
          table_name_, row_key, data.size());
      for (auto i : data) {
        DBG("[RocksDBColumnFamilyStream][NextRow] collected column={}",
            i.first);
      }
      row_data_ = std::move(data);
      row_key_ = row_key;
      return;
    }

    bool past_all_ranges = true;
    for (auto const& range : row_ranges_->disjoint_ranges()) {
      if (!range.IsAboveEnd(row_key)) {
        past_all_ranges = false;
        break;
      }
    }

    if (past_all_ranges) {
      break;
    }

    row_iter_->Next();
  }
  row_data_ = absl::nullopt;
}

bool RocksDBColumnFamilyStream::Next(NextMode mode) {
  InitializeIfNeeded();
  cur_value_.reset();
  assert(row_data_.has_value());
  assert(column_it_.value() != columns_.value().end());
  assert(cell_it_.value() != cells_.value().end());

  if (mode == NextMode::kCell) {
    ++(cell_it_.value());
    if (cell_it_.value() != cells_.value().end()) {
      return true;
    }
  }
  if (mode == NextMode::kCell || mode == NextMode::kColumn) {
    ++(column_it_.value());
    if (PointToFirstCellAfterColumnChange()) {
      return true;
    }
  }
  DBG("[RocksDBColumnFamilyStream][Next] advancing to next row table={} cf={}",
      table_name_, column_family_name_);
  NextRow();
  PointToFirstCellAfterRowChange();
  return true;
}

void RocksDBColumnFamilyStream::InitializeIfNeeded() const {
  DBG("[RocksDBColumnFamilyStream][InitializeIfNeeded] table={} cf={} "
      "initialized={}",
      table_name_, column_family_name_, initialized_);
  if (!initialized_) {
    rocksdb::ColumnFamilyHandle* cf = nullptr;
    for (auto const& i : db_->column_families_handles_map) {
      if (i.first == column_family_name_) {
        cf = i.second;
      }
    }
    DBG("[RocksDBColumnFamilyStream][InitializeIfNeeded] creating row iterator "
        "table={} cf={}",
        table_name_, column_family_name_);
    row_iter_ = db_->db->NewIterator(db_->roptions, cf);
    row_data_ = absl::nullopt;
    DBG("[RocksDBColumnFamilyStream][InitializeIfNeeded] calling NextRow");
    NextRow();
    initialized_ = true;
    PointToFirstCellAfterRowChange();
  }
}

bool RocksDBColumnFamilyStream::PointToFirstCellAfterColumnChange() const {
  for (; column_it_.value() != columns_.value().end(); ++(column_it_.value())) {
    cells_ = TimestampRangeFilteredMapView<TColumnRow>(
        column_it_.value()->second, timestamp_ranges_);
    cell_it_ = cells_.value().begin();
    if (cell_it_.value() != cells_.value().end()) {
      return true;
    }
  }
  return false;
}

bool RocksDBColumnFamilyStream::PointToFirstCellAfterRowChange() const {
  DBG("[RocksDBColumnFamilyStream][PointToFirstCellAfterRowChange] table={} "
      "cf={}",
      table_name_, column_family_name_);
  for (; row_data_.has_value(); NextRow()) {
    columns_ =
        RegexFiteredMapView<StringRangeFilteredMapView<TColumnFamilyRow>>(
            StringRangeFilteredMapView<TColumnFamilyRow>(row_data_.value(),
                                                         column_ranges_),
            column_regexes_);
    column_it_ = columns_.value().begin();
    if (PointToFirstCellAfterColumnChange()) {
      return true;
    }
  }
  return false;
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
