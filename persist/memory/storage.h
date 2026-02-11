/**
 * @file storage.h
 * @brief volatile memory-backed storage implementation for the Bigtable
 * emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_H

#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "filter.h"
#include "google/protobuf/util/field_mask_util.h"
#include "persist/utils/logging.h"
#include "persist/memory/storage_row_tx.h"
#include "persist/metadata_view.h"
#include "persist/proto/storage.pb.h"
#include "persist/storage.h"
#include "persist/storage_row_tx.h"
#include "table.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <algorithm>
#include <string_view>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

using google::protobuf::util::FieldMaskUtil;

/**
 * In-memory storage implementation for the Bigtable emulator.
 *
 * Does not offer persistence; all data is stored in a hierarchy of classes.
 * The top-level mapping is map<string, Table>.
 */
class MemoryStorage : public Storage {
 private:
  std::map<std::string, std::shared_ptr<Table>> table_by_name_;
  mutable std::mutex mu_;

  /** Looks up a table by name; returns error if not found. */
  StatusOr<std::shared_ptr<Table>> FindTable(std::string const& table_name){
    std::lock_guard<std::mutex> lock(mu_);
    auto it = table_by_name_.find(table_name);
    if (it == table_by_name_.end()) {
      LERROR("[MemoryStorage][FindTable] No such table table_name={}",
            table_name);
      return NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                                                  "table_name", table_name));
    }
    return it->second;
  };

public:
/** Default constructor for in-memory storage (no persistence, no config). */
explicit MemoryStorage() { DBG("[MemoryStorage][MemoryStorage] constructed"); }

/**
 * Returns a cached view of table metadata for tables whose names start with
 * prefix.
 * @param prefix Table name prefix; empty string returns all tables.
 * @return Cached view of matching table metadata.
 */
virtual CachedTablesMetadataView Tables(
    std::string const& prefix) const override {
  DBG("[MemoryStorage][Tables] prefix={}", prefix.empty() ? "(all)" : prefix);
  std::vector<std::tuple<std::string, storage::TableMeta>> tables_metas;
  std::transform(table_by_name_.begin(), table_by_name_.end(),
                 std::back_inserter(tables_metas), [](auto el) -> auto {
                   auto const schema = el.second->GetSchema();
                   storage::TableMeta meta;
                   *meta.mutable_table() = schema;
                   return std::make_tuple(el.first, meta);
                 });

  if (prefix.size() > 0) {
    std::vector<std::tuple<std::string, storage::TableMeta>>
        tables_metas_filtered;
    std::copy_if(tables_metas.begin(), tables_metas.end(),
                 std::back_inserter(tables_metas_filtered), [prefix](auto el) {
                   // Check if table has correct prefix
                   return std::get<0>(el).rfind(prefix, 0) == 0;
                 });
    std::swap(tables_metas_filtered, tables_metas);
  }

  return CachedTablesMetadataView(tables_metas);
}

/** Closes the storage and releases resources (no-op for in-memory storage). */
virtual Status UncheckedClose() override {
  DBG("[MemoryStorage][UncheckedClose]");
  return Status();
};

/** Starts a row-scoped transaction for the given table and row key. */
virtual StatusOr<std::unique_ptr<StorageRowTX>> RowTransaction(
    std::string const& table_name, std::string const& row_key) override {
  DBG("[MemoryStorage][RowTransaction] table={} row={}", table_name,
      row_key);
  auto maybe_table = FindTable(table_name);
  LERROR("[MemoryStorage][RowTransaction] Failed to find table table={}", table_name);
  if (!maybe_table.ok()) {
    return maybe_table.status();
  }
  return StatusOr<std::unique_ptr<StorageRowTX>>(std::unique_ptr<MemoryStorageRowTX>(
      new MemoryStorageRowTX(table_name, row_key, this, maybe_table.value())));
};

/** Opens the storage (no-op for in-memory storage). */
virtual Status UncheckedOpen(
    std::vector<std::string> additional_cf_names = {}) override {
  DBG("[MemoryStorage][UncheckedOpen] additional_cf_names.size()={}",
      additional_cf_names.size());
  return Status();
};

/** Creates a table with the given schema in memory. */
virtual Status CreateTable(
    google::bigtable::admin::v2::Table& schema) override {
  DBG("[MemoryStorage][CreateTable] name={}", schema.name());
  auto maybe_table = Table::Create(schema);
  if (!maybe_table) {
    return maybe_table.status();
  }
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto const& table_name = schema.name();
    if (!table_by_name_.emplace(table_name, *maybe_table).second) {
      LERROR("[MemoryStorage][CreateTable] Table already exists table_name={}",
             table_name);
      return google::cloud::internal::AlreadyExistsError(
          "Table already exists.",
          GCP_ERROR_INFO().WithMetadata("table_name", table_name));
    }
  }
  return Status();
};

/** Deletes a table after running the precondition. */
virtual Status DeleteTable(
    std::string table_name,
    std::function<Status(std::string, storage::TableMeta)>&& precondition_fn)
    override {
  DBG("[MemoryStorage][DeleteTable] table_name={}", table_name);
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = table_by_name_.find(table_name);
    if (it == table_by_name_.end()) {
      LERROR("[MemoryStorage][DeleteTable] No such table table_name={}",
             table_name);
      return NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                                                 "table_name", table_name));
    }
    storage::TableMeta table_meta;
    *table_meta.mutable_table() = it->second->GetSchema();
    auto const precondition_status = precondition_fn(table_name, table_meta);
    if (!precondition_status.ok()) {
      return precondition_status;
    }
    table_by_name_.erase(it);
  }
  return Status();
};
/** Returns table metadata for the given table name. */
virtual StatusOr<storage::TableMeta> GetTable(
    std::string table_name) const override {
  DBG("[MemoryStorage][GetTable] table_name={}", table_name);
  std::shared_ptr<Table> found_table;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = table_by_name_.find(table_name);
    if (it == table_by_name_.end()) {
      LERROR("[MemoryStorage][GetTable] No such table table_name={}",
             table_name);
      return NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                                                 "table_name", table_name));
    }
    found_table = it->second;
  }
  storage::TableMeta table_meta;
  *table_meta.mutable_table() = found_table->GetSchema();
  return table_meta;
};
/** Updates table metadata (e.g. change_stream_config, deletion_protection). */
virtual Status UpdateTableMetadata(std::string table_name,
                                   storage::TableMeta const& meta) override {
  DBG("[MemoryStorage][UpdateTableMetadata] table_name={}", table_name);
  auto maybe_table = FindTable(table_name);
  if (!maybe_table.ok()) {
    return maybe_table.status();
  }
  google::protobuf::FieldMask allowed_mask;
  FieldMaskUtil::FromString(
      "change_stream_config,"
      "change_stream_config.retention_period,"
      "deletion_protection",
      &allowed_mask);
  return (*maybe_table)->Update(meta.table(), allowed_mask);
};
/** Ensures the given column families exist (no-op for in-memory storage). */
virtual Status EnsureColumnFamiliesExist(
    std::vector<std::string> const& cf_names) override {
  return Status();
};
/** Returns true if the table exists. */
virtual bool HasTable(std::string table_name) const override {
  std::lock_guard<std::mutex> lock(mu_);
  return table_by_name_.find(table_name) != table_by_name_.end();
};
/** Deletes a column family (no-op for in-memory storage). */
virtual Status DeleteColumnFamily(std::string const& cf_name) override {
  return Status();
};

/** Returns a cell stream over the table for the given row set (in-memory). */
virtual StatusOr<CellStream> StreamTable(
    std::string const& table_name, std::shared_ptr<StringRangeSet> range_set,
    bool prefetch_all_columns) override {
  DBG("[MemoryStorage][StreamTable] table={} prefetch_all_columns={}",
      table_name, prefetch_all_columns);
  auto maybe_table = FindTable(table_name);
  if (!maybe_table.ok()) {
    return maybe_table.status();
  }
  return maybe_table.value()->CreateCellStream(range_set, absl::nullopt);
}

};  // namespace bigtable

}  // namespace cloud
}  // namespace google
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_MEMORY_STORAGE_H
