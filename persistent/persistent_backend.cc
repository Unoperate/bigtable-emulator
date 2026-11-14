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

#include "persistent/persistent_backend.h"
#include "google/cloud/internal/big_endian.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_format.h"
#include "column_family.h"
#include "filter.h"
#include "google/protobuf/util/field_mask_util.h"
#include "in_memory/in_memory_column_family.h"
#include "persistent/key_coder.h"
#include "persistent/persistent_column_family.h"
#include "persistent/redolog.h"
#include "range_set.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table_operations.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

namespace btadmin = ::google::bigtable::admin::v2;

namespace {
std::shared_ptr<rocksdb::ColumnFamilyHandle> AdoptHandle(
    std::shared_ptr<rocksdb::TransactionDB> db,
    rocksdb::ColumnFamilyHandle* raw) {
  return std::shared_ptr<rocksdb::ColumnFamilyHandle>(
      raw, [db = std::move(db)](rocksdb::ColumnFamilyHandle* h) {
        if (!h) {
          return;
        }
        (void)db->DestroyColumnFamilyHandle(h);
      });
}

constexpr char kSchemaKey[] = "t_emulator:meta:schema_pb";

}  // anonymous namespace

StatusOr<std::unique_ptr<PersistentBackend>> PersistentBackend::Open(
    BackendOptions const& options) {
  return std::unique_ptr<PersistentBackend>(new PersistentBackend(options));
}

StatusOr<std::vector<RestoredTable>> PersistentBackend::Restore() {
  std::vector<RestoredTable> res;
  std::error_code ec;
  std::filesystem::path root(options_.data_dir);
  if (!std::filesystem::exists(root, ec)) {
    return res;
  }

  for (auto it = std::filesystem::recursive_directory_iterator(
           root, std::filesystem::directory_options::skip_permission_denied,
           ec);
       it != std::filesystem::recursive_directory_iterator();
       it.increment(ec)) {
    if (ec) {
      ec.clear();
      continue;
    }
    if (!it->is_directory(ec)) {
      continue;
    }

    auto db_path = it->path();
    auto current = db_path / "CURRENT";
    if (!std::filesystem::exists(current, ec)) {
      continue;
    }

    it.disable_recursion_pending();

    auto table_name = db_path.lexically_relative(root).generic_string();
    btadmin::Table schema;
    schema.set_name(table_name);
    auto maybe_operations =
        PersistentTableOperations::OpenExisting(options_.data_dir, schema);
    if (!maybe_operations) {
      std::cerr << "Restore: failed to load table " << table_name << ": "
                << maybe_operations.status() << "\n";
      continue;
    }

    res.push_back(RestoredTable{std::move(table_name), std::move(schema),
                                *std::move(maybe_operations)});
  }
  return res;
}

StatusOr<std::unique_ptr<TableOperations>> PersistentBackend::CreateTable(
    btadmin::Table const& schema) {
  auto schema_copy = schema;
  return PersistentTableOperations::CreateNew(options_.data_dir, schema_copy);
}

void PersistentBackend::DestroyTable(
    std::unique_ptr<TableOperations> operations) {
  auto db_path =
      std::filesystem::path(options_.data_dir) /
      static_cast<PersistentTableOperations&>(*operations).table_name();

  /**
   * DestroyDB needs the RocksDB instance closed. Every column family, column
   * family handle and live cell stream holds a shared_ptr to the
   * TransactionDB, so close it explicitly before destroying.
   */
  operations->Close();
  operations.reset();

  rocksdb::Options options;
  auto s = rocksdb::DestroyDB(db_path.string(), options);
  std::error_code ec;
  std::filesystem::remove_all(db_path, ec);
  if (!s.ok()) {
    std::cerr << "Failed to destroy table RocksDB at " << db_path.string()
              << "; " << s.ToString() << "\n";
  }
}

StatusOr<std::unique_ptr<TableOperations>> PersistentTableOperations::CreateNew(
    std::string const& data_root, google::bigtable::admin::v2::Table& schema) {
  std::string rel = schema.name();
  std::filesystem::path db_path = std::filesystem::path(data_root) / rel;
  std::filesystem::path parent_path = db_path.parent_path();

  std::error_code ec;
  std::filesystem::create_directories(parent_path, ec);
  if (ec) {
    return InternalError(
        "failed to create directory: " + parent_path.string() +
            "; Error status: " + ec.message(),
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }

  rocksdb::Options options;
  rocksdb::TransactionDBOptions txn_options;
  txn_options.lock_mgr_handle.reset(rocksdb::NewRangeLockManager(nullptr));

  options.create_if_missing = true;
  options.create_missing_column_families = false;
  std::vector<std::string> cf_names;
  bool db_exists = std::filesystem::exists(db_path / "CURRENT");

  if (!db_exists) {
    cf_names = {rocksdb::kDefaultColumnFamilyName};
  } else {
    rocksdb::Status list_s =
        rocksdb::DB::ListColumnFamilies(options, db_path.string(), &cf_names);
    if (!list_s.ok()) {
      auto msg = "Failed to list column families for table at " +
                 db_path.string() + "; " + list_s.ToString();
      return InternalError(
          msg, GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
    }
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> descs;
  descs.reserve(cf_names.size());
  for (auto const& n : cf_names) {
    descs.emplace_back(n, rocksdb::ColumnFamilyOptions());
  }

  rocksdb::TransactionDB* raw_db = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> raw_handles;
  rocksdb::Status status = rocksdb::TransactionDB::Open(
      options, txn_options, db_path.string(), descs, &raw_handles, &raw_db);
  if (!status.ok()) {
    return InternalError(
        "failed to open rocksdb instance; " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  std::unique_ptr<PersistentTableOperations> res =
      std::make_unique<PersistentTableOperations>();
  res->db_.reset(raw_db);
  res->table_name_ = schema.name();

  std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>>
      handles_by_name;
  for (std::size_t i = 0; i < descs.size(); ++i) {
    handles_by_name[descs[i].name] = AdoptHandle(res->db_, raw_handles[i]);
  }

  auto maybe_schema = res->LoadSchema();
  if (maybe_schema.ok()) {
    schema = std::move(maybe_schema.value());
  } else if (maybe_schema.status().code() == StatusCode::kNotFound) {
    auto st = res->PersistSchema(schema);
    if (!st.ok()) {
      return st;
    }
  } else {
    return maybe_schema.status();
  }

  for (auto const& cfd : schema.column_families()) {
    absl::optional<google::bigtable::admin::v2::Type> opt_value_type =
        absl::nullopt;
    if (cfd.second.has_value_type()) {
      opt_value_type = cfd.second.value_type();
    }

    if (opt_value_type.has_value()) {
      auto new_cf = PersistentColumnFamily::ConstructAggregateColumnFamily(
          opt_value_type.value(), res->db_, cfd.first);
      if (!new_cf) {
        return new_cf.status();
      }
      res->column_families_.emplace(cfd.first, new_cf.value());
      handles_by_name[cfd.first] = new_cf.value()->GetHandle();
    } else {
      rocksdb::ColumnFamilyOptions opts;
      auto maybe_new_cf =
          PersistentColumnFamily::Create(res->db_, opts, cfd.first);
      if (!maybe_new_cf.ok()) {
        return InternalError(
            "failed to create column family " + cfd.first +
                "; Error status: " + maybe_new_cf.status().message(),
            GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
      }
      res->column_families_.emplace(cfd.first, maybe_new_cf.value());
      handles_by_name[cfd.first] = maybe_new_cf.value()->GetHandle();
    }
  }

  {
    SchemaRedoLog redo(res->db_);
    auto pending = redo.Load();
    if (pending.ok()) {
      auto st = res->ReconcileColumnFamiliesToTarget(pending.value());
      if (!st.ok()) {
        return st;
      }

      st = res->PersistSchema(pending.value());
      if (!st.ok()) {
        return st;
      }

      st = redo.Finish();
      if (!st.ok()) {
        return st;
      }

      schema = pending.value();
    } else if (pending.status().code() != StatusCode::kNotFound) {
      return pending.status();
    }
  }
  return StatusOr<std::unique_ptr<TableOperations>>(std::move(res));
}

StatusOr<std::unique_ptr<TableOperations>>
PersistentTableOperations::OpenExisting(
    std::string const& data_root, google::bigtable::admin::v2::Table& schema) {
  bool const create_if_missing = false;

  std::string rel = schema.name();
  std::filesystem::path db_path = std::filesystem::path(data_root) / rel;
  std::filesystem::path parent_path = db_path.parent_path();

  std::error_code ec;
  std::filesystem::create_directories(parent_path, ec);
  if (ec) {
    return InternalError(
        "failed to create directory: " + parent_path.string() +
            "; Error status: " + ec.message(),
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }

  rocksdb::Options options;
  rocksdb::TransactionDBOptions txn_options;
  txn_options.lock_mgr_handle.reset(rocksdb::NewRangeLockManager(nullptr));

  options.create_if_missing = create_if_missing;
  options.create_missing_column_families = false;

  std::vector<std::string> cf_names;
  bool db_exists = std::filesystem::exists(db_path / "CURRENT");

  if (!db_exists) {
    return NotFoundError(
        "No such table; database directory not initialized.",
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  rocksdb::Status list_s =
      rocksdb::DB::ListColumnFamilies(options, db_path.string(), &cf_names);
  if (!list_s.ok()) {
    auto msg = "Failed to list column families for table at " +
               db_path.string() + "; " + list_s.ToString();
    return InternalError(
        msg, GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> descs;
  descs.reserve(cf_names.size());
  for (auto const& n : cf_names) {
    descs.emplace_back(n, rocksdb::ColumnFamilyOptions());
  }

  rocksdb::TransactionDB* raw_db = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> raw_handles;
  rocksdb::Status status = rocksdb::TransactionDB::Open(
      options, txn_options, db_path.string(), descs, &raw_handles, &raw_db);
  if (!status.ok()) {
    return NotFoundError(
        "No such table; " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  std::unique_ptr<PersistentTableOperations> res =
      std::make_unique<PersistentTableOperations>();
  res->db_.reset(raw_db);
  res->table_name_ = schema.name();

  std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>>
      handles_by_name;
  for (std::size_t i = 0; i < descs.size(); ++i) {
    handles_by_name[descs[i].name] = AdoptHandle(res->db_, raw_handles[i]);
  }

  auto maybe_schema = res->LoadSchema();
  if (!maybe_schema.ok()) {
    return maybe_schema.status();
  }
  schema = std::move(maybe_schema.value());

  for (auto const& cfd : schema.column_families()) {
    absl::optional<google::bigtable::admin::v2::Type> opt_value_type =
        absl::nullopt;
    if (cfd.second.has_value_type()) {
      opt_value_type = cfd.second.value_type();
    }

    auto hit = handles_by_name.find(cfd.first);
    if (hit != handles_by_name.end()) {
      auto maybe_cf = PersistentColumnFamily::OpenExisting(
          res->db_, hit->second, opt_value_type);
      if (!maybe_cf) {
        return maybe_cf.status();
      }
      res->column_families_.emplace(cfd.first, maybe_cf.value());
      continue;
    }

    if (opt_value_type.has_value()) {
      auto new_cf = PersistentColumnFamily::ConstructAggregateColumnFamily(
          opt_value_type.value(), res->db_, cfd.first);
      if (!new_cf) {
        return new_cf.status();
      }
      res->column_families_.emplace(cfd.first, new_cf.value());
      handles_by_name[cfd.first] = new_cf.value()->GetHandle();
    } else {
      rocksdb::ColumnFamilyOptions opts;
      auto maybe_new_cf =
          PersistentColumnFamily::Create(res->db_, opts, cfd.first);
      if (!maybe_new_cf.ok()) {
        return InternalError(
            "failed to create column family " + cfd.first +
                "; Error status: " + maybe_new_cf.status().message(),
            GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
      }
      res->column_families_.emplace(cfd.first, maybe_new_cf.value());
      handles_by_name[cfd.first] = maybe_new_cf.value()->GetHandle();
    }
  }

  {
    SchemaRedoLog redo(res->db_);
    auto pending = redo.Load();
    if (pending.ok()) {
      auto st = res->ReconcileColumnFamiliesToTarget(pending.value());
      if (!st.ok()) {
        return st;
      }

      st = res->PersistSchema(pending.value());
      if (!st.ok()) {
        return st;
      }

      st = redo.Finish();
      if (!st.ok()) {
        return st;
      }

      schema = pending.value();
    } else if (pending.status().code() != StatusCode::kNotFound) {
      return pending.status();
    }
  }

  return StatusOr<std::unique_ptr<TableOperations>>(std::move(res));
}

StatusOr<std::unique_ptr<RowTransaction>>
PersistentTableOperations::NewRowTransaction(std::string const& row_key,
                                             Deadline /*deadline*/) {
  return std::unique_ptr<RowTransaction>(
      std::make_unique<PersistentRowTransaction>(*this, row_key, db_.get()));
}

StatusOr<std::vector<RowKeySample>> PersistentTableOperations::SampleRowKeys() {
  return SampleRowKeysByScanning(*this,
                                 [this] { return GetRowCountEstimate(); });
}

StatusOr<CellStream> PersistentTableOperations::CreateCellStream(
    std::shared_ptr<StringRangeSet> range_set,
    absl::optional<google::bigtable::v2::RowFilter> maybe_row_filter) const {
  auto snapshot = std::make_shared<rocksdb::ManagedSnapshot>(db_.get());
  return CreateFilteredTableStream(
      [range_set = std::move(range_set), snapshot = std::move(snapshot), this] {
        std::vector<std::unique_ptr<FilteredColumnFamilyStream>> per_cf_streams;
        per_cf_streams.reserve(column_families_.size());
        for (auto const& handle : column_families_) {
          auto stream = handle.second->GetFilteredColumnFamilyStream(
              range_set, handle.first, snapshot);
          per_cf_streams.emplace_back(std::move(stream));
        }
        return per_cf_streams;
      },
      maybe_row_filter);
}

StatusOr<std::size_t> PersistentTableOperations::GetRowCountEstimate() const {
  // Just iterate all rows and count them.
  auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
  auto maybe_all_rows_stream = CreateCellStream(all_rows_set, absl::nullopt);
  if (!maybe_all_rows_stream) {
    return maybe_all_rows_stream.status();
  }

  auto& stream = *maybe_all_rows_stream;
  std::size_t res = 0;
  while (stream.HasValue()) {
    ++stream;
    ++res;
  }
  return res;
}

Status PersistentTableOperations::RemoveAllDataFromColumnFamilies() {
  return DropRowRange("");
}

Status PersistentTableOperations::DropRowRange(
    std::string const& row_key_prefix) {
  auto const prefix = KeyCoder::EscapedRow(row_key_prefix);
  auto txn = std::unique_ptr<rocksdb::Transaction>(
      db_->BeginTransaction(rocksdb::WriteOptions()));
  rocksdb::Endpoint start(prefix, false);
  rocksdb::Endpoint end(prefix, true);
  rocksdb::Status status;

  // 1. Lock the specified range in every column family
  for (auto& cf : column_families_) {
    status = txn->GetRangeLock(cf.second->GetRaw(), start, end);
    if (!status.ok()) {
      return InternalError(
          "Failed to drop row range: " + status.ToString(),
          GCP_ERROR_INFO().WithMetadata("row key prefix", row_key_prefix));
    }
  }

  // 2. Delete specified (and locked) range in each column family.
  for (auto& cf : column_families_) {
    auto cf_it = std::unique_ptr<rocksdb::Iterator>(
        txn->GetIterator(rocksdb::ReadOptions(), cf.second->GetRaw()));
    cf_it->Seek(prefix);
    while (cf_it->Valid() && cf_it->key().starts_with(prefix)) {
      status = txn->Delete(cf.second->GetRaw(), cf_it->key());
      if (!status.ok()) {
        txn->Rollback();
        return InternalError(
            "Failed to drop row range: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key prefix", row_key_prefix));
      }
      cf_it->Next();
    }
  }

  status = txn->Commit();
  if (!status.ok()) {
    return InternalError(
        "Failed to drop row range: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("row key prefix", row_key_prefix));
  }
  return Status();
}

StatusOr<google::bigtable::admin::v2::Table>
PersistentTableOperations::ModifyColumnFamilies(
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
    google::bigtable::admin::v2::Table schema) {
  {
    SchemaRedoLog redo(db_);
    auto pending = redo.Load();
    if (pending.ok()) {
      auto st = ReconcileColumnFamiliesToTarget(pending.value());
      if (!st.ok()) {
        return st;
      }

      st = PersistSchema(pending.value());
      if (!st.ok()) {
        return st;
      }

      st = redo.Finish();
      if (!st.ok()) {
        return st;
      }

      schema = pending.value();
    } else if (pending.status().code() != StatusCode::kNotFound) {
      return pending.status();
    }
  }

  auto maybe_target = ApplyModifyColumnFamiliesToSchemaOnly(request, schema);
  if (!maybe_target) {
    return maybe_target.status();
  }
  auto target_schema = maybe_target.value();

  SchemaRedoLog redo(db_);
  auto st = redo.Begin(target_schema);
  if (!st.ok()) {
    return st;
  }

  st = ReconcileColumnFamiliesToTarget(target_schema);
  if (!st.ok()) {
    return st;
  }

  st = PersistSchema(target_schema);
  if (!st.ok()) {
    return st;
  }

  st = redo.Finish();
  if (!st.ok()) {
    return st;
  }

  return target_schema;
}

Status PersistentTableOperations::ReconcileColumnFamiliesToTarget(
    btadmin::Table const& target_schema) {
  for (auto const& [cf_name, cf_def] : target_schema.column_families()) {
    if (column_families_.find(cf_name) != column_families_.end()) {
      continue;
    }

    std::shared_ptr<PersistentColumnFamily> cf;
    if (cf_def.has_value_type()) {
      auto maybe_cf = PersistentColumnFamily::ConstructAggregateColumnFamily(
          cf_def.value_type(), db_, cf_name);
      if (!maybe_cf) {
        return maybe_cf.status();
      }
      cf = std::move(maybe_cf.value());
    } else {
      auto maybe_cf = PersistentColumnFamily::Create(
          db_, rocksdb::ColumnFamilyOptions(), cf_name);
      if (!maybe_cf.ok()) {
        return InternalError(
            "Failed to create CF: " + maybe_cf.status().message(),
            GCP_ERROR_INFO().WithMetadata("cf", cf_name));
      }
      cf = maybe_cf.value();
    }
    column_families_.emplace(cf_name, std::move(cf));
  }

  for (auto it = column_families_.begin(); it != column_families_.end();) {
    auto const& name = it->first;
    if (target_schema.column_families().find(name) !=
        target_schema.column_families().end()) {
      ++it;
      continue;
    }

    auto handle = it->second->GetHandle();
    rocksdb::Status s = db_->DropColumnFamily(handle.get());
    if (!s.ok()) {
      return InternalError("Failed to drop CF in RocksDB: " + s.ToString(),
                           GCP_ERROR_INFO().WithMetadata("cf", name));
    }
    it = column_families_.erase(it);
  }

  return Status();
}

Status PersistentTableOperations::PersistSchema(
    google::bigtable::admin::v2::Table const& schema) {
  std::string bytes;
  if (!schema.SerializeToString(&bytes)) {
    return InternalError(
        "Failed to serialize table schema",
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }

  rocksdb::WriteOptions wopts;
  auto* default_cf = db_->DefaultColumnFamily();

  rocksdb::Status s = db_->Put(wopts, default_cf, kSchemaKey, bytes);
  if (!s.ok()) {
    return InternalError("Failed to persist schema in RocksDB: " + s.ToString(),
                         GCP_ERROR_INFO()
                             .WithMetadata("table", table_name_)
                             .WithMetadata("key", kSchemaKey));
  }
  return Status();
}

StatusOr<google::bigtable::admin::v2::Table>
PersistentTableOperations::LoadSchema() const {
  std::string serialized;

  rocksdb::ReadOptions ro;
  rocksdb::Status s =
      db_->Get(ro, db_->DefaultColumnFamily(), kSchemaKey, &serialized);

  if (s.IsNotFound()) {
    return NotFoundError("Persisted schema not found in default CF.",
                         GCP_ERROR_INFO().WithMetadata("key", kSchemaKey));
  }
  if (!s.ok()) {
    return InternalError("Failed to read persisted schema: " + s.ToString(),
                         GCP_ERROR_INFO().WithMetadata("key", kSchemaKey));
  }

  google::bigtable::admin::v2::Table schema;
  if (!schema.ParseFromString(serialized)) {
    return InternalError("Failed to parse persisted schema proto.",
                         GCP_ERROR_INFO().WithMetadata("key", kSchemaKey));
  }

  return schema;
}

Status PersistentRowTransaction::Commit() {
  rocksdb::Status status = txn_->Commit();
  if (!status.ok()) {
    txn_->Rollback();
    return InvalidArgumentError(
        "Failed to commit the transaction: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("row key", row_key_));
  }
  return Status();
}

Status PersistentRowTransaction::SetCell(
    ::google::bigtable::v2::Mutation_SetCell const& set_cell,
    absl::optional<std::chrono::microseconds> timestamp_override) {
  auto maybe_column_family = utilities_.FindColumnFamily(set_cell);
  if (!maybe_column_family) {
    return maybe_column_family.status();
  }

  auto const& column_family = maybe_column_family.value();

  auto const timestamp = timestamp_override.value_or(
      std::chrono::microseconds(set_cell.timestamp_micros()));

  std::string prepared_key =
      KeyCoder::Encode(row_key_, set_cell.column_qualifier(),
                       static_cast<std::uint64_t>(timestamp.count()));
  rocksdb::Status status =
      txn_->Put(column_family->GetRaw(), prepared_key, set_cell.value());

  if (!status.ok()) {
    return InternalError(
        "Failed to put write into the transaction; " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("column_family",
                                      column_family->GetRaw()->GetName()));
  }
  return Status();
}

Status PersistentRowTransaction::MergeToCell(
    ::google::bigtable::v2::Mutation_MergeToCell const& merge_to_cell) {
  return UnimplementedError(
      "Unsupported mutation type.",
      GCP_ERROR_INFO().WithMetadata("mutation", merge_to_cell.DebugString()));
}

StatusOr<bool> PersistentRowTransaction::RowMatches(
    absl::optional<btproto::RowFilter> const& filter) {
  return RowHasMatchingCells(utilities_, row_key_, filter);
}

Status PersistentRowTransaction::AddToCell(
    ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
    absl::optional<std::chrono::microseconds> timestamp_override) {
  auto find_status = utilities_.FindColumnFamily(add_to_cell);
  if (!find_status.ok()) {
    return find_status.status();
  }

  auto const& cf = find_status.value();
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

  rocksdb::ColumnFamilyHandle* raw_cf = cf->GetRaw();

  // TODO(gustaw): this should be removed when RowTransactions are fixed.
  // First, the locking does not belong here, but to MutateRow/s.
  // Second, The whole row should be locked, not only
  // the specific column inside it.
  std::string const column_prefix = KeyCoder::ColumnPrefix(
      row_key_, add_to_cell.column_qualifier().raw_value());
  rocksdb::Endpoint start(column_prefix, false);
  rocksdb::Endpoint end(column_prefix, true);

  rocksdb::Status status = txn_->GetRangeLock(raw_cf, start, end);
  if (!status.ok()) {
    return InternalError(
        "Failed to add to cell: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }

  // TODO(gustaw): this merges into the newest cell of the column regardless of
  // its timestamp and deletes it. It should merge only into the cell with the
  // same timestamp (like the in-memory implementation), and create a new cell
  // otherwise.
  std::unique_ptr<rocksdb::Iterator> it(
      txn_->GetIterator(rocksdb::ReadOptions(), raw_cf));
  it->Seek(column_prefix);
  std::string new_value = value;
  if (it->Valid() && it->key().starts_with(column_prefix)) {
    auto maybe_result =
        cf->update_cell_(it->value().ToString(), std::move(value));
    if (!maybe_result) {
      return InternalError(
          "Failed to add to cell: " + maybe_result.status().message(),
          GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
    }
    new_value = maybe_result.value();
  }

  std::string new_key =
      KeyCoder::Encode(row_key_, add_to_cell.column_qualifier().raw_value(),
                       static_cast<std::uint64_t>(timestamp.count()));
  status = txn_->Put(raw_cf, new_key, std::move(new_value));
  if (!status.ok()) {
    return InternalError(
        "Failed to add to cell: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }
  if (it->Valid() && it->key().starts_with(column_prefix) &&
      it->key() != new_key) {
    status = txn_->Delete(raw_cf, it->key());
    if (!status.ok()) {
      return InternalError(
          "Failed to add to cell: " + status.ToString(),
          GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
    }
  }

  return Status();
}

Status PersistentRowTransaction::DeleteFromColumn(
    ::google::bigtable::v2::Mutation_DeleteFromColumn const&
        delete_from_column) {
  auto maybe_column_family = utilities_.FindColumnFamily(delete_from_column);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  uint64_t start_count = std::numeric_limits<uint64_t>::max();
  uint64_t end_count = std::numeric_limits<uint64_t>::min();
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

    start_count = start.count();
    if (delete_from_column.time_range().end_timestamp_micros() == 0) {
      end_count = std::numeric_limits<uint64_t>::max();
    } else {
      end_count = end.count();
    }
  }

  // The idea here is to iterate over each row, and for each row
  // lock the specified col+timestamp range

  std::vector<std::string> starts;
  std::vector<std::string> ends;

  // TODO(gustaw): this walks every row of the column family and deletes the
  // column in all of them, not only in row_key_.
  // 1. Acquire locks
  auto const& column_family = maybe_column_family.value();
  auto cf_it = std::unique_ptr<rocksdb::Iterator>(
      txn_->GetIterator(rocksdb::ReadOptions(), column_family->GetRaw()));
  cf_it->SeekToFirst();
  while (cf_it->Valid()) {
    auto maybe_decoded = KeyCoder::Decode(
        std::string_view(cf_it->key().data(), cf_it->key().size()));
    if (!maybe_decoded) {
      return InternalError(
          "Failed to delete from column: " + maybe_decoded.status().message(),
          GCP_ERROR_INFO());
    }
    auto const& decoded = maybe_decoded.value();

    // Remember that we keep timestamps reversed; that's why start_key
    // contains the end_count.
    auto start_key = KeyCoder::Encode(
        decoded.row, delete_from_column.column_qualifier(), end_count);
    auto end_key = KeyCoder::Encode(
        decoded.row, delete_from_column.column_qualifier(), start_count);
    rocksdb::Endpoint start(start_key, false);
    rocksdb::Endpoint end(end_key, false);
    rocksdb::Status status =
        txn_->GetRangeLock(column_family->GetRaw(), start, end);
    if (!status.ok()) {
      return InternalError("Failed to delete from column: " + status.ToString(),
                           GCP_ERROR_INFO());
    }

    starts.push_back(start_key);
    ends.push_back(end_key);

    cf_it->Seek(KeyCoder::NextRow(decoded.row));
  }

  // 2. Now that things to delete are locked, we can delete them
  for (size_t i = 0; i < starts.size(); ++i) {
    cf_it->Seek(starts[i]);
    while (cf_it->Valid()) {
      if (cf_it->key().ToString() > ends[i]) {
        break;
      }
      auto status = txn_->Delete(column_family->GetRaw(), cf_it->key());
      if (!status.ok()) {
        return InternalError(
            "Failed to delete from column: " + status.ToString(),
            GCP_ERROR_INFO());
      }
      cf_it->Next();
    }
    cf_it->Refresh();
  }
  return Status();
}

Status PersistentRowTransaction::DeleteFromRow() {
  bool row_existed = false;
  std::string const row_prefix = KeyCoder::RowPrefix(row_key_);
  rocksdb::Endpoint start(row_prefix, false);
  rocksdb::Endpoint end(row_prefix, true);

  for (auto& column_family : utilities_) {
    // TODO(gustaw): Move locking out of the mutations.
    rocksdb::Status status =
        txn_->GetRangeLock(column_family.second->GetRaw(), start, end);
    if (!status.ok()) {
      return InternalError("Failed to delete from row: " + status.ToString(),
                           GCP_ERROR_INFO().WithMetadata("row key", row_key_));
    }

    auto cf_it = std::unique_ptr<rocksdb::Iterator>(txn_->GetIterator(
        rocksdb::ReadOptions(), column_family.second->GetRaw()));
    cf_it->Seek(row_prefix);
    if (!cf_it->Valid() || !cf_it->key().starts_with(row_prefix)) {
      continue;
    }
    row_existed = true;

    std::vector<std::string> keys_to_delete;
    while (cf_it->Valid() && cf_it->key().starts_with(row_prefix)) {
      keys_to_delete.push_back(cf_it->key().ToString());
      cf_it->Next();
    }
    for (auto const& key : keys_to_delete) {
      status = txn_->Delete(column_family.second->GetRaw(), key);
      if (!status.ok()) {
        return InternalError(
            "Failed to delete from row: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key", row_key_));
      }
    }
  }

  if (row_existed) {
    return Status();
  }

  return NotFoundError("row not found in table",
                       GCP_ERROR_INFO().WithMetadata("row", row_key_));
}

Status PersistentRowTransaction::DeleteFromFamily(
    ::google::bigtable::v2::Mutation_DeleteFromFamily const&
        delete_from_family) {
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

  auto const& column_family = column_family_it->second;
  std::string const row_prefix = KeyCoder::RowPrefix(row_key_);
  rocksdb::Endpoint start(row_prefix, false);
  rocksdb::Endpoint end(row_prefix, true);

  // TODO(gustaw): Move locking out of mutations.
  rocksdb::Status status =
      txn_->GetRangeLock(column_family->GetRaw(), start, end);
  if (!status.ok()) {
    return InternalError(
        "Failed to delete from family: " + status.ToString(),
        GCP_ERROR_INFO()
            .WithMetadata("row key", row_key_)
            .WithMetadata("column family", delete_from_family.family_name()));
  }

  auto cf_it = std::unique_ptr<rocksdb::Iterator>(
      txn_->GetIterator(rocksdb::ReadOptions(), column_family->GetRaw()));
  cf_it->Seek(row_prefix);
  if (!cf_it->Valid() || !cf_it->key().starts_with(row_prefix)) {
    return NotFoundError(
        "row key is not found in column family",
        GCP_ERROR_INFO()
            .WithMetadata("row key", row_key_)
            .WithMetadata("column family", column_family_it->first));
  }

  std::vector<std::string> keys_to_delete;
  while (cf_it->Valid() && cf_it->key().starts_with(row_prefix)) {
    keys_to_delete.push_back(cf_it->key().ToString());
    cf_it->Next();
  }
  for (auto const& key : keys_to_delete) {
    status = txn_->Delete(column_family->GetRaw(), key);
    if (!status.ok()) {
      return InternalError(
          "Failed to delete from family: " + status.ToString(),
          GCP_ERROR_INFO()
              .WithMetadata("row key", row_key_)
              .WithMetadata("column family", delete_from_family.family_name()));
    }
  }

  return Status();
}

StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
PersistentRowTransaction::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request) {
  if (row_key_.empty()) {
    return InvalidArgumentError(
        "row key not set",
        GCP_ERROR_INFO().WithMetadata("request", request.DebugString()));
  }
  // Copying behaviour from the InMemoryRowTransaction.
  // In this case, InMemoryColumnFamily is a perfect storage
  // for the partial results, and it lets us reuse some logic
  // from the InMemoryRowTransaction.
  std::map<std::string, InMemoryColumnFamily> tmp_families;

  for (auto const& rule : request.rules()) {
    auto maybe_column_family = utilities_.FindColumnFamily(rule);
    if (!maybe_column_family) {
      return maybe_column_family.status();
    }

    if (!rule.has_increment_amount() && !rule.has_append_value()) {
      return InvalidArgumentError(
          "either append value or increment amount must be set",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }

    auto const& column_family = maybe_column_family.value();
    std::string const column_prefix =
        KeyCoder::ColumnPrefix(request.row_key(), rule.column_qualifier());

    // 1. Lock the key range
    // TODO(gustaw): Move locking out of mutations.
    rocksdb::Endpoint start(column_prefix, false);
    rocksdb::Endpoint end(column_prefix, true);
    rocksdb::Status status =
        txn_->GetRangeLock(column_family->GetRaw(), start, end);
    if (!status.ok()) {
      return InternalError(
          "Failed to read modify row: " + status.ToString(),
          GCP_ERROR_INFO().WithMetadata("row key prefix", column_prefix));
    }

    // 2. Acquire iterator to the first value in range (if exists)
    auto cf_it = std::unique_ptr<rocksdb::Iterator>(
        txn_->GetIterator(rocksdb::ReadOptions(), column_family->GetRaw()));
    cf_it->Seek(column_prefix);

    // 3. Main logic
    auto const system_time = NowTruncatedToMillis();
    auto const system_micros = static_cast<uint64_t>(system_time.count());
    if (!cf_it->Valid()) {
      std::string value;
      if (rule.has_append_value()) {
        value = rule.append_value();
      } else {
        value =
            google::cloud::internal::EncodeBigEndian(rule.increment_amount());
      }
      txn_->Put(column_family->GetRaw(),
                KeyCoder::Encode(request.row_key(), rule.column_qualifier(),
                                 system_micros),
                value);

      auto result_timestamp = system_time;
      tmp_families[rule.family_name()].SetCell(
          request.row_key(), rule.column_qualifier(), result_timestamp,
          std::move(value));
      continue;
    }

    auto maybe_decoded = KeyCoder::Decode(
        std::string_view(cf_it->key().data(), cf_it->key().size()));
    if (!maybe_decoded) {
      return InvalidArgumentError(
          "either append value or increment amount must be set",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }
    auto const& decoded_key = maybe_decoded.value();
    // TODO(gustaw): for an increment rule, this writes append_value instead of
    // the increment amount.
    if (decoded_key.row != request.row_key() ||
        decoded_key.col != rule.column_qualifier()) {
      auto result_timestamp = system_time;
      tmp_families[rule.family_name()].SetCell(
          request.row_key(), rule.column_qualifier(), result_timestamp,
          rule.append_value());
      txn_->Put(column_family->GetRaw(),
                KeyCoder::Encode(request.row_key(), rule.column_qualifier(),
                                 system_micros),
                rule.append_value());
      continue;
    }

    std::string prev_value;
    status = txn_->Get(rocksdb::ReadOptions(), column_family->GetRaw(),
                       cf_it->key(), &prev_value);
    if (!status.ok()) {
      return InternalError(
          "Failed to read modify row: " + status.ToString(),
          GCP_ERROR_INFO().WithMetadata("row key prefix", column_prefix));
    }

    std::string value;
    if (rule.has_append_value()) {
      value = prev_value + rule.append_value();
    } else {  // has increment value
      auto maybe_prev_value_int =
          google::cloud::internal::DecodeBigEndian<std::int64_t>(prev_value);
      // TODO(gustaw): this returns OK when the existing value is not a 64-bit
      // big-endian integer.
      if (!maybe_prev_value_int) {
        return maybe_column_family.status();
      }
      value = google::cloud::internal::EncodeBigEndian(
          rule.increment_amount() + maybe_prev_value_int.value());
    }

    auto result_timestamp = system_time;
    if (decoded_key.timestamp > system_micros) {
      result_timestamp = std::chrono::microseconds(decoded_key.timestamp);
      status = txn_->Delete(column_family->GetRaw(), cf_it->key());
      if (!status.ok()) {
        return InternalError(
            "Failed to read modify row: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key prefix", column_prefix));
      }
      status = txn_->Put(column_family->GetRaw(), cf_it->key(), value);
      if (!status.ok()) {
        return InternalError(
            "Failed to read modify row: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key prefix", column_prefix));
      }
    } else {
      std::string new_key = KeyCoder::Encode(
          request.row_key(), rule.column_qualifier(), system_micros);
      status = txn_->Put(column_family->GetRaw(), new_key, value);
      if (!status.ok()) {
        return InternalError(
            "Failed to read modify row: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key prefix", column_prefix));
      }
    }

    tmp_families[rule.family_name()].SetCell(
        request.row_key(), rule.column_qualifier(), result_timestamp,
        std::move(value));
  }

  return FamiliesToReadModifyWriteResponse(row_key_, tmp_families);
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
