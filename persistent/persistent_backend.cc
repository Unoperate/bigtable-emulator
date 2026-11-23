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
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "column_family.h"
#include "filter.h"
#include "google/protobuf/util/field_mask_util.h"
#include "in_memory/in_memory_column_family.h"
#include "persistent/key_coder.h"
#include "persistent/persistent_column_family.h"
#include "persistent/table_metadata.pb.h"
#include "range_set.h"
#include "rocksdb/env.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table_operations.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_set>
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

constexpr char kTableMetadataKey[] = "t_emulator:meta:table_metadata_pb";

constexpr std::size_t kBlockCacheBytes = 256 << 20;
constexpr std::size_t kWriteBufferBytes = 128 << 20;

rocksdb::ColumnFamilyOptions MakeColumnFamilyOptions(
    SharedRocksDbMemory const& memory) {
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = memory.block_cache;
  table_options.cache_index_and_filter_blocks = true;
  rocksdb::ColumnFamilyOptions options;
  options.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options));
  options.max_write_buffer_size_to_maintain = 1;
  return options;
}

rocksdb::Options MakeDatabaseOptions(SharedRocksDbMemory const& memory) {
  rocksdb::DBOptions db_options;
  db_options.write_buffer_manager = memory.write_buffer_manager;
  return rocksdb::Options(db_options, MakeColumnFamilyOptions(memory));
}

struct OpenedDatabase {
  std::shared_ptr<rocksdb::TransactionDB> db;
  std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>>
      handles_by_name;
};

/// Opens the database in `db_path` with every column family it has.
StatusOr<OpenedDatabase> OpenDatabase(std::filesystem::path const& db_path,
                                      SharedRocksDbMemory const& memory) {
  rocksdb::Options options = MakeDatabaseOptions(memory);
  rocksdb::TransactionDBOptions txn_options;
  txn_options.lock_mgr_handle.reset(rocksdb::NewRangeLockManager(nullptr));

  std::vector<std::string> names;
  std::vector<rocksdb::ColumnFamilyDescriptor> descs;
  std::vector<rocksdb::ColumnFamilyHandle*> raw_handles;
  rocksdb::TransactionDB* raw_db = nullptr;
  auto status =
      rocksdb::DB::ListColumnFamilies(options, db_path.string(), &names);
  if (status.ok()) {
    for (auto const& name : names) {
      descs.emplace_back(name, MakeColumnFamilyOptions(memory));
    }
    status = rocksdb::TransactionDB::Open(
        options, txn_options, db_path.string(), descs, &raw_handles, &raw_db);
  }
  if (!status.ok()) {
    auto message = "Failed to open the table's database: " + status.ToString();
    if (status.IsCorruption()) {
      return DataLossError(std::move(message), GCP_ERROR_INFO().WithMetadata(
                                                   "path", db_path.string()));
    }
    return InternalError(std::move(message), GCP_ERROR_INFO().WithMetadata(
                                                 "path", db_path.string()));
  }

  OpenedDatabase opened;
  opened.db.reset(raw_db);
  for (std::size_t i = 0; i < descs.size(); ++i) {
    opened.handles_by_name[descs[i].name] =
        AdoptHandle(opened.db, raw_handles[i]);
  }
  return opened;
}

StatusOr<absl::optional<TableMetadata>> ReadMetadata(
    rocksdb::TransactionDB& db) {
  std::string serialized;
  rocksdb::Status s = db.Get(rocksdb::ReadOptions(), db.DefaultColumnFamily(),
                             kTableMetadataKey, &serialized);
  if (s.IsNotFound()) {
    return absl::optional<TableMetadata>();
  }
  if (!s.ok()) {
    return InternalError(
        "Failed to read table metadata: " + s.ToString(),
        GCP_ERROR_INFO().WithMetadata("key", kTableMetadataKey));
  }
  TableMetadata metadata;
  if (!metadata.ParseFromString(serialized)) {
    return DataLossError(
        "Failed to parse table metadata.",
        GCP_ERROR_INFO().WithMetadata("key", kTableMetadataKey));
  }
  return absl::optional<TableMetadata>(std::move(metadata));
}

Status SyncDirectory(std::filesystem::path const& path) {
  std::unique_ptr<rocksdb::Directory> dir;
  auto s = rocksdb::Env::Default()->NewDirectory(path.string(), &dir);
  if (s.ok()) {
    s = dir->Fsync();
  }
  if (!s.ok()) {
    return InternalError("Failed to sync directory: " + s.ToString(),
                         GCP_ERROR_INFO().WithMetadata("path", path.string()));
  }
  return Status();
}

/// Destroys the closed database in `db_path` and removes the directory.
Status DestroyDirectory(std::filesystem::path const& db_path) {
  std::error_code ec;
  std::filesystem::remove(db_path / "CURRENT", ec);
  if (ec) {
    return InternalError(
        "Failed to remove the table's CURRENT file: " + ec.message(),
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }
  auto status = SyncDirectory(db_path);
  if (!status.ok()) {
    return status;
  }
  auto s = rocksdb::DestroyDB(db_path.string(), rocksdb::Options());
  if (!s.ok()) {
    return InternalError(
        "Failed to destroy the table's database: " + s.ToString(),
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }
  std::filesystem::remove_all(db_path, ec);
  if (ec) {
    return InternalError(
        "Failed to remove the table directory: " + ec.message(),
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }
  return Status();
}

Status WithTablePath(Status const& status,
                     std::filesystem::path const& db_path) {
  auto info = GCP_ERROR_INFO().WithMetadata("path", db_path.string());
  for (auto const& [key, value] : status.error_info().metadata()) {
    info.WithMetadata(key, value);
  }
  return Status(status.code(), status.message(),
                std::move(info).Build(status.code()));
}

/// Lists the entries of `<data_dir>/tables` named by a table id; logs and
/// skips the others. A missing directory holds no tables.
StatusOr<std::vector<std::filesystem::path>> ListTableDirectories(
    std::string const& data_dir) {
  std::vector<std::filesystem::path> res;
  auto const tables_dir = std::filesystem::path(data_dir) / "tables";
  std::error_code ec;
  for (auto it = std::filesystem::directory_iterator(tables_dir, ec);
       !ec && it != std::filesystem::directory_iterator(); it.increment(ec)) {
    auto const name = it->path().filename().string();
    std::uint64_t id = 0;
    if (!absl::SimpleAtoi(name, &id) || absl::StrFormat("%06d", id) != name) {
      std::cerr << "Skipping unexpected entry in the tables directory: "
                << it->path().string() << "\n";
      continue;
    }
    res.push_back(it->path());
  }
  if (ec && ec != std::errc::no_such_file_or_directory) {
    return InternalError(
        "Failed to read the tables directory: " + ec.message(),
        GCP_ERROR_INFO().WithMetadata("path", tables_dir.string()));
  }
  return res;
}

}  // anonymous namespace

SharedRocksDbMemory MakeSharedRocksDbMemory() {
  SharedRocksDbMemory memory;
  memory.block_cache = rocksdb::NewLRUCache(kBlockCacheBytes);
  memory.write_buffer_manager = std::make_shared<rocksdb::WriteBufferManager>(
      kWriteBufferBytes, memory.block_cache);
  return memory;
}

StatusOr<std::unique_ptr<PersistentBackend>> PersistentBackend::Open(
    BackendOptions const& options) {
  auto table_dirs = ListTableDirectories(options.data_dir);
  if (!table_dirs) {
    return table_dirs.status();
  }
  std::uint64_t next_table_id = 1;
  for (auto const& db_path : *table_dirs) {
    std::uint64_t id = 0;
    if (absl::SimpleAtoi(db_path.filename().string(), &id)) {
      next_table_id = std::max(next_table_id, id + 1);
    }
  }
  return std::unique_ptr<PersistentBackend>(
      new PersistentBackend(options, next_table_id));
}

std::filesystem::path PersistentBackend::TableDir(std::uint64_t id) const {
  return std::filesystem::path(options_.data_dir) / "tables" /
         absl::StrFormat("%06d", id);
}

StatusOr<std::vector<RestoredTable>> PersistentBackend::Restore() {
  std::vector<RestoredTable> res;
  std::map<std::string, std::filesystem::path> path_by_name;
  auto table_dirs = ListTableDirectories(options_.data_dir);
  if (!table_dirs) {
    return table_dirs.status();
  }
  for (auto const& db_path : *table_dirs) {
    std::error_code exists_ec;
    bool const has_current =
        std::filesystem::exists(db_path / "CURRENT", exists_ec);
    if (exists_ec) {
      return InternalError(
          "Failed to check the table directory: " + exists_ec.message(),
          GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
    }
    if (!has_current) {
      std::error_code remove_ec;
      std::filesystem::remove_all(db_path, remove_ec);
      if (remove_ec) {
        return InternalError(
            "Failed to remove the table directory: " + remove_ec.message(),
            GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
      }
      continue;
    }

    auto opened = OpenDatabase(db_path, memory_);
    if (!opened) {
      return opened.status();
    }
    auto maybe_metadata = ReadMetadata(*opened->db);
    if (!maybe_metadata) {
      return WithTablePath(maybe_metadata.status(), db_path);
    }
    if (!*maybe_metadata || (*maybe_metadata)->state() == STATE_CREATING ||
        (*maybe_metadata)->state() == STATE_DELETING) {
      opened->handles_by_name.clear();
      opened->db.reset();
      auto status = DestroyDirectory(db_path);
      if (!status.ok()) {
        return status;
      }
      continue;
    }
    auto metadata = **std::move(maybe_metadata);
    if (metadata.state() != STATE_ACTIVE) {
      return FailedPreconditionError(
          "Table has an unexpected state.",
          GCP_ERROR_INFO()
              .WithMetadata("path", db_path.string())
              .WithMetadata("state", std::to_string(metadata.state())));
    }
    auto status = ValidateSchema(metadata.schema());
    if (!status.ok()) {
      return DataLossError(
          "The table's schema is invalid: " + status.message(),
          GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
    }

    auto const& name = metadata.schema().name();
    auto [seen, inserted] = path_by_name.emplace(name, db_path);
    if (!inserted) {
      return FailedPreconditionError(
          "Two tables have the same name.",
          GCP_ERROR_INFO()
              .WithMetadata("table_name", name)
              .WithMetadata("path", seen->second.string())
              .WithMetadata("other_path", db_path.string()));
    }

    auto schema = metadata.schema();
    auto maybe_operations = PersistentTableOperations::Restore(
        db_path, memory_, std::move(opened->db), opened->handles_by_name,
        std::move(metadata));
    if (!maybe_operations) {
      return WithTablePath(maybe_operations.status(), db_path);
    }
    auto table_name = schema.name();
    res.push_back(RestoredTable{
        std::move(table_name),
        std::move(schema),
        *std::move(maybe_operations),
    });
  }
  return res;
}

StatusOr<std::unique_ptr<TableOperations>> PersistentBackend::CreateTable(
    btadmin::Table const& schema) {
  return PersistentTableOperations::CreateNew(TableDir(next_table_id_++),
                                              memory_, schema);
}

void PersistentBackend::DestroyTable(
    std::unique_ptr<TableOperations> operations) {
  auto db_path = static_cast<PersistentTableOperations&>(*operations).DbPath();

  /**
   * DestroyDB needs the RocksDB instance closed. Every column family, column
   * family handle and live cell stream holds a shared_ptr to the
   * TransactionDB, so release them all before destroying.
   */
  operations.reset();
  auto status = DestroyDirectory(db_path);
  if (!status.ok()) {
    std::cerr << "DestroyTable: " << status << "\n";
  }
}

StatusOr<std::unique_ptr<TableOperations>> PersistentTableOperations::CreateNew(
    std::filesystem::path const& db_path, SharedRocksDbMemory const& memory,
    google::bigtable::admin::v2::Table const& schema) {
  std::filesystem::path parent_path = db_path.parent_path();

  std::error_code ec;
  std::filesystem::create_directories(parent_path, ec);
  if (ec) {
    return InternalError(
        "failed to create directory: " + parent_path.string() +
            "; Error status: " + ec.message(),
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }

  rocksdb::Options options = MakeDatabaseOptions(memory);
  rocksdb::TransactionDBOptions txn_options;
  txn_options.lock_mgr_handle.reset(rocksdb::NewRangeLockManager(nullptr));

  options.create_if_missing = true;
  options.error_if_exists = true;

  rocksdb::TransactionDB* raw_db = nullptr;
  rocksdb::Status status = rocksdb::TransactionDB::Open(
      options, txn_options, db_path.string(), &raw_db);
  if (!status.ok()) {
    return InternalError(
        "failed to open rocksdb instance; " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  std::unique_ptr<PersistentTableOperations> res =
      std::make_unique<PersistentTableOperations>();
  res->db_.reset(raw_db);
  res->table_name_ = schema.name();
  res->db_path_ = db_path;
  res->column_family_options_ = MakeColumnFamilyOptions(memory);

  for (auto const& dir : {parent_path, parent_path.parent_path()}) {
    auto st = SyncDirectory(dir);
    if (!st.ok()) {
      return st;
    }
  }

  TableMetadata metadata;
  *metadata.mutable_schema() = schema;
  metadata.set_state(STATE_CREATING);
  auto st = res->PersistMetadata(metadata);
  if (!st.ok()) {
    return st;
  }

  for (auto const& cfd : schema.column_families()) {
    auto status = res->CreateColumnFamily(cfd.first, cfd.second);
    if (!status.ok()) {
      return status;
    }
  }

  // The commit point: the table exists once its metadata is ACTIVE. A
  // failure before this, e.g. while creating a column family, leaves a
  // database with no metadata or with CREATING, which startup does not
  // restore. A failure of this write may still have reached the WAL, so the
  // outcome is unknown until the next startup.
  metadata.set_state(STATE_ACTIVE);
  st = res->PersistMetadata(metadata);
  if (!st.ok()) {
    return UnavailableError(
        "Failed to commit the table creation: " + st.message(),
        GCP_ERROR_INFO().WithMetadata("table", schema.name()));
  }
  return StatusOr<std::unique_ptr<TableOperations>>(std::move(res));
}

StatusOr<std::unique_ptr<TableOperations>> PersistentTableOperations::Restore(
    std::filesystem::path const& db_path, SharedRocksDbMemory const& memory,
    std::shared_ptr<rocksdb::TransactionDB> db,
    std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> const&
        handles_by_name,
    TableMetadata metadata) {
  std::unique_ptr<PersistentTableOperations> res =
      std::make_unique<PersistentTableOperations>();
  res->db_ = std::move(db);
  res->db_path_ = db_path;
  res->column_family_options_ = MakeColumnFamilyOptions(memory);
  auto const& schema = metadata.schema();
  res->table_name_ = schema.name();

  for (auto const& id : metadata.intent().families_to_empty()) {
    if (schema.column_families().find(id) == schema.column_families().end()) {
      return DataLossError(
          "The intent names a column family missing from the schema.",
          GCP_ERROR_INFO()
              .WithMetadata("path", db_path.string())
              .WithMetadata("column_family", id));
    }
  }

  for (auto const& cfd : schema.column_families()) {
    absl::optional<google::bigtable::admin::v2::Type> opt_value_type =
        absl::nullopt;
    if (cfd.second.has_value_type()) {
      opt_value_type = cfd.second.value_type();
    }

    auto hit = handles_by_name.find(RocksDbColumnFamilyName(cfd.first));
    if (hit != handles_by_name.end()) {
      auto maybe_cf = PersistentColumnFamily::OpenExisting(
          res->db_, hit->second, opt_value_type);
      if (!maybe_cf) {
        return maybe_cf.status();
      }
      res->column_families_.emplace(cfd.first, maybe_cf.value());
      continue;
    }

    auto const& to_empty = metadata.intent().families_to_empty();
    if (std::find(to_empty.begin(), to_empty.end(), cfd.first) ==
        to_empty.end()) {
      return DataLossError("A column family named by the schema is missing.",
                           GCP_ERROR_INFO()
                               .WithMetadata("path", db_path.string())
                               .WithMetadata("column_family", cfd.first));
    }
    auto status = res->CreateColumnFamily(cfd.first, cfd.second);
    if (!status.ok()) {
      return status;
    }
  }

  std::unordered_set<std::string> named;
  for (auto const& cfd : schema.column_families()) {
    named.insert(RocksDbColumnFamilyName(cfd.first));
  }
  for (auto const& [name, handle] : handles_by_name) {
    if (name == rocksdb::kDefaultColumnFamilyName || named.count(name) != 0) {
      continue;
    }
    auto drop_status = res->db_->DropColumnFamily(handle.get());
    if (!drop_status.ok()) {
      return InternalError(
          "Failed to drop column family: " + drop_status.ToString(),
          GCP_ERROR_INFO().WithMetadata("column_family", name));
    }
  }

  if (metadata.has_intent()) {
    for (auto const& id : metadata.intent().families_to_empty()) {
      auto status = res->DropColumnFamily(id);
      if (!status.ok()) {
        return status;
      }
      status = res->CreateColumnFamily(id, schema.column_families().at(id));
      if (!status.ok()) {
        return status;
      }
    }
    metadata.clear_intent();
    auto status = res->PersistMetadata(metadata);
    if (!status.ok()) {
      return status;
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
  auto maybe_target =
      ApplyModifyColumnFamiliesToSchemaOnly(request, std::move(schema));
  if (!maybe_target) {
    return maybe_target.status();
  }

  auto unavailable = [this](Status const& status) {
    return UnavailableError(
        "Failed to apply the column family changes: " + status.message(),
        GCP_ERROR_INFO().WithMetadata("table", table_name_));
  };

  // Replays the request in order. The shared validation above has already
  // rejected every invalid sequence, e.g. a create of a family that exists.
  std::unordered_set<std::string> families_to_create;
  std::unordered_set<std::string> families_to_recreate;
  std::unordered_set<std::string> families_to_drop;
  for (auto const& modification : request.modifications()) {
    auto const& id = modification.id();
    if (modification.drop()) {
      if (families_to_create.erase(id) == 0) {
        families_to_recreate.erase(id);
        families_to_drop.insert(id);
      }
    } else if (modification.has_create()) {
      if (families_to_drop.erase(id) != 0) {
        families_to_recreate.insert(id);
      } else {
        families_to_create.insert(id);
      }
    }
  }

  // Create the intent that we will persist in the metadata.
  Intent intent;
  for (auto const& id : families_to_recreate) {
    intent.add_families_to_empty(id);
  }

  auto const& target = maybe_target.value();
  std::vector<std::string> created;
  for (auto const& id : families_to_create) {
    auto status = CreateColumnFamily(id, target.column_families().at(id));
    if (!status.ok()) {
      for (auto const& created_id : created) {
        if (!DropColumnFamily(created_id).ok()) {
          return unavailable(status);
        }
      }
      return status;
    }
    created.push_back(id);
  }

  TableMetadata metadata;
  *metadata.mutable_schema() = target;
  metadata.set_state(STATE_ACTIVE);
  if (!families_to_recreate.empty()) {
    *metadata.mutable_intent() = std::move(intent);
  }
  // First commit point. A failure while creating the column families above
  // drops the ones already created and applies nothing; if that drop fails,
  // the table is marked unavailable. A failure of this write, or one before it
  // that could not be undone, leaves column families the committed schema
  // doesn't name, which the next startup drops.
  // Once it succeeds, a later failure is completed at startup: the intent
  // says which families to drop and re-create, and the persisted schema
  // which column families to drop.
  auto status = PersistMetadata(metadata);
  if (!status.ok()) {
    return unavailable(status);
  }

  for (auto const& id : families_to_recreate) {
    status = DropColumnFamily(id);
    if (!status.ok()) {
      return unavailable(status);
    }
    status = CreateColumnFamily(id, target.column_families().at(id));
    if (!status.ok()) {
      return unavailable(status);
    }
  }

  // Second commit point, made only when there is an intent: clears it. A
  // failure between the two commits is completed at startup by replaying the
  // intent, which is safe to repeat: dropping and re-creating an empty column
  // family changes nothing. Once it succeeds, a later failure only leaves
  // dropped column families behind, which startup drops because the schema
  // doesn't name them.
  if (metadata.has_intent()) {
    metadata.clear_intent();
    status = PersistMetadata(metadata);
    if (!status.ok()) {
      return unavailable(status);
    }
  }

  for (auto const& id : families_to_drop) {
    status = DropColumnFamily(id);
    if (!status.ok()) {
      return unavailable(status);
    }
  }

  return *std::move(maybe_target);
}

Status PersistentTableOperations::UpdateTable(
    google::bigtable::admin::v2::Table const& new_schema) {
  TableMetadata metadata;
  *metadata.mutable_schema() = new_schema;
  metadata.set_state(STATE_ACTIVE);
  auto status = PersistMetadata(metadata);
  if (!status.ok()) {
    return UnavailableError(
        "Failed to commit the table update: " + status.message(),
        GCP_ERROR_INFO().WithMetadata("table", table_name_));
  }
  return Status();
}

Status PersistentTableOperations::CommitDeletion() {
  auto maybe_metadata = LoadMetadata();
  if (!maybe_metadata) {
    return maybe_metadata.status();
  }
  maybe_metadata->set_state(STATE_DELETING);
  auto status = PersistMetadata(*maybe_metadata);
  if (!status.ok()) {
    return UnavailableError(
        "Failed to commit the table deletion: " + status.message(),
        GCP_ERROR_INFO().WithMetadata("table", table_name_));
  }
  return Status();
}

Status PersistentTableOperations::PersistMetadata(
    TableMetadata const& metadata) {
  std::string bytes;
  if (!metadata.SerializeToString(&bytes)) {
    return InternalError(
        "Failed to serialize table metadata",
        GCP_ERROR_INFO().WithMetadata("metadata", metadata.DebugString()));
  }

  rocksdb::WriteOptions wopts;
  wopts.sync = true;
  rocksdb::Status s =
      db_->Put(wopts, db_->DefaultColumnFamily(), kTableMetadataKey, bytes);
  if (!s.ok()) {
    return InternalError(
        "Failed to persist table metadata in RocksDB: " + s.ToString(),
        GCP_ERROR_INFO()
            .WithMetadata("table", table_name_)
            .WithMetadata("key", kTableMetadataKey));
  }
  s = db_->Flush(rocksdb::FlushOptions(), db_->DefaultColumnFamily());
  if (!s.ok()) {
    return InternalError(
        "Failed to flush table metadata in RocksDB: " + s.ToString(),
        GCP_ERROR_INFO()
            .WithMetadata("table", table_name_)
            .WithMetadata("key", kTableMetadataKey));
  }
  return Status();
}

StatusOr<TableMetadata> PersistentTableOperations::LoadMetadata() const {
  auto metadata = ReadMetadata(*db_);
  if (!metadata) {
    return metadata.status();
  }
  if (!*metadata) {
    return DataLossError(
        "Table metadata is missing.",
        GCP_ERROR_INFO().WithMetadata("key", kTableMetadataKey));
  }
  return **std::move(metadata);
}

Status PersistentTableOperations::CreateColumnFamily(
    std::string const& id,
    google::bigtable::admin::v2::ColumnFamily const& definition) {
  auto maybe_cf =
      definition.has_value_type()
          ? PersistentColumnFamily::ConstructAggregateColumnFamily(
                definition.value_type(), db_, column_family_options_, id)
          : PersistentColumnFamily::Create(db_, column_family_options_, id);
  if (!maybe_cf) {
    return maybe_cf.status();
  }
  column_families_.emplace(id, *std::move(maybe_cf));
  return Status();
}

Status PersistentTableOperations::DropColumnFamily(std::string const& id) {
  auto it = column_families_.find(id);
  if (it == column_families_.end()) {
    return InternalError("Column family to drop not found",
                         GCP_ERROR_INFO().WithMetadata("column_family", id));
  }
  auto status = db_->DropColumnFamily(it->second->GetRaw());
  if (!status.ok()) {
    return InternalError("Failed to drop column family: " + status.ToString(),
                         GCP_ERROR_INFO().WithMetadata("column_family", id));
  }
  column_families_.erase(it);
  return Status();
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
  std::string const column_prefix = KeyCoder::EscapedRowColWithSeparator(
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
  std::string const row_prefix = KeyCoder::EscapedRowWithSeparator(row_key_);
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
  std::string const row_prefix = KeyCoder::EscapedRowWithSeparator(row_key_);
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
    std::string const column_prefix = KeyCoder::EscapedRowColWithSeparator(
        request.row_key(), rule.column_qualifier());

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
