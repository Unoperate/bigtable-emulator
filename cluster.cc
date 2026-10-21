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

#include "cluster.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/match.h"
#include "table.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

namespace btadmin = google::bigtable::admin::v2;

/**
 * Obtain a limited view of a `Table`'s schema, by applying a `TableView`.
 *
 * @param table_name table name in the form of
 *     `/projects/{}/instances/{}/tables/{}` to be returned in the
 * @param table the table in question
 * @param view the view to apply
 * @param default_view the view to fall back to in case `view` is set to
 *     `btadmin::Table::VIEW_UNSPECIFIED`. `default_view` should not be set to
 *     `btadmin::Table::VIEW_UNSPECIFIED`.
 * @return the trimmed schema or error
 */
StatusOr<btadmin::Table> ApplyView(std::string const& table_name,
                                   Table const& table, btadmin::Table_View view,
                                   btadmin::Table_View default_view) {
  if (view == btadmin::Table::VIEW_UNSPECIFIED) {
    view = default_view;
  }
  switch (view) {
    case btadmin::Table::VIEW_UNSPECIFIED:
      return google::cloud::internal::InternalError(
          "VIEW_UNSPECIFIED cannot be the default view");
    case btadmin::Table::NAME_ONLY: {
      btadmin::Table res;
      res.set_name(table_name);
      return res;
    }
    case btadmin::Table::SCHEMA_VIEW: {
      btadmin::Table res;
      res.set_name(table_name);
      auto before_view = table.GetSchema();
      *res.mutable_column_families() =
          std::move(*before_view.mutable_column_families());
      res.set_granularity(before_view.granularity());
      return res;
    }
    case btadmin::Table::REPLICATION_VIEW:
    case btadmin::Table::ENCRYPTION_VIEW: {
      btadmin::Table res;
      res.set_name(table_name);
      auto before_view = table.GetSchema();
      *res.mutable_cluster_states() =
          std::move(*before_view.mutable_cluster_states());
      return res;
    }
    case btadmin::Table::FULL:
      return table.GetSchema();
    default:
      return google::cloud::internal::UnimplementedError(
          "Unsupported view.",
          GCP_ERROR_INFO().WithMetadata("view", Table_View_Name(view)));
  }
}

Status ValidateTableName(std::string const& s) {
  if (s.empty()) {
    return InvalidArgumentError("Table name is empty", GCP_ERROR_INFO());
  }
  if (absl::StrContains(s, "//")) {
    return InvalidArgumentError("Table name must not contain '//'",
                                GCP_ERROR_INFO().WithMetadata("table_name", s));
  }
  if (absl::StrContains(s, '\\')) {
    return InvalidArgumentError("Table name must not contain '\\\\'",
                                GCP_ERROR_INFO().WithMetadata("table_name", s));
  }
  return Status();
}
}  // anonymous namespace

Cluster::Cluster(bool const should_persist) : should_persist_(should_persist) {}

/**
 * Lazily-eager approach to loading of existing tables.
 * Until we get a first request, we don't know where the tables are stored.
 * But when we get a request, we get a table name, as well as its "table space".
 * Then, BootstrapTablesFromDisk checks if we didn't load it before, and if we
 * didn't it loads the whole table space.
 * @param table_path string representing the path of the tabel that triggered
 * the bootstrapping
 */
void Cluster::BootstrapTablesFromDisk(std::string const& table_path) {
  std::error_code ec;
  std::string tp_copy = table_path;
  if (!tp_copy.empty() && tp_copy.front() == '/') tp_copy.erase(0, 1);
  std::filesystem::path root(data_root_ + tp_copy);
  root = root.parent_path();
  auto table_parent = root.parent_path().string();
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (!std::filesystem::exists(root, ec)) return;
    if (loaded_tables_paths_.find(table_parent) != loaded_tables_paths_.end()) {
      return;
    }
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
    if (!it->is_directory(ec)) continue;

    auto db_path = it->path();
    auto current = db_path / "CURRENT";
    if (!std::filesystem::exists(current, ec)) continue;

    it.disable_recursion_pending();

    auto maybe_table = Table::Load(tp_copy, data_root_);
    if (!maybe_table) {
      std::cerr << "BootstrapTablesFromDisk: failed to load table "
                << table_path << ": " << maybe_table.status() << "\n";
      continue;
    }

    std::lock_guard<std::mutex> lock(mu_);
    table_by_name_.emplace(table_path, std::move(maybe_table.value()));
    loaded_tables_paths_.emplace(table_parent);
  }
}

StatusOr<btadmin::Table> Cluster::CreateTable(std::string const& table_name,
                                              btadmin::Table schema) {
  BootstrapTablesFromDisk(table_name);
  auto st = ValidateTableName(table_name);
  if (!st.ok()) {
    return st;
  }
  schema.set_name(table_name);

  std::lock_guard<std::mutex> lock(mu_);
  if (table_by_name_.find(table_name) != table_by_name_.end()) {
    return google::cloud::internal::AlreadyExistsError(
        "Table already exists.",
        GCP_ERROR_INFO().WithMetadata("table_name", table_name));
  }

  /**
   * Table locking happens by using its utilities.
   * However, utilities are created in Table::Create, so in order to
   * protect table for this duration, cluster lock is utilized.
   * Table::Create may take some time in case of the persistent tables,
   * but tables won't be created often.
   */
  auto maybe_table =
      Table::Create(std::move(schema), should_persist_, data_root_);
  if (!maybe_table) {
    return maybe_table.status();
  }

  table_by_name_.emplace(table_name, maybe_table.value());
  return (*maybe_table)->GetSchema();
}

StatusOr<std::vector<btadmin::Table>> Cluster::ListTables(
    std::string const& instance_name, btadmin::Table_View view) {
  auto st = ValidateTableName(instance_name);
  if (!st.ok()) return st;

  std::map<std::string, std::shared_ptr<Table>> table_by_name_copy;
  {
    std::lock_guard<std::mutex> lock(mu_);
    table_by_name_copy = table_by_name_;
  }
  std::vector<btadmin::Table> res;
  std::string const prefix = instance_name + "/tables/";
  BootstrapTablesFromDisk(prefix);
  std::cout << "Listing tables with prefix " << prefix << std::endl;
  for (auto name_and_table_it = table_by_name_copy.lower_bound(prefix);
       name_and_table_it != table_by_name_copy.end() &&
       absl::StartsWith(name_and_table_it->first, prefix);
       ++name_and_table_it) {
    auto maybe_view =
        ApplyView(name_and_table_it->first, *name_and_table_it->second, view,
                  btadmin::Table::NAME_ONLY);
    if (!maybe_view) {
      return maybe_view.status();
    }
    res.emplace_back(*maybe_view);
  }
  return res;
}

StatusOr<btadmin::Table> Cluster::GetTable(std::string const& table_name,
                                           btadmin::Table_View view) {
  auto st = ValidateTableName(table_name);
  if (!st.ok()) return st;
  BootstrapTablesFromDisk(table_name);
  std::shared_ptr<Table> found_table;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = table_by_name_.find(table_name);
    if (it != table_by_name_.end()) {
      found_table = it->second;
    }
  }

  if (!found_table) {
    return NotFoundError("No such table", GCP_ERROR_INFO().WithMetadata(
                                              "table_name", table_name));
  }

  return ApplyView(table_name, *found_table, view, btadmin::Table::SCHEMA_VIEW);
}

Status Cluster::DeleteTable(std::string const& table_name) {
  auto st = ValidateTableName(table_name);
  if (!st.ok()) return st;
  BootstrapTablesFromDisk(table_name);

  std::string key;
  std::filesystem::path db_path;
  std::shared_ptr<Table> doomed;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = table_by_name_.find(table_name);
    if (it == table_by_name_.end()) {
      return NotFoundError("No such table", GCP_ERROR_INFO().WithMetadata(
                                                "table_name", table_name));
    }
    if (it->second->IsDeleteProtected()) {
      return FailedPreconditionError(
          "The table has deletion protection.",
          GCP_ERROR_INFO().WithMetadata("table_name", table_name));
    }

    doomed = it->second;
    db_path = std::filesystem::path(data_root_) / table_name;
    table_by_name_.erase(it);
  }

  if (!should_persist_) return Status();

  /**
   * There may be many live shared_ptrs to the Table. So, normally, we would
   * like to wait for the destructor. However, Table has a vector of column
   * families, which have a shared_ptr to the owning Table. So, the
   * MarkForDeletion will delete all column fams. It is safe, as this function
   * will acquire an exclusive lock to the Table, and so when it exits, each
   * other request that started processing will just return an error that CF is
   * non-existent.
   */
  doomed->MarkForDeletion();
  doomed.reset();

  rocksdb::Options options;
  auto s = rocksdb::DestroyDB(db_path.string(), options);
  if (!s.ok()) {
    std::error_code ec;
    std::filesystem::remove_all(db_path, ec);

    return google::cloud::internal::InternalError(
        "Failed to destroy table RocksDB at " + db_path.string() + "; " +
            s.ToString(),
        GCP_ERROR_INFO().WithMetadata("table_name", table_name));
  }

  std::error_code ec;
  std::filesystem::remove_all(db_path, ec);
  return Status();
}

bool Cluster::HasTable(std::string const& table_name) {
  auto st = ValidateTableName(table_name);
  if (!st.ok()) return false;
  BootstrapTablesFromDisk(table_name);
  std::lock_guard<std::mutex> lock(mu_);
  return table_by_name_.find(table_name) != table_by_name_.end();
}

StatusOr<std::shared_ptr<Table>> Cluster::FindTable(
    std::string const& table_name) {
  auto st = ValidateTableName(table_name);
  if (!st.ok()) return st;
  BootstrapTablesFromDisk(table_name);

  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = table_by_name_.find(table_name);
    if (it != table_by_name_.end()) return it->second;
  }

  return NotFoundError("No such table",
                       GCP_ERROR_INFO().WithMetadata("table_name", table_name));
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
