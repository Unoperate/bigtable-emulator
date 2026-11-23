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

#include "registry.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/match.h"
#include "absl/synchronization/mutex.h"
#include "bigtable_limits.h"
#include "re2/re2.h"
#include "table.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <iostream>
#include <map>
#include <memory>
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
      auto before_view = table.Schema();
      *res.mutable_column_families() = before_view->column_families();
      res.set_granularity(before_view->granularity());
      return res;
    }
    case btadmin::Table::REPLICATION_VIEW:
    case btadmin::Table::ENCRYPTION_VIEW: {
      btadmin::Table res;
      res.set_name(table_name);
      *res.mutable_cluster_states() = table.Schema()->cluster_states();
      return res;
    }
    case btadmin::Table::FULL:
      return *table.Schema();
    default:
      return google::cloud::internal::UnimplementedError(
          "Unsupported view.",
          GCP_ERROR_INFO().WithMetadata("view", Table_View_Name(view)));
  }
}

Status ValidateTableName(std::string const& table_name) {
  static auto const* const kTableNamePattern =
      new re2::RE2("projects/[^/]+/instances/[^/]+/tables/([^/]*)");
  static auto const* const kTableIdPattern =
      new re2::RE2("[_a-zA-Z0-9][-_.a-zA-Z0-9]{0,49}");
  std::string table_id;
  if (!re2::RE2::FullMatch(table_name, *kTableNamePattern, &table_id)) {
    return InvalidArgumentError(
        "Table name must have the form "
        "projects/{project}/instances/{instance}/tables/{table}.",
        GCP_ERROR_INFO().WithMetadata("table_name", table_name));
  }
  if (!re2::RE2::FullMatch(table_id, *kTableIdPattern)) {
    return InvalidArgumentError(
        "Invalid table ID: it must be 1-50 characters long, start with a "
        "letter, digit or '_', and contain only letters, digits, '_', '-' "
        "and '.'.",
        GCP_ERROR_INFO().WithMetadata("table_id", table_id));
  }
  return Status();
}
}  // anonymous namespace

StatusOr<std::shared_ptr<Registry>> Registry::Create(
    std::unique_ptr<Backend> backend) {
  auto maybe_restored = backend->Restore();
  if (!maybe_restored) {
    return maybe_restored.status();
  }

  std::shared_ptr<Registry> registry(new Registry(std::move(backend)));
  for (auto& restored : *maybe_restored) {
    if (restored.schema.granularity() != btadmin::Table::MILLIS) {
      std::cerr << "Registry::Create: skipping table " << restored.name
                << ": unsupported timestamp granularity\n";
      continue;
    }
    auto name = restored.name;
    registry->table_by_name_.emplace(
        std::move(name),
        std::make_shared<Table>(std::move(restored.name),
                                std::move(restored.schema),
                                std::move(restored.operations)));
  }
  return registry;
}

StatusOr<btadmin::Table> Registry::CreateTable(std::string const& table_name,
                                               btadmin::Table schema) {
  auto st = ValidateTableName(table_name);
  if (!st.ok()) {
    return st;
  }
  schema.set_name(table_name);
  st = PrepareTableSchema(schema);
  if (!st.ok()) {
    return st;
  }

  {
    absl::WriterMutexLock lock(&mu_);
    if (shutting_down_) {
      return UnavailableError("The emulator is shutting down.",
                              GCP_ERROR_INFO());
    }
    if (table_by_name_.count(table_name) != 0) {
      return google::cloud::internal::AlreadyExistsError(
          "Table already exists.",
          GCP_ERROR_INFO().WithMetadata("table_name", table_name));
    }
    if (table_by_name_.size() >= kMaxTablesPerInstance) {
      return google::cloud::internal::ResourceExhaustedError(
          "Too many tables: an emulator instance can have at most " +
              std::to_string(kMaxTablesPerInstance) + ".",
          GCP_ERROR_INFO()
              .WithMetadata("table_name", table_name)
              .WithMetadata("limit", std::to_string(kMaxTablesPerInstance)));
    }
    table_by_name_.emplace(table_name, nullptr);
  }

  // The name is reserved, so the storage is created without the lock.
  auto maybe_operations = backend_->CreateTable(schema);

  absl::WriterMutexLock lock(&mu_);
  auto it = table_by_name_.find(table_name);
  if (!maybe_operations) {
    if (maybe_operations.status().code() != StatusCode::kUnavailable) {
      table_by_name_.erase(it);
    }
    return maybe_operations.status();
  }
  it->second = std::make_shared<Table>(table_name, std::move(schema),
                                       *std::move(maybe_operations));
  return *it->second->Schema();
}

StatusOr<std::vector<btadmin::Table>> Registry::ListTables(
    std::string const& instance_name, btadmin::Table_View view) {
  std::map<std::string, std::shared_ptr<Table>> table_by_name_copy;
  {
    absl::ReaderMutexLock lock(&mu_);
    table_by_name_copy = table_by_name_;
  }
  std::vector<btadmin::Table> res;
  std::string const prefix = instance_name + "/tables/";
  for (auto name_and_table_it = table_by_name_copy.lower_bound(prefix);
       name_and_table_it != table_by_name_copy.end() &&
       absl::StartsWith(name_and_table_it->first, prefix);
       ++name_and_table_it) {
    if (!name_and_table_it->second) {
      continue;  // still being created
    }
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

StatusOr<btadmin::Table> Registry::GetTable(std::string const& table_name,
                                            btadmin::Table_View view) {
  auto maybe_table = FindTable(table_name);
  if (!maybe_table) {
    return maybe_table.status();
  }
  auto status = (*maybe_table)->CheckAvailable();
  if (!status.ok()) {
    return status;
  }
  return ApplyView(table_name, **maybe_table, view,
                   btadmin::Table::SCHEMA_VIEW);
}

Status Registry::DeleteTable(std::string const& table_name) {
  auto maybe_table = FindTable(table_name);
  if (!maybe_table) {
    return maybe_table.status();
  }
  auto& doomed = *maybe_table;

  // Waits for the requests running on the table, then detaches its storage.
  auto maybe_operations = doomed->Delete();
  if (!maybe_operations) {
    return maybe_operations.status();
  }

  {
    absl::WriterMutexLock lock(&mu_);
    auto it = table_by_name_.find(table_name);
    if (it != table_by_name_.end() && it->second == doomed) {
      table_by_name_.erase(it);
    }
  }

  backend_->DestroyTable(*std::move(maybe_operations));
  return Status();
}

Status Registry::UpdateTable(btadmin::Table const& table,
                             google::protobuf::FieldMask const& mask) {
  auto maybe_table = FindTable(table.name());
  if (!maybe_table) {
    return maybe_table.status();
  }
  return (*maybe_table)->Update(table, mask);
}

StatusOr<btadmin::Table> Registry::ModifyColumnFamilies(
    btadmin::ModifyColumnFamiliesRequest const& request) {
  auto maybe_table = FindTable(request.name());
  if (!maybe_table) {
    return maybe_table.status();
  }
  return (*maybe_table)->ModifyColumnFamilies(request);
}

Status Registry::DropRowRange(btadmin::DropRowRangeRequest const& request) {
  auto maybe_table = FindTable(request.name());
  if (!maybe_table) {
    return maybe_table.status();
  }
  return (*maybe_table)->DropRowRange(request);
}

Status Registry::CheckTableExists(std::string const& table_name) {
  auto maybe_table = FindTable(table_name);
  if (!maybe_table) {
    return maybe_table.status();
  }
  return (*maybe_table)->CheckAvailable();
}

StatusOr<std::shared_ptr<Table>> Registry::FindTable(
    std::string const& table_name) {
  auto st = ValidateTableName(table_name);
  if (!st.ok()) {
    return st;
  }

  absl::ReaderMutexLock lock(&mu_);
  if (shutting_down_) {
    return UnavailableError("The emulator is shutting down.", GCP_ERROR_INFO());
  }
  auto it = table_by_name_.find(table_name);
  if (it == table_by_name_.end()) {
    return NotFoundError("No such table", GCP_ERROR_INFO().WithMetadata(
                                              "table_name", table_name));
  }
  if (!it->second) {
    return FailedPreconditionError(
        "The table is being created.",
        GCP_ERROR_INFO().WithMetadata("table_name", table_name));
  }
  return it->second;
}

Status Registry::Shutdown() {
  std::map<std::string, std::shared_ptr<Table>> tables;
  {
    absl::WriterMutexLock lock(&mu_);
    if (shutting_down_) {
      return Status();
    }
    shutting_down_ = true;
    tables = table_by_name_;
  }

  backend_->StopBackgroundWork();

  // TODO(prawilny): wait until no CreateTable is in flight (no null entries in
  // table_by_name_) before shutting the backend down.
  Status result;
  for (auto& name_and_table : tables) {
    if (!name_and_table.second) {
      continue;
    }
    auto status = name_and_table.second->Close();
    if (!status.ok() && result.ok()) {
      result = std::move(status);
    }
  }

  auto status = backend_->Shutdown();
  if (!status.ok() && result.ok()) {
    result = std::move(status);
  }
  return result;
}

Registry::~Registry() {
  auto status = Shutdown();
  if (!status.ok()) {
    std::cerr << "Registry shutdown failed: " << status << "\n";
  }
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
