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
                                   google::bigtable::admin::v2::Table const& table, btadmin::Table_View view,
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
      auto before_view = table;
      *res.mutable_column_families() =
          std::move(*before_view.mutable_column_families());
      res.set_granularity(before_view.granularity());
      return res;
    }
    case btadmin::Table::REPLICATION_VIEW:
    case btadmin::Table::ENCRYPTION_VIEW: {
      btadmin::Table res;
      res.set_name(table_name);
      auto before_view = table;
      *res.mutable_cluster_states() =
          std::move(*before_view.mutable_cluster_states());
      return res;
    }
    case btadmin::Table::FULL:
      return table;
    default:
      return google::cloud::internal::UnimplementedError(
          "Unsupported view.",
          GCP_ERROR_INFO().WithMetadata("view", Table_View_Name(view)));
  }
}

}  // anonymous namespace

StatusOr<btadmin::Table> Cluster::CreateTable(std::string const& table_name,
                                              btadmin::Table schema) {
  schema.set_name(table_name);
  auto status = storage_->CreateTable(schema);
  if (!status.ok()) {
    return status;
  } else {
    return schema;
  }
}

StatusOr<std::vector<btadmin::Table>> Cluster::ListTables(
    std::string const& instance_name, btadmin::Table_View view) const {
  std::map<std::string, std::shared_ptr<Table>> table_by_name_copy;
  std::vector<btadmin::Table> res;
  std::string const prefix = instance_name + "/tables/";

  for(auto [name, meta] : storage_->Tables()) {
    auto maybe_view =
        ApplyView(name, meta.table(), view, btadmin::Table::NAME_ONLY);
    if (!maybe_view) {
      return maybe_view.status();
    }
    res.emplace_back(*maybe_view);
  }
  return res;
}

StatusOr<btadmin::Table> Cluster::GetTable(std::string const& table_name,
                                           btadmin::Table_View view) const {

  const auto meta = storage_->GetTable(table_name);
  if (!meta.ok()) {
    return meta.status();
  }
  return meta.value().table();
}

Status Cluster::DeleteTable(std::string const& table_name) {
  return storage_->DeleteTable(table_name, [](auto const& table_name, auto const& meta) -> Status {
    if (meta.table().deletion_protection()) {
      return FailedPreconditionError(
          "The table has deletion protection.",
          GCP_ERROR_INFO().WithMetadata("table_name", table_name));
    }
    return Status();
  });
}

bool Cluster::HasTable(std::string const& table_name) const {
  return storage_->HasTable(table_name);
}

StatusOr<std::shared_ptr<PersistedTable>> Cluster::FindTable(
    std::string const& table_name) {
  auto maybe_table = storage_->GetTable(table_name);
  if (!maybe_table.ok()) {
    return NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                                               "table_name", table_name));
  }
  return std::make_shared<PersistedTable>(table_name, storage_);
}

StatusOr<std::shared_ptr<Table>> Cluster::FindLegacyTable(
    std::string const& table_name) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = table_by_name_.find(table_name);
    if (it == table_by_name_.end()) {
      return NotFoundError("No such table.", GCP_ERROR_INFO().WithMetadata(
                                                 "table_name", table_name));
    }
    return it->second;
  }
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
