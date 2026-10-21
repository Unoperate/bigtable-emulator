// Copyright 2025 Google LLC
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

#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "column_family.h"
#include "table.h"
#include "test_util.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

Status SetCellsInMultipleRows(
    std::shared_ptr<google::cloud::bigtable::emulator::Table> table,
    std::string const& table_name,
    std::map<std::string, std::vector<SetCellParams>> params) {
  for (auto& p : params) {
    auto status = SetCells(table, table_name, p.first, p.second);
    if (!status.ok()) {
      return status;
    }
  }

  return Status();
}

StatusOr<bool> HasInMemoryRowBool(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key) {
  auto utilities =
      std::static_pointer_cast<InMemoryTableOperations>(table->GetUtilities());
  auto column_family_it = utilities->find(column_family);
  if (column_family_it == utilities->end()) {
    return NotFoundError(
        "column family not found in table",
        GCP_ERROR_INFO().WithMetadata("column family", column_family));
  }

  auto const& cf =
      std::static_pointer_cast<InMemoryColumnFamily>(column_family_it->second);
  auto column_family_row_it = cf->find(row_key);
  if (column_family_row_it == cf->end()) {
    return false;
  }

  return true;
}

StatusOr<bool> HasPersistentRowBool(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key) {
  auto utilities = std::static_pointer_cast<PersistentTableOperations>(
      table->GetUtilities());
  auto column_family_it = utilities->find(column_family);
  if (column_family_it == utilities->end()) {
    return NotFoundError(
        "column family not found in table",
        GCP_ERROR_INFO().WithMetadata("column family", column_family));
  }

  auto const& cf = std::static_pointer_cast<PersistentColumnFamily>(
      column_family_it->second);
  auto iter = utilities->GetIterator(cf);
  iter->Seek(row_key);
  if (!iter->Valid()) {
    return false;
  }
  auto decoded_key = KeyCoder::Decode(iter->key().ToString()).value();
  if (decoded_key.row != row_key) return false;

  return true;
}

TEST(InMemoryDropRowRange, DropAll) {
  auto const* const table_name = "drw_projects/test/instances/test/tables/test";
  std::vector<std::string> column_families = {"column_family_1",
                                              "column_family_2"};

  auto maybe_table = CreateTable(table_name, column_families, false);
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  std::map<std::string, std::vector<SetCellParams>> params = {
      {"0",
       {{column_families[0], "column_1", 1000, "data_0"},
        {column_families[1], "column_1", 3000, "data_2"}}},
      {"1",
       {{column_families[0], "column_1", 2000, "data_1"},
        {column_families[1], "column_1", 4000, "data_3"}}}};

  ASSERT_STATUS_OK(SetCellsInMultipleRows(table, table_name, params));

  ::google::bigtable::admin::v2::DropRowRangeRequest request;
  request.set_name(table_name);
  request.set_delete_all_data_from_table(true);

  auto status = table->DropRowRange(request);
  ASSERT_STATUS_OK(status);

  for (auto& p : params) {
    for (auto& set_cell_params : p.second) {
      auto status_or = HasInMemoryRowBool(
          table, set_cell_params.column_family_name, p.first);
      ASSERT_STATUS_OK(status_or);
      ASSERT_FALSE(status_or.value());
    }
  }
}

TEST(PersistentDropRowRange, DropAll) {
  std::string const table_name = "drw_projects/test/instances/test/tables/test";
  std::filesystem::remove_all("/tmp/" + table_name);
  std::vector<std::string> column_families = {"column_family_1",
                                              "column_family_2"};

  auto maybe_table = CreateTable(table_name, column_families, true);
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  std::map<std::string, std::vector<SetCellParams>> params = {
      {"0",
       {{column_families[0], "column_1", 1000, "data_0"},
        {column_families[1], "column_1", 3000, "data_2"}}},
      {"1",
       {{column_families[0], "column_1", 2000, "data_1"},
        {column_families[1], "column_1", 4000, "data_3"}}}};

  ASSERT_STATUS_OK(SetCellsInMultipleRows(table, table_name, params));

  ::google::bigtable::admin::v2::DropRowRangeRequest request;
  request.set_name(table_name);
  request.set_delete_all_data_from_table(true);

  auto status = table->DropRowRange(request);
  ASSERT_STATUS_OK(status);

  for (auto& p : params) {
    for (auto& set_cell_params : p.second) {
      auto status_or = HasPersistentRowBool(
          table, set_cell_params.column_family_name, p.first);
      ASSERT_STATUS_OK(status_or);
      ASSERT_FALSE(status_or.value());
    }
  }
  std::filesystem::remove_all("/tmp/drw_projects");
}

TEST(InMemoryDropRowRange, DropSome) {
  auto const* const table_name = "drw_projects/test/instances/test/tables/test";
  std::vector<std::string> column_families = {"column_family_1",
                                              "column_family_2"};

  auto maybe_table = CreateTable(table_name, column_families, false);
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  std::map<std::string, std::vector<SetCellParams>> params = {
      {"a",
       {
           {column_families[0], "column_1", 1000, "data_0"},
       }},
      {"aa",
       {{column_families[0], "column_1", 2000, "data_1"},
        {column_families[1], "column_1", 5000, "data_5"}}},
      {"aaa", {{column_families[0], "column_1", 3000, "data_2"}}},
      {"aab", {{column_families[0], "column_1", 4000, "data_3"}}},
      {"ab", {{column_families[1], "column_1", 6000, "data_6"}}},
  };

  ASSERT_STATUS_OK(SetCellsInMultipleRows(table, table_name, params));

  ::google::bigtable::admin::v2::DropRowRangeRequest request;
  request.set_name(table_name);
  std::string prefix = "aa";
  request.set_row_key_prefix(prefix);

  auto status = table->DropRowRange(request);
  ASSERT_STATUS_OK(status);

  for (auto& p : params) {
    for (auto& set_cell_params : p.second) {
      if (absl::StartsWith(p.first, prefix)) {
        auto status_or = HasInMemoryRowBool(
            table, set_cell_params.column_family_name, p.first);
        ASSERT_STATUS_OK(status_or);
        ASSERT_FALSE(status_or.value());
      } else {
        auto status_or = HasInMemoryRowBool(
            table, set_cell_params.column_family_name, p.first);
        ASSERT_STATUS_OK(status_or);
        ASSERT_TRUE(status_or.value());
      }
    }
  }
}

TEST(PersistentDropRowRange, DropSome) {
  std::string const table_name = "drw_projects/test/instances/test/tables/test";
  std::filesystem::remove_all("/tmp/" + table_name);
  std::vector<std::string> column_families = {"column_family_1",
                                              "column_family_2"};

  auto maybe_table = CreateTable(table_name, column_families, true);
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  std::map<std::string, std::vector<SetCellParams>> params = {
      {"a",
       {
           {column_families[0], "column_1", 1000, "data_0"},
       }},
      {"aa",
       {{column_families[0], "column_1", 2000, "data_1"},
        {column_families[1], "column_1", 5000, "data_5"}}},
      {"aaa", {{column_families[0], "column_1", 3000, "data_2"}}},
      {"aab", {{column_families[0], "column_1", 4000, "data_3"}}},
      {"ab", {{column_families[1], "column_1", 6000, "data_6"}}},
  };

  ASSERT_STATUS_OK(SetCellsInMultipleRows(table, table_name, params));

  ::google::bigtable::admin::v2::DropRowRangeRequest request;
  request.set_name(table_name);
  std::string prefix = "aa";
  request.set_row_key_prefix(prefix);

  auto status = table->DropRowRange(request);
  ASSERT_STATUS_OK(status);

  for (auto& p : params) {
    for (auto& set_cell_params : p.second) {
      if (absl::StartsWith(p.first, prefix)) {
        auto status_or = HasPersistentRowBool(
            table, set_cell_params.column_family_name, p.first);
        ASSERT_STATUS_OK(status_or);
        ASSERT_FALSE(status_or.value());
      } else {
        auto status_or = HasPersistentRowBool(
            table, set_cell_params.column_family_name, p.first);
        ASSERT_STATUS_OK(status_or);
        ASSERT_TRUE(status_or.value());
      }
    }
  }
  std::filesystem::remove_all("/tmp/drw_projects");
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
