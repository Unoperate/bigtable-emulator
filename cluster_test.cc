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

#include "cluster.h"
#include "persist/rocksdb/storage.h"
#include "persist/utils/test_utils.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

TEST(Cluster, BasicTableOperations) {
  RocksDBStorageTestManager m;
  std::shared_ptr<Cluster> cluster = std::make_shared<Cluster>(m.getStorage());

  auto const* const table_name = "projects/test/instances/test/tables/test";
  auto const* const column_family_name = "test_column_family";
  auto const* const row_key = "0";
  auto const* const column_qualifier = "column_1";
  auto timestamp_micros = 1000;
  auto const* const true_mutation_value = "set by a true mutation";
  auto const* const false_mutation_value = "set by a false mutation";

  std::vector<std::string> column_families = {column_family_name};
  ::google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  for (auto& column_family_name : column_families) {
    (*schema.mutable_column_families())[column_family_name] =
        ::google::bigtable::admin::v2::ColumnFamily();
  }
  auto maybe_table = cluster->CreateTable(table_name, schema);
  EXPECT_TRUE(maybe_table.ok());

  ::google::bigtable::admin::v2::Table_View view =
      ::google::bigtable::admin::v2::Table_View_FULL;
  auto maybe_tables = cluster->ListTables("projects/test/instances/test", view);
  ASSERT_TRUE(maybe_tables.ok());

  std::vector<std::string> table_names;
  std::transform(maybe_tables.value().begin(), maybe_tables.value().end(),
                 std::back_inserter(table_names),
                 [](auto el) -> auto { return el.name(); });
  EXPECT_EQ(table_names, std::vector<std::string>{
                             "projects/test/instances/test/tables/test"});

  auto get_table = cluster->GetTable(table_name, view);
  EXPECT_TRUE(get_table.ok());
  EXPECT_EQ(get_table.value().name(), table_name);

  // Delete and list everything again
  auto del_table = cluster->DeleteTable(table_name);
  EXPECT_OK(del_table);

  maybe_tables = cluster->ListTables("projects/test/instances/test", view);
  EXPECT_TRUE(maybe_tables.ok());

  table_names.clear();
  std::transform(maybe_tables.value().begin(), maybe_tables.value().end(),
                 std::back_inserter(table_names),
                 [](auto el) -> auto { return el.name(); });
  ASSERT_EQ(table_names, std::vector<std::string>{});

  // Check existence
  assert(!cluster->HasTable(table_name));
  maybe_table = cluster->CreateTable(table_name, schema);
  EXPECT_TRUE(maybe_table.ok());

  // Check existence
  assert(cluster->HasTable(table_name));

  auto maybe_get_table = cluster->FindTable(table_name);
  if (!maybe_get_table.ok()) {
    DBG("[ClusterTest][BasicTableOperations] FindTable failed table_name={} "
        "error={}",
        table_name, maybe_get_table.status().message());
    return;
  }
  auto primary_table = maybe_get_table.value();

  auto const t = m.now();

  auto mut_request = google::bigtable::v2::CheckAndMutateRowRequest();
  auto mut1 = mut_request.add_false_mutations();
  auto mut1_sc = mut1->mutable_set_cell();
  mut1_sc->set_family_name(column_family_name);
  mut1_sc->set_column_qualifier(column_qualifier);
  mut1_sc->set_timestamp_micros(RocksDBStorageTestManager::toMicros(t));
  mut1_sc->set_value("TEST_ROW_VALUE");
  mut_request.set_row_key("TEST_ROW");

  auto check_and_mutate = primary_table->CheckAndMutateRow(mut_request);
  ASSERT_TRUE(check_and_mutate.ok());

  EXPECT_ROWS(
      m, table_name,
      {absl::StrCat(column_family_name, ".", "TEST_ROW", ".", column_qualifier),
       t, "TEST_ROW_VALUE"});
}

}  // anonymous namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
