// Copyright 2026 Google LLC
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

#include "table.h"
#include "storage.h"
#include "constants.h"
#include "google/cloud/testing_util/status_matchers.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <gtest/gtest.h>
#include <unistd.h>
#include <atomic>
#include <filesystem>
#include <sstream>
#include <string>
#include <system_error>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

namespace btadmin = ::google::bigtable::admin::v2;

std::string MakeUniqueDbPath() {
  static std::atomic<int> counter{0};
  auto const id = counter.fetch_add(1);
  auto const path = std::filesystem::path(::testing::TempDir()) /
                    ("table_persistence_test_" + std::to_string(getpid()) +
                     "_" + std::to_string(id));
  return path.string();
}

std::string MakeUniqueTableName() {
  static std::atomic<int> counter{0};
  auto const id = counter.fetch_add(1);
  return "projects/test/instances/test/tables/persist_" +
         std::to_string(getpid()) + "_" + std::to_string(id);
}

int CountManifestMatches(std::string const& manifest,
                         std::string const& expected_line) {
  std::istringstream iss(manifest);
  std::string line;
  int count = 0;
  while (std::getline(iss, line)) {
    if (Trim(line) == expected_line) ++count;
  }
  return count;
}

bool ManifestContains(std::string const& manifest, std::string const& line) {
  return CountManifestMatches(manifest, line) > 0;
}

btadmin::Table ReadPersistedSchema(Storage& storage,
                                   std::string const& table_name) {
  btadmin::Table persisted;
  auto const blob = storage.GetRow(kTablesPrefix + table_name);
  EXPECT_FALSE(blob.empty());
  EXPECT_TRUE(persisted.ParseFromString(blob));
  return persisted;
}

class TablePersistenceTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    db_path_ = MakeUniqueDbPath();
    std::error_code ec;
    std::filesystem::remove_all(db_path_, ec);

    ASSERT_EQ(0, InitGlobalStorage(db_path_.c_str()));
    ASSERT_NE(nullptr, GetGlobalStorage());
  }

  static void TearDownTestSuite() {
    CloseGlobalStorage();
    std::error_code ec;
    std::filesystem::remove_all(db_path_, ec);
  }

  Storage& storage() {
    auto* s = GetGlobalStorage();
    EXPECT_NE(nullptr, s);
    return *s;
  }

  static std::string db_path_;
};

std::string TablePersistenceTest::db_path_;

TEST_F(TablePersistenceTest, CreatePersistsSchemaAndManifest) {
  auto const table_name = MakeUniqueTableName();

  btadmin::Table schema;
  schema.set_name(table_name);
  (*schema.mutable_column_families())["cf1"] = btadmin::ColumnFamily{};

  auto maybe_table = Table::Create(schema);
  ASSERT_STATUS_OK(maybe_table);

  auto const manifest = storage().GetRow(kManifestKey);
  EXPECT_TRUE(ManifestContains(manifest, kTablesPrefix + table_name));

  auto persisted = ReadPersistedSchema(storage(), table_name);
  EXPECT_EQ(table_name, persisted.name());
  EXPECT_NE(persisted.column_families().find("cf1"),
            persisted.column_families().end());
}

TEST_F(TablePersistenceTest, CreateNormalizesPrefixedColumnFamilyIds) {
  auto const table_name = MakeUniqueTableName();
  auto const prefixed_cf = table_name + "/cf_pref";

  btadmin::Table schema;
  schema.set_name(table_name);
  (*schema.mutable_column_families())[prefixed_cf] = btadmin::ColumnFamily{};

  auto maybe_table = Table::Create(schema);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  EXPECT_NE(table->find("cf_pref"), table->end());
  EXPECT_EQ(table->find(prefixed_cf), table->end());

  auto persisted = ReadPersistedSchema(storage(), table_name);
  EXPECT_NE(persisted.column_families().find("cf_pref"),
            persisted.column_families().end());
  EXPECT_EQ(persisted.column_families().find(prefixed_cf),
            persisted.column_families().end());
}

TEST_F(TablePersistenceTest, ModifyColumnFamiliesCreateIsPersisted) {
  auto const table_name = MakeUniqueTableName();

  btadmin::Table schema;
  schema.set_name(table_name);
  (*schema.mutable_column_families())["cf1"] = btadmin::ColumnFamily{};

  auto maybe_table = Table::Create(schema);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  btadmin::ModifyColumnFamiliesRequest request;
  request.set_name(table_name);
  auto* mod = request.add_modifications();
  mod->set_id("cf2");
  mod->mutable_create();

  auto maybe_updated = table->ModifyColumnFamilies(request);
  ASSERT_STATUS_OK(maybe_updated);

  auto persisted = ReadPersistedSchema(storage(), table_name);
  EXPECT_NE(persisted.column_families().find("cf1"),
            persisted.column_families().end());
  EXPECT_NE(persisted.column_families().find("cf2"),
            persisted.column_families().end());
}

TEST_F(TablePersistenceTest, UpdateWithMaskPersistsDeletionProtection) {
  auto const table_name = MakeUniqueTableName();

  btadmin::Table schema;
  schema.set_name(table_name);
  (*schema.mutable_column_families())["cf1"] = btadmin::ColumnFamily{};

  auto maybe_table = Table::Create(schema);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  btadmin::Table patch;
  patch.set_deletion_protection(true);
  google::protobuf::FieldMask mask;
  mask.add_paths("deletion_protection");

  ASSERT_STATUS_OK(table->Update(patch, mask));

  auto persisted = ReadPersistedSchema(storage(), table_name);
  EXPECT_TRUE(persisted.deletion_protection());
}

TEST_F(TablePersistenceTest, RepeatedCreateDoesNotDuplicateManifestEntry) {
  auto const table_name = MakeUniqueTableName();
  auto const expected_manifest_line = kTablesPrefix + table_name;

  btadmin::Table schema;
  schema.set_name(table_name);
  (*schema.mutable_column_families())["cf1"] = btadmin::ColumnFamily{};

  ASSERT_STATUS_OK(Table::Create(schema));
  ASSERT_STATUS_OK(Table::Create(schema));

  auto const manifest = storage().GetRow(kManifestKey);
  EXPECT_EQ(1, CountManifestMatches(manifest, expected_manifest_line));
}

TEST_F(TablePersistenceTest, FailedMutateRowRollsBackPersistedSetCell) {
  auto const table_name = MakeUniqueTableName();
  auto const row_key = "row-rollback";

  btadmin::Table schema;
  schema.set_name(table_name);
  (*schema.mutable_column_families())["cf1"] = btadmin::ColumnFamily{};

  auto maybe_table = Table::Create(schema);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  google::bigtable::v2::MutateRowRequest request;
  request.set_table_name(table_name);
  request.set_row_key(row_key);

  auto* good_mutation = request.add_mutations()->mutable_set_cell();
  good_mutation->set_family_name("cf1");
  good_mutation->set_column_qualifier("col1");
  good_mutation->set_timestamp_micros(1000000);
  good_mutation->set_value("value-1");

  auto* failing_mutation = request.add_mutations()->mutable_set_cell();
  failing_mutation->set_family_name("missing_cf");
  failing_mutation->set_column_qualifier("col2");
  failing_mutation->set_timestamp_micros(2000000);
  failing_mutation->set_value("value-2");

  EXPECT_FALSE(table->MutateRow(request).ok());
  EXPECT_FALSE(storage().RowExists(table_name, row_key));
}

}  // namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
