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

#include "storage.h"
#include "constants.h"
#include <gtest/gtest.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace {

namespace bt_emulator = ::google::cloud::bigtable::emulator;

using bt_emulator::CalculatePrefixEnd;
using bt_emulator::Storage;
using bt_emulator::Trim;

std::string MakeUniqueDbPath() {
  static std::atomic<int> counter{0};
  auto const id = counter.fetch_add(1);
  auto const path = std::filesystem::path(::testing::TempDir()) /
                    ("storage_test_" + std::to_string(getpid()) + "_" +
                     std::to_string(id));
  return path.string();
}

std::size_t CountKeysWithPrefix(Storage& storage, std::string const& cf_name,
                                std::string const& prefix) {
  std::unique_ptr<rocksdb::Iterator> it(storage.NewIterator(cf_name));
  if (!it) return 0;

  std::size_t count = 0;
  for (it->Seek(prefix); it->Valid(); it->Next()) {
    auto const key = it->key().ToString();
    if (key.rfind(prefix, 0) != 0) break;
    ++count;
  }
  return count;
}

class StorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = MakeUniqueDbPath();
    storage_ = std::make_unique<Storage>(db_path_);
  }

  void TearDown() override {
    storage_.reset();
    std::error_code ec;
    std::filesystem::remove_all(db_path_, ec);
  }

  std::string db_path_;
  std::unique_ptr<Storage> storage_;
};

TEST_F(StorageTest, PutRowGetRowAndDeleteRow) {
  EXPECT_TRUE(storage_->PutRow("meta-key", "meta-value"));
  EXPECT_EQ("meta-value", storage_->GetRow("meta-key"));

  EXPECT_TRUE(storage_->DeleteRow("meta-key"));
  EXPECT_EQ("", storage_->GetRow("meta-key"));
}

TEST_F(StorageTest, PutBatchWritesAllEntriesToDefaultColumnFamily) {
  std::vector<std::pair<std::string, std::string>> kvs = {
      {"k1", "v1"},
      {"k2", "v2"},
      {"k3", "v3"},
  };

  EXPECT_TRUE(storage_->PutBatch(kvs));
  EXPECT_EQ("v1", storage_->GetRow("k1"));
  EXPECT_EQ("v2", storage_->GetRow("k2"));
  EXPECT_EQ("v3", storage_->GetRow("k3"));
}

TEST_F(StorageTest, PutCellCreatesColumnFamilyAndRowIsDiscoverable) {
  auto const table = "projects/p/instances/i/tables/t1";
  auto const cf = std::string(table) + "/cf1";
  auto const row = "row-1";
  auto const qualifier = "col-1";
  auto const ts = std::chrono::milliseconds(123);
  auto const prefix = "/tables/" + std::string(table) + "/" + row + "/";

  EXPECT_FALSE(storage_->CFExists(cf));
  EXPECT_TRUE(storage_->PutCell(table, row, cf, qualifier, ts, "value-1"));
  EXPECT_TRUE(storage_->CFExists(cf));
  EXPECT_TRUE(storage_->RowExists(table, row));
  EXPECT_TRUE(storage_->RowExistsInCF(table, row, cf));
  EXPECT_EQ(1U, CountKeysWithPrefix(*storage_, cf, prefix));
}

TEST_F(StorageTest, DeleteColumnRemovesOnlyTargetQualifier) {
  auto const table = "projects/p/instances/i/tables/t2";
  auto const cf = std::string(table) + "/cf1";
  auto const row = "row-1";

  EXPECT_TRUE(
      storage_->PutCell(table, row, cf, "c1", std::chrono::milliseconds(1), "v1"));
  EXPECT_TRUE(
      storage_->PutCell(table, row, cf, "c1", std::chrono::milliseconds(2), "v2"));
  EXPECT_TRUE(
      storage_->PutCell(table, row, cf, "c2", std::chrono::milliseconds(3), "v3"));

  auto const c1_prefix =
      "/tables/" + std::string(table) + "/" + row + "/c1/";
  auto const c2_prefix =
      "/tables/" + std::string(table) + "/" + row + "/c2/";

  EXPECT_EQ(2U, CountKeysWithPrefix(*storage_, cf, c1_prefix));
  EXPECT_EQ(1U, CountKeysWithPrefix(*storage_, cf, c2_prefix));

  storage_->DeleteColumn(table, row, cf, "c1");

  EXPECT_EQ(0U, CountKeysWithPrefix(*storage_, cf, c1_prefix));
  EXPECT_EQ(1U, CountKeysWithPrefix(*storage_, cf, c2_prefix));
  EXPECT_TRUE(storage_->RowExistsInCF(table, row, cf));
}

TEST_F(StorageTest, DeleteCFRowAffectsOnlySelectedColumnFamily) {
  auto const table = "projects/p/instances/i/tables/t3";
  auto const cf1 = std::string(table) + "/cf1";
  auto const cf2 = std::string(table) + "/cf2";
  auto const row = "row-1";

  EXPECT_TRUE(
      storage_->PutCell(table, row, cf1, "c1", std::chrono::milliseconds(1), "v1"));
  EXPECT_TRUE(
      storage_->PutCell(table, row, cf2, "c1", std::chrono::milliseconds(1), "v2"));

  EXPECT_TRUE(storage_->DeleteCFRow(table, row, cf1));
  EXPECT_FALSE(storage_->RowExistsInCF(table, row, cf1));
  EXPECT_TRUE(storage_->RowExistsInCF(table, row, cf2));
  EXPECT_TRUE(storage_->RowExists(table, row));

  EXPECT_FALSE(storage_->DeleteCFRow(table, row, std::string(table) + "/missing"));
}

TEST_F(StorageTest, DeleteRowRemovesDataAcrossAllColumnFamiliesForThatRow) {
  auto const table = "projects/p/instances/i/tables/t4";
  auto const cf1 = std::string(table) + "/cf1";
  auto const cf2 = std::string(table) + "/cf2";
  auto const row1 = "row-1";
  auto const row2 = "row-2";

  EXPECT_TRUE(
      storage_->PutCell(table, row1, cf1, "c1", std::chrono::milliseconds(1), "v1"));
  EXPECT_TRUE(
      storage_->PutCell(table, row1, cf2, "c1", std::chrono::milliseconds(1), "v2"));
  EXPECT_TRUE(
      storage_->PutCell(table, row2, cf2, "c1", std::chrono::milliseconds(1), "v3"));

  storage_->DeleteRow(table, row1);

  EXPECT_FALSE(storage_->RowExists(table, row1));
  EXPECT_TRUE(storage_->RowExists(table, row2));
  EXPECT_TRUE(storage_->RowExistsInCF(table, row2, cf2));
}

TEST_F(StorageTest, DeleteTableUpdatesManifestAndDropsOnlyThatTablesFamilies) {
  auto const table1 = "projects/p/instances/i/tables/t5";
  auto const table2 = "projects/p/instances/i/tables/t6";
  auto const cf1 = std::string(table1) + "/cf1";
  auto const cf2 = std::string(table2) + "/cf1";
  auto const row = "row-1";

  auto const manifest = kTablesPrefix + std::string(table1) + "\n" +
                        kTablesPrefix + std::string(table2) + "\n";
  EXPECT_TRUE(storage_->PutRow(kManifestKey, manifest));
  EXPECT_TRUE(storage_->PutRow(kTablesPrefix + std::string(table1), "schema-1"));
  EXPECT_TRUE(storage_->PutRow(kTablesPrefix + std::string(table2), "schema-2"));

  EXPECT_TRUE(
      storage_->PutCell(table1, row, cf1, "c1", std::chrono::milliseconds(1), "v1"));
  EXPECT_TRUE(
      storage_->PutCell(table2, row, cf2, "c1", std::chrono::milliseconds(1), "v2"));

  storage_->DeleteTable(table1);

  EXPECT_FALSE(storage_->CFExists(cf1));
  EXPECT_TRUE(storage_->CFExists(cf2));
  EXPECT_EQ("", storage_->GetRow(kTablesPrefix + std::string(table1)));
  EXPECT_EQ("schema-2", storage_->GetRow(kTablesPrefix + std::string(table2)));
  EXPECT_EQ(kTablesPrefix + std::string(table2) + "\n",
            storage_->GetRow(kManifestKey));
  EXPECT_TRUE(storage_->RowExistsInCF(table2, row, cf2));
}

TEST_F(StorageTest, ReopenPreservesDefaultAndNamedColumnFamilyData) {
  auto const table = "projects/p/instances/i/tables/t7";
  auto const cf = std::string(table) + "/cf1";
  auto const row = "row-1";

  EXPECT_TRUE(storage_->PutRow("meta", "value"));
  EXPECT_TRUE(
      storage_->PutCell(table, row, cf, "c1", std::chrono::milliseconds(7), "v7"));

  storage_.reset();
  storage_ = std::make_unique<Storage>(db_path_);

  EXPECT_EQ("value", storage_->GetRow("meta"));
  EXPECT_TRUE(storage_->CFExists(cf));
  EXPECT_TRUE(storage_->RowExistsInCF(table, row, cf));
}

TEST(StorageHelpersTest, TrimRemovesOnlyNewlineAndCarriageReturnAtEdges) {
  EXPECT_EQ("abc", Trim("\nabc\r\n"));
  EXPECT_EQ(" abc ", Trim(" abc "));
  EXPECT_EQ("", Trim("\n\r"));
}

TEST(StorageHelpersTest, CalculatePrefixEndHandlesSimpleAndTrailingFFCases) {
  EXPECT_EQ("abd", CalculatePrefixEnd("abc"));

  std::string with_trailing_ff = "ab";
  with_trailing_ff.push_back(static_cast<char>(0xFF));
  EXPECT_EQ("ac", CalculatePrefixEnd(with_trailing_ff));

  std::string all_ff(1, static_cast<char>(0xFF));
  EXPECT_EQ(std::string(1, static_cast<char>(0xFF)),
            CalculatePrefixEnd(all_ff));
}

}  // namespace
