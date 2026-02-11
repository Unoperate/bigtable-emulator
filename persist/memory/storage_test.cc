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

/**
 * @file storage_test.cc
 * @brief Unit tests for in-memory storage implementation (MemoryStorage,
 * MemoryStorageRowTX).
 */

#include "persist/memory/storage.h"
#include "persist/utils/test_utils.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

/** Tests CreateTable, RowTransaction, SetCell, DeleteRowColumn,
 * DeleteRowFromColumnFamily, DeleteRowFromAllColumnFamilies. */
TEST(MemoryStorage, CreateTableBasic) {
  MemoryStorageTestManager m;

  auto storage = m.getStorage();

  auto const table_name1 = m.createTestTable({"cf_1"});
  auto const table_name2 = m.createTestTable({"cf_1"});
  EXPECT_TABLE_NAMES_PREFIX(m.testTablePrefix(), table_name1, table_name2);

  auto const write_tx = TXN(storage, table_name1, "row_1");
  auto t = m.now();
  auto t1 = t++;
  EXPECT_OK(write_tx->SetCell("cf_1", "col_1", t1, "value_1"));
  EXPECT_OK(write_tx->Commit());

  // Test if we do not mirror row inserts across tables
  EXPECT_ROWS(m, table_name1, {"cf_1.row_1.col_1", t1, "value_1"});
  EXPECT_ROWS(m, table_name2);

  auto const write_tx2 = TXN(storage, table_name2, "row_1");
  auto t2 = t++;
  EXPECT_OK(write_tx2->SetCell("cf_1", "col_1", t1, "value_2a"));
  EXPECT_OK(write_tx2->SetCell("cf_1", "col_1", t2, "value_2b"));
  EXPECT_OK(write_tx2->SetCell("cf_1", "col_2", t2, "value_3"));
  EXPECT_OK(write_tx2->Commit());

  // Verify that we have new rows
  EXPECT_ROWS(m, table_name1, {"cf_1.row_1.col_1", t1, "value_1"});
  EXPECT_ROWS(m, table_name2, {"cf_1.row_1.col_1", t2, "value_2b"},
              {"cf_1.row_1.col_1", t1, "value_2a"},
              {"cf_1.row_1.col_2", t2, "value_3"});

  auto t3 = t++;
  auto const del_tx3 = TXN(storage, table_name2, "row_1");
  EXPECT_OK(del_tx3->DeleteRowColumn("cf_1", "col_1", t1, t3));
  EXPECT_OK(del_tx3->Commit());

  EXPECT_ROWS(m, table_name2, {"cf_1.row_1.col_2", t2, "value_3"});

  // We delete entire row
  auto const del_tx4 = TXN(storage, table_name2, "row_1");
  del_tx4->DeleteRowFromColumnFamily("cf_1");
  del_tx4->Commit();

  // Table should be empty, second table should be unchanged
  EXPECT_ROWS(m, table_name2);
  EXPECT_ROWS(m, table_name1, {"cf_1.row_1.col_1", t1, "value_1"});

  // Empty second table as well
  auto const del_tx5 = TXN(storage, table_name1, "row_1");
  del_tx5->DeleteRowFromAllColumnFamilies();
  del_tx5->Commit();
  EXPECT_ROWS(m, table_name1);
}

}  // anonymous namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
