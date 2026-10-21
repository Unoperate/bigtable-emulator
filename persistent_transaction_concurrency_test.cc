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

#include "google/cloud/testing_util/status_matchers.h"
#include "google/cloud/status.h"
#include "test_util.h"
#include <google/bigtable/v2/bigtable.pb.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <future>
#include <string>
#include <thread>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

TEST(PersistentTransactionConcurrency, ConcurrentSetCellSameRow) {
  auto const* const table_name =
      "persistent_transactions_projects/test/instances/test/tables/test";
  auto const* const row_key = "row_0";
  auto const* const column_family_name = "column_family";
  auto const* const column_qualifier = "col";

  std::vector<std::string> column_families = {column_family_name};
  std::filesystem::remove_all("/tmp/persistent_transactions_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest req1;
  req1.set_table_name(table_name);
  req1.set_row_key(row_key);
  auto* mut1 = req1.add_mutations();
  auto* set1 = mut1->mutable_set_cell();
  set1->set_family_name(column_family_name);
  set1->set_column_qualifier(column_qualifier);
  set1->set_timestamp_micros(1000);
  set1->set_value("value_1");

  ::google::bigtable::v2::MutateRowRequest req2;
  req2.set_table_name(table_name);
  req2.set_row_key(row_key);
  auto* mut2 = req2.add_mutations();
  auto* set2 = mut2->mutable_set_cell();
  set2->set_family_name(column_family_name);
  set2->set_column_qualifier(column_qualifier);
  set2->set_timestamp_micros(2000);
  set2->set_value("value_2");

  std::promise<void> start_signal;
  auto start_future = start_signal.get_future().share();
  std::promise<Status> res1_promise;
  std::promise<Status> res2_promise;
  auto res1_future = res1_promise.get_future();
  auto res2_future = res2_promise.get_future();

  std::thread t1([&] {
    start_future.wait();
    res1_promise.set_value(table->MutateRow(req1));
  });
  std::thread t2([&] {
    start_future.wait();
    res2_promise.set_value(table->MutateRow(req2));
  });

  start_signal.set_value();
  t1.join();
  t2.join();

  ASSERT_STATUS_OK(res1_future.get());
  ASSERT_STATUS_OK(res2_future.get());
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 1000, "value_1"));
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 2000, "value_2"));

  std::filesystem::remove_all("/tmp/persistent_transactions_projects");
}

TEST(PersistentTransactionConcurrency, ConcurrentSetAndDeleteSameRow) {
  auto const* const table_name =
      "persistent_transactions_projects/test/instances/test/tables/test";
  auto const* const row_key = "row_0";
  auto const* const column_family_name = "column_family";
  auto const* const column_qualifier = "col";

  std::vector<std::string> column_families = {column_family_name};
  std::filesystem::remove_all("/tmp/persistent_transactions_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> seed = {
      {column_family_name, column_qualifier, 1000, "value_1"},
      {column_family_name, column_qualifier, 2000, "value_2"}};
  ASSERT_STATUS_OK(SetCells(table, table_name, row_key, seed));

  ::google::bigtable::v2::MutateRowRequest set_req;
  set_req.set_table_name(table_name);
  set_req.set_row_key(row_key);
  auto* set_mut = set_req.add_mutations();
  auto* set_cell = set_mut->mutable_set_cell();
  set_cell->set_family_name(column_family_name);
  set_cell->set_column_qualifier(column_qualifier);
  set_cell->set_timestamp_micros(3000);
  set_cell->set_value("value_3");

  ::google::bigtable::v2::MutateRowRequest del_req;
  del_req.set_table_name(table_name);
  del_req.set_row_key(row_key);
  auto* del_mut = del_req.add_mutations();
  auto* del_family = del_mut->mutable_delete_from_family();
  del_family->set_family_name(column_family_name);

  std::promise<void> start_signal;
  auto start_future = start_signal.get_future().share();
  std::promise<Status> set_promise;
  std::promise<Status> del_promise;
  auto set_future = set_promise.get_future();
  auto del_future = del_promise.get_future();

  std::thread t1([&] {
    start_future.wait();
    set_promise.set_value(table->MutateRow(set_req));
  });
  std::thread t2([&] {
    start_future.wait();
    del_promise.set_value(table->MutateRow(del_req));
  });

  start_signal.set_value();
  t1.join();
  t2.join();

  ASSERT_STATUS_OK(set_future.get());
  ASSERT_STATUS_OK(del_future.get());

  auto cell1 = HasPersistentCell(table, column_family_name, row_key,
                                 column_qualifier, 1000, "value_1");
  auto cell2 = HasPersistentCell(table, column_family_name, row_key,
                                 column_qualifier, 2000, "value_2");
  auto cell3 = HasPersistentCell(table, column_family_name, row_key,
                                 column_qualifier, 3000, "value_3");
  ASSERT_TRUE(cell3.ok() || (!cell1.ok() && !cell2.ok() && !cell3.ok()));

  std::filesystem::remove_all("/tmp/persistent_transactions_projects");
}

TEST(PersistentTransactionConcurrency, ConcurrentIndependentColumns) {
  auto const* const table_name =
      "persistent_transactions_projects/test/instances/test/tables/test";
  auto const* const row_key = "row_0";
  auto const* const column_family_name = "column_family";
  auto const* const column_qualifier_a = "col_a";
  auto const* const column_qualifier_b = "col_b";

  std::vector<std::string> column_families = {column_family_name};
  std::filesystem::remove_all("/tmp/persistent_transactions_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest req_a;
  req_a.set_table_name(table_name);
  req_a.set_row_key(row_key);
  auto* mut_a = req_a.add_mutations();
  auto* set_a = mut_a->mutable_set_cell();
  set_a->set_family_name(column_family_name);
  set_a->set_column_qualifier(column_qualifier_a);
  set_a->set_timestamp_micros(1000);
  set_a->set_value("value_a");

  ::google::bigtable::v2::MutateRowRequest req_b;
  req_b.set_table_name(table_name);
  req_b.set_row_key(row_key);
  auto* mut_b = req_b.add_mutations();
  auto* set_b = mut_b->mutable_set_cell();
  set_b->set_family_name(column_family_name);
  set_b->set_column_qualifier(column_qualifier_b);
  set_b->set_timestamp_micros(1000);
  set_b->set_value("value_b");

  std::promise<void> start_signal;
  auto start_future = start_signal.get_future().share();
  std::promise<Status> a_promise;
  std::promise<Status> b_promise;
  auto a_future = a_promise.get_future();
  auto b_future = b_promise.get_future();

  std::thread t1([&] {
    start_future.wait();
    a_promise.set_value(table->MutateRow(req_a));
  });
  std::thread t2([&] {
    start_future.wait();
    b_promise.set_value(table->MutateRow(req_b));
  });

  start_signal.set_value();
  t1.join();
  t2.join();

  ASSERT_STATUS_OK(a_future.get());
  ASSERT_STATUS_OK(b_future.get());
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier_a, 1000, "value_a"));
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier_b, 1000, "value_b"));

  std::filesystem::remove_all("/tmp/persistent_transactions_projects");
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
