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

#include "google/cloud/internal/big_endian.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "absl/strings/str_format.h"
#include "column_family.h"
#include "table.h"
#include "test_util.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
using ::google::protobuf::TextFormat;
using std::string;

::google::bigtable::admin::v2::ColumnFamily MakeBEAggregateCFProto(
    ::google::bigtable::admin::v2::Type_Aggregate::AggregatorCase aggregator) {
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto* value_type = column_family.mutable_value_type();
  auto* kind_aggregate_type = value_type->mutable_aggregate_type();
  switch (aggregator) {
    case google::bigtable::admin::v2::Type::Aggregate::kSum:
      kind_aggregate_type->mutable_sum();
      break;
    case google::bigtable::admin::v2::Type::Aggregate::kMax:
      kind_aggregate_type->mutable_max();
      break;
    case google::bigtable::admin::v2::Type::Aggregate::kMin:
      kind_aggregate_type->mutable_min();
      break;
    default:
      std::abort();
  }
  auto* input_type = kind_aggregate_type->mutable_input_type();
  auto* int64_type = input_type->mutable_int64_type();
  // We need to set the encoding
  auto* encoding = int64_type->mutable_encoding();
  encoding->mutable_big_endian_bytes();

  // What do we do about the state_type?
  // FIXME: Is this correct?
  auto* state_type = kind_aggregate_type->mutable_state_type();
  int64_type = state_type->mutable_int64_type();
  encoding = int64_type->mutable_encoding();
  encoding->mutable_big_endian_bytes();

  return column_family;
}

Status DeleteFromFamilies(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& table_name, std::string const& row_key,
    std::vector<string> const& column_families) {
  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  for (auto column_family : column_families) {
    auto* mutation_request_mutation = mutation_request.add_mutations();
    auto* delete_from_family_mutation =
        mutation_request_mutation->mutable_delete_from_family();
    delete_from_family_mutation->set_family_name(column_family);
  }

  return table->MutateRow(mutation_request);
}

struct DeleteFromColumnParams {
  std::string column_family;
  std::string column_qualifier;
  ::google::bigtable::v2::TimestampRange* timestamp_range;
};

Status DeleteFromColumns(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& table_name, std::string const& row_key,
    std::vector<DeleteFromColumnParams> v) {
  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  for (auto& param : v) {
    auto* mutation_request_mutation = mutation_request.add_mutations();
    auto* delete_from_column_mutation =
        mutation_request_mutation->mutable_delete_from_column();
    delete_from_column_mutation->set_family_name(param.column_family);
    delete_from_column_mutation->set_column_qualifier(param.column_qualifier);
    delete_from_column_mutation->set_allocated_time_range(
        param.timestamp_range);
  }

  return table->MutateRow(mutation_request);
}

Status HasInMemoryColumn(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key,
    std::string const& column_qualifier) {
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
    return NotFoundError("row key not found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("row key", row_key)
                             .WithMetadata("column family", column_family));
  }

  auto& column_family_row = column_family_row_it->second;
  auto column_row_it = column_family_row.find(column_qualifier);
  if (column_row_it == column_family_row.end()) {
    return NotFoundError(
        "no column found with supplied qualifier",
        GCP_ERROR_INFO().WithMetadata("column qualifier", column_qualifier));
  }

  return Status();
}

Status HasPersistentColumn(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key,
    std::string const& column_qualifier) {
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

  auto encoded_key = KeyCoder::PartialEncode(row_key, column_qualifier);
  auto iter = utilities->GetIterator(cf);
  iter->Seek(encoded_key);
  if (!iter->Valid()) {
    return NotFoundError("row key not found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("row key", row_key)
                             .WithMetadata("column family", column_family));
  }

  auto key = iter->key();
  auto decoded_key = KeyCoder::Decode(key.ToString());
  if (decoded_key->row != row_key) {
    return NotFoundError("no row found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("column qualifier", column_qualifier)
                             .WithMetadata("column family", column_family));
  }
  if (decoded_key->col != column_qualifier) {
    return NotFoundError("no column found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("column qualifier", column_qualifier)
                             .WithMetadata("column family", column_family));
  }
  return Status();
}

StatusOr<std::map<std::chrono::milliseconds, std::string>> GetInMemoryColumn(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key,
    std::string const& column_qualifier) {
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
    return NotFoundError("row key not found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("row key", row_key)
                             .WithMetadata("column family", column_family));
  }

  auto& column_family_row = column_family_row_it->second;
  auto column_row_it = column_family_row.find(column_qualifier);
  if (column_row_it == column_family_row.end()) {
    return NotFoundError(
        "no column found with supplied qualifier",
        GCP_ERROR_INFO().WithMetadata("column qualifier", column_qualifier));
  }

  std::map<std::chrono::milliseconds, std::string> ret(
      column_row_it->second.begin(), column_row_it->second.end());

  return ret;
}

StatusOr<std::map<std::chrono::milliseconds, std::string>> GetPersistentColumn(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key,
    std::string const& column_qualifier) {
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

  auto partial_key = KeyCoder::PartialEncode(row_key, column_qualifier);
  auto iter = utilities->GetIterator(cf);
  iter->Seek(partial_key);
  if (!iter->Valid()) {
    return NotFoundError("row key not found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("row key", row_key)
                             .WithMetadata("column family", column_family));
  }

  auto decoded_key = KeyCoder::Decode(iter->key().ToString());
  if (decoded_key->row != row_key) {
    return NotFoundError("no row found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("row key", row_key)
                             .WithMetadata("column family", column_family));
  }
  if (decoded_key->col != column_qualifier) {
    return NotFoundError("no column found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("column qualifier", column_qualifier)
                             .WithMetadata("column family", column_family));
  }

  std::map<std::chrono::milliseconds, std::string> ret;
  while (iter->Valid() && decoded_key->row == row_key &&
         decoded_key->col == column_qualifier) {
    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds(decoded_key->timestamp));
    ret.emplace(ts, iter->value().ToString());

    iter->Next();
    if (iter->Valid()) {
      decoded_key = KeyCoder::Decode(iter->key().ToString());
    }
  }

  return ret;
}

StatusOr<google::bigtable::v2::Column> GetResponseColumn(
    google::bigtable::v2::ReadModifyWriteRowResponse const& resp,
    std::string const& row_key, int family_index, std::string const& qual) {
  if (!resp.has_row()) {
    return NotFoundError(
        "response has no row",
        GCP_ERROR_INFO().WithMetadata("response message", resp.DebugString()));
  }

  if (resp.row().key() != row_key) {
    return InvalidArgumentError(
        "row key does not match",
        GCP_ERROR_INFO().WithMetadata(row_key, resp.row().key()));
  }

  if (family_index < 0) {
    return InvalidArgumentError(
        "supplied family index < 0",
        GCP_ERROR_INFO().WithMetadata("family_index",
                                      absl::StrFormat("%d", family_index)));
  }

  if (family_index > resp.row().families_size() - 1) {
    return InvalidArgumentError(
        "supplied family index is out of range",
        GCP_ERROR_INFO().WithMetadata("family index",
                                      absl::StrFormat("%d", family_index)));
  }

  // Check that column families and column qualifiers in the response
  // are neither empty nor repeated.
  std::set<std::string> families;
  for (int i = 0; i < resp.row().families_size(); i++) {
    auto ret = families.emplace(resp.row().families(i).name());
    // The family name should not be empty and should not be
    // repeated. Neither should the column qualifiers be empty or
    // repeated.
    if (ret.first->empty() || !ret.second) {
      return InvalidArgumentError(
          "empty or repeated family name",
          GCP_ERROR_INFO().WithMetadata("ReadModifyWriteRowResponse",
                                        resp.DebugString()));
    }

    std::set<std::string> column_qualifiers;
    for (auto const& col : resp.row().families(i).columns()) {
      auto ret = column_qualifiers.emplace(col.qualifier());
      if (ret.first->empty() || !ret.second) {
        return InvalidArgumentError(
            "empty or repeated column qualifier",
            GCP_ERROR_INFO().WithMetadata("ReadModifyWriteRowResponse",
                                          resp.DebugString()));
      }
    }
  }

  for (auto const& col : resp.row().families(family_index).columns()) {
    if (col.qualifier() == qual) {
      return col;
    }
  }

  return NotFoundError("column not found",
                       GCP_ERROR_INFO().WithMetadata("qualifier", qual));
}

// Test that SetCell does the right thing when it receives a zero or
// negative timestamp, and that the cell created can be correctly
// deleted if rollback occurs.
//
// In particular:
//
// Supplied with a timestamp of -1, it should store the current system time as
// timestamp.
//
// Supplied with a timestamp of 0, it should store it as is.
//
// Supplied with a timestamp < -1, it should return an error and fail the entire
// mutation chain.
TEST(InMemoryTransactionRollback, ZeroOrNegativeTimestampHandling) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const timestamp_micros = 0;
  auto const* data = "test";

  std::vector<std::string> column_families = {column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);

  auto status_or =
      GetInMemoryColumn(table, column_family_name, row_key, column_qualifier);
  ASSERT_STATUS_OK(status_or.status());
  auto column = status_or.value();
  ASSERT_EQ(1, column.size());
  for (auto const& cell : column) {
    ASSERT_EQ(cell.first.count(), 0);
    ASSERT_EQ(data, cell.second);
  }

  // Test that a mutation with timestamp 0 can be rolled back.
  v.clear();
  v = {{column_family_name, column_qualifier, 0, data},
       {"non_existent_column_family_name_causes_tx_rollback", column_qualifier,
        1000, data}};
  auto const* const row_key_2 = "1";
  status = SetCells(table, table_name, row_key_2, v);
  ASSERT_NE(true, status.ok());
  ASSERT_FALSE(HasInMemoryRow(table, column_family_name, row_key_2).ok());

  // Test that a mutation with timestamp 0 succeeds and stores 0 as
  // the timestamp.
  v.clear();
  v = {
      {column_family_name, column_qualifier, 0, data},
  };
  auto const* const row_key_3 = "2";
  status = SetCells(table, table_name, row_key_3, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasInMemoryCell(table, v[0].column_family_name, row_key_3,
                                   v[0].column_qualifier, 0, v[0].data));

  // Test that a mutation with timestamp < -1 fails
  v.clear();
  v = {
      {column_family_name, column_qualifier, -2, data},
  };
  auto const* const row_key_4 = "3";
  status = SetCells(table, table_name, row_key_4, v);
  ASSERT_FALSE(status.ok());

  // Test that a mutation with timestamp -1 succeeds and stores the
  // system time.
  v.clear();
  v = {
      {column_family_name, column_qualifier, -1, data},
  };
  auto const* const row_key_5 = "4";
  auto system_time_ms_before =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  status = SetCells(table, table_name, row_key_5, v);
  ASSERT_STATUS_OK(status);
  auto column_or = GetInMemoryColumn(table, v[0].column_family_name, row_key_5,
                                     v[0].column_qualifier);
  ASSERT_STATUS_OK(column_or.status());
  auto col = column_or.value();
  ASSERT_EQ(col.size(), 1);
  auto cell_it = col.begin();
  ASSERT_NE(cell_it, col.end());
  ASSERT_EQ(cell_it->second, v[0].data);
  ASSERT_GE(cell_it->first, system_time_ms_before);
}

TEST(PersistentTransactionRollback, ZeroOrNegativeTimestampHandling) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const timestamp_micros = 0;
  auto const* data = "test";

  std::vector<std::string> column_families = {column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);

  auto status_or =
      GetPersistentColumn(table, column_family_name, row_key, column_qualifier);
  ASSERT_STATUS_OK(status_or.status());
  auto column = status_or.value();
  ASSERT_EQ(1, column.size());
  for (auto const& cell : column) {
    ASSERT_EQ(cell.first.count(), 0);
    ASSERT_EQ(data, cell.second);
  }

  // Test that a mutation with timestamp 0 can be rolled back.
  v.clear();
  v = {{column_family_name, column_qualifier, 0, data},
       {"non_existent_column_family_name_causes_tx_rollback", column_qualifier,
        1000, data}};
  auto const* const row_key_2 = "1";
  status = SetCells(table, table_name, row_key_2, v);
  ASSERT_NE(true, status.ok());
  ASSERT_FALSE(HasPersistentRow(table, column_family_name, row_key_2).ok());

  // Test that a mutation with timestamp 0 succeeds and stores 0 as
  // the timestamp.
  v.clear();
  v = {
      {column_family_name, column_qualifier, 0, data},
  };
  auto const* const row_key_3 = "2";
  status = SetCells(table, table_name, row_key_3, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasPersistentCell(table, v[0].column_family_name, row_key_3,
                                     v[0].column_qualifier, 0, v[0].data));

  // Test that a mutation with timestamp < -1 fails
  v.clear();
  v = {
      {column_family_name, column_qualifier, -2, data},
  };
  auto const* const row_key_4 = "3";
  status = SetCells(table, table_name, row_key_4, v);
  ASSERT_FALSE(status.ok());

  // Test that a mutation with timestamp -1 succeeds and stores the
  // system time.
  v.clear();
  v = {
      {column_family_name, column_qualifier, -1, data},
  };
  auto const* const row_key_5 = "4";
  auto system_time_ms_before =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  status = SetCells(table, table_name, row_key_5, v);
  ASSERT_STATUS_OK(status);
  auto column_or = GetPersistentColumn(table, v[0].column_family_name,
                                       row_key_5, v[0].column_qualifier);
  ASSERT_STATUS_OK(column_or.status());
  auto col = column_or.value();
  ASSERT_EQ(col.size(), 1);
  auto cell_it = col.begin();
  ASSERT_NE(cell_it, col.end());
  ASSERT_EQ(cell_it->second, v[0].data);
  ASSERT_GE(cell_it->first, system_time_ms_before);

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Does the SetCell mutation work to set a cell to a specific value?
TEST(InMemoryTransactionRollback, SetCellBasicFunction) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const timestamp_micros = 1234;
  auto const* data = "test";

  std::vector<std::string> column_families = {column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);

  ASSERT_STATUS_OK(status);

  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, timestamp_micros, data));
}

TEST(PersistentTransactionRollback, SetCellBasicFunction) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const timestamp_micros = 1234;
  auto const* data = "test";

  std::vector<std::string> column_families = {column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);

  ASSERT_STATUS_OK(status);

  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, timestamp_micros, data));

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that an old value is correctly restored in a pre-populated
// cell, when one of a set of SetCell mutations fails after the cell
// had been updated with a new value.
TEST(InMemoryTransactionRollback, TestRestoreValue) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  // The table will be set up with a schema with
  // valid_column_family_name and mutations with this column family
  // name are expected to succeed. We will simulate a transaction
  // failure by setting some other not-pre-provisioned column family
  // name.
  auto const* const valid_column_family_name = "test";
  auto const* const column_qualifier = "test";
  int64_t good_mutation_timestamp_micros = 1000;
  auto const* const good_mutation_data = "expected to succeed";

  std::vector<std::string> column_families = {valid_column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {valid_column_family_name, column_qualifier,
                     good_mutation_timestamp_micros, good_mutation_data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasInMemoryCell(
      table, valid_column_family_name, row_key, column_qualifier,
      good_mutation_timestamp_micros, good_mutation_data));

  // Now atomically try 2 mutations. One modifies the above set cell,
  // and the other one is expected to fail. The test is that
  // RestoreValue will restore the previous value in cell with
  // timestamp 1000.
  std::vector<SetCellParams> w;
  // Everything is the same but we try and modify the value in the cell cell set
  // above.
  p.data = "new data";
  w.push_back(p);

  // Because "invalid_column_family" does not exist in the table
  // schema, a mutation with these SetCell parameters is expected to
  // fail.
  p = {"invalid_column_family", "test2", 1000, "expected to fail"};
  w.push_back(p);

  status = SetCells(table, table_name, row_key, w);
  ASSERT_NE(status.ok(), true);  // The whole mutation chain should
                                 // fail because the 2nd mutation
                                 // contains an invalid column family.

  // And the first mutation should have been rolled back by
  // RestoreValue and so should contain the old value, and not "new
  // data".
  ASSERT_STATUS_OK(HasInMemoryCell(
      table, valid_column_family_name, row_key, column_qualifier,
      good_mutation_timestamp_micros, good_mutation_data));
}

TEST(PersistentTransactionRollback, TestRestoreValue) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  // The table will be set up with a schema with
  // valid_column_family_name and mutations with this column family
  // name are expected to succeed. We will simulate a transaction
  // failure by setting some other not-pre-provisioned column family
  // name.
  auto const* const valid_column_family_name = "test";
  auto const* const column_qualifier = "test";
  int64_t good_mutation_timestamp_micros = 1000;
  auto const* const good_mutation_data = "expected to succeed";

  std::vector<std::string> column_families = {valid_column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {valid_column_family_name, column_qualifier,
                     good_mutation_timestamp_micros, good_mutation_data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasPersistentCell(
      table, valid_column_family_name, row_key, column_qualifier,
      good_mutation_timestamp_micros, good_mutation_data));

  // Now atomically try 2 mutations. One modifies the above set cell,
  // and the other one is expected to fail. The test is that
  // RestoreValue will restore the previous value in cell with
  // timestamp 1000.
  std::vector<SetCellParams> w;
  // Everything is the same but we try and modify the value in the cell cell set
  // above.
  p.data = "new data";
  w.push_back(p);

  // Because "invalid_column_family" does not exist in the table
  // schema, a mutation with these SetCell parameters is expected to
  // fail.
  p = {"invalid_column_family", "test2", 1000, "expected to fail"};
  w.push_back(p);

  status = SetCells(table, table_name, row_key, w);
  ASSERT_NE(status.ok(), true);  // The whole mutation chain should
                                 // fail because the 2nd mutation
                                 // contains an invalid column family.

  // And the first mutation should have been rolled back by
  // RestoreValue and so should contain the old value, and not "new
  // data".
  ASSERT_STATUS_OK(HasPersistentCell(
      table, valid_column_family_name, row_key, column_qualifier,
      good_mutation_timestamp_micros, good_mutation_data));

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that a new cell introduced in a chain of SetCell mutations is
// deleted on rollback if a subsequent mutation fails.
TEST(InMemoryTransactionRollback, DeleteValue) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  // The table will be set up with a schema with
  // valid_column_family_name and mutations with this column family
  // name are expected to succeed. We will simulate a transaction
  // failure by setting some other not-pre-provisioned column family
  // name.
  auto const* const valid_column_family_name = "test";
  std::vector<std::string> column_families = {valid_column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  // To test that we do not delete a row or column that we should not,
  // let us first commit a transaction on the same row where we will
  // do the DeleteValue test.
  std::vector<SetCellParams> v = {
      {valid_column_family_name, "test", 1000, "data"}};
  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasInMemoryCell(table, valid_column_family_name, row_key,
                                   v[0].column_qualifier, v[0].timestamp_micros,
                                   v[0].data));

  // We then setup a transaction chain with 2 SetCells, the first one
  // should succeed to add a new cell and the second one should fail
  // (because it assumes an invalid schema in column family name). We
  // expect the first cell to not exist after the rollback (and of
  // course also no data from the 2nd failing SetCell mutation should
  // exist either).
  v = {{valid_column_family_name, "test", 2000, "new data"},
       {"invalid_column_family_name", "test", 3000, "more new data"}};

  status = SetCells(table, table_name, row_key, v);
  ASSERT_NE(status.ok(), true);  // We expect the chain of mutations to
                                 // fail altogether.
  status =
      HasInMemoryCell(table, v[0].column_family_name, row_key,
                      v[0].column_qualifier, v[0].timestamp_micros, v[0].data);
  ASSERT_NE(status.ok(), true);  // Undo should delete the cell
  status =
      HasInMemoryCell(table, v[1].column_family_name, row_key,
                      v[1].column_qualifier, v[1].timestamp_micros, v[1].data);
  ASSERT_NE(status.ok(), true);  // Also the SetCell with invalid shema
                                 // should not have set anything.
}

TEST(PersistentTransactionRollback, DeleteValue) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  // The table will be set up with a schema with
  // valid_column_family_name and mutations with this column family
  // name are expected to succeed. We will simulate a transaction
  // failure by setting some other not-pre-provisioned column family
  // name.
  auto const* const valid_column_family_name = "test";
  std::vector<std::string> column_families = {valid_column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  // To test that we do not delete a row or column that we should not,
  // let us first commit a transaction on the same row where we will
  // do the DeleteValue test.
  std::vector<SetCellParams> v = {
      {valid_column_family_name, "test", 1000, "data"}};
  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasPersistentCell(table, valid_column_family_name, row_key,
                                     v[0].column_qualifier,
                                     v[0].timestamp_micros, v[0].data));

  // We then setup a transaction chain with 2 SetCells, the first one
  // should succeed to add a new cell and the second one should fail
  // (because it assumes an invalid schema in column family name). We
  // expect the first cell to not exist after the rollback (and of
  // course also no data from the 2nd failing SetCell mutation should
  // exist either).
  v = {{valid_column_family_name, "test", 2000, "new data"},
       {"invalid_column_family_name", "test", 3000, "more new data"}};

  status = SetCells(table, table_name, row_key, v);
  ASSERT_NE(status.ok(), true);  // We expect the chain of mutations to
                                 // fail altogether.
  status = HasPersistentCell(table, v[0].column_family_name, row_key,
                             v[0].column_qualifier, v[0].timestamp_micros,
                             v[0].data);
  ASSERT_NE(status.ok(), true);  // Undo should delete the cell
  status = HasPersistentCell(table, v[1].column_family_name, row_key,
                             v[1].column_qualifier, v[1].timestamp_micros,
                             v[1].data);
  ASSERT_NE(status.ok(), true);  // Also the SetCell with invalid shema
                                 // should not have set anything.

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that if a successful SetCell mutation in a chain of SetCell
// mutations in one transaction introduces a new column but a
// subsequent SetCell mutation fails (we simulate this by passing an
// column family name that is not in the table schema) then the column
// and any of the cells introduced is deleted in the rollback, but
// that any pre-transaction-attemot data in the row is unaffected.
TEST(InMemoryTransactionRollback, DeleteColumn) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  // The table will be set up with a schema with
  // valid_column_family_name and mutations with this column family
  // name are expected to succeed. We will simulate a transaction
  // failure by setting some other not-pre-provisioned column family
  // name.
  auto const* const valid_column_family_name = "test";
  std::vector<std::string> column_families = {valid_column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v = {
      {valid_column_family_name, "test", 1000, "data"}};
  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasInMemoryCell(table, valid_column_family_name, row_key,
                                   v[0].column_qualifier, v[0].timestamp_micros,
                                   v[0].data));

  // Introduce a new column in a chain of SetCell mutations, a
  // subsequent one of which must fail due to an invalid schema
  // assumption (bad column family name).
  v = {{valid_column_family_name, "new_column", 2000, "new data"},
       {"invalid_column_family_name", "test", 3000, "more new data"}};

  status = SetCells(table, table_name, row_key, v);
  ASSERT_NE(status.ok(),
            true);  // We expect the chain of mutations to
                    // fail altogether because the last one must fail.

  // The original column ("test") should still exist.
  status = HasInMemoryColumn(table, valid_column_family_name, row_key, "test");
  ASSERT_STATUS_OK(status);

  // Bit the new column introduced should have been rolled back.
  status = HasInMemoryColumn(table, v[0].column_family_name, row_key,
                             v[0].column_qualifier);
  ASSERT_NE(status.ok(), true);
}

TEST(PersistentTransactionRollback, DeleteColumn) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  // The table will be set up with a schema with
  // valid_column_family_name and mutations with this column family
  // name are expected to succeed. We will simulate a transaction
  // failure by setting some other not-pre-provisioned column family
  // name.
  auto const* const valid_column_family_name = "test";
  std::vector<std::string> column_families = {valid_column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v = {
      {valid_column_family_name, "test", 1000, "data"}};
  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasPersistentCell(table, valid_column_family_name, row_key,
                                     v[0].column_qualifier,
                                     v[0].timestamp_micros, v[0].data));

  // Introduce a new column in a chain of SetCell mutations, a
  // subsequent one of which must fail due to an invalid schema
  // assumption (bad column family name).
  v = {{valid_column_family_name, "new_column", 2000, "new data"},
       {"invalid_column_family_name", "test", 3000, "more new data"}};

  status = SetCells(table, table_name, row_key, v);
  ASSERT_NE(status.ok(),
            true);  // We expect the chain of mutations to
                    // fail altogether because the last one must fail.

  // The original column ("test") should still exist.
  status =
      HasPersistentColumn(table, valid_column_family_name, row_key, "test");
  ASSERT_STATUS_OK(status);

  // Bit the new column introduced should have been rolled back.
  status = HasPersistentColumn(table, v[0].column_family_name, row_key,
                               v[0].column_qualifier);
  ASSERT_NE(status.ok(), true);

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that a chain of SetCell mutations that initially introduces a
// new row, but one of which eventually fails, will end with the whole
// row rolled back.
TEST(InMemoryTransactionRollback, DeleteRow) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  // The table will be set up with a schema with
  // valid_column_family_name and mutations with this column family
  // name are expected to succeed. We will simulate a transaction
  // failure by setting some other not-pre-provisioned column family
  // name.
  auto const* const valid_column_family_name = "test";
  std::vector<std::string> column_families = {valid_column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  // First SetCell should succeed and introduce a new row with key
  // "0". The second one will fail due to bad schema settings. We
  // expect not to find the row after the row mutation call returns.
  std::vector<SetCellParams> v = {
      {valid_column_family_name, "test", 1000, "data"},
      {"invalid_column_family_name", "test", 2000,
       "more new data which should never be written"}};

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_NE(status.ok(),
            true);  // We expect the chain of mutations to
                    // fail altogether because the last one must fail.

  status = HasInMemoryRow(table, valid_column_family_name, row_key);
  ASSERT_NE(status.ok(), true);
}

TEST(PersistentTransactionRollback, DeleteRow) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  // The table will be set up with a schema with
  // valid_column_family_name and mutations with this column family
  // name are expected to succeed. We will simulate a transaction
  // failure by setting some other not-pre-provisioned column family
  // name.
  auto const* const valid_column_family_name = "test";
  std::vector<std::string> column_families = {valid_column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  // First SetCell should succeed and introduce a new row with key
  // "0". The second one will fail due to bad schema settings. We
  // expect not to find the row after the row mutation call returns.
  std::vector<SetCellParams> v = {
      {valid_column_family_name, "test", 1000, "data"},
      {"invalid_column_family_name", "test", 2000,
       "more new data which should never be written"}};

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_NE(status.ok(),
            true);  // We expect the chain of mutations to
  // fail altogether because the last one must fail.

  status = HasPersistentRow(table, valid_column_family_name, row_key);
  ASSERT_NE(status.ok(), true);

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Does the DeleteFromfamily mutation work to delete a row from a
// specific family and does it rows with the same row key in other
// column families alone?
TEST(InMemoryTransactionRollback, DeleteFromFamilyBasicFunction) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const timestamp_micros = 1234;
  auto const* data = "test";

  auto const* const second_column_family_name = "test2";

  std::vector<std::string> column_families = {column_family_name,
                                              second_column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  p = {second_column_family_name, column_qualifier, timestamp_micros, data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, timestamp_micros, data));
  ASSERT_STATUS_OK(
      HasInMemoryColumn(table, column_family_name, row_key, column_qualifier));
  ASSERT_STATUS_OK(HasInMemoryRow(table, column_family_name, row_key));

  // Having established that the data is there, test the basic
  // functionality of the DeleteFromFamily mutation by trying to
  // delete it.
  ASSERT_STATUS_OK(
      DeleteFromFamilies(table, table_name, row_key, {column_family_name}));
  ASSERT_NE(true, HasInMemoryRow(table, column_family_name, row_key).ok());

  // Ensure that we did not delete a row in another column family.
  ASSERT_EQ(true,
            HasInMemoryRow(table, second_column_family_name, row_key).ok());
}

TEST(PersistentTransactionRollback, DeleteFromFamilyBasicFunction) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const timestamp_micros = 1234;
  auto const* data = "test";

  auto const* const second_column_family_name = "test2";

  std::vector<std::string> column_families = {column_family_name,
                                              second_column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  p = {second_column_family_name, column_qualifier, timestamp_micros, data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, timestamp_micros, data));
  ASSERT_STATUS_OK(HasPersistentColumn(table, column_family_name, row_key,
                                       column_qualifier));
  ASSERT_STATUS_OK(HasPersistentRow(table, column_family_name, row_key));

  // Having established that the data is there, test the basic
  // functionality of the DeleteFromFamily mutation by trying to
  // delete it.
  ASSERT_STATUS_OK(
      DeleteFromFamilies(table, table_name, row_key, {column_family_name}));
  ASSERT_NE(true, HasPersistentRow(table, column_family_name, row_key).ok());

  // Ensure that we did not delete a row in another column family.
  ASSERT_EQ(true,
            HasPersistentRow(table, second_column_family_name, row_key).ok());

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that DeleteFromfamily can be rolled back in case a subsequent
// mutation fails.
TEST(InMemoryTransactionRollback, DeleteFromFamilyRollback) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const timestamp_micros = 1234;
  auto const* data = "test";

  // Failure of one of the mutations is simalted by having a mutation
  // with this column family, which has not been provisioned. Previous
  // successful mutations should be rolled back when RowTransaction
  // sees a mutation with this invalid column family name.
  auto const* const column_family_not_in_schema =
      "i_do_not_exist_in_the_schema";

  std::vector<std::string> column_families = {column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, timestamp_micros, data));
  ASSERT_STATUS_OK(
      HasInMemoryColumn(table, column_family_name, row_key, column_qualifier));
  ASSERT_STATUS_OK(HasInMemoryRow(table, column_family_name, row_key));

  // Setup two DeleteFromfamily mutation: The first one uses the
  // correct table schema (a column family that exists and is expected
  // to succeed to delete the row saved above. The second one uses a
  // column family not provisioned and should fail, which should
  // trigger a rollback of the previous row deletion. In the end, the
  // above row should still exist and all its data should be intact.
  status =
      DeleteFromFamilies(table, table_name, row_key,
                         {column_family_name, column_family_not_in_schema});
  ASSERT_NE(true, status.ok());  // The overall chain of mutations should fail.

  // Check that the row deleted by the first mutation is restored,
  // with all its data.
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, timestamp_micros, data));
  ASSERT_STATUS_OK(
      HasInMemoryColumn(table, column_family_name, row_key, column_qualifier));
  ASSERT_STATUS_OK(HasInMemoryRow(table, column_family_name, row_key));
}

TEST(PersistentTransactionRollback, DeleteFromFamilyRollback) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const timestamp_micros = 1234;
  auto const* data = "test";

  // Failure of one of the mutations is simalted by having a mutation
  // with this column family, which has not been provisioned. Previous
  // successful mutations should be rolled back when RowTransaction
  // sees a mutation with this invalid column family name.
  auto const* const column_family_not_in_schema =
      "i_do_not_exist_in_the_schema";

  std::vector<std::string> column_families = {column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, timestamp_micros, data));
  ASSERT_STATUS_OK(HasPersistentColumn(table, column_family_name, row_key,
                                       column_qualifier));
  ASSERT_STATUS_OK(HasPersistentRow(table, column_family_name, row_key));

  // Setup two DeleteFromfamily mutation: The first one uses the
  // correct table schema (a column family that exists and is expected
  // to succeed to delete the row saved above. The second one uses a
  // column family not provisioned and should fail, which should
  // trigger a rollback of the previous row deletion. In the end, the
  // above row should still exist and all its data should be intact.
  status =
      DeleteFromFamilies(table, table_name, row_key,
                         {column_family_name, column_family_not_in_schema});
  ASSERT_NE(true, status.ok());  // The overall chain of mutations should fail.

  // Check that the row deleted by the first mutation is restored,
  // with all its data.
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, timestamp_micros, data));
  ASSERT_STATUS_OK(HasPersistentColumn(table, column_family_name, row_key,
                                       column_qualifier));
  ASSERT_STATUS_OK(HasPersistentRow(table, column_family_name, row_key));

  std::filesystem::remove_all("/tmp/mutation_projects");
}

::google::bigtable::v2::TimestampRange* NewTimestampRange(int64_t start,
                                                          int64_t end) {
  auto* range = new (::google::bigtable::v2::TimestampRange);
  range->set_start_timestamp_micros(start);
  range->set_end_timestamp_micros(end);

  return range;
}

// Does DeleteFromColumn basically work?
TEST(InMemoryTransactionRollback, DeleteFromColumnBasicFunction) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const* data = "test";

  std::vector<std::string> column_families = {column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v = {
      {column_family_name, column_qualifier, 1000, data},
      {column_family_name, column_qualifier, 2000, data},
      {column_family_name, column_qualifier, 3000, data},
  };

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, 1000, data));
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, 2000, data));
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, 3000, data));

  std::vector<DeleteFromColumnParams> dv = {
      {column_family_name, column_qualifier,
       NewTimestampRange(v[0].timestamp_micros, v[2].timestamp_micros + 1000)}};

  ASSERT_STATUS_OK(DeleteFromColumns(table, table_name, row_key, dv));

  status =
      HasInMemoryColumn(table, column_family_name, row_key, column_qualifier);
  ASSERT_EQ(false, status.ok());
}

TEST(PersistentTransactionRollback, DeleteFromColumnBasicFunction) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  auto const* data = "test";

  std::vector<std::string> column_families = {column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v = {
      {column_family_name, column_qualifier, 1000, data},
      {column_family_name, column_qualifier, 2000, data},
      {column_family_name, column_qualifier, 3000, data},
  };

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 1000, data));
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 2000, data));
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 3000, data));

  std::vector<DeleteFromColumnParams> dv = {
      {column_family_name, column_qualifier,
       NewTimestampRange(v[0].timestamp_micros, v[2].timestamp_micros + 1000)}};

  ASSERT_STATUS_OK(DeleteFromColumns(table, table_name, row_key, dv));

  status =
      HasPersistentColumn(table, column_family_name, row_key, column_qualifier);
  ASSERT_NE(Status(), status);

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Does DeleteFromColumn rollback work?
TEST(InMemoryTransactionRollback, DeleteFromColumnRollback) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  // Simulate mutation failure and cause rollback by attempting a
  // mutation with a non-existent column family name.
  auto const* const bad_column_family_name =
      "this_column_family_does_not_exist";
  auto const* data = "test";

  std::vector<std::string> column_families = {column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v = {
      {column_family_name, column_qualifier, 1000, data},
      {column_family_name, column_qualifier, 2000, data},
      {column_family_name, column_qualifier, 3000, data},
  };

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, 1000, data));
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, 2000, data));
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, 3000, data));

  // The first mutation will succeed. The second assumes a schema that
  // does not exist - it should fail and cause rollback of the column
  // deletion in the first mutation.
  std::vector<DeleteFromColumnParams> dv = {
      {column_family_name, column_qualifier,
       NewTimestampRange(v[0].timestamp_micros, v[2].timestamp_micros + 1000)},
      {bad_column_family_name, column_qualifier, NewTimestampRange(1000, 2000)},
  };
  // The mutation chains should fail and rollback should occur.
  ASSERT_EQ(false, DeleteFromColumns(table, table_name, row_key, dv).ok());

  // The column should have been restored.
  ASSERT_STATUS_OK(
      HasInMemoryColumn(table, column_family_name, row_key, column_qualifier));
  // Check that the data is where and what we expect.
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, 1000, data));
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, 2000, data));
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, 3000, data));
}

TEST(PersistentTransactionRollback, DeleteFromColumnRollback) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "test";
  auto const* const column_qualifier = "test";
  // Simulate mutation failure and cause rollback by attempting a
  // mutation with a non-existent column family name.
  auto const* const bad_column_family_name =
      "this_column_family_does_not_exist";
  auto const* data = "test";

  std::vector<std::string> column_families = {column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v = {
      {column_family_name, column_qualifier, 1000, data},
      {column_family_name, column_qualifier, 2000, data},
      {column_family_name, column_qualifier, 3000, data},
  };

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 1000, data));
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 2000, data));
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 3000, data));

  // The first mutation will succeed. The second assumes a schema that
  // does not exist - it should fail and cause rollback of the column
  // deletion in the first mutation.
  std::vector<DeleteFromColumnParams> dv = {
      {column_family_name, column_qualifier,
       NewTimestampRange(v[0].timestamp_micros, v[2].timestamp_micros + 1000)},
      {bad_column_family_name, column_qualifier, NewTimestampRange(1000, 2000)},
  };
  // The mutation chains should fail and rollback should occur.
  ASSERT_EQ(false, DeleteFromColumns(table, table_name, row_key, dv).ok());

  // The column should have been restored.
  ASSERT_STATUS_OK(HasPersistentColumn(table, column_family_name, row_key,
                                       column_qualifier));
  // Check that the data is where and what we expect.
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 1000, data));
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 2000, data));
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, 3000, data));

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Can we delete a row from all column families?
TEST(InMemoryTransactionRollback, DeleteFromRowBasicFunction) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;
  auto const* data = "value";
  auto const* const second_column_family_name = "column_family_2";

  std::vector<std::string> column_families = {column_family_name,
                                              second_column_family_name};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  p = {second_column_family_name, column_qualifier, timestamp_micros, data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, timestamp_micros, data));
  ASSERT_STATUS_OK(HasInMemoryColumn(table, second_column_family_name, row_key,
                                     column_qualifier));
  ASSERT_STATUS_OK(HasInMemoryRow(table, column_family_name, row_key));

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  mutation_request_mutation->mutable_delete_from_row();

  ASSERT_STATUS_OK(table->MutateRow(mutation_request));
  ASSERT_EQ(false, HasInMemoryCell(table, column_family_name, row_key,
                                   column_qualifier, timestamp_micros, data)
                       .ok());
  ASSERT_EQ(false, HasInMemoryColumn(table, second_column_family_name, row_key,
                                     column_qualifier)
                       .ok());
}

TEST(PersistentTransactionRollback, DeleteFromRowBasicFunction) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;
  auto const* data = "value";
  auto const* const second_column_family_name = "column_family_2";

  std::vector<std::string> column_families = {column_family_name,
                                              second_column_family_name};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> v;
  SetCellParams p = {column_family_name, column_qualifier, timestamp_micros,
                     data};
  v.push_back(p);

  p = {second_column_family_name, column_qualifier, timestamp_micros, data};
  v.push_back(p);

  auto status = SetCells(table, table_name, row_key, v);
  ASSERT_STATUS_OK(status);
  ASSERT_STATUS_OK(HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, timestamp_micros, data));
  ASSERT_STATUS_OK(HasPersistentColumn(table, second_column_family_name,
                                       row_key, column_qualifier));
  ASSERT_STATUS_OK(HasPersistentRow(table, column_family_name, row_key));

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  mutation_request_mutation->mutable_delete_from_row();

  ASSERT_STATUS_OK(table->MutateRow(mutation_request));
  ASSERT_EQ(false, HasPersistentCell(table, column_family_name, row_key,
                                     column_qualifier, timestamp_micros, data)
                       .ok());
  ASSERT_EQ(false, HasPersistentColumn(table, second_column_family_name,
                                       row_key, column_qualifier)
                       .ok());

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Does AddToCell reject requests to add to a cell in a column family
// not provisioned for aggregation?
TEST(InMemoryTransactionRollback,
     AddToCellRejectsRequestsToNonAggregateColumnFamily) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;

  auto maybe_table = Table::Create(
      CreateSchema(table_name, {{column_family_name, column_family}}), false);

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  auto* add_to_cell_mutation = mutation_request_mutation->mutable_add_to_cell();

  add_to_cell_mutation->set_family_name(column_family_name);
  auto* mutable_column_qualifier =
      add_to_cell_mutation->mutable_column_qualifier();
  mutable_column_qualifier->set_raw_value(column_qualifier);
  auto* mutable_timestamp = add_to_cell_mutation->mutable_timestamp();
  mutable_timestamp->set_raw_timestamp_micros(timestamp_micros);
  auto* mutable_input = add_to_cell_mutation->mutable_input();
  mutable_input->set_int_value(100);

  // Should fail because `column_family' has not been provisioned for
  // aggregation. i.e. its value_type is not set all, in this case (it
  // would need to be set to `Aggregate'.
  ASSERT_EQ(false, table->MutateRow(mutation_request).ok());
}

TEST(PersistentTransactionRollback,
     AddToCellRejectsRequestsToNonAggregateColumnFamily) {
  ::google::bigtable::admin::v2::Table schema;
  ::google::bigtable::admin::v2::ColumnFamily column_family;

  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;

  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = Table::Create(
      CreateSchema(table_name, {{column_family_name, column_family}}), true,
      "/tmp/");

  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  auto* add_to_cell_mutation = mutation_request_mutation->mutable_add_to_cell();

  add_to_cell_mutation->set_family_name(column_family_name);
  auto* mutable_column_qualifier =
      add_to_cell_mutation->mutable_column_qualifier();
  mutable_column_qualifier->set_raw_value(column_qualifier);
  auto* mutable_timestamp = add_to_cell_mutation->mutable_timestamp();
  mutable_timestamp->set_raw_timestamp_micros(timestamp_micros);
  auto* mutable_input = add_to_cell_mutation->mutable_input();
  mutable_input->set_int_value(100);

  // Should fail because `column_family' has not been provisioned for
  // aggregation. i.e. its value_type is not set all, in this case (it
  // would need to be set to `Aggregate'.
  ASSERT_NE(Status(), table->MutateRow(mutation_request));

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test basic functionality of AddToCell Sum aggregation.
TEST(InMemoryTransactionRollback, AddToCellTestSum) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;

  auto maybe_table = Table::Create(
      CreateSchema(table_name,
                   {{column_family_name,
                     MakeBEAggregateCFProto(
                         google::bigtable::admin::v2::Type::Aggregate::kSum)}}),
      false);
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  auto* add_to_cell_mutation = mutation_request_mutation->mutable_add_to_cell();

  add_to_cell_mutation->set_family_name(column_family_name);
  auto* mutable_column_qualifier =
      add_to_cell_mutation->mutable_column_qualifier();
  mutable_column_qualifier->set_raw_value(column_qualifier);
  auto* mutable_timestamp = add_to_cell_mutation->mutable_timestamp();
  mutable_timestamp->set_raw_timestamp_micros(timestamp_micros);
  auto* mutable_input = add_to_cell_mutation->mutable_input();
  mutable_input->set_int_value(100);

  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(true,
            HasInMemoryCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(100))
                .ok());

  // Try and add 200
  mutable_input->set_int_value(200);
  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(true,
            HasInMemoryCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(300))
                .ok());

  // Try and subtract 50
  mutable_input->set_int_value(-50);
  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(true,
            HasInMemoryCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(250))
                .ok());
}

TEST(PersistentTransactionRollback, AddToCellTestSum) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;

  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = Table::Create(
      CreateSchema(table_name,
                   {{column_family_name,
                     MakeBEAggregateCFProto(
                         google::bigtable::admin::v2::Type::Aggregate::kSum)}}),
      true, "/tmp/");
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  auto* add_to_cell_mutation = mutation_request_mutation->mutable_add_to_cell();

  add_to_cell_mutation->set_family_name(column_family_name);
  auto* mutable_column_qualifier =
      add_to_cell_mutation->mutable_column_qualifier();
  mutable_column_qualifier->set_raw_value(column_qualifier);
  auto* mutable_timestamp = add_to_cell_mutation->mutable_timestamp();
  mutable_timestamp->set_raw_timestamp_micros(timestamp_micros);
  auto* mutable_input = add_to_cell_mutation->mutable_input();
  mutable_input->set_int_value(100);

  ASSERT_EQ(Status(), table->MutateRow(mutation_request));
  ASSERT_EQ(Status(),
            HasPersistentCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(100)));

  // Try and add 200
  mutable_input->set_int_value(200);
  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(Status(),
            HasPersistentCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(300)));

  // Try and subtract 50
  mutable_input->set_int_value(-50);
  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(Status(),
            HasPersistentCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(250)));

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test basic functionality of AddToCell Max aggregation.
TEST(InMemoryTransactionRollback, AddToCellTestMax) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;

  auto maybe_table = Table::Create(
      CreateSchema(table_name,
                   {{column_family_name,
                     MakeBEAggregateCFProto(
                         google::bigtable::admin::v2::Type::Aggregate::kMax)}}),
      false);
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  auto* add_to_cell_mutation = mutation_request_mutation->mutable_add_to_cell();

  add_to_cell_mutation->set_family_name(column_family_name);
  auto* mutable_column_qualifier =
      add_to_cell_mutation->mutable_column_qualifier();
  mutable_column_qualifier->set_raw_value(column_qualifier);
  auto* mutable_timestamp = add_to_cell_mutation->mutable_timestamp();
  mutable_timestamp->set_raw_timestamp_micros(timestamp_micros);
  auto* mutable_input = add_to_cell_mutation->mutable_input();
  mutable_input->set_int_value(100);

  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(Status(),
            HasInMemoryCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(100)));

  mutable_input->set_int_value(200);
  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(Status(),
            HasInMemoryCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(200)));
}

TEST(PersistentTransactionRollback, AddToCellTestMax) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;

  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = Table::Create(
      CreateSchema(table_name,
                   {{column_family_name,
                     MakeBEAggregateCFProto(
                         google::bigtable::admin::v2::Type::Aggregate::kMax)}}),
      true, "/tmp/");
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  auto* add_to_cell_mutation = mutation_request_mutation->mutable_add_to_cell();

  add_to_cell_mutation->set_family_name(column_family_name);
  auto* mutable_column_qualifier =
      add_to_cell_mutation->mutable_column_qualifier();
  mutable_column_qualifier->set_raw_value(column_qualifier);
  auto* mutable_timestamp = add_to_cell_mutation->mutable_timestamp();
  mutable_timestamp->set_raw_timestamp_micros(timestamp_micros);
  auto* mutable_input = add_to_cell_mutation->mutable_input();
  mutable_input->set_int_value(100);

  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(Status(),
            HasPersistentCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(100)));

  mutable_input->set_int_value(200);
  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(Status(),
            HasPersistentCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(200)));

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test basic functionality of AddToCell Min aggregation.
TEST(InMemoryTransactionRollback, AddToCellTestMin) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;

  auto maybe_table = Table::Create(
      CreateSchema(table_name,
                   {{column_family_name,
                     MakeBEAggregateCFProto(
                         google::bigtable::admin::v2::Type::Aggregate::kMin)}}),
      false);
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  auto* add_to_cell_mutation = mutation_request_mutation->mutable_add_to_cell();

  add_to_cell_mutation->set_family_name(column_family_name);
  auto* mutable_column_qualifier =
      add_to_cell_mutation->mutable_column_qualifier();
  mutable_column_qualifier->set_raw_value(column_qualifier);
  auto* mutable_timestamp = add_to_cell_mutation->mutable_timestamp();
  mutable_timestamp->set_raw_timestamp_micros(timestamp_micros);
  auto* mutable_input = add_to_cell_mutation->mutable_input();
  mutable_input->set_int_value(100);

  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(true,
            HasInMemoryCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(100))
                .ok());

  mutable_input->set_int_value(50);
  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(true,
            HasInMemoryCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(50))
                .ok());
}

TEST(PersistentTransactionRollback, AddToCellTestMin) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";
  auto const* const row_key = "0";
  auto const* const column_family_name = "column_family_1";
  auto const* const column_qualifier = "column_qualifier";
  auto const timestamp_micros = 1000;

  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = Table::Create(
      CreateSchema(table_name,
                   {{column_family_name,
                     MakeBEAggregateCFProto(
                         google::bigtable::admin::v2::Type::Aggregate::kMin)}}),
      true, "/tmp/");
  ASSERT_STATUS_OK(maybe_table);

  auto table = maybe_table.value();

  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  auto* mutation_request_mutation = mutation_request.add_mutations();
  auto* add_to_cell_mutation = mutation_request_mutation->mutable_add_to_cell();

  add_to_cell_mutation->set_family_name(column_family_name);
  auto* mutable_column_qualifier =
      add_to_cell_mutation->mutable_column_qualifier();
  mutable_column_qualifier->set_raw_value(column_qualifier);
  auto* mutable_timestamp = add_to_cell_mutation->mutable_timestamp();
  mutable_timestamp->set_raw_timestamp_micros(timestamp_micros);
  auto* mutable_input = add_to_cell_mutation->mutable_input();
  mutable_input->set_int_value(100);

  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(Status(),
            HasPersistentCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(100)));

  mutable_input->set_int_value(50);
  ASSERT_EQ(true, table->MutateRow(mutation_request).ok());
  ASSERT_EQ(Status(),
            HasPersistentCell(
                table, column_family_name, row_key, column_qualifier,
                timestamp_micros,
                google::cloud::internal::EncodeBigEndian<std::int64_t>(50)));

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that ReadModifyWrite does the correct thing when the row
// and/or the column is unset (it should introduce new cells with the
// timestamp of current system time and assume the missing values are
// 0 or an empty string).
TEST(InMemoryReadModifyWrite, Unsetcase) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      increment_amount: 1
    }
      , {
        family_name: "column_family"
        column_qualifier: "column_2"
        append_value: "a string"
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto system_time_ms_before =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_STATUS_OK(maybe_response);

  auto& response = maybe_response.value();
  ASSERT_EQ(response.row().key(), "0");
  ASSERT_EQ(response.row().families_size(), 1);
  ASSERT_EQ(response.row().families(0).name(), "column_family");
  ASSERT_EQ(response.row().families(0).columns_size(), 2);

  auto maybe_column = GetResponseColumn(response, "0", 0, "column_1");
  ASSERT_STATUS_OK(maybe_column);
  auto& col = maybe_column.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds(col.cells(0).timestamp_micros())),
            system_time_ms_before);
  ASSERT_EQ(col.cells(0).value(), ::google::cloud::internal::EncodeBigEndian(
                                      static_cast<std::int64_t>(1)));

  auto maybe_column_2 = GetResponseColumn(response, "0", 0, "column_2");
  ASSERT_STATUS_OK(maybe_column_2);
  col = maybe_column_2.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds(col.cells(0).timestamp_micros())),
            system_time_ms_before);
  ASSERT_EQ(col.cells(0).value(), "a string");

  auto maybe_cells = GetInMemoryColumn(table, "column_family", "0", "column_1");
  ASSERT_STATUS_OK(maybe_cells);
  auto& cells = maybe_cells.value();
  ASSERT_EQ(cells.size(), 1);
  auto cell_it = cells.begin();
  ASSERT_GE(cell_it->first, system_time_ms_before);
  ASSERT_EQ(cell_it->second, ::google::cloud::internal::EncodeBigEndian(
                                 static_cast<std::int64_t>(1)));

  auto maybe_cells_2 =
      GetInMemoryColumn(table, "column_family", "0", "column_2");
  ASSERT_STATUS_OK(maybe_cells_2);
  cells = maybe_cells_2.value();
  ASSERT_EQ(cells.size(), 1);
  cell_it = cells.begin();
  ASSERT_GE(cell_it->first, system_time_ms_before);
  ASSERT_EQ(cell_it->second, "a string");
}

TEST(PersistentReadModifyWrite, Unsetcase) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      increment_amount: 1
    }
      , {
        family_name: "column_family"
        column_qualifier: "column_2"
        append_value: "a string"
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto system_time_ms_before =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_STATUS_OK(maybe_response);

  auto& response = maybe_response.value();
  ASSERT_EQ(response.row().key(), "0");
  ASSERT_EQ(response.row().families_size(), 1);
  ASSERT_EQ(response.row().families(0).name(), "column_family");
  ASSERT_EQ(response.row().families(0).columns_size(), 2);

  auto maybe_column = GetResponseColumn(response, "0", 0, "column_1");
  ASSERT_STATUS_OK(maybe_column);
  auto& col = maybe_column.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds(col.cells(0).timestamp_micros())),
            system_time_ms_before);
  ASSERT_EQ(col.cells(0).value(), ::google::cloud::internal::EncodeBigEndian(
                                      static_cast<std::int64_t>(1)));

  auto maybe_column_2 = GetResponseColumn(response, "0", 0, "column_2");
  ASSERT_STATUS_OK(maybe_column_2);
  col = maybe_column_2.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds(col.cells(0).timestamp_micros())),
            system_time_ms_before);
  ASSERT_EQ(col.cells(0).value(), "a string");

  auto maybe_cells =
      GetPersistentColumn(table, "column_family", "0", "column_1");
  ASSERT_STATUS_OK(maybe_cells);
  auto& cells = maybe_cells.value();
  ASSERT_EQ(cells.size(), 1);
  auto cell_it = cells.begin();
  ASSERT_GE(cell_it->first, system_time_ms_before);
  ASSERT_EQ(cell_it->second, ::google::cloud::internal::EncodeBigEndian(
                                 static_cast<std::int64_t>(1)));

  auto maybe_cells_2 =
      GetPersistentColumn(table, "column_family", "0", "column_2");
  ASSERT_STATUS_OK(maybe_cells_2);
  cells = maybe_cells_2.value();
  ASSERT_EQ(cells.size(), 1);
  cell_it = cells.begin();
  ASSERT_GE(cell_it->first, system_time_ms_before);
  ASSERT_EQ(cell_it->second, "a string");

  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that the RPC does the right thing when the latest cell in the
// column has a newer timestamp than system time. In particular, it
// should update the latest cell with a new value (and not create a
// new cell). This also tests that the RPC chooses the latest cell to
// update (and will catch bugs in cell ordering).
TEST(InMemoryReadModifyWrite, SetAndNewerTimestampCase) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto usecs_in_day = (static_cast<std::int64_t>(24) * 60 * 60 * 1000 * 1000);

  auto far_future_us = (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count() *
                        1000) +
                       usecs_in_day;
  ASSERT_GT(far_future_us,
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

  auto far_future_us_latest = far_future_us + 1000;

  std::vector<SetCellParams> p = {
      {"column_family", "column_1", far_future_us, "older"},
      {"column_family", "column_1", far_future_us_latest, "latest"},
      {"column_family", "column_2", far_future_us,
       ::google::cloud::internal::EncodeBigEndian(
           static_cast<std::int64_t>(100))},
      {"column_family", "column_2", far_future_us_latest,
       ::google::cloud::internal::EncodeBigEndian(
           static_cast<std::int64_t>(200))},
  };

  auto status = SetCells(table, table_name, "0", p);
  ASSERT_STATUS_OK(status);

  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      append_value: "_with_suffix"
    }
      , {
        family_name: "column_family"
        column_qualifier: "column_2"
        increment_amount: 1
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_STATUS_OK(maybe_response);

  auto& response = maybe_response.value();
  ASSERT_EQ(response.row().key(), "0");
  ASSERT_EQ(response.row().families_size(), 1);
  ASSERT_EQ(response.row().families(0).name(), "column_family");
  ASSERT_EQ(response.row().families(0).columns_size(), 2);

  auto maybe_column = GetResponseColumn(response, "0", 0, "column_1");
  ASSERT_STATUS_OK(maybe_column);
  auto& col = maybe_column.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_EQ(col.cells(0).timestamp_micros(), far_future_us_latest);
  ASSERT_EQ(col.cells(0).value(), "latest_with_suffix");

  auto maybe_column_2 = GetResponseColumn(response, "0", 0, "column_2");
  ASSERT_STATUS_OK(maybe_column_2);
  col = maybe_column_2.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_EQ(col.cells(0).timestamp_micros(), far_future_us_latest);
  ASSERT_EQ(col.cells(0).value(), ::google::cloud::internal::EncodeBigEndian(
                                      static_cast<std::int64_t>(201)));

  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_1",
                                   far_future_us, "older"));
  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_1",
                                   far_future_us_latest, "latest_with_suffix"));

  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_2",
                                   far_future_us,
                                   ::google::cloud::internal::EncodeBigEndian(
                                       static_cast<std::int64_t>(100))));
  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_2",
                                   far_future_us_latest,
                                   ::google::cloud::internal::EncodeBigEndian(
                                       static_cast<std::int64_t>(201))));
}

TEST(PersistentReadModifyWrite, SetAndNewerTimestampCase) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto usecs_in_day = (static_cast<std::int64_t>(24) * 60 * 60 * 1000 * 1000);

  auto far_future_us = (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count() *
                        1000) +
                       usecs_in_day;
  ASSERT_GT(far_future_us,
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

  auto far_future_us_latest = far_future_us + 1000;

  std::vector<SetCellParams> p = {
      {"column_family", "column_1", far_future_us, "older"},
      {"column_family", "column_1", far_future_us_latest, "latest"},
      {"column_family", "column_2", far_future_us,
       ::google::cloud::internal::EncodeBigEndian(
           static_cast<std::int64_t>(100))},
      {"column_family", "column_2", far_future_us_latest,
       ::google::cloud::internal::EncodeBigEndian(
           static_cast<std::int64_t>(200))},
  };

  auto status = SetCells(table, table_name, "0", p);
  ASSERT_STATUS_OK(status);

  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      append_value: "_with_suffix"
    }
      , {
        family_name: "column_family"
        column_qualifier: "column_2"
        increment_amount: 1
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_STATUS_OK(maybe_response);

  auto& response = maybe_response.value();
  ASSERT_EQ(response.row().key(), "0");
  ASSERT_EQ(response.row().families_size(), 1);
  ASSERT_EQ(response.row().families(0).name(), "column_family");
  ASSERT_EQ(response.row().families(0).columns_size(), 2);

  auto maybe_column = GetResponseColumn(response, "0", 0, "column_1");
  ASSERT_STATUS_OK(maybe_column);
  auto& col = maybe_column.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_EQ(col.cells(0).timestamp_micros(), far_future_us_latest);
  ASSERT_EQ(col.cells(0).value(), "latest_with_suffix");

  auto maybe_column_2 = GetResponseColumn(response, "0", 0, "column_2");
  ASSERT_STATUS_OK(maybe_column_2);
  col = maybe_column_2.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_EQ(col.cells(0).timestamp_micros(), far_future_us_latest);
  ASSERT_EQ(col.cells(0).value(), ::google::cloud::internal::EncodeBigEndian(
                                      static_cast<std::int64_t>(201)));

  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_1",
                                     far_future_us, "older"));
  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_1",
                                     far_future_us_latest,
                                     "latest_with_suffix"));

  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_2",
                                     far_future_us,
                                     ::google::cloud::internal::EncodeBigEndian(
                                         static_cast<std::int64_t>(100))));
  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_2",
                                     far_future_us_latest,
                                     ::google::cloud::internal::EncodeBigEndian(
                                         static_cast<std::int64_t>(201))));
  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that the RPC does the right thing when the latest cell in the
// column has an older timestamp than system time. In particular, a
// new cell with the current system time should be added to the cell
// to contain the value after adding or appending.
TEST(InMemoryReadModifyWrite, SetAndOlderTimestampCase) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto usecs_in_day = (static_cast<std::int64_t>(24) * 60 * 60 * 1000 * 1000);

  auto far_past_us = (std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count() *
                      1000) -
                     usecs_in_day;
  ASSERT_LT(far_past_us,
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
  auto far_past_us_oldest = far_past_us - 1000;

  std::vector<SetCellParams> p = {
      {"column_family", "column_1", far_past_us, "old"},
      {"column_family", "column_1", far_past_us_oldest, "oldest"},
      {"column_family", "column_2", far_past_us,
       ::google::cloud::internal::EncodeBigEndian(
           static_cast<std::int64_t>(100))},
      {"column_family", "column_2", far_past_us_oldest,
       ::google::cloud::internal::EncodeBigEndian(
           static_cast<std::int64_t>(200))},
  };

  auto status = SetCells(table, table_name, "0", p);
  ASSERT_STATUS_OK(status);

  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      append_value: "_with_suffix"
    }
      , {
        family_name: "column_family"
        column_qualifier: "column_2"
        increment_amount: 1
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto system_time_us_before =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count() *
      1000;

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_STATUS_OK(maybe_response);

  auto& response = maybe_response.value();
  ASSERT_EQ(response.row().key(), "0");
  ASSERT_EQ(response.row().families_size(), 1);
  ASSERT_EQ(response.row().families(0).name(), "column_family");
  ASSERT_EQ(response.row().families(0).columns_size(), 2);

  auto maybe_column = GetResponseColumn(response, "0", 0, "column_1");
  ASSERT_STATUS_OK(maybe_column);
  auto& col = maybe_column.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_GE(col.cells(0).timestamp_micros(), system_time_us_before);
  ASSERT_EQ(col.cells(0).value(), "old_with_suffix");

  auto maybe_column_2 = GetResponseColumn(response, "0", 0, "column_2");
  ASSERT_STATUS_OK(maybe_column_2);
  auto& integer_col = maybe_column_2.value();
  ASSERT_EQ(integer_col.cells_size(), 1);
  ASSERT_GE(integer_col.cells(0).timestamp_micros(), system_time_us_before);
  ASSERT_EQ(integer_col.cells(0).value(),
            ::google::cloud::internal::EncodeBigEndian(
                static_cast<std::int64_t>(101)));

  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_1",
                                   far_past_us, "old"));
  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_1",
                                   far_past_us_oldest, "oldest"));
  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_1",
                                   col.cells(0).timestamp_micros(),
                                   "old_with_suffix"));

  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_2",
                                   far_past_us,
                                   ::google::cloud::internal::EncodeBigEndian(
                                       static_cast<std::int64_t>(100))));
  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_2",
                                   far_past_us_oldest,
                                   ::google::cloud::internal::EncodeBigEndian(
                                       static_cast<std::int64_t>(200))));
  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_2",
                                   integer_col.cells(0).timestamp_micros(),
                                   ::google::cloud::internal::EncodeBigEndian(
                                       static_cast<std::int64_t>(101))));
}

TEST(PersistentReadModifyWrite, SetAndOlderTimestampCase) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto usecs_in_day = (static_cast<std::int64_t>(24) * 60 * 60 * 1000 * 1000);

  auto far_past_us = (std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count() *
                      1000) -
                     usecs_in_day;
  ASSERT_LT(far_past_us,
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
  auto far_past_us_oldest = far_past_us - 1000;

  std::vector<SetCellParams> p = {
      {"column_family", "column_1", far_past_us, "old"},
      {"column_family", "column_1", far_past_us_oldest, "oldest"},
      {"column_family", "column_2", far_past_us,
       ::google::cloud::internal::EncodeBigEndian(
           static_cast<std::int64_t>(100))},
      {"column_family", "column_2", far_past_us_oldest,
       ::google::cloud::internal::EncodeBigEndian(
           static_cast<std::int64_t>(200))},
  };

  auto status = SetCells(table, table_name, "0", p);
  ASSERT_STATUS_OK(status);

  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      append_value: "_with_suffix"
    }
      , {
        family_name: "column_family"
        column_qualifier: "column_2"
        increment_amount: 1
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto system_time_us_before =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count() *
      1000;

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_STATUS_OK(maybe_response);

  auto& response = maybe_response.value();
  ASSERT_EQ(response.row().key(), "0");
  ASSERT_EQ(response.row().families_size(), 1);
  ASSERT_EQ(response.row().families(0).name(), "column_family");
  ASSERT_EQ(response.row().families(0).columns_size(), 2);

  auto maybe_column = GetResponseColumn(response, "0", 0, "column_1");
  ASSERT_STATUS_OK(maybe_column);
  auto& col = maybe_column.value();
  ASSERT_EQ(col.cells_size(), 1);
  ASSERT_GE(col.cells(0).timestamp_micros(), system_time_us_before);
  ASSERT_EQ(col.cells(0).value(), "old_with_suffix");

  auto maybe_column_2 = GetResponseColumn(response, "0", 0, "column_2");
  ASSERT_STATUS_OK(maybe_column_2);
  auto& integer_col = maybe_column_2.value();
  ASSERT_EQ(integer_col.cells_size(), 1);
  ASSERT_GE(integer_col.cells(0).timestamp_micros(), system_time_us_before);
  ASSERT_EQ(integer_col.cells(0).value(),
            ::google::cloud::internal::EncodeBigEndian(
                static_cast<std::int64_t>(101)));

  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_1",
                                     far_past_us, "old"));
  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_1",
                                     far_past_us_oldest, "oldest"));
  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_1",
                                     col.cells(0).timestamp_micros(),
                                     "old_with_suffix"));

  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_2",
                                     far_past_us,
                                     ::google::cloud::internal::EncodeBigEndian(
                                         static_cast<std::int64_t>(100))));
  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_2",
                                     far_past_us_oldest,
                                     ::google::cloud::internal::EncodeBigEndian(
                                         static_cast<std::int64_t>(200))));
  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_2",
                                     integer_col.cells(0).timestamp_micros(),
                                     ::google::cloud::internal::EncodeBigEndian(
                                         static_cast<std::int64_t>(101))));
  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that the RPC does the right thing when the latest cell in the
// column has a newer timestamp than system time, and we need to roll
// back. In particular the changes to the latest cell should be rolled
// back.
TEST(InMemoryReadModifyWrite, RollbackNewerTimestamp) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto usecs_in_day = (static_cast<std::int64_t>(24) * 60 * 60 * 1000 * 1000);

  auto far_future_us = (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count() *
                        1000) +
                       usecs_in_day;

  ASSERT_GT(far_future_us,
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

  std::vector<SetCellParams> p = {
      {"column_family", "column_1", far_future_us, "prefix"},
  };

  auto status = SetCells(table, table_name, "0", p);
  ASSERT_STATUS_OK(status);

  // The rules are evaluated in order. In this case, the 2nd rule
  // refers to a column family that does not exist and should trigger
  // a rollback.
  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      append_value: "_with_suffix"
    }
      , {
        family_name: "does_not_exist"
        column_qualifier: "column_2"
        increment_amount: 1
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_EQ(false, maybe_response.ok());

  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_1",
                                   far_future_us, "prefix"));
}

TEST(PersistentReadModifyWrite, RollbackNewerTimestamp) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto usecs_in_day = (static_cast<std::int64_t>(24) * 60 * 60 * 1000 * 1000);

  auto far_future_us = (std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count() *
                        1000) +
                       usecs_in_day;

  ASSERT_GT(far_future_us,
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

  std::vector<SetCellParams> p = {
      {"column_family", "column_1", far_future_us, "prefix"},
  };

  auto status = SetCells(table, table_name, "0", p);
  ASSERT_STATUS_OK(status);

  // The rules are evaluated in order. In this case, the 2nd rule
  // refers to a column family that does not exist and should trigger
  // a rollback.
  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      append_value: "_with_suffix"
    }
      , {
        family_name: "does_not_exist"
        column_qualifier: "column_2"
        increment_amount: 1
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_EQ(false, maybe_response.ok());

  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_1",
                                     far_future_us, "prefix"));
  std::filesystem::remove_all("/tmp/mutation_projects");
}

// Test that the RPC does the right thing when the latest cell in the
// column has an older timestamp than system time, and we need to roll
// back. In particular, the added cell should be deleted (no
// additional cell should be available after the failed transaction).
TEST(InMemoryReadModifyWrite, RollbackOlderTimestamp) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  auto maybe_table = CreateTable(table_name, column_families, false);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto usecs_in_day = (static_cast<std::int64_t>(24) * 60 * 60 * 1000 * 1000);

  auto far_past_us = (std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count() *
                      1000) -
                     usecs_in_day;
  ASSERT_LT(far_past_us,
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

  std::vector<SetCellParams> p = {
      {"column_family", "column_1", far_past_us, "old"},
  };

  auto status = SetCells(table, table_name, "0", p);
  ASSERT_STATUS_OK(status);

  // The rules are evaluated in order. In this case, the 2nd rule
  // refers to a column family that does not exist and should trigger
  // a rollback.
  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      append_value: "_with_suffix"
    }
      , {
        family_name: "does_not_exist"
        column_qualifier: "column_2"
        increment_amount: 1
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_EQ(false, maybe_response.ok());

  ASSERT_STATUS_OK(HasInMemoryCell(table, "column_family", "0", "column_1",
                                   far_past_us, "old"));
}

TEST(PersistentReadModifyWrite, RollbackOlderTimestamp) {
  auto const* const table_name =
      "mutation_projects/test/instances/test/tables/test";

  std::vector<std::string> column_families = {"column_family"};
  std::filesystem::remove_all("/tmp/mutation_projects");
  auto maybe_table = CreateTable(table_name, column_families, true);

  ASSERT_STATUS_OK(maybe_table);
  auto& table = maybe_table.value();

  auto usecs_in_day = (static_cast<std::int64_t>(24) * 60 * 60 * 1000 * 1000);

  auto far_past_us = (std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count() *
                      1000) -
                     usecs_in_day;
  ASSERT_LT(far_past_us,
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());

  std::vector<SetCellParams> p = {
      {"column_family", "column_1", far_past_us, "old"},
  };

  auto status = SetCells(table, table_name, "0", p);
  ASSERT_STATUS_OK(status);

  // The rules are evaluated in order. In this case, the 2nd rule
  // refers to a column family that does not exist and should trigger
  // a rollback.
  auto constexpr kRMWText = R"pb(
    table_name: "mutation_projects/test/instances/test/tables/test"
    row_key: "0"
    rules:
    [ {
      family_name: "column_family"
      column_qualifier: "column_1"
      append_value: "_with_suffix"
    }
      , {
        family_name: "does_not_exist"
        column_qualifier: "column_2"
        increment_amount: 1
      }]
  )pb";

  google::bigtable::v2::ReadModifyWriteRowRequest request;
  ASSERT_TRUE(TextFormat::ParseFromString(kRMWText, &request));

  auto maybe_response = table->ReadModifyWriteRow(request);
  ASSERT_EQ(false, maybe_response.ok());

  ASSERT_STATUS_OK(HasPersistentCell(table, "column_family", "0", "column_1",
                                     far_past_us, "old"));
  std::filesystem::remove_all("/tmp/mutation_projects");
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
