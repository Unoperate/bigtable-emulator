#include "test_util.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "backend.h"
#include "in_memory/in_memory_backend.h"
#include "in_memory/in_memory_column_family.h"
#include "persistent/key_coder.h"
#include "persistent/persistent_backend.h"
#include "persistent/persistent_column_family.h"
#include "table.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

StatusOr<std::shared_ptr<Table>> CreateTable(
    std::string const& table_name, std::vector<std::string>& column_families,
    bool should_persist) {
  ::google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  for (auto& column_family_name : column_families) {
    (*schema.mutable_column_families())[column_family_name] =
        ::google::bigtable::admin::v2::ColumnFamily();
  }
  return CreateTable(std::move(schema), should_persist);
}

std::string TestDataDir() {
  auto const* info = ::testing::UnitTest::GetInstance()->current_test_info();
  return std::filesystem::path(::testing::TempDir()) /
         (std::string(info->test_suite_name()) + "_" + info->name());
}

StatusOr<std::shared_ptr<Table>> CreateTable(
    ::google::bigtable::admin::v2::Table schema, bool should_persist) {
  BackendOptions options;
  if (should_persist) {
    options.data_dir = TestDataDir();
  }
  auto maybe_backend = Backend::Create(options);
  if (!maybe_backend) {
    return maybe_backend.status();
  }
  auto maybe_operations = (*maybe_backend)->CreateTable(schema);
  if (!maybe_operations) {
    return maybe_operations.status();
  }
  auto name = schema.name();
  return std::make_shared<Table>(std::move(name), std::move(schema),
                                 *std::move(maybe_operations));
}

StatusOr<std::shared_ptr<Table>> LoadTable(std::string const& table_name) {
  BackendOptions options;
  options.data_dir = TestDataDir();
  auto maybe_backend = PersistentBackend::Open(options);
  if (!maybe_backend) {
    return maybe_backend.status();
  }
  auto maybe_restored = (*maybe_backend)->Restore();
  if (!maybe_restored) {
    return maybe_restored.status();
  }
  for (auto& restored : *maybe_restored) {
    if (restored.name == table_name) {
      return std::make_shared<Table>(std::move(restored.name),
                                     std::move(restored.schema),
                                     std::move(restored.operations));
    }
  }
  return NotFoundError("No such table",
                       GCP_ERROR_INFO().WithMetadata("table_name", table_name));
}

Status SetCells(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& table_name, std::string const& row_key,
    std::vector<SetCellParams>& set_cell_params) {
  ::google::bigtable::v2::MutateRowRequest mutation_request;
  mutation_request.set_table_name(table_name);
  mutation_request.set_row_key(row_key);

  for (auto m : set_cell_params) {
    auto* mutation_request_mutation = mutation_request.add_mutations();
    auto* set_cell_mutation = mutation_request_mutation->mutable_set_cell();
    set_cell_mutation->set_family_name(m.column_family_name);
    set_cell_mutation->set_column_qualifier(m.column_qualifier);
    set_cell_mutation->set_timestamp_micros(m.timestamp_micros);
    set_cell_mutation->set_value(m.data);
  }

  return table->MutateRow(mutation_request, kNoDeadline);
}

::google::bigtable::admin::v2::Table CreateSchema(
    std::string const& table_name,
    std::map<std::string, ::google::bigtable::admin::v2::ColumnFamily> const&
        column_families) {
  ::google::bigtable::admin::v2::Table schema;

  schema.set_name(table_name);
  for (auto const& cf : column_families) {
    (*schema.mutable_column_families())[cf.first] = cf.second;
  }

  return schema;
}

Status HasInMemoryCell(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key,
    std::string const& column_qualifier, int64_t timestamp_micros,
    std::string const& value) {
  auto* utilities =
      static_cast<InMemoryTableOperations*>(table->GetUtilities());
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
    return NotFoundError("no row key found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("row key", row_key)
                             .WithMetadata("column family", column_family));
  }

  auto& column_family_row = column_family_row_it->second;
  auto column_row_it = column_family_row.find(column_qualifier);
  if (column_row_it == column_family_row.end()) {
    return NotFoundError(
        "no column found with qualifier",
        GCP_ERROR_INFO().WithMetadata("column qualifier", column_qualifier));
  }

  auto& column_row = column_row_it->second;
  auto timestamp_it =
      column_row.find(std::chrono::microseconds(timestamp_micros));
  if (timestamp_it == column_row.end()) {
    return NotFoundError(
        "timestamp not found",
        GCP_ERROR_INFO().WithMetadata("timestamp",
                                      absl::StrFormat("%d", timestamp_micros)));
  }

  if (timestamp_it->second != value) {
    return NotFoundError("wrong value",
                         GCP_ERROR_INFO()
                             .WithMetadata("expected", value)
                             .WithMetadata("found", timestamp_it->second));
  }

  return Status();
}

Status HasPersistentCell(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key,
    std::string const& column_qualifier, int64_t timestamp_micros,
    std::string const& value) {
  auto* utilities =
      static_cast<PersistentTableOperations*>(table->GetUtilities());
  auto column_family_it = utilities->find(column_family);
  if (column_family_it == utilities->end()) {
    return NotFoundError(
        "column family not found in table",
        GCP_ERROR_INFO().WithMetadata("column family", column_family));
  }

  auto const& cf = std::static_pointer_cast<PersistentColumnFamily>(
      column_family_it->second);

  auto const encoded_key = KeyCoder::Encode(
      row_key, column_qualifier, static_cast<uint64_t>(timestamp_micros));
  auto iter = utilities->GetIterator(cf);
  iter->Seek(encoded_key);
  if (!iter->Valid() || iter->key() != encoded_key) {
    return NotFoundError(
        "cell not found",
        GCP_ERROR_INFO()
            .WithMetadata("row key", row_key)
            .WithMetadata("column family", column_family)
            .WithMetadata("column qualifier", column_qualifier)
            .WithMetadata("timestamp",
                          absl::StrFormat("%d", timestamp_micros)));
  }
  if (iter->value().ToString() != value) {
    return NotFoundError("wrong value",
                         GCP_ERROR_INFO()
                             .WithMetadata("expected", value)
                             .WithMetadata("found", iter->value().ToString()));
  }

  return Status();
}

Status HasInMemoryRow(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key) {
  auto* utilities =
      static_cast<InMemoryTableOperations*>(table->GetUtilities());
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

  return Status();
}

Status HasPersistentRow(
    std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
    std::string const& column_family, std::string const& row_key) {
  auto* utilities =
      static_cast<PersistentTableOperations*>(table->GetUtilities());
  auto column_family_it = utilities->find(column_family);
  if (column_family_it == utilities->end()) {
    return NotFoundError(
        "column family not found in table",
        GCP_ERROR_INFO().WithMetadata("column family", column_family));
  }

  auto const& cf = std::static_pointer_cast<PersistentColumnFamily>(
      column_family_it->second);
  auto iter = utilities->GetIterator(cf);
  std::string const row_prefix = KeyCoder::EscapedRowWithSeparator(row_key);
  iter->Seek(row_prefix);
  if (!iter->Valid() || !iter->key().starts_with(row_prefix)) {
    return NotFoundError("row key not found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("row key", row_key)
                             .WithMetadata("column family", column_family));
  }

  return Status();
}

bool IsPersistent(Table& table) {
  return dynamic_cast<PersistentTableOperations*>(table.GetUtilities()) !=
         nullptr;
}

Status HasCell(std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
               std::string const& column_family, std::string const& row_key,
               std::string const& column_qualifier, int64_t timestamp_micros,
               std::string const& value) {
  if (IsPersistent(*table)) {
    return HasPersistentCell(table, column_family, row_key, column_qualifier,
                             timestamp_micros, value);
  }
  return HasInMemoryCell(table, column_family, row_key, column_qualifier,
                         timestamp_micros, value);
}

Status HasRow(std::shared_ptr<google::cloud::bigtable::emulator::Table>& table,
              std::string const& column_family, std::string const& row_key) {
  if (IsPersistent(*table)) {
    return HasPersistentRow(table, column_family, row_key);
  }
  return HasInMemoryRow(table, column_family, row_key);
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
