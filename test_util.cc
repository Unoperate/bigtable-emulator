#include "test_util.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "table.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
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
  return Table::Create(schema, should_persist, "/tmp/");
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

  return table->MutateRow(mutation_request);
}

void DeletePersistentDB() {
  std::filesystem::path target = "/tmp/projects";
  std::filesystem::remove_all(target);
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
      column_row.find(std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::microseconds(timestamp_micros)));
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
                             .WithMetadata("row key", row_key)
                             .WithMetadata("column family", column_family));
  }
  if (decoded_key->col != column_qualifier) {
    return NotFoundError("no column found in column family",
                         GCP_ERROR_INFO()
                             .WithMetadata("column qualifier", column_qualifier)
                             .WithMetadata("column family", column_family));
  }

  while (iter->Valid() && decoded_key->row == row_key &&
         decoded_key->col == column_qualifier) {
    decoded_key = KeyCoder::Decode(iter->key().ToString());
    if (decoded_key->timestamp == static_cast<uint64_t>(timestamp_micros))
      break;
    iter->Next();
  }
  if (decoded_key->timestamp != static_cast<uint64_t>(timestamp_micros)) {
    return NotFoundError(
        "timestamp not found",
        GCP_ERROR_INFO().WithMetadata("expected timestamp",
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

  return Status();
}

Status HasPersistentRow(
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
                             .WithMetadata("row key", row_key)
                             .WithMetadata("column family", column_family));
  }

  return Status();
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
