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

#include "table.h"
#include "google/cloud/internal/big_endian.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_format.h"
#include "absl/types/variant.h"
#include "column_family.h"
#include "filter.h"
#include "google/protobuf/util/field_mask_util.h"
#include "key_coder.h"
#include "limits.h"
#include "range_set.h"
#include "re2/re2.h"
#include "redolog.h"
#include "row_streamer.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <grpcpp/support/sync_stream.h>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <ostream>
#include <stack>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

namespace btadmin = ::google::bigtable::admin::v2;

namespace {
Status ValidateAddToCellTransaction(
    ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
    absl::optional<::google::bigtable::admin::v2::Type> const& cf_value_type) {
  if (!cf_value_type.has_value() ||
      !cf_value_type.value().has_aggregate_type()) {
    return InvalidArgumentError(
        "column family is not configured to contain aggregation cells or "
        "aggregation type not properly configured",
        GCP_ERROR_INFO().WithMetadata("column family",
                                      add_to_cell.family_name()));
  }

  // Ensure that we support the aggregation that is configured in the
  // column family.
  switch (cf_value_type.value().aggregate_type().aggregator_case()) {
    case google::bigtable::admin::v2::Type::Aggregate::kSum:
    case google::bigtable::admin::v2::Type::Aggregate::kMin:
    case google::bigtable::admin::v2::Type::Aggregate::kMax:
      break;
    default:
      return UnimplementedError(
          "column family configured with unimplemented aggregation",
          GCP_ERROR_INFO()
              .WithMetadata("column family", add_to_cell.family_name())
              .WithMetadata("configured aggregation",
                            absl::StrFormat("%d", cf_value_type.value()
                                                      .aggregate_type()
                                                      .aggregator_case())));
  }

  if (!add_to_cell.has_input()) {
    return InvalidArgumentError(
        "input not set",
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }

  switch (add_to_cell.input().kind_case()) {
    case google::bigtable::v2::Value::kIntValue:
      if (!add_to_cell.input().has_int_value()) {
        return InvalidArgumentError("input value not set",
                                    GCP_ERROR_INFO().WithMetadata(
                                        "mutation", add_to_cell.DebugString()));
      }
      break;
    default:
      return InvalidArgumentError(
          "only int64 values are supported",
          GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }

  if (!add_to_cell.has_column_qualifier() ||
      !add_to_cell.column_qualifier().has_raw_value()) {
    return InvalidArgumentError(
        "column qualifier not set",
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }

  return Status();
}

// ProcessReadModifyWriteRuleResult records the result of a
// ReadModifyWriteRule computation for possible undo in the undo log
// and also updates the tmp_families temporary table (containing only
// one row) with the modified cell for later return.
void ProcessReadModifyWriteResult(
    std::shared_ptr<InMemoryColumnFamily>& column_family,
    std::string const& row_key,
    std::stack<absl::variant<DeleteValue, RestoreValue>>& undo,
    google::bigtable::v2::ReadModifyWriteRule const& rule,
    ReadModifyWriteCellResult& result,
    std::map<std::string, InMemoryColumnFamily>& tmp_families) {
  if (result.maybe_old_value.has_value()) {
    // We overwrote a cell, we need to record a RestoreValue in the undo log
    RestoreValue restore_value{*column_family, rule.column_qualifier(),
                               result.timestamp,
                               std::move(result.maybe_old_value.value())};
    undo.emplace(std::move(restore_value));
  } else {
    // We created a new cell -- we would need to delete it in any rollback
    DeleteValue delete_value{*column_family, rule.column_qualifier(),
                             result.timestamp};
    undo.emplace(std::move(delete_value));
  }

  // Record the cell in our local mini table here to use in
  // assembling a row of changed cells for return.
  tmp_families[rule.family_name()].SetCell(row_key, rule.column_qualifier(),
                                           result.timestamp,
                                           std::move(result.value));
}

StatusOr<StringRangeSet> CreateStringRangeSet(
    google::bigtable::v2::RowSet const& row_set) {
  StringRangeSet res;
  for (auto const& row_key : row_set.row_keys()) {
    if (row_key.size() > kMaxRowLen) {
      return InvalidArgumentError(
          "The row_key in row_set is longer than 4KiB",
          GCP_ERROR_INFO()
              .WithMetadata("row_key size",
                            absl::StrFormat("%zu", row_key.size()))
              .WithMetadata("row_set", row_set.DebugString()));
    }

    if (row_key.empty()) {
      return InvalidArgumentError(
          "`row_key` empty",
          GCP_ERROR_INFO().WithMetadata("row_set", row_set.DebugString()));
    }
    res.Sum(StringRangeSet::Range(row_key, false, row_key, false));
  }
  for (auto const& row_range : row_set.row_ranges()) {
    auto maybe_range = StringRangeSet::Range::FromRowRange(row_range);
    if (!maybe_range) {
      return maybe_range.status();
    }
    if (maybe_range->IsEmpty()) {
      continue;
    }
    res.Sum(*std::move(maybe_range));
  }
  return res;
}

std::shared_ptr<rocksdb::ColumnFamilyHandle> AdoptHandle(
    std::shared_ptr<rocksdb::TransactionDB> db,
    rocksdb::ColumnFamilyHandle* raw) {
  return std::shared_ptr<rocksdb::ColumnFamilyHandle>(
      raw, [db = std::move(db)](rocksdb::ColumnFamilyHandle* h) {
        if (!h) return;
        (void)db->DestroyColumnFamilyHandle(h);
      });
}

constexpr char kSchemaKey[] = "t_emulator:meta:schema_pb";

StatusOr<btadmin::Table> ApplyModifyColumnFamiliesToSchemaOnly(
    btadmin::ModifyColumnFamiliesRequest const& request,
    btadmin::Table schema) {
  for (auto const& modification : request.modifications()) {
    if (modification.drop()) {
      if (schema.deletion_protection()) {
        return FailedPreconditionError(
            "The table has deletion protection.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString()));
      }
      if (schema.mutable_column_families()->erase(modification.id()) == 0) {
        return NotFoundError("No such column family.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }
    } else if (modification.has_update()) {
      auto& cfs = *schema.mutable_column_families();
      auto cf_it = cfs.find(modification.id());
      if (cf_it == cfs.end()) {
        return NotFoundError("No such column family.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }

      using google::protobuf::util::FieldMaskUtil;

      google::protobuf::FieldMask effective_mask;
      if (modification.has_update_mask()) {
        effective_mask = modification.update_mask();
        if (!FieldMaskUtil::IsValidFieldMask<
                google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
          return InvalidArgumentError(
              "Update mask is invalid.",
              GCP_ERROR_INFO().WithMetadata("modification",
                                            modification.DebugString()));
        }
      } else {
        FieldMaskUtil::FromString("gc_rule", &effective_mask);
        if (!FieldMaskUtil::IsValidFieldMask<
                google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
          return InternalError("Default update mask is invalid.",
                               GCP_ERROR_INFO().WithMetadata(
                                   "mask", effective_mask.DebugString()));
        }
      }

      if (FieldMaskUtil::IsPathInFieldMask("value_type", effective_mask)) {
        return InvalidArgumentError(
            "The value_type cannot be changed after column family creation",
            GCP_ERROR_INFO().WithMetadata("mask",
                                          effective_mask.DebugString()));
      }

      FieldMaskUtil::MergeMessageTo(modification.update(), effective_mask,
                                    FieldMaskUtil::MergeOptions(),
                                    &(cf_it->second));
    } else if (modification.has_create()) {
      if (schema.column_families().find(modification.id()) !=
          schema.column_families().end()) {
        return AlreadyExistsError(
            "Column family already exists.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString()));
      }
      if (!schema.mutable_column_families()
               ->emplace(modification.id(), modification.create())
               .second) {
        return InternalError("Column family with schema but no data.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }
    } else {
      return UnimplementedError(
          "Unsupported modification.",
          GCP_ERROR_INFO().WithMetadata("modification",
                                        modification.DebugString()));
    }
  }
  return schema;
}

constexpr auto kRangeEnd = KeyCoder::kExistingEsc;
}  // anonymous namespace

StatusOr<std::shared_ptr<TableOperations>> InMemoryTableOperations::Create(
    google::bigtable::admin::v2::Table const& schema) {
  auto res = std::make_shared<InMemoryTableOperations>();
  for (auto const& column_family_def : schema.column_families()) {
    absl::optional<google::bigtable::admin::v2::Type> opt_value_type =
        absl::nullopt;

    // Support for complex types (AddToCell aggregations, e.t.c.).
    if (column_family_def.second.has_value_type()) {
      opt_value_type = column_family_def.second.value_type();
    }

    if (opt_value_type.has_value()) {
      auto cf = InMemoryColumnFamily::ConstructAggregateColumnFamily(
          opt_value_type.value());
      if (!cf) {
        return cf.status();
      }
      res->column_families_.emplace(column_family_def.first, cf.value());
    } else {
      res->column_families_.emplace(column_family_def.first,
                                    std::make_shared<InMemoryColumnFamily>());
    }
  }
  return std::shared_ptr<TableOperations>(res);
}

std::unique_ptr<RowTransaction> InMemoryTableOperations::NewRowTransaction(
    std::string const& row_key) {
  return std::make_unique<InMemoryRowTransaction>(this->get(), row_key);
}

StatusOr<CellStream> InMemoryTableOperations::CreateCellStream(
    std::shared_ptr<StringRangeSet> range_set,
    absl::optional<google::bigtable::v2::RowFilter> maybe_row_filter) const {
  auto table_stream_ctor = [range_set = std::move(range_set), this] {
    std::vector<std::unique_ptr<AbstractCellStreamImpl>> per_cf_streams;
    per_cf_streams.reserve(column_families_.size());
    for (auto const& column_family : column_families_) {
      auto stream = column_family.second->GetFilteredColumnFamilyStream(
          range_set, column_family.first);
      per_cf_streams.emplace_back(std::move(stream));
    }
    return CellStream(std::make_unique<FilteredInMemoryTableStream>(
        std::move(per_cf_streams)));
  };

  if (maybe_row_filter.has_value()) {
    return CreateFilter(maybe_row_filter.value(), table_stream_ctor);
  }

  return table_stream_ctor();
}

StatusOr<google::bigtable::admin::v2::Table>
InMemoryTableOperations::ModifyColumnFamilies(
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
    google::bigtable::admin::v2::Table schema) {
  auto new_column_families = column_families_;
  for (auto const& modification : request.modifications()) {
    if (modification.drop()) {
      if (schema.deletion_protection()) {
        return FailedPreconditionError(
            "The table has deletion protection.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString()));
      }
      if (new_column_families.erase(modification.id()) == 0) {
        return NotFoundError("No such column family.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }
      if (schema.mutable_column_families()->erase(modification.id()) == 0) {
        return InternalError("Column family with no schema.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }
    } else if (modification.has_update()) {
      auto& cfs = *schema.mutable_column_families();
      auto cf_it = cfs.find(modification.id());
      if (cf_it == cfs.end()) {
        return NotFoundError("No such column family.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }

      using google::protobuf::util::FieldMaskUtil;

      using google::protobuf::util::FieldMaskUtil;
      google::protobuf::FieldMask effective_mask;
      if (modification.has_update_mask()) {
        effective_mask = modification.update_mask();
        if (!FieldMaskUtil::IsValidFieldMask<
                google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
          return InvalidArgumentError(
              "Update mask is invalid.",
              GCP_ERROR_INFO().WithMetadata("modification",
                                            modification.DebugString()));
        }
      } else {
        FieldMaskUtil::FromString("gc_rule", &effective_mask);
        if (!FieldMaskUtil::IsValidFieldMask<
                google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
          return InternalError("Default update mask is invalid.",
                               GCP_ERROR_INFO().WithMetadata(
                                   "mask", effective_mask.DebugString()));
        }
      }

      // Disallow the modification of the type of data stored in the
      // column family (the aggregate type -- which is currently the
      // only supported type -- can always be set during column family
      // creation).
      if (FieldMaskUtil::IsPathInFieldMask("value_type", effective_mask)) {
        return InvalidArgumentError(
            "The value_type cannot be changed after column family creation",
            GCP_ERROR_INFO().WithMetadata("mask",
                                          effective_mask.DebugString()));
      }

      FieldMaskUtil::MergeMessageTo(modification.update(), effective_mask,
                                    FieldMaskUtil::MergeOptions(),
                                    &(cf_it->second));
    } else if (modification.has_create()) {
      std::shared_ptr<InMemoryColumnFamily> cf;
      // Have we been asked to create an aggregate column family?
      if (modification.create().has_value_type()) {
        auto value_type = modification.create().value_type();
        auto maybe_cf =
            InMemoryColumnFamily::ConstructAggregateColumnFamily(value_type);
        if (!maybe_cf) {
          return maybe_cf.status();
        }
        cf = std::move(maybe_cf.value());
      } else {
        cf = std::make_shared<InMemoryColumnFamily>();
      }
      if (!new_column_families.emplace(modification.id(), cf).second) {
        return AlreadyExistsError(
            "Column family already exists.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString()));
      }
      if (!schema.mutable_column_families()
               ->emplace(modification.id(), modification.create())
               .second) {
        return InternalError("Column family with schema but no data.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }
    } else {
      return UnimplementedError(
          "Unsupported modification.",
          GCP_ERROR_INFO().WithMetadata("modification",
                                        modification.DebugString()));
    }
  }
  // Defer destroying potentially large objects to after releasing the lock.
  column_families_.swap(new_column_families);
  return schema;
}

StatusOr<std::shared_ptr<TableOperations>> PersistentTableOperations::CreateNew(
    std::string const& data_root, google::bigtable::admin::v2::Table& schema) {
  std::string rel = schema.name();
  std::filesystem::path db_path = std::filesystem::path(data_root) / rel;
  std::filesystem::path parent_path = db_path.parent_path();

  std::error_code ec;
  std::filesystem::create_directories(parent_path, ec);
  if (ec) {
    return InternalError(
        "failed to create directory: " + parent_path.string() +
            "; Error status: " + ec.message(),
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }

  rocksdb::Options options;
  rocksdb::TransactionDBOptions txn_options;
  txn_options.lock_mgr_handle.reset(rocksdb::NewRangeLockManager(nullptr));

  options.create_if_missing = true;
  options.create_missing_column_families = false;
  std::vector<std::string> cf_names;
  bool db_exists = std::filesystem::exists(db_path / "CURRENT");

  if (!db_exists) {
    cf_names = {rocksdb::kDefaultColumnFamilyName};
  } else {
    rocksdb::Status list_s =
        rocksdb::DB::ListColumnFamilies(options, db_path.string(), &cf_names);
    if (!list_s.ok()) {
      auto msg = "Failed to list column families for table at " +
                 db_path.string() + "; " + list_s.ToString();
      return InternalError(
          msg, GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
    }
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> descs;
  descs.reserve(cf_names.size());
  for (auto const& n : cf_names) {
    descs.emplace_back(n, rocksdb::ColumnFamilyOptions());
  }

  rocksdb::TransactionDB* raw_db = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> raw_handles;
  rocksdb::Status status = rocksdb::TransactionDB::Open(
      options, txn_options, db_path.string(), descs, &raw_handles, &raw_db);
  if (!status.ok()) {
    return InternalError(
        "failed to open rocksdb instance; " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  std::shared_ptr<PersistentTableOperations> res(new PersistentTableOperations);
  res->db_.reset(raw_db);
  res->table_name_ = schema.name();

  std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>>
      handles_by_name;
  for (std::size_t i = 0; i < descs.size(); ++i) {
    handles_by_name[descs[i].name] = AdoptHandle(res->db_, raw_handles[i]);
  }

  auto maybe_schema = res->LoadSchema();
  if (maybe_schema.ok()) {
    schema = std::move(maybe_schema.value());
  } else if (maybe_schema.status().code() == StatusCode::kNotFound) {
    auto st = res->PersistSchema(schema);
    if (!st.ok()) return st;
  } else {
    return maybe_schema.status();
  }

  for (auto const& cfd : schema.column_families()) {
    absl::optional<google::bigtable::admin::v2::Type> opt_value_type =
        absl::nullopt;
    if (cfd.second.has_value_type()) opt_value_type = cfd.second.value_type();

    /*auto hit = handles_by_name.find(cfd.first);
    if (hit != handles_by_name.end()) {
      auto maybe_cf = PersistentColumnFamily::OpenExisting(
          res->db_, hit->second, opt_value_type);
      if (!maybe_cf) return maybe_cf.status();
      res->column_families_.emplace(cfd.first, maybe_cf.value());
      continue;
    }*/

    if (opt_value_type.has_value()) {
      auto new_cf = PersistentColumnFamily::ConstructAggregateColumnFamily(
          opt_value_type.value(), res->db_, cfd.first);
      if (!new_cf) return new_cf.status();
      res->column_families_.emplace(cfd.first, new_cf.value());
      handles_by_name[cfd.first] = new_cf.value()->GetHandle();
    } else {
      rocksdb::ColumnFamilyOptions opts;
      auto maybe_new_cf =
          PersistentColumnFamily::Create(res->db_, opts, cfd.first);
      if (!maybe_new_cf.ok()) {
        return InternalError(
            "failed to create column family " + cfd.first +
                "; Error status: " + maybe_new_cf.status().message(),
            GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
      }
      res->column_families_.emplace(cfd.first, maybe_new_cf.value());
      handles_by_name[cfd.first] = maybe_new_cf.value()->GetHandle();
    }
  }

  {
    SchemaRedoLog redo(res->db_);
    auto pending = redo.Load();
    if (pending.ok()) {
      auto st = res->ReconcileColumnFamiliesToTarget(pending.value());
      if (!st.ok()) return st;

      st = res->PersistSchema(pending.value());
      if (!st.ok()) return st;

      st = redo.Finish();
      if (!st.ok()) return st;

      schema = pending.value();
    } else if (pending.status().code() != StatusCode::kNotFound) {
      return pending.status();
    }
  }
  return StatusOr<std::shared_ptr<TableOperations>>(std::move(res));
}

StatusOr<std::shared_ptr<TableOperations>>
PersistentTableOperations::OpenExisting(
    std::string const& data_root, google::bigtable::admin::v2::Table& schema) {
  bool const create_if_missing = false;

  std::string rel = schema.name();
  std::filesystem::path db_path = std::filesystem::path(data_root) / rel;
  std::filesystem::path parent_path = db_path.parent_path();

  std::error_code ec;
  std::filesystem::create_directories(parent_path, ec);
  if (ec) {
    return InternalError(
        "failed to create directory: " + parent_path.string() +
            "; Error status: " + ec.message(),
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }

  rocksdb::Options options;
  rocksdb::TransactionDBOptions txn_options;
  txn_options.lock_mgr_handle.reset(rocksdb::NewRangeLockManager(nullptr));

  options.create_if_missing = create_if_missing;
  options.create_missing_column_families = false;

  std::vector<std::string> cf_names;
  bool db_exists = std::filesystem::exists(db_path / "CURRENT");

  if (!db_exists) {
    return NotFoundError(
        "No such table; database directory not initialized.",
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  rocksdb::Status list_s =
      rocksdb::DB::ListColumnFamilies(options, db_path.string(), &cf_names);
  if (!list_s.ok()) {
    auto msg = "Failed to list column families for table at " +
               db_path.string() + "; " + list_s.ToString();
    return InternalError(
        msg, GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> descs;
  descs.reserve(cf_names.size());
  for (auto const& n : cf_names) {
    descs.emplace_back(n, rocksdb::ColumnFamilyOptions());
  }

  rocksdb::TransactionDB* raw_db = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> raw_handles;
  rocksdb::Status status = rocksdb::TransactionDB::Open(
      options, txn_options, db_path.string(), descs, &raw_handles, &raw_db);
  if (!status.ok()) {
    return NotFoundError(
        "No such table; " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("path", db_path.string()));
  }

  std::shared_ptr<PersistentTableOperations> res(new PersistentTableOperations);
  res->db_.reset(raw_db);
  res->table_name_ = schema.name();

  std::map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>>
      handles_by_name;
  for (std::size_t i = 0; i < descs.size(); ++i) {
    handles_by_name[descs[i].name] = AdoptHandle(res->db_, raw_handles[i]);
  }

  auto maybe_schema = res->LoadSchema();
  if (!maybe_schema.ok()) return maybe_schema.status();
  schema = std::move(maybe_schema.value());

  for (auto const& cfd : schema.column_families()) {
    absl::optional<google::bigtable::admin::v2::Type> opt_value_type =
        absl::nullopt;
    if (cfd.second.has_value_type()) opt_value_type = cfd.second.value_type();

    auto hit = handles_by_name.find(cfd.first);
    if (hit != handles_by_name.end()) {
      auto maybe_cf = PersistentColumnFamily::OpenExisting(
          res->db_, hit->second, opt_value_type);
      if (!maybe_cf) return maybe_cf.status();
      res->column_families_.emplace(cfd.first, maybe_cf.value());
      continue;
    }

    if (opt_value_type.has_value()) {
      auto new_cf = PersistentColumnFamily::ConstructAggregateColumnFamily(
          opt_value_type.value(), res->db_, cfd.first);
      if (!new_cf) return new_cf.status();
      res->column_families_.emplace(cfd.first, new_cf.value());
      handles_by_name[cfd.first] = new_cf.value()->GetHandle();
    } else {
      rocksdb::ColumnFamilyOptions opts;
      auto maybe_new_cf =
          PersistentColumnFamily::Create(res->db_, opts, cfd.first);
      if (!maybe_new_cf.ok()) {
        return InternalError(
            "failed to create column family " + cfd.first +
                "; Error status: " + maybe_new_cf.status().message(),
            GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
      }
      res->column_families_.emplace(cfd.first, maybe_new_cf.value());
      handles_by_name[cfd.first] = maybe_new_cf.value()->GetHandle();
    }
  }

  {
    SchemaRedoLog redo(res->db_);
    auto pending = redo.Load();
    if (pending.ok()) {
      auto st = res->ReconcileColumnFamiliesToTarget(pending.value());
      if (!st.ok()) return st;

      st = res->PersistSchema(pending.value());
      if (!st.ok()) return st;

      st = redo.Finish();
      if (!st.ok()) return st;

      schema = pending.value();
    } else if (pending.status().code() != StatusCode::kNotFound) {
      return pending.status();
    }
  }

  return StatusOr<std::shared_ptr<TableOperations>>(std::move(res));
}

std::unique_ptr<RowTransaction> PersistentTableOperations::NewRowTransaction(
    std::string const& row_key) {
  return std::make_unique<PersistentRowTransaction>(this->get(), row_key,
                                                    db_.get());
}

StatusOr<CellStream> PersistentTableOperations::CreateCellStream(
    std::shared_ptr<StringRangeSet> range_set,
    absl::optional<google::bigtable::v2::RowFilter> maybe_row_filter) const {
  auto table_stream_ctor = [range_set = std::move(range_set), this] {
    std::vector<std::unique_ptr<AbstractCellStreamImpl>> per_cf_streams;
    per_cf_streams.reserve(column_families_.size());
    for (auto const& handle : column_families_) {
      auto stream =
          handle.second->GetFilteredColumnFamilyStream(range_set, handle.first);
      per_cf_streams.emplace_back(std::move(stream));
    }
    return CellStream(std::make_unique<FilteredPersistentTableStream>(
        std::move(per_cf_streams)));
  };

  if (maybe_row_filter.has_value()) {
    return CreateFilter(maybe_row_filter.value(), table_stream_ctor);
  }

  return table_stream_ctor();
}

StatusOr<std::size_t> PersistentTableOperations::GetRowCountEstimate() {
  // Just iterate all rows and count them.
  auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
  auto maybe_all_rows_stream = CreateCellStream(all_rows_set, absl::nullopt);
  if (!maybe_all_rows_stream) {
    return maybe_all_rows_stream.status();
  }

  auto& stream = *maybe_all_rows_stream;
  std::size_t res = 0;
  while (stream.HasValue()) {
    ++stream;
    ++res;
  }
  return res;
}

Status PersistentTableOperations::RemoveAllDataFromColumnFamilies() {
  return DropRowRange("");
}

Status PersistentTableOperations::DropRowRange(
    std::string const& row_key_prefix) {
  std::string range_end = row_key_prefix + kRangeEnd;
  auto txn = std::unique_ptr<rocksdb::Transaction>(
      db_->BeginTransaction(rocksdb::WriteOptions()));
  rocksdb::Endpoint start(row_key_prefix, false);
  rocksdb::Endpoint end(range_end, false);
  rocksdb::Status status;

  // 1. Lock the specified range in every column family
  for (auto& cf : column_families_) {
    status = txn->GetRangeLock(cf.second->GetRaw(), start, end);
    if (!status.ok()) {
      return InternalError(
          "Failed to drop row range: " + status.ToString(),
          GCP_ERROR_INFO().WithMetadata("row key prefix", row_key_prefix));
    }
  }

  // 2. Delete specified (and locked) range in each column family.
  for (auto& cf : column_families_) {
    auto cf_it = std::unique_ptr<rocksdb::Iterator>(
        txn->GetIterator(rocksdb::ReadOptions(), cf.second->GetRaw()));
    cf_it->Seek(row_key_prefix);
    while (cf_it->Valid() && cf_it->key().starts_with(row_key_prefix) &&
           cf_it->key().ToString() < range_end) {
      status = txn->Delete(cf.second->GetRaw(), cf_it->key());
      if (!status.ok()) {
        txn->Rollback();
        return InternalError(
            "Failed to drop row range: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key prefix", row_key_prefix));
      }
      cf_it->Next();
    }
  }

  status = txn->Commit();
  if (!status.ok()) {
    return InternalError(
        "Failed to drop row range: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("row key prefix", row_key_prefix));
  }
  return Status();
}

StatusOr<google::bigtable::admin::v2::Table>
PersistentTableOperations::ModifyColumnFamilies(
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
    google::bigtable::admin::v2::Table schema) {
  {
    SchemaRedoLog redo(db_);
    auto pending = redo.Load();
    if (pending.ok()) {
      auto st = ReconcileColumnFamiliesToTarget(pending.value());
      if (!st.ok()) return st;

      st = PersistSchema(pending.value());
      if (!st.ok()) return st;

      st = redo.Finish();
      if (!st.ok()) return st;

      schema = pending.value();
    } else if (pending.status().code() != StatusCode::kNotFound) {
      return pending.status();
    }
  }

  auto maybe_target = ApplyModifyColumnFamiliesToSchemaOnly(request, schema);
  if (!maybe_target) return maybe_target.status();
  auto target_schema = maybe_target.value();

  SchemaRedoLog redo(db_);
  auto st = redo.Begin(target_schema);
  if (!st.ok()) return st;

  st = ReconcileColumnFamiliesToTarget(target_schema);
  if (!st.ok()) return st;

  st = PersistSchema(target_schema);
  if (!st.ok()) return st;

  st = redo.Finish();
  if (!st.ok()) return st;

  return target_schema;
}

Status PersistentTableOperations::ReconcileColumnFamiliesToTarget(
    btadmin::Table const& target_schema) {
  for (auto const& kv : target_schema.column_families()) {
    auto const& cf_name = kv.first;
    auto const& cf_def = kv.second;

    if (column_families_.find(cf_name) != column_families_.end()) continue;

    std::shared_ptr<PersistentColumnFamily> cf;
    if (cf_def.has_value_type()) {
      auto maybe_cf = PersistentColumnFamily::ConstructAggregateColumnFamily(
          cf_def.value_type(), db_, cf_name);
      if (!maybe_cf) return maybe_cf.status();
      cf = std::move(maybe_cf.value());
    } else {
      auto maybe_cf = PersistentColumnFamily::Create(
          db_, rocksdb::ColumnFamilyOptions(), cf_name);
      if (!maybe_cf.ok()) {
        return InternalError(
            "Failed to create CF: " + maybe_cf.status().message(),
            GCP_ERROR_INFO().WithMetadata("cf", cf_name));
      }
      cf = maybe_cf.value();
    }
    column_families_.emplace(cf_name, std::move(cf));
  }

  for (auto it = column_families_.begin(); it != column_families_.end();) {
    auto const& name = it->first;
    if (target_schema.column_families().find(name) !=
        target_schema.column_families().end()) {
      ++it;
      continue;
    }

    auto handle = it->second->GetHandle();
    rocksdb::Status s = db_->DropColumnFamily(handle.get());
    if (!s.ok()) {
      return InternalError("Failed to drop CF in RocksDB: " + s.ToString(),
                           GCP_ERROR_INFO().WithMetadata("cf", name));
    }
    it = column_families_.erase(it);
  }

  return Status();
}

Status PersistentTableOperations::PersistSchema(
    google::bigtable::admin::v2::Table const& schema) {
  std::string bytes;
  if (!schema.SerializeToString(&bytes)) {
    return InternalError(
        "Failed to serialize table schema",
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }

  rocksdb::WriteOptions wopts;
  auto* default_cf = db_->DefaultColumnFamily();

  rocksdb::Status s = db_->Put(wopts, default_cf, kSchemaKey, bytes);
  if (!s.ok()) {
    return InternalError("Failed to persist schema in RocksDB: " + s.ToString(),
                         GCP_ERROR_INFO()
                             .WithMetadata("table", table_name_)
                             .WithMetadata("key", kSchemaKey));
  }
  return Status();
}

StatusOr<google::bigtable::admin::v2::Table>
PersistentTableOperations::LoadSchema() const {
  std::string serialized;

  rocksdb::ReadOptions ro;
  rocksdb::Status s =
      db_->Get(ro, db_->DefaultColumnFamily(), kSchemaKey, &serialized);

  if (s.IsNotFound()) {
    return NotFoundError("Persisted schema not found in default CF.",
                         GCP_ERROR_INFO().WithMetadata("key", kSchemaKey));
  }
  if (!s.ok()) {
    return InternalError("Failed to read persisted schema: " + s.ToString(),
                         GCP_ERROR_INFO().WithMetadata("key", kSchemaKey));
  }

  google::bigtable::admin::v2::Table schema;
  if (!schema.ParseFromString(serialized)) {
    return InternalError("Failed to parse persisted schema proto.",
                         GCP_ERROR_INFO().WithMetadata("key", kSchemaKey));
  }

  return schema;
}

StatusOr<std::shared_ptr<Table>> Table::Create(
    google::bigtable::admin::v2::Table schema, bool should_persist,
    std::string const& data_root) {
  std::shared_ptr<Table> res(new Table);
  auto status = res->Construct(std::move(schema), should_persist, data_root,
                               OpenMode::kCreateNew);
  if (!status.ok()) return status;
  return res;
}

google::bigtable::admin::v2::Table Table::GetSchema() const {
  auto lock_scope = utilities_->LockScope(false);
  return schema_;
}

Status Table::PrepareSchema() {
  if (schema_.granularity() ==
      btadmin::Table::TIMESTAMP_GRANULARITY_UNSPECIFIED) {
    schema_.set_granularity(btadmin::Table::MILLIS);
  }
  if (schema_.cluster_states_size() > 0) {
    return InvalidArgumentError(
        "`cluster_states` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema_.DebugString()));
  }
  if (schema_.has_restore_info()) {
    return InvalidArgumentError(
        "`restore_info` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema_.DebugString()));
  }
  if (schema_.has_change_stream_config()) {
    return UnimplementedError(
        "`change_stream_config` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema_.DebugString()));
  }
  if (schema_.has_automated_backup_policy()) {
    return UnimplementedError(
        "`automated_backup_policy` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema_.DebugString()));
  }
  return Status();
}

Status Table::SampleRowKeys(
    double pass_probability,
    grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer)
    const {
  if (pass_probability <= 0.0) {
    return InvalidArgumentError(
        "The sampling probabality must be positive",
        GCP_ERROR_INFO().WithMetadata("provided sampling probability",
                                      absl::StrFormat("%f", pass_probability)));
  }

  auto sample_every =
      static_cast<std::uint64_t>(std::ceil(1.0 / pass_probability));

  /**
   * For Persistent Table, CreateCellStream will create new RocksDB Iterator.
   * However, each iterator can work on a different version of data,
   * so for simplicity let's just acquire the exclusive lock (on the
   * assumption that SampleRowKeys won't be invoked often).
   */
  auto lock_scope = utilities_->LockScope(true);

  // First, stream all rows and cells and compute the offsets.
  auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
  auto maybe_all_rows_stream =
      utilities_->CreateCellStream(all_rows_set, absl::nullopt);
  if (!maybe_all_rows_stream) {
    return maybe_all_rows_stream.status();
  }

  auto& stream = *maybe_all_rows_stream;

  absl::optional<std::string> first_row_key;
  // The first row read will be used as a constant estimate of row
  // sizes. If we are sampling 1/n rows, the value added to the offset
  // (which is to be regarded as the size of all the rows before the
  // sampled one) will be (n * row_size_estimate).
  //
  // That is every time a row is sampled, we do: offset += (n *
  // row_size_estimate).
  std::size_t row_size_estimate = 0;

  for (; stream; ++stream) {
    if (first_row_key.has_value() &&
        stream->row_key() != first_row_key.value()) {
      break;
    }

    first_row_key = stream->row_key();

    row_size_estimate += stream->row_key().size();
    row_size_estimate += stream->column_qualifier().size();
    row_size_estimate += stream->value().size();
    row_size_estimate += sizeof(stream->timestamp());
  }

  if (!first_row_key.has_value()) {
    // No rows in the table
    google::bigtable::v2::SampleRowKeysResponse resp;
    resp.set_row_key("");
    resp.set_offset_bytes(0);

    auto opts = grpc::WriteOptions();
    opts.set_last_message();

    writer->WriteLast(std::move(resp), opts);
    return Status();
  }

  std::int64_t offset_delta = sample_every * row_size_estimate;

  google::bigtable::v2::RowFilter sample_filter;
  sample_filter.set_row_sample_filter(pass_probability);

  auto maybe_stream = utilities_->CreateCellStream(all_rows_set, sample_filter);
  if (!maybe_stream) {
    return maybe_stream.status();
  }

  auto& sampled_stream = *maybe_stream;

  std::int64_t offset = 0;

  bool wrote_a_sample;

  for (; sampled_stream; sampled_stream.Next(NextMode::kRow)) {
    google::bigtable::v2::SampleRowKeysResponse resp;
    offset += offset_delta;
    resp.set_row_key(sampled_stream->row_key());
    resp.set_offset_bytes(offset);

    writer->Write(std::move(resp));

    wrote_a_sample = true;
  }

  // Cloud bigtable client tests expect that, if they populated the
  // table with at least one row, then at least one row sample is
  // returned.
  //
  // In such a case, return any string that represents the last key,
  // and an offset that is the estimated row size * the number of rows
  // in the largest column family. We can return any string because
  // the keys returned need not be in the table. See the proto
  // specification.
  if (!wrote_a_sample) {
    auto maybe_row_count_estimate = utilities_->GetRowCountEstimate();
    if (!maybe_row_count_estimate.ok()) {
      return maybe_row_count_estimate.status();
    }

    std::size_t row_count_estimate = maybe_row_count_estimate.value();

    std::int64_t this_offset = row_count_estimate * row_size_estimate;

    google::bigtable::v2::SampleRowKeysResponse resp;
    resp.set_row_key("last_key");
    resp.set_offset_bytes(this_offset);
    writer->Write(std::move(resp));

    offset += this_offset;
  }

  google::bigtable::v2::SampleRowKeysResponse resp;
  resp.set_row_key("");
  // Client test code expects offset_bytes to be strictly
  // increasing.
  resp.set_offset_bytes(offset + 1);
  auto opts = grpc::WriteOptions();
  opts.set_last_message();
  writer->WriteLast(std::move(resp), opts);

  return Status();
}

Status Table::Construct(google::bigtable::admin::v2::Table schema,
                        bool should_persist, std::string const& data_root,
                        OpenMode mode) {
  schema_ = std::move(schema);

  Status parse_result = PrepareSchema();
  if (!parse_result.ok()) return parse_result;

  StatusOr<std::shared_ptr<TableOperations>> maybe_utilities;
  if (!should_persist) {
    maybe_utilities = InMemoryTableOperations::Create(schema_);
  } else {
    if (mode == OpenMode::kCreateNew) {
      maybe_utilities =
          PersistentTableOperations::CreateNew(data_root, schema_);
    } else {
      maybe_utilities =
          PersistentTableOperations::OpenExisting(data_root, schema_);
    }
  }

  if (!maybe_utilities.ok()) return maybe_utilities.status();
  utilities_ = maybe_utilities.value();
  return Status();
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
StatusOr<btadmin::Table> Table::ModifyColumnFamilies(
    btadmin::ModifyColumnFamiliesRequest const& request) {
  std::cout << "Modify column families: " << request.DebugString() << std::endl;
  auto lock_scope = utilities_->LockScope(true);

  auto maybe_new_schema = utilities_->ModifyColumnFamilies(request, schema_);
  if (!maybe_new_schema) {
    return maybe_new_schema.status();
  }
  auto new_schema = maybe_new_schema.value();

  schema_ = new_schema;
  auto s = utilities_->PersistSchema(schema_);
  if (!s.ok()) return s;

  return new_schema;
}
// NOLINTEND(readability-function-cognitive-complexity)

Status Table::Update(google::bigtable::admin::v2::Table const& new_schema,
                     google::protobuf::FieldMask const& to_update) {
  std::cout << "Update schema: " << new_schema.DebugString()
            << " mask: " << to_update.DebugString() << std::endl;
  using google::protobuf::util::FieldMaskUtil;

  google::protobuf::FieldMask allowed_mask;
  FieldMaskUtil::FromString(
      "change_stream_config,"
      "change_stream_config.retention_period,"
      "deletion_protection",
      &allowed_mask);
  if (!FieldMaskUtil::IsValidFieldMask<google::bigtable::admin::v2::Table>(
          to_update)) {
    return InvalidArgumentError(
        "Update mask is invalid.",
        GCP_ERROR_INFO().WithMetadata("mask", to_update.DebugString()));
  }

  google::protobuf::FieldMask disallowed_mask;
  FieldMaskUtil::Subtract<google::bigtable::admin::v2::Table>(
      to_update, allowed_mask, &disallowed_mask);
  if (disallowed_mask.paths_size() > 0) {
    return UnimplementedError(
        "Update mask contains disallowed fields.",
        GCP_ERROR_INFO().WithMetadata("mask", disallowed_mask.DebugString()));
  }

  auto lock_scope = utilities_->LockScope(true);
  FieldMaskUtil::MergeMessageTo(new_schema, to_update,
                                FieldMaskUtil::MergeOptions(), &schema_);
  auto s = utilities_->PersistSchema(schema_);
  if (!s.ok()) return s;

  return Status();
}

Status Table::MutateRow(google::bigtable::v2::MutateRowRequest const& request) {
  auto lock_scope = utilities_->LockScope(false);

  return DoMutationsWithPossibleRollback(request.row_key(),
                                         request.mutations());
}

StatusOr<std::shared_ptr<Table>> Table::Load(std::string const& table_name,
                                             std::string const& data_root) {
  google::bigtable::admin::v2::Table placeholder;
  placeholder.set_name(table_name);
  std::shared_ptr<Table> res(new Table);
  auto st = res->Construct(std::move(placeholder), true, data_root,
                           OpenMode::kOpenExisting);
  if (!st.ok()) return st;
  return res;
}

Status Table::DoMutationsWithPossibleRollback(
    std::string const& row_key,
    google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
        mutations) {
  if (row_key.size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata("row_key size",
                                      absl::StrFormat("%zu", row_key.size())));
  }

  std::unique_ptr<RowTransaction> row_transaction =
      utilities_->NewRowTransaction(row_key);

  for (auto const& mutation : mutations) {
    if (mutation.has_set_cell()) {
      auto const& set_cell = mutation.set_cell();

      absl::optional<std::chrono::milliseconds> timestamp_override =
          absl::nullopt;

      if (set_cell.timestamp_micros() < -1) {
        return InvalidArgumentError(
            "Timestamp micros cannot be < -1.",
            GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
      }

      if (set_cell.timestamp_micros() == -1) {
        timestamp_override.emplace(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()));
      }
      auto status = row_transaction->SetCell(set_cell, timestamp_override);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_add_to_cell()) {
      auto const& add_to_cell = mutation.add_to_cell();

      absl::optional<std::chrono::milliseconds> timestamp_override =
          absl::nullopt;

      std::chrono::milliseconds timestamp = std::chrono::milliseconds::zero();

      if (add_to_cell.has_timestamp() &&
          add_to_cell.timestamp().has_raw_timestamp_micros()) {
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds(
                add_to_cell.timestamp().raw_timestamp_micros()));
      }

      // If no valid timestamp is provided, override with the system time.
      if (timestamp <= std::chrono::milliseconds::zero()) {
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
        timestamp_override.emplace(std::move(timestamp));
      }

      auto status = row_transaction->AddToCell(add_to_cell, timestamp_override);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_merge_to_cell()) {
      return UnimplementedError(
          "Unsupported mutation type.",
          GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
    } else if (mutation.has_delete_from_column()) {
      auto const& delete_from_column = mutation.delete_from_column();
      auto status = row_transaction->DeleteFromColumn(delete_from_column);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_delete_from_family()) {
      auto const& delete_from_family = mutation.delete_from_family();
      auto status = row_transaction->DeleteFromFamily(delete_from_family);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_delete_from_row()) {
      auto status = row_transaction->DeleteFromRow();
      if (!status.ok()) {
        return status;
      }
    } else {
      return UnimplementedError(
          "Unsupported mutation type.",
          GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
    }
  }

  // If we get here, all mutations on the row have succeeded. We can
  // commit and return which will prevent the destructor from undoing
  // the transaction.
  return row_transaction->commit();
}

bool FilteredInMemoryTableStream::ApplyFilter(
    InternalFilter const& internal_filter) {
  if (!absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
      !absl::holds_alternative<ColumnRange>(internal_filter)) {
    return MergeCellStreams::ApplyFilter(internal_filter);
  }
  // internal_filter is either FamilyNameRegex or ColumnRange
  for (auto stream_it = unfinished_streams_.begin();
       stream_it != unfinished_streams_.end();) {
    auto* cf_stream =
        static_cast<FilteredInMemoryColumnFamilyStream*>(&(*stream_it)->impl());
    assert(cf_stream);

    if ((absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
         !re2::RE2::PartialMatch(
             cf_stream->column_family_name(),
             *absl::get<FamilyNameRegex>(internal_filter).regex)) ||
        (absl::holds_alternative<ColumnRange>(internal_filter) &&
         absl::get<ColumnRange>(internal_filter).column_family !=
             cf_stream->column_family_name())) {
      stream_it = unfinished_streams_.erase(stream_it);
      continue;
    }

    if (absl::holds_alternative<ColumnRange>(internal_filter) &&
        absl::get<ColumnRange>(internal_filter).column_family ==
            cf_stream->column_family_name()) {
      cf_stream->ApplyFilter(internal_filter);
    }

    stream_it++;
  }

  return true;
}

std::vector<CellStream> FilteredInMemoryTableStream::CreateCellStreams(
    std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams) {
  std::vector<CellStream> res;
  res.reserve(cf_streams.size());
  for (auto& stream : cf_streams) {
    res.emplace_back(std::move(stream));
  }
  return res;
}

bool FilteredPersistentTableStream::ApplyFilter(
    InternalFilter const& internal_filter) {
  if (!absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
      !absl::holds_alternative<ColumnRange>(internal_filter)) {
    return MergeCellStreams::ApplyFilter(internal_filter);
  }
  // internal_filter is either FamilyNameRegex or ColumnRange
  for (auto stream_it = unfinished_streams_.begin();
       stream_it != unfinished_streams_.end();) {
    auto* cf_stream = static_cast<FilteredPersistentColumnFamilyStream*>(
        &(*stream_it)->impl());
    assert(cf_stream);

    if ((absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
         !re2::RE2::PartialMatch(
             cf_stream->column_family_name(),
             *absl::get<FamilyNameRegex>(internal_filter).regex)) ||
        (absl::holds_alternative<ColumnRange>(internal_filter) &&
         absl::get<ColumnRange>(internal_filter).column_family !=
             cf_stream->column_family_name())) {
      stream_it = unfinished_streams_.erase(stream_it);
      continue;
    }

    if (absl::holds_alternative<ColumnRange>(internal_filter) &&
        absl::get<ColumnRange>(internal_filter).column_family ==
            cf_stream->column_family_name()) {
      cf_stream->ApplyFilter(internal_filter);
    }

    stream_it++;
  }

  return true;
}

std::vector<CellStream> FilteredPersistentTableStream::CreateCellStreams(
    std::vector<std::unique_ptr<AbstractCellStreamImpl>> cf_streams) {
  std::vector<CellStream> res;
  res.reserve(cf_streams.size());
  for (auto& stream : cf_streams) {
    res.emplace_back(std::move(stream));
  }
  return res;
}

StatusOr<google::bigtable::v2::CheckAndMutateRowResponse>
Table::CheckAndMutateRow(
    google::bigtable::v2::CheckAndMutateRowRequest const& request) {
  auto lock_scope = utilities_->LockScope(false);

  auto const& row_key = request.row_key();

  if (row_key.size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO()
            .WithMetadata("row_key size",
                          absl::StrFormat("%zu", row_key.size()))
            .WithMetadata("CheckAndMutateRequest", request.DebugString()));
  }

  if (row_key.empty()) {
    return InvalidArgumentError(
        "row key required",
        GCP_ERROR_INFO().WithMetadata("CheckAndMutateRowRequest",
                                      request.DebugString()));
  }

  if (request.true_mutations_size() == 0 &&
      request.false_mutations_size() == 0) {
    return InvalidArgumentError(
        "both true mutations and false mutations are empty",
        GCP_ERROR_INFO().WithMetadata("CheckAndMutateRowRequest",
                                      request.DebugString()));
  }

  auto range_set = std::make_shared<StringRangeSet>();
  range_set->Sum(StringRangeSet::Range(row_key, false, row_key, false));

  StatusOr<CellStream> maybe_stream;
  if (request.has_predicate_filter()) {
    maybe_stream = utilities_->CreateCellStream(
        range_set, std::move(request.predicate_filter()));
  } else {
    maybe_stream = utilities_->CreateCellStream(range_set, absl::nullopt);
  }

  if (!maybe_stream) {
    return maybe_stream.status();
  }

  bool a_cell_is_found = false;

  CellStream& stream = *maybe_stream;
  if (stream) {  // At least one cell/value found when filter is applied
    a_cell_is_found = true;
  }

  Status status;
  if (a_cell_is_found) {
    status = DoMutationsWithPossibleRollback(request.row_key(),
                                             request.true_mutations());
  } else {
    status = DoMutationsWithPossibleRollback(request.row_key(),
                                             request.false_mutations());
  }

  if (!status.ok()) {
    return status;
  }

  google::bigtable::v2::CheckAndMutateRowResponse success_response;
  success_response.set_predicate_matched(a_cell_is_found);

  return success_response;
}

Status Table::ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                       RowStreamer& row_streamer) const {
  std::shared_ptr<StringRangeSet> row_set;
  // We need to check that, not only do we have rows, but that it is
  // not empty (i.e. at least one of row_range or rows is specified).
  if (request.has_rows() && (request.rows().row_ranges_size() > 0 ||
                             request.rows().row_keys_size() > 0)) {
    auto maybe_row_set = CreateStringRangeSet(request.rows());
    if (!maybe_row_set) {
      return maybe_row_set.status();
    }

    row_set = std::make_shared<StringRangeSet>(*std::move(maybe_row_set));
  } else {
    row_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
  }
  auto lock_scope = utilities_->LockScope(false);
  StatusOr<CellStream> maybe_stream;
  if (request.has_filter()) {
    maybe_stream =
        utilities_->CreateCellStream(row_set, std::move(request.filter()));
  } else {
    maybe_stream = utilities_->CreateCellStream(row_set, absl::nullopt);
  }

  if (!maybe_stream) {
    return maybe_stream.status();
  }

  std::int64_t rows_count = 0;
  absl::optional<std::string> current_row_key;
  CellStream& stream = *maybe_stream;
  for (; stream; ++stream) {
    std::cout << "Row: " << stream->row_key()
              << " column_family: " << stream->column_family()
              << " column_qualifier: " << stream->column_qualifier()
              << " column_timestamp: " << stream->timestamp().count()
              << " column_value: " << stream->value() << " label: "
              << (stream->HasLabel() ? stream->label() : std::string("unset"))
              << std::endl;
    if (request.rows_limit() > 0) {
      if (!current_row_key.has_value() ||
          stream->row_key() != current_row_key.value()) {
        rows_count++;
        current_row_key = stream->row_key();
      }

      if (rows_count > request.rows_limit()) {
        break;
      }
    }

    if (!row_streamer.Stream(*stream)) {
      std::cout << "HOW?" << std::endl;
      return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
    }
  }

  if (!row_streamer.Flush(true)) {
    std::cout << "Flush failed?" << std::endl;
    return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
  }
  std::cout << "Print stop" << std::endl;
  return Status();
}

bool Table::IsDeleteProtected() const {
  auto lock_scope = utilities_->LockScope(false);
  return IsDeleteProtectedNoLock();
}

bool Table::IsDeleteProtectedNoLock() const {
  auto lock_scope = utilities_->LockScope(false);
  return schema_.deletion_protection();
}

Status Table::DropRowRange(
    ::google::bigtable::admin::v2::DropRowRangeRequest const& request) {
  auto lock_scope = utilities_->LockScope(false);

  if (!request.has_row_key_prefix() &&
      !request.has_delete_all_data_from_table()) {
    return InvalidArgumentError(
        "Neither row prefix nor deleted all data from table is set",
        GCP_ERROR_INFO().WithMetadata("DropRowRange request",
                                      request.DebugString()));
  }

  if (request.has_delete_all_data_from_table()) {
    Status status = utilities_->RemoveAllDataFromColumnFamilies();
    if (!status.ok()) {
      return status;
    }
    return Status();
  }

  auto const& row_key_prefix = request.row_key_prefix();
  if (request.row_key_prefix().size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key_prefix is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata(
            "row_key_prefix size",
            absl::StrFormat("%zu", request.row_key_prefix().size())));
  }
  if (row_key_prefix.empty()) {
    return InvalidArgumentError(
        "Row prefix provided is empty.",
        GCP_ERROR_INFO().WithMetadata("DropRowRange request",
                                      request.DebugString()));
  }

  std::cout << "START" << std::endl;
  Status status = utilities_->DropRowRange(row_key_prefix);
  if (!status.ok()) {
    return status;
  }
  return Status();
}

StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
Table::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request) {
  if (request.row_key().size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata(
            "row_key size", absl::StrFormat("%zu", request.row_key().size())));
  }

  auto lock_scope = utilities_->LockScope(false);

  std::unique_ptr<RowTransaction> row_transaction =
      utilities_->NewRowTransaction(request.row_key());

  auto maybe_response = row_transaction->ReadModifyWriteRow(request);
  if (!maybe_response) {
    return maybe_response.status();
  }

  row_transaction->commit();

  return std::move(maybe_response.value());
}

// NOLINTBEGIN(readability-convert-member-functions-to-static)
Status InMemoryRowTransaction::AddToCell(
    ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
    absl::optional<std::chrono::milliseconds> timestamp_override) {
  auto status = utilities_->FindColumnFamily(add_to_cell);
  if (!status.ok()) {
    return status.status();
  }

  auto const& cf = status.value();
  auto cf_value_type = cf->GetValueType();

  auto validation_res =
      ValidateAddToCellTransaction(add_to_cell, cf_value_type);
  if (!validation_res.ok()) {
    return validation_res;
  }

  auto int64_input = add_to_cell.input().int_value();

  auto value = google::cloud::internal::EncodeBigEndian(int64_input);

  std::chrono::milliseconds ts_ms;
  if (timestamp_override.has_value()) {
    ts_ms = timestamp_override.value();
  } else {
    ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds(
            add_to_cell.timestamp().raw_timestamp_micros()));
  }

  auto column_qualifier = add_to_cell.column_qualifier().raw_value();
  auto maybe_old_value =
      cf->UpdateCell(row_key_, column_qualifier, ts_ms, value);
  if (!maybe_old_value) {
    return maybe_old_value.status();
  }

  if (!maybe_old_value.value()) {
    DeleteValue delete_value{*cf, std::move(column_qualifier), ts_ms};
    undo_.emplace(std::move(delete_value));
  } else {
    RestoreValue restore_value{*cf, std::move(column_qualifier), ts_ms,
                               std::move(maybe_old_value.value().value())};
    undo_.emplace(std::move(restore_value));
  }
  return Status();
}

Status RowTransaction::MergeToCell(
    ::google::bigtable::v2::Mutation_MergeToCell const& merge_to_cell) {
  return UnimplementedError(
      "Unsupported mutation type.",
      GCP_ERROR_INFO().WithMetadata("mutation", merge_to_cell.DebugString()));
}
// NOLINTEND(readability-convert-member-functions-to-static)

Status InMemoryRowTransaction::DeleteFromColumn(
    ::google::bigtable::v2::Mutation_DeleteFromColumn const&
        delete_from_column) {
  auto maybe_column_family = utilities_->FindColumnFamily(delete_from_column);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  // We need to check if the given timerange is empty or reversed, but
  // only up to the server's time accuracy (in our case, milliseconds)
  // - For example a time range of [1000, 1200] would be empty.
  if (delete_from_column.has_time_range()) {
    auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds(
            delete_from_column.time_range().start_timestamp_micros()));
    auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds(
            delete_from_column.time_range().end_timestamp_micros()));

    // An end timestamp micros of 0 is to be interpreted as infinity,
    // so we allow that.
    if (end <= start &&
        delete_from_column.time_range().end_timestamp_micros() != 0) {
      return InvalidArgumentError(
          "empty or reversed time range: the end timestamp must be more than "
          "the start timestamp when they are truncated to the server's time "
          "precision (milliseconds)",
          GCP_ERROR_INFO().WithMetadata("delete_from_column proto",
                                        delete_from_column.DebugString()));
    }
  }

  auto const& column_family = maybe_column_family.value();

  auto deleted_cells = column_family->DeleteColumn(
      row_key_, delete_from_column.column_qualifier(),
      delete_from_column.time_range());

  for (auto& cell : deleted_cells) {
    RestoreValue restore_value{
        *column_family, delete_from_column.column_qualifier(),
        std::move(cell.timestamp), std::move(cell.value)};
    undo_.emplace(std::move(restore_value));
  }

  return Status();
}

Status InMemoryRowTransaction::DeleteFromRow() {
  bool row_existed = false;
  for (auto& column_family : *utilities_) {
    auto deleted_columns = column_family.second->DeleteRow(row_key_);

    for (auto& column : deleted_columns) {
      for (auto& cell : column.second) {
        RestoreValue restore_value = {*column_family.second,
                                      std::move(column.first), cell.timestamp,
                                      std::move(cell.value)};
        undo_.emplace(std::move(restore_value));
        row_existed = true;
      }
    }
  }

  if (row_existed) {
    return Status();
  }

  return NotFoundError("row not found in table",
                       GCP_ERROR_INFO().WithMetadata("row", row_key_));
}

Status InMemoryRowTransaction::DeleteFromFamily(
    ::google::bigtable::v2::Mutation_DeleteFromFamily const&
        delete_from_family) {
  // If the request references an incorrect schema (non-existent
  // column family) then return a failure status error immediately.
  auto maybe_column_family = utilities_->FindColumnFamily(delete_from_family);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  auto column_family_it = utilities_->find(delete_from_family.family_name());
  if (column_family_it == utilities_->end()) {
    return NotFoundError(
        "column family not found in table",
        GCP_ERROR_INFO().WithMetadata("column family",
                                      delete_from_family.family_name()));
  }

  auto ccf = column_family_it->second;
  if (!ccf->RowKeyExists(row_key_)) {
    // The row does not exist
    return NotFoundError(
        "row key is not found in column family",
        GCP_ERROR_INFO()
            .WithMetadata("row key", row_key_)
            .WithMetadata("column family", column_family_it->first));
  }

  auto deleted = column_family_it->second->DeleteRow(row_key_);
  for (auto const& column : deleted) {
    for (auto const& cell : column.second) {
      RestoreValue restore_value{*column_family_it->second,
                                 std::move(column.first), cell.timestamp,
                                 std::move(cell.value)};
      undo_.emplace(std::move(restore_value));
    }
  }

  return Status();
}

// timestamp_override, if provided, will be used instead of
// set_cell.timestamp. The override is used to set the timestamp to
// the server time in case a timestamp <= 0 is provided.
Status InMemoryRowTransaction::SetCell(
    ::google::bigtable::v2::Mutation_SetCell const& set_cell,
    absl::optional<std::chrono::milliseconds> timestamp_override) {
  auto maybe_column_family = utilities_->FindColumnFamily(set_cell);
  if (!maybe_column_family) {
    return maybe_column_family.status();
  }

  auto const& column_family = maybe_column_family.value();

  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::microseconds(set_cell.timestamp_micros()));

  if (timestamp_override.has_value()) {
    timestamp = timestamp_override.value();
  }

  auto maybe_old_value = column_family->SetCell(
      row_key_, set_cell.column_qualifier(), timestamp, set_cell.value());

  if (!maybe_old_value) {
    DeleteValue delete_value{*column_family,
                             std::move(set_cell.column_qualifier()), timestamp};
    undo_.emplace(std::move(delete_value));
  } else {
    RestoreValue restore_value{*column_family,
                               std::move(set_cell.column_qualifier()),
                               timestamp, std::move(maybe_old_value.value())};
    undo_.emplace(std::move(restore_value));
  }

  return Status();
}

Status PersistentRowTransaction::commit() {
  rocksdb::Status status = txn_->Commit();
  if (!status.ok()) {
    txn_->Rollback();
    return InvalidArgumentError(
        "Failed to commit the transaction: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("row key", row_key_));
  }
  return Status();
}

Status PersistentRowTransaction::SetCell(
    ::google::bigtable::v2::Mutation_SetCell const& set_cell,
    absl::optional<std::chrono::milliseconds> timestamp_override) {
  auto maybe_column_family = utilities_->FindColumnFamily(set_cell);
  if (!maybe_column_family) {
    return maybe_column_family.status();
  }

  auto const& column_family = maybe_column_family.value();

  auto timestamp = set_cell.timestamp_micros();
  if (timestamp_override.has_value()) {
    timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                    timestamp_override.value())
                    .count();
  }

  std::cout << "Writing to column family: "
            << column_family->GetRaw()->GetName() << "; row key: " << row_key_
            << "; column: " << set_cell.column_qualifier()
            << "; value: " << set_cell.value() << "; timestamp: " << timestamp
            << std::endl;
  std::string prepared_key =
      KeyCoder::Encode(row_key_, set_cell.column_qualifier(), timestamp);
  rocksdb::Status status =
      txn_->Put(column_family->GetRaw(), prepared_key, set_cell.value());

  if (!status.ok()) {
    return InternalError(
        "Failed to put write into the transaction; " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("column_family",
                                      column_family->GetRaw()->GetName()));
  }
  return Status();
}

Status PersistentRowTransaction::AddToCell(
    ::google::bigtable::v2::Mutation_AddToCell const& add_to_cell,
    absl::optional<std::chrono::milliseconds> timestamp_override) {
  auto find_status = utilities_->FindColumnFamily(add_to_cell);
  if (!find_status.ok()) {
    return find_status.status();
  }

  auto const& cf = find_status.value();
  auto cf_value_type = cf->GetValueType();

  auto validation_res =
      ValidateAddToCellTransaction(add_to_cell, cf_value_type);
  if (!validation_res.ok()) {
    return validation_res;
  }

  auto int64_input = add_to_cell.input().int_value();

  auto value = google::cloud::internal::EncodeBigEndian(int64_input);

  auto timestamp = add_to_cell.timestamp().raw_timestamp_micros();
  if (timestamp_override.has_value()) {
    timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                    timestamp_override.value())
                    .count();
  }

  rocksdb::ColumnFamilyHandle* raw_cf = cf->GetRaw();

  std::string start_key = KeyCoder::PartialEncode(
      row_key_, add_to_cell.column_qualifier().raw_value());
  std::string end_key = start_key + kRangeEnd;
  rocksdb::Endpoint start(start_key, false);
  rocksdb::Endpoint end(end_key, false);

  /**
   * This will lock a range [partial_key; partial_key + 0xFF]; because of the
   * key encoding, the first byte after partial key in the full key will be
   * 0x00, so this way, we will lock the range containing this key. For more
   * detail on the encoding, see key_coder.h.
   */
  rocksdb::Status status = txn_->GetRangeLock(raw_cf, start, end);
  if (!status.ok()) {
    return InternalError(
        "Failed to add to cell: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }

  std::unique_ptr<rocksdb::Iterator> it(
      txn_->GetIterator(rocksdb::ReadOptions(), raw_cf));
  it->Seek(start_key);
  std::string new_value = value;
  if (it->Valid() && it->key().starts_with(start_key)) {
    auto maybe_result =
        cf->update_cell_(it->value().ToString(), std::move(value));
    if (!maybe_result) {
      return InternalError(
          "Failed to add to cell: " + maybe_result.status().message(),
          GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
    }
    new_value = maybe_result.value();
  }

  std::string new_key = KeyCoder::Encode(
      row_key_, add_to_cell.column_qualifier().raw_value(), timestamp);
  status = txn_->Put(raw_cf, new_key, std::move(new_value));
  if (!status.ok()) {
    return InternalError(
        "Failed to add to cell: " + status.ToString(),
        GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
  }
  if (it->Valid() && it->key().starts_with(start_key) && it->key() != new_key) {
    status = txn_->Delete(raw_cf, it->key());
    if (!status.ok()) {
      return InternalError(
          "Failed to add to cell: " + status.ToString(),
          GCP_ERROR_INFO().WithMetadata("mutation", add_to_cell.DebugString()));
    }
  }

  return Status();
}

Status PersistentRowTransaction::DeleteFromColumn(
    ::google::bigtable::v2::Mutation_DeleteFromColumn const&
        delete_from_column) {
  auto maybe_column_family = utilities_->FindColumnFamily(delete_from_column);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  // We need to check if the given timerange is empty or reversed, but
  // only up to the server's time accuracy (in our case, milliseconds)
  // - For example a time range of [1000, 1200] would be empty.
  uint64_t start_count = std::numeric_limits<uint64_t>::max();
  uint64_t end_count = std::numeric_limits<uint64_t>::min();
  if (delete_from_column.has_time_range()) {
    auto start = std::chrono::microseconds(
        delete_from_column.time_range().start_timestamp_micros());
    auto end = std::chrono::microseconds(
        delete_from_column.time_range().end_timestamp_micros());

    // An end timestamp micros of 0 is to be interpreted as infinity,
    // so we allow that.
    if (end <= start &&
        delete_from_column.time_range().end_timestamp_micros() != 0) {
      return InvalidArgumentError(
          "empty or reversed time range: the end timestamp must be more than "
          "the start timestamp when they are truncated to the server's time "
          "precision (milliseconds)",
          GCP_ERROR_INFO().WithMetadata("delete_from_column proto",
                                        delete_from_column.DebugString()));
    }

    start_count = start.count();
    if (delete_from_column.time_range().end_timestamp_micros() == 0) {
      end_count = std::numeric_limits<uint64_t>::max();
    } else {
      end_count = end.count();
    }
  }

  // The idea here is to iterate over each row, and for each row
  // lock the specified col+timestamp range

  std::vector<std::string> starts;
  std::vector<std::string> ends;

  // 1. Acquire locks
  auto const& column_family = maybe_column_family.value();
  auto cf_it = std::unique_ptr<rocksdb::Iterator>(
      txn_->GetIterator(rocksdb::ReadOptions(), column_family->GetRaw()));
  cf_it->SeekToFirst();
  while (cf_it->Valid()) {
    auto maybe_decoded = KeyCoder::Decode(
        std::string_view(cf_it->key().data(), cf_it->key().size()));
    if (!maybe_decoded) {
      return InternalError(
          "Failed to delete from column: " + maybe_decoded.status().message(),
          GCP_ERROR_INFO());
    }
    auto const& decoded = maybe_decoded.value();

    // Remember that we keep timestamps reversed; that's why start_key
    // contains the end_count.
    auto start_key = KeyCoder::Encode(
        decoded.row, delete_from_column.column_qualifier(), end_count);
    auto end_key = KeyCoder::Encode(
        decoded.row, delete_from_column.column_qualifier(), start_count);
    rocksdb::Endpoint start(start_key, false);
    rocksdb::Endpoint end(end_key, false);
    rocksdb::Status status =
        txn_->GetRangeLock(column_family->GetRaw(), start, end);
    if (!status.ok()) {
      return InternalError("Failed to delete from column: " + status.ToString(),
                           GCP_ERROR_INFO());
    }

    starts.push_back(start_key);
    ends.push_back(end_key);

    cf_it->Seek(decoded.row + kRangeEnd);
  }

  // 2. Now that things to delete are locked, we can delete them
  for (size_t i = 0; i < starts.size(); ++i) {
    cf_it->Seek(starts[i]);
    while (cf_it->Valid()) {
      std::cout << "Deleting from column; Key: " << cf_it->key().ToString()
                << '\n';
      if (cf_it->key().ToString() > ends[i]) {
        break;
      }
      auto status = txn_->Delete(column_family->GetRaw(), cf_it->key());
      if (!status.ok()) {
        return InternalError(
            "Failed to delete from column: " + status.ToString(),
            GCP_ERROR_INFO());
      }
      cf_it->Next();
    }
    cf_it->Refresh();
  }
  return Status();
}

Status PersistentRowTransaction::DeleteFromRow() {
  bool row_existed = false;
  std::string end_key = row_key_ + kRangeEnd;
  rocksdb::Endpoint start(row_key_, false);
  rocksdb::Endpoint end(end_key, false);

  for (auto& column_family : *utilities_) {
    rocksdb::Status status =
        txn_->GetRangeLock(column_family.second->GetRaw(), start, end);
    if (!status.ok()) {
      return InternalError("Failed to delete from row: " + status.ToString(),
                           GCP_ERROR_INFO().WithMetadata("row key", row_key_));
    }

    auto cf_it = std::unique_ptr<rocksdb::Iterator>(txn_->GetIterator(
        rocksdb::ReadOptions(), column_family.second->GetRaw()));
    cf_it->Seek(row_key_);
    if (!cf_it->Valid() || !cf_it->key().starts_with(row_key_)) {
      continue;
    }
    row_existed = true;

    std::vector<std::string> keys_to_delete;
    while (cf_it->Valid() && cf_it->key().starts_with(row_key_)) {
      keys_to_delete.push_back(cf_it->key().ToString());
      cf_it->Next();
    }
    for (auto const& key : keys_to_delete) {
      status = txn_->Delete(column_family.second->GetRaw(), key);
      if (!status.ok()) {
        return InternalError(
            "Failed to delete from row: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key", row_key_));
      }
    }
  }

  if (row_existed) {
    return Status();
  }

  return NotFoundError("row not found in table",
                       GCP_ERROR_INFO().WithMetadata("row", row_key_));
}

Status PersistentRowTransaction::DeleteFromFamily(
    ::google::bigtable::v2::Mutation_DeleteFromFamily const&
        delete_from_family) {
  auto maybe_column_family = utilities_->FindColumnFamily(delete_from_family);
  if (!maybe_column_family.ok()) {
    return maybe_column_family.status();
  }

  auto column_family_it = utilities_->find(delete_from_family.family_name());
  if (column_family_it == utilities_->end()) {
    return NotFoundError(
        "column family not found in table",
        GCP_ERROR_INFO().WithMetadata("column family",
                                      delete_from_family.family_name()));
  }

  auto const& column_family = column_family_it->second;
  std::string end_key = row_key_ + kRangeEnd;
  rocksdb::Endpoint start(row_key_, false);
  rocksdb::Endpoint end(end_key, false);

  rocksdb::Status status =
      txn_->GetRangeLock(column_family->GetRaw(), start, end);
  if (!status.ok()) {
    return InternalError(
        "Failed to delete from family: " + status.ToString(),
        GCP_ERROR_INFO()
            .WithMetadata("row key", row_key_)
            .WithMetadata("column family", delete_from_family.family_name()));
  }

  auto cf_it = std::unique_ptr<rocksdb::Iterator>(
      txn_->GetIterator(rocksdb::ReadOptions(), column_family->GetRaw()));
  cf_it->Seek(row_key_);
  if (!cf_it->Valid() || !cf_it->key().starts_with(row_key_)) {
    return NotFoundError(
        "row key is not found in column family",
        GCP_ERROR_INFO()
            .WithMetadata("row key", row_key_)
            .WithMetadata("column family", column_family_it->first));
  }

  std::vector<std::string> keys_to_delete;
  while (cf_it->Valid() && cf_it->key().starts_with(row_key_)) {
    keys_to_delete.push_back(cf_it->key().ToString());
    cf_it->Next();
  }
  for (auto const& key : keys_to_delete) {
    status = txn_->Delete(column_family->GetRaw(), key);
    if (!status.ok()) {
      return InternalError(
          "Failed to delete from family: " + status.ToString(),
          GCP_ERROR_INFO()
              .WithMetadata("row key", row_key_)
              .WithMetadata("column family", delete_from_family.family_name()));
    }
  }

  return Status();
}

StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
PersistentRowTransaction::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request) {
  if (row_key_.empty()) {
    return InvalidArgumentError(
        "row key not set",
        GCP_ERROR_INFO().WithMetadata("request", request.DebugString()));
  }
  std::cout << "Processing ReadModifyWriteRow request\n";

  // Copying behaviour from the InMemoryRowTransaction.
  // In this case, InMemoryColumnFamily is a perfect storage
  // for the partial results, and it lets us reuse some logic
  // from the InMemoryRowTransaction.
  std::map<std::string, InMemoryColumnFamily> tmp_families;

  for (auto const& rule : request.rules()) {
    auto maybe_column_family = utilities_->FindColumnFamily(rule);
    if (!maybe_column_family) {
      return maybe_column_family.status();
    }

    if (!rule.has_increment_amount() && !rule.has_append_value()) {
      return InvalidArgumentError(
          "either append value or increment amount must be set",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }

    auto const& column_family = maybe_column_family.value();
    std::string partial_key =
        KeyCoder::PartialEncode(request.row_key(), rule.column_qualifier());

    // 1. Lock the key range
    rocksdb::Endpoint start(partial_key, false);
    rocksdb::Endpoint end(partial_key + kRangeEnd, false);
    rocksdb::Status status =
        txn_->GetRangeLock(column_family->GetRaw(), start, end);
    if (!status.ok()) {
      return InternalError(
          "Failed to read modify row: " + status.ToString(),
          GCP_ERROR_INFO().WithMetadata("row key prefix", partial_key));
    }

    // 2. Acquire iterator to the first value in range (if exists)
    auto cf_it = std::unique_ptr<rocksdb::Iterator>(
        txn_->GetIterator(rocksdb::ReadOptions(), column_family->GetRaw()));
    cf_it->Seek(partial_key);

    // 3. Main logic
    uint64_t system_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count() *
        1000;
    if (!cf_it->Valid()) {
      std::string value;
      if (rule.has_append_value()) {
        value = rule.append_value();
      } else {
        value =
            google::cloud::internal::EncodeBigEndian(rule.increment_amount());
      }
      txn_->Put(column_family->GetRaw(),
                KeyCoder::Encode(request.row_key(), rule.column_qualifier(),
                                 system_ms),
                value);

      auto result_timestamp =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::microseconds(system_ms));
      tmp_families[rule.family_name()].SetCell(
          request.row_key(), rule.column_qualifier(), result_timestamp,
          std::move(value));
      continue;
    }

    auto maybe_decoded = KeyCoder::Decode(
        std::string_view(cf_it->key().data(), cf_it->key().size()));
    if (!maybe_decoded) {
      return InvalidArgumentError(
          "either append value or increment amount must be set",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }
    auto const& decoded_key = maybe_decoded.value();
    if (decoded_key.row != request.row_key() ||
        decoded_key.col != rule.column_qualifier()) {
      auto result_timestamp =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::microseconds(system_ms));
      tmp_families[rule.family_name()].SetCell(
          request.row_key(), rule.column_qualifier(), result_timestamp,
          rule.append_value());
      txn_->Put(column_family->GetRaw(),
                KeyCoder::Encode(request.row_key(), rule.column_qualifier(),
                                 system_ms),
                rule.append_value());
      continue;
    }

    std::string prev_value;
    status = txn_->Get(rocksdb::ReadOptions(), column_family->GetRaw(),
                       cf_it->key(), &prev_value);
    if (!status.ok()) {
      return InternalError(
          "Failed to read modify row: " + status.ToString(),
          GCP_ERROR_INFO().WithMetadata("row key prefix", partial_key));
    }

    std::string value;
    if (rule.has_append_value()) {
      std::cout << "Procesing append rule with append value: "
                << rule.append_value() << '\n';
      value = prev_value + rule.append_value();
    } else {  // has increment value
      std::cout << "Procesing increment rule with increment amount: "
                << rule.increment_amount() << '\n';
      auto maybe_prev_value_int =
          google::cloud::internal::DecodeBigEndian<std::int64_t>(prev_value);
      if (!maybe_prev_value_int) {
        return maybe_column_family.status();
      }
      value = google::cloud::internal::EncodeBigEndian(
          rule.increment_amount() + maybe_prev_value_int.value());
    }

    auto result_timestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds(system_ms));
    if (decoded_key.timestamp > system_ms) {
      result_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::microseconds(decoded_key.timestamp));
      status = txn_->Delete(column_family->GetRaw(), cf_it->key());
      if (!status.ok()) {
        return InternalError(
            "Failed to read modify row: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key prefix", partial_key));
      }
      status = txn_->Put(column_family->GetRaw(), cf_it->key(), value);
      if (!status.ok()) {
        return InternalError(
            "Failed to read modify row: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key prefix", partial_key));
      }
    } else {
      std::string new_key = KeyCoder::Encode(
          request.row_key(), rule.column_qualifier(), system_ms);
      status = txn_->Put(column_family->GetRaw(), new_key, value);
      if (!status.ok()) {
        return InternalError(
            "Failed to read modify row: " + status.ToString(),
            GCP_ERROR_INFO().WithMetadata("row key prefix", partial_key));
      }
    }

    tmp_families[rule.family_name()].SetCell(
        request.row_key(), rule.column_qualifier(), result_timestamp,
        std::move(value));
  }

  return FamiliesToReadModifyWriteResponse(row_key_, tmp_families);
}

google::bigtable::v2::ReadModifyWriteRowResponse
FamiliesToReadModifyWriteResponse(
    std::string const& row_key,
    std::map<std::string, InMemoryColumnFamily> const& families) {
  google::bigtable::v2::ReadModifyWriteRowResponse resp;
  auto* row = resp.mutable_row();
  row->set_key(row_key);

  for (auto const& fam : families) {
    auto* family = row->add_families();
    family->set_name(fam.first);
    for (auto const& r : fam.second) {
      for (auto const& cfr : r.second) {
        auto* col = family->add_columns();
        col->set_qualifier(cfr.first);
        for (auto const& cr : cfr.second) {
          auto* cell = col->add_cells();
          cell->set_timestamp_micros(
              std::chrono::duration_cast<std::chrono::microseconds>(cr.first)
                  .count());
          cell->set_value(std::move(cr.second));
        }
      }
    }
  }

  return resp;
}

template <typename MESSAGE>
StatusOr<std::shared_ptr<InMemoryColumnFamily>>
InMemoryTableOperations::FindColumnFamily(MESSAGE const& message) const {
  auto column_family_it = column_families_.find(message.family_name());
  if (column_family_it == column_families_.end()) {
    return NotFoundError(
        "No such column family.",
        GCP_ERROR_INFO().WithMetadata("mutation", message.DebugString()));
  }
  return column_family_it->second;
}

template <typename MESSAGE>
StatusOr<std::shared_ptr<PersistentColumnFamily>>
PersistentTableOperations::FindColumnFamily(MESSAGE const& message) const {
  auto column_family_it = column_families_.find(message.family_name());
  if (column_family_it == column_families_.end()) {
    return NotFoundError(
        "No such column family.",
        GCP_ERROR_INFO().WithMetadata("mutation", message.DebugString()));
  }
  return column_family_it->second;
}

StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
InMemoryRowTransaction::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request) {
  if (row_key_.empty()) {
    return InvalidArgumentError(
        "row key not set",
        GCP_ERROR_INFO().WithMetadata("request", request.DebugString()));
  }

  // tmp_families is a small one row mini table used to accumulate
  // changed cells efficiently for later return in the row returned by
  // the RPC.
  std::map<std::string, InMemoryColumnFamily> tmp_families;

  for (auto const& rule : request.rules()) {
    auto maybe_column_family = utilities_->FindColumnFamily(rule);
    if (!maybe_column_family) {
      return maybe_column_family.status();
    }

    auto column_family = maybe_column_family.value();

    if (rule.has_append_value()) {
      auto result = column_family->ReadModifyWrite(
          row_key_, rule.column_qualifier(), rule.append_value());

      ProcessReadModifyWriteResult(column_family, row_key_, undo_, rule, result,
                                   tmp_families);

    } else if (rule.has_increment_amount()) {
      auto maybe_result = column_family->ReadModifyWrite(
          row_key_, rule.column_qualifier(), rule.increment_amount());
      if (!maybe_result) {
        return maybe_result.status();
      }

      auto& result = maybe_result.value();

      ProcessReadModifyWriteResult(column_family, row_key_, undo_, rule, result,
                                   tmp_families);

    } else {
      return InvalidArgumentError(
          "either append value or increment amount must be set",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }
  }

  // Now assemble the returned value.
  return FamiliesToReadModifyWriteResponse(row_key_, tmp_families);
}

void InMemoryRowTransaction::Undo() {
  auto row_key = row_key_;

  while (!undo_.empty()) {
    auto op = undo_.top();
    undo_.pop();

    auto* restore_value = absl::get_if<RestoreValue>(&op);
    if (restore_value) {
      restore_value->column_family.SetCell(
          row_key, std::move(restore_value->column_qualifier),
          restore_value->timestamp, std::move(restore_value->value));
      continue;
    }

    auto* delete_value = absl::get_if<DeleteValue>(&op);
    if (delete_value) {
      delete_value->column_family.DeleteTimeStamp(
          row_key, std::move(delete_value->column_qualifier),
          delete_value->timestamp);
      continue;
    }

    // If we get here, there is a type of undo log that has not been
    // implemented!
    std::abort();
  }
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
