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
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_format.h"
#include "bigtable_limits.h"
#include "column_family.h"
#include "filter.h"
#include "google/protobuf/util/field_mask_util.h"
#include "range_set.h"
#include "row_streamer.h"
#include "table_operations.h"
#include "to_grpc_status.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/sync_stream.h>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

namespace btadmin = ::google::bigtable::admin::v2;

namespace {
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

/// Holds `mu` for a data operation, shared or exclusive.
class DataLock {
 public:
  DataLock(absl::Mutex& mu, bool shared) : mu_(mu), shared_(shared) {
    if (shared_) {
      mu_.ReaderLock();
    } else {
      mu_.Lock();
    }
  }

  DataLock(DataLock const&) = delete;
  DataLock& operator=(DataLock const&) = delete;

  ~DataLock() {
    if (shared_) {
      mu_.ReaderUnlock();
    } else {
      mu_.Unlock();
    }
  }

 private:
  absl::Mutex& mu_;
  bool shared_;
};
}  // anonymous namespace

Table::Table(std::string name, btadmin::Table schema,
             std::unique_ptr<TableOperations> operations)
    : name_(std::move(name)),
      concurrent_data_operations_(operations->AllowsConcurrentDataOperations()),
      operations_(std::move(operations)),
      schema_(std::make_shared<btadmin::Table const>(std::move(schema))) {}

std::shared_ptr<btadmin::Table const> Table::schema() const {
  // TODO(prawilny): this read races with PublishSchema(); make the pointer
  // swap atomic.
  return schema_;
}

Status Table::Close() {
  absl::WriterMutexLock lock(&mu_);
  if (!operations_) {
    return Status();
  }
  auto status = operations_->Close();
  operations_.reset();
  return status;
}

// TODO(prawilny): add helpers that take the lock, get the operations and mark
// the table unavailable, to remove the repeated status checks below.
StatusOr<DataOperations*> Table::data_operations() const {
  if (!operations_) {
    return NotFoundError("No such table",
                         GCP_ERROR_INFO().WithMetadata("table_name", name_));
  }
  if (!unavailable_.ok()) {
    return UnavailableError(
        "The table is unavailable: " + unavailable_.message(),
        GCP_ERROR_INFO().WithMetadata("table_name", name_));
  }
  auto status = operations_->BackgroundError();
  if (!status.ok()) {
    return UnavailableError("The table is unavailable: " + status.message(),
                            GCP_ERROR_INFO().WithMetadata("table_name", name_));
  }
  return operations_.get();
}

StatusOr<AdminOperations*> Table::admin_operations() {
  auto maybe_operations = data_operations();
  if (!maybe_operations) {
    return maybe_operations.status();
  }
  return operations_.get();
}

void Table::PublishSchema(btadmin::Table new_schema) {
  schema_ = std::make_shared<btadmin::Table const>(std::move(new_schema));
}

void Table::MarkUnavailable(Status reason) { unavailable_ = std::move(reason); }

Status PrepareTableSchema(btadmin::Table& schema) {
  if (schema.granularity() ==
      btadmin::Table::TIMESTAMP_GRANULARITY_UNSPECIFIED) {
    schema.set_granularity(btadmin::Table::MILLIS);
  }
  if (schema.granularity() != btadmin::Table::MILLIS) {
    return InvalidArgumentError(
        "Unsupported timestamp granularity.",
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }
  if (schema.cluster_states_size() > 0) {
    return InvalidArgumentError(
        "`cluster_states` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }
  if (schema.has_restore_info()) {
    return InvalidArgumentError(
        "`restore_info` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }
  if (schema.has_change_stream_config()) {
    return UnimplementedError(
        "`change_stream_config` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }
  if (schema.has_automated_backup_policy()) {
    return UnimplementedError(
        "`automated_backup_policy` not empty.",
        GCP_ERROR_INFO().WithMetadata("schema", schema.DebugString()));
  }
  return Status();
}

Status Table::SampleRowKeys(
    grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer)
    const {
  /**
   * For Persistent Table, CreateCellStream will create new RocksDB Iterator.
   * However, each iterator can work on a different version of data,
   * so for simplicity let's just acquire the exclusive lock (on the
   * assumption that SampleRowKeys won't be invoked often).
   */
  // TODO(prawilny): fix this.
  absl::WriterMutexLock lock(&mu_);

  auto maybe_operations = data_operations();
  if (!maybe_operations) {
    return maybe_operations.status();
  }
  auto maybe_samples = (*maybe_operations)->SampleRowKeys();
  if (!maybe_samples) {
    return maybe_samples.status();
  }
  auto& samples = *maybe_samples;

  for (std::size_t i = 0; i != samples.size(); ++i) {
    google::bigtable::v2::SampleRowKeysResponse resp;
    resp.set_row_key(std::move(samples[i].row_key));
    resp.set_offset_bytes(samples[i].offset_bytes);
    if (i + 1 == samples.size()) {
      auto opts = grpc::WriteOptions();
      opts.set_last_message();
      writer->WriteLast(std::move(resp), opts);
    } else {
      writer->Write(std::move(resp));
    }
  }

  return Status();
}

StatusOr<btadmin::Table> Table::ModifyColumnFamilies(
    btadmin::ModifyColumnFamiliesRequest const& request) {
  absl::WriterMutexLock lock(&mu_);

  auto maybe_operations = admin_operations();
  if (!maybe_operations) {
    return maybe_operations.status();
  }
  auto maybe_new_schema =
      (*maybe_operations)->ModifyColumnFamilies(request, *schema_);
  if (!maybe_new_schema) {
    if (maybe_new_schema.status().code() == StatusCode::kUnavailable) {
      MarkUnavailable(maybe_new_schema.status());
    }
    return maybe_new_schema.status();
  }
  PublishSchema(*maybe_new_schema);
  return *std::move(maybe_new_schema);
}

Status Table::Update(google::bigtable::admin::v2::Table const& new_schema,
                     google::protobuf::FieldMask const& to_update) {
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

  absl::WriterMutexLock lock(&mu_);
  auto maybe_operations = admin_operations();
  if (!maybe_operations) {
    return maybe_operations.status();
  }
  auto updated_schema = *schema_;
  FieldMaskUtil::MergeMessageTo(new_schema, to_update,
                                FieldMaskUtil::MergeOptions(), &updated_schema);
  auto s = (*maybe_operations)->UpdateTable(updated_schema);
  if (!s.ok()) {
    if (s.code() == StatusCode::kUnavailable) {
      MarkUnavailable(s);
    }
    return s;
  }
  PublishSchema(std::move(updated_schema));

  return Status();
}

Status Table::MutateRow(google::bigtable::v2::MutateRowRequest const& request,
                        Deadline deadline) {
  DataLock lock(mu_, concurrent_data_operations_);

  return DoMutationsWithPossibleRollback(request.row_key(), request.mutations(),
                                         deadline);
}

Status Table::MutateRows(
    google::bigtable::v2::MutateRowsRequest const& request,
    grpc::ServerWriter<google::bigtable::v2::MutateRowsResponse>* writer,
    Deadline deadline) {
  int64_t index = 0;
  google::bigtable::v2::MutateRowsResponse response;

  for (auto const& entry : request.entries()) {
    response.Clear();

    Status status;
    {
      DataLock lock(mu_, concurrent_data_operations_);
      status = DoMutationsWithPossibleRollback(entry.row_key(),
                                               entry.mutations(), deadline);
    }

    auto* response_entry = response.add_entries();
    response_entry->set_index(index++);
    auto* s = response_entry->mutable_status();
    *s = ToGoogleRPCStatus(status);

    if (index == request.entries_size()) {
      auto opts = grpc::WriteOptions();
      opts.set_last_message();
      writer->WriteLast(response, opts);
    } else {
      writer->Write(response);
    }
  }

  return Status();
}

namespace {

/// Applies `mutations` to `row_transaction` in order and stops at the first
/// failure. Does not commit.
Status ApplyMutations(
    RowTransaction& row_transaction,
    google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
        mutations) {
  for (auto const& mutation : mutations) {
    if (mutation.has_set_cell()) {
      auto const& set_cell = mutation.set_cell();

      absl::optional<std::chrono::microseconds> timestamp_override =
          absl::nullopt;

      if (set_cell.timestamp_micros() < -1) {
        return InvalidArgumentError(
            "Timestamp micros cannot be < -1.",
            GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
      }

      if (set_cell.timestamp_micros() != -1 &&
          set_cell.timestamp_micros() % 1000 != 0) {
        return InvalidArgumentError(
            "Timestamp micros must be a multiple of 1000 (millisecond "
            "granularity).",
            GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
      }

      if (set_cell.timestamp_micros() == -1) {
        timestamp_override.emplace(NowTruncatedToMillis());
      }
      auto status = row_transaction.SetCell(set_cell, timestamp_override);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_add_to_cell()) {
      auto const& add_to_cell = mutation.add_to_cell();

      absl::optional<std::chrono::microseconds> timestamp_override =
          absl::nullopt;

      std::chrono::microseconds timestamp = std::chrono::microseconds::zero();

      if (add_to_cell.has_timestamp() &&
          add_to_cell.timestamp().has_raw_timestamp_micros()) {
        if (add_to_cell.timestamp().raw_timestamp_micros() > 0 &&
            add_to_cell.timestamp().raw_timestamp_micros() % 1000 != 0) {
          return InvalidArgumentError(
              "Timestamp micros must be a multiple of 1000 (millisecond "
              "granularity).",
              GCP_ERROR_INFO().WithMetadata("mutation",
                                            mutation.DebugString()));
        }
        timestamp = std::chrono::microseconds(
            add_to_cell.timestamp().raw_timestamp_micros());
      }

      // If no valid timestamp is provided, override with the system time.
      if (timestamp <= std::chrono::microseconds::zero()) {
        timestamp = NowTruncatedToMillis();
        timestamp_override.emplace(std::move(timestamp));
      }

      auto status = row_transaction.AddToCell(add_to_cell, timestamp_override);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_merge_to_cell()) {
      return UnimplementedError(
          "Unsupported mutation type.",
          GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
    } else if (mutation.has_delete_from_column()) {
      auto const& delete_from_column = mutation.delete_from_column();
      auto const& time_range = delete_from_column.time_range();
      if (time_range.start_timestamp_micros() % 1000 != 0 ||
          time_range.end_timestamp_micros() % 1000 != 0) {
        return InvalidArgumentError(
            "Timestamp micros must be a multiple of 1000 (millisecond "
            "granularity).",
            GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
      }
      auto status = row_transaction.DeleteFromColumn(delete_from_column);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_delete_from_family()) {
      auto const& delete_from_family = mutation.delete_from_family();
      auto status = row_transaction.DeleteFromFamily(delete_from_family);
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_delete_from_row()) {
      auto status = row_transaction.DeleteFromRow();
      if (!status.ok()) {
        return status;
      }
    } else {
      return UnimplementedError(
          "Unsupported mutation type.",
          GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
    }
  }

  return Status();
}

}  // anonymous namespace

Status Table::DoMutationsWithPossibleRollback(
    std::string const& row_key,
    google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
        mutations,
    Deadline deadline) {
  if (row_key.size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata("row_key size",
                                      absl::StrFormat("%zu", row_key.size())));
  }

  auto maybe_operations = data_operations();
  if (!maybe_operations) {
    return maybe_operations.status();
  }
  auto maybe_row_transaction =
      (*maybe_operations)->NewRowTransaction(row_key, deadline);
  if (!maybe_row_transaction) {
    return maybe_row_transaction.status();
  }
  auto& row_transaction = *maybe_row_transaction;

  auto status = ApplyMutations(*row_transaction, mutations);
  if (!status.ok()) {
    return status;
  }

  // If we get here, all mutations on the row have succeeded. We can
  // commit and return which will prevent the destructor from undoing
  // the transaction.
  return row_transaction->Commit();
}

StatusOr<google::bigtable::v2::CheckAndMutateRowResponse>
Table::CheckAndMutateRow(
    google::bigtable::v2::CheckAndMutateRowRequest const& request,
    Deadline deadline) {
  DataLock lock(mu_, concurrent_data_operations_);

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

  auto maybe_operations = data_operations();
  if (!maybe_operations) {
    return maybe_operations.status();
  }
  auto maybe_row_transaction =
      (*maybe_operations)->NewRowTransaction(row_key, deadline);
  if (!maybe_row_transaction) {
    return maybe_row_transaction.status();
  }
  auto& row_transaction = *maybe_row_transaction;

  absl::optional<google::bigtable::v2::RowFilter> predicate;
  if (request.has_predicate_filter()) {
    predicate = request.predicate_filter();
  }
  auto maybe_a_cell_is_found = row_transaction->RowMatches(predicate);
  if (!maybe_a_cell_is_found) {
    return maybe_a_cell_is_found.status();
  }
  bool const a_cell_is_found = *maybe_a_cell_is_found;

  auto status = ApplyMutations(
      *row_transaction,
      a_cell_is_found ? request.true_mutations() : request.false_mutations());
  if (!status.ok()) {
    return status;
  }

  status = row_transaction->Commit();
  if (!status.ok()) {
    return status;
  }

  google::bigtable::v2::CheckAndMutateRowResponse success_response;
  success_response.set_predicate_matched(a_cell_is_found);

  return success_response;
}

Status Table::ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                       RowStreamer& row_streamer,
                       grpc::ServerContext const& /*context*/) const {
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
  DataLock lock(mu_, concurrent_data_operations_);
  auto maybe_operations = data_operations();
  if (!maybe_operations) {
    return maybe_operations.status();
  }
  StatusOr<CellStream> maybe_stream;
  if (request.has_filter()) {
    maybe_stream =
        (*maybe_operations)->CreateCellStream(row_set, request.filter());
  } else {
    maybe_stream =
        (*maybe_operations)->CreateCellStream(row_set, absl::nullopt);
  }

  if (!maybe_stream) {
    return maybe_stream.status();
  }

  std::int64_t rows_count = 0;
  absl::optional<std::string> current_row_key;
  CellStream& stream = *maybe_stream;
  for (; stream; ++stream) {
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
      return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
    }
  }

  if (!row_streamer.Flush(true)) {
    return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
  }
  return Status();
}

StatusOr<std::unique_ptr<TableOperations>> Table::Delete() {
  absl::WriterMutexLock lock(&mu_);
  if (!operations_) {
    return NotFoundError("No such table",
                         GCP_ERROR_INFO().WithMetadata("table_name", name_));
  }
  if (schema_->deletion_protection()) {
    return FailedPreconditionError(
        "The table has deletion protection.",
        GCP_ERROR_INFO().WithMetadata("table_name", name_));
  }
  auto status = operations_->CommitDeletion();
  if (!status.ok()) {
    if (status.code() == StatusCode::kUnavailable) {
      MarkUnavailable(status);
    }
    return status;
  }
  return std::move(operations_);
}

Status Table::DropRowRange(
    ::google::bigtable::admin::v2::DropRowRangeRequest const& request) {
  absl::WriterMutexLock lock(&mu_);
  auto maybe_operations = admin_operations();
  if (!maybe_operations) {
    return maybe_operations.status();
  }

  if (!request.has_row_key_prefix() &&
      !request.has_delete_all_data_from_table()) {
    return InvalidArgumentError(
        "Neither row prefix nor deleted all data from table is set",
        GCP_ERROR_INFO().WithMetadata("DropRowRange request",
                                      request.DebugString()));
  }

  if (request.has_delete_all_data_from_table()) {
    Status status = (*maybe_operations)->RemoveAllDataFromColumnFamilies();
    if (!status.ok()) {
      if (status.code() == StatusCode::kUnavailable) {
        MarkUnavailable(status);
      }
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

  Status status = (*maybe_operations)->DropRowRange(row_key_prefix);
  if (!status.ok()) {
    if (status.code() == StatusCode::kUnavailable) {
      MarkUnavailable(status);
    }
    return status;
  }
  return Status();
}

StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
Table::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request,
    Deadline deadline) {
  if (request.row_key().size() > kMaxRowLen) {
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata(
            "row_key size", absl::StrFormat("%zu", request.row_key().size())));
  }

  DataLock lock(mu_, concurrent_data_operations_);

  auto maybe_operations = data_operations();
  if (!maybe_operations) {
    return maybe_operations.status();
  }
  auto maybe_row_transaction =
      (*maybe_operations)->NewRowTransaction(request.row_key(), deadline);
  if (!maybe_row_transaction) {
    return maybe_row_transaction.status();
  }
  auto& row_transaction = *maybe_row_transaction;

  auto maybe_response = row_transaction->ReadModifyWriteRow(request);
  if (!maybe_response) {
    return maybe_response.status();
  }

  row_transaction->Commit();

  return std::move(maybe_response.value());
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
