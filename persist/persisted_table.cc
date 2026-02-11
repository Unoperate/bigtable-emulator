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

#include "persist/persisted_table.h"
#include "google/cloud/internal/make_status.h"
#include "persist/utils/logging.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include <cmath>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

StatusOr<StringRangeSet> CreateStringRangeSet(
    google::bigtable::v2::RowSet const& row_set) {
  StringRangeSet res;
  for (auto const& row_key : row_set.row_keys()) {
    if (row_key.size() > kMaxRowLen) {
      LERROR(
          "[CreateStringRangeSet] row_key longer than 4KiB row_key_size={} "
          "max={}",
          row_key.size(), kMaxRowLen);
      return InvalidArgumentError(
          "The row_key in row_set is longer than 4KiB",
          GCP_ERROR_INFO()
              .WithMetadata("row_key size",
                            absl::StrFormat("%zu", row_key.size()))
              .WithMetadata("row_set", row_set.DebugString()));
    }

    if (row_key.empty()) {
      LERROR("[CreateStringRangeSet] row_key empty");
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

PersistedTable::PersistedTable(std::string const& name,
                               std::shared_ptr<Storage> storage)
    : name_(name), storage_(storage) {}

absl::optional<google::bigtable::admin::v2::Type>
PersistedTable::GetColumnFamilyType(std::string const& family_name) {
  auto& cfs =
      storage_->GetTable(name_).value().mutable_table()->column_families();
  auto const& it = cfs.find(family_name);
  if (it == cfs.end()) {
    return absl::nullopt;
  }
  return it->second.value_type();
}

Status PersistedTable::MutateRow(
    google::bigtable::v2::MutateRowRequest const& request) {
  DBG("[PersistedTable][MutateRow] table={} row_key={} mutations_count={}",
      name_, request.row_key(), request.mutations_size());
  return DoMutationsWithPossibleRollback(request.row_key(),
                                         request.mutations());
}

Status PersistedTable::ReadRows(
    google::bigtable::v2::ReadRowsRequest const& request,
    RowStreamer& row_streamer) const {
  DBG("[PersistedTable][ReadRows] table={} rows_limit={} has_filter={}",
      name_, request.rows_limit(), request.has_filter());
  std::shared_ptr<StringRangeSet> row_set;
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

  StatusOr<CellStream> maybe_stream;
  if (request.has_filter()) {
    maybe_stream = CreateCellStream(row_set, std::move(request.filter()));
  } else {
    maybe_stream = CreateCellStream(row_set, absl::nullopt);
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
      LERROR("[PersistedTable][ReadRows] Stream closed by client table={}",
             name_);
      return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
    }
  }

  if (!row_streamer.Flush(true)) {
    LERROR("[PersistedTable][ReadRows] Flush failed stream closed table={}",
           name_);
    return AbortedError("Stream closed by the client.", GCP_ERROR_INFO());
  }
  DBG("[PersistedTable][ReadRows] exit table={} rows_streamed={}", name_,
      rows_count);
  return Status();
}

Status PersistedTable::SampleRowKeys(
    double pass_probability,
    grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer) {
  DBG("[PersistedTable][SampleRowKeys] table={} pass_probability={}", name_,
      pass_probability);
  if (pass_probability <= 0.0) {
    LERROR(
        "[PersistedTable][SampleRowKeys] sampling probability must be "
        "positive table={} pass_probability={}",
        name_, pass_probability);
    return InvalidArgumentError(
        "The sampling probabality must be positive",
        GCP_ERROR_INFO().WithMetadata("provided sampling probability",
                                      absl::StrFormat("%f", pass_probability)));
  }

  auto sample_every =
      static_cast<std::uint64_t>(std::ceil(1.0 / pass_probability));

  auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
  auto maybe_all_rows_stream = CreateCellStream(all_rows_set, absl::nullopt);
  if (!maybe_all_rows_stream) {
    return maybe_all_rows_stream.status();
  }

  auto& stream = *maybe_all_rows_stream;

  absl::optional<std::string> first_row_key;
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
    google::bigtable::v2::SampleRowKeysResponse resp;
    resp.set_row_key("");
    resp.set_offset_bytes(0);
    auto opts = grpc::WriteOptions();
    opts.set_last_message();
    writer->WriteLast(std::move(resp), opts);
    DBG("[PersistedTable][SampleRowKeys] empty table exit table={}", name_);
    return Status();
  }

  std::int64_t offset_delta = sample_every * row_size_estimate;

  google::bigtable::v2::RowFilter sample_filter;
  sample_filter.set_row_sample_filter(pass_probability);

  auto maybe_stream = CreateCellStream(all_rows_set, sample_filter);
  if (!maybe_stream) {
    return maybe_stream.status();
  }

  auto& sampled_stream = *maybe_stream;
  std::int64_t offset = 0;
  bool wrote_a_sample = false;

  for (; sampled_stream; sampled_stream.Next(NextMode::kRow)) {
    google::bigtable::v2::SampleRowKeysResponse resp;
    offset += offset_delta;
    resp.set_row_key(sampled_stream->row_key());
    resp.set_offset_bytes(offset);
    writer->Write(std::move(resp));
    wrote_a_sample = true;
  }

  if (!wrote_a_sample) {
    google::bigtable::v2::SampleRowKeysResponse resp;
    resp.set_row_key(first_row_key.value() + "\\x00");
    resp.set_offset_bytes(row_size_estimate);
    auto opts = grpc::WriteOptions();
    opts.set_last_message();
    writer->WriteLast(std::move(resp), opts);
  }

  DBG("[PersistedTable][SampleRowKeys] exit table={} wrote_samples={}",
      name_, wrote_a_sample);
  return Status();
}

Status PersistedTable::DoMutationsWithPossibleRollback(
    std::string const& row_key,
    google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
        mutations) {
  DBG("[PersistedTable][DoMutationsWithPossibleRollback] table={} row_key={} "
      "row_key_size={} mutations_count={}",
      name_, row_key, row_key.size(), mutations.size());
  if (row_key.size() > kMaxRowLen) {
    LERROR(
        "[PersistedTable][DoMutationsWithPossibleRollback] row_key longer "
        "than 4KiB table={} row_key_size={} max={}",
        name_, row_key.size(), kMaxRowLen);
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata("row_key size",
                                      absl::StrFormat("%zu", row_key.size())));
  }

  auto maybeTxn = storage_->RowTransaction(name_, row_key);
  if (!maybeTxn.ok()) {
    LERROR("[PersistedTable][ReadModifyWriteRow] Failed to create row transaction status={}", maybeTxn.status());
    return InternalError(
      "failed to create row transaction with lock",
      GCP_ERROR_INFO().WithMetadata("row_key", row_key));
  }
  auto const& txn = maybeTxn.value();

  for (auto const& mutation : mutations) {
    if (mutation.has_set_cell()) {
      auto const& set_cell = mutation.set_cell();

      absl::optional<std::chrono::milliseconds> timestamp_override =
          absl::nullopt;

      if (set_cell.timestamp_micros() < -1) {
        LERROR(
            "[PersistedTable][DoMutationsWithPossibleRollback] timestamp "
            "micros < -1 table={} row_key={} ts_micros={}",
            name_, row_key, set_cell.timestamp_micros());
        return InvalidArgumentError(
            "Timestamp micros cannot be < -1.",
            GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
      }

      if (set_cell.timestamp_micros() == -1) {
        timestamp_override.emplace(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()));
      }

      auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::microseconds(set_cell.timestamp_micros()));

      if (timestamp_override.has_value()) {
        timestamp = timestamp_override.value();
      }

      auto status =
          txn->SetCell(set_cell.family_name(), set_cell.column_qualifier(),
                       timestamp, set_cell.value());
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

      if (timestamp <= std::chrono::milliseconds::zero()) {
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
        timestamp_override.emplace(std::move(timestamp));
      }

      auto cf_value_type = GetColumnFamilyType(add_to_cell.family_name());
      if (!cf_value_type.has_value() ||
          !cf_value_type.value().has_aggregate_type()) {
        LERROR(
            "[PersistedTable][DoMutationsWithPossibleRollback] column family "
            "not configured for aggregation table={} row_key={} cf={}",
            name_, row_key, add_to_cell.family_name());
        return InvalidArgumentError(
            "column family is not configured to contain aggregation cells or "
            "aggregation type not properly configured",
            GCP_ERROR_INFO().WithMetadata("column family",
                                          add_to_cell.family_name()));
      }

      switch (cf_value_type.value().aggregate_type().aggregator_case()) {
        case google::bigtable::admin::v2::Type::Aggregate::kSum:
        case google::bigtable::admin::v2::Type::Aggregate::kMin:
        case google::bigtable::admin::v2::Type::Aggregate::kMax:
          break;
        default:
          LERROR(
              "[PersistedTable][DoMutationsWithPossibleRollback] unimplemented "
              "aggregation table={} row_key={} cf={} aggregator_case={}",
              name_, row_key, add_to_cell.family_name(),
              static_cast<int>(cf_value_type.value()
                                    .aggregate_type()
                                    .aggregator_case()));
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
        LERROR(
            "[PersistedTable][DoMutationsWithPossibleRollback] input not set "
            "table={} row_key={} cf={}",
            name_, row_key, add_to_cell.family_name());
        return InvalidArgumentError("input not set",
                                    GCP_ERROR_INFO().WithMetadata(
                                        "mutation", add_to_cell.DebugString()));
      }

      switch (add_to_cell.input().kind_case()) {
        case google::bigtable::v2::Value::kIntValue:
          if (!add_to_cell.input().has_int_value()) {
            LERROR(
                "[PersistedTable][DoMutationsWithPossibleRollback] input value "
                "not set table={} row_key={} cf={}",
                name_, row_key, add_to_cell.family_name());
            return InvalidArgumentError(
                "input value not set",
                GCP_ERROR_INFO().WithMetadata("mutation",
                                              add_to_cell.DebugString()));
          }
          break;
        default:
          LERROR(
              "[PersistedTable][DoMutationsWithPossibleRollback] only int64 "
              "supported table={} row_key={} cf={} kind_case={}",
              name_, row_key, add_to_cell.family_name(),
              add_to_cell.input().kind_case());
          return InvalidArgumentError(
              "only int64 values are supported",
              GCP_ERROR_INFO().WithMetadata("mutation",
                                            add_to_cell.DebugString()));
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

      if (!add_to_cell.has_column_qualifier() ||
          !add_to_cell.column_qualifier().has_raw_value()) {
        LERROR(
            "[PersistedTable][DoMutationsWithPossibleRollback] column "
            "qualifier not set table={} row_key={} cf={}",
            name_, row_key, add_to_cell.family_name());
        return InvalidArgumentError("column qualifier not set",
                                    GCP_ERROR_INFO().WithMetadata(
                                        "mutation", add_to_cell.DebugString()));
      }
      auto column_qualifier = add_to_cell.column_qualifier().raw_value();

      auto maybe_old_value = txn->UpdateCell(
          add_to_cell.family_name(), column_qualifier, ts_ms, value,
          [](std::string const& /*existing_value*/, std::string&& new_value)
              -> StatusOr<std::string> { return new_value; });

      if (!maybe_old_value) {
        return maybe_old_value.status();
      }

      return Status();
    } else if (mutation.has_merge_to_cell()) {
      LERROR(
          "[PersistedTable][DoMutationsWithPossibleRollback] unsupported "
          "mutation merge_to_cell table={} row_key={}",
          name_, row_key);
      return UnimplementedError(
          "Unsupported mutation type.",
          GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
    } else if (mutation.has_delete_from_column()) {
      auto const& delete_from_column = mutation.delete_from_column();

      if (delete_from_column.has_time_range()) {
        auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds(
                delete_from_column.time_range().start_timestamp_micros()));
        auto end = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds(
                delete_from_column.time_range().end_timestamp_micros()));

        if (end <= start &&
            delete_from_column.time_range().end_timestamp_micros() != 0) {
          LERROR(
              "[PersistedTable][DoMutationsWithPossibleRollback] empty or "
              "reversed time range table={} row_key={} start_micros={} "
              "end_micros={}",
              name_, row_key,
              delete_from_column.time_range().start_timestamp_micros(),
              delete_from_column.time_range().end_timestamp_micros());
          return InvalidArgumentError(
              "empty or reversed time range: the end timestamp must be more "
              "than "
              "the start timestamp when they are truncated to the server's "
              "time "
              "precision (milliseconds)",
              GCP_ERROR_INFO().WithMetadata("delete_from_column proto",
                                            delete_from_column.DebugString()));
        }
      }

      auto status = txn->DeleteRowColumn(delete_from_column.family_name(),
                                         delete_from_column.column_qualifier(),
                                         delete_from_column.time_range());
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_delete_from_family()) {
      auto const& delete_from_family = mutation.delete_from_family();
      auto status =
          txn->DeleteRowFromColumnFamily(delete_from_family.family_name());
      if (!status.ok()) {
        return status;
      }
    } else if (mutation.has_delete_from_row()) {
      auto status = txn->DeleteRowFromAllColumnFamilies();
      if (!status.ok()) {
        return status;
      }
    } else {
      LERROR(
          "[PersistedTable][DoMutationsWithPossibleRollback] unsupported "
          "mutation type table={} row_key={}",
          name_, row_key);
      return UnimplementedError(
          "Unsupported mutation type.",
          GCP_ERROR_INFO().WithMetadata("mutation", mutation.DebugString()));
    }
  }

  DBG("[PersistedTable][DoMutationsWithPossibleRollback] committing "
      "table={} row_key={}",
      name_, row_key);
  return txn->Commit();
}

StatusOr<google::bigtable::v2::ReadModifyWriteRowResponse>
PersistedTable::ReadModifyWriteRow(
    google::bigtable::v2::ReadModifyWriteRowRequest const& request) {
  DBG("[PersistedTable][ReadModifyWriteRow] table={} row_key={} rules_count={}",
      name_, request.row_key(), request.rules_size());
  if (request.row_key().size() > kMaxRowLen) {
    LERROR(
        "[PersistedTable][ReadModifyWriteRow] row_key longer than 4KiB "
        "table={} row_key_size={} max={}",
        name_, request.row_key().size(), kMaxRowLen);
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata(
            "row_key size", absl::StrFormat("%zu", request.row_key().size())));
  }

  if (request.row_key().empty()) {
    LERROR("[PersistedTable][ReadModifyWriteRow] row key not set table={}",
           name_);
    return InvalidArgumentError(
        "row key not set",
        GCP_ERROR_INFO().WithMetadata("request", request.DebugString()));
  }

  auto maybeTxn = storage_->RowTransaction(name_, request.row_key());
  if (!maybeTxn.ok()) {
    LERROR("[PersistedTable][ReadModifyWriteRow] Failed to create row transaction status={}", maybeTxn.status());
    return InternalError(
      "failed to create row transaction with lock",
      GCP_ERROR_INFO().WithMetadata("request", request.DebugString()));
  }
  auto const& txn = maybeTxn.value();

  google::bigtable::v2::ReadModifyWriteRowResponse resp;
  auto* response_row = resp.mutable_row();
  response_row->set_key(request.row_key());

  for (auto const& rule : request.rules()) {
    auto cf_type = GetColumnFamilyType(rule.family_name());
    if (!cf_type.has_value()) {
      LERROR(
          "[PersistedTable][ReadModifyWriteRow] column family not found "
          "table={} row_key={} family_name={}",
          name_, request.row_key(), rule.family_name());
      return InvalidArgumentError(
          "Column family not found",
          GCP_ERROR_INFO().WithMetadata("family_name", rule.family_name()));
    }

    std::chrono::milliseconds timestamp;
    std::string new_value;

    if (rule.has_append_value()) {
      timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());

      std::string append_str = rule.append_value();
      auto maybe_old_value = txn->UpdateCell(
          rule.family_name(), rule.column_qualifier(), timestamp, append_str,
          [](std::string const& old_value, std::string&& append_value)
              -> StatusOr<std::string> { return old_value + append_value; });

      if (!maybe_old_value.ok()) {
        return maybe_old_value.status();
      }

      if (maybe_old_value.value().has_value()) {
        new_value = maybe_old_value.value().value() + append_str;
      } else {
        new_value = append_str;
      }

    } else if (rule.has_increment_amount()) {
      timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());

      std::int64_t increment = rule.increment_amount();
      std::string dummy_value;

      auto maybe_old_value = txn->UpdateCell(
          rule.family_name(), rule.column_qualifier(), timestamp, dummy_value,
          [increment](std::string const& old_value,
                      std::string&&) -> StatusOr<std::string> {
            if (old_value.empty()) {
              return google::cloud::internal::EncodeBigEndian(increment);
            }

            auto maybe_old_int =
                google::cloud::internal::DecodeBigEndian<std::int64_t>(
                    old_value);
            if (!maybe_old_int.ok()) {
              return maybe_old_int.status();
            }

            std::int64_t result = maybe_old_int.value() + increment;
            return google::cloud::internal::EncodeBigEndian(result);
          });

      if (!maybe_old_value.ok()) {
        return maybe_old_value.status();
      }

      std::int64_t result_int = increment;
      if (maybe_old_value.value().has_value()) {
        auto maybe_old_int =
            google::cloud::internal::DecodeBigEndian<std::int64_t>(
                maybe_old_value.value().value());
        if (maybe_old_int.ok()) {
          result_int = maybe_old_int.value() + increment;
        }
      }

      new_value = google::cloud::internal::EncodeBigEndian(result_int);

    } else {
      LERROR(
          "[PersistedTable][ReadModifyWriteRow] rule must have append_value or "
          "increment_amount table={} row_key={} family_name={}",
          name_, request.row_key(), rule.family_name());
      return InvalidArgumentError(
          "either append value or increment amount must be set",
          GCP_ERROR_INFO().WithMetadata("rule", rule.DebugString()));
    }

    google::bigtable::v2::Family* family = nullptr;
    for (auto& fam : *response_row->mutable_families()) {
      if (fam.name() == rule.family_name()) {
        family = &fam;
        break;
      }
    }
    if (family == nullptr) {
      family = response_row->add_families();
      family->set_name(rule.family_name());
    }

    auto* column = family->add_columns();
    column->set_qualifier(rule.column_qualifier());

    auto* cell = column->add_cells();
    cell->set_timestamp_micros(
        std::chrono::duration_cast<std::chrono::microseconds>(timestamp)
            .count());
    cell->set_value(std::move(new_value));
  }

  auto commit_status = txn->Commit();
  if (!commit_status.ok()) {
    return commit_status;
  }

  DBG("[PersistedTable][ReadModifyWriteRow] exit table={} row_key={}", name_,
      request.row_key());
  return resp;
}

StatusOr<google::bigtable::v2::CheckAndMutateRowResponse>
PersistedTable::CheckAndMutateRow(
    google::bigtable::v2::CheckAndMutateRowRequest const& request) {
  DBG("[PersistedTable][CheckAndMutateRow] table={} row_key={} "
      "true_mutations={} false_mutations={}",
      name_, request.row_key(), request.true_mutations_size(),
      request.false_mutations_size());
  auto const& row_key = request.row_key();

  if (row_key.size() > kMaxRowLen) {
    LERROR(
        "[PersistedTable][CheckAndMutateRow] row_key longer than 4KiB "
        "table={} row_key_size={} max={}",
        name_, row_key.size(), kMaxRowLen);
    return InvalidArgumentError(
        "The row_key is longer than 4KiB",
        GCP_ERROR_INFO()
            .WithMetadata("row_key size",
                          absl::StrFormat("%zu", row_key.size()))
            .WithMetadata("CheckAndMutateRequest", request.DebugString()));
  }

  if (row_key.empty()) {
    LERROR("[PersistedTable][CheckAndMutateRow] row key required table={}",
           name_);
    return InvalidArgumentError(
        "row key required",
        GCP_ERROR_INFO().WithMetadata("CheckAndMutateRowRequest",
                                      request.DebugString()));
  }

  if (request.true_mutations_size() == 0 &&
      request.false_mutations_size() == 0) {
    LERROR(
        "[PersistedTable][CheckAndMutateRow] both mutations empty table={} "
        "row_key={}",
        name_, row_key);
    return InvalidArgumentError(
        "both true mutations and false mutations are empty",
        GCP_ERROR_INFO().WithMetadata("CheckAndMutateRowRequest",
                                      request.DebugString()));
  }

  auto range_set = std::make_shared<StringRangeSet>();

  range_set->Sum(StringRangeSet::Range(row_key, false, row_key, false));

  StatusOr<CellStream> maybe_stream;
  if (request.has_predicate_filter()) {
    maybe_stream =
        CreateCellStream(range_set, std::move(request.predicate_filter()));
  } else {
    maybe_stream = CreateCellStream(range_set, absl::nullopt);
  }

  if (!maybe_stream) {
    return maybe_stream.status();
  }

  bool a_cell_is_found = false;

  CellStream& stream = *maybe_stream;
  if (stream) {
    a_cell_is_found = true;
    DBG("[PersistedTable][CheckAndMutateRow] predicate matched table={} "
        "row_key={}",
        name_, row_key);
  } else {
    DBG("[PersistedTable][CheckAndMutateRow] predicate not matched table={} "
        "row_key={}",
        name_, row_key);
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

  DBG("[PersistedTable][CheckAndMutateRow] exit table={} row_key={} "
      "predicate_matched={}",
      name_, row_key, a_cell_is_found);
  return success_response;
}

StatusOr<CellStream> PersistedTable::CreateCellStream(
    std::shared_ptr<StringRangeSet> range_set,
    absl::optional<google::bigtable::v2::RowFilter> maybe_row_filter) const {
  

  auto maybeStream = storage_->StreamTable(name_, range_set, false);
  if (!maybeStream.ok()) {
    LERROR(
      "[PersistedTable][CreateCellStream] Failes to create stream for table name={}",
      name_);
    return maybeStream.status();
  }

  auto table_stream_ctor = [&maybeStream, this] {
    return std::move(maybeStream.value());
  };

  if (maybe_row_filter.has_value()) {
    return CreateFilter(maybe_row_filter.value(), table_stream_ctor);
  }

  return table_stream_ctor();
}

Status PersistedTable::Update(
    google::bigtable::admin::v2::Table const& new_schema,
    google::protobuf::FieldMask const& to_update) {
  DBG("[PersistedTable][Update] table={} mask_paths={}", name_,
      to_update.paths_size());
  using google::protobuf::util::FieldMaskUtil;
  google::protobuf::FieldMask allowed_mask;
  FieldMaskUtil::FromString(
      "change_stream_config,"
      "change_stream_config.retention_period,"
      "deletion_protection",
      &allowed_mask);

  if (!FieldMaskUtil::IsValidFieldMask<google::bigtable::admin::v2::Table>(
          to_update)) {
    LERROR(
        "[PersistedTable][Update] Update mask invalid table={} mask_paths={}",
        name_, to_update.paths_size());
    return InvalidArgumentError(
        "Update mask is invalid.",
        GCP_ERROR_INFO().WithMetadata("mask", to_update.DebugString()));
  }

  google::protobuf::FieldMask disallowed_mask;
  FieldMaskUtil::Subtract<google::bigtable::admin::v2::Table>(
      to_update, allowed_mask, &disallowed_mask);

  if (disallowed_mask.paths_size() > 0) {
    LERROR(
        "[PersistedTable][Update] Update mask contains disallowed fields "
        "table={} disallowed_paths={}",
        name_, disallowed_mask.paths_size());
    return UnimplementedError(
        "Update mask contains disallowed fields.",
        GCP_ERROR_INFO().WithMetadata("mask", disallowed_mask.DebugString()));
  }

  auto maybe_meta = storage_->GetTable(name_);
  if (!maybe_meta.ok()) {
    return maybe_meta.status();
  }

  auto updated_schema = maybe_meta.value().table();
  FieldMaskUtil::MergeMessageTo(new_schema, to_update,
                                FieldMaskUtil::MergeOptions(), &updated_schema);

  storage::TableMeta new_meta;
  *new_meta.mutable_table() = updated_schema;

  auto status = storage_->UpdateTableMetadata(name_, new_meta);
  if (!status.ok()) {
    return status;
  }

  DBG("[PersistedTable][Update] exit table={}", name_);
  return Status();
}

StatusOr<google::bigtable::admin::v2::Table>
PersistedTable::ModifyColumnFamilies(
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request) {
  DBG("[PersistedTable][ModifyColumnFamilies] table={} modifications_count={}",
      name_, request.modifications_size());
  auto maybe_meta = storage_->GetTable(name_);
  if (!maybe_meta.ok()) {
    return maybe_meta.status();
  }

  auto new_schema = maybe_meta.value().table();

  std::vector<std::string> new_cf_names;

  for (auto const& modification : request.modifications()) {
    if (modification.drop()) {
      if (new_schema.deletion_protection()) {
        LERROR(
            "[PersistedTable][ModifyColumnFamilies] table has deletion "
            "protection table={} cf_id={}",
            name_, modification.id());
        return FailedPreconditionError(
            "The table has deletion protection.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString()));
      }

      if (new_schema.mutable_column_families()->erase(modification.id()) == 0) {
        LERROR(
            "[PersistedTable][ModifyColumnFamilies] no such column family "
            "(drop) table={} cf_id={}",
            name_, modification.id());
        return NotFoundError("No such column family.",
                             GCP_ERROR_INFO().WithMetadata(
                                 "modification", modification.DebugString()));
      }

      auto drop_status = storage_->DeleteColumnFamily(modification.id());
      if (!drop_status.ok()) {
        return drop_status;
      }

    } else if (modification.has_update()) {
      auto& cfs = *new_schema.mutable_column_families();
      auto cf_it = cfs.find(modification.id());
      if (cf_it == cfs.end()) {
        LERROR(
            "[PersistedTable][ModifyColumnFamilies] no such column family "
            "(update) table={} cf_id={}",
            name_, modification.id());
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
          LERROR(
              "[PersistedTable][ModifyColumnFamilies] update mask invalid "
              "table={} cf_id={}",
              name_, modification.id());
          return InvalidArgumentError(
              "Update mask is invalid.",
              GCP_ERROR_INFO().WithMetadata("modification",
                                            modification.DebugString()));
        }
      } else {
        FieldMaskUtil::FromString("gc_rule", &effective_mask);
        if (!FieldMaskUtil::IsValidFieldMask<
                google::bigtable::admin::v2::ColumnFamily>(effective_mask)) {
          LERROR(
              "[PersistedTable][ModifyColumnFamilies] default update mask "
              "invalid table={} cf_id={}",
              name_, modification.id());
          return InternalError("Default update mask is invalid.",
                               GCP_ERROR_INFO().WithMetadata(
                                   "mask", effective_mask.DebugString()));
        }
      }

      if (FieldMaskUtil::IsPathInFieldMask("value_type", effective_mask)) {
        LERROR(
            "[PersistedTable][ModifyColumnFamilies] value_type cannot be "
            "changed table={} cf_id={}",
            name_, modification.id());
        return InvalidArgumentError(
            "The value_type cannot be changed after column family creation",
            GCP_ERROR_INFO().WithMetadata("mask",
                                          effective_mask.DebugString()));
      }

      FieldMaskUtil::MergeMessageTo(modification.update(), effective_mask,
                                    FieldMaskUtil::MergeOptions(),
                                    &(cf_it->second));

    } else if (modification.has_create()) {
      if (!new_schema.mutable_column_families()
               ->emplace(modification.id(), modification.create())
               .second) {
        LERROR(
            "[PersistedTable][ModifyColumnFamilies] column family already "
            "exists table={} cf_id={}",
            name_, modification.id());
        return AlreadyExistsError(
            "Column family already exists.",
            GCP_ERROR_INFO().WithMetadata("modification",
                                          modification.DebugString()));
      }

      new_cf_names.push_back(modification.id());

    } else {
      LERROR(
          "[PersistedTable][ModifyColumnFamilies] unsupported modification "
          "table={} modification_id={}",
          name_, modification.id());
      return UnimplementedError(
          "Unsupported modification.",
          GCP_ERROR_INFO().WithMetadata("modification",
                                        modification.DebugString()));
    }
  }

  if (!new_cf_names.empty()) {
    auto ensure_status = storage_->EnsureColumnFamiliesExist(new_cf_names);
    if (!ensure_status.ok()) {
      return ensure_status;
    }
  }

  storage::TableMeta updated_meta;
  *updated_meta.mutable_table() = new_schema;

  auto status = storage_->UpdateTableMetadata(name_, updated_meta);
  if (!status.ok()) {
    return status;
  }

  DBG("[PersistedTable][ModifyColumnFamilies] exit table={} new_cf_count={}",
      name_, new_schema.column_families_size());
  return new_schema;
}

Status PersistedTable::DropRowRange(
    google::bigtable::admin::v2::DropRowRangeRequest const& request) {
  DBG("[PersistedTable][DropRowRange] table={} has_prefix={} delete_all={}",
      name_, request.has_row_key_prefix(),
      request.has_delete_all_data_from_table());
  if (!request.has_row_key_prefix() &&
      !request.has_delete_all_data_from_table()) {
    LERROR(
        "[PersistedTable][DropRowRange] neither row prefix nor delete_all set "
        "table={}",
        name_);
    return InvalidArgumentError(
        "Neither row prefix nor delete all data from table is set",
        GCP_ERROR_INFO().WithMetadata("DropRowRange request",
                                      request.DebugString()));
  }

  if (request.has_delete_all_data_from_table()) {
    LERROR(
        "[PersistedTable][DropRowRange] delete_all_data_from_table not "
        "implemented table={}",
        name_);
    return UnimplementedError(
        "delete_all_data_from_table not yet implemented for RocksDB storage",
        GCP_ERROR_INFO());
  }

  auto const& row_key_prefix = request.row_key_prefix();
  if (row_key_prefix.size() > kMaxRowLen) {
    LERROR(
        "[PersistedTable][DropRowRange] row_key_prefix longer than 4KiB "
        "table={} prefix_size={} max={}",
        name_, row_key_prefix.size(), kMaxRowLen);
    return InvalidArgumentError(
        "The row_key_prefix is longer than 4KiB",
        GCP_ERROR_INFO().WithMetadata(
            "row_key_prefix size",
            absl::StrFormat("%zu", row_key_prefix.size())));
  }

  if (row_key_prefix.empty()) {
    LERROR(
        "[PersistedTable][DropRowRange] row prefix empty table={}",
        name_);
    return InvalidArgumentError(
        "Row prefix provided is empty.",
        GCP_ERROR_INFO().WithMetadata("DropRowRange request",
                                      request.DebugString()));
  }

  LERROR(
      "[PersistedTable][DropRowRange] row_key_prefix deletion not "
      "implemented table={} prefix_size={}",
      name_, row_key_prefix.size());
  return UnimplementedError(
      "row_key_prefix deletion not yet implemented for RocksDB storage",
      GCP_ERROR_INFO().WithMetadata("row_key_prefix", row_key_prefix));
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
