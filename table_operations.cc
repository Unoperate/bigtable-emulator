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

#include "table_operations.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "absl/strings/str_format.h"
#include "bigtable_limits.h"
#include "column_family.h"
#include "filter.h"
#include "filtered_table_stream.h"
#include "google/protobuf/util/field_mask_util.h"
#include "in_memory/in_memory_column_family.h"
#include "range_set.h"
#include "re2/re2.h"
#include <google/bigtable/admin/v2/types.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

/// The fraction of rows SampleRowKeysByScanning() returns.
constexpr double kSampleRowKeysPassProbability = 0.0001;

}  // anonymous namespace

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

Status ValidateColumnFamilyId(std::string const& column_family_id) {
  static auto const* const kColumnFamilyIdPattern =
      new re2::RE2("[_a-zA-Z0-9][-_.a-zA-Z0-9]{0,63}");
  if (!re2::RE2::FullMatch(column_family_id, *kColumnFamilyIdPattern)) {
    return InvalidArgumentError(
        "Invalid column family ID: it must be 1-64 characters long, start "
        "with a letter, digit or '_', and contain only letters, digits, '_', "
        "'-' and '.'.",
        GCP_ERROR_INFO().WithMetadata("column_family_id", column_family_id));
  }
  return Status();
}

Status ValidateSchema(google::bigtable::admin::v2::Table const& schema) {
  if (static_cast<std::size_t>(schema.column_families_size()) >
      kMaxColumnFamiliesPerTable) {
    return google::cloud::internal::ResourceExhaustedError(
        "Too many column families: a table can have at most " +
            std::to_string(kMaxColumnFamiliesPerTable) + ".",
        GCP_ERROR_INFO().WithMetadata(
            "limit", std::to_string(kMaxColumnFamiliesPerTable)));
  }
  for (auto const& column_family : schema.column_families()) {
    auto status = ValidateColumnFamilyId(column_family.first);
    if (!status.ok()) {
      return status;
    }
    if (column_family.second.has_value_type()) {
      status = ValidateValueType(column_family.second.value_type());
      if (!status.ok()) {
        return status;
      }
    }
  }
  return Status();
}

StatusOr<google::bigtable::admin::v2::Table>
ApplyModifyColumnFamiliesToSchemaOnly(
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request,
    google::bigtable::admin::v2::Table schema) {
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
                                    &cf_it->second);
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
  auto status = ValidateSchema(schema);
  if (!status.ok()) {
    return status;
  }
  return schema;
}

StatusOr<CellStream> CreateFilteredTableStream(
    std::function<std::vector<std::unique_ptr<FilteredColumnFamilyStream>>()>
        create_column_family_streams,
    absl::optional<btproto::RowFilter> const& filter) {
  auto table_stream_ctor = [create_column_family_streams =
                                std::move(create_column_family_streams)] {
    return CellStream(
        std::make_unique<FilteredTableStream>(create_column_family_streams()));
  };

  if (filter.has_value()) {
    return CreateFilter(filter.value(), table_stream_ctor);
  }

  return table_stream_ctor();
}

StatusOr<bool> RowHasMatchingCells(
    DataOperations const& operations, std::string const& row_key,
    absl::optional<btproto::RowFilter> const& filter) {
  auto range_set = std::make_shared<StringRangeSet>();
  range_set->Sum(StringRangeSet::Range(row_key, false, row_key, false));

  auto maybe_stream = operations.CreateCellStream(range_set, filter);
  if (!maybe_stream) {
    return maybe_stream.status();
  }

  bool a_cell_is_found = false;

  CellStream& stream = *maybe_stream;
  if (stream) {  // At least one cell/value found when filter is applied
    a_cell_is_found = true;
  }

  return a_cell_is_found;
}

StatusOr<std::vector<RowKeySample>> SampleRowKeysByScanning(
    DataOperations const& operations,
    std::function<StatusOr<std::size_t>()> const& estimate_row_count) {
  auto sample_every = static_cast<std::uint64_t>(
      std::ceil(1.0 / kSampleRowKeysPassProbability));

  std::vector<RowKeySample> samples;

  // First, stream all rows and cells and compute the offsets.
  auto all_rows_set = std::make_shared<StringRangeSet>(StringRangeSet::All());
  auto maybe_all_rows_stream =
      operations.CreateCellStream(all_rows_set, absl::nullopt);
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
    samples.push_back(RowKeySample{"", 0});
    return samples;
  }

  std::int64_t offset_delta = sample_every * row_size_estimate;

  google::bigtable::v2::RowFilter sample_filter;
  sample_filter.set_row_sample_filter(kSampleRowKeysPassProbability);

  auto maybe_stream = operations.CreateCellStream(all_rows_set, sample_filter);
  if (!maybe_stream) {
    return maybe_stream.status();
  }

  auto& sampled_stream = *maybe_stream;

  std::int64_t offset = 0;

  bool wrote_a_sample;

  for (; sampled_stream; sampled_stream.Next(NextMode::kRow)) {
    offset += offset_delta;
    samples.push_back(RowKeySample{sampled_stream->row_key(), offset});

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
    auto maybe_row_count_estimate = estimate_row_count();
    if (!maybe_row_count_estimate.ok()) {
      return maybe_row_count_estimate.status();
    }

    std::size_t row_count_estimate = maybe_row_count_estimate.value();

    std::int64_t this_offset = row_count_estimate * row_size_estimate;

    samples.push_back(RowKeySample{"last_key", this_offset});

    offset += this_offset;
  }

  // Client test code expects offset_bytes to be strictly
  // increasing.
  samples.push_back(RowKeySample{"", offset + 1});

  return samples;
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
          cell->set_timestamp_micros(cr.first.count());
          cell->set_value(std::move(cr.second));
        }
      }
    }
  }

  return resp;
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
