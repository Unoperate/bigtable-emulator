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

#include "column_family.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "absl/strings/str_format.h"
#include <google/bigtable/admin/v2/types.pb.h>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

Status ValidateValueType(google::bigtable::admin::v2::Type const& value_type) {
  if (!value_type.has_aggregate_type()) {
    return InvalidArgumentError(
        "no aggregate type set in the supplied value_type",
        GCP_ERROR_INFO().WithMetadata("supplied value type",
                                      value_type.DebugString()));
  }
  auto const& aggregate_type = value_type.aggregate_type();
  switch (aggregate_type.aggregator_case()) {
    case google::bigtable::admin::v2::Type::Aggregate::kSum:
    case google::bigtable::admin::v2::Type::Aggregate::kMin:
    case google::bigtable::admin::v2::Type::Aggregate::kMax:
      return Status();
    default:
      return InvalidArgumentError(
          "unsupported aggregation type",
          GCP_ERROR_INFO().WithMetadata(
              "aggregation case",
              absl::StrFormat("%d", aggregate_type.aggregator_case())));
  }
}

Status ColumnFamily::ChooseCellUpdateFunction(
    google::bigtable::admin::v2::Type value_type) {
  auto status = ValidateValueType(value_type);
  if (!status.ok()) {
    return status;
  }
  switch (value_type.aggregate_type().aggregator_case()) {
    case google::bigtable::admin::v2::Type::Aggregate::kSum:
      update_cell_ = SumUpdateCellBEInt64;
      break;
    case google::bigtable::admin::v2::Type::Aggregate::kMin:
      update_cell_ = MinUpdateCellBEInt64;
      break;
    case google::bigtable::admin::v2::Type::Aggregate::kMax:
      update_cell_ = MaxUpdateCellBEInt64;
      break;
    default:
      break;
  }
  value_type_ = std::move(value_type);
  return Status();
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
