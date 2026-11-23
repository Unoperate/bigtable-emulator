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

#include "filtered_table_stream.h"
#include "absl/types/variant.h"
#include "column_family.h"
#include "filter.h"
#include "re2/re2.h"
#include <memory>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

bool FilteredTableStream::ApplyFilter(InternalFilter const& internal_filter) {
  if (!absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
      !absl::holds_alternative<ColumnRange>(internal_filter)) {
    return MergeCellStreams::ApplyFilter(internal_filter);
  }
  // internal_filter is either FamilyNameRegex or ColumnRange
  for (auto stream_it = unfinished_streams_.begin();
       stream_it != unfinished_streams_.end();) {
    auto* cf_stream =
        static_cast<FilteredColumnFamilyStream*>(&(*stream_it)->impl());

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

    if (absl::holds_alternative<ColumnRange>(internal_filter)) {
      cf_stream->ApplyFilter(internal_filter);
    }

    stream_it++;
  }

  return true;
}

std::vector<CellStream> FilteredTableStream::CreateCellStreams(
    std::vector<std::unique_ptr<FilteredColumnFamilyStream>> cf_streams) {
  std::vector<CellStream> res;
  res.reserve(cf_streams.size());
  for (auto& stream : cf_streams) {
    res.emplace_back(std::move(stream));
  }
  return res;
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
