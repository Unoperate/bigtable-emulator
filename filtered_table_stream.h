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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_FILTERED_TABLE_STREAM_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_FILTERED_TABLE_STREAM_H

#include "column_family.h"
#include "filter.h"
#include <memory>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
 * A `AbstractCellStreamImpl` which streams filtered contents of the table.
 *
 * Underneath is essentially a collection of `FilteredColumnFamilyStream`s.
 * All filters applied to `FilteredTableStream` are propagated to the
 * underlying `FilteredColumnFamilyStream`s, except for `FamilyNameRegex` and
 * `ColumnRange`, which are handled by this subclass: streams of non-matching
 * column families are dropped and a `ColumnRange` is passed only to the stream
 * of its column family.
 *
 * This class is public only to enable testing.
 */
class FilteredTableStream : public MergeCellStreams {
 public:
  explicit FilteredTableStream(
      std::vector<std::unique_ptr<FilteredColumnFamilyStream>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  bool ApplyFilter(InternalFilter const& internal_filter) override;

 private:
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<FilteredColumnFamilyStream>> cf_streams);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_FILTERED_TABLE_STREAM_H
