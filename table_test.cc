// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "table.h"
#include "google/cloud/testing_util/chrono_literals.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "column_family.h"
#include "filter.h"
#include "filtered_table_stream.h"
#include "in_memory/in_memory_column_family.h"
#include "range_set.h"
#include "re2/re2.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

std::string DumpStream(AbstractCellStreamImpl& stream,
                       NextMode next_mode = NextMode::kCell) {
  std::stringstream ss;
  for (; stream.HasValue(); stream.Next(next_mode)) {
    auto const& cell = stream.Value();
    ss << cell.row_key() << " " << cell.column_family() << ":"
       << cell.column_qualifier() << " @"
       << std::chrono::duration_cast<std::chrono::milliseconds>(
              cell.timestamp())
              .count()
       << "ms: " << cell.value() << std::endl;
  }
  return ss.str();
}

TEST(FilteredTableStream, Empty) {
  FilteredTableStream stream({});
  EXPECT_EQ("", DumpStream(stream));
}

TEST(FilteredTableStream, EmptyColumnFamilies) {
  InMemoryColumnFamily fam1;
  InMemoryColumnFamily fam2;
  auto ffam1 = std::make_unique<FilteredInMemoryColumnFamilyStream>(
      fam1, "fam1", std::make_unique<StringRangeSet>(StringRangeSet::All()));
  auto ffam2 = std::make_unique<FilteredInMemoryColumnFamilyStream>(
      fam2, "fam2", std::make_unique<StringRangeSet>(StringRangeSet::All()));
  std::vector<std::unique_ptr<FilteredColumnFamilyStream>> fams;
  fams.emplace_back(std::move(ffam1));
  fams.emplace_back(std::move(ffam2));
  FilteredTableStream stream(std::move(fams));
  EXPECT_EQ("", DumpStream(stream));
}

TEST(FilteredTableStream, ColumnFamiliesAreFiltered) {
  using testing_util::chrono_literals::operator""_ms;

  InMemoryColumnFamily fam1;
  InMemoryColumnFamily fam2;
  fam1.SetCell("row0", "col0", 10_ms, "foo");
  fam2.SetCell("row0", "col0", 10_ms, "foo");
  auto ffam1 = std::make_unique<FilteredInMemoryColumnFamilyStream>(
      fam1, "fam1", std::make_unique<StringRangeSet>(StringRangeSet::All()));
  auto ffam2 = std::make_unique<FilteredInMemoryColumnFamilyStream>(
      fam2, "fam2", std::make_unique<StringRangeSet>(StringRangeSet::All()));
  std::vector<std::unique_ptr<FilteredColumnFamilyStream>> fams;
  fams.emplace_back(std::move(ffam1));
  fams.emplace_back(std::move(ffam2));
  FilteredTableStream stream(std::move(fams));
  auto family_pattern = std::make_shared<re2::RE2>("fam1");
  ASSERT_TRUE(family_pattern->ok());
  stream.ApplyFilter(FamilyNameRegex{family_pattern});
  EXPECT_EQ("row0 fam1:col0 @10ms: foo\n", DumpStream(stream));
}

TEST(FilteredTableStream, OnlyRightFamilyColumnsAreFiltered) {
  using testing_util::chrono_literals::operator""_ms;

  InMemoryColumnFamily fam1;
  InMemoryColumnFamily fam2;
  fam1.SetCell("row0", "col0", 10_ms, "foo");
  fam2.SetCell("row0", "col0", 10_ms, "foo");
  auto ffam1 = std::make_unique<FilteredInMemoryColumnFamilyStream>(
      fam1, "fam1", std::make_unique<StringRangeSet>(StringRangeSet::All()));
  auto ffam2 = std::make_unique<FilteredInMemoryColumnFamilyStream>(
      fam2, "fam2", std::make_unique<StringRangeSet>(StringRangeSet::All()));
  std::vector<std::unique_ptr<FilteredColumnFamilyStream>> fams;
  fams.emplace_back(std::move(ffam1));
  fams.emplace_back(std::move(ffam2));
  FilteredTableStream stream(std::move(fams));

  stream.ApplyFilter(
      ColumnRange{"fam2", StringRangeSet::Range("col0", false, "col1", true)});
  EXPECT_EQ("row0 fam2:col0 @10ms: foo\n", DumpStream(stream));
}

TEST(FilteredTableStream, OtherFiltersArePropagated) {
  using testing_util::chrono_literals::operator""_ms;

  InMemoryColumnFamily fam1;
  InMemoryColumnFamily fam2;
  fam1.SetCell("row1", "col1", 10_ms, "foo");
  fam1.SetCell("row0", "col1", 10_ms, "foo");    // row key regex
  fam2.SetCell("row1", "col1", 10_ms, "foo");    // column family regex
  fam1.SetCell("row1", "col2", 10_ms, "foo");    // column qualifier regex
  fam1.SetCell("row1", "a1", 10_ms, "foo");      // column range
  fam1.SetCell("row1", "col1", 1000_ms, "foo");  // timestamp range
  auto ffam1 = std::make_unique<FilteredInMemoryColumnFamilyStream>(
      fam1, "fam1", std::make_unique<StringRangeSet>(StringRangeSet::All()));
  auto ffam2 = std::make_unique<FilteredInMemoryColumnFamilyStream>(
      fam2, "fam2", std::make_unique<StringRangeSet>(StringRangeSet::All()));
  std::vector<std::unique_ptr<FilteredColumnFamilyStream>> fams;
  fams.emplace_back(std::move(ffam1));
  fams.emplace_back(std::move(ffam2));
  FilteredTableStream stream(std::move(fams));

  auto row_key_pattern = std::make_shared<re2::RE2>("row1");
  ASSERT_TRUE(row_key_pattern->ok());
  EXPECT_TRUE(stream.ApplyFilter(RowKeyRegex{row_key_pattern}));

  auto family_pattern = std::make_shared<re2::RE2>("fam1");
  ASSERT_TRUE(family_pattern->ok());
  EXPECT_TRUE(stream.ApplyFilter(FamilyNameRegex{family_pattern}));

  auto qualifier_pattern = std::make_shared<re2::RE2>("1$");
  ASSERT_TRUE(qualifier_pattern->ok());
  EXPECT_TRUE(stream.ApplyFilter(ColumnRegex{qualifier_pattern}));

  EXPECT_TRUE(stream.ApplyFilter(
      ColumnRange{"fam1", StringRangeSet::Range("co", false, "com", false)}));

  EXPECT_TRUE(stream.ApplyFilter(
      TimestampRange{TimestampRangeSet::Range(0_ms, 300_ms)}));

  EXPECT_EQ("row1 fam1:col1 @10ms: foo\n", DumpStream(stream));
}

class PersistentCellStream : public ::testing::Test {
 protected:
  static constexpr char const* kTableName =
      "snapshot_projects/test/instances/test/tables/test";

  void SetUp() override {
    std::filesystem::remove_all("/tmp/snapshot_projects");
    std::vector<std::string> column_families = {"fam1", "fam2"};
    auto maybe_table = CreateTable(kTableName, column_families, true);
    ASSERT_STATUS_OK(maybe_table);
    table_ = maybe_table.value();

    std::vector<SetCellParams> before = {
        {"fam1", "col", 1000, "old"},
        {"fam2", "col", 1000, "old"},
    };
    ASSERT_STATUS_OK(SetCells(table_, kTableName, "row", before));
  }

  void TearDown() override {
    table_.reset();
    std::filesystem::remove_all("/tmp/snapshot_projects");
  }

  std::shared_ptr<Table> table_;
};

TEST_F(PersistentCellStream, IgnoresWritesAfterCreation) {
  auto stream = table_->GetUtilities()->CreateCellStream(
      std::make_shared<StringRangeSet>(StringRangeSet::All()), absl::nullopt);
  ASSERT_STATUS_OK(stream);

  std::vector<SetCellParams> after = {{"fam2", "col", 2000, "new"}};
  ASSERT_STATUS_OK(SetCells(table_, kTableName, "row", after));

  std::vector<std::string> values;
  for (; stream->HasValue(); stream->Next()) {
    values.push_back(stream->Value().value());
  }
  EXPECT_EQ((std::vector<std::string>{"old", "old"}), values);
}

}  // anonymous namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
