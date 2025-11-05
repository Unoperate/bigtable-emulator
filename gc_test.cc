// Copyright 2025 Google LLC
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

#include "google/cloud/testing_util/chrono_literals.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "column_family.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/protobuf/duration.pb.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

using ::google::cloud::StatusCode;
using ::google::cloud::testing_util::StatusIs;
using testing_util::chrono_literals::operator""_ms;

class GCTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto cf_result = ColumnFamily::ConstructColumnFamily();
    ASSERT_TRUE(cf_result.ok());
    cf_ = cf_result.value();
  }

  void AddTestData() {
    cf_->SetCell("row1", "col1", 100_ms, "v1");
    cf_->SetCell("row1", "col1", 200_ms, "v2");
    cf_->SetCell("row1", "col1", 300_ms, "v3");
    cf_->SetCell("row1", "col1", 400_ms, "v4");
    cf_->SetCell("row1", "col1", 500_ms, "v5");

    cf_->SetCell("row1", "col2", 150_ms, "v6");
    cf_->SetCell("row1", "col2", 250_ms, "v7");
    cf_->SetCell("row1", "col2", 350_ms, "v8");

    cf_->SetCell("row2", "col1", 100_ms, "v9");
    cf_->SetCell("row2", "col1", 200_ms, "v10");

    cf_->SetCell("row2", "col2", 300_ms, "v11");
  }

  int CountTotalCells() {
    int count = 0;
    for (auto const& row : *cf_) {
      for (auto const& col : row.second) {
        count += static_cast<int>(col.second.size());
      }
    }
    return count;
  }

  std::set<std::chrono::milliseconds> GetColumnTimestamps(
      std::string const& row_key, std::string const& col_key) {
    std::set<std::chrono::milliseconds> column_timestamps;
    auto row_it = cf_->find(row_key);
    if (row_it == cf_->end()) {
      return column_timestamps;
    }
    auto col_it = row_it->second.find(col_key);
    if (col_it == row_it->second.end()) {
      return column_timestamps;
    };
    std::transform(col_it->second.begin(), col_it->second.end(),
                   std::inserter(column_timestamps, column_timestamps.begin()),
                   [](auto const& pair) { return pair.first; });
    return column_timestamps;
  }

  int CountCellsInColumn(std::string const& row_key,
                         std::string const& col_key) {
    return GetColumnTimestamps(row_key, col_key).size();
  }

  std::shared_ptr<ColumnFamily> cf_;
};

TEST_F(GCTest, MaxNumVersionsKeepsAllWhenBelowLimit) {
  AddTestData();
  EXPECT_EQ(11, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.set_max_num_versions(10);
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  EXPECT_EQ(11, CountTotalCells());
}

TEST_F(GCTest, MaxNumVersionsRemovesOldestCells) {
  AddTestData();
  EXPECT_EQ(11, CountTotalCells());
  EXPECT_EQ(5, CountCellsInColumn("row1", "col1"));

  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.set_max_num_versions(3);
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  // Each column keeps its 3 newest cells (or all if it has ≤3)
  EXPECT_EQ((std::set<std::chrono::milliseconds>{500_ms, 400_ms, 300_ms}),
            GetColumnTimestamps("row1", "col1"));  // was 5, keeps 3 newest
  EXPECT_EQ((std::set<std::chrono::milliseconds>{350_ms, 250_ms, 150_ms}),
            GetColumnTimestamps("row1", "col2"));  // was 3, keeps all 3
  EXPECT_EQ((std::set<std::chrono::milliseconds>{200_ms, 100_ms}),
            GetColumnTimestamps("row2", "col1"));  // was 2, keeps all 2
  EXPECT_EQ((std::set<std::chrono::milliseconds>{300_ms}),
            GetColumnTimestamps("row2", "col2"));  // was 1, keeps all 1
  // Total: 3+3+2+1 = 9 cells remaining
  EXPECT_EQ(9, CountTotalCells());
}

TEST_F(GCTest, MaxAgeRemovesOldCells) {
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());

  // Use 24-hour margin for deterministic behavior
  auto old_time =
      now - std::chrono::milliseconds(24LL * 60 * 60 * 1000);  // 24 hours ago
  auto recent_time =
      now - std::chrono::milliseconds(30 * 60 * 1000);  // 30 minutes ago

  cf_->SetCell("row1", "col1", old_time, "old_value");
  cf_->SetCell("row1", "col1", recent_time, "recent_value");
  cf_->SetCell("row1", "col2", old_time, "old_value2");

  EXPECT_EQ(3, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  auto* max_age = gc_rule.mutable_max_age();
  max_age->set_seconds(3600);  // 1 hour max age
  max_age->set_nanos(0);
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  EXPECT_EQ(1, CountTotalCells());
  EXPECT_EQ((std::set<std::chrono::milliseconds>{recent_time}),
            GetColumnTimestamps("row1", "col1"));
}

TEST_F(GCTest, MaxAgeKeepsRecentCells) {
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());

  // Both timestamps well within the 1-hour limit
  auto recent_time1 =
      now - std::chrono::milliseconds(10 * 60 * 1000);  // 10 minutes ago
  auto recent_time2 =
      now - std::chrono::milliseconds(20 * 60 * 1000);  // 20 minutes ago

  cf_->SetCell("row1", "col1", recent_time1, "recent1");
  cf_->SetCell("row1", "col1", recent_time2, "recent2");

  EXPECT_EQ(2, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  auto* max_age = gc_rule.mutable_max_age();
  max_age->set_seconds(3600);  // 1 hour max age
  max_age->set_nanos(0);
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  EXPECT_EQ(2, CountTotalCells());
}

TEST_F(GCTest, IntersectionAllRulesMustMatch) {
  AddTestData();
  EXPECT_EQ(11, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  auto* intersection = gc_rule.mutable_intersection();

  auto* rule1 = intersection->add_rules();
  rule1->set_max_num_versions(3);

  auto* rule2 = intersection->add_rules();
  auto* max_age = rule2->mutable_max_age();
  max_age->set_seconds(30);  // 30 seconds
  max_age->set_nanos(0);

  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  // Intersection: cell removed ONLY if ALL rules would remove it
  // max_num_versions(3): keeps 3 newest per column (9 total)
  // max_age(30s): removes all cells (test data is very old, near Unix epoch)
  // Result: cells kept by max_num_versions are preserved (9 cells)
  EXPECT_EQ(9, CountTotalCells());
  EXPECT_EQ((std::set<std::chrono::milliseconds>{500_ms, 400_ms, 300_ms}),
            GetColumnTimestamps("row1", "col1"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{350_ms, 250_ms, 150_ms}),
            GetColumnTimestamps("row1", "col2"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{200_ms, 100_ms}),
            GetColumnTimestamps("row2", "col1"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{300_ms}),
            GetColumnTimestamps("row2", "col2"));
}

TEST_F(GCTest, IntersectionEmptyRulesKeepsAll) {
  AddTestData();
  EXPECT_EQ(11, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.mutable_intersection();
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();
  EXPECT_EQ(11, CountTotalCells());
}

TEST_F(GCTest, UnionAnyRuleMatches) {
  AddTestData();

  // Add recent cells that should survive both rules
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  auto recent_time =
      now - std::chrono::milliseconds(10 * 1000);  // 10 seconds ago

  cf_->SetCell("row3", "col1", recent_time, "recent1");
  cf_->SetCell("row3", "col2", recent_time, "recent2");

  EXPECT_EQ(13, CountTotalCells());  // 11 + 2 recent

  google::bigtable::admin::v2::GcRule gc_rule;
  auto* union_rule = gc_rule.mutable_union_();

  auto* rule1 = union_rule->add_rules();
  rule1->set_max_num_versions(2);

  auto* rule2 = union_rule->add_rules();
  auto* max_age = rule2->mutable_max_age();
  max_age->set_seconds(30);  // 30 seconds
  max_age->set_nanos(0);

  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  // Union: cell removed if ANY rule would remove it
  // Old cells removed by max_age, recent cells survive both rules
  // Recent cells: pass max_age and there's only 1 per column (≤2)
  EXPECT_EQ(2, CountTotalCells());
  EXPECT_EQ((std::set<std::chrono::milliseconds>{recent_time}),
            GetColumnTimestamps("row3", "col1"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{recent_time}),
            GetColumnTimestamps("row3", "col2"));
}

TEST_F(GCTest, UnionEmptyRulesKeepsAll) {
  AddTestData();
  EXPECT_EQ(11, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.mutable_union_();
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  EXPECT_EQ(11, CountTotalCells());
}

TEST_F(GCTest, NestedRulesIntersectionOfUnions) {
  AddTestData();

  // Add recent cells that should survive the max_age rules
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  auto recent_time =
      now - std::chrono::milliseconds(10 * 1000);  // 10 seconds ago

  cf_->SetCell("row4", "col1", recent_time, "recent_cell");

  EXPECT_EQ(12, CountTotalCells());  // 11 + 1 recent

  google::bigtable::admin::v2::GcRule gc_rule;
  auto* intersection = gc_rule.mutable_intersection();

  auto* union1 = intersection->add_rules()->mutable_union_();
  union1->add_rules()->set_max_num_versions(3);
  union1->add_rules()->set_max_num_versions(4);

  auto* union2 = intersection->add_rules()->mutable_union_();
  auto* max_age1 = union2->add_rules()->mutable_max_age();
  max_age1->set_seconds(30);  // 30 seconds
  max_age1->set_nanos(0);
  auto* max_age2 = union2->add_rules()->mutable_max_age();
  max_age2->set_seconds(25);  // 25 seconds
  max_age2->set_nanos(0);

  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  // We expect only the v1 and v2 cells in row1 to be deleted (They
  // are the only ones that are > 25ms of age AND in a column where it
  // is not among the most recent 3 OR the most recent 4). Therefore
  // from 12 cells, we expect 10 to survive the GC.
  EXPECT_EQ(10, CountTotalCells());
  EXPECT_EQ((std::set<std::chrono::milliseconds>{500_ms, 400_ms, 300_ms}),
            GetColumnTimestamps("row1", "col1"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{350_ms, 250_ms, 150_ms}),
            GetColumnTimestamps("row1", "col2"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{200_ms, 100_ms}),
            GetColumnTimestamps("row2", "col1"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{300_ms}),
            GetColumnTimestamps("row2", "col2"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{recent_time}),
            GetColumnTimestamps("row4", "col1"));
}

TEST_F(GCTest, NestedRulesUnionOfIntersections) {
  AddTestData();

  // Add recent cells that should survive the max_age rules
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  auto recent_time =
      now - std::chrono::milliseconds(10 * 1000);  // 10 seconds ago

  cf_->SetCell("row5", "col1", recent_time, "recent1");
  cf_->SetCell("row5", "col2", recent_time, "recent2");

  EXPECT_EQ(13, CountTotalCells());  // 11 + 2 recent

  google::bigtable::admin::v2::GcRule gc_rule;
  auto* union_rule = gc_rule.mutable_union_();

  auto* intersection1 = union_rule->add_rules()->mutable_intersection();
  intersection1->add_rules()->set_max_num_versions(2);
  auto* max_age1 = intersection1->add_rules()->mutable_max_age();
  max_age1->set_seconds(25);  // 25 seconds
  max_age1->set_nanos(0);

  auto* intersection2 = union_rule->add_rules()->mutable_intersection();
  intersection2->add_rules()->set_max_num_versions(3);
  auto* max_age2 = intersection2->add_rules()->mutable_max_age();
  max_age2->set_seconds(30);  // 30 seconds
  max_age2->set_nanos(0);

  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  // We expect v1, v2, v3 and v6 to be deleted, leaving 9.
  EXPECT_EQ(9, CountTotalCells());
  EXPECT_EQ((std::set<std::chrono::milliseconds>{500_ms, 400_ms}),
            GetColumnTimestamps("row1", "col1"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{350_ms, 250_ms}),
            GetColumnTimestamps("row1", "col2"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{200_ms, 100_ms}),
            GetColumnTimestamps("row2", "col1"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{300_ms}),
            GetColumnTimestamps("row2", "col2"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{recent_time}),
            GetColumnTimestamps("row5", "col1"));
  EXPECT_EQ((std::set<std::chrono::milliseconds>{recent_time}),
            GetColumnTimestamps("row5", "col2"));
}

TEST_F(GCTest, NoGCRuleNoGarbageCollection) {
  AddTestData();
  EXPECT_EQ(11, CountTotalCells());

  cf_->RunGC();
  EXPECT_EQ(11, CountTotalCells());
}

TEST_F(GCTest, EmptyColumnFamilyGCDoesNothing) {
  EXPECT_EQ(0, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.set_max_num_versions(1);
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  EXPECT_EQ(0, CountTotalCells());
}

TEST_F(GCTest, SingleCellMaxVersionsOne) {
  cf_->SetCell("row1", "col1", 100_ms, "value");
  EXPECT_EQ(1, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.set_max_num_versions(1);
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  EXPECT_EQ(1, CountTotalCells());
}

TEST_F(GCTest, ColumnRowGCDirect) {
  AddTestData();

  auto row_it = cf_->find("row1");
  ASSERT_NE(row_it, cf_->end());
  auto col_it = row_it->second.find("col1");
  ASSERT_NE(col_it, row_it->second.end());

  EXPECT_EQ(5, std::distance(col_it->second.begin(), col_it->second.end()));

  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.set_max_num_versions(3);
  col_it->second.RunGC(gc_rule);

  EXPECT_EQ(3, static_cast<int>(std::distance(col_it->second.begin(),
                                              col_it->second.end())));
}

TEST_F(GCTest, DeepNestingHandlesRecursion) {
  AddTestData();

  google::bigtable::admin::v2::GcRule gc_rule;
  auto* current = &gc_rule;

  for (int i = 0; i < 10; ++i) {
    auto* intersection = current->mutable_intersection();
    intersection->add_rules()->set_max_num_versions(5);
    current = intersection->add_rules();
  }
  current->set_max_num_versions(2);

  cf_->SetGCRule(gc_rule);
  EXPECT_EQ(11, CountTotalCells());
  cf_->RunGC();
  EXPECT_EQ(11, CountTotalCells());
  // The GC deleted nothing because some of the intersected
  // rules are empty intersections which do NOT delete anything.
  EXPECT_EQ(5, CountCellsInColumn("row1", "col1"));
}

TEST_F(GCTest, MaxNumVersionsVeryLarge) {
  AddTestData();
  EXPECT_EQ(11, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.set_max_num_versions(1000000);  // Very large number
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  EXPECT_EQ(11, CountTotalCells());  // Should keep all cells
}

TEST_F(GCTest, MaxAgeBoundaryCondition) {
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());

  // Create cells with safe margins around the 1-hour boundary
  auto clearly_over_1_hour_ago =
      now - std::chrono::milliseconds(65 * 60 * 1000);  // 65 minutes ago
  auto clearly_under_1_hour_ago =
      now - std::chrono::milliseconds(50 * 60 * 1000);  // 50 minutes ago
  auto very_recent =
      now - std::chrono::milliseconds(10 * 1000);  // 10 seconds ago

  cf_->SetCell("row1", "col1", clearly_over_1_hour_ago, "old");
  cf_->SetCell("row1", "col1", clearly_under_1_hour_ago, "medium");
  cf_->SetCell("row1", "col1", very_recent, "recent");

  EXPECT_EQ(3, CountTotalCells());

  google::bigtable::admin::v2::GcRule gc_rule;
  auto* max_age = gc_rule.mutable_max_age();
  max_age->set_seconds(3600);  // 1 hour
  max_age->set_nanos(0);
  cf_->SetGCRule(gc_rule);
  cf_->RunGC();

  // The two recent cells (50min and 10sec old) should remain
  EXPECT_EQ(2, CountTotalCells());
  EXPECT_EQ((std::set<std::chrono::milliseconds>{very_recent,
                                                 clearly_under_1_hour_ago}),
            GetColumnTimestamps("row1", "col1"));
}

// FIXME: Write some concurrency tests once garbage collections starts taking
// more granular locks instead of the table ones.

TEST(GCRuleTest, UnsetRuleIsValid) {
  google::bigtable::admin::v2::GcRule gc_rule;
  auto status = CheckGCRuleIsValid(gc_rule);
  EXPECT_STATUS_OK(status);
}

TEST(GCRuleTest, EmptyIntersectionIsValid) {
  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.mutable_intersection();
  auto status = CheckGCRuleIsValid(gc_rule);
  EXPECT_STATUS_OK(status);
}

TEST(GCRuleTest, EmptyUnionIsValid) {
  google::bigtable::admin::v2::GcRule gc_rule;
  gc_rule.mutable_union_();
  auto status = CheckGCRuleIsValid(gc_rule);
  EXPECT_STATUS_OK(status);
}

TEST(GCRuleTest, MaxNumVersionsLessThanOneIsInvalid) {
  for (auto max_versions : {INT32_MIN, -1, 0}) {
    google::bigtable::admin::v2::GcRule gc_rule;
    gc_rule.set_max_num_versions(max_versions);
    auto status = CheckGCRuleIsValid(gc_rule);
    EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
  }
}

TEST(GCRuleTest, MaxAgeNegativeDurationIsInvalid) {
  google::bigtable::admin::v2::GcRule gc_rule;
  auto* max_age = gc_rule.mutable_max_age();
  max_age->set_seconds(-1);
  max_age->set_nanos(0);
  auto status = CheckGCRuleIsValid(gc_rule);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

// The proto definition requires max_age duration to be at least 1ms.
TEST(GCRuleTest, MaxAgeShorterThanOneMillisecondIsInvalid) {
  google::bigtable::admin::v2::GcRule gc_rule;
  auto* max_age = gc_rule.mutable_max_age();
  max_age->set_seconds(0);
  max_age->set_nanos(999999);
  auto status = CheckGCRuleIsValid(gc_rule);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(GCRuleTest, MaxAgeOfExactlyOneMillisecondIsValid) {
  google::bigtable::admin::v2::GcRule gc_rule;
  auto* max_age = gc_rule.mutable_max_age();
  max_age->set_seconds(0);
  max_age->set_nanos(1000000);
  auto status = CheckGCRuleIsValid(gc_rule);
  EXPECT_STATUS_OK(status);
}

TEST(GCRuleTest, IntersectionWithInvalidRuleIsInvalid) {
  google::bigtable::admin::v2::GcRule gc_rule;
  auto* intersection = gc_rule.mutable_intersection();
  intersection->add_rules()->set_max_num_versions(3);
  intersection->add_rules()->set_max_num_versions(-1);
  auto status = CheckGCRuleIsValid(gc_rule);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(GCRuleTest, UnionWithInvalidRuleIsInvalid) {
  google::bigtable::admin::v2::GcRule gc_rule;
  auto* union_rule = gc_rule.mutable_union_();
  union_rule->add_rules()->set_max_num_versions(3);
  union_rule->add_rules()->set_max_num_versions(-1);
  auto status = CheckGCRuleIsValid(gc_rule);
  EXPECT_THAT(status, StatusIs(StatusCode::kInvalidArgument));
}

TEST(GCRuleTest, TooLargeGCRuleIsInvalid) {
  google::bigtable::admin::v2::GcRule large_rule;
  auto* union_rule = large_rule.mutable_union_();

  for (int i = 0; i < 200; ++i) {
    union_rule->add_rules()->set_max_num_versions(i + 1);
  }

  auto cf_result =
      ColumnFamily::ConstructColumnFamily(absl::nullopt, large_rule);
  EXPECT_THAT(cf_result, StatusIs(StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
