#include "google/cloud/testing_util/chrono_literals.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "filtered_map.h"
#include "re2/re2.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

using testing_util::chrono_literals::operator""_ms;

bool const kOpen = true;
bool const kClosed = false;

class FilteredStreamTest : public ::testing::Test {
 protected:
  std::string table_path_;
  std::shared_ptr<rocksdb::TransactionDB> db_;
  std::shared_ptr<rocksdb::ColumnFamilyHandle> cf_;
  std::string const cf_name_ = "cf_test";
  std::string const col_key_ = "col";

  void SetUp() override {
    auto unit_test = ::testing::UnitTest::GetInstance();
    table_path_ = std::string("/tmp/filter_projects/") +
                  unit_test->current_test_info()->test_suite_name() + "_" +
                  unit_test->current_test_info()->name();

    std::filesystem::remove_all(table_path_);  // Clean start
    std::filesystem::create_directories(table_path_);

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = false;
    rocksdb::TransactionDBOptions txn_options;

    rocksdb::TransactionDB* raw_db = nullptr;
    auto status = rocksdb::TransactionDB::Open(options, txn_options,
                                               table_path_, &raw_db);
    ASSERT_TRUE(status.ok()) << status.ToString();
    db_.reset(raw_db);

    rocksdb::ColumnFamilyHandle* raw_cf = nullptr;
    status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), cf_name_,
                                     &raw_cf);
    ASSERT_TRUE(status.ok()) << status.ToString();
    cf_.reset(raw_cf);
  }

  void TearDown() override {
    cf_.reset();
    db_.reset();
    std::filesystem::remove_all("/tmp/filter_projects");
  }

  void Populate(std::map<std::string, std::string> const& data) {
    for (auto const& [q, v] : data) {
      auto status = db_->Put(rocksdb::WriteOptions(), cf_.get(),
                             KeyCoder::Encode(q, col_key_, 0), v);
      ASSERT_TRUE(status.ok());
    }
  }

  void Populate(std::map<std::chrono::milliseconds, std::string,
                         std::greater<>> const& data) {
    for (auto const& [ts, v] : data) {
      auto status =
          db_->Put(rocksdb::WriteOptions(), cf_.get(),
                   KeyCoder::Encode(col_key_, col_key_, ts.count()), v);
      ASSERT_TRUE(status.ok());
    }
  }

  std::map<std::string, std::string> RunStream(
      std::shared_ptr<StringRangeSet> const& filter) {
    std::map<std::string, std::string> result_data;
    auto stream =
        FilteredPersistentColumnFamilyStream(cf_, cf_name_, db_, filter);

    while (stream.HasValue()) {
      auto val = stream.Value();
      result_data.emplace(val.row_key(), val.value());
      stream.Next(NextMode::kCell);
    }
    return result_data;
  }

  std::vector<std::chrono::milliseconds> RunStream(
      std::shared_ptr<TimestampRangeSet> const& filter) {
    std::vector<std::chrono::milliseconds> result_data;

    auto stream = FilteredPersistentColumnFamilyStream(
        cf_, cf_name_, db_,
        std::make_shared<StringRangeSet>(StringRangeSet::All()));

    for (auto r : filter->disjoint_ranges()) {
      stream.ApplyFilter(InternalFilter(TimestampRange{.range = r}));
    }
    if (filter->disjoint_ranges().empty()) {
      stream.ApplyFilter(InternalFilter(TimestampRange{
          .range = TimestampRangeSet::Range(std::chrono::milliseconds(1),
                                            std::chrono::milliseconds(1))}));
    }

    while (stream.HasValue()) {
      auto val = stream.Value();
      result_data.push_back(val.timestamp());
      stream.Next(NextMode::kCell);
    }
    return result_data;
  }

  std::map<std::string, std::string> RunStream(
      std::vector<std::shared_ptr<re2::RE2>> const& patterns) {
    std::map<std::string, std::string> result_data;
    auto stream = FilteredPersistentColumnFamilyStream(
        cf_, cf_name_, db_,
        std::make_shared<StringRangeSet>(StringRangeSet::All()));

    for (auto const& p : patterns) {
      stream.ApplyFilter(InternalFilter(RowKeyRegex{.regex = p}));
    }

    while (stream.HasValue()) {
      auto val = stream.Value();
      result_data.emplace(val.row_key(), val.value());
      stream.Next(NextMode::kCell);
    }
    return result_data;
  }
};

static std::vector<std::string> keys(
    std::map<std::string, std::string> const& m) {
  std::vector<std::string> res;
  for (auto const& x : m) {
    res.push_back(x.first);
  }
  return res;
}

TEST_F(FilteredStreamTest, NoStreamFilter) {
  std::map<std::string, std::string> test_data{
      {"zero", "0"}, {"one", "1"}, {"two", "2"}};

  Populate(test_data);

  auto filter = std::make_shared<StringRangeSet>(StringRangeSet::All());
  auto result = RunStream(filter);

  ASSERT_EQ(test_data, result);
}

TEST_F(FilteredStreamTest, EmptyStreamFilter) {
  std::map<std::string, std::string> test_data{
      {"zero", "0"}, {"one", "1"}, {"two", "2"}};

  Populate(test_data);  // Populate, but filter is empty

  auto filter = std::make_shared<StringRangeSet>(StringRangeSet::Empty());
  auto result = RunStream(filter);

  ASSERT_TRUE(result.empty());
}

TEST_F(FilteredStreamTest, OneStreamOpen) {
  std::map<std::string, std::string> unfiltered{{"AA", "0"},   {"AAA", "0"},
                                                {"AAAa", "0"}, {"AAAb", "0"},
                                                {"AAB", "0"},  {"AAC", "0"}};
  Populate(unfiltered);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kOpen, "AAB", kOpen));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream(filter);
  std::vector<std::string> expected{"AAAa", "AAAb"};
  ASSERT_EQ(expected, keys(result));
}

TEST_F(FilteredStreamTest, OneStreamClosed) {
  std::map<std::string, std::string> unfiltered{{"AA", "0"},   {"AAA", "0"},
                                                {"AAAa", "0"}, {"AAAb", "0"},
                                                {"AAB", "0"},  {"AAC", "0"}};
  Populate(unfiltered);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kClosed, "AAB", kClosed));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream(filter);
  std::vector<std::string> expected{"AAA", "AAAa", "AAAb", "AAB"};
  ASSERT_EQ(expected, keys(result));
}

TEST_F(FilteredStreamTest, NoEntriesAfterClosedStreamFilter) {
  std::map<std::string, std::string> unfiltered{
      {"AA", "0"}, {"AAA", "0"}, {"AAAa", "0"}, {"AAAb", "0"}};
  Populate(unfiltered);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kClosed, "AAB", kClosed));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream(filter);
  std::vector<std::string> expected{"AAA", "AAAa", "AAAb"};
  ASSERT_EQ(expected, keys(result));
}

TEST_F(FilteredStreamTest, NoEntriesAfterOpenStreamFilter) {
  std::map<std::string, std::string> unfiltered{
      {"AA", "0"}, {"AAA", "0"}, {"AAAa", "0"}, {"AAAb", "0"}};
  Populate(unfiltered);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kOpen, "AAB", kOpen));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream(filter);
  std::vector<std::string> expected{"AAAa", "AAAb"};
  ASSERT_EQ(expected, keys(result));
}

TEST_F(FilteredStreamTest, NoEntriesBeforeClosedStreamFilter) {
  std::map<std::string, std::string> unfiltered{
      {"AAA", "0"}, {"AAAa", "0"}, {"AAAb", "0"}, {"AAB", "0"}, {"AAC", "0"}};
  Populate(unfiltered);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kClosed, "AAB", kClosed));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream(filter);
  std::vector<std::string> expected{"AAA", "AAAa", "AAAb", "AAB"};
  ASSERT_EQ(expected, keys(result));
}

TEST_F(FilteredStreamTest, NoEntriesBeforeOpenStreamFilter) {
  std::map<std::string, std::string> unfiltered{
      {"AAAa", "0"}, {"AAAb", "0"}, {"AAB", "0"}, {"AAC", "0"}};
  Populate(unfiltered);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kOpen, "AAB", kOpen));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream(filter);
  std::vector<std::string> expected{"AAAa", "AAAb"};
  ASSERT_EQ(expected, keys(result));
}

TEST_F(FilteredStreamTest, MultipleStreamFilters) {
  std::map<std::string, std::string> unfiltered{
      {"AA", "0"},   {"AAA", "0"}, {"AAAa", "0"}, {"AAAb", "0"}, {"AAB", "0"},
      {"AAC", "0"},  {"BB", "0"},  {"BBB", "0"},  {"BBBb", "0"}, {"CCCa", "0"},
      {"CCCb", "0"}, {"CCD", "0"}, {"CCE", "0"}};
  Populate(unfiltered);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kOpen, "AAB", kClosed));
  range_set.Sum(StringRangeSet::Range("BBB", kClosed, "BBC", kOpen));
  range_set.Sum(StringRangeSet::Range("CCC", kClosed, "CCD", kOpen));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream(filter);
  std::vector<std::string> expected{"AAAa", "AAAb", "AAB", "BBB",
                                    "BBBb", "CCCa", "CCCb"};
  ASSERT_EQ(expected, keys(result));
}

TEST_F(FilteredStreamTest, NoTimestampFilter) {
  std::map<std::chrono::milliseconds, std::string, std::greater<>> unfiltered{
      {0_ms, "0"}, {1_ms, "1"}, {2_ms, "2"}};
  Populate(unfiltered);

  auto ts_set = std::make_shared<TimestampRangeSet>(TimestampRangeSet::All());
  auto result = RunStream(ts_set);
  ASSERT_EQ(std::vector<std::chrono::milliseconds>({2_ms, 1_ms, 0_ms}), result);
}

TEST_F(FilteredStreamTest, EmptyTimestampFilter) {
  std::map<std::chrono::milliseconds, std::string, std::greater<>> unfiltered{
      {0_ms, "0"}, {1_ms, "1"}, {2_ms, "2"}};
  Populate(unfiltered);

  auto ts_set = std::make_shared<TimestampRangeSet>(TimestampRangeSet::Empty());
  auto result = RunStream(ts_set);
  ASSERT_EQ(std::vector<std::chrono::milliseconds>(), result);
}

TEST_F(FilteredStreamTest, FiniteTimestampRange) {
  std::map<std::chrono::milliseconds, std::string, std::greater<>> unfiltered{
      {0_ms, "0"}, {1_ms, "0"}, {2_ms, "0"}, {3_ms, "0"}, {4_ms, "0"}};
  Populate(unfiltered);

  auto ts_set = std::make_shared<TimestampRangeSet>(TimestampRangeSet::Empty());
  ts_set->Sum(TimestampRangeSet::Range(1_ms, 3_ms));
  auto result = RunStream(ts_set);
  ASSERT_EQ(std::vector<std::chrono::milliseconds>({2_ms, 1_ms}), result);
}

TEST_F(FilteredStreamTest, InfiniteTimestampRange) {
  std::map<std::chrono::milliseconds, std::string, std::greater<>> unfiltered{
      {0_ms, "0"}, {1_ms, "0"}, {2_ms, "0"}, {3_ms, "0"}, {4_ms, "0"}};
  Populate(unfiltered);

  auto ts_set = std::make_shared<TimestampRangeSet>(TimestampRangeSet::Empty());
  ts_set->Sum(TimestampRangeSet::Range(1_ms, 0_ms));
  auto result = RunStream(ts_set);
  ASSERT_EQ(std::vector<std::chrono::milliseconds>({4_ms, 3_ms, 2_ms, 1_ms}),
            result);
}

TEST_F(FilteredStreamTest, MultipleJointTimestampFilters) {
  std::map<std::chrono::milliseconds, std::string, std::greater<>> unfiltered{
      {0_ms, "0"},  {1_ms, "0"},  {2_ms, "0"},  {3_ms, "0"},
      {4_ms, "0"},  {5_ms, "0"},  {6_ms, "0"},  {7_ms, "0"},
      {8_ms, "0"},  {9_ms, "0"},  {10_ms, "0"}, {11_ms, "0"},
      {12_ms, "0"}, {13_ms, "0"}, {14_ms, "0"}, {15_ms, "0"},
  };
  Populate(unfiltered);

  auto ts_set = std::make_shared<TimestampRangeSet>(TimestampRangeSet::Empty());
  ts_set->Sum(TimestampRangeSet::Range(1_ms, 3_ms));
  ts_set->Sum(TimestampRangeSet::Range(3_ms, 5_ms));
  ts_set->Sum(TimestampRangeSet::Range(5_ms, 8_ms));

  auto result = RunStream(ts_set);
  ASSERT_EQ(std::vector<std::chrono::milliseconds>(
                {7_ms, 6_ms, 5_ms, 4_ms, 3_ms, 2_ms, 1_ms}),
            result);
}

TEST_F(FilteredStreamTest, MultipleDisjointTimestampFilters) {
  std::map<std::chrono::milliseconds, std::string, std::greater<>> unfiltered{
      {0_ms, "0"},  {1_ms, "0"},  {2_ms, "0"},  {3_ms, "0"},
      {4_ms, "0"},  {5_ms, "0"},  {6_ms, "0"},  {7_ms, "0"},
      {8_ms, "0"},  {9_ms, "0"},  {10_ms, "0"}, {11_ms, "0"},
      {12_ms, "0"}, {13_ms, "0"}, {14_ms, "0"}, {15_ms, "0"},
  };
  Populate(unfiltered);

  auto ts_set = std::make_shared<TimestampRangeSet>(TimestampRangeSet::Empty());
  ts_set->Sum(TimestampRangeSet::Range(1_ms, 3_ms));
  ts_set->Sum(TimestampRangeSet::Range(3_ms, 5_ms));
  ts_set->Sum(TimestampRangeSet::Range(6_ms, 8_ms));

  auto result = RunStream(ts_set);
  ASSERT_EQ(std::vector<std::chrono::milliseconds>({}), result);
}

TEST_F(FilteredStreamTest, EmptyRegexFilter) {
  auto pattern = std::make_shared<re2::RE2>("this_will_not_be_matched");
  ASSERT_TRUE(pattern->ok());
  std::vector<std::shared_ptr<re2::RE2>> patterns({std::move(pattern)});

  std::map<std::string, std::string> unfiltered{
      {"zero", "0"}, {"one", "1"}, {"two", "2"}};
  Populate(unfiltered);

  auto result = RunStream(patterns);
  ASSERT_EQ(std::vector<std::string>(), keys(result));
}

TEST_F(FilteredStreamTest, OneRegexFilter) {
  auto pattern = std::make_shared<re2::RE2>("^[a-z_]*$");
  ASSERT_TRUE(pattern->ok());
  std::vector<std::shared_ptr<re2::RE2>> patterns({std::move(pattern)});

  std::map<std::string, std::string> unfiltered{
      {"NO_MATCH", "0"}, {"match", "1"}, {"another_match", "2"}};
  Populate(unfiltered);

  auto result = RunStream(patterns);
  ASSERT_EQ(std::vector<std::string>({"another_match", "match"}), keys(result));
}

TEST_F(FilteredStreamTest, MultipleFilters) {
  auto has_a = std::make_shared<re2::RE2>("a");
  ASSERT_TRUE(has_a->ok());
  auto has_b = std::make_shared<re2::RE2>("b");
  ASSERT_TRUE(has_b->ok());
  auto has_c = std::make_shared<re2::RE2>("c");
  ASSERT_TRUE(has_c->ok());
  std::vector<std::shared_ptr<re2::RE2>> patterns(
      {std::move(has_a), std::move(has_b), std::move(has_c)});

  std::map<std::string, std::string> unfiltered{{"abc", "0"},
                                                {"ab", "1"},
                                                {"a", "2"},
                                                {"QQ b QQ c QQ a QQ", "4"},
                                                {"ac", "5"}};
  Populate(unfiltered);

  auto result = RunStream(patterns);
  ASSERT_EQ(std::vector<std::string>({"QQ b QQ c QQ a QQ", "abc"}),
            keys(result));
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
