#include "google/cloud/testing_util/chrono_literals.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "filtered_map.h"
#include "persistent/persistent_column_family.h"
#include "re2/re2.h"
#include "test_util.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

using testing_util::chrono_literals::operator""_ms;

bool const kOpen = true;
bool const kClosed = false;

struct StreamedCell {
  std::string row;
  std::string column;
  std::chrono::microseconds timestamp;
  std::string value;

  bool operator==(StreamedCell const& other) const {
    return std::tie(row, column, timestamp, value) ==
           std::tie(other.row, other.column, other.timestamp, other.value);
  }
};

void PrintTo(StreamedCell const& cell, std::ostream* os) {
  *os << "{" << cell.row << ", " << cell.column << ", "
      << cell.timestamp.count() << "us, " << cell.value << "}";
}

class PersistentColumnFamilyStreamTest : public ::testing::Test {
 protected:
  std::string table_path_;
  std::shared_ptr<rocksdb::TransactionDB> db_;
  std::shared_ptr<rocksdb::ColumnFamilyHandle> cf_;
  std::string const cf_name_ = "cf_test";
  std::string const row_ = "row";
  std::string const column_ = "col";

  void SetUp() override {
    auto* unit_test = ::testing::UnitTest::GetInstance();
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

  void Populate(std::vector<StreamedCell> const& cells) {
    for (auto const& cell : cells) {
      auto status = db_->Put(
          rocksdb::WriteOptions(), cf_.get(),
          KeyCoder::Encode(cell.row, cell.column, cell.timestamp.count()),
          cell.value);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
  }

  InternalFilter ColumnRangeFilter(StringRangeSet::Range range) const {
    return ColumnRange{cf_name_, std::move(range)};
  }

  std::vector<StreamedCell> RunStream(
      std::vector<InternalFilter> const& filters = {},
      std::shared_ptr<StringRangeSet const> rows =
          std::make_shared<StringRangeSet>(StringRangeSet::All()),
      NextMode mode = NextMode::kCell) {
    CellStream stream(std::make_unique<FilteredPersistentColumnFamilyStream>(
        cf_, cf_name_, db_, std::move(rows),
        std::make_shared<rocksdb::ManagedSnapshot>(db_.get())));
    for (auto const& filter : filters) {
      EXPECT_TRUE(stream.ApplyFilter(filter));
    }

    std::vector<StreamedCell> cells;
    for (; stream; stream.Next(mode)) {
      cells.push_back({
          stream->row_key(),
          stream->column_qualifier(),
          stream->timestamp(),
          stream->value(),
      });
    }
    return cells;
  }
};

namespace {

std::vector<std::string> Rows(std::vector<StreamedCell> const& cells) {
  std::vector<std::string> res;
  res.reserve(cells.size());
  for (auto const& cell : cells) {
    res.push_back(cell.row);
  }
  return res;
}

std::vector<std::chrono::microseconds> Timestamps(
    std::vector<StreamedCell> const& cells) {
  std::vector<std::chrono::microseconds> res;
  res.reserve(cells.size());
  for (auto const& cell : cells) {
    res.push_back(cell.timestamp);
  }
  return res;
}

std::vector<InternalFilter> TimestampFilters(TimestampRangeSet const& ranges) {
  std::vector<InternalFilter> res;
  for (auto const& range : ranges.disjoint_ranges()) {
    res.emplace_back(TimestampRange{range});
  }
  return res;
}

std::vector<std::string> Columns(std::vector<StreamedCell> const& cells) {
  std::vector<std::string> res;
  res.reserve(cells.size());
  for (auto const& cell : cells) {
    res.push_back(cell.column);
  }
  return res;
}

}  // namespace

TEST_F(PersistentColumnFamilyStreamTest, RowRangeAll) {
  std::vector<StreamedCell> data{
      {"one", column_, 0_ms, "1"},
      {"two", column_, 0_ms, "2"},
      {"zero", column_, 0_ms, "0"},
  };
  Populate(data);

  auto filter = std::make_shared<StringRangeSet>(StringRangeSet::All());
  auto result = RunStream({}, filter);

  ASSERT_EQ(data, result);
}

TEST_F(PersistentColumnFamilyStreamTest, RowRangeEmpty) {
  std::vector<StreamedCell> data{
      {"one", column_, 0_ms, "1"},
      {"two", column_, 0_ms, "2"},
      {"zero", column_, 0_ms, "0"},
  };
  Populate(data);  // Populate, but filter is empty

  auto filter = std::make_shared<StringRangeSet>(StringRangeSet::Empty());
  auto result = RunStream({}, filter);

  ASSERT_TRUE(result.empty());
}

TEST_F(PersistentColumnFamilyStreamTest, RowRangeOpenExcludesBounds) {
  std::vector<StreamedCell> data{
      {"AA", column_, 0_ms, "0"},   {"AAA", column_, 0_ms, "0"},
      {"AAAa", column_, 0_ms, "0"}, {"AAAb", column_, 0_ms, "0"},
      {"AAB", column_, 0_ms, "0"},  {"AAC", column_, 0_ms, "0"},
  };
  Populate(data);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kOpen, "AAB", kOpen));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream({}, filter);
  std::vector<std::string> expected{"AAAa", "AAAb"};
  ASSERT_EQ(expected, Rows(result));
}

TEST_F(PersistentColumnFamilyStreamTest, RowRangeClosedIncludesBounds) {
  std::vector<StreamedCell> data{
      {"AA", column_, 0_ms, "0"},   {"AAA", column_, 0_ms, "0"},
      {"AAAa", column_, 0_ms, "0"}, {"AAAb", column_, 0_ms, "0"},
      {"AAB", column_, 0_ms, "0"},  {"AAC", column_, 0_ms, "0"},
  };
  Populate(data);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kClosed, "AAB", kClosed));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream({}, filter);
  std::vector<std::string> expected{"AAA", "AAAa", "AAAb", "AAB"};
  ASSERT_EQ(expected, Rows(result));
}

TEST_F(PersistentColumnFamilyStreamTest, RowRangeBoundsOutsideData) {
  std::vector<StreamedCell> data{
      {"AAAa", column_, 0_ms, "0"},
      {"AAAb", column_, 0_ms, "0"},
  };
  Populate(data);

  auto closed = StringRangeSet::Empty();
  closed.Sum(StringRangeSet::Range("AAA", kClosed, "AAB", kClosed));
  ASSERT_EQ(data, RunStream({}, std::make_shared<StringRangeSet>(closed)));

  auto open = StringRangeSet::Empty();
  open.Sum(StringRangeSet::Range("AAA", kOpen, "AAB", kOpen));
  ASSERT_EQ(data, RunStream({}, std::make_shared<StringRangeSet>(open)));
}

TEST_F(PersistentColumnFamilyStreamTest, RowRangeMultipleDisjoint) {
  std::vector<StreamedCell> data{
      {"AA", column_, 0_ms, "0"},   {"AAA", column_, 0_ms, "0"},
      {"AAAa", column_, 0_ms, "0"}, {"AAAb", column_, 0_ms, "0"},
      {"AAB", column_, 0_ms, "0"},  {"AAC", column_, 0_ms, "0"},
      {"BB", column_, 0_ms, "0"},   {"BBB", column_, 0_ms, "0"},
      {"BBBb", column_, 0_ms, "0"}, {"CCCa", column_, 0_ms, "0"},
      {"CCCb", column_, 0_ms, "0"}, {"CCD", column_, 0_ms, "0"},
      {"CCE", column_, 0_ms, "0"},
  };
  Populate(data);

  auto range_set = StringRangeSet::Empty();
  range_set.Sum(StringRangeSet::Range("AAA", kOpen, "AAB", kClosed));
  range_set.Sum(StringRangeSet::Range("BBB", kClosed, "BBC", kOpen));
  range_set.Sum(StringRangeSet::Range("CCC", kClosed, "CCD", kOpen));

  auto filter = std::make_shared<StringRangeSet>(range_set);
  auto result = RunStream({}, filter);
  std::vector<std::string> expected{
      "AAAa", "AAAb", "AAB", "BBB", "BBBb", "CCCa", "CCCb",
  };
  ASSERT_EQ(expected, Rows(result));
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRangeAll) {
  std::vector<StreamedCell> data{
      {row_, "one", 0_ms, "1"},
      {row_, "two", 0_ms, "2"},
      {row_, "zero", 0_ms, "0"},
  };
  Populate(data);

  auto result = RunStream({
      ColumnRangeFilter(StringRangeSet::Range(
          "", kClosed, StringRangeSet::Range::Infinity{}, kClosed)),
  });

  ASSERT_EQ(data, result);
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRangeEmpty) {
  std::vector<StreamedCell> data{
      {row_, "one", 0_ms, "1"},
      {row_, "two", 0_ms, "2"},
      {row_, "zero", 0_ms, "0"},
  };
  Populate(data);

  auto result = RunStream(
      {ColumnRangeFilter(StringRangeSet::Range("one", kClosed, "one", kOpen))});
  ASSERT_TRUE(result.empty());
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRangeOpenExcludesBounds) {
  std::vector<StreamedCell> data{
      {row_, "AA", 0_ms, "0"},   {row_, "AAA", 0_ms, "0"},
      {row_, "AAAa", 0_ms, "0"}, {row_, "AAAb", 0_ms, "0"},
      {row_, "AAB", 0_ms, "0"},  {row_, "AAC", 0_ms, "0"},
  };
  Populate(data);

  auto result = RunStream(
      {ColumnRangeFilter(StringRangeSet::Range("AAA", kOpen, "AAB", kOpen))});
  std::vector<std::string> expected{"AAAa", "AAAb"};
  ASSERT_EQ(expected, Columns(result));
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRangeClosedIncludesBounds) {
  std::vector<StreamedCell> data{
      {row_, "AA", 0_ms, "0"},   {row_, "AAA", 0_ms, "0"},
      {row_, "AAAa", 0_ms, "0"}, {row_, "AAAb", 0_ms, "0"},
      {row_, "AAB", 0_ms, "0"},  {row_, "AAC", 0_ms, "0"},
  };
  Populate(data);

  auto result = RunStream({
      ColumnRangeFilter(StringRangeSet::Range("AAA", kClosed, "AAB", kClosed)),
  });
  std::vector<std::string> expected{"AAA", "AAAa", "AAAb", "AAB"};
  ASSERT_EQ(expected, Columns(result));
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRangeBoundsOutsideData) {
  std::vector<StreamedCell> data{
      {row_, "AAAa", 0_ms, "0"},
      {row_, "AAAb", 0_ms, "0"},
  };
  Populate(data);

  ASSERT_EQ(data, RunStream({ColumnRangeFilter(
                      StringRangeSet::Range("AAA", kClosed, "AAB", kClosed))}));
  ASSERT_EQ(data, RunStream({ColumnRangeFilter(
                      StringRangeSet::Range("AAA", kOpen, "AAB", kOpen))}));
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRangesIntersect) {
  std::vector<StreamedCell> data{
      {row_, "AA", 0_ms, "0"},   {row_, "AAA", 0_ms, "0"},
      {row_, "AAAa", 0_ms, "0"}, {row_, "AAAb", 0_ms, "0"},
      {row_, "AAB", 0_ms, "0"},  {row_, "AAC", 0_ms, "0"},
  };
  Populate(data);

  auto result = RunStream({
      ColumnRangeFilter(StringRangeSet::Range("AA", kClosed, "AAB", kOpen)),
      ColumnRangeFilter(StringRangeSet::Range("AAAa", kClosed, "AAC", kClosed)),
  });
  std::vector<std::string> expected{"AAAa", "AAAb"};
  ASSERT_EQ(expected, Columns(result));
}

TEST_F(PersistentColumnFamilyStreamTest, TimestampRangeAll) {
  std::vector<StreamedCell> data{
      {row_, column_, 2_ms, "2"},
      {row_, column_, 1_ms, "1"},
      {row_, column_, 0_ms, "0"},
  };
  Populate(data);

  auto result = RunStream(TimestampFilters(TimestampRangeSet::All()));
  ASSERT_EQ(std::vector<std::chrono::microseconds>({2_ms, 1_ms, 0_ms}),
            Timestamps(result));
}

TEST_F(PersistentColumnFamilyStreamTest, TimestampRangeEmpty) {
  std::vector<StreamedCell> data{
      {row_, column_, 2_ms, "2"},
      {row_, column_, 1_ms, "1"},
      {row_, column_, 0_ms, "0"},
  };
  Populate(data);

  auto result =
      RunStream({TimestampRange{TimestampRangeSet::Range(1_ms, 1_ms)}});
  ASSERT_EQ(std::vector<std::chrono::microseconds>(), Timestamps(result));
}

TEST_F(PersistentColumnFamilyStreamTest, TimestampRangeFiniteEndExclusive) {
  std::vector<StreamedCell> data{
      {row_, column_, 4_ms, "0"}, {row_, column_, 3_ms, "0"},
      {row_, column_, 2_ms, "0"}, {row_, column_, 1_ms, "0"},
      {row_, column_, 0_ms, "0"},
  };
  Populate(data);

  auto ts_set = TimestampRangeSet::Empty();
  ts_set.Sum(TimestampRangeSet::Range(1_ms, 3_ms));
  auto result = RunStream(TimestampFilters(ts_set));
  ASSERT_EQ(std::vector<std::chrono::microseconds>({2_ms, 1_ms}),
            Timestamps(result));
}

TEST_F(PersistentColumnFamilyStreamTest, TimestampRangeUnboundedEnd) {
  std::vector<StreamedCell> data{
      {row_, column_, 4_ms, "0"}, {row_, column_, 3_ms, "0"},
      {row_, column_, 2_ms, "0"}, {row_, column_, 1_ms, "0"},
      {row_, column_, 0_ms, "0"},
  };
  Populate(data);

  auto ts_set = TimestampRangeSet::Empty();
  ts_set.Sum(TimestampRangeSet::Range(1_ms, 0_ms));
  auto result = RunStream(TimestampFilters(ts_set));
  ASSERT_EQ(std::vector<std::chrono::microseconds>({4_ms, 3_ms, 2_ms, 1_ms}),
            Timestamps(result));
}

TEST_F(PersistentColumnFamilyStreamTest, TimestampRangesAdjacentMerge) {
  std::vector<StreamedCell> data{
      {row_, column_, 15_ms, "0"}, {row_, column_, 14_ms, "0"},
      {row_, column_, 13_ms, "0"}, {row_, column_, 12_ms, "0"},
      {row_, column_, 11_ms, "0"}, {row_, column_, 10_ms, "0"},
      {row_, column_, 9_ms, "0"},  {row_, column_, 8_ms, "0"},
      {row_, column_, 7_ms, "0"},  {row_, column_, 6_ms, "0"},
      {row_, column_, 5_ms, "0"},  {row_, column_, 4_ms, "0"},
      {row_, column_, 3_ms, "0"},  {row_, column_, 2_ms, "0"},
      {row_, column_, 1_ms, "0"},  {row_, column_, 0_ms, "0"},
  };
  Populate(data);

  auto ts_set = TimestampRangeSet::Empty();
  ts_set.Sum(TimestampRangeSet::Range(1_ms, 3_ms));
  ts_set.Sum(TimestampRangeSet::Range(3_ms, 5_ms));
  ts_set.Sum(TimestampRangeSet::Range(5_ms, 8_ms));

  auto result = RunStream(TimestampFilters(ts_set));
  ASSERT_EQ(std::vector<std::chrono::microseconds>(
                {7_ms, 6_ms, 5_ms, 4_ms, 3_ms, 2_ms, 1_ms}),
            Timestamps(result));
}

TEST_F(PersistentColumnFamilyStreamTest, TimestampFiltersIntersect) {
  std::vector<StreamedCell> data{
      {row_, column_, 15_ms, "0"}, {row_, column_, 14_ms, "0"},
      {row_, column_, 13_ms, "0"}, {row_, column_, 12_ms, "0"},
      {row_, column_, 11_ms, "0"}, {row_, column_, 10_ms, "0"},
      {row_, column_, 9_ms, "0"},  {row_, column_, 8_ms, "0"},
      {row_, column_, 7_ms, "0"},  {row_, column_, 6_ms, "0"},
      {row_, column_, 5_ms, "0"},  {row_, column_, 4_ms, "0"},
      {row_, column_, 3_ms, "0"},  {row_, column_, 2_ms, "0"},
      {row_, column_, 1_ms, "0"},  {row_, column_, 0_ms, "0"},
  };
  Populate(data);

  auto ts_set = TimestampRangeSet::Empty();
  ts_set.Sum(TimestampRangeSet::Range(1_ms, 3_ms));
  ts_set.Sum(TimestampRangeSet::Range(3_ms, 5_ms));
  ts_set.Sum(TimestampRangeSet::Range(6_ms, 8_ms));

  auto result = RunStream(TimestampFilters(ts_set));
  ASSERT_EQ(std::vector<std::chrono::microseconds>({}), Timestamps(result));
}

TEST_F(PersistentColumnFamilyStreamTest, RowRegexNoMatch) {
  auto pattern = std::make_shared<re2::RE2>("this_will_not_be_matched");
  ASSERT_TRUE(pattern->ok());

  std::vector<StreamedCell> data{
      {"one", column_, 0_ms, "1"},
      {"two", column_, 0_ms, "2"},
      {"zero", column_, 0_ms, "0"},
  };
  Populate(data);

  auto result = RunStream({RowKeyRegex{std::move(pattern)}});
  ASSERT_EQ(std::vector<std::string>(), Rows(result));
}

TEST_F(PersistentColumnFamilyStreamTest, RowRegexSingle) {
  auto pattern = std::make_shared<re2::RE2>("^[a-z_]*$");
  ASSERT_TRUE(pattern->ok());

  std::vector<StreamedCell> data{
      {"NO_MATCH", column_, 0_ms, "0"},
      {"another_match", column_, 0_ms, "2"},
      {"match", column_, 0_ms, "1"},
  };
  Populate(data);

  auto result = RunStream({RowKeyRegex{std::move(pattern)}});
  ASSERT_EQ(std::vector<std::string>({"another_match", "match"}), Rows(result));
}

TEST_F(PersistentColumnFamilyStreamTest, RowRegexesAllMustMatch) {
  auto has_a = std::make_shared<re2::RE2>("a");
  ASSERT_TRUE(has_a->ok());
  auto has_b = std::make_shared<re2::RE2>("b");
  ASSERT_TRUE(has_b->ok());
  auto has_c = std::make_shared<re2::RE2>("c");
  ASSERT_TRUE(has_c->ok());

  std::vector<StreamedCell> data{
      {"QQ b QQ c QQ a QQ", column_, 0_ms, "4"},
      {"a", column_, 0_ms, "2"},
      {"ab", column_, 0_ms, "1"},
      {"abc", column_, 0_ms, "0"},
      {"ac", column_, 0_ms, "5"},
  };
  Populate(data);

  auto result = RunStream({
      RowKeyRegex{std::move(has_a)},
      RowKeyRegex{std::move(has_b)},
      RowKeyRegex{std::move(has_c)},
  });
  ASSERT_EQ(std::vector<std::string>({"QQ b QQ c QQ a QQ", "abc"}),
            Rows(result));
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRegexNoMatch) {
  auto pattern = std::make_shared<re2::RE2>("this_will_not_be_matched");
  ASSERT_TRUE(pattern->ok());

  std::vector<StreamedCell> data{
      {row_, "one", 0_ms, "1"},
      {row_, "two", 0_ms, "2"},
      {row_, "zero", 0_ms, "0"},
  };
  Populate(data);

  auto result = RunStream({ColumnRegex{std::move(pattern)}});
  ASSERT_EQ(std::vector<std::string>(), Columns(result));
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRegexSingle) {
  auto pattern = std::make_shared<re2::RE2>("^[a-z_]*$");
  ASSERT_TRUE(pattern->ok());

  std::vector<StreamedCell> data{
      {row_, "NO_MATCH", 0_ms, "0"},
      {row_, "another_match", 0_ms, "2"},
      {row_, "match", 0_ms, "1"},
  };
  Populate(data);

  auto result = RunStream({ColumnRegex{std::move(pattern)}});
  ASSERT_EQ(std::vector<std::string>({"another_match", "match"}),
            Columns(result));
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRegexesAllMustMatch) {
  auto has_a = std::make_shared<re2::RE2>("a");
  ASSERT_TRUE(has_a->ok());
  auto has_b = std::make_shared<re2::RE2>("b");
  ASSERT_TRUE(has_b->ok());
  auto has_c = std::make_shared<re2::RE2>("c");
  ASSERT_TRUE(has_c->ok());

  std::vector<StreamedCell> data{
      {row_, "QQ b QQ c QQ a QQ", 0_ms, "4"},
      {row_, "a", 0_ms, "2"},
      {row_, "ab", 0_ms, "1"},
      {row_, "abc", 0_ms, "0"},
      {row_, "ac", 0_ms, "5"},
  };
  Populate(data);

  auto result = RunStream({
      ColumnRegex{std::move(has_a)},
      ColumnRegex{std::move(has_b)},
      ColumnRegex{std::move(has_c)},
  });
  ASSERT_EQ(std::vector<std::string>({"QQ b QQ c QQ a QQ", "abc"}),
            Columns(result));
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRegexAppliesToEmptyQualifier) {
  auto non_empty = std::make_shared<re2::RE2>(".+");
  ASSERT_TRUE(non_empty->ok());

  std::vector<StreamedCell> data{
      {row_, "", 0_ms, "0"},
      {row_, "a", 0_ms, "1"},
      {row_, "b", 0_ms, "2"},
  };
  Populate(data);

  auto result = RunStream({ColumnRegex{std::move(non_empty)}});
  ASSERT_EQ(std::vector<std::string>({"a", "b"}), Columns(result));
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRangeResetsEveryRow) {
  std::vector<StreamedCell> data;
  for (auto const* row : {"r1", "r2"}) {
    for (auto const* column : {"a", "b", "c"}) {
      data.push_back({row, column, 0_ms, "v"});
    }
  }
  Populate(data);

  auto result = RunStream(
      {ColumnRangeFilter(StringRangeSet::Range("b", kClosed, "b", kClosed))});
  std::vector<StreamedCell> expected{
      {"r1", "b", 0_ms, "v"},
      {"r2", "b", 0_ms, "v"},
  };
  ASSERT_EQ(expected, result);
}

TEST_F(PersistentColumnFamilyStreamTest,
       TimestampRangeResetsEveryColumnAndRow) {
  std::vector<StreamedCell> data;
  for (auto const* row : {"r1", "r2"}) {
    for (auto const* column : {"a", "b"}) {
      for (auto ts : {1_ms, 2_ms, 3_ms}) {
        data.push_back({row, column, ts, "v"});
      }
    }
  }
  Populate(data);

  auto result =
      RunStream({TimestampRange{TimestampRangeSet::Range(2_ms, 3_ms)}});
  std::vector<StreamedCell> expected{
      {"r1", "a", 2_ms, "v"},
      {"r1", "b", 2_ms, "v"},
      {"r2", "a", 2_ms, "v"},
      {"r2", "b", 2_ms, "v"},
  };
  ASSERT_EQ(expected, result);
}

TEST_F(PersistentColumnFamilyStreamTest, ColumnRangeSeekStopsAtRowRangeEnd) {
  std::vector<StreamedCell> data;
  for (auto const* row : {"a", "b", "c"}) {
    for (auto const* column : {"x", "y"}) {
      data.push_back({row, column, 0_ms, "v"});
    }
  }
  Populate(data);

  auto rows = StringRangeSet::Empty();
  rows.Sum(StringRangeSet::Range("a", kClosed, "b", kClosed));
  auto result = RunStream(
      {ColumnRangeFilter(StringRangeSet::Range("x", kClosed, "x", kClosed))},
      std::make_shared<StringRangeSet>(rows));
  std::vector<StreamedCell> expected{
      {"a", "x", 0_ms, "v"},
      {"b", "x", 0_ms, "v"},
  };
  ASSERT_EQ(expected, result);
}

TEST_F(PersistentColumnFamilyStreamTest, NextColumnSkipsRemainingTimestamps) {
  std::vector<StreamedCell> data;
  for (auto const* column : {"a", "b"}) {
    for (auto ts : {1_ms, 2_ms, 3_ms}) {
      data.push_back({row_, column, ts, "v"});
    }
  }
  Populate(data);

  auto result =
      RunStream({TimestampRange{TimestampRangeSet::Range(1_ms, 3_ms)}},
                std::make_shared<StringRangeSet>(StringRangeSet::All()),
                NextMode::kColumn);
  std::vector<StreamedCell> expected{
      {row_, "a", 2_ms, "v"},
      {row_, "b", 2_ms, "v"},
  };
  ASSERT_EQ(expected, result);
}

TEST_F(PersistentColumnFamilyStreamTest, NextRowSkipsRemainingColumns) {
  std::vector<StreamedCell> data;
  for (auto const* row : {"r1", "r2"}) {
    for (auto const* column : {"a", "b", "c"}) {
      data.push_back({row, column, 0_ms, "v"});
    }
  }
  Populate(data);

  auto result = RunStream(
      {ColumnRangeFilter(StringRangeSet::Range("b", kClosed, "c", kClosed))},
      std::make_shared<StringRangeSet>(StringRangeSet::All()), NextMode::kRow);
  std::vector<StreamedCell> expected{
      {"r1", "b", 0_ms, "v"},
      {"r2", "b", 0_ms, "v"},
  };
  ASSERT_EQ(expected, result);
}

TEST_F(PersistentColumnFamilyStreamTest, SeeksRespectEscapedKeys) {
  std::string const a0("a\0", 2);
  std::string const c0("c\0", 2);
  std::vector<StreamedCell> data;
  for (auto const& row : {std::string("a"), a0, std::string("ab")}) {
    for (auto const& column : {std::string("c"), c0, std::string("cd")}) {
      for (auto ts : {1_ms, 2_ms}) {
        data.push_back({row, column, ts, "v"});
      }
    }
  }
  Populate(data);

  auto all_rows = std::make_shared<StringRangeSet>(StringRangeSet::All());

  std::vector<StreamedCell> expected_columns;
  for (auto const& row : {std::string("a"), a0, std::string("ab")}) {
    for (auto const& column : {std::string("c"), c0, std::string("cd")}) {
      expected_columns.push_back({row, column, 2_ms, "v"});
    }
  }
  ASSERT_EQ(expected_columns, RunStream({}, all_rows, NextMode::kColumn));

  std::vector<StreamedCell> expected_rows{
      {"a", "c", 2_ms, "v"},
      {a0, "c", 2_ms, "v"},
      {"ab", "c", 2_ms, "v"},
  };
  ASSERT_EQ(expected_rows, RunStream({}, all_rows, NextMode::kRow));
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
