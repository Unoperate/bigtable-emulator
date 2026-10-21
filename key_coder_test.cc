#include "key_coder.h"
#include "google/cloud/testing_util/status_matchers.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <string>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

using ::google::cloud::testing_util::StatusIs;
using ::testing::HasSubstr;

TEST(KeyCoderTest, BasicBackAndFourth) {
  std::string row = "user_001";
  std::string col = "stats";
  uint64_t ts = 1678888888000;

  auto encoded = KeyCoder::Encode(row, col, ts);
  auto decoded = KeyCoder::Decode(encoded);

  ASSERT_STATUS_OK(decoded);
  EXPECT_EQ(decoded->row, row);
  EXPECT_EQ(decoded->col, col);
  EXPECT_EQ(decoded->timestamp, ts);
}

TEST(KeyCoderTest, NullBytesHandling) {
  std::string row = std::string("user\0name", 9);
  std::string col = std::string("meta\0data", 9);
  uint64_t ts = 1000;

  auto encoded = KeyCoder::Encode(row, col, ts);
  auto decoded = KeyCoder::Decode(encoded);

  ASSERT_STATUS_OK(decoded);
  EXPECT_EQ(decoded->row, row);
  EXPECT_EQ(decoded->col, col);
}

TEST(KeyCoderTest, KeySortingWorks) {
  auto key_old = KeyCoder::Encode("row1", "col1", 1000);  // Older
  auto key_new = KeyCoder::Encode("row1", "col1", 2000);  // Newer

  EXPECT_LT(key_new, key_old);

  auto row_a = KeyCoder::Encode("a", "col", 1000);
  auto row_b = KeyCoder::Encode("b", "col", 1000);
  EXPECT_LT(row_a, row_b);
}

TEST(KeyCoderTest, PartialEncodeMatchesPrefix) {
  std::string row = "row_key";
  std::string col = "col_qual";
  uint64_t ts = 12345;

  auto full = KeyCoder::Encode(row, col, ts);
  auto partial = KeyCoder::PartialEncode(row, col);
  EXPECT_THAT(full, testing::StartsWith(partial));
}

TEST(KeyCoderTest, RejectInvalidKeys) {
  std::string bad_key = "row\0\xFFcol\0\xFF";
  auto res = KeyCoder::Decode(bad_key);
  EXPECT_THAT(
      res, StatusIs(StatusCode::kInternal, HasSubstr("Failed to decode key")));

  std::string short_ts = bad_key + "FFFF";
  res = KeyCoder::Decode(short_ts);
  EXPECT_THAT(
      res, StatusIs(StatusCode::kInternal, HasSubstr("Failed to decode key")));

  std::string broken_esc = "row\0";
  res = KeyCoder::Decode(broken_esc);
  EXPECT_THAT(
      res, StatusIs(StatusCode::kInternal, HasSubstr("Failed to decode key")));
}

TEST(KeyCoderTest, RejectNonHexTimestamp) {
  std::string manual_key = "";
  manual_key.append("row");
  manual_key.push_back('\0');
  manual_key.push_back('\xFF');
  manual_key.append("col");
  manual_key.push_back('\0');
  manual_key.push_back('\xFF');
  manual_key.append("ZZZZZZZZZZZZZZZZ");

  auto res = KeyCoder::Decode(manual_key);
  EXPECT_THAT(
      res, StatusIs(StatusCode::kInternal, HasSubstr("Failed to decode key")));
}

}  // namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google