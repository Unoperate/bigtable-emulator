#include "persistent/key_coder.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <limits>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

using ::google::cloud::testing_util::StatusIs;
using ::std::string_literals::operator""s;

std::string const kTrickyStrings[] = {""s,       "\0"s,    "\0\0"s,  "\0\x01"s,
                                      "\0\xFF"s, "\x01"s,  "\xFF"s,  "a"s,
                                      "a\0"s,    "a\x01"s, "a\xFF"s, "a!"s,
                                      "ab"s,     "row1"s,  "row10"s, "row2"s};

constexpr uint64_t kTrickyTimestamps[] = {0, 1, 1000,
                                          std::numeric_limits<uint64_t>::max()};

struct KeyParts {
  std::string row;
  std::string col;
  uint64_t ts;

  bool operator<(KeyParts const& other) const {
    if (row != other.row) {
      return row < other.row;
    }
    if (col != other.col) {
      return col < other.col;
    }
    return ts > other.ts;
  }
};

std::vector<KeyParts> AllKeyParts() {
  std::vector<KeyParts> key_parts;
  for (auto const& row : kTrickyStrings) {
    for (auto const& col : kTrickyStrings) {
      for (auto ts : kTrickyTimestamps) {
        key_parts.push_back({row, col, ts});
      }
    }
  }
  return key_parts;
}

StringRangeSet::Range RangeStartingAt(std::string start) {
  return StringRangeSet::Range(std::move(start), false,
                               StringRangeSet::Range::Infinity{}, false);
}

StringRangeSet::Range RangeStartingAfter(std::string start) {
  return StringRangeSet::Range(std::move(start), true,
                               StringRangeSet::Range::Infinity{}, false);
}

std::string ColumnKeyWithTimestampBytes(std::size_t n) {
  return KeyCoder::ColumnPrefix("row", "col") + std::string(n, '\x42');
}

TEST(KeyCoderTest, EncodeBytes) {
  EXPECT_EQ(KeyCoder::Encode("r", "c", 0),
            "r\0\x01"s + "c\0\x01"s + std::string(8, '\xFF'));
  EXPECT_EQ(
      KeyCoder::Encode("a\0b"s, "", 1),
      "a\0\xFF"s + "b\0\x01"s + "\0\x01"s + std::string(7, '\xFF') + "\xFE"s);
  EXPECT_EQ(KeyCoder::Encode("", "", std::numeric_limits<uint64_t>::max()),
            "\0\x01\0\x01"s + std::string(8, '\0'));
}

TEST(KeyCoderTest, RowPrefixBytes) {
  EXPECT_EQ(KeyCoder::RowPrefix(""), "\0\x01"s);
  EXPECT_EQ(KeyCoder::RowPrefix("a"), "a\0\x01"s);
  EXPECT_EQ(KeyCoder::RowPrefix("\0"s), "\0\xFF\0\x01"s);
}

TEST(KeyCoderTest, ColumnPrefixBytes) {
  EXPECT_EQ(KeyCoder::ColumnPrefix("a", "b"), "a\0\x01"s + "b\0\x01"s);
  EXPECT_EQ(KeyCoder::ColumnPrefix("a", ""), "a\0\x01\0\x01"s);
}

TEST(KeyCoderTest, NextRowBytes) {
  EXPECT_EQ(KeyCoder::NextRow("a"), "a\0\x02"s);
}

TEST(KeyCoderTest, NextColumnBytes) {
  EXPECT_EQ(KeyCoder::NextColumn("a", "b"), "a\0\x01"s + "b\0\x02"s);
}

TEST(KeyCoderTest, RowRangeStartKeyBytes) {
  EXPECT_EQ(KeyCoder::RowRangeStartKey(RangeStartingAt("a")), "a"s);
  EXPECT_EQ(KeyCoder::RowRangeStartKey(RangeStartingAt("a\0"s)), "a\0\xFF"s);
  EXPECT_EQ(KeyCoder::RowRangeStartKey(RangeStartingAfter("a")), "a\0\x02"s);
}

TEST(KeyCoderTest, ColumnRangeStartKeyBytes) {
  EXPECT_EQ(KeyCoder::ColumnRangeStartKey("r", RangeStartingAt("c")),
            "r\0\x01"s + "c\0\x01"s);
  EXPECT_EQ(KeyCoder::ColumnRangeStartKey("r", RangeStartingAfter("c")),
            "r\0\x01"s + "c\0\x02"s);
}

TEST(KeyCoderTest, RoundTrip) {
  for (auto const& parts : AllKeyParts()) {
    auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
    auto decoded = KeyCoder::Decode(key);
    ASSERT_STATUS_OK(decoded) << absl::CHexEscape(key);
    EXPECT_EQ(decoded->row, parts.row) << absl::CHexEscape(key);
    EXPECT_EQ(decoded->col, parts.col) << absl::CHexEscape(key);
    EXPECT_EQ(decoded->timestamp, parts.ts) << absl::CHexEscape(key);
  }
}

TEST(KeyCoderTest, OrderMatchesTupleOrder) {
  auto const key_parts = AllKeyParts();
  for (auto const& a : key_parts) {
    auto const a_key = KeyCoder::Encode(a.row, a.col, a.ts);
    for (auto const& b : key_parts) {
      auto const b_key = KeyCoder::Encode(b.row, b.col, b.ts);
      if (a < b) {
        EXPECT_LT(a_key, b_key)
            << absl::CHexEscape(a_key) << " vs " << absl::CHexEscape(b_key);
      } else {
        EXPECT_GE(a_key, b_key)
            << absl::CHexEscape(a_key) << " vs " << absl::CHexEscape(b_key);
      }
    }
  }
}

TEST(KeyCoderTest, RowPrefixMatchesOnlyItsRow) {
  auto const key_parts = AllKeyParts();
  for (auto const& row : kTrickyStrings) {
    auto const prefix = KeyCoder::RowPrefix(row);
    for (auto const& parts : key_parts) {
      auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
      if (parts.row == row) {
        EXPECT_TRUE(absl::StartsWith(key, prefix))
            << absl::CHexEscape(prefix) << " vs " << absl::CHexEscape(key);
      } else {
        EXPECT_FALSE(absl::StartsWith(key, prefix))
            << absl::CHexEscape(prefix) << " vs " << absl::CHexEscape(key);
      }
    }
  }
}

TEST(KeyCoderTest, EscapedRowMatchesRowsWithPrefix) {
  auto const key_parts = AllKeyParts();
  for (auto const& row_prefix : kTrickyStrings) {
    auto const prefix = KeyCoder::EscapedRow(row_prefix);
    for (auto const& parts : key_parts) {
      auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
      if (absl::StartsWith(parts.row, row_prefix)) {
        EXPECT_TRUE(absl::StartsWith(key, prefix))
            << absl::CHexEscape(prefix) << " vs " << absl::CHexEscape(key);
      } else {
        EXPECT_FALSE(absl::StartsWith(key, prefix))
            << absl::CHexEscape(prefix) << " vs " << absl::CHexEscape(key);
      }
    }
  }
}

TEST(KeyCoderTest, ColumnPrefixMatchesOnlyItsColumn) {
  auto const key_parts = AllKeyParts();
  for (auto const& row : kTrickyStrings) {
    for (auto const& col : kTrickyStrings) {
      auto const prefix = KeyCoder::ColumnPrefix(row, col);
      for (auto const& parts : key_parts) {
        auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
        if (parts.row == row && parts.col == col) {
          EXPECT_TRUE(absl::StartsWith(key, prefix))
              << absl::CHexEscape(prefix) << " vs " << absl::CHexEscape(key);
        } else {
          EXPECT_FALSE(absl::StartsWith(key, prefix))
              << absl::CHexEscape(prefix) << " vs " << absl::CHexEscape(key);
        }
      }
    }
  }
}

TEST(KeyCoderTest, NextRowBoundsRow) {
  auto const key_parts = AllKeyParts();
  for (auto const& row : kTrickyStrings) {
    auto const bound = KeyCoder::NextRow(row);
    for (auto const& parts : key_parts) {
      auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
      if (parts.row <= row) {
        EXPECT_LT(key, bound)
            << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
      } else {
        EXPECT_LE(bound, key)
            << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
      }
    }
  }
}

TEST(KeyCoderTest, NextColumnBoundsColumn) {
  auto const key_parts = AllKeyParts();
  for (auto const& row : kTrickyStrings) {
    for (auto const& col : kTrickyStrings) {
      auto const bound = KeyCoder::NextColumn(row, col);
      for (auto const& parts : key_parts) {
        auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
        if (std::tie(parts.row, parts.col) <= std::tie(row, col)) {
          EXPECT_LT(key, bound)
              << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
        } else {
          EXPECT_LE(bound, key)
              << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
        }
      }
    }
  }
}

TEST(KeyCoderTest, RowRangeStartKeyBoundsClosedRange) {
  auto const key_parts = AllKeyParts();
  for (auto const& start : kTrickyStrings) {
    auto const bound = KeyCoder::RowRangeStartKey(RangeStartingAt(start));
    for (auto const& parts : key_parts) {
      auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
      if (parts.row < start) {
        EXPECT_LT(key, bound)
            << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
      } else {
        EXPECT_LE(bound, key)
            << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
      }
    }
  }
}

TEST(KeyCoderTest, RowRangeStartKeyBoundsOpenRange) {
  auto const key_parts = AllKeyParts();
  for (auto const& start : kTrickyStrings) {
    auto const bound = KeyCoder::RowRangeStartKey(RangeStartingAfter(start));
    for (auto const& parts : key_parts) {
      auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
      if (parts.row <= start) {
        EXPECT_LT(key, bound)
            << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
      } else {
        EXPECT_LE(bound, key)
            << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
      }
    }
  }
}

TEST(KeyCoderTest, ColumnRangeStartKeyBoundsClosedRange) {
  auto const key_parts = AllKeyParts();
  for (auto const& row : kTrickyStrings) {
    for (auto const& start : kTrickyStrings) {
      auto const bound =
          KeyCoder::ColumnRangeStartKey(row, RangeStartingAt(start));
      for (auto const& parts : key_parts) {
        auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
        if (std::tie(parts.row, parts.col) < std::tie(row, start)) {
          EXPECT_LT(key, bound)
              << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
        } else {
          EXPECT_LE(bound, key)
              << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
        }
      }
    }
  }
}

TEST(KeyCoderTest, ColumnRangeStartKeyBoundsOpenRange) {
  auto const key_parts = AllKeyParts();
  for (auto const& row : kTrickyStrings) {
    for (auto const& start : kTrickyStrings) {
      auto const bound =
          KeyCoder::ColumnRangeStartKey(row, RangeStartingAfter(start));
      for (auto const& parts : key_parts) {
        auto const key = KeyCoder::Encode(parts.row, parts.col, parts.ts);
        if (std::tie(parts.row, parts.col) <= std::tie(row, start)) {
          EXPECT_LT(key, bound)
              << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
        } else {
          EXPECT_LE(bound, key)
              << absl::CHexEscape(bound) << " vs " << absl::CHexEscape(key);
        }
      }
    }
  }
}

TEST(KeyCoderTest, DecodeRejectsEmpty) {
  EXPECT_THAT(KeyCoder::Decode(""), StatusIs(StatusCode::kInternal));
}

TEST(KeyCoderTest, DecodeRejectsMissingTerminator) {
  EXPECT_THAT(KeyCoder::Decode("row"), StatusIs(StatusCode::kInternal));
}

TEST(KeyCoderTest, DecodeRejectsTrailingSeparator) {
  EXPECT_THAT(KeyCoder::Decode("row\0"s), StatusIs(StatusCode::kInternal));
}

TEST(KeyCoderTest, DecodeRejectsInvalidEscape) {
  EXPECT_THAT(
      KeyCoder::Decode("a\0\x02"s + "col\0\x01"s + std::string(8, '\0')),
      StatusIs(StatusCode::kInternal));
}

TEST(KeyCoderTest, DecodeRejectsMissingColumn) {
  EXPECT_THAT(KeyCoder::Decode("row\0\x01"s), StatusIs(StatusCode::kInternal));
}

TEST(KeyCoderTest, DecodeRejectsMissingTimestamp) {
  EXPECT_THAT(KeyCoder::Decode(ColumnKeyWithTimestampBytes(0)),
              StatusIs(StatusCode::kInternal));
}

TEST(KeyCoderTest, DecodeAcceptsEightByteTimestamp) {
  auto decoded = KeyCoder::Decode(ColumnKeyWithTimestampBytes(8));
  ASSERT_STATUS_OK(decoded);
  EXPECT_EQ(decoded->row, "row");
  EXPECT_EQ(decoded->col, "col");
  EXPECT_EQ(decoded->timestamp, ~std::uint64_t{0x4242424242424242});
}

TEST(KeyCoderTest, DecodeRejectsShortTimestamp) {
  EXPECT_THAT(KeyCoder::Decode(ColumnKeyWithTimestampBytes(7)),
              StatusIs(StatusCode::kInternal));
}

TEST(KeyCoderTest, DecodeRejectsLongTimestamp) {
  EXPECT_THAT(KeyCoder::Decode(ColumnKeyWithTimestampBytes(9)),
              StatusIs(StatusCode::kInternal));
}

}  // namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
