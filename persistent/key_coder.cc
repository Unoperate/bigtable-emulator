#include "persistent/key_coder.h"
#include "google/cloud/internal/big_endian.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include <cstdint>
#include <string_view>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

namespace {
using ::std::string_view_literals::operator""sv;

constexpr std::string_view kZero = "\x00"sv;
constexpr std::string_view kEscapedZero = "\x00\xFF"sv;
constexpr std::string_view kSeparator = "\x00\x01"sv;
constexpr std::string_view kAfterSeparator = "\x00\x02"sv;

uint64_t InvertTimestamp(uint64_t timestamp) { return ~timestamp; }

std::string Escape(std::string_view to_be_escaped) {
  return absl::StrReplaceAll(to_be_escaped, {{kZero, kEscapedZero}});
}

StatusOr<std::string> Unescape(std::string_view field,
                               std::string_view full_key) {
  if (absl::StrContains(absl::StrReplaceAll(field, {{kEscapedZero, ""}}),
                        kZero)) {
    return InternalError(
        "Failed to decode key: invalid escape",
        GCP_ERROR_INFO().WithMetadata("key", absl::CHexEscape(full_key)));
  }
  return absl::StrReplaceAll(field, {{kEscapedZero, kZero}});
}
}  // anonymous namespace

std::string KeyCoder::RowRangeStartKey(StringRangeSet::Range const& range) {
  return range.start_open() ? NextRow(range.start_finite())
                            : EscapedRowWithSeparator(range.start_finite());
}

std::string KeyCoder::ColumnRangeStartKey(std::string_view row,
                                          StringRangeSet::Range const& range) {
  return range.start_open()
             ? NextColumn(row, range.start_finite())
             : EscapedRowColWithSeparator(row, range.start_finite());
}

std::string KeyCoder::Encode(std::string_view row, std::string_view col,
                             uint64_t timestamp) {
  return EscapedRowColWithSeparator(row, col) +
         google::cloud::internal::EncodeBigEndian(InvertTimestamp(timestamp));
}

std::string KeyCoder::EscapedRowColWithSeparator(std::string_view row,
                                                 std::string_view col) {
  return absl::StrCat(Escape(row), kSeparator, Escape(col), kSeparator);
}

std::string KeyCoder::NextRow(std::string_view row) {
  return absl::StrCat(Escape(row), kAfterSeparator);
}

std::string KeyCoder::NextColumn(std::string_view row, std::string_view col) {
  return absl::StrCat(Escape(row), kSeparator, Escape(col), kAfterSeparator);
}

std::string KeyCoder::EscapedRowWithSeparator(std::string_view row) {
  return absl::StrCat(Escape(row), kSeparator);
}

std::string KeyCoder::EscapedRow(std::string_view row) { return Escape(row); }

StatusOr<DecodeResult> KeyCoder::Decode(std::string_view full_key) {
  std::vector<std::string_view> fields =
      absl::StrSplit(full_key, absl::MaxSplits(absl::ByString(kSeparator), 2));
  if (fields.size() != 3) {
    return InternalError(
        "Failed to decode key: missing field terminator",
        GCP_ERROR_INFO().WithMetadata("key", absl::CHexEscape(full_key)));
  }

  auto row = Unescape(fields[0], full_key);
  if (!row.ok()) {
    return row.status();
  }
  auto col = Unescape(fields[1], full_key);
  if (!col.ok()) {
    return col.status();
  }

  auto be_ts = google::cloud::internal::DecodeBigEndian<std::uint64_t>(
      std::string(fields[2]));
  if (!be_ts) {
    return InternalError(
        "Failed to decode timestamp: " + be_ts.status().message(),
        GCP_ERROR_INFO().WithMetadata("key", absl::CHexEscape(full_key)));
  }

  return DecodeResult{
      *std::move(row),
      *std::move(col),
      InvertTimestamp(*be_ts),
  };
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
