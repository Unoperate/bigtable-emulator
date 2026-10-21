#include "key_coder.h"
#include <charconv>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

std::string KeyCoder::Encode(std::string const& row, std::string const& col,
                             uint64_t timestamp) {
  std::string buffer;
  AppendEscaped(buffer, row);
  buffer.append({kSeparator, kEndEsc});

  AppendEscaped(buffer, col);
  buffer.append({kSeparator, kEndEsc});

  std::string encoded_ts = absl::StrFormat("%016x", ~timestamp);
  buffer.append(encoded_ts);

  return buffer;
}

std::string KeyCoder::PartialEncode(std::string const& row,
                                    std::string const& col) {
  std::string buffer;
  AppendEscaped(buffer, row);
  buffer.append({kSeparator, kEndEsc});

  AppendEscaped(buffer, col);

  return buffer;
}

StatusOr<DecodeResult> KeyCoder::Decode(std::string_view full_key) {
  size_t pos = 0;
  DecodeResult decode_result;
  auto res = ConsumeField(full_key, pos);
  if (!res.ok()) return res.status();
  decode_result.row = res.value();

  res = ConsumeField(full_key, pos);
  if (!res.ok()) return res.status();
  decode_result.col = res.value();

  if (full_key.size() - pos != 16)
    return InternalError("Received invalid key",
                         GCP_ERROR_INFO().WithMetadata("key", full_key));

  std::string_view ts_hex = full_key.substr(pos);
  uint64_t be_ts;

  auto [ptr, ec] =
      std::from_chars(ts_hex.data(), ts_hex.data() + ts_hex.size(), be_ts, 16);

  if (ec == std::errc()) {
    decode_result.timestamp = ~be_ts;
  } else {
    return InternalError(
        "Failed to parse timestamp hex",
        GCP_ERROR_INFO().WithMetadata("ts_hex", std::string(ts_hex)));
  }

  return decode_result;
}

void KeyCoder::AppendEscaped(std::string& dest, std::string const& src) {
  size_t pos = 0;
  while (true) {
    size_t sep_idx = src.find(kSeparator, pos);

    if (sep_idx == absl::string_view::npos) {
      dest.append(src.data() + pos, src.size() - pos);
      break;
    }

    dest.append(src.data() + pos, sep_idx - pos);
    dest.append({kSeparator, kExistingEsc});
    pos = sep_idx + 1;
  }
}

StatusOr<std::string> KeyCoder::ConsumeField(std::string_view src,
                                             size_t& pos) {
  std::string res;
  while (pos < src.size()) {
    size_t sep_idx = src.find(kSeparator, pos);
    if (sep_idx == std::string::npos) {
      return InternalError("Failed to decode key",
                           GCP_ERROR_INFO().WithMetadata("key_left", src));
    }

    res.append(src, pos, sep_idx - pos);
    if (sep_idx + 1 >= src.size())
      return InternalError("Failed to decode key",
                           GCP_ERROR_INFO().WithMetadata("key_left", src));
    char next_byte = src[sep_idx + 1];
    if (next_byte == kExistingEsc) {
      res.push_back(kSeparator);
      pos = sep_idx + 2;
    } else if (next_byte == kEndEsc) {
      pos = sep_idx + 2;
      return res;
    } else {
      return InternalError("Failed to decode key",
                           GCP_ERROR_INFO().WithMetadata("key_left", src));
    }
  }
  return InternalError("Broken key: loop fell through",
                       GCP_ERROR_INFO().WithMetadata("key_left", src));
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google