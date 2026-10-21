#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H

#include "google/cloud/internal/make_status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_format.h"
#include <string>

/**
 * KeyCoder class is responsible for API used in encoding and decoding keys for
 * the persistent database. The general format of the key is row#col#timestamp,
 * where # is a separator, and the timestamp has a fixed length of 16 and  has
 * its bits reversed. # is two bytes: "\x00\x01". 0x00 is the "logical"
 * separator. 0x01 is used to distinguish 0x00 added by the encoder from the
 * 0x00 already present in the row or key. For that, any existing 0x00 in the
 * row or col is swapped for "\x00\xFF".
 */
namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

struct DecodeResult {
  std::string row;
  std::string col;
  uint64_t timestamp;
};

class KeyCoder {
 public:
  static std::string Encode(std::string const& row, std::string const& col,
                            uint64_t timestamp);

  static std::string PartialEncode(std::string const& row,
                                   std::string const& col);

  static StatusOr<DecodeResult> Decode(std::string_view full_key);

  static constexpr char kSeparator{';'};
  static constexpr char kEndEsc{'\x01'};
  static constexpr char kExistingEsc{'\xFF'};

 private:
  static void AppendEscaped(std::string& dest, std::string const& src);

  static StatusOr<std::string> ConsumeField(std::string_view src, size_t& pos);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H