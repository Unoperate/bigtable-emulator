#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H

#include "google/cloud/internal/make_status.h"
#include "google/cloud/status_or.h"
#include "range_set.h"
#include <cstdint>
#include <string>
#include <string_view>

/**
 * KeyCoder class exposes API used in encoding and decoding keys for
 * the persistent database. The general format of the key is row#col#timestamp,
 * where # is a separator, and the timestamp is 8 bytes: bitwise-inverted,
 * big-endian encoding of the timestamp, so that newer cells sort first.
 * For the separator "#" we use "\x00\x01", and we use "\xFF" to escape
 * "\x00" that are already present in the row or column.
 * Because 0x01 is smaller than 0xFF and any other byte is greater
 * than 0x00, bytewise order of the encoded keys matches the order of
 * (row, col) tuples.
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
  /**
   * @param row row key
   * @param col column key
   * @param timestamp unix timestamp in milliseconds
   * @return RocksDB key in the format described in the header.
   */
  static std::string Encode(std::string_view row, std::string_view col,
                            uint64_t timestamp);

  /**
   * @param row row key
   * @return row key with "\x00" escaped as "\x00\xFF" and with the
   * separator "\x00\x01" appended at the end.
   */
  static std::string RowPrefix(std::string_view row);

  /**
   * @param prefix row key prefix
   * @return row key prefix with "\x00" escaped as "\x00\xFF"
   */
  static std::string EscapedRow(std::string_view prefix);

  /**
   * @param row row key
   * @param col column key
   * @return row key and column key with "\x00" escaped as "\x00\xFF" and
   * with the separator "\x00\x01" between them and at the end of the column
   * key.
   */
  static std::string ColumnPrefix(std::string_view row, std::string_view col);

  /**
   * @param row row key
   * @return returns a key bigger than all keys starting with the row prefix
   * key. Can be used with rocksdb iterator seek to jump to the next row.
   */
  static std::string NextRow(std::string_view row);

  /**
   * @param row row key
   * @param col column key
   * @return returns a key bigger than all keys starting with the column prefix
   * constructed from the row and col keys. Can be used with rocksdb iterator
   * seek to jump to the next column.
   */
  static std::string NextColumn(std::string_view row, std::string_view col);

  /**
   * @param full_key RocksDB key in the format described in the header
   * @return row key, column key and the unix timestamp in millis that were part
   * of the input key.
   */
  static StatusOr<DecodeResult> Decode(std::string_view full_key);

  /**
   * @param range string range set
   * @return row range start encoded by KeyCoder, where the encoding depends on
   * whether the range is closed or open
   */
  static std::string RowRangeStartKey(StringRangeSet::Range const& range);

  /**
   * @param row row key
   * @param range string range set
   * @return column range start encoded by KeyCoder, where the encoding depends
   * on whether the range is closed or open
   */
  static std::string ColumnRangeStartKey(std::string_view row,
                                         StringRangeSet::Range const& range);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H
