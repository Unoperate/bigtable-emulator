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
 * The separator "#" is "\x00\x01". A row or column is "escaped" by writing
 * every "\x00" in it as "\x00\xFF", so an escaped field never contains the
 * separator.
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
   * @param timestamp unix timestamp in microseconds
   * @return RocksDB key in the format described in the header.
   */
  static std::string Encode(std::string_view row, std::string_view col,
                            uint64_t timestamp);

  /**
   * @param row row key
   * @return "escaped" row key with the separator added at the end.
   */
  static std::string EscapedRowWithSeparator(std::string_view row);

  /**
   * @param row row key
   * @return "escaped" row key, but without the separator at the end.
   */
  static std::string EscapedRow(std::string_view row);

  /**
   * @param row row key
   * @param col column key
   * @return Concatenated "escaped" row key and column key with the separator
   * added to the end of both of them.
   */
  static std::string EscapedRowColWithSeparator(std::string_view row,
                                                std::string_view col);

  /**
   * @param row row key
   * @return a key that is greater than every key of the row and not greater
   * than any key of a later row. Can be used with rocksdb iterator seek to jump
   * to the next row.
   */
  static std::string NextRow(std::string_view row);

  /**
   * @param row row key
   * @param col column key
   * @return a key that is greater than every key of the column in the row and
   * not greater than any key of a later column or row. Can be used with rocksdb
   * iterator seek to jump to the next column.
   */
  static std::string NextColumn(std::string_view row, std::string_view col);

  /**
   * @param full_key RocksDB key in the format described in the header
   * @return row key, column key and the unix timestamp in microseconds that
   * were part of the input key.
   */
  static StatusOr<DecodeResult> Decode(std::string_view full_key);

  /**
   * @param range row range
   * @return a key that is not greater than any key of a row in the range and
   * greater than every key of a row before the range. Can be used with rocksdb
   * iterator seek to jump to the first row of the range.
   */
  static std::string RowRangeStartKey(StringRangeSet::Range const& range);

  /**
   * @param row row key
   * @param range column range
   * @return a key that is not greater than any key of the row with a column in
   * the range and greater than every key of the row with a column before the
   * range. Can be used with rocksdb iterator seek to jump to the first column
   * of the range.
   */
  static std::string ColumnRangeStartKey(std::string_view row,
                                         StringRangeSet::Range const& range);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_KEY_CODER_H
