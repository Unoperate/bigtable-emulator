/**
 * @file key_encoding.h
 * @brief Key encoding/decoding utilities for RocksDB storage keys.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_KEY_ENCODING_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_KEY_ENCODING_H

#include "persist/utils/logging.h"

#include "absl/strings/str_cat.h"
#include <cassert>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <fmt/ranges.h>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
 * Key encoding for RocksDB storage keys.
 *
 * Encodes (table_name, row_key, column_qualifier) into a single string with
 * configurable delimiter. The delimiter character is escaped in component
 * values using backslash; backslash itself is also escaped.
 *
 * @tparam Delimiter Character used to separate key components. Cannot be '\\' equal to Escaper
 *         (backslash) which is reserved for escaping.
 * @tparam Escaper   Character used to escape Delimiter character. Backslack by default
 * @tparam Unsafe    Set to true (false by default) if you are absolutely sure that rows ids, column quialifiers and other IDs
 *                   won't contain any character equal to Delimiter.
 *                   UB if Unsafe = true and Delimiter is in key
 */
template <char Delimiter = '|', char Escaper = '\\', bool Unsafe = false>
class KeyEncoding {
  static_assert(Delimiter != Escaper,
                "Escaper character cannot be used as delimiter");
 private:
  const std::string delimiter_str_;
 public:

  explicit KeyEncoding() : delimiter_str_(1, Delimiter) {};

  /**
   * Escapes a string so it can safely contain the delimiter character.
   * Replaces '\\' with "\\\\" and Delimiter with "\\" + Delimiter.
   */
  template <bool U = !Unsafe>
  static std::enable_if<U, std::string>::type Escape(const std::string& s) {
    std::string result;
    result.reserve(s.size() + 8);  // conservative reserve for escape chars
    for (char c : s) {
      if (c == Escaper) {
        result += Escaper;
        result += Escaper;
      } else if (c == static_cast<unsigned char>(Delimiter)) {
        result += Escaper;
        result += static_cast<char>(Delimiter);
      } else {
        result += static_cast<char>(c);
      }
    }
    return result;
  }

  // Unsafe version that is no-op
  template <bool U = Unsafe>
  static std::enable_if<U, std::string_view>::type Escape(const std::string& s) {
    return s;
  }

  /**
   * Unescapes a string produced by Escape().
   */
  template <bool U = !Unsafe>
  static std::enable_if<U, std::string>::type Unescape(const std::string& s) {
    std::string result;
    result.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
      if (s[i] == Escaper && i + 1 < s.size()) {
        result += s[i + 1];
        ++i;
      } else {
        result += s[i];
      }
    }
    return result;
  }

  // Unsafe version that is no-op
  template <bool U = Unsafe>
  static std::enable_if<U, std::string_view>::type Unescape(const std::string& s) {
    return s;
  }

  /**
   * Builds the RocksDB key for a (table, row, column_qualifier).
   * All components are escaped before concatenation.
   */
  std::string StorageKey(std::string const& table_name,
                         std::string const& row_key,
                         std::string const& column_qualifier) const {
    return absl::StrCat(Escape(table_name), delimiter_str_, Escape(row_key),
    delimiter_str_, Escape(column_qualifier));
  }

  /**
   * Builds the RocksDB key prefix for a (table, row) to iterate all columns.
   * All components are escaped before concatenation.
   */
  std::string StorageKeyPartial(std::string const& table_name,
                                std::string const& row_key) const {
    return absl::StrCat(Escape(table_name), delimiter_str_, Escape(row_key));
  }

  /**
   * Parses a RocksDB key into (table_name, row_key, column_qualifier).
   * Splits on unescaped delimiters and unescapes each component.
   *
   * @param key The encoded key string.
   * @return Tuple of (table_name, row_key, column_qualifier).
   */
   std::tuple<std::string, std::string, std::string> ParseRowKey(
      std::string const& key
  ) const {
    std::vector<std::string> parts;
    std::string current;
    current.reserve(key.size() );
    for (size_t i = 0; i < key.size(); ++i) {
      if (!Unsafe && key[i] == Escaper && i + 1 < key.size()) {
        // Skip this in unsafe mode
        current += key[i + 1];
        ++i;
      } else if (key[i] == Delimiter) {
        parts.push_back(std::move(current));
        current.clear();
      } else {
        current += key[i];
      }
    }
    parts.push_back(std::move(current));
    if (parts.size() < 3) {
      LERROR(fmt::format("ParseRowKey expects key with at least 3 components, got: '{}' with parts: {}", key, fmt::join(parts, ","))); 
    }
    assert(parts.size() >= 3);
    return std::make_tuple(std::move(parts[0]), std::move(parts[1]),
                           std::move(parts[2]));
  }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_KEY_ENCODING_H
