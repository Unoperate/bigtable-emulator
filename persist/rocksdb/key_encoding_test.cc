// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "persist/utils/test_utils.h"
#include "persist/rocksdb/key_encoding.h"
#include <gtest/gtest.h>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

TEST(KeyEncoding, DefaultDelimiterRoundtrip) {
  KeyEncoding<'|'> enc;
  auto key = enc.StorageKey("table", "row", "col");
  EXPECT_EQ(key, "table|row|col");

  auto [t, r, c] = enc.ParseRowKey(key);
  EXPECT_EQ(t, "table");
  EXPECT_EQ(r, "row");
  EXPECT_EQ(c, "col");
}

TEST(KeyEncoding, DefaultDelimiterRoundtripUnsafe) {
  KeyEncoding<'|', '\\', true> enc;
  auto key = enc.StorageKey("table", "row", "col");
  EXPECT_EQ(key, "table|row|col");

  auto [t, r, c] = enc.ParseRowKey(key);
  EXPECT_EQ(t, "table");
  EXPECT_EQ(r, "row");
  EXPECT_EQ(c, "col");
}

TEST(KeyEncoding, EscapesDelimiterInComponents) {
  KeyEncoding<'|'> enc;
  // table_name contains "|", row_key contains "|", column_qualifier contains "|"
  auto key = enc.StorageKey("tab|le", "ro|w", "c|ol");
  EXPECT_EQ(key, "tab\\|le|ro\\|w|c\\|ol");

  auto [t, r, c] = enc.ParseRowKey(key);
  EXPECT_EQ(t, "tab|le");
  EXPECT_EQ(r, "ro|w");
  EXPECT_EQ(c, "c|ol");
}

TEST(KeyEncoding, EscapesDelimiterInComponentsCustomEscape) {
  // Custom escape provided
  KeyEncoding<'@', '#'> enc;
  // table_name contains "@", row_key contains "@", column_qualifier contains "@"
  auto key = enc.StorageKey("tab@le", "ro@w", "c@ol");
  EXPECT_EQ(key, "tab#@le@ro#@w@c#@ol");

  auto [t, r, c] = enc.ParseRowKey(key);
  EXPECT_EQ(t, "tab@le");
  EXPECT_EQ(r, "ro@w");
  EXPECT_EQ(c, "c@ol");
}

TEST(KeyEncoding, EscapesBackslash) {
  KeyEncoding<'|'> enc;
  auto key = enc.StorageKey("tab\\le", "row", "col");
  EXPECT_EQ(key, "tab\\\\le|row|col");

  auto [t, r, c] = enc.ParseRowKey(key);
  EXPECT_EQ(t, "tab\\le");
  EXPECT_EQ(r, "row");
  EXPECT_EQ(c, "col");
}

TEST(KeyEncoding, EscapesBothDelimiterAndBackslash) {
  KeyEncoding<'|'> enc;
  auto key = enc.StorageKey("tab|\\le", "row", "col");
  EXPECT_EQ(key, "tab\\|\\\\le|row|col");

  auto [t, r, c] = enc.ParseRowKey(key);
  EXPECT_EQ(t, "tab|\\le");
  EXPECT_EQ(r, "row");
  EXPECT_EQ(c, "col");
}

TEST(KeyEncoding, StorageKeyPartial) {
  KeyEncoding<'|'> enc;
  auto key = enc.StorageKeyPartial("table", "row");
  EXPECT_EQ(key, "table|row");

  auto full_key = enc.StorageKey("table", "row", "col");
  // full_key.starts_with(key)
  EXPECT_TRUE(full_key.rfind(key, 0) == 0);
}

TEST(KeyEncoding, CustomDelimiter) {
  KeyEncoding<'X'> enc;  // Use ASCII 1 as delimiter
  auto key = enc.StorageKey("table", "row", "col");
  EXPECT_EQ(key, "tableXrowXcol");

  auto [t, r, c] = enc.ParseRowKey(key);
  EXPECT_EQ(t, "table");
  EXPECT_EQ(r, "row");
  EXPECT_EQ(c, "col");
}

TEST(KeyEncoding, CustomDelimiterWithEscape) {
  KeyEncoding<'\x01'> enc;
  std::string const table_with_delim = "tab" + std::string(1, '\x01') + "le";
  auto key = enc.StorageKey(table_with_delim, "row", "col");
  auto [t, r, c] = enc.ParseRowKey(key);
  EXPECT_EQ(t, table_with_delim);
  EXPECT_EQ(r, "row");
  EXPECT_EQ(c, "col");
}

}  // anonymous namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
