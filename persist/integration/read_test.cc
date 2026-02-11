// Copyright 2024 Google LLC
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

#include "persist/rocksdb/storage.h"
#include "persist/utils/test_utils.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

// CBT integration test
TEST(IntegrationTest, Read) {
  RocksDBStorageTestManager m;
  auto server = m.RunServer();
  auto client = server.Client();

  google::bigtable::admin::v2::Table t;
  auto& families = *t.mutable_column_families();
  families["fam"].mutable_gc_rule()->set_max_num_versions(10);

  auto maybe_create = client.CreateTable(cbt::InstanceName("project1", "instance1"), "table1", std::move(t));
  EXPECT_OK_STATUS(maybe_create);

  auto table = server.Table("project1", "instance1", "table1");

  std::vector<std::string> greetings{"Hello World!", "Hello Cloud Bigtable!", "Hello C++!"};
  int i = 0;
  for (auto const& greeting : greetings) {
    std::string row_key = "key-" + std::to_string(i);
    google::cloud::Status status = table.Apply(cbt::SingleRowMutation(
    std::move(row_key), cbt::SetCell("fam", "c0", greeting)));
    EXPECT_OK(status);
    ++i;
  }

  WAIT();

  EXPECT_ROWS_CBT(table, {"fam.key-0.c0", "Hello World!"}, {"fam.key-1.c0", "Hello Cloud Bigtable!"}, {"fam.key-2.c0", "Hello C++!"});

}

}  // anonymous namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
