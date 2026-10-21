// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "google/cloud/testing_util/status_matchers.h"
#include "table.h"
#include "test_util.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

constexpr char const* kTableName =
    "persistent_modify_cf_projects/test/instances/test/tables/test";
constexpr char const* kDataRoot = "/tmp/";

class PersistentModifyColumnFamiliesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all("/tmp/persistent_modify_cf_projects");
  }

  void TearDown() override {
    std::filesystem::remove_all("/tmp/persistent_modify_cf_projects");
  }

  StatusOr<std::shared_ptr<Table>> CreatePersistentTable(
      std::vector<std::string> const& column_families) {
    std::vector<std::string> cfs = column_families;
    return CreateTable(kTableName, cfs, true);
  }

  google::bigtable::admin::v2::ModifyColumnFamiliesRequest
  MakeModifyRequest() {
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest request;
    request.set_name(kTableName);
    return request;
  }
};

TEST_F(PersistentModifyColumnFamiliesTest, CreateColumnFamily) {
  std::vector<std::string> initial_cfs = {};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  const auto& table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("fam_a");
  mod->mutable_create();

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_STATUS_OK(result);
  EXPECT_EQ(1, result->column_families().size());
  EXPECT_TRUE(result->column_families().count("fam_a"));
}

TEST_F(PersistentModifyColumnFamiliesTest, CreateMultipleColumnFamilies) {
  std::vector<std::string> initial_cfs = {"fam_initial"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  const auto& table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod1 = request.add_modifications();
  mod1->set_id("fam_a");
  mod1->mutable_create();
  auto* mod2 = request.add_modifications();
  mod2->set_id("fam_b");
  mod2->mutable_create();
  auto* mod3 = request.add_modifications();
  mod3->set_id("fam_c");
  mod3->mutable_create();

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_STATUS_OK(result);
  EXPECT_EQ(4, result->column_families().size());
  EXPECT_TRUE(result->column_families().count("fam_initial"));
  EXPECT_TRUE(result->column_families().count("fam_a"));
  EXPECT_TRUE(result->column_families().count("fam_b"));
  EXPECT_TRUE(result->column_families().count("fam_c"));
}

TEST_F(PersistentModifyColumnFamiliesTest, DropColumnFamily) {
  std::vector<std::string> initial_cfs = {"fam_a", "fam_b", "fam_c"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  const auto& table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("fam_b");
  mod->set_drop(true);

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_STATUS_OK(result);
  EXPECT_EQ(2, result->column_families().size());
  EXPECT_TRUE(result->column_families().count("fam_a"));
  EXPECT_FALSE(result->column_families().count("fam_b"));
  EXPECT_TRUE(result->column_families().count("fam_c"));
}

TEST_F(PersistentModifyColumnFamiliesTest, DropNonExistentColumnFamilyFails) {
  std::vector<std::string> initial_cfs = {"fam_a"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("nonexistent");
  mod->set_drop(true);

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(StatusCode::kNotFound, result.status().code());
}

TEST_F(PersistentModifyColumnFamiliesTest, CreateAlreadyExistingColumnFamilyFails) {
  std::vector<std::string> initial_cfs = {"fam_a"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("fam_a");
  mod->mutable_create();

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(StatusCode::kAlreadyExists, result.status().code());
}

TEST_F(PersistentModifyColumnFamiliesTest, UpdateColumnFamilyGcRule) {
  std::vector<std::string> initial_cfs = {"fam_a"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("fam_a");
  mod->mutable_update()->mutable_gc_rule()->mutable_max_age()->set_seconds(3600);
  google::protobuf::FieldMask* mask = mod->mutable_update_mask();
  mask->add_paths("gc_rule");

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_STATUS_OK(result);
  EXPECT_EQ(1, result->column_families().size());
  ASSERT_TRUE(result->column_families().count("fam_a"));
  EXPECT_TRUE(result->column_families().at("fam_a").gc_rule().has_max_age());
  EXPECT_EQ(3600, result->column_families().at("fam_a").gc_rule().max_age().seconds());
}

TEST_F(PersistentModifyColumnFamiliesTest, UpdateNonExistentColumnFamilyFails) {
  std::vector<std::string> initial_cfs = {"fam_a"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("nonexistent");
  mod->mutable_update()->mutable_gc_rule()->mutable_max_age()->set_seconds(3600);
  mod->mutable_update_mask()->add_paths("gc_rule");

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(StatusCode::kNotFound, result.status().code());
}

TEST_F(PersistentModifyColumnFamiliesTest, UpdateWithValueTypeInMaskFails) {
  std::vector<std::string> initial_cfs = {"fam_a"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("fam_a");
  mod->mutable_update();
  mod->mutable_update_mask()->add_paths("value_type");

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(StatusCode::kInvalidArgument, result.status().code());
}

TEST_F(PersistentModifyColumnFamiliesTest, DropWithDeletionProtectionFails) {
  std::vector<std::string> initial_cfs = {"fam_a"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  auto schema = CreateSchema(kTableName, {{"fam_a", {}}});
  schema.set_deletion_protection(true);
  google::protobuf::FieldMask mask;
  mask.add_paths("deletion_protection");
  ASSERT_STATUS_OK(table->Update(schema, mask));

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("fam_a");
  mod->set_drop(true);

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(StatusCode::kFailedPrecondition, result.status().code());
}

TEST_F(PersistentModifyColumnFamiliesTest, CombinedCreateDropUpdate) {
  std::vector<std::string> initial_cfs = {"fam_a", "fam_b", "fam_c"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod_drop = request.add_modifications();
  mod_drop->set_id("fam_b");
  mod_drop->set_drop(true);

  auto* mod_create = request.add_modifications();
  mod_create->set_id("fam_d");
  mod_create->mutable_create();

  auto* mod_update = request.add_modifications();
  mod_update->set_id("fam_a");
  mod_update->mutable_update()->mutable_gc_rule()->set_max_num_versions(5);
  mod_update->mutable_update_mask()->add_paths("gc_rule");

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_STATUS_OK(result);
  EXPECT_EQ(3, result->column_families().size());
  EXPECT_TRUE(result->column_families().count("fam_a"));
  EXPECT_FALSE(result->column_families().count("fam_b"));
  EXPECT_TRUE(result->column_families().count("fam_c"));
  EXPECT_TRUE(result->column_families().count("fam_d"));
  EXPECT_TRUE(result->column_families().at("fam_a").gc_rule().has_max_num_versions());
  EXPECT_EQ(5, result->column_families().at("fam_a").gc_rule().max_num_versions());
}

}  // namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
