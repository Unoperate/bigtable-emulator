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

#include "registry.h"
#include "google/cloud/status.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "absl/synchronization/notification.h"
#include "backend.h"
#include "bigtable_limits.h"
#include "test_util.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

using ::google::cloud::testing_util::StatusIs;

constexpr char const* kInstanceName = "projects/p/instances/i";
constexpr char const* kTableName = "projects/p/instances/i/tables/t";

/// Forwards to the in-memory backend, but CreateTable() announces that it has
/// started and then waits to be released.
class PausingBackend : public Backend {
 public:
  PausingBackend(std::unique_ptr<Backend> backend, absl::Notification& entered,
                 absl::Notification& release)
      : backend_(std::move(backend)), entered_(entered), release_(release) {}

  StatusOr<std::vector<RestoredTable>> Restore() override {
    return backend_->Restore();
  }
  StatusOr<std::unique_ptr<TableOperations>> CreateTable(
      google::bigtable::admin::v2::Table const& schema) override {
    entered_.Notify();
    release_.WaitForNotification();
    return backend_->CreateTable(schema);
  }
  void DestroyTable(std::unique_ptr<TableOperations> operations) override {
    backend_->DestroyTable(std::move(operations));
  }
  void StopBackgroundWork() override { backend_->StopBackgroundWork(); }
  Status Shutdown() override { return backend_->Shutdown(); }

 private:
  std::unique_ptr<Backend> backend_;
  absl::Notification& entered_;
  absl::Notification& release_;
};

class UncommittedCreateBackend : public Backend {
 public:
  StatusOr<std::vector<RestoredTable>> Restore() override {
    return std::vector<RestoredTable>();
  }
  StatusOr<std::unique_ptr<TableOperations>> CreateTable(
      google::bigtable::admin::v2::Table const&) override {
    return UnavailableError("commit failed", GCP_ERROR_INFO());
  }
  void DestroyTable(std::unique_ptr<TableOperations>) override {}
  void StopBackgroundWork() override {}
  Status Shutdown() override { return Status(); }
};

class RegistryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto backend = Backend::Create(BackendOptions{});
    ASSERT_STATUS_OK(backend);
    auto registry = Registry::Create(*std::move(backend));
    ASSERT_STATUS_OK(registry);
    registry_ = *std::move(registry);
  }

  Status CreateTable(std::string const& table_id) {
    return registry_
        ->CreateTable(std::string(kInstanceName) + "/tables/" + table_id,
                      google::bigtable::admin::v2::Table())
        .status();
  }

  Status CreateTableWithFamily(std::string const& column_family_id) {
    google::bigtable::admin::v2::Table schema;
    (*schema.mutable_column_families())[column_family_id];
    return registry_->CreateTable(kTableName, std::move(schema)).status();
  }

  Status CreateFamily(std::string const& column_family_id) {
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest request;
    request.set_name(kTableName);
    auto* modification = request.add_modifications();
    modification->set_id(column_family_id);
    modification->mutable_create();
    return registry_->ModifyColumnFamilies(request).status();
  }

  Status CreateTableWithFamilies(std::size_t count) {
    google::bigtable::admin::v2::Table schema;
    for (std::size_t i = 0; i < count; ++i) {
      (*schema.mutable_column_families())["f" + std::to_string(i)];
    }
    return registry_->CreateTable(kTableName, std::move(schema)).status();
  }

  std::shared_ptr<Registry> registry_;
};

TEST_F(RegistryTest, CreateTableAcceptsEveryAllowedCharacter) {
  EXPECT_STATUS_OK(CreateTable("_aZ9-."));
}

TEST_F(RegistryTest, CreateTableAccepts50CharacterId) {
  EXPECT_STATUS_OK(CreateTable(std::string(50, 'a')));
}

TEST_F(RegistryTest, CreateTableWith51CharacterIdFails) {
  EXPECT_THAT(CreateTable(std::string(51, 'a')),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableWithEmptyIdFails) {
  EXPECT_THAT(CreateTable(""), StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableWithIdStartingWithDashFails) {
  EXPECT_THAT(CreateTable("-a"), StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableWithIdStartingWithDotFails) {
  EXPECT_THAT(CreateTable(".a"), StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableWithDotDotIdFails) {
  EXPECT_THAT(CreateTable(".."), StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableWithSlashInIdFails) {
  EXPECT_THAT(CreateTable("a/b"), StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableWithTablesSegmentInIdFails) {
  EXPECT_THAT(CreateTable("a/tables/b"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableWithDisallowedCharacterFails) {
  EXPECT_THAT(CreateTable("a b"), StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableInProjectAndInstanceNamedTables) {
  EXPECT_STATUS_OK(
      registry_
          ->CreateTable("projects/tables/instances/tables/tables/t",
                        google::bigtable::admin::v2::Table())
          .status());
}

TEST_F(RegistryTest, CreateTableBeyondTableLimitFails) {
  for (std::size_t i = 0; i < kMaxTablesPerInstance; ++i) {
    ASSERT_STATUS_OK(CreateTable("t" + std::to_string(i)));
  }
  EXPECT_THAT(CreateTable("one_too_many"),
              StatusIs(StatusCode::kResourceExhausted));
}

TEST_F(RegistryTest, CreateExistingTableAtTableLimitFailsAsExisting) {
  for (std::size_t i = 0; i < kMaxTablesPerInstance; ++i) {
    ASSERT_STATUS_OK(CreateTable("t" + std::to_string(i)));
  }
  EXPECT_THAT(CreateTable("t0"), StatusIs(StatusCode::kAlreadyExists));
}

TEST_F(RegistryTest, CreateTableAcceptsEveryAllowedFamilyCharacter) {
  EXPECT_STATUS_OK(CreateTableWithFamily("_-.aZ9"));
}

TEST_F(RegistryTest, CreateTableWithFamilyStartingWithDashFails) {
  EXPECT_THAT(CreateTableWithFamily("-a"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableAccepts64CharacterFamilyId) {
  EXPECT_STATUS_OK(CreateTableWithFamily(std::string(64, 'a')));
}

TEST_F(RegistryTest, CreateTableWith65CharacterFamilyIdFails) {
  EXPECT_THAT(CreateTableWithFamily(std::string(65, 'a')),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableWithEmptyFamilyIdFails) {
  EXPECT_THAT(CreateTableWithFamily(""),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableWithDisallowedFamilyCharacterFails) {
  EXPECT_THAT(CreateTableWithFamily("a b"),
              StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, ModifyColumnFamiliesCreateWithDisallowedCharacterFails) {
  ASSERT_STATUS_OK(CreateTable("t"));
  EXPECT_THAT(CreateFamily("a b"), StatusIs(StatusCode::kInvalidArgument));
}

TEST_F(RegistryTest, CreateTableAtFamilyLimitSucceeds) {
  EXPECT_STATUS_OK(CreateTableWithFamilies(kMaxColumnFamiliesPerTable));
}

TEST_F(RegistryTest, CreateTableBeyondFamilyLimitFails) {
  EXPECT_THAT(CreateTableWithFamilies(kMaxColumnFamiliesPerTable + 1),
              StatusIs(StatusCode::kResourceExhausted));
}

TEST_F(RegistryTest, ModifyColumnFamiliesBeyondFamilyLimitFails) {
  ASSERT_STATUS_OK(CreateTableWithFamilies(kMaxColumnFamiliesPerTable));
  EXPECT_THAT(CreateFamily("one_too_many"),
              StatusIs(StatusCode::kResourceExhausted));
}

TEST_F(RegistryTest, ModifyColumnFamiliesCountsFamiliesAfterAllChanges) {
  ASSERT_STATUS_OK(CreateTableWithFamilies(kMaxColumnFamiliesPerTable));
  google::bigtable::admin::v2::ModifyColumnFamiliesRequest request;
  request.set_name(kTableName);
  auto* create = request.add_modifications();
  create->set_id("new");
  create->mutable_create();
  auto* drop = request.add_modifications();
  drop->set_id("f0");
  drop->set_drop(true);
  EXPECT_STATUS_OK(registry_->ModifyColumnFamilies(request).status());
}

TEST(RegistryCreationTest, FindTableBeingCreatedReturnsFailedPrecondition) {
  absl::Notification entered;
  absl::Notification release;
  auto backend = Backend::Create(BackendOptions{});
  ASSERT_STATUS_OK(backend);
  auto registry = Registry::Create(
      std::make_unique<PausingBackend>(*std::move(backend), entered, release));
  ASSERT_STATUS_OK(registry);

  Status create_status;
  std::thread create([&] {
    create_status =
        (*registry)
            ->CreateTable(kTableName, google::bigtable::admin::v2::Table())
            .status();
  });
  entered.WaitForNotification();

  EXPECT_THAT((*registry)->FindTable(kTableName).status(),
              StatusIs(StatusCode::kFailedPrecondition));

  release.Notify();
  create.join();
  EXPECT_STATUS_OK(create_status);
  EXPECT_STATUS_OK((*registry)->FindTable(kTableName).status());
}

TEST(RegistryCreationTest, CreateTableWithFailedCommitReservesName) {
  auto registry =
      Registry::Create(std::make_unique<UncommittedCreateBackend>());
  ASSERT_STATUS_OK(registry);
  google::bigtable::admin::v2::Table schema;

  EXPECT_THAT((*registry)->CreateTable(kTableName, schema).status(),
              StatusIs(StatusCode::kUnavailable));
  EXPECT_THAT(
      (*registry)
          ->GetTable(kTableName, google::bigtable::admin::v2::Table::NAME_ONLY)
          .status(),
      StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_THAT((*registry)->CheckTableExists(kTableName),
              StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_THAT((*registry)->CreateTable(kTableName, schema).status(),
              StatusIs(StatusCode::kAlreadyExists));
}

TEST(RegistryCreationTest, ListTablesExcludesTableWithFailedCommit) {
  auto registry =
      Registry::Create(std::make_unique<UncommittedCreateBackend>());
  ASSERT_STATUS_OK(registry);
  EXPECT_THAT(
      (*registry)
          ->CreateTable(kTableName, google::bigtable::admin::v2::Table())
          .status(),
      StatusIs(StatusCode::kUnavailable));

  auto tables = (*registry)->ListTables(
      kInstanceName, google::bigtable::admin::v2::Table::NAME_ONLY);
  ASSERT_STATUS_OK(tables);
  EXPECT_TRUE(tables->empty());
}

TEST(RegistryCreationTest, ConcurrentCreateTableOfOneNameCreatesOneTable) {
  BackendOptions options;
  options.data_dir = TestDataDir();
  auto backend = Backend::Create(options);
  ASSERT_STATUS_OK(backend);
  auto registry = Registry::Create(*std::move(backend));
  ASSERT_STATUS_OK(registry);

  Status first;
  Status second;
  auto create = [&registry](Status& result) {
    result = (*registry)
                 ->CreateTable(kTableName, google::bigtable::admin::v2::Table())
                 .status();
  };
  std::thread first_thread(create, std::ref(first));
  std::thread second_thread(create, std::ref(second));
  first_thread.join();
  second_thread.join();

  EXPECT_NE(first.ok(), second.ok());
  EXPECT_THAT(first.ok() ? second : first,
              StatusIs(StatusCode::kAlreadyExists));
  EXPECT_EQ(1, std::distance(std::filesystem::directory_iterator(TestDataDir() +
                                                                 "/tables"),
                             std::filesystem::directory_iterator()));
}

}  // namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
