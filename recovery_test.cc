#include "google/cloud/testing_util/status_matchers.h"
#include "column_family.h"
#include "filter.h"
#include "re2/re2.h"
#include "server.h"
#include "table.h"
#include "test_util.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.grpc.pb.h>
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/status.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

Status CompareSchemas(::google::bigtable::admin::v2::Table const& A,
                      ::google::bigtable::admin::v2::Table const& B) {
  if (A.name() != B.name()) {
    return NotFoundError("Schemas names do not match",
                         GCP_ERROR_INFO()
                             .WithMetadata("schema A", A.name())
                             .WithMetadata("Schema B", B.name()));
  }

  for (auto const& cf : A.column_families()) {
    if (B.column_families().find(cf.first) == B.column_families().end()) {
      return NotFoundError("have different column families",
                           GCP_ERROR_INFO()
                               .WithMetadata("schema A", A.name())
                               .WithMetadata("Schema B", B.name())
                               .WithMetadata("Column Family", cf.first));
    }
  }

  for (auto const& cf : B.column_families()) {
    if (A.column_families().find(cf.first) == A.column_families().end()) {
      return NotFoundError("have different column families",
                           GCP_ERROR_INFO()
                               .WithMetadata("schema A", A.name())
                               .WithMetadata("Schema B", B.name())
                               .WithMetadata("Column Family", cf.first));
    }
  }

  return Status();
}

class ServerTest : public ::testing::Test {
 protected:
  std::unique_ptr<EmulatorServer> server_;
  std::shared_ptr<grpc::Channel> channel_;

  void SetUp() override {
    auto maybe_server = CreateDefaultEmulatorServer("127.0.0.1", 0, true);
    ASSERT_STATUS_OK(maybe_server);
    server_ = std::move(maybe_server.value());
    channel_ = grpc::CreateChannel(
        "localhost:" + std::to_string(server_->bound_port()),
        grpc::InsecureChannelCredentials());
  }

  std::unique_ptr<google::bigtable::v2::Bigtable::Stub> DataClient() {
    return google::bigtable::v2::Bigtable::NewStub(channel_);
  }

  std::unique_ptr<google::bigtable::admin::v2::BigtableTableAdmin::Stub>
  TableAdminClient() {
    return google::bigtable::admin::v2::BigtableTableAdmin::NewStub(channel_);
  }
};

constexpr char const* kModifyTableName =
    "recovery_projects/test/instances/test/tables/test";

class PersistentModifyColumnFamiliesRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all("/tmp/recovery_projects");
  }

  void TearDown() override {
    std::filesystem::remove_all("/tmp/recovery_projects");
  }

  StatusOr<std::shared_ptr<Table>> CreatePersistentTable(
      std::vector<std::string> const& column_families) {
    std::vector<std::string> cfs = column_families;
    return CreateTable(kModifyTableName, cfs, true);
  }

  google::bigtable::admin::v2::ModifyColumnFamiliesRequest
  MakeModifyRequest() {
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest request;
    request.set_name(kModifyTableName);
    return request;
  }
};

TEST_F(ServerTest, RecoverEmptyTable) {
  std::string const table_name_short = "recovery";
  std::string const table_name_long = "recovery_projects/tables/recovery";
  std::filesystem::remove_all("/tmp/" + table_name_long);
  grpc::Status status;
  {
    google::bigtable::admin::v2::CreateTableRequest request;
    request.set_table_id(table_name_short);
    request.set_parent("recovery_projects");
    google::bigtable::admin::v2::Table response;
    grpc::ClientContext context;
    ASSERT_EQ(
        true,
        TableAdminClient()->CreateTable(&context, request, &response).ok());
  }

  server_.reset();
  auto maybe_server = CreateDefaultEmulatorServer("127.0.0.1", 0, true);
  ASSERT_STATUS_OK(maybe_server);
  server_ = std::move(maybe_server.value());
  channel_ =
      grpc::CreateChannel("localhost:" + std::to_string(server_->bound_port()),
                          grpc::InsecureChannelCredentials());

  {
    google::bigtable::admin::v2::GetTableRequest request;
    request.set_name(table_name_long);
    google::bigtable::admin::v2::Table response;
    google::bigtable::admin::v2::Table empty_schema =
        CreateSchema(table_name_long, {});
    grpc::ClientContext context;
    ASSERT_EQ(true,
              TableAdminClient()->GetTable(&context, request, &response).ok());
    ASSERT_EQ(Status(), CompareSchemas(empty_schema, response));
  }

  std::filesystem::remove_all("/tmp/recovery_projects");
}

TEST_F(ServerTest, RecoverTableWithColumnFamilies) {
  std::string const table_name_short = "recovery";
  std::string const table_name_long = "recovery_projects/tables/recovery";
  std::filesystem::remove_all("/tmp/" + table_name_long);
  {
    google::bigtable::admin::v2::CreateTableRequest request;
    request.set_table_id(table_name_short);
    request.set_parent("recovery_projects");
    google::bigtable::admin::v2::Table response;
    grpc::ClientContext context;
    ASSERT_EQ(
        true,
        TableAdminClient()->CreateTable(&context, request, &response).ok());
  }
  {
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest request;
    request.set_name(table_name_long);

    auto* mod1 = request.add_modifications();
    mod1->set_id("fam_a");
    mod1->mutable_create();

    auto* mod2 = request.add_modifications();
    mod2->set_id("fam_b");
    mod2->mutable_create();

    google::bigtable::admin::v2::Table response;
    grpc::ClientContext context;

    grpc::Status status =
        TableAdminClient()->ModifyColumnFamilies(&context, request, &response);
    ASSERT_TRUE(status.ok());

    ASSERT_TRUE(response.column_families().count("fam_a"));
    ASSERT_TRUE(response.column_families().count("fam_b"));
  }

  server_.reset();
  auto maybe_server = CreateDefaultEmulatorServer("127.0.0.1", 0, true);
  ASSERT_STATUS_OK(maybe_server);
  server_ = std::move(maybe_server.value());
  channel_ =
      grpc::CreateChannel("localhost:" + std::to_string(server_->bound_port()),
                          grpc::InsecureChannelCredentials());

  {
    google::bigtable::admin::v2::GetTableRequest request;
    request.set_name(table_name_long);
    google::bigtable::admin::v2::Table response;
    google::bigtable::admin::v2::Table empty_schema =
        CreateSchema(table_name_long, {{"fam_a", {}}, {"fam_b", {}}});
    grpc::ClientContext context;
    ASSERT_EQ(true,
              TableAdminClient()->GetTable(&context, request, &response).ok());
    ASSERT_EQ(Status(), CompareSchemas(empty_schema, response));
  }

  server_.reset();
  maybe_server = CreateDefaultEmulatorServer("127.0.0.1", 0, true);
  ASSERT_STATUS_OK(maybe_server);
  server_ = std::move(maybe_server.value());
  channel_ =
      grpc::CreateChannel("localhost:" + std::to_string(server_->bound_port()),
                          grpc::InsecureChannelCredentials());

  {
    google::bigtable::v2::MutateRowRequest request;
    request.set_table_name(table_name_long);
    request.set_row_key("row_1");

    auto* mutation_a = request.add_mutations();
    auto* set_cell_a = mutation_a->mutable_set_cell();
    set_cell_a->set_family_name("fam_a");
    set_cell_a->set_column_qualifier("col_1");
    set_cell_a->set_value("value_a");

    auto* mutation_b = request.add_mutations();
    auto* set_cell_b = mutation_b->mutable_set_cell();
    set_cell_b->set_family_name("fam_b");
    set_cell_b->set_column_qualifier("col_1");
    set_cell_b->set_value("value_b");

    google::bigtable::v2::MutateRowResponse response;
    grpc::ClientContext context;

    grpc::Status status = DataClient()->MutateRow(&context, request, &response);
    std::cout << status.error_message() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  server_.reset();
  maybe_server = CreateDefaultEmulatorServer("127.0.0.1", 0, true);
  ASSERT_STATUS_OK(maybe_server);
  server_ = std::move(maybe_server.value());
  channel_ =
      grpc::CreateChannel("localhost:" + std::to_string(server_->bound_port()),
                          grpc::InsecureChannelCredentials());

  {
    google::bigtable::v2::ReadRowsRequest request;
    request.set_table_name(table_name_long);
    request.mutable_rows()->add_row_keys("row_1");

    grpc::ClientContext context;
    auto reader = DataClient()->ReadRows(&context, request);
    google::bigtable::v2::ReadRowsResponse response;

    bool found_a = false;
    bool found_b = false;

    while (reader->Read(&response)) {
      for (auto const& chunk : response.chunks()) {
        if (chunk.value() == "value_a") found_a = true;
        if (chunk.value() == "value_b") found_b = true;
      }
    }

    grpc::Status status = reader->Finish();
    ASSERT_TRUE(status.ok());

    ASSERT_TRUE(found_a);
    ASSERT_TRUE(found_b);
  }

  std::filesystem::remove_all("/tmp/recovery_projects");
}

TEST_F(PersistentModifyColumnFamiliesRecoveryTest, SchemaPersistsAcrossReload) {
  std::vector<std::string> initial_cfs = {"fam_initial"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  auto request = MakeModifyRequest();
  auto* mod1 = request.add_modifications();
  mod1->set_id("fam_a");
  mod1->mutable_create();
  auto* mod2 = request.add_modifications();
  mod2->set_id("fam_b");
  mod2->mutable_create();

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_STATUS_OK(result);

  table->MarkForDeletion();
  table.reset();

  auto maybe_loaded = Table::Load(kModifyTableName, "/tmp/");
  ASSERT_STATUS_OK(maybe_loaded);
  auto loaded_table = maybe_loaded.value();

  auto schema = loaded_table->GetSchema();
  EXPECT_EQ(3, schema.column_families().size());
  EXPECT_TRUE(schema.column_families().count("fam_initial"));
  EXPECT_TRUE(schema.column_families().count("fam_a"));
  EXPECT_TRUE(schema.column_families().count("fam_b"));
}

TEST_F(PersistentModifyColumnFamiliesRecoveryTest,
       DataRestoredAfterModifyAndReload) {
  std::vector<std::string> initial_cfs = {"fam_a", "fam_b"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> cells = {
      {"fam_a", "col1", 1000, "value_a"},
      {"fam_b", "col1", 2000, "value_b"},
  };
  ASSERT_STATUS_OK(SetCells(table, kModifyTableName, "row1", cells));

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("fam_c");
  mod->mutable_create();

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_STATUS_OK(result);

  cells = {{"fam_c", "col1", 3000, "value_c"}};
  ASSERT_STATUS_OK(SetCells(table, kModifyTableName, "row2", cells));

  table->MarkForDeletion();
  table.reset();

  auto maybe_loaded = Table::Load(kModifyTableName, "/tmp/");
  ASSERT_STATUS_OK(maybe_loaded);
  auto loaded_table = maybe_loaded.value();

  ASSERT_STATUS_OK(
      HasPersistentCell(loaded_table, "fam_a", "row1", "col1", 1000, "value_a"));
  ASSERT_STATUS_OK(
      HasPersistentCell(loaded_table, "fam_b", "row1", "col1", 2000, "value_b"));
  ASSERT_STATUS_OK(
      HasPersistentCell(loaded_table, "fam_c", "row2", "col1", 3000, "value_c"));
}

TEST_F(PersistentModifyColumnFamiliesRecoveryTest,
       DroppedColumnFamilyDataRemoved) {
  std::vector<std::string> initial_cfs = {"fam_a", "fam_b"};
  auto maybe_table = CreatePersistentTable(initial_cfs);
  ASSERT_STATUS_OK(maybe_table);
  auto table = maybe_table.value();

  std::vector<SetCellParams> cells = {
      {"fam_a", "col1", 1000, "value_a"},
      {"fam_b", "col1", 2000, "value_b"},
  };
  ASSERT_STATUS_OK(SetCells(table, kModifyTableName, "row1", cells));

  auto request = MakeModifyRequest();
  auto* mod = request.add_modifications();
  mod->set_id("fam_b");
  mod->set_drop(true);

  auto result = table->ModifyColumnFamilies(request);
  ASSERT_STATUS_OK(result);

  table->MarkForDeletion();
  table.reset();

  auto maybe_loaded = Table::Load(kModifyTableName, "/tmp/");
  ASSERT_STATUS_OK(maybe_loaded);
  auto loaded_table = maybe_loaded.value();

  ASSERT_STATUS_OK(
      HasPersistentCell(loaded_table, "fam_a", "row1", "col1", 1000, "value_a"));
  auto utils = std::static_pointer_cast<PersistentTableOperations>(
      loaded_table->GetUtilities());
  EXPECT_EQ(utils->find("fam_b"), utils->end());
}

}  // namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google