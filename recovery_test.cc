#include "google/cloud/testing_util/status_matchers.h"
#include "backend.h"
#include "column_family.h"
#include "filter.h"
#include "persistent/persistent_backend.h"
#include "persistent/table_metadata.pb.h"
#include "re2/re2.h"
#include "registry.h"
#include "rocksdb/db.h"
#include "server.h"
#include "table.h"
#include "test_util.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.grpc.pb.h>
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <gmock/gmock.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/status.h>
#include <gtest/gtest.h>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {
namespace {

BackendOptions PersistentOptions() {
  BackendOptions options;
  options.data_dir = TestDataDir();
  return options;
}

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
    // The server restores the tables it finds at startup.
    std::filesystem::remove_all(TestDataDir());
    auto maybe_server =
        CreateDefaultEmulatorServer("127.0.0.1", 0, PersistentOptions());
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
  void SetUp() override { std::filesystem::remove_all(TestDataDir()); }

  void TearDown() override { std::filesystem::remove_all(TestDataDir()); }

  static StatusOr<std::shared_ptr<Table>> CreatePersistentTable(
      std::vector<std::string> const& column_families) {
    std::vector<std::string> cfs = column_families;
    return CreateTable(kModifyTableName, cfs, true);
  }

  static google::bigtable::admin::v2::ModifyColumnFamiliesRequest
  MakeModifyRequest() {
    google::bigtable::admin::v2::ModifyColumnFamiliesRequest request;
    request.set_name(kModifyTableName);
    return request;
  }
};

TEST_F(ServerTest, RecoverEmptyTable) {
  std::string const table_name_short = "recovery";
  std::string const table_name_long =
      "projects/recovery/instances/recovery/tables/recovery";
  grpc::Status status;
  {
    google::bigtable::admin::v2::CreateTableRequest request;
    request.set_table_id(table_name_short);
    request.set_parent("projects/recovery/instances/recovery");
    google::bigtable::admin::v2::Table response;
    grpc::ClientContext context;
    ASSERT_EQ(
        true,
        TableAdminClient()->CreateTable(&context, request, &response).ok());
  }

  server_.reset();
  auto maybe_server =
      CreateDefaultEmulatorServer("127.0.0.1", 0, PersistentOptions());
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

  std::filesystem::remove_all(TestDataDir());
}

TEST_F(ServerTest, RecoverTableWithColumnFamilies) {
  std::string const table_name_short = "recovery";
  std::string const table_name_long =
      "projects/recovery/instances/recovery/tables/recovery";
  {
    google::bigtable::admin::v2::CreateTableRequest request;
    request.set_table_id(table_name_short);
    request.set_parent("projects/recovery/instances/recovery");
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
  auto maybe_server =
      CreateDefaultEmulatorServer("127.0.0.1", 0, PersistentOptions());
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
  maybe_server =
      CreateDefaultEmulatorServer("127.0.0.1", 0, PersistentOptions());
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
  maybe_server =
      CreateDefaultEmulatorServer("127.0.0.1", 0, PersistentOptions());
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
        if (chunk.value() == "value_a") {
          found_a = true;
        }
        if (chunk.value() == "value_b") {
          found_b = true;
        }
      }
    }

    grpc::Status status = reader->Finish();
    ASSERT_TRUE(status.ok());

    ASSERT_TRUE(found_a);
    ASSERT_TRUE(found_b);
  }

  std::filesystem::remove_all(TestDataDir());
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

  ASSERT_STATUS_OK(table->Close());
  table.reset();

  auto maybe_loaded = LoadTable(kModifyTableName);
  ASSERT_STATUS_OK(maybe_loaded);
  auto const& loaded_table = maybe_loaded.value();

  auto schema = *loaded_table->Schema();
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

  ASSERT_STATUS_OK(table->Close());
  table.reset();

  auto maybe_loaded = LoadTable(kModifyTableName);
  ASSERT_STATUS_OK(maybe_loaded);
  auto loaded_table = maybe_loaded.value();

  ASSERT_STATUS_OK(HasPersistentCell(loaded_table, "fam_a", "row1", "col1",
                                     1000, "value_a"));
  ASSERT_STATUS_OK(HasPersistentCell(loaded_table, "fam_b", "row1", "col1",
                                     2000, "value_b"));
  ASSERT_STATUS_OK(HasPersistentCell(loaded_table, "fam_c", "row2", "col1",
                                     3000, "value_c"));
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

  ASSERT_STATUS_OK(table->Close());
  table.reset();

  auto maybe_loaded = LoadTable(kModifyTableName);
  ASSERT_STATUS_OK(maybe_loaded);
  auto loaded_table = maybe_loaded.value();

  ASSERT_STATUS_OK(HasPersistentCell(loaded_table, "fam_a", "row1", "col1",
                                     1000, "value_a"));
  auto* utils =
      static_cast<PersistentTableOperations*>(loaded_table->GetUtilities());
  EXPECT_EQ(utils->find("fam_b"), utils->end());
}

TEST(PersistentTableDirectoryTest, CreateTableInNewBackendKeepsExistingTable) {
  std::string const first_name = "projects/p/instances/i/tables/first";
  std::string const second_name = "projects/p/instances/i/tables/second";
  std::vector<std::string> no_families;

  auto first = CreateTable(first_name, no_families, true);
  ASSERT_STATUS_OK(first);
  ASSERT_STATUS_OK((*first)->Close());

  auto second = CreateTable(second_name, no_families, true);
  ASSERT_STATUS_OK(second);
  ASSERT_STATUS_OK((*second)->Close());

  EXPECT_TRUE(
      std::filesystem::exists(TestDataDir() + "/tables/000001/CURRENT"));
  EXPECT_TRUE(
      std::filesystem::exists(TestDataDir() + "/tables/000002/CURRENT"));
  EXPECT_STATUS_OK(LoadTable(first_name).status());
  EXPECT_STATUS_OK(LoadTable(second_name).status());
}

using ::google::cloud::testing_util::StatusIs;

constexpr char const* kTableMetadataKey = "t_emulator:meta:table_metadata_pb";

using ColumnFamilyHandles = std::map<std::string, rocksdb::ColumnFamilyHandle*>;

/// Opens the database in `db_path` with plain RocksDB and every column family
/// it has, runs `f` on it, then closes it. Lets a test leave the state a crash
/// would.
void WithDatabase(
    std::string const& db_path,
    std::function<void(rocksdb::DB&, ColumnFamilyHandles const&)> const& f) {
  std::vector<std::string> names;
  ASSERT_TRUE(
      rocksdb::DB::ListColumnFamilies(rocksdb::Options(), db_path, &names)
          .ok());
  std::vector<rocksdb::ColumnFamilyDescriptor> descs;
  descs.reserve(names.size());
  for (auto const& name : names) {
    descs.emplace_back(name, rocksdb::ColumnFamilyOptions());
  }
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::DB* raw_db = nullptr;
  ASSERT_TRUE(
      rocksdb::DB::Open(rocksdb::Options(), db_path, descs, &handles, &raw_db)
          .ok());
  std::unique_ptr<rocksdb::DB> db(raw_db);
  ColumnFamilyHandles handles_by_name;
  for (std::size_t i = 0; i < names.size(); ++i) {
    handles_by_name.emplace(names[i], handles[i]);
  }
  f(*db, handles_by_name);
  for (auto* handle : handles) {
    ASSERT_TRUE(db->DestroyColumnFamilyHandle(handle).ok());
  }
}

void WithDatabase(std::string const& db_path,
                  std::function<void(rocksdb::DB&)> const& f) {
  WithDatabase(db_path,
               [&f](rocksdb::DB& db, ColumnFamilyHandles const&) { f(db); });
}

TableMetadata ReadMetadata(rocksdb::DB& db) {
  std::string bytes;
  EXPECT_TRUE(db.Get(rocksdb::ReadOptions(), kTableMetadataKey, &bytes).ok());
  TableMetadata metadata;
  EXPECT_TRUE(metadata.ParseFromString(bytes));
  return metadata;
}

TEST(PersistentTableRestoreTest, RestoreDropsUnnamedColumnFamily) {
  std::string const table_name = "projects/p/instances/i/tables/t";
  std::vector<std::string> families = {"f"};
  auto table = CreateTable(table_name, families, true);
  ASSERT_STATUS_OK(table);
  ASSERT_STATUS_OK((*table)->Close());

  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [](rocksdb::DB& db) {
    rocksdb::ColumnFamilyHandle* handle = nullptr;
    ASSERT_TRUE(db.CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
                                      "cf_orphan", &handle)
                    .ok());
    ASSERT_TRUE(db.DestroyColumnFamilyHandle(handle).ok());
  }));

  auto restored = LoadTable(table_name);
  ASSERT_STATUS_OK(restored);
  ASSERT_STATUS_OK((*restored)->Close());

  std::vector<std::string> names;
  ASSERT_TRUE(
      rocksdb::DB::ListColumnFamilies(rocksdb::Options(), db_path, &names)
          .ok());
  EXPECT_THAT(names, ::testing::Contains(RocksDbColumnFamilyName("f")));
  EXPECT_THAT(names, ::testing::Not(::testing::Contains("cf_orphan")));
}

TEST(PersistentTableRestoreTest, RestoreReplaysIntent) {
  std::string const table_name = "projects/p/instances/i/tables/t";
  std::vector<std::string> families = {"f"};
  auto table = CreateTable(table_name, families, true);
  ASSERT_STATUS_OK(table);
  std::vector<SetCellParams> cells = {{"f", "c", 1000, "v"}};
  ASSERT_STATUS_OK(SetCells(*table, table_name, "row", cells));
  ASSERT_STATUS_OK((*table)->Close());

  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [](rocksdb::DB& db) {
    auto metadata = ReadMetadata(db);
    metadata.mutable_intent()->add_families_to_empty("f");
    ASSERT_TRUE(db.Put(rocksdb::WriteOptions(), kTableMetadataKey,
                       metadata.SerializeAsString())
                    .ok());
  }));

  auto restored = LoadTable(table_name);
  ASSERT_STATUS_OK(restored);
  EXPECT_THAT(HasPersistentRow(*restored, "f", "row"),
              StatusIs(StatusCode::kNotFound));
  ASSERT_STATUS_OK((*restored)->Close());

  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [](rocksdb::DB& db) {
    EXPECT_FALSE(ReadMetadata(db).has_intent());
  }));
}

TEST(PersistentTableRestoreTest, RestoreReplaysIntentForDroppedFamily) {
  std::string const table_name = "projects/p/instances/i/tables/t";
  std::vector<std::string> families = {"f"};
  auto table = CreateTable(table_name, families, true);
  ASSERT_STATUS_OK(table);
  std::vector<SetCellParams> cells = {{"f", "c", 1000, "v"}};
  ASSERT_STATUS_OK(SetCells(*table, table_name, "row", cells));
  ASSERT_STATUS_OK((*table)->Close());

  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(WithDatabase(
      db_path, [](rocksdb::DB& db, ColumnFamilyHandles const& handles) {
        auto metadata = ReadMetadata(db);
        metadata.mutable_intent()->add_families_to_empty("f");
        ASSERT_TRUE(db.Put(rocksdb::WriteOptions(), kTableMetadataKey,
                           metadata.SerializeAsString())
                        .ok());
        ASSERT_TRUE(
            db.DropColumnFamily(handles.at(RocksDbColumnFamilyName("f"))).ok());
      }));

  auto restored = LoadTable(table_name);
  ASSERT_STATUS_OK(restored);
  EXPECT_THAT(HasPersistentRow(*restored, "f", "row"),
              StatusIs(StatusCode::kNotFound));
  ASSERT_STATUS_OK((*restored)->Close());

  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [](rocksdb::DB& db) {
    EXPECT_FALSE(ReadMetadata(db).has_intent());
  }));
}

TEST(PersistentTableRestoreTest, RestoreFailsOnIntentForFamilyNotInSchema) {
  std::string const table_name = "projects/p/instances/i/tables/t";
  std::vector<std::string> families = {"f", "g"};
  auto table = CreateTable(table_name, families, true);
  ASSERT_STATUS_OK(table);
  ASSERT_STATUS_OK((*table)->Close());

  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [](rocksdb::DB& db) {
    auto metadata = ReadMetadata(db);
    metadata.mutable_schema()->mutable_column_families()->erase("g");
    metadata.mutable_intent()->add_families_to_empty("g");
    ASSERT_TRUE(db.Put(rocksdb::WriteOptions(), kTableMetadataKey,
                       metadata.SerializeAsString())
                    .ok());
  }));

  EXPECT_THAT(LoadTable(table_name), StatusIs(StatusCode::kDataLoss));
  std::vector<std::string> names;
  ASSERT_TRUE(
      rocksdb::DB::ListColumnFamilies(rocksdb::Options(), db_path, &names)
          .ok());
  EXPECT_THAT(names, ::testing::Contains(RocksDbColumnFamilyName("g")));
}

TEST(PersistentTableRestoreTest, RestoreFailsOnMissingColumnFamily) {
  std::string const table_name = "projects/p/instances/i/tables/t";
  std::vector<std::string> families = {"f"};
  auto table = CreateTable(table_name, families, true);
  ASSERT_STATUS_OK(table);
  ASSERT_STATUS_OK((*table)->Close());

  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [](rocksdb::DB& db) {
    auto metadata = ReadMetadata(db);
    (*metadata.mutable_schema()->mutable_column_families())["g"] =
        ::google::bigtable::admin::v2::ColumnFamily();
    ASSERT_TRUE(db.Put(rocksdb::WriteOptions(), kTableMetadataKey,
                       metadata.SerializeAsString())
                    .ok());
  }));

  EXPECT_THAT(LoadTable(table_name), StatusIs(StatusCode::kDataLoss));
}

constexpr char const* kRestoredTableName = "projects/p/instances/i/tables/t";

void CreateClosedTable(std::string const& table_name) {
  std::vector<std::string> no_families;
  auto table = CreateTable(table_name, no_families, true);
  ASSERT_STATUS_OK(table);
  ASSERT_STATUS_OK((*table)->Close());
}

void SetState(std::string const& db_path, State state) {
  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [state](rocksdb::DB& db) {
    auto metadata = ReadMetadata(db);
    metadata.set_state(state);
    ASSERT_TRUE(db.Put(rocksdb::WriteOptions(), kTableMetadataKey,
                       metadata.SerializeAsString())
                    .ok());
  }));
}

StatusOr<std::vector<RestoredTable>> RestoreTables() {
  BackendOptions options;
  options.data_dir = TestDataDir();
  auto backend = PersistentBackend::Open(options);
  if (!backend) {
    return backend.status();
  }
  return (*backend)->Restore();
}

TEST(PersistentBackendRestoreTest, RestoreDestroysCreatingTable) {
  ASSERT_NO_FATAL_FAILURE(CreateClosedTable(kRestoredTableName));
  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(SetState(db_path, STATE_CREATING));

  auto restored = RestoreTables();
  ASSERT_STATUS_OK(restored);
  EXPECT_TRUE(restored->empty());
  EXPECT_FALSE(std::filesystem::exists(db_path));
}

TEST(PersistentBackendRestoreTest, RestoreFailsOnInvalidSchema) {
  std::vector<std::string> families = {"f"};
  auto table = CreateTable(kRestoredTableName, families, true);
  ASSERT_STATUS_OK(table);
  ASSERT_STATUS_OK((*table)->Close());
  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [](rocksdb::DB& db) {
    auto metadata = ReadMetadata(db);
    (*metadata.mutable_schema()->mutable_column_families())["f"]
        .mutable_value_type()
        ->mutable_bytes_type();
    ASSERT_TRUE(db.Put(rocksdb::WriteOptions(), kTableMetadataKey,
                       metadata.SerializeAsString())
                    .ok());
  }));

  EXPECT_THAT(RestoreTables(), StatusIs(StatusCode::kDataLoss));
}

TEST(PersistentBackendRestoreTest, RestoreDestroysDeletingTable) {
  ASSERT_NO_FATAL_FAILURE(CreateClosedTable(kRestoredTableName));
  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(SetState(db_path, STATE_DELETING));

  auto restored = RestoreTables();
  ASSERT_STATUS_OK(restored);
  EXPECT_TRUE(restored->empty());
  EXPECT_FALSE(std::filesystem::exists(db_path));
}

TEST(PersistentBackendRestoreTest, RestoreDestroysTableWithoutMetadata) {
  ASSERT_NO_FATAL_FAILURE(CreateClosedTable(kRestoredTableName));
  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [](rocksdb::DB& db) {
    ASSERT_TRUE(db.Delete(rocksdb::WriteOptions(), kTableMetadataKey).ok());
  }));

  auto restored = RestoreTables();
  ASSERT_STATUS_OK(restored);
  EXPECT_TRUE(restored->empty());
  EXPECT_FALSE(std::filesystem::exists(db_path));
}

TEST(PersistentBackendRestoreTest, RestoreRemovesDirectoryWithoutCurrent) {
  auto const db_path = TestDataDir() + "/tables/000007";
  std::filesystem::create_directories(db_path);
  std::ofstream(db_path + "/LOG") << "leftover\n";

  auto restored = RestoreTables();
  ASSERT_STATUS_OK(restored);
  EXPECT_TRUE(restored->empty());
  EXPECT_FALSE(std::filesystem::exists(db_path));
}

TEST(PersistentBackendRestoreTest, RestoreKeepsEntryNotNamedLikeTableDir) {
  auto const entry = TestDataDir() + "/tables/+7";
  std::filesystem::create_directories(entry);

  auto restored = RestoreTables();
  ASSERT_STATUS_OK(restored);
  EXPECT_TRUE(restored->empty());
  EXPECT_TRUE(std::filesystem::exists(entry));
}

TEST(PersistentBackendRestoreTest, RestoreRemovesDatabaseWithoutCurrent) {
  ASSERT_NO_FATAL_FAILURE(CreateClosedTable(kRestoredTableName));
  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_TRUE(std::filesystem::remove(db_path + "/CURRENT"));

  auto restored = RestoreTables();
  ASSERT_STATUS_OK(restored);
  EXPECT_TRUE(restored->empty());
  EXPECT_FALSE(std::filesystem::exists(db_path));
}

TEST(PersistentBackendRestoreTest, RestoreFailsOnDatabaseThatDoesNotOpen) {
  ASSERT_NO_FATAL_FAILURE(CreateClosedTable(kRestoredTableName));
  auto const db_path = TestDataDir() + "/tables/000001";
  std::ofstream(db_path + "/CURRENT", std::ios::trunc) << "garbage\n";

  EXPECT_THAT(RestoreTables(), StatusIs(StatusCode::kDataLoss));
  EXPECT_TRUE(std::filesystem::exists(db_path + "/CURRENT"));
}

TEST(PersistentBackendRestoreTest, RestoreFailsOnUnspecifiedState) {
  ASSERT_NO_FATAL_FAILURE(CreateClosedTable(kRestoredTableName));
  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_NO_FATAL_FAILURE(SetState(db_path, STATE_UNSPECIFIED));

  EXPECT_THAT(RestoreTables(), StatusIs(StatusCode::kFailedPrecondition));
  EXPECT_TRUE(std::filesystem::exists(db_path));
}

TEST(PersistentBackendRestoreTest, RestoreFailsOnTwoTablesWithOneName) {
  ASSERT_NO_FATAL_FAILURE(CreateClosedTable("projects/p/instances/i/tables/a"));
  ASSERT_NO_FATAL_FAILURE(CreateClosedTable("projects/p/instances/i/tables/b"));
  auto const db_path = TestDataDir() + "/tables/000002";
  ASSERT_NO_FATAL_FAILURE(WithDatabase(db_path, [](rocksdb::DB& db) {
    auto metadata = ReadMetadata(db);
    metadata.mutable_schema()->set_name("projects/p/instances/i/tables/a");
    ASSERT_TRUE(db.Put(rocksdb::WriteOptions(), kTableMetadataKey,
                       metadata.SerializeAsString())
                    .ok());
  }));

  EXPECT_THAT(RestoreTables(), StatusIs(StatusCode::kFailedPrecondition));
}

TEST(PersistentRegistryTest, DeleteTableRemovesDirectory) {
  BackendOptions options;
  options.data_dir = TestDataDir();
  auto backend = Backend::Create(options);
  ASSERT_STATUS_OK(backend);
  auto registry = Registry::Create(*std::move(backend));
  ASSERT_STATUS_OK(registry);
  ASSERT_STATUS_OK((*registry)->CreateTable(
      kRestoredTableName, google::bigtable::admin::v2::Table()));
  auto const db_path = TestDataDir() + "/tables/000001";
  ASSERT_TRUE(std::filesystem::exists(db_path));

  ASSERT_STATUS_OK((*registry)->DeleteTable(kRestoredTableName));
  EXPECT_FALSE(std::filesystem::exists(db_path));
}

TEST(PersistentRegistryTest, CreateTableConcurrentlyRestoresBoth) {
  std::string const table_a = "projects/p/instances/i/tables/a";
  std::string const table_b = "projects/p/instances/i/tables/b";
  {
    BackendOptions options;
    options.data_dir = TestDataDir();
    auto backend = Backend::Create(options);
    ASSERT_STATUS_OK(backend);
    auto registry = Registry::Create(*std::move(backend));
    ASSERT_STATUS_OK(registry);

    auto create = [&](std::string const& name) {
      return (*registry)
          ->CreateTable(name, google::bigtable::admin::v2::Table())
          .status();
    };
    Status a_status;
    Status b_status;
    std::thread a([&] { a_status = create(table_a); });
    std::thread b([&] { b_status = create(table_b); });
    a.join();
    b.join();
    ASSERT_STATUS_OK(a_status);
    ASSERT_STATUS_OK(b_status);
  }

  auto restored = RestoreTables();
  ASSERT_STATUS_OK(restored);
  std::vector<std::string> names;
  for (auto const& table : *restored) {
    names.push_back(table.name);
  }
  EXPECT_THAT(names, ::testing::UnorderedElementsAre(table_a, table_b));
}

}  // namespace
}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
