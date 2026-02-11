/**
 * @file persist/utils/test_utils.cc
 * @brief Definitions for test utilities shared by persistence tests.
 */

#include "persist/utils/test_utils.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

template <typename StorageT>
void StorageTestManager<StorageT>::setStorage(
    std::shared_ptr<StorageT>&& new_storage) {
  storage = std::move(new_storage);
}

template <typename StorageT>
std::string const StorageTestManager<StorageT>::testTablePrefix() const {
  return test_table_prefix;
}

template <typename StorageT>
std::shared_ptr<StorageT> StorageTestManager<StorageT>::getStorage() const {
  return storage;
}

template <typename StorageT>
std::string StorageTestManager<StorageT>::testTableName(
    std::string const& name) const {
  return absl::StrCat(testTablePrefix(), "/tables/", name);
}

template <typename StorageT>
IntegrationServer StorageTestManager<StorageT>::RunServer() {
  return IntegrationServer(test_table_uid, storage);
}

template <typename StorageT>
rows_dump StorageTestManager<StorageT>::getTableRowsDump(
    std::string const& table_name) {
  rows_dump vals;
  auto stream = storage->StreamTableFull(table_name).value();
  DBG("[TestUtils][getTableRowsDump] table={} stream.HasValue()={}", table_name,
      stream.HasValue());
  for (; stream.HasValue(); stream.Next(NextMode::kCell)) {
    auto& v = stream.Value();
    auto row_msg = absl::StrCat(v.column_family(), ".", v.row_key(), ".",
                                v.column_qualifier());
    vals.push_back(std::make_tuple(row_msg, v.timestamp(), v.value()));
  }
  return vals;
}

template <typename StorageT>
std::string StorageTestManager<StorageT>::createTestTable(
    std::vector<std::string> const column_family_names) {
  auto const table_name =
      testTableName(absl::StrCat("table_", test_table_uid));
  ++test_table_uid;
  ::google::bigtable::admin::v2::Table schema;
  schema.set_name(table_name);
  for (auto& column_family_name : column_family_names) {
    (*schema.mutable_column_families())[column_family_name] =
        ::google::bigtable::admin::v2::ColumnFamily();
  }
  auto create_table_status = storage->CreateTable(schema);
  if (!create_table_status.ok()) {
    DBG("[TestUtils][createTestTable] CreateTable failed table={} error={}",
        table_name, create_table_status.message());
  }
  assert(create_table_status.ok());
  return table_name;
}

template <typename StorageT>
uint64_t StorageTestManager<StorageT>::toMicros(
    std::chrono::milliseconds const& millis) {
  return std::chrono::duration_cast<std::chrono::microseconds>(millis).count();
}

template <typename StorageT>
std::chrono::milliseconds StorageTestManager<StorageT>::now() {
  std::chrono::time_point<std::chrono::system_clock> now =
      std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(duration);
}

IntegrationServer::IntegrationServer(size_t test_uid,
                                     std::shared_ptr<Storage> storage)
    : storage_(std::move(storage)),
      port_(static_cast<uint16_t>(8080 + test_uid)),
      host_("0.0.0.0") {
  auto maybe_emulator =
      google::cloud::bigtable::emulator::CreateDefaultEmulatorServer(
          host_, port_, storage_);
  assert(maybe_emulator.ok());
  emulator_ = std::move(maybe_emulator.value());
  worker_ = std::thread([this]() { this->emulator_->Wait(); });
}

Options IntegrationServer::getClientOptions() {
  return Options{}
      .set<EndpointOption>(fmt::format("{}:{}", host_, port_))
      .set<LoggingComponentsOption>(
          std::set<std::string>{"rpc", "rpc-streams", "auth"})
      .set<UnifiedCredentialsOption>(MakeInsecureCredentials());
}

void IntegrationServer::Kill() {
  if (!active_) {
    return;
  }
  emulator_->Shutdown();
  if (worker_.joinable()) {
    worker_.join();
  }
  active_ = false;
}

IntegrationServer::~IntegrationServer() { Kill(); }

cbt::Table IntegrationServer::Table(std::string project_id,
                                    std::string instance_id,
                                    std::string table_id) {
  return cbt::Table(cbt::MakeDataConnection(getClientOptions()),
                    cbt::TableResource(std::move(project_id),
                                       std::move(instance_id),
                                       std::move(table_id)));
}

cbta::BigtableTableAdminClient IntegrationServer::Client() {
  return cbta::BigtableTableAdminClient(
      cbta::MakeBigtableTableAdminConnection(getClientOptions()));
}

// Explicit template instantiations for the storage types used in tests.
template class StorageTestManager<MemoryStorage>;
template class StorageTestManager<RocksDBStorage>;

MemoryStorageTestManager::MemoryStorageTestManager() {
  setStorage(std::make_shared<MemoryStorage>());
}

RocksDBStorageTestManager::RocksDBStorageTestManager() {
  test_storage_path = (std::filesystem::temp_directory_path() /= "bte_test") /=
                      std::tmpnam(nullptr);
  // Attempt to create the directory.
  std::filesystem::create_directories(test_storage_path);

  storage_config.set_db_path(test_storage_path);
  storage_config.set_meta_column_family("bte_metadata");
  setStorage(std::make_shared<RocksDBStorage>(storage_config));
  assert(getStorage()->Open().ok());
}

Status RocksDBStorageTestManager::reconnect() {
  auto close_status = getStorage()->Close();
  if (!close_status.ok()) {
    return close_status;
  }
  getStorage().reset();
  setStorage(std::make_unique<RocksDBStorage>(storage_config));
  auto open_status = getStorage()->Open();
  if (!open_status.ok()) {
    return open_status;
  }
  return Status();
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

