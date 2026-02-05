/**
 * @file persist/utils/test_utils.h
 * @brief Test utilities for Bigtable emulator persistence backends.
 *
 * This header provides common helper macros and classes used in tests for
 * storage backends (in‑memory and RocksDB) as well as integration tests
 * against the Bigtable emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_PERSIST_UTILS_TEST_UTILS
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_PERSIST_UTILS_TEST_UTILS

#include <gtest/gtest.h>

#include "filter.h"
#include "persist/memory/storage.h"
#include "persist/rocksdb/storage.h"
#include "server.h"

#include "google/cloud/bigtable/admin/bigtable_table_admin_client.h"
#include "google/cloud/bigtable/resource_names.h"
#include "google/cloud/bigtable/table.h"
#include "google/cloud/credentials.h"

#include "fmt/format.h"
#include "fmt/ranges.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

/**
 * @brief Expect that the table names under a prefix match the given list.
 *
 * The macro assumes there is a `storage` pointer in scope that provides
 * `Tables(PREFIX).names()`.
 *
 * @param PREFIX The table name prefix to query.
 * @param ... Expected table names as a parameter pack.
 */
#define EXPECT_TABLE_NAMES_PREFIX(PREFIX, ...) \
  EXPECT_EQ((storage->Tables(PREFIX).names()), \
            (std::vector<std::string>{__VA_ARGS__}));

/**
 * @brief Assert that a `StatusOr<T>`-like expression evaluates to OK.
 *
 * On failure, the contained message is compared with an empty string to make
 * debugging test failures easier.
 *
 * @param VALUE Expression returning an object with `ok()` and `message()`.
 */
#define EXPECT_OK(VALUE)          \
  if (true) {                     \
    auto v = (VALUE);             \
    if (!v.ok()) {                \
      EXPECT_EQ(v.message(), ""); \
    }                             \
  };

/**
 * @brief Assert that a `StatusOr<T>`-like expression evaluates to OK.
 *
 * Compared to EXPECT_OK, this variant prints the full status using `fmt`.
 *
 * @param VALUE Expression returning an object with `ok()` and `status()`.
 */
#define EXPECT_OK_STATUS(VALUE)                                    \
  if (true) {                                                      \
    auto v = (VALUE);                                              \
    if (!v.ok()) {                                                 \
      EXPECT_EQ(fmt::format("{}", v.status()), "OK");              \
    }                                                              \
  };

/**
 * @brief Expect that the rows of a table managed by a `StorageTestManager`
 *        match an expected dump.
 *
 * @param MANAGER A `StorageTestManager` instance.
 * @param TABLE_NAME Name of the table to dump.
 * @param ... Expected rows as `rows_dump` entries.
 */
#define EXPECT_ROWS(MANAGER, TABLE_NAME, ...) \
  EXPECT_EQ(((MANAGER).getTableRowsDump(TABLE_NAME)), (rows_dump{__VA_ARGS__}));

/**
 * @brief Expect that the rows in a Cloud Bigtable `Table` match an expected
 *        list of `(qualified_cell_id, value)` pairs.
 *
 * This macro iterates over all rows in the table and collects cells into a
 * `std::vector<std::pair<std::string, std::string>>`, then compares it with
 * the expected initializer list.
 *
 * @param TABLE A `cbt::Table` instance.
 * @param ... Expected `(key, value)` pairs.
 */
#define EXPECT_ROWS_CBT(TABLE, ...)                                                \
  if (true) {                                                                      \
    std::vector<std::pair<std::string, std::string>> dumped_rows;                 \
    for (auto& row : (TABLE).ReadRows(cbt::RowRange::InfiniteRange(),             \
                                      cbt::Filter::PassAllFilter())) {            \
      EXPECT_OK_STATUS(row);                                                      \
      for (cbt::Cell const& c : row->cells()) {                                   \
        dumped_rows.push_back(std::make_pair(                                     \
            fmt::format("{}.{}.{}", c.family_name(), c.row_key(),                 \
                        c.column_qualifier()),                                    \
            c.value()));                                                          \
      }                                                                           \
    }                                                                             \
    EXPECT_EQ(dumped_rows,                                                        \
              (std::vector<std::pair<std::string, std::string>>{__VA_ARGS__}));   \
  }

/**
 * @brief Sleep for a short, fixed amount of time.
 *
 * Useful in integration tests that need to wait for eventual consistency.
 */
#define WAIT() \
  std::this_thread::sleep_for(std::chrono::milliseconds(250));

/**
 * @brief Helper macro to create a row transaction or fail the test.
 *
 * The macro calls `RowTransaction` on the given storage and asserts that the
 * returned status is OK, returning the created transaction.
 *
 * @param STORAGE Pointer (or smart pointer) to a storage implementation.
 * @param ... Arguments forwarded to `RowTransaction`.
 */
#define TXN(STORAGE, ...)                     \
  ([&]() -> auto {                            \
    auto maybeTxn = (STORAGE)->RowTransaction(__VA_ARGS__); \
    if (!maybeTxn.ok()) {                     \
      LERROR("Transaction creation failed in test"); \
      assert(false);                          \
    }                                         \
    return std::move(maybeTxn.value());       \
  }())

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/// Convenient alias representing a dump of table rows produced in tests.
using rows_dump = std::vector<
    std::tuple<std::string, std::chrono::milliseconds, std::string>>;

namespace cbt = ::google::cloud::bigtable;
namespace cbta = ::google::cloud::bigtable_admin;

template <typename StorageT>
class StorageTestManager;

/**
 * @brief Helper class to start and manage an emulator-based integration server.
 *
 * The server is started on construction and shut down automatically on
 * destruction via `Kill()`.
 */
class IntegrationServer {
  template <typename StorageT>
  friend class StorageTestManager;

 private:
  std::shared_ptr<Storage> storage_;
  uint16_t port_;
  std::string host_;
  std::unique_ptr<EmulatorServer> emulator_;
  std::thread worker_;
  bool active_ = true;

  /**
   * @brief Construct an integration server bound to a given storage backend.
   *
   * @param test_uid Unique identifier used to select the listening port.
   * @param storage Shared pointer to the storage backend instance.
   */
  explicit IntegrationServer(size_t test_uid,
                             std::shared_ptr<Storage> storage);

  /**
   * @brief Build client options for connecting to the emulator server.
   *
   * @return Configured `Options` instance.
   */
  Options getClientOptions();

 public:
  /**
   * @brief Stop the emulator server, if still running.
   *
   * It is safe to call this multiple times.
   */
  void Kill();

  /// Destructor stops the emulator server if still active.
  ~IntegrationServer();

  /**
   * @brief Create a `cbt::Table` client for the given identifiers.
   *
   * @param project_id GCP project identifier.
   * @param instance_id Bigtable instance identifier.
   * @param table_id Table identifier.
   * @return A `cbt::Table` bound to the emulator endpoint.
   */
  cbt::Table Table(std::string project_id, std::string instance_id,
                   std::string table_id);

  /**
   * @brief Create a Bigtable table admin client bound to the emulator.
   *
   * @return A `cbta::BigtableTableAdminClient` instance.
   */
  cbta::BigtableTableAdminClient Client();
};

/**
 * @brief Base helper for tests that exercise a particular storage backend.
 *
 * The template is parameterized on the storage type and provides convenience
 * utilities for creating tables and inspecting their contents.
 *
 * @tparam StorageT The concrete storage backend type.
 */
template <typename StorageT>
class StorageTestManager {
 private:
  std::shared_ptr<StorageT> storage;
  std::string const test_table_prefix = "projects/test/instances/test";
  size_t test_table_uid = 1;

 protected:
  /**
   * @brief Install a storage backend to be used by subsequent operations.
   *
   * @param new_storage New storage shared pointer.
   */
  void setStorage(std::shared_ptr<StorageT>&& new_storage);

 public:
  /// @brief Return the test table name prefix.
  std::string const testTablePrefix() const;

  /// @brief Return the underlying storage backend.
  std::shared_ptr<StorageT> getStorage() const;

  /**
   * @brief Build a fully-qualified table name for a short name.
   *
   * @param name Short table name suffix.
   * @return Fully-qualified test table name.
   */
  std::string testTableName(std::string const& name) const;

  /**
   * @brief Start an `IntegrationServer` using the current storage backend.
   *
   * Each invocation uses an incrementing identifier to pick a different port.
   *
   * @return Newly started `IntegrationServer` instance.
   */
  IntegrationServer RunServer();

  /**
   * @brief Dump all rows of a table into a `rows_dump` container.
   *
   * @param table_name Fully-qualified table name.
   * @return Collected rows.
   */
  rows_dump getTableRowsDump(std::string const& table_name);

  /**
   * @brief Create a new test table with optional column families.
   *
   * @param column_family_names Names of column families to add to the schema.
   * @return Name of the created table.
   */
  std::string createTestTable(
      std::vector<std::string> const column_family_names = {});

  /**
   * @brief Convert a millisecond duration to microseconds.
   *
   * @param millis Duration in milliseconds.
   * @return Duration in microseconds.
   */
  static uint64_t toMicros(std::chrono::milliseconds const& millis);

  /**
   * @brief Return the current wall-clock time in milliseconds since epoch.
   *
   * @return Current timestamp in milliseconds.
   */
  std::chrono::milliseconds now();
};

/**
 * @brief Test manager configured with an in‑memory storage backend.
 */
class MemoryStorageTestManager : public StorageTestManager<MemoryStorage> {
 public:
  /// @brief Construct a manager using a freshly created `MemoryStorage`.
  explicit MemoryStorageTestManager();
};

/**
 * @brief Test manager configured with a RocksDB‑backed storage backend.
 *
 * This helper manages creation of a temporary RocksDB database directory and
 * provides a `reconnect()` helper that reopens the database.
 */
class RocksDBStorageTestManager
    : public StorageTestManager<RocksDBStorage> {
 private:
  storage::StorageRocksDBConfig storage_config;
  std::filesystem::path test_storage_path;

 public:
  /// @brief Construct a manager using a temporary RocksDB storage directory.
  explicit RocksDBStorageTestManager();

  /**
   * @brief Close and reopen the underlying RocksDB storage.
   *
   * @return OK status on success, or the first error encountered.
   */
  Status reconnect();
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_PERSIST_UTILS_TEST_UTILS

