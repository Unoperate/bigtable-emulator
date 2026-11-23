// Copyright 2024 Google LLC
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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_BACKEND_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_BACKEND_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "table_operations.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

struct BackendOptions {
  /// Empty selects the in-memory backend.
  std::string data_dir;
  /// Ignored for now.
  bool sync_writes = false;
  /// Ignored for now.
  bool compact_at_startup = false;
};

/// A table found in storage by Backend::Restore().
struct RestoredTable {
  std::string name;
  google::bigtable::admin::v2::Table schema;
  std::unique_ptr<TableOperations> operations;
};

/// Creates, restores and destroys table storage. Owned by Registry.
class Backend {
 public:
  /// In-memory if `options.data_dir` is empty, persistent otherwise.
  static StatusOr<std::unique_ptr<Backend>> Create(
      BackendOptions const& options);
  virtual ~Backend() = default;

  /// Opens the tables found in storage. Called once, at startup.
  virtual StatusOr<std::vector<RestoredTable>> Restore() = 0;

  /// UNAVAILABLE means the outcome is unknown until restart; the Registry
  /// keeps the name blocked. Any other error means the table was not created;
  /// its leftovers are removed at the next startup.
  virtual StatusOr<std::unique_ptr<TableOperations>> CreateTable(
      google::bigtable::admin::v2::Table const& schema) = 0;

  /// Frees the storage of a table detached by Table::Delete().
  virtual void DestroyTable(std::unique_ptr<TableOperations> operations) = 0;

  /// Called at shutdown before the tables are closed.
  virtual void StopBackgroundWork() = 0;

  /// Called at shutdown after the tables are closed.
  virtual Status Shutdown() = 0;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_BACKEND_H
