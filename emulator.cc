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

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "absl/strings/str_cat.h"
#include "server.h"
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <sstream>
#include "storage.h"
#include "cluster.h"

ABSL_FLAG(std::string, host, "localhost",
          "the address to bind to on the local machine");
ABSL_FLAG(std::uint16_t, port, 8888,
          "the port to bind to on the local machine");
ABSL_FLAG(std::string, db_path, "test_db",
          "path to the RocksDB directory used for persistence");

int main(int argc, char* argv[]) {
  namespace bt_emulator = ::google::cloud::bigtable::emulator;

  absl::SetProgramUsageMessage(
      absl::StrCat("Usage: %s --host=<host> --port=<port> "
                   "[--db_path=<rocksdb_dir>]",
                   argv[0]));
  absl::ParseCommandLine(argc, argv);
  auto const db_path = absl::GetFlag(FLAGS_db_path);
  if (db_path.empty()) {
    std::cerr << "DB path cannot be empty" << std::endl;
    return 1;
  }

  auto maybe_server =
      google::cloud::bigtable::emulator::CreateDefaultEmulatorServer(
          absl::GetFlag(FLAGS_host), absl::GetFlag(FLAGS_port));
  if (!maybe_server) {
    std::cerr << "CreateDefaultEmulatorServer() failed. See logs for "
                 "possible reason"
              << std::endl;
    return 1;
  }
  if (bt_emulator::InitGlobalStorage(db_path.c_str()) != 0) {
    fprintf(stderr, "Failed to open DB\n");
    return 1;
  }
  bt_emulator::Storage* storage = bt_emulator::GetGlobalStorage();
  auto& server = maybe_server.value();
  // Read manifest
  std::string manifest = storage->GetRow("/sys/tables/_manifest");
  if (!manifest.empty()) {
    std::istringstream iss(manifest);
    std::string table_key;
    while (std::getline(iss, table_key)) {
      table_key = bt_emulator::Trim(table_key);
      if (table_key.empty()) continue;
      std::string proto_blob = storage->GetRow(table_key);
      if (proto_blob.empty()) {
        std::cerr << "Warning: manifest references key with empty value: " << table_key << std::endl;
        continue;
      }
      google::bigtable::admin::v2::Table schema;
      if (!schema.ParseFromString(proto_blob)) {
        std::cerr << "Failed to parse Table proto from storage key: " << table_key << std::endl;
        continue;
      }

      // Create runtime Table object from the persisted schema
      auto maybe_table = google::cloud::bigtable::emulator::Table::Create(schema);
      if (!maybe_table) {
        std::cerr << "Table::Create failed for persisted schema: " << schema.DebugString()
                  << " error: " << maybe_table.status() << std::endl;
        continue;
      }

      auto cluster_ptr = server->cluster();
      if (!cluster_ptr) {
        std::cerr << "Server returned null cluster pointer" << std::endl;
        continue;
      }
      auto attach_status = cluster_ptr->AttachTable(schema.name(), maybe_table.value());
      if (!attach_status.ok()) {
        std::cerr << "Failed to attach persisted table into cluster: "
                  << attach_status << " name=" << schema.name() << std::endl;
        continue;
      }

    }
  }

  std::cout << "Server running on port " << server->bound_port() << "\n";
  server->Wait();
  bt_emulator::CloseGlobalStorage();
  return 0;
}
