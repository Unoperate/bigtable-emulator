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
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

// TODO(prawilny): replace the polling and sigaction with a real shutdown
// implementation that also works on Windows.
namespace {
std::sig_atomic_t volatile shutdown_requested = 0;
}  // namespace

/// Requests a clean shutdown. SA_RESETHAND restores the default action, so a
/// second signal terminates the process.
extern "C" void HandleShutdownSignal(int /*signal*/) { shutdown_requested = 1; }

ABSL_FLAG(std::string, host, "localhost",
          "the address to bind to on the local machine");
ABSL_FLAG(std::uint16_t, port, 8888,
          "the port to bind to on the local machine");
ABSL_FLAG(std::string, data_dir, "",
          "the directory to persist the tables in; empty keeps them in memory");

int main(int argc, char* argv[]) {
  absl::SetProgramUsageMessage(absl::StrCat(
      "Usage: ", argv[0], " --host=<host> --port=<port> [--data_dir=<dir>]"));
  absl::ParseCommandLine(argc, argv);
  google::cloud::bigtable::emulator::BackendOptions options;
  options.data_dir = absl::GetFlag(FLAGS_data_dir);
  auto maybe_server =
      google::cloud::bigtable::emulator::CreateDefaultEmulatorServer(
          absl::GetFlag(FLAGS_host), absl::GetFlag(FLAGS_port), options);
  if (!maybe_server) {
    std::cerr << "CreateDefaultEmulatorServer() failed. See logs for "
                 "possible reason"
              << std::endl;
    return 1;
  }

  auto& server = maybe_server.value();

  struct sigaction action {};
  action.sa_handler = HandleShutdownSignal;
  action.sa_flags = SA_RESETHAND;
  sigemptyset(&action.sa_mask);
  sigaction(SIGINT, &action, nullptr);
  sigaction(SIGTERM, &action, nullptr);

  std::cout << "Server running on port " << server->bound_port() << "\n";

  // Shutdown() is not async-signal-safe, so it runs here, not in the handler.
  while (shutdown_requested == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  std::cout << "Shutting down\n";
  auto status = server->Shutdown();
  server->Wait();
  if (!status.ok()) {
    std::cerr << "Shutdown failed: " << status << "\n";
    return 1;
  }
  return 0;
}
