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

#include "backend.h"
#include "google/cloud/status_or.h"
#include "in_memory/in_memory_backend.h"
#include "persistent/persistent_backend.h"
#include <memory>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

StatusOr<std::unique_ptr<Backend>> Backend::Create(
    BackendOptions const& options) {
  if (options.data_dir.empty()) {
    return std::unique_ptr<Backend>(std::make_unique<InMemoryBackend>());
  }
  auto maybe_backend = PersistentBackend::Open(options);
  if (!maybe_backend) {
    return maybe_backend.status();
  }
  return std::unique_ptr<Backend>(*std::move(maybe_backend));
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
