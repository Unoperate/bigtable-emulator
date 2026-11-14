# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

bigtable_emulator_common_hdrs = [
    "backend.h",
    "bigtable_limits.h",
    "cell_view.h",
    "column_family.h",
    "filter.h",
    "filtered_map.h",
    "filtered_table_stream.h",
    "in_memory/in_memory_backend.h",
    "in_memory/in_memory_column_family.h",
    "persistent/key_coder.h",
    "persistent/persistent_backend.h",
    "persistent/persistent_column_family.h",
    "persistent/redolog.h",
    "range_set.h",
    "registry.h",
    "row_streamer.h",
    "server.h",
    "table.h",
    "table_operations.h",
    "test_util.h",
    "to_grpc_status.h",
]

bigtable_emulator_common_srcs = [
    "backend.cc",
    "column_family.cc",
    "filter.cc",
    "filtered_table_stream.cc",
    "in_memory/in_memory_backend.cc",
    "in_memory/in_memory_column_family.cc",
    "persistent/key_coder.cc",
    "persistent/persistent_backend.cc",
    "persistent/persistent_column_family.cc",
    "persistent/redolog.cc",
    "range_set.cc",
    "registry.cc",
    "row_streamer.cc",
    "server.cc",
    "table.cc",
    "table_operations.cc",
    "test_util.cc",
    "to_grpc_status.cc",
]
