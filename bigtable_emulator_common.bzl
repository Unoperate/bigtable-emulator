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
    "cell_view.h",
    "cluster.h",
    "column_family.h",
    "filter.h",
    "filtered_map.h",
    "bigtable_limits.h",
    "range_set.h",
    "row_streamer.h",
    "server.h",
    "table.h",
    "test_util.h",
    "to_grpc_status.h",
    "persist/storage.h",
    "persist/storage_row_tx.h",
    "persist/rocksdb/storage.h",
    "persist/rocksdb/storage_row_tx.h",
    "persist/rocksdb/column_family_stream.h",
    "persist/rocksdb/filtered_table_stream.h",
    "persist/rocksdb/key_encoding.h",
    "persist/persisted_table.h",
    "persist/utils/test_utils.h",
    "persist/metadata_view.h",
    "persist/memory/storage.h",
    "persist/memory/storage_row_tx.h",
    "persist/utils/logging.h",
]

bigtable_emulator_common_srcs = [
    "cluster.cc",
    "column_family.cc",
    "filter.cc",
    "range_set.cc",
    "row_streamer.cc",
    "server.cc",
    "table.cc",
    "test_util.cc",
    "to_grpc_status.cc",
    "persist/rocksdb/storage_row_tx.cc",
    "persist/rocksdb/column_family_stream.cc",
    "persist/persisted_table.cc",
    "persist/memory/storage_row_tx.cc",
]
