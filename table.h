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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/synchronization/mutex.h"
#include "row_streamer.h"
#include "table_operations.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/sync_stream.h>
#include <memory>
#include <string>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class InMemoryRowTransaction;
class PersistentRowTransaction;

/// Validates a new table's schema and fills in defaults.
Status PrepareTableSchema(google::bigtable::admin::v2::Table& schema);

/// Objects of this class represent Bigtable tables.
class Table {
 public:
  Table(std::string name, google::bigtable::admin::v2::Table schema,
        std::unique_ptr<TableOperations> operations);

  std::string const& name() const { return name_; }

  /// Takes no table lock.
  std::shared_ptr<google::bigtable::admin::v2::Table const> schema() const;

  Status Update(google::bigtable::admin::v2::Table const& new_schema,
                google::protobuf::FieldMask const& to_update);

  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request);

  /// Checks deletion protection, commits the deletion and detaches the
  /// storage, which the caller hands to Backend::DestroyTable().
  StatusOr<std::unique_ptr<TableOperations>> Delete();

  // Data API.
  StatusOr<google::bigtable::v2::CheckAndMutateRowResponse> CheckAndMutateRow(
      google::bigtable::v2::CheckAndMutateRowRequest const& request,
      Deadline deadline);

  Status MutateRow(google::bigtable::v2::MutateRowRequest const& request,
                   Deadline deadline);

  Status MutateRows(
      google::bigtable::v2::MutateRowsRequest const& request,
      grpc::ServerWriter<google::bigtable::v2::MutateRowsResponse>* writer,
      Deadline deadline);

  /// `context` is ignored for now.
  Status ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                  RowStreamer& row_streamer,
                  grpc::ServerContext const& context) const;

  StatusOr<::google::bigtable::v2::ReadModifyWriteRowResponse>
  ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request,
      Deadline deadline);

  Status SampleRowKeys(
      grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer)
      const;

  Status DropRowRange(
      ::google::bigtable::admin::v2::DropRowRangeRequest const& request);

  /// Closes the storage at shutdown. Afterwards the table behaves as deleted.
  Status Close();

  // TODO(prawilny): make it follow appropriate convention so that nobody uses
  // it by accident. For testing only
  TableOperations* GetUtilities() { return operations_.get(); }

  ~Table() = default;

 private:
  friend class RowTransaction;
  friend class InMemoryRowTransaction;
  friend class PersistentRowTransaction;

  /// NOT_FOUND after Delete() or Close(); UNAVAILABLE once the table is
  /// unavailable. Call with `mu_` held.
  StatusOr<DataOperations*> data_operations() const;
  StatusOr<AdminOperations*> admin_operations();

  void PublishSchema(google::bigtable::admin::v2::Table new_schema);
  /// Until restart.
  void MarkUnavailable(Status reason);

  Status DoMutationsWithPossibleRollback(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations,
      Deadline deadline);

  std::string const name_;
  bool const concurrent_data_operations_;

  /// Data operations take it shared only if the backend allows concurrent
  /// data operations; admin operations take it exclusive.
  mutable absl::Mutex mu_;
  /// Null after Delete() or Close().
  std::unique_ptr<TableOperations> operations_;
  /// Not OK once the table is unavailable.
  Status unavailable_;

  std::shared_ptr<google::bigtable::admin::v2::Table const> schema_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_TABLE_H
