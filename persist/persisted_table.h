/**
 * @file persisted_table.h
 * @brief Table interface backed by RocksDB-persisted storage for the Bigtable
 * emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSISTED_TABLE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSISTED_TABLE_H

#include "google/cloud/internal/big_endian.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "column_family.h"
#include "filter.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "google/protobuf/util/field_mask_util.h"
#include "limits.h"
#include "persist/storage.h"
#include "range_set.h"
#include "row_streamer.h"
#include <google/bigtable/admin/v2/bigtable_table_admin.pb.h>
#include <google/bigtable/admin/v2/table.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/protobuf/field_mask.pb.h>
#include <grpcpp/support/sync_stream.h>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
 * Converts a Bigtable RowSet proto into a StringRangeSet for row iteration.
 *
 * @param row_set The RowSet containing row keys and row ranges from the
 * request.
 * @return The constructed StringRangeSet or an error if validation fails.
 */
StatusOr<StringRangeSet> CreateStringRangeSet(
    google::bigtable::v2::RowSet const& row_set);

/**
 * Table interface backed by RocksDB-persisted storage.
 *
 * Provides Bigtable data operations (mutate, read, sample) and admin operations
 * (update schema, modify column families, drop row range) against persisted
 * storage.
 */
class PersistedTable {
 private:
  /** Underlying RocksDB storage instance. */
  std::shared_ptr<Storage> storage_;

  /** Table name in the form /projects/{}/instances/{}/tables/{}. */
  std::string name_;

  /**
   * Looks up the value type for a column family.
   *
   * @param family_name The column family name.
   * @return The Type proto if the family exists, nullopt otherwise.
   */
  absl::optional<google::bigtable::admin::v2::Type> GetColumnFamilyType(
      std::string const& family_name);

 public:
  /**
   * Constructs a PersistedTable for the given table name and storage.
   *
   * @param name Table name in the form /projects/{}/instances/{}/tables/{}.
   * @param storage Shared pointer to the RocksDB storage backend.
   */
  PersistedTable(std::string const& name, std::shared_ptr<Storage> storage);

  /**
   * Applies mutations to a single row atomically.
   *
   * @param request The MutateRow request containing row key and mutations.
   * @return Ok on success, or error describing the failure.
   */
  Status MutateRow(google::bigtable::v2::MutateRowRequest const& request);

  /**
   * Streams rows matching the request criteria to the row_streamer.
   *
   * @param request The ReadRows request specifying row set, filter, and limits.
   * @param row_streamer Sink for streaming row chunks to the client.
   * @return Ok on success, or error if stream closed or validation fails.
   */
  Status ReadRows(google::bigtable::v2::ReadRowsRequest const& request,
                  RowStreamer& row_streamer) const;

  /**
   * Samples row keys for load balancing.
   *
   * @param pass_probability Probability of including each row in the sample.
   * @param writer gRPC writer to send SampleRowKeysResponse messages.
   * @return Ok on success, or error if validation fails.
   */
  Status SampleRowKeys(
      double pass_probability,
      grpc::ServerWriter<google::bigtable::v2::SampleRowKeysResponse>* writer);

  /**
   * Applies a list of mutations to a row with rollback on failure.
   *
   * @param row_key The row to mutate.
   * @param mutations The list of mutations to apply.
   * @return Ok on success; on failure the transaction is rolled back.
   */
  Status DoMutationsWithPossibleRollback(
      std::string const& row_key,
      google::protobuf::RepeatedPtrField<google::bigtable::v2::Mutation> const&
          mutations);

  /**
   * Performs a read-modify-write on a row (append/increment rules).
   *
   * @param request The ReadModifyWriteRow request with rules.
   * @return The response row with modified cells, or error.
   */
  StatusOr<google::bigtable::v2::ReadModifyWriteRowResponse> ReadModifyWriteRow(
      google::bigtable::v2::ReadModifyWriteRowRequest const& request);

  /**
   * Conditionally applies true or false mutations based on predicate.
   *
   * @param request The CheckAndMutateRow request with predicate and mutations.
   * @return The response indicating which mutations were applied.
   */
  StatusOr<google::bigtable::v2::CheckAndMutateRowResponse> CheckAndMutateRow(
      google::bigtable::v2::CheckAndMutateRowRequest const& request);

  /**
   * Creates a cell stream over the given row range with optional filter.
   *
   * @param range_set The row range to iterate.
   * @param maybe_row_filter Optional row filter to apply.
   * @return A CellStream over the filtered rows, or error.
   */
  StatusOr<CellStream> CreateCellStream(
      std::shared_ptr<StringRangeSet> range_set,
      absl::optional<google::bigtable::v2::RowFilter> maybe_row_filter) const;

  /**
   * Updates table schema fields according to the field mask.
   *
   * @param new_schema The new schema values.
   * @param to_update Field mask specifying which fields to update.
   * @return Ok on success.
   */
  Status Update(google::bigtable::admin::v2::Table const& new_schema,
                google::protobuf::FieldMask const& to_update);

  /**
   * Applies column family modifications (create, update, drop).
   *
   * @param request The ModifyColumnFamilies request.
   * @return The updated table schema on success.
   */
  StatusOr<google::bigtable::admin::v2::Table> ModifyColumnFamilies(
      google::bigtable::admin::v2::ModifyColumnFamiliesRequest const& request);

  /**
   * Drops a row range by prefix or all table data.
   *
   * @param request The DropRowRange request.
   * @return Ok on success. Currently unimplemented for RocksDB storage.
   */
  Status DropRowRange(
      google::bigtable::admin::v2::DropRowRangeRequest const& request);
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_PERSISTED_TABLE_H
