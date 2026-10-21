#include "redolog.h"
#include "google/cloud/internal/make_status.h"
#include "rocksdb/write_batch.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

constexpr char kPendingSchemaKey[] = "t_emulator:meta:pending_schema_pb";

SchemaRedoLog::SchemaRedoLog(std::shared_ptr<rocksdb::TransactionDB> db)
    : db_(std::move(db)), default_cf_(db_->DefaultColumnFamily()) {}

Status SchemaRedoLog::Begin(
    ::google::bigtable::admin::v2::Table const& target_schema) {
  std::string bytes;
  if (!target_schema.SerializeToString(&bytes)) {
    return InternalError("Failed to serialize target schema", GCP_ERROR_INFO());
  }

  rocksdb::WriteOptions wo;
  auto s = db_->Put(wo, default_cf_, kPendingSchemaKey, bytes);
  if (!s.ok()) {
    return InternalError(
        "Failed to write pending schema: " + s.ToString(),
        GCP_ERROR_INFO().WithMetadata("key", kPendingSchemaKey));
  }
  return Status();
}

StatusOr<::google::bigtable::admin::v2::Table> SchemaRedoLog::Load() const {
  std::string bytes;
  rocksdb::ReadOptions ro;
  auto s = db_->Get(ro, default_cf_, kPendingSchemaKey, &bytes);

  if (s.IsNotFound()) {
    return NotFoundError("No pending schema", GCP_ERROR_INFO().WithMetadata(
                                                  "key", kPendingSchemaKey));
  }
  if (!s.ok()) {
    return InternalError(
        "Failed to read pending schema: " + s.ToString(),
        GCP_ERROR_INFO().WithMetadata("key", kPendingSchemaKey));
  }

  ::google::bigtable::admin::v2::Table t;
  if (!t.ParseFromString(bytes)) {
    return InternalError(
        "Failed to parse pending schema proto",
        GCP_ERROR_INFO().WithMetadata("key", kPendingSchemaKey));
  }
  return t;
}

Status SchemaRedoLog::Finish() {
  rocksdb::WriteOptions wo;
  auto s = db_->Delete(wo, default_cf_, kPendingSchemaKey);
  if (!s.ok()) {
    return InternalError(
        "Failed to delete pending schema: " + s.ToString(),
        GCP_ERROR_INFO().WithMetadata("key", kPendingSchemaKey));
  }
  return Status();
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
