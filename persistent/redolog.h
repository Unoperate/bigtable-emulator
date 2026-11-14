// utils.h
#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_REDOLOG_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_REDOLOG_H

#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include <google/bigtable/admin/v2/table.pb.h>
#include <memory>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class SchemaRedoLog {
 public:
  explicit SchemaRedoLog(std::shared_ptr<rocksdb::TransactionDB> db);

  Status Begin(::google::bigtable::admin::v2::Table const& target_schema);

  StatusOr<::google::bigtable::admin::v2::Table> Load() const;

  Status Finish();

 private:
  std::shared_ptr<rocksdb::TransactionDB> db_;
  rocksdb::ColumnFamilyHandle* default_cf_;
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_REDOLOG_H
