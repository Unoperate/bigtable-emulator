#pragma once
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "rocksdb/db.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class Storage {
 public:
  Storage(std::string const& db_path);
  ~Storage();

  bool PutCell(std::string const& table_name, std::string const& row_key,
               std::string const& column_family,
               std::string const& column_qualifier,
               std::chrono::milliseconds const& timestamp,
               std::string const& value);

  bool PutRow(std::string const& row_key, std::string const& value);
  std::string GetRow(std::string const& row_key);
  bool DeleteRow(std::string const& row_key);

  bool PutBatch(
      std::vector<std::pair<std::string, std::string>> const& kvs);
  void ScanDatabase(void);
  void GetRowData(std::string const& table_name, std::string const& row_key);
  void DeleteTable(std::string table_name);
  void DeleteColumnFamiliesForTable(std::string const& table_prefix);
  void DeleteColumnFamily(std::string const& prefixed_cf_name);
  void DeleteColumn(std::string const& table_name, std::string const& row_key,
                    std::string const& prefixed_cf_name,
                    std::string const& column_name);
  bool DeleteCell(std::string const& table_name, std::string const& row_key,
                  std::string const& prefixed_cf_name,
                  std::string const& column_name,
                  std::chrono::milliseconds const& timestamp);
  void DeleteRow(std::string const& table_name, std::string const& row_key);
  bool DeleteCFRow(std::string const& table_name, std::string const& row_key,
                   std::string const& prefixed_cf_name);
  bool CFExists(std::string const& prefixed_cf_name);
  bool RowExistsInCF(std::string const& table_name, std::string const& row_key,
                     std::string const& prefixed_cf_name);
  bool RowExists(std::string const& table_name, std::string const& row_key);
  rocksdb::Iterator* NewIterator(std::string const& cf_name);
  bool IsRangeEmpty(rocksdb::ColumnFamilyHandle* handle,
                    rocksdb::Slice const& start_key,
                    rocksdb::Slice const& end_key);

 private:
  std::unique_ptr<rocksdb::DB> db_;
  std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> cf_handles_;
  std::mutex cf_mutex_;
  rocksdb::ColumnFamilyHandle* GetOrAddHandle(std::string const& cf_name);
};

int InitGlobalStorage(char const* path);
Storage* GetGlobalStorage(void);
void CloseGlobalStorage(void);
int GetNextSchemaIdx();
void RollbackSchemaIdx();
std::string Trim(std::string const& s);
std::string CalculatePrefixEnd(std::string const& prefix);

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
