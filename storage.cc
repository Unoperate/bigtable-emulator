#include "storage.h"
#include "constants.h"
#include "rocksdb/iterator.h"
#include <atomic>
#include <iostream>
#include <sstream>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

Storage::Storage(const std::string& db_path) {
    rocksdb::Options options;
    options.create_if_missing = true; 

    // 1. List existing column families
    std::vector<std::string> family_names;
    rocksdb::Status status = rocksdb::DB::ListColumnFamilies(options, db_path, &family_names);
    
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

    // If DB exists but ListColumnFamilies failed (unlikely) or returned empty, 
    // we must at least open the default.
    if (!status.ok() || family_names.empty()) {
        column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    } else {
        for (const auto& name : family_names) {
            column_families.push_back(rocksdb::ColumnFamilyDescriptor(name, rocksdb::ColumnFamilyOptions()));
        }
    }

    // 2. Open DB with all column families
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* db_ptr = nullptr;
    status = rocksdb::DB::Open(options, db_path, column_families, &handles, &db_ptr);

    if (!status.ok()) {
        std::cerr << "Failed to open RocksDB: " << status.ToString() << std::endl;
    } else {
        // 3. Map handles to names
        for (size_t i = 0; i < column_families.size(); i++) {
            cf_handles_[column_families[i].name] = handles[i];
        }
    }
    db_.reset(db_ptr);
}

Storage::~Storage() {
    // ColumnFamilyHandles must be deleted before the DB is deleted.
    for (auto& pair : cf_handles_) {
        if (pair.second) {
            db_->DestroyColumnFamilyHandle(pair.second);
        }
    }
    cf_handles_.clear();
    // db_ unique_ptr will close the DB automatically here
}

rocksdb::ColumnFamilyHandle* Storage::GetOrAddHandle(const std::string& cf_name) {
    std::lock_guard<std::mutex> lock(cf_mutex_);
    
    // Check if it exists
    auto it = cf_handles_.find(cf_name);
    if (it != cf_handles_.end()) {
        return it->second;
    }

    // Create new column family
    rocksdb::ColumnFamilyHandle* handle;
    rocksdb::Status status = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), cf_name, &handle);
    
    if (!status.ok()) {
        std::cerr << "Failed to create Column Family '" << cf_name << "': " << status.ToString() << std::endl;
        return nullptr;
    }

    cf_handles_[cf_name] = handle;
    return handle;
}

bool Storage::PutCell(const std::string& table_name, const std::string& row_key, const std::string& column_family,
      const std::string& column_qualifier, const std::chrono::milliseconds& timestamp, 
      const std::string& value) {
    
    rocksdb::ColumnFamilyHandle* handle = GetOrAddHandle(column_family);
    if (!handle) return false;

    // Note: column_family name is removed from the key string because 
    // it is now represented by the physical RocksDB Column Family.
    std::string full_key = "/tables/" + table_name + "/" + row_key + "/" + column_qualifier + "/" + std::to_string(timestamp.count());

    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), handle, full_key, value);
    return status.ok();
}

bool Storage::PutRow(const std::string& row_key, const std::string& value) {
    // Uses Default CF
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), row_key, value);
    return status.ok();
}

std::string Storage::GetRow(const std::string& row_key) {
    // Uses Default CF
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), row_key, &value);
    if (!status.ok()) {
        return ""; 
    }
    return value;
}

bool Storage::DeleteRow(const std::string& row_key) {
    // Uses Default CF
    rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), row_key);
    return status.ok();
}

bool Storage::PutBatch(const std::vector<std::pair<std::string,std::string>>& kvs) {
    // Writes to Default CF
    rocksdb::WriteBatch batch;
    for (auto const& kv : kvs) {
        batch.Put(kv.first, kv.second);
    }
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        std::cerr << "PutBatch failed: " << status.ToString() << std::endl;
        return false;
    }
    return true;
}

void Storage::ScanDatabase(void) {
    std::lock_guard<std::mutex> lock(cf_mutex_);
    rocksdb::ReadOptions read_options;

    std::cout << "--- Scanning Database ---\n";
    for (const auto& pair : cf_handles_) {
        std::string cf_name = pair.first;
        rocksdb::ColumnFamilyHandle* handle = pair.second;

        std::cout << "Column Family: [" << cf_name << "]\n";
        
        rocksdb::Iterator* it = db_->NewIterator(read_options, handle);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::cout << "  " << it->key().ToString() << " => " << it->value().ToString() << "\n";
        }
        if (!it->status().ok()) {
            std::cerr << "  Error: " << it->status().ToString() << "\n";
        }
        delete it;
    }
    std::cout << "-------------------------\n";
}

void Storage::GetRowData(const std::string& table_name, const std::string& row_key) {
    // Since data is split across column families, we must check all of them 
    // for this specific row key prefix.
    
    std::string prefix = "/tables/" + table_name + "/" + row_key + "/";
    rocksdb::ReadOptions read_options;
    
    std::lock_guard<std::mutex> lock(cf_mutex_);

    for (const auto& pair : cf_handles_) {
        rocksdb::ColumnFamilyHandle* handle = pair.second;
        rocksdb::Iterator* it = db_->NewIterator(read_options, handle);

        for (it->Seek(prefix); it->Valid(); it->Next()) {
            if (!it->key().starts_with(prefix)) {
                break;
            }
            // Output format: CF:Key => Value
            std::cout << "[" << pair.first << "] " << it->key().ToString() << " | Value: " << it->value().ToString() << std::endl;
        }
        delete it;
    }
}

void Storage::DeleteTable(std::string table_key) {
    std::string table_key_to_remove = kTablesPrefix + table_key;
    std::string manifest = GetRow(kManifestKey);

    std::string new_manifest;
    bool changed = false;

    if (!manifest.empty()) {
        std::istringstream iss(manifest);
        std::string line;

        // Iterate through lines, keeping only those that do not match the key
        while (std::getline(iss, line)) {
            if (Trim(line) != table_key_to_remove) {
            new_manifest += line;
            new_manifest.push_back('\n');
            } else {
            changed = true;
            }
        }
    }

    if (changed) {
        PutRow(kManifestKey, new_manifest);

        DeleteColumnFamiliesForTable(table_key + "/");

        DeleteRow(table_key_to_remove);
    }
}

void Storage::DeleteColumnFamiliesForTable(const std::string& table_prefix) {
    std::lock_guard<std::mutex> lock(cf_mutex_);
    std::vector<std::string> cfs_to_remove;

    for (const auto& pair : cf_handles_) {
        const std::string& name = pair.first;

        if (name == rocksdb::kDefaultColumnFamilyName) {
            continue;
        }

        if (name.size() >= table_prefix.size() && 
            name.compare(0, table_prefix.size(), table_prefix) == 0) {
            cfs_to_remove.push_back(name);
        }
    }

    for (const auto& name : cfs_to_remove) {
        DeleteColumnFamily(name);
    }
}

void Storage::DeleteColumnFamily(const std::string &prefixed_cf_name) {
    auto it = cf_handles_.find(prefixed_cf_name);
    if (it == cf_handles_.end()) {
        return;
    }
    rocksdb::ColumnFamilyHandle* handle = it->second;

    rocksdb::Status status = db_->DropColumnFamily(handle);
    if (!status.ok()) {
        std::cerr << "Failed to drop Column Family '" << prefixed_cf_name << "': " << status.ToString() << "\n";
        return;
    }

    status = db_->DestroyColumnFamilyHandle(handle);
    if (!status.ok()) {
        std::cerr << "Failed to destroy handle for '" << prefixed_cf_name << "': " << status.ToString() << "\n";
    }

    cf_handles_.erase(it);
}

void Storage::DeleteColumn(const std::string& table_name, const std::string& row_key, 
                            const std::string &prefixed_cf_name, const std::string &column_name) {
    std::string start_key = "/tables/" + table_name + "/" + row_key + "/" + column_name + "/";
    std::string end_key = CalculatePrefixEnd(start_key);

    rocksdb::ColumnFamilyHandle* handle = cf_handles_[prefixed_cf_name];
    rocksdb::WriteBatch batch;

    batch.DeleteRange(handle, start_key, end_key);
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        std::cerr << "DeleteColumn failed for '" << prefixed_cf_name
                  << "': " << status.ToString() << "\n";
    }
}

bool Storage::DeleteCell(
    std::string const& table_name, std::string const& row_key,
    std::string const& prefixed_cf_name, std::string const& column_name,
    std::chrono::milliseconds const& timestamp) {
    rocksdb::ColumnFamilyHandle* handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(cf_mutex_);
        auto it = cf_handles_.find(prefixed_cf_name);
        if (it == cf_handles_.end()) return false;
        handle = it->second;
    }

    std::string const full_key = "/tables/" + table_name + "/" + row_key + "/" +
                                 column_name + "/" +
                                 std::to_string(timestamp.count());
    auto const status = db_->Delete(rocksdb::WriteOptions(), handle, full_key);
    if (!status.ok()) {
        std::cerr << "DeleteCell failed for '" << prefixed_cf_name
                  << "': " << status.ToString() << "\n";
        return false;
    }
    return true;
}

void Storage::DeleteRow(const std::string& table_name, const std::string& row_key) {
    std::string start_key = "/tables/" + table_name + "/" + row_key + "/";
    std::string end_key = CalculatePrefixEnd(start_key);

    rocksdb::WriteBatch batch;
    for (const auto& pair : cf_handles_) {
        rocksdb::ColumnFamilyHandle* handle = pair.second;
        batch.DeleteRange(handle, start_key, end_key);
    }
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        std::cerr << "DeleteRow failed for row '" << row_key
                  << "': " << status.ToString() << "\n";
    }
}

bool Storage::DeleteCFRow(const std::string& table_name, const std::string& row_key,
        const std::string &prefixed_cf_name) {
    rocksdb::ColumnFamilyHandle* handle;
    auto it = cf_handles_.find(prefixed_cf_name);
    if (it != cf_handles_.end()) {
        handle = it->second;
    } else {
        return false;
    }

    std::string start_key = "/tables/" + table_name + "/" + row_key + "/";
    std::string end_key = CalculatePrefixEnd(start_key);

    rocksdb::WriteBatch batch;
    batch.DeleteRange(handle, start_key, end_key);
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        std::cerr << "DeleteCFRow failed for '" << prefixed_cf_name
                  << "': " << status.ToString() << "\n";
        return false;
    }

    return true;
}

bool Storage::CFExists(const std::string &prefixed_cf_name) {
    return cf_handles_.find(prefixed_cf_name) != cf_handles_.end();
}

bool Storage::RowExistsInCF(const std::string& table_name, const std::string& row_key,
    const std::string &prefixed_cf_name) {
    rocksdb::ColumnFamilyHandle* handle = cf_handles_[prefixed_cf_name];

    std::string start_key = "/tables/" + table_name + "/" + row_key + "/";
    std::string end_key = CalculatePrefixEnd(start_key);

    return !IsRangeEmpty(handle, start_key, end_key);
}

bool Storage::RowExists(const std::string& table_name, const std::string& row_key) {
    for (const auto& pair : cf_handles_) {
        rocksdb::ColumnFamilyHandle* handle = pair.second;
        std::string start_key = "/tables/" + table_name + "/" + row_key + "/";
        std::string end_key = CalculatePrefixEnd(start_key);

        if (!IsRangeEmpty(handle, start_key, end_key)) {
            return true;
        }
    }

    return false;
}

rocksdb::Iterator* Storage::NewIterator(const std::string& cf_name) {
    rocksdb::ColumnFamilyHandle* handle = GetOrAddHandle(cf_name);
    if (!handle) return nullptr;
    return db_->NewIterator(rocksdb::ReadOptions(), handle);
}

bool Storage::IsRangeEmpty(rocksdb::ColumnFamilyHandle* handle, 
    const rocksdb::Slice& start_key, 
    const rocksdb::Slice& end_key) {
    rocksdb::ReadOptions read_options;
    // Optimization: Set the upper bound to avoid internal work beyond end_key
    read_options.iterate_upper_bound = &end_key; 

    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options, handle));
    it->Seek(start_key);

    if (it->Valid()) {
      if (it->key().compare(end_key) < 0) {
        return false;
      }
    }

    return true;
}

static Storage* g_storage = nullptr;
static pthread_once_t g_storage_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t g_storage_mu = PTHREAD_MUTEX_INITIALIZER;
static char* g_storage_db_name = nullptr;
std::atomic<int> idx{0};

static void init_storage_once(void) {
  if (g_storage_db_name == nullptr) {
    return;
  }
  g_storage = new Storage(g_storage_db_name);
}

int InitGlobalStorage(char const* db_name) {
  if (db_name == nullptr) return -1;
  pthread_mutex_lock(&g_storage_mu);
  if (g_storage != nullptr) {
    // already initialized
    pthread_mutex_unlock(&g_storage_mu);
    return 0;
  }

  g_storage_db_name = strdup(db_name);
  if (g_storage_db_name == nullptr) {
    pthread_mutex_unlock(&g_storage_mu);
    return -1;
  }

  int rc = pthread_once(&g_storage_once, init_storage_once);
  pthread_mutex_unlock(&g_storage_mu);
  return rc;
}

Storage* GetGlobalStorage(void) {
  return g_storage;
}

void CloseGlobalStorage(void) {
  pthread_mutex_lock(&g_storage_mu);
  if (g_storage) {
    delete g_storage;
    g_storage = nullptr;
  }
  if (g_storage_db_name) {
    free(g_storage_db_name);
    g_storage_db_name = nullptr;
  }
  pthread_mutex_unlock(&g_storage_mu);
}

int GetNextSchemaIdx() {
    return idx++;
}

void RollbackSchemaIdx() {
    idx--;
}

std::string Trim(const std::string& s) {
  size_t a = 0;
  while (a < s.size() && (s[a] == '\n' || s[a] == '\r')) ++a;
  size_t b = s.size();
  while (b > a && (s[b-1] == '\n' || s[b-1] == '\r')) --b;
  return s.substr(a, b - a);
}

std::string CalculatePrefixEnd(const std::string& prefix) {
    std::string end_key = prefix;
    // Strip trailing 0xFF bytes as they cannot be incremented
    while (!end_key.empty() && static_cast<unsigned char>(end_key.back()) == 0xFF) {
        end_key.pop_back();
    }
    
    if (end_key.empty()) {
        // Corner case: The prefix was all 0xFF. Theoretically, there is no end key.
        // In your specific schema ("/tables/..."), this will never happen.
        return "\xff"; 
    }
    
    // Increment the last byte to get the next prefix
    end_key.back()++;
    return end_key;
}

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google
