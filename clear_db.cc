#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>
#include "storage.h"

int main(int argc, char* argv[]) {
  if (const char* env_p = std::getenv("BUILD_WORKING_DIRECTORY")) {
    if (chdir(env_p) != 0) {
      std::cerr << "Failed to switch to workspace directory.\n";
    }
  }

  std::string db_path = "test_db";
  if (argc > 1) db_path = argv[1];
  if (db_path.empty()) {
    std::cerr << "DB path cannot be empty.\n";
    return 1;
  }

  rocksdb::Options options;
  rocksdb::Status status = rocksdb::DestroyDB(db_path, options);
  std::cout << "Status of destroying DB: " << status.ok() << "\n";
  return 0;
}
