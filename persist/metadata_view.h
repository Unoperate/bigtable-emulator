/**
 * @file storage.h
 * @brief Abstract storage interface and helpers for the Bigtable emulator.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_METADATA_VIEW_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_METADATA_VIEW_H

#include "persist/utils/logging.h"
#include "persist/proto/storage.pb.h"

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
 * This class represents cached list of tuples of tables' names and their
 * metadata. Should be returned by Tables() method in abstract class Storage().
 * We chose to use such a simple wrapper to make it easier to deal with polymorphism on iterators
 * returned by abstract Storage class.
 *
 * Storage::Tables() guarantees:
 *  - Random const access iterator (specifically std continous range)
 *  - Provides snapshot of tables when call to Tables() happened, even if mutations happen in meantime
 */
class CachedTablesMetadataView {
 public:
  using meta_t = std::tuple<std::string, storage::TableMeta>;
  using container_t = std::vector<meta_t>;

 private:
  container_t data_;

 public:
  template <typename T>
  CachedTablesMetadataView(T abstract_metadata_view) {
    container_t tmp;
    std::transform(abstract_metadata_view.begin(), abstract_metadata_view.end(),
                   std::back_inserter(tmp), [](auto el) -> auto { return el; });
    data_ = tmp;
  };

  container_t::const_iterator begin() const { return data_.cbegin(); };
  container_t::const_iterator end() const { return data_.cend(); };

  /** Returns vector of table names in this view. */
  inline std::vector<std::string> names() const {
    std::vector<std::string> tmp;
    DBG("[TablesMetadataView][names] listing tables");
    std::transform(
        begin(), end(), std::back_inserter(tmp), [](auto el) -> auto {
          DBG("[TablesMetadataView][names] table => {}", std::get<0>(el));
          return std::get<0>(el);
        });
    DBG("[TablesMetadataView][names] end, count={}", tmp.size());
    return tmp;
  }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_METADATA_VIEW_H
