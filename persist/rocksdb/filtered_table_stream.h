/**
 * @file filtered_table_stream.h
 * @brief RocksDB merge of multiple column family streams with filter pushdown.
 */
#include "filter.h"
#include "re2/re2.h"
#include <memory>
#include <string>
#include <vector>

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_FILTERED_TABLE_STREAM_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_FILTERED_TABLE_STREAM_H

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

/**
  * This is very similar to FilteredTableStream.
  * We didn't want to move all the files to persist/ directory, because that would make diff
  * for persistence implementation unreadable. Currently persist/memory offers thin wrapper around Table
  * class (used previously for in-memory storage). All other required logic was extracted from Table class.
  * This includes this utility class.
  *
  * In Table this class is used only to enable testing. We use it to group multiple streams into one in RocksDB storage implementation.
  * It differs from FilteredTableStream by the unique ptr type.
  *    (Here)  :  std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>>
  *    (There) :  std::vector<std::unique_ptr<FilteredColumnFamilyStream>>
  *
  * TODO: This could be turned into templated class in the future
  */
class StorageFitleredTableStream : public MergeCellStreams {
 public:
  /**
   * Constructs from a vector of column family stream implementations.
   * @param cf_streams Column family streams to merge (ownership transferred).
   */
  explicit StorageFitleredTableStream(
      std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> cf_streams)
      : MergeCellStreams(CreateCellStreams(std::move(cf_streams))) {}

  /**
   * Applies filter at the merge level: for FamilyNameRegex or ColumnRange,
   * prunes streams that don't match or delegates to the matching stream.
   * Other filters are passed to the base MergeCellStreams.
   * @param internal_filter Filter to apply.
   * @return true if the filter was applied (or is a no-op).
   */
  bool ApplyFilter(InternalFilter const& internal_filter) override {
    if (!absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
        !absl::holds_alternative<ColumnRange>(internal_filter)) {
      return MergeCellStreams::ApplyFilter(internal_filter);
    }
    // internal_filter is either FamilyNameRegex or ColumnRange
    for (auto stream_it = unfinished_streams_.begin();
         stream_it != unfinished_streams_.end();) {
      auto* cf_stream =
          dynamic_cast<AbstractFamilyColumnStreamImpl*>(&(*stream_it)->impl());
      assert(cf_stream);

      if ((absl::holds_alternative<FamilyNameRegex>(internal_filter) &&
           !re2::RE2::PartialMatch(
               cf_stream->column_family_name(),
               *absl::get<FamilyNameRegex>(internal_filter).regex)) ||
          (absl::holds_alternative<ColumnRange>(internal_filter) &&
           absl::get<ColumnRange>(internal_filter).column_family !=
               cf_stream->column_family_name())) {
        stream_it = unfinished_streams_.erase(stream_it);
        continue;
      }

      if (absl::holds_alternative<ColumnRange>(internal_filter) &&
          absl::get<ColumnRange>(internal_filter).column_family ==
              cf_stream->column_family_name()) {
        cf_stream->ApplyFilter(internal_filter);
      }

      stream_it++;
    }

    return true;
  }

 private:
  /**
   * Builds the CellStream vector expected by MergeCellStreams from column
   * family stream implementations.
   * @param cf_streams Column family streams (ownership transferred).
   * @return Vector of CellStream wrapping the given streams.
   */
  static std::vector<CellStream> CreateCellStreams(
      std::vector<std::unique_ptr<AbstractFamilyColumnStreamImpl>> cf_streams) {
    std::vector<CellStream> res;
    res.reserve(cf_streams.size());
    for (auto& stream : cf_streams) {
      res.emplace_back(std::move(stream));
    }
    return res;
  }
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_FILTERED_TABLE_STREAM_H