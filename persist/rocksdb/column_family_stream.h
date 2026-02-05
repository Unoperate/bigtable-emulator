/**
 * @file column_family_stream.h
 * @brief RocksDB-backed column family stream for iterating cells.
 */

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_COLUMN_FAMILY_STREAM_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_COLUMN_FAMILY_STREAM_H

#include "absl/types/optional.h"
#include "cell_view.h"
#include "filter.h"
#include "filtered_map.h"
#include "re2/re2.h"
#include "rocksdb/iterator.h"
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable {
namespace emulator {

class RocksDBStorage;

/**
 * RocksDB-backed column family stream; iterates over cells in a column family
 * for a row set.
 */
class RocksDBColumnFamilyStream : public AbstractFamilyColumnStreamImpl {
  friend class RocksDBStorage;

 public:
  /**
   * Constructs a column family stream.
   * @param column_family_name Name of the column family; used in returned
   * CellViews.
   * @param handle RocksDB column family handle (must remain valid for this
   * object's lifetime).
   * @param row_set Row keys to include in the stream.
   * @param db Pointer to the RocksDB storage.
   * @param table_name Table name.
   * @param prefetch_all_columns If true, prefetch all columns for each row.
   */
  RocksDBColumnFamilyStream(std::string const& column_family_name,
                            rocksdb::ColumnFamilyHandle* handle,
                            std::shared_ptr<StringRangeSet const> row_set,
                            RocksDBStorage* db, std::string const table_name,
                            bool const prefetch_all_columns);

  virtual ~RocksDBColumnFamilyStream();

  /** No-op; this stream does not push filters down to RocksDB. */
  bool ApplyFilter(InternalFilter const& internal_filter) override {
    return true;
  }

  /**
   * Returns whether the stream currently has a valid cell to read.
   * Lazily initializes the iterator and advances to the first matching row
   * if not yet initialized.
   * @return true if Value() can be called.
   */
  bool HasValue() const override;

  /**
   * Returns the current cell view (row key, column family, qualifier,
   * timestamp, value). Requires HasValue() to be true.
   * @return Reference to the current CellView.
   */
  CellView const& Value() const override;

  /**
   * Advances the stream to the next cell, column, or row depending on mode.
   * @param mode Whether to advance by cell, column, or row.
   * @return true if there is a next value (HasValue() will be true).
   */
  bool Next(NextMode mode) override;

  /** Returns the column family name for this stream. */
  std::string const& column_family_name() const override {
    return column_family_name_;
  }

 private:
  using TColumnRow = std::map<std::chrono::milliseconds, std::string, std::greater<>>;
  using TColumnFamilyRow = std::map<std::string, TColumnRow>;

  std::string column_family_name_;   /**< Column family name for this stream. */
  std::string table_name_;           /**< Table name. */
  bool const prefetch_all_columns_;  /**< If true, load all columns per row. */
  rocksdb::ColumnFamilyHandle* handle_; /**< RocksDB column family handle. */
  std::shared_ptr<StringRangeSet const> row_ranges_; /**< Row keys to include. */
  std::vector<std::shared_ptr<re2::RE2 const>> row_regexes_; /**< Row key regexes. */
  mutable StringRangeSet column_ranges_;   /**< Column qualifier ranges. */
  std::vector<std::shared_ptr<re2::RE2 const>> column_regexes_; /**< Column regexes. */
  mutable TimestampRangeSet timestamp_ranges_; /**< Timestamp filter. */
  RocksDBStorage* db_;               /**< Backing RocksDB storage. */
  mutable rocksdb::Iterator* row_iter_; /**< Iterator over raw row keys. */
  mutable std::string row_key_;     /**< Current row key (when row_data_ set). */
  mutable absl::optional<TColumnFamilyRow> row_data_; /**< Current row's columns/cells. */

  /**
   * Advances to the next row in the row set; updates row_key_ and row_data_
   * or clears row_data_ if no more matching rows.
   */
  void NextRow() const;

  /**
   * Lazily creates the RocksDB iterator and seeks to the first matching row.
   * Idempotent after the first call.
   */
  void InitializeIfNeeded() const;

  /**
   * Positions column_it_ and cell_it_ on the first cell that passes
   * column/timestamp filters within the current row. Called after advancing
   * column_it_.
   * @return true if a valid cell was found; false if end of row.
   */
  bool PointToFirstCellAfterColumnChange() const;

  /**
   * Positions on the first valid cell in the current row (or the next row if
   * the current row has no matching cells). Calls NextRow() until a row with
   * at least one matching cell is found.
   * @return true if a valid cell was found; false if no more rows.
   */
  bool PointToFirstCellAfterRowChange() const;

  /** Filtered view of current row's columns (by range/regex). */
  mutable absl::optional<
      RegexFiteredMapView<StringRangeFilteredMapView<TColumnFamilyRow>>>
      columns_;
  /** Filtered view of current column's cells (by timestamp). */
  mutable absl::optional<TimestampRangeFilteredMapView<TColumnRow>> cells_;
  /** Iterator over columns_ for the current row. */
  mutable absl::optional<RegexFiteredMapView<
      StringRangeFilteredMapView<TColumnFamilyRow>>::const_iterator>
      column_it_;
  /** Iterator over cells_ for the current column. */
  mutable absl::optional<
      TimestampRangeFilteredMapView<TColumnRow>::const_iterator>
      cell_it_;
  /** Cached CellView for the current (row, column, timestamp) position. */
  mutable absl::optional<CellView> cur_value_;
  /** True after the iterator has been created and first row sought. */
  mutable bool initialized_{false};
};

}  // namespace emulator
}  // namespace bigtable
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_EMULATOR_ROCKSDB_COLUMN_FAMILY_STREAM_H
