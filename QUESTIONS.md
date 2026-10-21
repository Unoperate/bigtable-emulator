https://medium.com/pinterest-engineering/building-pinterests-new-wide-column-database-using-rocksdb-f5277ee4e3d2
RocksDB has something called "Wide columns"; technically it looks like rows,
but I didn't see any iterator API for it (maybe I just didn't look hard enough).
Nonetheless, I think it will be easier to just simulate rows by keys:
(row, key) -> (value, timestamp) will be stored in RocksDB
as "row:key" -> (value, timestamp). Keys are sorted, so iterating
should work nicely.