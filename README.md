# Single node, Persistent Emulator for Google Bigtable

## Table of Contents

- [🗄️ Single node, Persistent Emulator for Google Bigtable](#single-node-persistent-emulator-for-google-bigtable)
- [💾 Persistent storage patch](#persistent-storage-patch)
  - [⚙️ Implementation](#implementation)
  - [🧠 MemoryStorage: how data is stored](#memorystorage-how-data-is-stored)
  - [🪨 RocksDBStorage: how data is stored](#rocksdbstorage-how-data-is-stored)
  - [🔌 CustomStorage: implementing custom storage interface](#customstorage-implementing-custom-storage-interface)
  - [🧪 Testing](#testing)
  - [📋 What's left for the future (TODO)?](#whats-left-for-the-future-todo)
- [ℹ️ About](#about)
- [📦 Dependencies](#dependencies)
- [🏗️ Building](#building)
- [▶️ Running the Unit Tests](#running-the-unit-tests)
- [🔍 Running clang-tidy with Bazel](#running-clang-tidy-with-bazel)
- [🚀 Running the Emulator](#running-the-emulator)
- [🔗 Connecting to the Emulator](#connecting-to-the-emulator)
- [💾 Using RocksDB](#using-rocksdb)
- [📝 compile_commands.json](#compile_commandsjson)
- [🛠️ Development](#development)
  - [✨ Formatting the code](#formatting-the-code)
  - [📝 compile_commands.json](#compile_commandsjson-1)
- [🤝 Contributing changes](#contributing-changes)
- [⚖️ Licensing](#licensing)

## Persistent storage patch

**👋 Persistence was added to the emulator by the following contributors:**
* [⭐⭐ Piotr Styczyński](https://github.com/styczynski)
  * Majority of the code for storage interface and tests
* [⭐ Piotr Bublik](https://github.com/pibuxd)
  * Implemented all core data operations (MutateRow, ReadRows, CheckAndMutateRow, ReadModifyWriteRow)
  * Solved RocksDB TransactionDB column family creation constraints
  * Implemented server endpoints to use `PersistentTable` implementation
* [Mikołaj Woliński](https://github.com/mik04rm)
  * Minimal README updates and some TODO fixes

### Implementation

Main implementation is in `persist/storage.h`. The storage class is abstract as well as the row transaction model.
`PersistedTable` provides the table interface that can be supplied with any class implementing abstract `Storage` interface.

| Implementation   | Path                         | Description |
|------------------|------------------------------|-------------|
| **MemoryStorage** | `persist/memory/storage.h`   | Thin wrapper around the pre-existing (non-persistent) in-memory class hierarchy in `table.h`. No persistence; data lives in `std::map` structures with mutexes. Top-level mapping: `map<string, Table>`. |
| **RocksDBStorage** | `persist/rocksdb/storage.h` | Uses RocksDB `TransactionDB` for ACID guarantees. All mutation operations go through transactions. Table metadata is stored in a dedicated RocksDB column family (e.g. `bte_metadata`). |

Protobuf definitions live in `persist/proto/storage.proto` (generated headers: `persist/proto/storage.pb.h`):

| Message                  | Purpose |
|--------------------------|---------|
| `StorageRocksDBConfig`   | Server/storage configuration (e.g. `db_path`, `meta_column_family`). |
| `TableMeta`              | Wrapper for table schema (`google.bigtable.admin.v2.Table`); used for metadata. |
| `RowData`                | Serialization of row cell data; contains `RowColumnData` (map of timestamp → value) per column. |

### MemoryStorage: how data is stored 

Data is stored within the `Table` and `ColumnFamily` hierarchy of classes in local fields (`std::map` with `std::string` keys) protected by mutexes.

| Level | Class             | File            | Field              | Maps |
|-------|-------------------|-----------------|--------------------|------|
| 1     | `Table`           | `table.h`       | `column_families_` | column family name → `std::shared_ptr<ColumnFamily>` |
| 2     | `ColumnFamily`    | `column_family.h` | `rows_`          | row key → `ColumnFamilyRow` |
| 3     | `ColumnFamilyRow` | `column_family.h` | `columns_`       | column qualifier → `ColumnRow` |
| 4     | `ColumnRow`       | `column_family.h` | `cells_`         | timestamp (`std::chrono::milliseconds`) → value (`std::string`); ordered by timestamp descending (`std::greater<>`). |

![Storage diagram for RocksDB](https://github.com/pibuxd/bigtable-emulator/blob/implement-rocksdb-persistence/static/storage_diagram_mem.png?raw=true)

### RocksDBStorage: how data is stored 

We have special column family called `"bte_metadata"` (configurable as many other parameters via `StorageRocksDBConfig`).
This column family has keys (table names) and values (values of type `TableMeta` that is just protobuf wrapping `TableSchema` protobuf).
That way we know what tables we have and what schemas they have.

BigTable maps the data in the following way:
`val data: Map<(string, string, string, std::chrono::milliseconds), string>`
The keys are:
`data[column_family, row_key, column_qualifier, timestamp] = value`

Now in RocksDB we have column families, but we need to somehow map all the other keys.

Data is grouped as follows:

| RocksDB level        | Key (RocksDB key)                    | Value (RocksDB value) |
|----------------------|--------------------------------------|------------------------|
| Metadata CF (e.g. `bte_metadata`) | Table name (string)                  | Serialized `TableMeta` (table schema). |
| Data CF (one per Bigtable column family) | `table_name \| row_key \| column_qualifier` | Serialized `RowData` (contains `RowColumnData`: map of timestamp_ms → value for that (row, column)). |

*Summary:* 
Bigtable column family → RocksDB column family
(table, row, column_qualifier) → one RocksDB key
all timestamps for that (row, column) are stored in the single `RowData` protobuf value.

![Storage diagram for RocksDB](https://github.com/pibuxd/bigtable-emulator/blob/implement-rocksdb-persistence/static/storage_diagram_rdb.png?raw=true)

This layout lets us read a single row at a time and stream through rows efficiently.

**IMPORTANT NOTE:** Protobuf `map<>` does not preserve key ordering according to the spec. In BT we have strict ordering of increasing timestamps (see `std::map<std::chrono::milliseconds, std::string, std::greater<>> cells_;` in `column_family.h`)
This means that we need to do some suboptimal stuff in `storage.h`. To stream rows we need to construct iterator. As we group the data we load one row at a time - see `RocksDBStorageRowTX::LoadRow()`. When it happens we 
need to map protobuf into C++ map to get correct ordering. I think this is avoidable somehow, but that kind of performance issue isn't our highest-priority concern right now.

### CustomStorage: implementing custom storage interface

To implement custom storage you need to provide two classes: one that inherits from `Storage` in `persist/storage.h` and one that inherits from `StorageRowTX` in `persist/storage_row_tx.h`. The storage class is responsible for metadata, table lifecycle, and opening/closing the backend; it must also create row-scoped transactions that return your `StorageRowTX` implementation. The row transaction class performs all per-row mutations (SetCell, DeleteRowColumn, etc.) and commit/rollback. Reference implementations are `MemoryStorage` with `MemoryStorageRowTX` in `persist/memory/` and `RocksDBStorage` with `RocksDBStorageRowTX` in `persist/rocksdb/`.

**Storage (persist/storage.h)** — implement these virtual methods:

| Method | Description (from source) |
|--------|---------------------------|
| `UncheckedOpen(additional_cf_names)` | Protected. Setup the storage engine; guaranteed not to be double-invoked (either first call or after `Close()`). Argument: list of extra column families to load. |
| `UncheckedClose()` | Protected. Tear down the storage engine; same single-invocation guarantee as `UncheckedOpen()`. |
| `RowTransaction(table_name, row_key)` | Starts a row-scoped transaction; returns your `StorageRowTX` implementation. |
| `CreateTable(schema)` | Creates a table with the given schema. |
| `DeleteTable(table_name, precondition_fn)` | Deletes a table after running the precondition. |
| `GetTable(table_name)` | Returns table metadata. |
| `UpdateTableMetadata(table_name, meta)` | Updates table metadata (e.g. after CreateTable()). |
| `EnsureColumnFamiliesExist(cf_names)` | Ensures the given column families exist. |
| `HasTable(table_name)` | Returns true if the table exists. |
| `DeleteColumnFamily(cf_name)` | Deletes a column family (may be expensive depending on implementation). |
| `Tables(prefix)` | Returns a view of table metadata with keys starting with prefix. Guarantees: random const access iterator (std contiguous range), snapshot of tables at call time even if mutations happen meanwhile. |
| `StreamTable(table_name, range_set, prefetch_all_columns)` | Returns a cell stream over the table for the given row set. `prefetch_all_columns` can fetch all columns for a row in bulk (useful for small column sizes); implementation may ignore it. |

`Open()` and `Close()` are non-virtual helpers that guard against double open/close and call your `UncheckedOpen`/`UncheckedClose`. `Tables()` with no argument and `StreamTableFull`/`StreamTable` with one or two arguments delegate to the methods above.

**StorageRowTX (persist/storage_row_tx.h)** — implement commit/rollback and row mutations:

| Method | Description (from source) |
|--------|---------------------------|
| `Commit()` | Commits the transaction. |
| `Rollback(s)` | Rolls back the transaction with the given status. |
| `SetCell(column_family, column_qualifier, timestamp, value)` | Sets a specific cell value. |
| `UpdateCell(column_family, column_qualifier, timestamp, value, update_fn)` | Updates a cell with the given functor; returns the old value. |
| `DeleteRowColumn(column_family, column_qualifier, time_range)` | Deletes cells in the given time range (inclusive start, exclusive end). |
| `DeleteRowColumn(..., start, end)` | Overload with start/end in milliseconds. |
| `DeleteRowColumn(..., value)` | Deletes the cell at the exact timestamp (start <= timestamp < start+1ms). |
| `DeleteRowFromColumnFamily(column_family)` | Deletes the row from the given column family. |
| `DeleteRowFromAllColumnFamilies()` | Deletes the row from all column families. |

Wire your storage into the emulator the same way RocksDB is wired in `server.cc` (e.g. instantiate your `Storage` and pass it to the server/cluster).

### Testing

We provide several levels of testing:

| Test file                           | Level        | Storage      | Description |
|-------------------------------------|--------------|--------------|-------------|
| `persist/integration/read_test.cc`  | Integration  | RocksDB      | Uses the CBT client: creates a table, applies mutations, reads rows, and verifies data persists across restarts. |
| `persist/memory/storage_test.cc`    | Unit         | MemoryStorage| Exercises the `Storage` interface: CreateTable, row transactions, SetCell, DeleteRowColumn, DeleteRowFromColumnFamily, etc. |
| `cluster_test.cc`                   | Unit         | RocksDB      | Cluster API: CreateTable, ListTables, GetTable, mutations, CheckAndMutateRow for a given storage backend. |
| `server_test.cc`                   | Unit         | RocksDB      | Server RPC layer: gRPC Bigtable and BigtableTableAdmin stubs against an in-process emulator server. |

### What's left for the future (TODO)?

There are some things that can be added/improved:

1. **Garbage collection** - Automatic deletion of old cell versions based on `gc_rule`. See `TODO` in code.
2. **Atomic schema updates** - ModifyColumnFamilies doesn't save changes to storage
3. **Efficient bulk deletes** - DropRowRange could use RocksDB DeleteRange instead of iteration
4. **Better CF management** - Drop/Create column families sometimes requires DB restart now. Should be easy to implement.

## About

This is a single-node, persistent emulator for Google's Bigtable.

It should pass all the integration tests in Google's C++ client
repository (google-cloud-cpp), except those that must run against
production Bigtable.

## Dependencies

The Bigtable-emulator depends on `google-cloud-cpp` (which the build
tools retrieve and build automatically) and the `abseil`
library. Other dependencies such as `GRPC` are provided by
`google-cloud-cpp`.

## Building

Building the Bigtable emulator requires `bazel`.

```shell
bazel build //...
```

## Running the Unit Tests

```shell
bazel test //...
```

## Running clang-tidy with Bazel

```shell
bazel build --config clang-tidy //...
```

## Running the Emulator

```shell
bazel run :emulator -- --host=127.0.0.1 --port=8888
```

or just

```shell
bazel run :emulator
```

## Connecting to the Emulator

Install [CLI tool - cbt](https://docs.cloud.google.com/bigtable/docs/cbt-overview)

Create `~/.cbtrc` file. Example:
```
project = projekcik
instance = instancyjka
creds = whatever
```
Set env variable. Example:
```
export BIGTABLE_EMULATOR_HOST=localhost:8888
```

Now you can use `cbt` and your data will persist between emulator restarts:
```bash
cbt createtable my-table
cbt createfamily my-table cf1
cbt set my-table row1 cf1:col1=value1
cbt read my-table
# restart emulator
cbt read my-table  # data is still there
```

## Using RocksDB

See `server.cc` for wiring RocksDB storage into the emulator and `persist/storage.h` for the abstract storage interface.

## `compile_commands.json`

If you need to generate `compile_commands.json` for your tooling, run:
```shell
bazel run --config=compile-commands
```

## Development

It's a good idea to set home (`$HOME/.bazelrc` on Unixes) or system `bazelrc`
and enable compilation cache there with this line:
```
build --disk_cache=~/.cache/bazel/disk-cache
```
Note that the cache directory grows indefinitely.

### Formatting the code

```bash
# On bash you need to enable globstar with `shopt -s globstar` first
clang-format -i -style=file -assume-filename=.clang-format **/*.cc **/*.h
```

### `compile_commands.json`

If you need to generate `compile_commands.json` for your tooling, run:
```shell
bazel run --config=compile-commands
```

## Contributing changes

See [`CONTRIBUTING.md`](/CONTRIBUTING.md) for details on how to contribute to
this project, including how to build and test your changes as well as how to
properly format your code.

## Licensing

Apache 2.0; see [`LICENSE`](/LICENSE) for details.
