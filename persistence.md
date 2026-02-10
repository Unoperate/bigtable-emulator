# Bigtable Emulator Persistence Design

## Overview

This document describes the persistence subsystem built on top of the existing
in-memory Bigtable implementation.

Persistence has two goals:

1. Persist table metadata (schemas + manifest of existing tables).
2. Persist table cell data so reads survive process restarts.

Runtime behavior is hybrid:

- Mutation logic executes through in-memory `Table` / `ColumnFamily` objects.
- With storage enabled, reads are served from RocksDB-backed iterators.

## Storage Model

### RocksDB column families

- **Default CF** stores metadata:
  - table schema blobs
  - manifest
- **One RocksDB CF per Bigtable column family**:
  - CF name format: `<table_name>/<column_family_name>`

### Metadata keys

- Manifest key: `/sys/tables/_manifest`
- Table schema key: `/sys/tables/<full_table_name>`

Manifest value is newline-separated table schema keys.

### Cell key format

Within each RocksDB CF, cells are stored as:

`/tables/<table_name>/<row_key>/<column_qualifier>/<timestamp_ms>`

Value is the raw cell value bytes.

## Lifecycle

### Startup restore

On emulator startup:

1. Global storage is initialized (`InitGlobalStorage`).
2. Manifest is loaded from `/sys/tables/_manifest`.
3. Each referenced schema blob is parsed into `google::bigtable::admin::v2::Table`.
4. Runtime `Table` instances are created and attached to the cluster.

The RocksDB directory is selected by the emulator `--db_path` flag
(default: `test_db`).

This restores table definitions and enables reading persisted cell data through persistent streams.

### Schema persistence

Schema persistence happens on:

- table create
- table update
- column family modifications

The helper `store_schema(...)` writes:

- the serialized table proto under `/sys/tables/<table_name>`
- manifest update if table key is not already listed

This avoids duplicate manifest entries.

## Data Operations Persistence

### Persisted write/delete paths

The following operations mirror changes to RocksDB:

- `SetCell` -> `Storage::PutCell(...)`
- `DeleteFromColumn` -> `Storage::DeleteColumn(...)`
- `DeleteFromFamily` -> `Storage::DeleteCFRow(...)`
- `DeleteFromRow` -> `Storage::DeleteRow(...)`
- Drop column family (admin path) -> `Storage::DeleteColumnFamily(...)`
- Drop table -> `Storage::DeleteTable(...)`

### Rollback consistency

Row-level mutation transactions use an undo log.

When rollback needs to remove a cell that was newly created in the transaction (`DeleteValue` undo path), the system now deletes in both places:

1. RocksDB (`Storage::DeleteCell(...)`)
2. in-memory column family (`DeleteTimeStamp(...)`)

This keeps storage and memory consistent when a multi-mutation request fails partway through.

### Read path with persistence enabled

When global storage is present, `Table::CreateCellStream(...)` uses `PersistentFilteredColumnFamilyStream` instead of in-memory family streams.

That stream:

- iterates RocksDB keys in the target CF
- parses row key / qualifier / timestamp from key bytes
- applies row, column, regex, and timestamp filters
- returns `CellView` values sourced from persistent data

## Test Coverage

### `storage_test.cc`

Validates core RocksDB behavior:

- put/get/delete metadata rows
- batch writes
- per-cell writes
- delete column / CF row / row
- table deletion behavior (manifest + CF cleanup)
- reopen persistence behavior

### `table_persistence_test.cc`

Validates table-level persistence behavior:

- create persists schema + manifest
- prefixed CF names are normalized in persisted schema
- modify/update schema persistence
- manifest deduplication on repeated create
- failed `MutateRow` rollback does not leave persisted row data
