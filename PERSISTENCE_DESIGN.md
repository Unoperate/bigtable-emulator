# Persistence Design

This file contains a basic design of adding persistence to the emulator.

Persistence itself will be done using rocksdb.

The general idea is to reuse as much of the existing code as possible. This means that for classes such as
Table, ColumnFamily, RowTransaction etc. there should be a similar class wrapping rocksdb functionality.
This way, functions responsible for handling gRPC requests on the server side should not change much.

As there is no concept of a table in rocksdb, each table in the emulator
will be represented by a separate database instance. This way, it will be easier to
use multiple tables, and each one of them will have a separate directory in the file system.

Because rocksdb supports a concept of a column family, each column family in the
emulator will have a backing column family in the rocksdb (represented by 
a ColumnFamilyHandle object, wrapped inside a PersistentColumnFamily class).

There is no concept of a row inside rocksdb. Because of that, each key in the database
will have a format: ```row_key#col_key#(~timestamp)```. '~' performed on the timestamp will preserve
the required ordering of keys. # is a separator in the form "\x00\x01". The "logical" separator is ';', and 0x01
is there to distinguish it from the ';' already present in row_key or col_key. ';' present in them are swapped for
";\xFF". Form implementation details, see "key_coder.h".

Transactions will be done using pessimistic transactions built in the RocksDB. To use them,
we use ```rocksdb::TransactionDB``` instead of the ```rocksdb::DB``` as the storage for a table.

## Schema Persistence

Each table has its own RocksDB instance (one DB directory per table). The table schema is persisted inside that same DB so that the emulator can be restarted and reconstruct:

- the `google::bigtable::admin::v2::Table` proto (table name + column families + GC rules etc.),
- the corresponding RocksDB column families that must exist for that schema.

Schema data is stored in the default column family under reserved metadata keys.

### Reserved keys

- Current schema
  - Key: `t_emulator:meta:schema_pb`
  - Value: Protobuf-serialized `google::bigtable::admin::v2::Table`

- Pending (in-progress) schema change
  - Key: `t_emulator:meta:pending_schema_pb`
  - Value: Protobuf-serialized *target* `Table` schema we are transitioning to

The presence of `pending_schema_pb` indicates that a schema modification started and may not have finished (e.g., crash mid-operation).

---

### PersistSchema (writing the stable schema)

`PersistentTableOperations::PersistSchema()` is responsible for committing the “stable” schema to disk.

What it does:

1. Serialize the in-memory `Table` proto using `SerializeToString()`
2. Store the bytes with `db_->Put()` in the default column family under `kSchemaKey`
3. After this succeeds, `schema_pb` is the authoritative schema used on startup

This operation is intentionally simple: a single key/value write that can be read back quickly during `OpenExisting()`.

---

### LoadSchema

`PersistentTableOperations::LoadSchema()` reconstructs the schema from disk:

1. `db_->Get()` from the default CF for key `kSchemaKey`
2. If the key is missing:
   - return `NotFoundError` (used by `CreateNew()` to decide whether to persist the provided schema)
3. If present:
   - parse bytes into a `Table` via `ParseFromString()`
4. Return the deserialized `Table`

---

## Schema modification and crash recovery (SchemaRedoLog)

Changing schema can require creating/dropping RocksDB column families. Those changes are not safe to do “halfway” without a recovery plan. The emulator therefore uses a redo-log style marker to make schema transitions restart-safe.

### Why a redo log is needed

A schema update may involve steps like:

- create new RocksDB column families for newly added Bigtable CFs,
- drop RocksDB column families removed from schema,
- potentially rebuild aggregate CF structures.

If the process crashes after creating some CFs but before writing the final schema, the DB can end up in an intermediate state. `pending_schema_pb` is the durable “intent” record that allows the emulator to complete the transition on restart.

### Modification flow

1. Begin
   - Serialize the *target* schema and write it to `pending_schema_pb`
   - After this point, the system guarantees it can recover to the target schema

2. Reconcile RocksDB state
   - Apply the schema delta to RocksDB:
     - create missing CFs
     - drop removed CFs
     - construct aggregate CFs as needed

3. Commit stable schema
   - Write the target schema to `schema_pb` via `PersistSchema()`

4. Finish (clear redo marker)
   - Delete `pending_schema_pb`
   - This marks the operation as fully complete

---

### Recovery on startup

When opening a table (both `CreateNew` and `OpenExisting` paths), the emulator checks if a schema transition was in progress:

1. Load `pending_schema_pb` (via `SchemaRedoLog::Load()`)
2. If present:
   - we previously crashed before Finish
   - run `ReconcileColumnFamiliesToTarget()` to ensure RocksDB CFs match the pending schema
   - `PersistSchema()` to make the pending schema the stable schema
   - `Finish()` to delete `pending_schema_pb`
3. If absent:
   - proceed with `schema_pb` as the stable, already-committed schema

This ensures the system converges to a consistent state after any crash during schema updates: either the update never started (no pending key), or it can be completed deterministically (pending key exists).