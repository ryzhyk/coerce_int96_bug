# `Int96Coercer` drops the metadata of struct, list and map fields

Reading a Parquet file with `datafusion.execution.parquet.coerce_int96` set is
supposed to change one thing: the time unit that INT96 timestamp columns decode
at. It also empties the metadata of every struct, list and map field in the
file.

Reproduced against **DataFusion 55.0.0**.

## Reproduce

```
cargo test
```

```
test without_int96_nothing_is_coerced ... ok
test struct_field_keeps_its_metadata ... FAILED

assertion `left == right` failed: struct field lost its metadata
  left: []
 right: [("PARQUET:field_id", "2")]
```

The test writes a Parquet file with a field id on every field, the way Spark and
Delta Lake write them:

```
message spark_schema {
    REQUIRED INT64 id = 1;
    OPTIONAL group nested = 2 {
        OPTIONAL BYTE_ARRAY name (STRING) = 3;
    }
    OPTIONAL INT96 ts = 4;
}
```

and calls `Int96Coercer` on it (`coerce_int96_to_resolution`, deprecated
since 53.2.0, is a thin wrapper over the same code and behaves identically). The leaf field `id` keeps
`PARQUET:field_id`. The struct field `nested` loses it.

The second test is the control: the same file with an INT64 microsecond
timestamp is not coerced at all, and nothing is lost. The metadata disappears
because the file contains an INT96 column, not because of anything about the
struct.

## Cause

In `datafusion/datasource-parquet/src/schema_coercion.rs`, leaf fields are
cloned:

```rust
// :423
fn field_with_new_type(field: &FieldRef, new_type: DataType) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(new_type))
}
```

while container fields are constructed fresh, and `Field::new*` starts from
empty metadata:

```rust
// :327
let processed_struct = Field::new_struct(
    current_field.name(),
    processed_children.as_slice(),
    current_field.is_nullable(),
);
// :360
let processed_list = Field::new_list(
    current_field.name(),
    Arc::clone(&processed_children[0]),
    current_field.is_nullable(),
);
// :392
DataType::Map(Arc::clone(&processed_children[0]), *sorted),
```

Name, type and nullability are carried across; `current_field.metadata()` is
never read.

This looks like an oversight rather than a decision:

- the same function ends with
  `Schema::new_with_metadata(fields, file_schema.metadata.clone())` (`:412`),
  so schema-level metadata is deliberately preserved;
- `apply_file_schema_type_coercions`, in the same file, routes every field
  through `field_with_new_type` and so preserves field metadata;
- iceberg-rust's equivalent INT96 coercion calls
  `.with_metadata(field_info.metadata().clone())` on struct, list, map and leaf
  alike.

## Why it matters

`PARQUET:field_id` lives in that metadata, and formats that identify columns by
id rather than by name rely on it. Delta Lake's column mapping is one: the read
schema names columns `col-<id>` and pairs them with the file by field id. A
struct column that loses its id is resolved by neither id nor name, so it reads
back as NULL, or errors if the table declares it NOT NULL.

The two conditions meet in ordinary tables: the writers that emit INT96 are the
same ones that enable column mapping.

Any other field metadata goes the same way, including an Arrow extension type
(`ARROW:extension:name`) declared on a struct.

## Suggested fix

Carry the metadata over in the three constructors, for example:

```rust
let processed_struct = Field::new_struct(
    current_field.name(),
    processed_children.as_slice(),
    current_field.is_nullable(),
)
.with_metadata(current_field.metadata().clone());
```

and likewise for the list and map arms. `fix.patch` in this repository is that
change against `datafusion/datasource-parquet/src/schema_coercion.rs`; applying
it to DataFusion 55.0.0 and pointing this crate at the patched source with

```toml
[patch.crates-io]
datafusion-datasource-parquet = { path = "..." }
```

makes both tests pass. That is how the fix was verified, not by inspection.
