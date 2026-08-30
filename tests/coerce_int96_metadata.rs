//! `coerce_int96_to_resolution` drops the metadata of struct, list and map
//! fields.
//!
//! The file below carries a Parquet field id on every field, the way Spark and
//! Delta Lake write them. Reading it with `coerce_int96` enabled is supposed to
//! change one thing, the time unit of the INT96 column. It also empties the
//! metadata of every container field in the file.

use std::fs::File;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, TimeUnit};
use datafusion::datasource::file_format::parquet::Int96Coercer;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::data_type::{ByteArray, ByteArrayType, Int64Type, Int96, Int96Type};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::writer::SerializedFileWriter;
use datafusion::parquet::schema::parser::parse_message_type;

/// A file with a field id on every field, a struct, and one timestamp column
/// whose physical type is `timestamp`: either the deprecated INT96 that Spark
/// writes, or INT64 microseconds.
fn write_parquet(path: &std::path::Path, int96: bool) {
    let ts_column = if int96 {
        "OPTIONAL INT96 ts = 4;"
    } else {
        "OPTIONAL INT64 ts (TIMESTAMP(MICROS,true)) = 4;"
    };
    let message_type = format!(
        "message spark_schema {{
            REQUIRED INT64 id = 1;
            OPTIONAL group nested = 2 {{
                OPTIONAL BYTE_ARRAY name (STRING) = 3;
            }}
            {ts_column}
        }}"
    );

    let schema = Arc::new(parse_message_type(&message_type).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(File::create(path).unwrap(), schema, props).unwrap();
    let mut row_group = writer.next_row_group().unwrap();

    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&[1], None, None)
        .unwrap();
    column.close().unwrap();

    let mut column = row_group.next_column().unwrap().unwrap();
    column
        .typed::<ByteArrayType>()
        .write_batch(&[ByteArray::from("x")], Some(&[2]), None)
        .unwrap();
    column.close().unwrap();

    let mut column = row_group.next_column().unwrap().unwrap();
    if int96 {
        // 2024-06-01T12:00:00Z as a Julian day plus nanoseconds within the day.
        let mut value = Int96::new();
        value.set_data(0x4B9E_2C00, 0x0000_2CB4, 2_460_463);
        column
            .typed::<Int96Type>()
            .write_batch(&[value], Some(&[1]), None)
            .unwrap();
    } else {
        column
            .typed::<Int64Type>()
            .write_batch(&[1_717_243_200_000_000], Some(&[1]), None)
            .unwrap();
    }
    column.close().unwrap();

    row_group.close().unwrap();
    writer.close().unwrap();
}

/// Metadata of the `nested` struct field and of the `id` leaf field, before and
/// after coercion. `None` means the file had no INT96 column, so coercion did
/// not apply.
fn metadata_across_coercion(int96: bool) -> Option<(Vec<(String, String)>, Vec<(String, String)>)> {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("data.parquet");
    write_parquet(&path, int96);

    let file = File::open(&path).unwrap();
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::default()).unwrap();

    let sorted = |field: &Field| {
        let mut pairs: Vec<(String, String)> = field
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        pairs.sort();
        pairs
    };

    let coerced = Int96Coercer::new(
        metadata.parquet_schema(),
        metadata.schema(),
        &TimeUnit::Microsecond,
    )
    .coerce()?;

    Some((
        sorted(coerced.field_with_name("nested").unwrap()),
        sorted(coerced.field_with_name("id").unwrap()),
    ))
}

/// The bug: a struct field loses its metadata, and with it the Parquet field
/// id that identifies the column.
#[test]
fn struct_field_keeps_its_metadata() {
    let (nested, id) = metadata_across_coercion(true).expect("an INT96 file must be coerced");

    // Leaf fields are fine: they go through `field_with_new_type`, which clones.
    assert_eq!(
        id,
        vec![("PARQUET:field_id".to_string(), "1".to_string())],
        "leaf field lost its metadata",
    );

    // Container fields are rebuilt with `Field::new_struct`, which starts from
    // empty metadata. This is the bug.
    assert_eq!(
        nested,
        vec![("PARQUET:field_id".to_string(), "2".to_string())],
        "struct field lost its metadata",
    );
}

/// Control: the same file with an INT64 microsecond timestamp needs no
/// coercion, so the metadata is never touched. The loss above is caused by the
/// presence of an INT96 column, not by anything about the struct.
#[test]
fn without_int96_nothing_is_coerced() {
    assert!(
        metadata_across_coercion(false).is_none(),
        "a file with no INT96 column must not be coerced",
    );
}
