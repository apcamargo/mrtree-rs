use std::io::{BufReader, BufWriter, Read, Write};

use csv::{ReaderBuilder, StringRecord, WriterBuilder};

use crate::error::MrtreeError;
use crate::model::{EffectiveTable, InputTable, LabelMatrix, Path, PathLabel, RealLabel};

pub fn read_tsv<R>(reader: R, header: bool) -> crate::Result<InputTable>
where
    R: Read,
{
    let mut csv_reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(header)
        .flexible(true)
        .from_reader(BufReader::new(reader));

    let (sample_header, cluster_headers, mut expected_fields) = if header {
        let headers = csv_reader.headers().map_err(tsv_read_error)?.clone();
        if headers.len() < 3 {
            return Err(MrtreeError::InputHasTooFewColumns);
        }
        (
            Some(headers.get(0).unwrap_or_default().to_owned()),
            Some(
                headers
                    .iter()
                    .skip(1)
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>(),
            ),
            Some(headers.len()),
        )
    } else {
        (None, None, None)
    };

    let mut sample_ids = Vec::new();
    let mut labels = Vec::new();
    let first_data_line = if header { 2 } else { 1 };

    for (row_idx, record) in csv_reader.records().enumerate() {
        let line_number = first_data_line + row_idx;
        let record = record.map_err(tsv_read_error)?;
        validate_record_width(&record, expected_fields, line_number)?;
        expected_fields = Some(record.len());

        if record.len() < 3 {
            return Err(MrtreeError::InputHasTooFewColumns);
        }

        let row_labels = parse_cluster_row(&record, line_number, header)?;
        sample_ids.push(record.get(0).unwrap_or_default().to_owned());
        labels.extend(row_labels);
    }

    if sample_ids.is_empty() {
        return Err(if header {
            MrtreeError::HeaderOnlyInput
        } else {
            MrtreeError::EmptyInput
        });
    }

    let n_fields = expected_fields.unwrap_or_default();
    let n_rows = sample_ids.len();
    InputTable::new(
        sample_header,
        sample_ids,
        cluster_headers,
        LabelMatrix::new(n_rows, n_fields - 1, labels),
    )
}

pub fn write_tsv<W>(
    writer: W,
    include_header: bool,
    effective: &EffectiveTable,
    output: &[Path],
) -> crate::Result<()>
where
    W: Write,
{
    effective.validate_output_row_count(output.len())?;

    let mut csv_writer = WriterBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_writer(BufWriter::new(writer));

    if include_header {
        csv_writer
            .write_record(build_header_row(effective))
            .map_err(tsv_write_error)?;
    }

    for (row_idx, (sample_id, path)) in effective.sample_ids().iter().zip(output).enumerate() {
        effective.validate_output_path(row_idx, path)?;
        let mut row = Vec::with_capacity(path.len() + 1);
        row.push(sample_id.clone());
        row.extend(path.iter().map(|label| serialize_path_label(*label)));
        csv_writer.write_record(row).map_err(tsv_write_error)?;
    }

    csv_writer.flush().map_err(tsv_write_error)?;
    Ok(())
}

fn tsv_read_error(error: impl std::fmt::Display) -> MrtreeError {
    MrtreeError::TsvRead(error.to_string())
}

fn tsv_write_error(error: impl std::fmt::Display) -> MrtreeError {
    MrtreeError::TsvWrite(error.to_string())
}

fn build_header_row(effective: &EffectiveTable) -> Vec<String> {
    let mut header = Vec::with_capacity(effective.labels().n_cols() + 1);
    header.push(
        effective
            .sample_header()
            .map_or_else(|| "sample_id".to_owned(), ToOwned::to_owned),
    );

    if let Some(cluster_headers) = effective.cluster_headers() {
        header.extend(cluster_headers.iter().cloned());
    } else {
        header.extend(
            effective
                .original_column_indices()
                .iter()
                .map(|&column| fallback_level_name(column)),
        );
    }

    header
}

fn fallback_level_name(column: usize) -> String {
    format!("level_{}", column + 1)
}

fn validate_record_width(
    record: &StringRecord,
    expected_fields: Option<usize>,
    line_number: usize,
) -> crate::Result<()> {
    if let Some(expected) = expected_fields
        && record.len() != expected
    {
        return Err(MrtreeError::RaggedRow {
            line: line_number,
            expected,
            actual: record.len(),
        });
    }

    Ok(())
}

fn parse_cluster_row(
    record: &StringRecord,
    line_number: usize,
    header_enabled: bool,
) -> crate::Result<Vec<RealLabel>> {
    let cluster_field_count = record.len().saturating_sub(1);
    let mut first_failed_column = None;
    let mut all_failed = true;
    let mut labels = Vec::with_capacity(cluster_field_count);

    for (cluster_idx, field) in record.iter().skip(1).enumerate() {
        if field.is_empty() {
            return Err(MrtreeError::MissingClusterLabel {
                line: line_number,
                column: cluster_idx + 2,
            });
        }

        if field.eq_ignore_ascii_case("na") || field.eq_ignore_ascii_case("nan") {
            return Err(MrtreeError::InvalidClusterLabel {
                line: line_number,
                column: cluster_idx + 2,
                value: field.to_owned(),
                hint: String::new(),
            });
        }

        if let Ok(value) = field.parse::<u64>() {
            labels.push(RealLabel::new(value));
            all_failed = false;
        } else if let Ok(value) = field.parse::<i64>() {
            if value < 0 {
                return Err(MrtreeError::NegativeClusterLabel {
                    line: line_number,
                    column: cluster_idx + 2,
                    value: field.to_owned(),
                });
            }
        } else {
            first_failed_column.get_or_insert_with(|| (cluster_idx + 2, field.to_owned()));
        }
    }

    if let Some((column, value)) = first_failed_column {
        let hint = if !header_enabled && all_failed && line_number == 1 {
            "; input appears to have a header row; retry with --header".to_owned()
        } else {
            String::new()
        };
        return Err(MrtreeError::InvalidClusterLabel {
            line: line_number,
            column,
            value,
            hint,
        });
    }

    Ok(labels)
}

fn serialize_path_label(label: PathLabel) -> String {
    match label {
        PathLabel::Real(value) => value.to_string(),
        PathLabel::Augmented => "-1".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headerless_effective_table(original_column_indices: Vec<usize>) -> EffectiveTable {
        EffectiveTable::new(
            None,
            vec!["sample_a".to_owned(), "sample_b".to_owned()],
            None,
            LabelMatrix::new(
                2,
                2,
                vec![
                    RealLabel::new(11),
                    RealLabel::new(101),
                    RealLabel::new(22),
                    RealLabel::new(202),
                ],
            ),
            original_column_indices,
            vec![2, 3],
        )
        .expect("effective table should be valid")
    }

    fn headerless_output_paths() -> Vec<Path> {
        vec![
            vec![
                PathLabel::Real(RealLabel::new(1)),
                PathLabel::Real(RealLabel::new(10)),
            ],
            vec![
                PathLabel::Real(RealLabel::new(2)),
                PathLabel::Real(RealLabel::new(20)),
            ],
        ]
    }

    #[test]
    fn rejects_empty_input() {
        let error = read_tsv(&b""[..], false).expect_err("empty input should fail");

        assert!(matches!(error, MrtreeError::EmptyInput));
    }

    #[test]
    fn read_tsv_headerless_invalid_first_row_reports_line_one_with_header_hint() {
        let error = read_tsv(&b"sample\tk1\tk2\n"[..], false)
            .expect_err("header-like first row should fail without --header");

        assert!(matches!(
            error,
            MrtreeError::InvalidClusterLabel {
                line: 1,
                column: 2,
                value,
                hint,
            } if value == "k1"
                && hint == "; input appears to have a header row; retry with --header"
        ));
    }

    #[test]
    fn read_tsv_headered_invalid_first_data_row_reports_line_two() {
        let error = read_tsv(&b"sample\tk1\tk2\nrow_a\tx\t1\n"[..], true)
            .expect_err("invalid first data row should preserve physical line number");

        assert!(matches!(
            error,
            MrtreeError::InvalidClusterLabel {
                line: 2,
                column: 2,
                value,
                hint,
            } if value == "x" && hint.is_empty()
        ));
    }

    #[test]
    fn read_tsv_headered_ragged_second_data_row_reports_line_three() {
        let error = read_tsv(&b"sample\tk1\tk2\nrow_a\t1\t2\nrow_b\t3\t4\t5\n"[..], true)
            .expect_err("ragged row should report physical file line number");

        assert!(matches!(
            error,
            MrtreeError::RaggedRow {
                line: 3,
                expected: 3,
                actual: 4,
            }
        ));
    }

    #[test]
    fn write_tsv_rejects_output_with_wrong_row_count() {
        let effective = headerless_effective_table(vec![1, 3]);
        let error = write_tsv(
            Vec::new(),
            false,
            &effective,
            &[vec![
                PathLabel::Real(RealLabel::new(1)),
                PathLabel::Real(RealLabel::new(10)),
            ]],
        )
        .expect_err("mismatched output row count should fail");

        assert!(matches!(
            error,
            MrtreeError::InternalAlgorithmInvariantViolation(message)
                if message == "output contains 1 rows, expected 2"
        ));
    }

    #[test]
    fn write_tsv_with_synthesized_headers_round_trips_through_headered_reader() {
        let effective = headerless_effective_table(vec![1, 3]);
        let mut written = Vec::new();

        write_tsv(&mut written, true, &effective, &headerless_output_paths())
            .expect("headered output should succeed");

        let reparsed = read_tsv(written.as_slice(), true)
            .expect("synthesized headers should produce valid headered TSV");
        let expected_headers = vec!["level_2".to_owned(), "level_4".to_owned()];

        assert_eq!(reparsed.sample_header(), Some("sample_id"));
        assert_eq!(
            reparsed.cluster_headers(),
            Some(expected_headers.as_slice())
        );
        assert_eq!(reparsed.sample_ids(), ["sample_a", "sample_b"]);
        assert_eq!(reparsed.labels().n_cols(), 2);
    }
}
