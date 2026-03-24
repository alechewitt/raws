use anyhow::Result;
use serde_json::Value;

#[cfg(test)]
fn format_table(value: &Value) -> Result<String> {
    format_table_with_title(value, None)
}

pub fn format_table_with_title(value: &Value, title: Option<&str>) -> Result<String> {
    let output = match value {
        Value::Null => String::new(),
        Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            // Single scalar: render as a one-cell table
            let s = scalar_to_string(value);
            let total_width = s.len() + 4 + 2; // content + padding + border
            let widths = vec![total_width];
            let mut out = String::new();
            out.push_str(&line_break(&widths));
            out.push('\n');
            out.push_str(&data_row(&widths, &[&s]));
            out.push('\n');
            out.push_str(&line_break(&widths));
            out
        }
        Value::Array(arr) => format_top_level_array(arr),
        Value::Object(map) => format_top_level_object(map, title),
    };
    Ok(output)
}

/// Format a top-level object. The AWS CLI renders each top-level key as a
/// separate section. Scalar-only objects become a vertical key-value table.
fn format_top_level_object(map: &serde_json::Map<String, Value>, title: Option<&str>) -> String {
    if map.is_empty() {
        return String::new();
    }

    // Sort keys alphabetically (matching AWS CLI table formatter behavior)
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();

    // Separate scalar and non-scalar keys (both sorted)
    let scalar_keys: Vec<&String> = keys
        .iter()
        .filter(|k| is_scalar(&map[k.as_str()]))
        .copied()
        .collect();
    let non_scalar_keys: Vec<&String> = keys
        .iter()
        .filter(|k| !is_scalar(&map[k.as_str()]))
        .copied()
        .collect();

    // Check if all values are scalar -> vertical table
    if non_scalar_keys.is_empty() {
        let pairs: Vec<(String, String)> = scalar_keys
            .iter()
            .map(|k| ((*k).clone(), scalar_to_string(&map[k.as_str()])))
            .collect();
        return render_vertical_table(title, &pairs);
    }

    // Mixed: render scalar pairs as vertical table, then each non-scalar key
    // as its own section
    let mut sections: Vec<String> = Vec::new();

    if !scalar_keys.is_empty() {
        let scalar_pairs: Vec<(String, String)> = scalar_keys
            .iter()
            .map(|k| ((*k).clone(), scalar_to_string(&map[k.as_str()])))
            .collect();
        sections.push(render_vertical_table(None, &scalar_pairs));
    }

    for key in &non_scalar_keys {
        let val = &map[key.as_str()];
        match val {
            Value::Array(arr) => {
                if arr.is_empty() {
                    continue;
                }
                sections.push(format_array_section(key, arr));
            }
            Value::Object(inner) => {
                if inner.is_empty() {
                    continue;
                }
                // Nested object: render as vertical table with title
                let mut inner_keys: Vec<&String> = inner.keys().collect();
                inner_keys.sort();
                let pairs: Vec<(String, String)> = inner_keys
                    .iter()
                    .map(|k| ((*k).clone(), value_to_string(&inner[k.as_str()])))
                    .collect();
                sections.push(render_vertical_table(Some(key), &pairs));
            }
            _ => {}
        }
    }

    sections.join("\n")
}

/// Format an array that appears under a named key in the top-level object.
fn format_array_section(title: &str, arr: &[Value]) -> String {
    if arr.is_empty() {
        return String::new();
    }

    // Check if all elements are scalars
    let all_scalars = arr.iter().all(is_scalar);
    if all_scalars {
        let values: Vec<String> = arr.iter().map(scalar_to_string).collect();
        return render_scalar_list_table(title, &values);
    }

    // Check if all elements are objects
    let all_objects = arr.iter().all(|v| v.is_object());
    if all_objects {
        return render_object_list_table(title, arr);
    }

    // Fallback: stringify each element in a single-column table
    let values: Vec<String> = arr.iter().map(value_to_string).collect();
    render_scalar_list_table(title, &values)
}

/// Format a top-level array (bare array, not under a key).
fn format_top_level_array(arr: &[Value]) -> String {
    if arr.is_empty() {
        return String::new();
    }

    let all_scalars = arr.iter().all(is_scalar);
    if all_scalars {
        let values: Vec<String> = arr.iter().map(scalar_to_string).collect();
        return render_scalar_list_table_no_title(&values);
    }

    let all_objects = arr.iter().all(|v| v.is_object());
    if all_objects {
        return render_object_list_table_no_title(arr);
    }

    // Fallback
    let values: Vec<String> = arr.iter().map(value_to_string).collect();
    render_scalar_list_table_no_title(&values)
}

// ---------------------------------------------------------------------------
// Column width calculation (matching AWS CLI algorithm)
// ---------------------------------------------------------------------------

/// Calculate column widths with proportional scaling to match AWS CLI.
///
/// Each returned width represents the total characters a column occupies:
/// - First column: includes left `|` and right `|`
/// - Subsequent columns: include right `|` only (left shared with previous)
///
/// The widths sum to total_width.
fn calculate_widths(max_content_widths: &[usize], title: Option<&str>) -> (usize, Vec<usize>) {
    let padding = 4; // 2-space left pad, 2-space right pad conceptually
    let border = 2; // left | and right |

    let unscaled: Vec<usize> = max_content_widths.iter().map(|w| w + padding).collect();
    let unscaled_sum: usize = unscaled.iter().sum();
    let total_from_cols = unscaled_sum + border;
    let total_from_title = title.map_or(0, |t| t.len() + border);
    let total_width = std::cmp::max(total_from_cols, total_from_title);

    if unscaled_sum == 0 {
        return (total_width, unscaled);
    }

    // Proportionally scale columns so they sum to total_width
    let scale_factor = total_width as f64 / unscaled_sum as f64;
    let mut scaled: Vec<usize> = unscaled
        .iter()
        .map(|w| (*w as f64 * scale_factor).round() as usize)
        .collect();

    // Adjust for rounding errors
    let mut off_by: isize = scaled.iter().sum::<usize>() as isize - total_width as isize;
    while off_by != 0 {
        if off_by > 0 {
            for w in &mut scaled {
                if off_by == 0 {
                    break;
                }
                *w -= 1;
                off_by -= 1;
            }
        } else {
            for w in scaled.iter_mut().rev() {
                if off_by == 0 {
                    break;
                }
                *w += 1;
                off_by += 1;
            }
        }
    }

    (total_width, scaled)
}

// ---------------------------------------------------------------------------
// Rendering helpers (matching AWS CLI table.py)
// ---------------------------------------------------------------------------

/// Render a full-width dash line: `---...---` of exactly total_width characters.
fn dash_line(total_width: usize) -> String {
    "-".repeat(total_width)
}

/// Render a `+---+---+` separator line.
/// First column: `+dashes+` (width chars total).
/// Subsequent: `dashes+` (width chars total).
fn line_break(widths: &[usize]) -> String {
    let mut out = String::new();
    for (i, w) in widths.iter().enumerate() {
        if i == 0 {
            out.push('+');
            out.push_str(&"-".repeat(w.saturating_sub(2)));
            out.push('+');
        } else {
            out.push_str(&"-".repeat(w.saturating_sub(1)));
            out.push('+');
        }
    }
    out
}

/// Render a title line: `|  Title  |` centered within total_width.
fn title_line(title: &str, total_width: usize) -> String {
    center_text(title, total_width, "|", "|")
}

/// Center text between edge characters within a given total width.
/// Matches AWS CLI `center_text()` algorithm.
fn center_text(text: &str, length: usize, left_edge: &str, right_edge: &str) -> String {
    let text_len = text.len();
    let char_start = (length / 2)
        .saturating_sub(text_len / 2)
        .saturating_sub(1);

    let mut out = String::new();
    out.push_str(left_edge);
    out.push_str(&" ".repeat(char_start));
    out.push_str(text);

    let length_so_far = left_edge.len() + char_start + text_len;
    let right_spaces = length.saturating_sub(right_edge.len() + length_so_far);
    out.push_str(&" ".repeat(right_spaces));
    out.push_str(right_edge);
    out
}

/// Left-align text in a cell. Matches AWS CLI `align_left()` algorithm.
fn align_left(text: &str, width: usize, left_edge: &str, right_edge: &str) -> String {
    let text_len = text.len();
    let left_padding = 2;
    let computed_length = text_len + left_padding + left_edge.len() + right_edge.len();
    let padding = if width >= computed_length {
        left_padding
    } else {
        0
    };

    let mut out = String::new();
    out.push_str(left_edge);
    out.push_str(&" ".repeat(padding));
    out.push_str(text);
    let length_so_far = left_edge.len() + padding + text_len;
    let right_spaces = width.saturating_sub(length_so_far + right_edge.len());
    out.push_str(&" ".repeat(right_spaces));
    out.push_str(right_edge);
    out
}

/// Render a data row with left-aligned text in each cell.
/// First column: `|  text  pad|`, subsequent: `  text  pad|`.
fn data_row(widths: &[usize], cells: &[&str]) -> String {
    let mut out = String::new();
    for (i, w) in widths.iter().enumerate() {
        let cell = if i < cells.len() { cells[i] } else { "" };
        if i == 0 {
            out.push_str(&align_left(cell, *w, "|", "|"));
        } else {
            out.push_str(&align_left(cell, *w, "", "|"));
        }
    }
    out
}

/// Render a header row with centered text in each cell.
fn header_row(widths: &[usize], headers: &[String]) -> String {
    let mut out = String::new();
    for (i, w) in widths.iter().enumerate() {
        let header = if i < headers.len() {
            headers[i].as_str()
        } else {
            ""
        };
        if i == 0 {
            out.push_str(&center_text(header, *w, "|", "|"));
        } else {
            out.push_str(&center_text(header, *w, "", "|"));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Table renderers
// ---------------------------------------------------------------------------

/// Render a vertical key-value table with an optional title.
fn render_vertical_table(title: Option<&str>, pairs: &[(String, String)]) -> String {
    if pairs.is_empty() {
        return String::new();
    }

    let max_key_len = pairs.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
    let max_val_len = pairs.iter().map(|(_, v)| v.len()).max().unwrap_or(0);

    let (total_width, widths) = calculate_widths(&[max_key_len, max_val_len], title);

    let mut out = String::new();

    if let Some(t) = title {
        out.push_str(&dash_line(total_width));
        out.push('\n');
        out.push_str(&title_line(t, total_width));
        out.push('\n');
    }

    out.push_str(&line_break(&widths));
    out.push('\n');

    for (key, val) in pairs {
        out.push_str(&data_row(&widths, &[key, val]));
        out.push('\n');
    }

    out.push_str(&line_break(&widths));

    out
}

/// Render a horizontal table with a title, column headers, and rows.
fn render_horizontal_table(
    title: Option<&str>,
    headers: &[String],
    rows: &[Vec<String>],
) -> String {
    let col_count = headers.len();

    // Calculate max content widths across headers and all rows
    let mut max_content: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_count {
                max_content[i] = std::cmp::max(max_content[i], cell.len());
            }
        }
    }

    let (total_width, widths) = calculate_widths(&max_content, title);

    let mut out = String::new();

    // Title section
    if let Some(t) = title {
        out.push_str(&dash_line(total_width));
        out.push('\n');
        out.push_str(&title_line(t, total_width));
        out.push('\n');
    }

    // Header separator
    out.push_str(&line_break(&widths));
    out.push('\n');

    // Header row (centered)
    out.push_str(&header_row(&widths, headers));
    out.push('\n');

    // Separator after headers
    out.push_str(&line_break(&widths));
    out.push('\n');

    // Data rows
    for row in rows {
        let cells: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
        out.push_str(&data_row(&widths, &cells));
        out.push('\n');
    }

    // Bottom border
    out.push_str(&line_break(&widths));

    out
}

/// Render a single-column table for a list of scalars, with title.
fn render_scalar_list_table(title: &str, values: &[String]) -> String {
    if values.is_empty() {
        return String::new();
    }

    let max_val = values.iter().map(|v| v.len()).max().unwrap_or(0);
    let (total_width, widths) = calculate_widths(&[max_val], Some(title));

    let mut out = String::new();

    out.push_str(&dash_line(total_width));
    out.push('\n');
    out.push_str(&title_line(title, total_width));
    out.push('\n');
    out.push_str(&line_break(&widths));
    out.push('\n');

    for val in values {
        out.push_str(&data_row(&widths, &[val.as_str()]));
        out.push('\n');
    }

    out.push_str(&line_break(&widths));

    out
}

/// Render a single-column table for a list of scalars without a title.
fn render_scalar_list_table_no_title(values: &[String]) -> String {
    if values.is_empty() {
        return String::new();
    }

    let max_val = values.iter().map(|v| v.len()).max().unwrap_or(0);
    let (_total_width, widths) = calculate_widths(&[max_val], None);

    let mut out = String::new();

    out.push_str(&line_break(&widths));
    out.push('\n');

    for val in values {
        out.push_str(&data_row(&widths, &[val.as_str()]));
        out.push('\n');
    }

    out.push_str(&line_break(&widths));

    out
}

/// Render a list of objects as a horizontal table with a title.
fn render_object_list_table(title: &str, items: &[Value]) -> String {
    let (headers, rows) = extract_headers_and_rows(items);
    render_horizontal_table(Some(title), &headers, &rows)
}

/// Render a list of objects as a horizontal table without a title.
fn render_object_list_table_no_title(items: &[Value]) -> String {
    let (headers, rows) = extract_headers_and_rows(items);
    render_horizontal_table(None, &headers, &rows)
}

/// Extract sorted headers and row data from a list of JSON objects.
fn extract_headers_and_rows(items: &[Value]) -> (Vec<String>, Vec<Vec<String>>) {
    // Gather all unique keys across items, sorted alphabetically
    let mut header_set = std::collections::BTreeSet::new();
    for item in items {
        if let Value::Object(map) = item {
            for key in map.keys() {
                header_set.insert(key.clone());
            }
        }
    }
    let headers: Vec<String> = header_set.into_iter().collect();

    let rows: Vec<Vec<String>> = items
        .iter()
        .map(|item| {
            headers
                .iter()
                .map(|h| {
                    item.get(h)
                        .map(value_to_string)
                        .unwrap_or_default()
                })
                .collect()
        })
        .collect();

    (headers, rows)
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

fn is_scalar(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn scalar_to_string(value: &Value) -> String {
    match value {
        Value::Null => "None".to_string(),
        Value::Bool(b) => {
            if *b {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => String::new(),
    }
}

fn value_to_string(value: &Value) -> String {
    if is_scalar(value) {
        scalar_to_string(value)
    } else {
        // For non-scalar values in table cells, use compact JSON
        value.to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_table_format_sts_get_caller_identity() {
        // STS get-caller-identity: flat object with all scalar values -> vertical table
        let value = json!({
            "UserId": "AROAZXO3:alechewt",
            "Account": "668864905351",
            "Arn": "arn:aws:sts::668864905351:assumed-role/Admin/alechewt"
        });
        let result = format_table(&value).unwrap();

        // Keys should be sorted alphabetically
        assert!(result.contains("Account"), "should contain Account key");
        assert!(result.contains("668864905351"), "should contain account value");
        assert!(result.contains("Arn"), "should contain Arn key");
        assert!(
            result.contains("arn:aws:sts::668864905351:assumed-role/Admin/alechewt"),
            "should contain arn value"
        );
        assert!(result.contains("UserId"), "should contain UserId key");
        assert!(
            result.contains("AROAZXO3:alechewt"),
            "should contain userid value"
        );

        // Verify structure: should have `+` separators and `|` borders
        assert!(result.contains('+'), "should contain + separators");
        assert!(result.contains('|'), "should contain | borders");
        assert!(result.contains('-'), "should contain - dashes");

        // Verify key-value pairs appear on separate rows
        let lines: Vec<&str> = result.lines().collect();
        // Structure: +---+---+, | key | val |, | key | val |, ..., +---+---+
        assert!(
            lines.first().map_or(false, |l| l.starts_with('+')),
            "first line should be separator"
        );
        assert!(
            lines.last().map_or(false, |l| l.starts_with('+')),
            "last line should be separator"
        );

        // Count data rows (lines starting with | that contain key-value data)
        let data_lines: Vec<&&str> = lines
            .iter()
            .filter(|l| l.starts_with('|') && l.contains("Account"))
            .collect();
        assert_eq!(data_lines.len(), 1, "Account should appear in exactly one row");

        // Verify alphabetical order: Account before Arn before UserId
        let account_pos = result.find("Account").unwrap();
        let arn_pos = result.find("Arn").unwrap();
        let userid_pos = result.find("UserId").unwrap();
        assert!(account_pos < arn_pos, "Account should come before Arn");
        assert!(arn_pos < userid_pos, "Arn should come before UserId");

        // All lines should have the same width
        let bordered_lines: Vec<&str> = lines
            .iter()
            .filter(|l| l.starts_with('|') || l.starts_with('+'))
            .copied()
            .collect();
        if bordered_lines.len() > 1 {
            let expected_len = bordered_lines[0].len();
            for line in &bordered_lines {
                assert_eq!(line.len(), expected_len, "all lines should have same width");
            }
        }
    }

    #[test]
    fn test_table_format_scalar_list() {
        // Object with a list of scalars (like DynamoDB list-tables)
        let value = json!({
            "TableNames": ["my-table-1", "my-table-2", "my-table-3"]
        });
        let result = format_table(&value).unwrap();

        // Should have a title "TableNames"
        assert!(
            result.contains("TableNames"),
            "should contain the key as title"
        );
        assert!(result.contains("my-table-1"), "should contain first table");
        assert!(result.contains("my-table-2"), "should contain second table");
        assert!(result.contains("my-table-3"), "should contain third table");

        // The title line should be bordered with dashes above
        let lines: Vec<&str> = result.lines().collect();
        assert!(
            lines[0].chars().all(|c| c == '-'),
            "first line should be all dashes"
        );
    }

    #[test]
    fn test_table_format_list_of_objects() {
        // Object with a list of objects (like S3 list-buckets Buckets)
        let value = json!({
            "Buckets": [
                {"CreationDate": "2023-01-01T00:00:00", "Name": "my-bucket-1"},
                {"CreationDate": "2023-06-15T00:00:00", "Name": "my-bucket-2"}
            ]
        });
        let result = format_table(&value).unwrap();

        // Should have title "Buckets"
        assert!(result.contains("Buckets"), "should contain Buckets title");

        // Should have column headers
        assert!(
            result.contains("CreationDate"),
            "should contain CreationDate header"
        );
        assert!(result.contains("Name"), "should contain Name header");

        // Should have data
        assert!(
            result.contains("my-bucket-1"),
            "should contain first bucket name"
        );
        assert!(
            result.contains("my-bucket-2"),
            "should contain second bucket name"
        );
        assert!(
            result.contains("2023-01-01T00:00:00"),
            "should contain first date"
        );

        // Verify horizontal table structure (headers appear between separators)
        let lines: Vec<&str> = result.lines().collect();
        // Find the header line
        let header_line = lines
            .iter()
            .find(|l| l.contains("CreationDate") && l.contains("Name"));
        assert!(header_line.is_some(), "should have a header line with both column names");
    }

    #[test]
    fn test_table_format_empty_object() {
        let value = json!({});
        let result = format_table(&value).unwrap();
        assert_eq!(result, "", "empty object should produce empty output");
    }

    #[test]
    fn test_table_format_mixed_scalar_and_list() {
        // Object with both scalar values and a list
        let value = json!({
            "Count": 3,
            "Items": ["a", "b", "c"]
        });
        let result = format_table(&value).unwrap();

        // Scalar "Count" should appear as a vertical key-value pair
        assert!(result.contains("Count"), "should contain Count key");
        assert!(result.contains("3"), "should contain count value");

        // List "Items" should appear as a titled section
        assert!(result.contains("Items"), "should contain Items title");
        assert!(result.contains("a"), "should contain first item");
        assert!(result.contains("b"), "should contain second item");
        assert!(result.contains("c"), "should contain third item");
    }

    #[test]
    fn test_table_format_null_value() {
        let value = Value::Null;
        let result = format_table(&value).unwrap();
        assert_eq!(result, "", "null should produce empty output");
    }

    #[test]
    fn test_table_format_scalar_string() {
        let value = json!("hello");
        let result = format_table(&value).unwrap();
        assert!(result.contains("hello"), "should contain the scalar value");
        assert!(result.contains('|'), "should have table borders");
    }

    #[test]
    fn test_table_format_line_break() {
        // Width 7 = first column (includes both | edges)
        // Width 10 = second column (includes right | only)
        // +-----+--------+  (no, that's the old format)
        // New format: first: +dashes+ (width chars), subsequent: dashes+ (width chars)
        // width=7: +-----+ (1+5+1=7), width=10: ---------+ (9+1=10)
        assert_eq!(line_break(&[7, 10]), "+-----+---------+");
        // Single column, width=5: +---+ (1+3+1=5)
        assert_eq!(line_break(&[5]), "+---+");
    }

    #[test]
    fn test_table_format_data_row() {
        // First column width=12 includes both |, second width=14 includes right |
        // align_left("foo", 12, "|", "|") = |  foo     | (12 chars)
        // align_left("bar", 14, "", "|") =   bar         | (14 chars)
        let row = data_row(&[12, 14], &["foo", "bar"]);
        assert_eq!(row.len(), 12 + 14);
        assert!(row.starts_with("|  foo"));
        assert!(row.ends_with('|'));
        assert!(row.contains("bar"));
    }

    #[test]
    fn test_table_format_center_text() {
        // center_text("Hi", 12, "|", "|")
        // char_start = 12/2 - 2/2 - 1 = 6 - 1 - 1 = 4
        // | + 4 spaces + Hi + spaces + |
        // length_so_far = 1 + 4 + 2 = 7, right_spaces = 12 - 1 - 7 = 4
        // Result: |    Hi    | (12 chars)
        let centered = center_text("Hi", 12, "|", "|");
        assert_eq!(centered.len(), 12);
        assert!(centered.contains("Hi"));
        assert_eq!(centered, "|    Hi    |");
    }

    #[test]
    fn test_table_format_title_line() {
        let t = title_line("MyTitle", 22);
        assert!(t.starts_with('|'));
        assert!(t.ends_with('|'));
        assert!(t.contains("MyTitle"));
        assert_eq!(t.len(), 22);
    }

    #[test]
    fn test_table_format_vertical_table_structure() {
        let pairs = vec![
            ("Key1".to_string(), "Value1".to_string()),
            ("LongerKey".to_string(), "V2".to_string()),
        ];
        let result = render_vertical_table(Some("Test"), &pairs);

        let lines: Vec<&str> = result.lines().collect();
        // Structure: dashes, title, separator, row, row, separator
        assert_eq!(lines.len(), 6);
        assert!(lines[0].chars().all(|c| c == '-'), "top dashes");
        assert!(lines[1].contains("Test"), "title");
        assert!(lines[2].starts_with('+'), "separator after title");
        assert!(lines[3].contains("Key1"), "first data row");
        assert!(lines[3].contains("Value1"), "first data value");
        assert!(lines[4].contains("LongerKey"), "second data row");
        assert!(lines[5].starts_with('+'), "bottom separator");

        // All lines should have the same width
        let expected_len = lines[0].len();
        for line in &lines {
            assert_eq!(line.len(), expected_len, "all lines same width: '{}'", line);
        }
    }

    #[test]
    fn test_table_format_horizontal_table_structure() {
        let headers = vec!["Name".to_string(), "Age".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "30".to_string()],
            vec!["Bob".to_string(), "25".to_string()],
        ];
        let result = render_horizontal_table(Some("People"), &headers, &rows);

        let lines: Vec<&str> = result.lines().collect();
        // Structure: dashes, title, separator, headers, separator, data, data, separator
        assert_eq!(lines.len(), 8);
        assert!(lines[0].chars().all(|c| c == '-'), "top dashes");
        assert!(lines[1].contains("People"), "title line");
        assert!(lines[2].starts_with('+'), "separator");
        assert!(lines[3].contains("Name"), "header Name");
        assert!(lines[3].contains("Age"), "header Age");
        assert!(lines[4].starts_with('+'), "separator after headers");
        assert!(lines[5].contains("Alice"), "first data row");
        assert!(lines[5].contains("30"), "first data row age");
        assert!(lines[6].contains("Bob"), "second data row");
        assert!(lines[7].starts_with('+'), "bottom separator");

        // All lines should have the same width
        let expected_len = lines[0].len();
        for line in &lines {
            assert_eq!(line.len(), expected_len, "all lines same width: '{}'", line);
        }
    }

    #[test]
    fn test_table_format_empty_array() {
        let value = json!([]);
        let result = format_table(&value).unwrap();
        assert_eq!(result, "", "empty array should produce empty output");
    }

    #[test]
    fn test_table_format_bare_scalar_array() {
        let value = json!(["alpha", "beta", "gamma"]);
        let result = format_table(&value).unwrap();
        assert!(result.contains("alpha"));
        assert!(result.contains("beta"));
        assert!(result.contains("gamma"));
    }

    #[test]
    fn test_table_format_bare_object_array() {
        let value = json!([
            {"Id": "1", "Name": "First"},
            {"Id": "2", "Name": "Second"}
        ]);
        let result = format_table(&value).unwrap();
        assert!(result.contains("Id"), "should have Id header");
        assert!(result.contains("Name"), "should have Name header");
        assert!(result.contains("First"));
        assert!(result.contains("Second"));
    }

    #[test]
    fn test_table_format_column_widths_adapt() {
        // Ensure columns are wide enough for the longest value
        let value = json!({
            "Items": [
                {"Short": "a", "Long": "this is a very long value indeed"},
                {"Short": "b", "Long": "x"}
            ]
        });
        let result = format_table(&value).unwrap();

        // The Long column should be wide enough for the longest value
        assert!(result.contains("this is a very long value indeed"));

        // All lines should have the same width
        let lines: Vec<&str> = result.lines().collect();
        let bordered_lines: Vec<&&str> = lines
            .iter()
            .filter(|l| l.starts_with('|') || l.starts_with('+'))
            .collect();
        if bordered_lines.len() > 1 {
            let expected_len = bordered_lines[0].len();
            for line in &bordered_lines {
                assert_eq!(
                    line.len(),
                    expected_len,
                    "all bordered lines should have same length, but '{}' differs",
                    line
                );
            }
        }
    }

    #[test]
    fn test_table_format_boolean_and_number() {
        let value = json!({
            "Enabled": true,
            "Count": 42
        });
        let result = format_table(&value).unwrap();
        assert!(result.contains("True"), "boolean true");
        assert!(result.contains("42"), "number");
        assert!(result.contains("Count"), "key Count");
        assert!(result.contains("Enabled"), "key Enabled");
    }

    #[test]
    fn test_table_format_nested_object_in_list() {
        // Objects in a list that have nested non-scalar values get JSON stringified
        let value = json!({
            "Results": [
                {"Name": "test", "Tags": {"env": "prod"}}
            ]
        });
        let result = format_table(&value).unwrap();
        assert!(result.contains("Name"), "should have Name header");
        assert!(result.contains("Tags"), "should have Tags header");
        assert!(result.contains("test"), "should have test value");
    }

    #[test]
    fn test_table_format_alignment_consistency() {
        // All rows in a column should have the same width
        let value = json!({
            "Data": [
                {"A": "short", "B": "x"},
                {"A": "a bit longer value", "B": "y"}
            ]
        });
        let result = format_table(&value).unwrap();
        let lines: Vec<&str> = result.lines().collect();

        // All lines that start with | or + should have the same length
        let bordered_lines: Vec<&&str> = lines
            .iter()
            .filter(|l| l.starts_with('|') || l.starts_with('+'))
            .collect();

        if bordered_lines.len() > 1 {
            let expected_len = bordered_lines[0].len();
            for line in &bordered_lines {
                assert_eq!(
                    line.len(),
                    expected_len,
                    "all bordered lines should have same length, but '{}' differs",
                    line
                );
            }
        }
    }

    #[test]
    fn test_table_calculate_widths_proportional() {
        // Verify proportional scaling matches AWS CLI behavior
        // STS-like: max_key=7, max_val=62, title="GetCallerIdentity" (17)
        let (total_width, widths) = calculate_widths(&[7, 62], Some("GetCallerIdentity"));
        // unscaled = [11, 66], sum = 77
        // total_from_cols = 79, total_from_title = 19
        // total_width = 79
        assert_eq!(total_width, 79);
        assert_eq!(widths.iter().sum::<usize>(), 79);
        // The first column should be around 11, second around 68
        assert_eq!(widths[0], 11);
        assert_eq!(widths[1], 68);
    }

    #[test]
    fn test_table_calculate_widths_title_wider() {
        // When title is wider than columns, total_width increases
        let (total_width, widths) = calculate_widths(&[3], Some("AVeryLongSectionTitle"));
        // unscaled = [7], sum = 7, total_from_cols = 9
        // title = 21 chars, total_from_title = 23
        // total_width = 23
        assert_eq!(total_width, 23);
        // Single column takes full width
        assert_eq!(widths[0], 23);
    }
}
