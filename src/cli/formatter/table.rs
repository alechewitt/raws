use anyhow::Result;
use serde_json::Value;

pub fn format_table(value: &Value) -> Result<String> {
    let output = match value {
        Value::Null => String::new(),
        Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            // Single scalar: render as a one-cell table
            let s = scalar_to_string(value);
            let width = s.len() + 4; // 2-space padding each side
            let mut out = String::new();
            out.push_str(&line_break(&[width]));
            out.push('\n');
            out.push_str(&data_row(&[width], &[&s]));
            out.push('\n');
            out.push_str(&line_break(&[width]));
            out
        }
        Value::Array(arr) => format_top_level_array(arr),
        Value::Object(map) => format_top_level_object(map),
    };
    Ok(output)
}

/// Format a top-level object. The AWS CLI renders each top-level key as a
/// separate section. Scalar-only objects become a vertical key-value table.
fn format_top_level_object(map: &serde_json::Map<String, Value>) -> String {
    if map.is_empty() {
        return String::new();
    }

    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();

    // Check if all values are scalar -> vertical table
    let all_scalar = keys.iter().all(|k| is_scalar(&map[k.as_str()]));
    if all_scalar {
        let pairs: Vec<(String, String)> = keys
            .iter()
            .map(|k| ((*k).clone(), scalar_to_string(&map[k.as_str()])))
            .collect();
        return render_vertical_table(None, &pairs);
    }

    // Mixed: render scalar pairs as vertical table, then each non-scalar key
    // as its own section
    let mut sections: Vec<String> = Vec::new();

    let scalar_pairs: Vec<(String, String)> = keys
        .iter()
        .filter(|k| is_scalar(&map[k.as_str()]))
        .map(|k| ((*k).clone(), scalar_to_string(&map[k.as_str()])))
        .collect();

    if !scalar_pairs.is_empty() {
        sections.push(render_vertical_table(None, &scalar_pairs));
    }

    for key in &keys {
        let val = &map[key.as_str()];
        if is_scalar(val) {
            continue;
        }
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
// Rendering helpers
// ---------------------------------------------------------------------------

/// Render a horizontal table with a title, column headers, and rows.
///
/// ```text
/// ---------------------------
/// |        Title            |
/// +-----------+-------------+
/// |  Header1  |  Header2    |
/// +-----------+-------------+
/// |  val1     |  val2       |
/// +-----------+-------------+
/// ```
fn render_horizontal_table(
    title: Option<&str>,
    headers: &[String],
    rows: &[Vec<String>],
) -> String {
    let col_count = headers.len();
    // Compute column widths: max of header and all row values, plus 4 for padding
    let mut widths: Vec<usize> = headers
        .iter()
        .map(|h| h.len() + 4) // 2-space padding each side
        .collect();

    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_count {
                let needed = cell.len() + 4;
                if needed > widths[i] {
                    widths[i] = needed;
                }
            }
        }
    }

    let mut out = String::new();

    // Title section
    if let Some(t) = title {
        let total_inner = total_inner_width(&widths);
        let top_width = total_inner;
        out.push_str(&dash_line(top_width));
        out.push('\n');
        out.push_str(&title_line(t, top_width));
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

/// Render a vertical key-value table with an optional title.
///
/// ```text
/// --------------------
/// |      Title       |
/// +--------+---------+
/// |  Key1  |  Val1   |
/// |  Key2  |  Val2   |
/// +--------+---------+
/// ```
fn render_vertical_table(title: Option<&str>, pairs: &[(String, String)]) -> String {
    if pairs.is_empty() {
        return String::new();
    }

    let key_width = pairs
        .iter()
        .map(|(k, _)| k.len() + 4)
        .max()
        .unwrap_or(4);
    let val_width = pairs
        .iter()
        .map(|(_, v)| v.len() + 4)
        .max()
        .unwrap_or(4);

    let widths = [key_width, val_width];

    let mut out = String::new();

    if let Some(t) = title {
        let total_inner = total_inner_width(&widths);
        out.push_str(&dash_line(total_inner));
        out.push('\n');
        out.push_str(&title_line(t, total_inner));
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

/// Render a single-column table for a list of scalars, with title.
///
/// ```text
/// --------------------
/// |     Title        |
/// +------------------+
/// |  value1          |
/// |  value2          |
/// +------------------+
/// ```
fn render_scalar_list_table(title: &str, values: &[String]) -> String {
    if values.is_empty() {
        return String::new();
    }

    let max_val = values.iter().map(|v| v.len()).max().unwrap_or(0);
    let title_needed = title.len() + 4;
    let col_width = std::cmp::max(max_val + 4, title_needed);
    let widths = [col_width];

    let mut out = String::new();
    let total_inner = total_inner_width(&widths);

    out.push_str(&dash_line(total_inner));
    out.push('\n');
    out.push_str(&title_line(title, total_inner));
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
    let col_width = max_val + 4;
    let widths = [col_width];

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
    // Gather all unique keys across items, sorted
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
// Low-level line drawing
// ---------------------------------------------------------------------------

/// Calculate the total inner width for a set of column widths.
/// This is the sum of all column widths plus the separators between them.
/// For N columns with widths [w1, w2, ...], the total is w1 + 1 + w2 + 1 + ...
/// (each `|` separator between columns counts as 1).
fn total_inner_width(widths: &[usize]) -> usize {
    if widths.is_empty() {
        return 0;
    }
    let sum: usize = widths.iter().sum();
    // Between N columns there are N-1 separators of width 1 (the `+` or `|`)
    sum + widths.len() - 1
}

/// Render a full-width dash line: `----...----`
fn dash_line(inner_width: usize) -> String {
    // The outer border adds 2 characters (one `|` each side) but for the top
    // dashes we use `-` for the full width including borders.
    let total = inner_width + 2;
    "-".repeat(total)
}

/// Render a `+---+---+` separator line.
fn line_break(widths: &[usize]) -> String {
    let mut out = String::new();
    for w in widths {
        out.push('+');
        out.push_str(&"-".repeat(*w));
    }
    out.push('+');
    out
}

/// Render a title line: `|  Title  |` centered within total_inner_width.
fn title_line(title: &str, inner_width: usize) -> String {
    let mut out = String::new();
    out.push('|');
    let content = center_text(title, inner_width);
    out.push_str(&content);
    out.push('|');
    out
}

/// Render a data row with 2-space left padding in each cell.
fn data_row(widths: &[usize], cells: &[&str]) -> String {
    let mut out = String::new();
    for (i, w) in widths.iter().enumerate() {
        out.push('|');
        let cell = if i < cells.len() { cells[i] } else { "" };
        // 2-space left padding, then content, then right-pad to fill width
        let padded = format!("  {}", cell);
        let needed = if *w > padded.len() { w - padded.len() } else { 0 };
        out.push_str(&padded);
        out.push_str(&" ".repeat(needed));
    }
    out.push('|');
    out
}

/// Render a header row with centered text in each cell.
fn header_row(widths: &[usize], headers: &[String]) -> String {
    let mut out = String::new();
    for (i, w) in widths.iter().enumerate() {
        out.push('|');
        let header = if i < headers.len() {
            headers[i].as_str()
        } else {
            ""
        };
        out.push_str(&center_text(header, *w));
    }
    out.push('|');
    out
}

/// Center text within a given width. If the text is wider than the width,
/// it is returned with minimal padding.
fn center_text(text: &str, width: usize) -> String {
    if text.len() >= width {
        return format!(" {} ", text);
    }
    let total_pad = width - text.len();
    let left = total_pad / 2;
    let right = total_pad - left;
    format!("{}{}{}", " ".repeat(left), text, " ".repeat(right))
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

        // Should be a vertical key-value table (keys sorted alphabetically)
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
        assert_eq!(line_break(&[5, 8]), "+-----+--------+");
        assert_eq!(line_break(&[3]), "+---+");
    }

    #[test]
    fn test_table_format_data_row() {
        let row = data_row(&[10, 12], &["foo", "bar"]);
        // width 10: "  foo" (5 chars) + 5 spaces = 10
        // width 12: "  bar" (5 chars) + 7 spaces = 12
        assert_eq!(row, "|  foo     |  bar       |");
    }

    #[test]
    fn test_table_format_center_text() {
        let centered = center_text("Hi", 10);
        assert_eq!(centered.len(), 10);
        assert!(centered.contains("Hi"));
        // "Hi" is 2 chars, 10 - 2 = 8 padding total, 4 left, 4 right
        assert_eq!(centered, "    Hi    ");
    }

    #[test]
    fn test_table_format_title_line() {
        let t = title_line("MyTitle", 20);
        assert!(t.starts_with('|'));
        assert!(t.ends_with('|'));
        assert!(t.contains("MyTitle"));
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

        // Find a separator line to check column widths
        let lines: Vec<&str> = result.lines().collect();
        let sep_line = lines.iter().find(|l| l.starts_with('+')).unwrap();
        // The Long column should be wide enough for the longest value
        assert!(
            sep_line.len() > 30,
            "separator should be wide enough for long values"
        );
        assert!(result.contains("this is a very long value indeed"));
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
}
