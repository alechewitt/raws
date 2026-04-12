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

// ---------------------------------------------------------------------------
// Depth-aware indentation helper
// ---------------------------------------------------------------------------

/// Wrap each line of `content` with `depth` pipe characters on each side.
/// At depth 0 this is a no-op.
fn indent_lines(content: &str, depth: usize) -> String {
    if depth == 0 {
        return content.to_string();
    }
    let prefix = "|".repeat(depth);
    let suffix = "|".repeat(depth);
    content
        .lines()
        .map(|line| format!("{}{}{}", prefix, line, suffix))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Format a top-level object. The AWS CLI renders an operation name title at
/// depth 0 (e.g., "DescribeVpcs") with each top-level key's content at depth 1.
/// Scalar-only objects become a vertical key-value table with the title.
fn format_top_level_object(map: &serde_json::Map<String, Value>, title: Option<&str>) -> String {
    if map.is_empty() && title.is_none() {
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

    // Check if all values are scalar -> vertical table (with title, no depth indentation)
    if non_scalar_keys.is_empty() {
        let pairs: Vec<(String, String)> = scalar_keys
            .iter()
            .map(|k| ((*k).clone(), scalar_to_string(&map[k.as_str()])))
            .collect();
        return render_vertical_table(title, &pairs);
    }

    // When there's a title (operation name), render it at depth 0 and content at depth 1.
    // When there's no title, render content at depth 0 (no wrapping).
    let base_depth: usize = if title.is_some() { 1 } else { 0 };

    // Compute the total width needed for all content
    let total_width = compute_top_level_width(map, title);
    let content_width = if base_depth > 0 {
        total_width.saturating_sub(2)
    } else {
        total_width
    };

    let mut out = String::new();
    let title_len = out.len();

    // Render the operation name title at depth 0
    if let Some(t) = title {
        out.push_str(&dash_line(total_width));
        out.push('\n');
        out.push_str(&title_line(t, total_width));
        out.push('\n');
    }
    let after_title_len = out.len();

    // Render scalar pairs
    if !scalar_keys.is_empty() {
        let scalar_pairs: Vec<(String, String)> = scalar_keys
            .iter()
            .map(|k| ((*k).clone(), scalar_to_string(&map[k.as_str()])))
            .collect();
        let inner = render_vertical_table_constrained(None, &scalar_pairs, Some(content_width));
        out.push_str(&indent_lines(&inner, base_depth));
    }

    // Render each non-scalar section
    for key in &non_scalar_keys {
        let val = &map[key.as_str()];
        let section = match val {
            Value::Array(arr) => {
                if arr.is_empty() {
                    continue;
                }
                render_array_at_depth(key, arr, base_depth, content_width)
            }
            Value::Object(inner) => {
                if inner.is_empty() {
                    continue;
                }
                render_object_at_depth(inner, Some(key), base_depth, content_width)
            }
            _ => continue,
        };
        if !section.is_empty() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&section);
        }
    }

    // If we rendered a title but no content below it, close with a +---+ border
    if title.is_some() && out.len() == after_title_len && title_len < after_title_len {
        out.push_str(&plus_line(total_width));
    }

    out
}

/// Compute the total width for the top-level table including all nested sections.
/// When `title` is Some, content is at depth 1 (+2 for pipe wrapping).
/// When `title` is None, content is at depth 0 (no wrapping).
fn compute_top_level_width(
    map: &serde_json::Map<String, Value>,
    title: Option<&str>,
) -> usize {
    let indent = if title.is_some() { 2 } else { 0 };

    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();

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

    // Width from scalar fields (+ indent for pipe wrapping if titled)
    let scalar_width = if !scalar_keys.is_empty() {
        let max_key_len = scalar_keys.iter().map(|k| k.len()).max().unwrap_or(0);
        let max_val_len = scalar_keys
            .iter()
            .map(|k| scalar_to_string(&map[k.as_str()]).len())
            .max()
            .unwrap_or(0);
        let (w, _) = calculate_widths(&[max_key_len, max_val_len], None);
        w + indent
    } else {
        0
    };

    // Width from non-scalar sections (+ indent for pipe wrapping if titled)
    let section_width = non_scalar_keys
        .iter()
        .map(|k| {
            let val = &map[k.as_str()];
            match val {
                Value::Array(arr) => compute_array_width(k, arr) + indent,
                Value::Object(inner) => compute_object_width(inner, Some(k)) + indent,
                _ => 0,
            }
        })
        .max()
        .unwrap_or(0);

    // Title width
    let title_width = title.map_or(0, |t| t.len() + 2);

    std::cmp::max(title_width, std::cmp::max(scalar_width, section_width))
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
// Width computation (bottom-up pass)
// ---------------------------------------------------------------------------

/// Compute the minimum width needed to render an object at depth 0.
/// This accounts for scalar fields, title, and any nested sub-sections.
/// Children at depth+1 contribute their width + 2 to the parent.
fn compute_object_width(
    map: &serde_json::Map<String, Value>,
    title: Option<&str>,
) -> usize {
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();

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

    // Width from scalar key-value pairs
    let scalar_width = if !scalar_keys.is_empty() {
        let max_key_len = scalar_keys.iter().map(|k| k.len()).max().unwrap_or(0);
        let max_val_len = scalar_keys
            .iter()
            .map(|k| scalar_to_string(&map[k.as_str()]).len())
            .max()
            .unwrap_or(0);
        let (w, _) = calculate_widths(&[max_key_len, max_val_len], title);
        w
    } else {
        title.map_or(0, |t| t.len() + 2)
    };

    // Width from non-scalar children (each adds 2 for the pipe wrapping)
    let child_width = non_scalar_keys
        .iter()
        .map(|k| {
            let val = &map[k.as_str()];
            match val {
                Value::Array(arr) => compute_array_width(k, arr) + 2,
                Value::Object(inner) => compute_object_width(inner, Some(k)) + 2,
                _ => 0,
            }
        })
        .max()
        .unwrap_or(0);

    std::cmp::max(scalar_width, child_width)
}

/// Compute the minimum width needed to render an array section.
fn compute_array_width(title: &str, arr: &[Value]) -> usize {
    if arr.is_empty() {
        return 0;
    }

    let all_scalars = arr.iter().all(is_scalar);
    if all_scalars {
        let max_val = arr.iter().map(|v| scalar_to_string(v).len()).max().unwrap_or(0);
        let (w, _) = calculate_widths(&[max_val], Some(title));
        return w;
    }

    let all_objects = arr.iter().all(|v| v.is_object());
    if all_objects {
        return compute_object_list_width(title, arr);
    }

    // Fallback: stringify
    let max_val = arr.iter().map(|v| value_to_string(v).len()).max().unwrap_or(0);
    let (w, _) = calculate_widths(&[max_val], Some(title));
    w
}

/// Compute the minimum width for a list of objects rendered as a table.
fn compute_object_list_width(title: &str, items: &[Value]) -> usize {
    // Check if any has non-scalar fields
    let has_non_scalar = items.iter().any(|item| {
        if let Value::Object(map) = item {
            map.values().any(|v| !is_scalar(v))
        } else {
            false
        }
    });

    if !has_non_scalar {
        // Pure scalar: horizontal table
        let (headers, rows) = extract_headers_and_rows(items);
        let max_content: Vec<usize> = headers
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let max_data = rows.iter().map(|r| r.get(i).map_or(0, |c| c.len())).max().unwrap_or(0);
                std::cmp::max(h.len(), max_data)
            })
            .collect();
        let (w, _) = calculate_widths(&max_content, Some(title));
        return w;
    }

    // Mixed: scalar columns in horizontal table + non-scalar sub-sections
    let mut all_scalar_headers = std::collections::BTreeSet::new();
    let mut all_non_scalar_keys = std::collections::BTreeSet::new();

    for item in items {
        if let Value::Object(map) = item {
            for (k, v) in map {
                if is_scalar(v) {
                    all_scalar_headers.insert(k.clone());
                } else {
                    all_non_scalar_keys.insert(k.clone());
                }
            }
        }
    }

    let scalar_headers: Vec<String> = all_scalar_headers.into_iter().collect();

    // Width from scalar horizontal table
    let scalar_width = if !scalar_headers.is_empty() {
        let mut max_content: Vec<usize> = scalar_headers.iter().map(|h| h.len()).collect();
        for item in items {
            for (i, h) in scalar_headers.iter().enumerate() {
                let val_len = item.get(h).map(|v| scalar_to_string(v).len()).unwrap_or(0);
                if i < max_content.len() {
                    max_content[i] = std::cmp::max(max_content[i], val_len);
                }
            }
        }
        let (w, _) = calculate_widths(&max_content, Some(title));
        w
    } else {
        title.len() + 2
    };

    // Width from non-scalar children
    let child_width: usize = items
        .iter()
        .filter_map(|item| {
            if let Value::Object(map) = item {
                let w = all_non_scalar_keys
                    .iter()
                    .filter_map(|nsk| {
                        map.get(nsk.as_str()).map(|val| match val {
                            Value::Array(arr) => compute_array_width(nsk, arr) + 2,
                            Value::Object(inner) => compute_object_width(inner, Some(nsk)) + 2,
                            _ => 0,
                        })
                    })
                    .max()
                    .unwrap_or(0);
                Some(w)
            } else {
                None
            }
        })
        .max()
        .unwrap_or(0);

    std::cmp::max(scalar_width, child_width)
}

// ---------------------------------------------------------------------------
// Depth-aware object rendering (recursive, top-down pass)
// ---------------------------------------------------------------------------

/// Render an object at a given nesting depth with a fixed width.
///
/// - Scalar fields become key-value rows in a vertical table
/// - Non-scalar fields become sub-sections rendered at depth+1
/// - `width` is the total width for this depth level's content (before indentation)
///
/// The resulting content is indented by `depth` pipe characters on each side.
fn render_object_at_depth(
    map: &serde_json::Map<String, Value>,
    title: Option<&str>,
    depth: usize,
    width: usize,
) -> String {
    if map.is_empty() {
        return String::new();
    }

    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();

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

    let pairs: Vec<(String, String)> = scalar_keys
        .iter()
        .map(|k| ((*k).clone(), scalar_to_string(&map[k.as_str()])))
        .collect();

    let mut out = String::new();

    // Render scalar pairs as a vertical table
    if !pairs.is_empty() {
        let inner = render_vertical_table_constrained(title, &pairs, Some(width));
        out.push_str(&indent_lines(&inner, depth));
    } else if title.is_some() {
        // No scalar fields but we have a title and sub-sections.
        // Nothing to render at this level for the title-only case.
    }

    // Render non-scalar fields as sub-sections
    let inner_width = width.saturating_sub(2);
    for key in &non_scalar_keys {
        let val = &map[key.as_str()];

        let sub_content = match val {
            Value::Array(arr) => {
                if arr.is_empty() {
                    continue;
                }
                render_array_at_depth(key, arr, depth + 1, inner_width)
            }
            Value::Object(inner_map) => {
                if inner_map.is_empty() {
                    continue;
                }
                render_object_at_depth(inner_map, Some(key), depth + 1, inner_width)
            }
            _ => continue,
        };

        if !sub_content.is_empty() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&sub_content);
        }
    }

    out
}

/// Render an array at a given depth with a title and fixed width.
/// The content is indented by `depth` pipe chars on each side.
fn render_array_at_depth(
    title: &str,
    arr: &[Value],
    depth: usize,
    width: usize,
) -> String {
    if arr.is_empty() {
        return String::new();
    }

    let all_scalars = arr.iter().all(is_scalar);
    if all_scalars {
        let values: Vec<String> = arr.iter().map(scalar_to_string).collect();
        let inner = render_scalar_list_table_constrained(title, &values, Some(width));
        return indent_lines(&inner, depth);
    }

    let all_objects = arr.iter().all(|v| v.is_object());
    if all_objects {
        return render_object_list_at_depth(title, arr, depth, width);
    }

    // Fallback
    let values: Vec<String> = arr.iter().map(value_to_string).collect();
    let inner = render_scalar_list_table_constrained(title, &values, Some(width));
    indent_lines(&inner, depth)
}

/// Render a list of objects at a given depth with a fixed width.
///
/// AWS CLI behavior:
/// - If all objects have only scalar fields → horizontal table (column headers + rows)
/// - If any object has non-scalar fields → each object as vertical key-value table,
///   with non-scalar fields as sub-sections at depth+1
fn render_object_list_at_depth(
    title: &str,
    items: &[Value],
    depth: usize,
    width: usize,
) -> String {
    if items.is_empty() {
        return String::new();
    }

    // Check if any object has non-scalar fields
    let has_non_scalar = items.iter().any(|item| {
        if let Value::Object(map) = item {
            map.values().any(|v| !is_scalar(v))
        } else {
            false
        }
    });

    if !has_non_scalar {
        // Pure scalar objects: render as horizontal table
        let (headers, rows) = extract_headers_and_rows(items);
        let inner = render_horizontal_table_constrained(Some(title), &headers, &rows, Some(width));
        return indent_lines(&inner, depth);
    }

    // Objects with nested fields: render each item as a vertical table
    // with sub-sections for non-scalar fields, matching AWS CLI behavior
    let mut out = String::new();

    for item in items {
        if let Value::Object(map) = item {
            // Render this object at the current depth with the title
            let section = render_object_at_depth(map, Some(title), depth, width);
            if !section.is_empty() {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(&section);
            }
        }
    }

    out
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

/// Render a `+----...----+` closing border line for title-only tables.
fn plus_line(total_width: usize) -> String {
    format!("+{}+", "-".repeat(total_width.saturating_sub(2)))
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
// Constrained-width rendering helpers (for nested sub-tables)
// ---------------------------------------------------------------------------

/// Render a vertical key-value table with an optional title, constrained to a max width.
fn render_vertical_table_constrained(
    title: Option<&str>,
    pairs: &[(String, String)],
    max_width: Option<usize>,
) -> String {
    if pairs.is_empty() {
        return String::new();
    }

    let max_key_len = pairs.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
    let max_val_len = pairs.iter().map(|(_, v)| v.len()).max().unwrap_or(0);

    let (mut total_width, mut widths) = calculate_widths(&[max_key_len, max_val_len], title);

    // If we have a max_width constraint and our natural width is smaller, expand
    if let Some(mw) = max_width {
        if total_width < mw {
            // Recalculate with the wider width (add extra to the value column)
            let extra = mw - total_width;
            total_width = mw;
            if let Some(last) = widths.last_mut() {
                *last += extra;
            }
        }
    }

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

/// Render a horizontal table constrained to a max width.
fn render_horizontal_table_constrained(
    title: Option<&str>,
    headers: &[String],
    rows: &[Vec<String>],
    max_width: Option<usize>,
) -> String {
    let col_count = headers.len();

    let mut max_content: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_count {
                max_content[i] = std::cmp::max(max_content[i], cell.len());
            }
        }
    }

    let (mut total_width, mut widths) = calculate_widths(&max_content, title);

    if let Some(mw) = max_width {
        if total_width < mw {
            let extra = mw - total_width;
            total_width = mw;
            if let Some(last) = widths.last_mut() {
                *last += extra;
            }
        }
    }

    let mut out = String::new();

    if let Some(t) = title {
        out.push_str(&dash_line(total_width));
        out.push('\n');
        out.push_str(&title_line(t, total_width));
        out.push('\n');
    }

    out.push_str(&line_break(&widths));
    out.push('\n');

    out.push_str(&header_row(&widths, headers));
    out.push('\n');

    out.push_str(&line_break(&widths));
    out.push('\n');

    for row in rows {
        let cells: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
        out.push_str(&data_row(&widths, &cells));
        out.push('\n');
    }

    out.push_str(&line_break(&widths));

    out
}

/// Render a single-column scalar list table constrained to a max width.
fn render_scalar_list_table_constrained(
    title: &str,
    values: &[String],
    max_width: Option<usize>,
) -> String {
    if values.is_empty() {
        return String::new();
    }

    let max_val = values.iter().map(|v| v.len()).max().unwrap_or(0);
    let (mut total_width, mut widths) = calculate_widths(&[max_val], Some(title));

    if let Some(mw) = max_width {
        if total_width < mw {
            let extra = mw - total_width;
            total_width = mw;
            if let Some(last) = widths.last_mut() {
                *last += extra;
            }
        }
    }

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

    // -----------------------------------------------------------------------
    // Nested sub-table rendering tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_indent_lines_depth_0() {
        let content = "+---+\n| a |\n+---+";
        let result = indent_lines(content, 0);
        assert_eq!(result, content, "depth 0 should be identity");
    }

    #[test]
    fn test_indent_lines_depth_1() {
        let content = "+---+\n| a |\n+---+";
        let result = indent_lines(content, 1);
        let lines: Vec<&str> = result.lines().collect();
        assert_eq!(lines[0], "|+---+|");
        assert_eq!(lines[1], "|| a ||");
        assert_eq!(lines[2], "|+---+|");
    }

    #[test]
    fn test_indent_lines_depth_2() {
        let content = "+--+\n|ab|\n+--+";
        let result = indent_lines(content, 2);
        let lines: Vec<&str> = result.lines().collect();
        assert_eq!(lines[0], "||+--+||");
        assert_eq!(lines[1], "|||ab|||");
        assert_eq!(lines[2], "||+--+||");
    }

    #[test]
    fn test_nested_object_renders_sub_table() {
        // Object with a nested object field
        let value = json!({
            "Name": "test-vpc",
            "Config": {
                "Mode": "enabled",
                "Type": "standard"
            }
        });
        let result = format_table(&value).unwrap();

        // Scalar field should be in the output
        assert!(result.contains("Name"), "should contain scalar key Name");
        assert!(result.contains("test-vpc"), "should contain scalar value");

        // Nested object should be rendered as sub-table, not JSON
        assert!(
            !result.contains('{'),
            "should not contain JSON braces - nested object should be a sub-table"
        );
        assert!(result.contains("Config"), "should contain sub-section title Config");
        assert!(result.contains("Mode"), "should contain nested key Mode");
        assert!(result.contains("enabled"), "should contain nested value enabled");
        assert!(result.contains("Type"), "should contain nested key Type");
        assert!(result.contains("standard"), "should contain nested value standard");
    }

    #[test]
    fn test_nested_object_in_list_renders_sub_table() {
        // List of objects where each object has non-scalar fields
        // (like ec2 describe-vpcs)
        let value = json!({
            "Vpcs": [
                {
                    "CidrBlock": "172.31.0.0/16",
                    "State": "available",
                    "BlockPublicAccessStates": {
                        "InternetGatewayBlockMode": "off"
                    }
                }
            ]
        });
        let result = format_table(&value).unwrap();

        // Scalar columns should appear
        assert!(result.contains("CidrBlock"), "should contain CidrBlock");
        assert!(result.contains("172.31.0.0/16"), "should contain CIDR value");
        assert!(result.contains("State"), "should contain State");
        assert!(result.contains("available"), "should contain state value");

        // Non-scalar field should render as a sub-section, not JSON
        assert!(
            !result.contains('{'),
            "should not contain JSON braces - nested objects should be sub-tables"
        );
        assert!(
            result.contains("BlockPublicAccessStates"),
            "should contain nested section title"
        );
        assert!(
            result.contains("InternetGatewayBlockMode"),
            "should contain nested key"
        );
        assert!(result.contains("off"), "should contain nested value");
    }

    #[test]
    fn test_nested_depth_1_has_double_pipes() {
        // Verify that depth-1 sub-tables have || borders
        let value = json!({
            "Vpcs": [
                {
                    "CidrBlock": "172.31.0.0/16",
                    "BlockPublicAccessStates": {
                        "InternetGatewayBlockMode": "off"
                    }
                }
            ]
        });
        let result = format_table(&value).unwrap();

        // Find lines that contain "InternetGatewayBlockMode" - they should have || borders
        let nested_lines: Vec<&str> = result
            .lines()
            .filter(|l| l.contains("InternetGatewayBlockMode"))
            .collect();
        assert!(
            !nested_lines.is_empty(),
            "should have lines with InternetGatewayBlockMode"
        );

        for line in &nested_lines {
            // At depth 2 (nested within Vpcs at depth 1), should have || on each side
            // The line should start with || and end with ||
            assert!(
                line.starts_with("||"),
                "nested data line should start with || (depth 2): '{}'",
                line
            );
            assert!(
                line.ends_with("||"),
                "nested data line should end with || (depth 2): '{}'",
                line
            );
        }
    }

    #[test]
    fn test_nested_array_of_scalars_at_depth() {
        // Object with a nested array of scalars
        let value = json!({
            "Name": "test",
            "Tags": ["tag1", "tag2", "tag3"]
        });
        let result = format_table(&value).unwrap();

        assert!(result.contains("Name"), "should contain Name key");
        assert!(result.contains("test"), "should contain Name value");
        assert!(result.contains("Tags"), "should contain Tags title");
        assert!(result.contains("tag1"), "should contain tag1");
        assert!(result.contains("tag2"), "should contain tag2");
        assert!(result.contains("tag3"), "should contain tag3");
    }

    #[test]
    fn test_nested_multiple_sub_sections() {
        // Object with multiple non-scalar fields
        let value = json!({
            "Id": "vpc-123",
            "CidrBlockAssociations": [
                {"AssociationId": "assoc-1", "CidrBlock": "172.31.0.0/16"}
            ],
            "Tags": [
                {"Key": "Name", "Value": "default"}
            ]
        });
        let result = format_table(&value).unwrap();

        assert!(result.contains("Id"), "should contain Id");
        assert!(result.contains("vpc-123"), "should contain Id value");
        assert!(result.contains("CidrBlockAssociations"), "should contain first sub-section");
        assert!(result.contains("AssociationId"), "should contain AssociationId");
        assert!(result.contains("assoc-1"), "should contain association value");
        assert!(result.contains("Tags"), "should contain Tags sub-section");
        assert!(result.contains("Name"), "should contain tag key");
        assert!(result.contains("default"), "should contain tag value");
    }

    #[test]
    fn test_nested_all_lines_consistent_width() {
        // Each depth level should have consistent line widths
        let value = json!({
            "Vpcs": [
                {
                    "CidrBlock": "172.31.0.0/16",
                    "State": "available",
                    "BlockPublicAccessStates": {
                        "InternetGatewayBlockMode": "off"
                    }
                }
            ]
        });
        let result = format_table(&value).unwrap();

        // Group lines by their depth (number of leading | chars before first non-| char)
        let mut depth_widths: std::collections::HashMap<usize, Vec<(usize, String)>> =
            std::collections::HashMap::new();
        for (i, line) in result.lines().enumerate() {
            if line.is_empty() {
                continue;
            }
            let depth = line
                .chars()
                .take_while(|c| *c == '|')
                .count()
                .min(
                    line.chars()
                        .rev()
                        .take_while(|c| *c == '|')
                        .count(),
                );
            depth_widths
                .entry(depth)
                .or_default()
                .push((i, line.to_string()));
        }

        // At each depth, all lines should have the same width
        for (depth, lines) in &depth_widths {
            if lines.len() > 1 {
                let expected_len = lines[0].1.len();
                for (line_num, line) in lines {
                    assert_eq!(
                        line.len(),
                        expected_len,
                        "at depth {}, line {} should have width {} but has {}: '{}'",
                        depth,
                        line_num,
                        expected_len,
                        line.len(),
                        line
                    );
                }
            }
        }
    }

    #[test]
    fn test_nested_object_field_in_top_level_object() {
        // Top-level object with a direct nested object (not in an array)
        let value = json!({
            "Status": "active",
            "Metadata": {
                "CreatedBy": "admin",
                "Version": "1.0"
            }
        });
        let result = format_table(&value).unwrap();

        // Should not contain JSON
        assert!(!result.contains('{'), "should not contain JSON braces");

        // Should have Metadata as a sub-section title
        assert!(result.contains("Metadata"), "should contain Metadata section");
        assert!(result.contains("CreatedBy"), "should contain CreatedBy");
        assert!(result.contains("admin"), "should contain admin value");
        assert!(result.contains("Version"), "should contain Version");
        assert!(result.contains("1.0"), "should contain version value");
    }

    #[test]
    fn test_deeply_nested_object() {
        // Test 3 levels of nesting
        let value = json!({
            "Level1Key": "level1val",
            "Nested": {
                "Level2Key": "level2val",
                "DeepNested": {
                    "Level3Key": "level3val"
                }
            }
        });
        let result = format_table(&value).unwrap();

        assert!(result.contains("Level1Key"), "should contain level 1 key");
        assert!(result.contains("level1val"), "should contain level 1 value");
        assert!(result.contains("Level2Key"), "should contain level 2 key");
        assert!(result.contains("level2val"), "should contain level 2 value");
        assert!(result.contains("Level3Key"), "should contain level 3 key");
        assert!(result.contains("level3val"), "should contain level 3 value");
        assert!(!result.contains('{'), "no JSON braces in output");
    }
}
