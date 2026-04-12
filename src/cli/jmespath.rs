//! Minimal JMESPath evaluator for the --query flag.
//!
//! Supports the most common subset used with the AWS CLI:
//! - Field access: `Account`
//! - Nested field access: `a.b.c`
//! - Array index: `[0]`, `[-1]`
//! - Flatten projection: `[]`
//! - List wildcard: `[*]` (projects without flattening)
//! - Multi-select list: `[Account, Arn]`
//! - Wildcard: `*`
//! - Filter: `[?Key=='Name']` (equality and inequality)
//! - Pipe: `|`
//! - Literal: backtick-delimited JSON values, single-quoted strings

use anyhow::{bail, Result};
use serde_json::Value;

/// Evaluate a JMESPath expression against a JSON value.
pub fn evaluate(expression: &str, value: &Value) -> Result<Value> {
    let expr = expression.trim();
    if expr.is_empty() {
        return Ok(value.clone());
    }
    let tokens = tokenize(expr)?;
    eval_tokens(&tokens, value)
}

// ---------------------------------------------------------------------------
// Token types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    /// A bare identifier: `Account`
    Field(String),
    /// A double-quoted identifier: `"foo bar"`
    QuotedField(String),
    /// Dot separator
    Dot,
    /// `[<index>]` array index access
    Index(i64),
    /// `[]` flatten / array projection (flattens nested arrays)
    Flatten,
    /// `[*]` list wildcard projection (projects without flattening)
    ListWildcard,
    /// `[?lhs==rhs]` or `[?lhs!=rhs]` filter expression
    Filter(FilterExpr),
    /// `[expr1, expr2, ...]` multi-select list
    MultiSelect(Vec<Vec<Token>>),
    /// `*` wildcard (object values projection)
    Wildcard,
    /// `|` pipe
    Pipe,
    /// A literal value (backtick or single-quoted string)
    Literal(Value),
}

#[derive(Debug, Clone, PartialEq)]
struct FilterExpr {
    lhs: Vec<Token>,
    op: FilterOp,
    rhs: Vec<Token>,
}

#[derive(Debug, Clone, PartialEq)]
enum FilterOp {
    Eq,
    NotEq,
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

fn tokenize(input: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' | '\r' | '\n' => {
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            '|' => {
                tokens.push(Token::Pipe);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Wildcard);
                i += 1;
            }
            '"' => {
                // Double-quoted identifier
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '"' {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        i += 1;
                    }
                    i += 1;
                }
                let field: String = chars[start..i].iter().collect();
                if i < chars.len() {
                    i += 1; // skip closing "
                }
                tokens.push(Token::QuotedField(field));
            }
            '\'' => {
                // Single-quoted string literal (used in filters)
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '\'' {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        i += 1;
                    }
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                if i < chars.len() {
                    i += 1; // skip closing '
                }
                tokens.push(Token::Literal(Value::String(s)));
            }
            '`' => {
                // Backtick-delimited literal value
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '`' {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        i += 1;
                    }
                    i += 1;
                }
                let lit_str: String = chars[start..i].iter().collect();
                if i < chars.len() {
                    i += 1; // skip closing `
                }
                let lit_value = serde_json::from_str(&lit_str)
                    .unwrap_or(Value::String(lit_str));
                tokens.push(Token::Literal(lit_value));
            }
            '[' => {
                i += 1; // skip '['

                if i < chars.len() && chars[i] == ']' {
                    // Flatten: []
                    tokens.push(Token::Flatten);
                    i += 1;
                } else if i < chars.len() && chars[i] == '?' {
                    // Filter: [?expr]
                    i += 1; // skip '?'
                    let (filter, new_i) = parse_filter(&chars, i)?;
                    tokens.push(Token::Filter(filter));
                    i = new_i;
                } else if i + 1 < chars.len() && chars[i] == '*' && chars[i + 1] == ']' {
                    // List wildcard: [*] — projects over elements without flattening
                    tokens.push(Token::ListWildcard);
                    i += 2; // skip '*]'
                } else if i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '-') {
                    // Array index: [0], [-1]
                    let start = i;
                    if chars[i] == '-' {
                        i += 1;
                    }
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    let num_str: String = chars[start..i].iter().collect();
                    let idx: i64 = num_str
                        .parse()
                        .map_err(|_| anyhow::anyhow!("Invalid array index: {}", num_str))?;
                    if i < chars.len() && chars[i] == ']' {
                        i += 1;
                    }
                    tokens.push(Token::Index(idx));
                } else {
                    // Multi-select list: [expr1, expr2, ...]
                    let (multi, new_i) = parse_multi_select(&chars, i)?;
                    tokens.push(Token::MultiSelect(multi));
                    i = new_i;
                }
            }
            c if c.is_alphanumeric() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let field: String = chars[start..i].iter().collect();
                tokens.push(Token::Field(field));
            }
            other => {
                bail!(
                    "Unexpected character '{}' in JMESPath expression at position {}",
                    other,
                    i
                );
            }
        }
    }

    Ok(tokens)
}

/// Parse a filter expression starting after `[?`. Finds the matching `]`,
/// splits on `==` or `!=` (respecting quotes), and tokenizes both sides.
fn parse_filter(chars: &[char], start: usize) -> Result<(FilterExpr, usize)> {
    let mut i = start;
    let mut depth = 1;

    while i < chars.len() && depth > 0 {
        match chars[i] {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            '\'' => {
                i += 1;
                while i < chars.len() && chars[i] != '\'' {
                    i += 1;
                }
            }
            '`' => {
                i += 1;
                while i < chars.len() && chars[i] != '`' {
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    let inner: String = chars[start..i].iter().collect();
    let close_i = if i < chars.len() && chars[i] == ']' {
        i + 1
    } else {
        i
    };

    // Find the operator (== or !=), skipping quoted regions
    let (lhs_str, op, rhs_str) = find_filter_operator(&inner)?;

    let lhs = tokenize(lhs_str.trim())?;
    let rhs = tokenize(rhs_str.trim())?;

    Ok((FilterExpr { lhs, op, rhs }, close_i))
}

/// Find `==` or `!=` in a filter body, skipping quoted regions.
fn find_filter_operator(inner: &str) -> Result<(&str, FilterOp, &str)> {
    let chars: Vec<char> = inner.chars().collect();
    let mut i = 0;
    let mut byte_offset = 0;

    while i < chars.len() {
        let ch = chars[i];
        match ch {
            '\'' | '`' => {
                byte_offset += ch.len_utf8();
                i += 1;
                while i < chars.len() && chars[i] != ch {
                    byte_offset += chars[i].len_utf8();
                    i += 1;
                }
                if i < chars.len() {
                    byte_offset += chars[i].len_utf8();
                    i += 1;
                }
            }
            '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                return Ok((&inner[..byte_offset], FilterOp::NotEq, &inner[byte_offset + 2..]));
            }
            '=' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                return Ok((&inner[..byte_offset], FilterOp::Eq, &inner[byte_offset + 2..]));
            }
            _ => {
                byte_offset += ch.len_utf8();
                i += 1;
            }
        }
    }

    bail!("Filter expression must contain == or !=: [?{}]", inner)
}

/// Parse a multi-select list starting after `[`. Finds the matching `]`,
/// splits on commas at depth 1, and tokenizes each sub-expression.
fn parse_multi_select(chars: &[char], start: usize) -> Result<(Vec<Vec<Token>>, usize)> {
    let mut i = start;
    let mut depth = 1;
    let mut mark = start;
    let mut expressions: Vec<Vec<Token>> = Vec::new();

    while i < chars.len() && depth > 0 {
        match chars[i] {
            '[' => {
                depth += 1;
                i += 1;
            }
            ']' => {
                depth -= 1;
                if depth == 0 {
                    let expr_str: String = chars[mark..i].iter().collect();
                    let trimmed = expr_str.trim();
                    if !trimmed.is_empty() {
                        expressions.push(tokenize(trimmed)?);
                    }
                    i += 1; // skip closing ']'
                    return Ok((expressions, i));
                }
                i += 1;
            }
            ',' if depth == 1 => {
                let expr_str: String = chars[mark..i].iter().collect();
                let trimmed = expr_str.trim();
                if !trimmed.is_empty() {
                    expressions.push(tokenize(trimmed)?);
                }
                i += 1;
                mark = i;
            }
            '\'' | '`' => {
                let quote = chars[i];
                i += 1;
                while i < chars.len() && chars[i] != quote {
                    i += 1;
                }
                if i < chars.len() {
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    // Unterminated bracket -- collect whatever we have
    let expr_str: String = chars[mark..i].iter().collect();
    let trimmed = expr_str.trim();
    if !trimmed.is_empty() {
        expressions.push(tokenize(trimmed)?);
    }
    Ok((expressions, i))
}

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

fn eval_tokens(tokens: &[Token], value: &Value) -> Result<Value> {
    if tokens.is_empty() {
        return Ok(value.clone());
    }

    // Split on Pipe (lowest precedence): evaluate left side, then feed result to right side
    if let Some(pipe_pos) = tokens.iter().position(|t| matches!(t, Token::Pipe)) {
        let left = &tokens[..pipe_pos];
        let right = &tokens[pipe_pos + 1..];
        let intermediate = eval_tokens(left, value)?;
        return eval_tokens(right, &intermediate);
    }

    let mut current = value.clone();
    let mut i = 0;

    while i < tokens.len() {
        if matches!(tokens[i], Token::Dot) {
            i += 1;
            continue;
        }

        match &tokens[i] {
            Token::Field(name) | Token::QuotedField(name) => {
                current = field_access(&current, name);
                i += 1;
            }
            Token::Index(idx) => {
                current = index_access(&current, *idx);
                i += 1;
            }
            Token::Flatten => {
                let flattened = flatten_value(&current);
                i += 1;
                // Skip optional dot after flatten
                if matches!(tokens.get(i), Some(Token::Dot)) {
                    i += 1;
                }
                let rest = &tokens[i..];
                if !rest.is_empty() {
                    return project_array_flatten(&flattened, rest);
                }
                current = flattened;
            }
            Token::ListWildcard => {
                // [*] projects over array elements without flattening
                i += 1;
                if matches!(tokens.get(i), Some(Token::Dot)) {
                    i += 1;
                }
                let rest = &tokens[i..];
                if !rest.is_empty() {
                    return project_array(&current, rest);
                }
                // Identity: just return the array as-is
            }
            Token::Wildcard => {
                let values = wildcard_values(&current);
                i += 1;
                if matches!(tokens.get(i), Some(Token::Dot)) {
                    i += 1;
                }
                let rest = &tokens[i..];
                if !rest.is_empty() {
                    return project_array(&values, rest);
                }
                current = values;
            }
            Token::Filter(filter_expr) => {
                current = apply_filter(&current, filter_expr)?;
                i += 1;
                if matches!(tokens.get(i), Some(Token::Dot)) {
                    i += 1;
                }
                let rest = &tokens[i..];
                if !rest.is_empty() {
                    return project_array(&current, rest);
                }
            }
            Token::MultiSelect(selections) => {
                let mut results = Vec::new();
                for sel_tokens in selections {
                    results.push(eval_tokens(sel_tokens, &current)?);
                }
                current = Value::Array(results);
                i += 1;
            }
            Token::Literal(val) => {
                current = val.clone();
                i += 1;
            }
            Token::Pipe | Token::Dot => {
                i += 1;
            }
        }
    }

    Ok(current)
}

fn field_access(value: &Value, field: &str) -> Value {
    match value {
        Value::Object(map) => map.get(field).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn index_access(value: &Value, idx: i64) -> Value {
    match value {
        Value::Array(arr) => {
            let actual = if idx < 0 {
                let len = arr.len() as i64;
                (len + idx) as usize
            } else {
                idx as usize
            };
            arr.get(actual).cloned().unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

fn flatten_value(value: &Value) -> Value {
    match value {
        Value::Array(arr) => {
            let mut result = Vec::new();
            for item in arr {
                match item {
                    Value::Array(inner) => result.extend(inner.clone()),
                    other => result.push(other.clone()),
                }
            }
            Value::Array(result)
        }
        _ => Value::Null,
    }
}

fn wildcard_values(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Array(map.values().cloned().collect()),
        _ => Value::Null,
    }
}

fn project_array(value: &Value, remaining: &[Token]) -> Result<Value> {
    match value {
        Value::Array(arr) => {
            let mut results = Vec::new();
            for item in arr {
                let result = eval_tokens(remaining, item)?;
                if !result.is_null() {
                    results.push(result);
                }
            }
            Ok(Value::Array(results))
        }
        _ => Ok(Value::Null),
    }
}

/// Like project_array, but flattens array results into the output.
/// Used by the Flatten (`[]`) projection so that nested projections
/// produce a flat list rather than nested arrays.
fn project_array_flatten(value: &Value, remaining: &[Token]) -> Result<Value> {
    match value {
        Value::Array(arr) => {
            let mut results = Vec::new();
            for item in arr {
                let result = eval_tokens(remaining, item)?;
                match result {
                    Value::Null => {}
                    Value::Array(inner) => results.extend(inner),
                    other => results.push(other),
                }
            }
            Ok(Value::Array(results))
        }
        _ => Ok(Value::Null),
    }
}

fn apply_filter(value: &Value, filter: &FilterExpr) -> Result<Value> {
    match value {
        Value::Array(arr) => {
            let mut results = Vec::new();
            for item in arr {
                let lhs_val = eval_tokens(&filter.lhs, item)?;
                let rhs_val = eval_tokens(&filter.rhs, item)?;
                let matched = match filter.op {
                    FilterOp::Eq => values_equal(&lhs_val, &rhs_val),
                    FilterOp::NotEq => !values_equal(&lhs_val, &rhs_val),
                };
                if matched {
                    results.push(item.clone());
                }
            }
            Ok(Value::Array(results))
        }
        _ => Ok(Value::Null),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    a == b
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_jmespath_simple_field_access() {
        let data = json!({"Account": "123456789012", "Arn": "arn:aws:iam::123:root"});
        let result = evaluate("Account", &data).unwrap();
        assert_eq!(result, json!("123456789012"));
    }

    #[test]
    fn test_jmespath_nested_field_access() {
        let data = json!({"a": {"b": {"c": "deep_value"}}});
        let result = evaluate("a.b.c", &data).unwrap();
        assert_eq!(result, json!("deep_value"));
    }

    #[test]
    fn test_jmespath_array_index() {
        let data = json!({"items": ["first", "second", "third"]});
        let result = evaluate("items[0]", &data).unwrap();
        assert_eq!(result, json!("first"));
    }

    #[test]
    fn test_jmespath_array_index_negative() {
        let data = json!({"items": ["first", "second", "third"]});
        let result = evaluate("items[-1]", &data).unwrap();
        assert_eq!(result, json!("third"));
    }

    #[test]
    fn test_jmespath_array_flatten_with_field() {
        let data = json!({"items": [{"name": "a"}, {"name": "b"}, {"name": "c"}]});
        let result = evaluate("items[].name", &data).unwrap();
        assert_eq!(result, json!(["a", "b", "c"]));
    }

    #[test]
    fn test_jmespath_multi_select_list() {
        let data = json!({"Account": "123", "Arn": "arn:aws:iam::123:root", "UserId": "AIDA"});
        let result = evaluate("[Account, Arn]", &data).unwrap();
        assert_eq!(result, json!(["123", "arn:aws:iam::123:root"]));
    }

    #[test]
    fn test_jmespath_identity_empty_expression() {
        let data = json!({"Account": "123"});
        let result = evaluate("", &data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_jmespath_nonexistent_field_returns_null() {
        let data = json!({"Account": "123"});
        let result = evaluate("NonExistent", &data).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_jmespath_nested_nonexistent() {
        let data = json!({"a": {"b": 1}});
        let result = evaluate("a.x.y", &data).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_jmespath_wildcard() {
        let data = json!({"a": 1, "b": 2, "c": 3});
        let result = evaluate("*", &data).unwrap();
        assert!(result.is_array());
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert!(arr.contains(&json!(1)));
        assert!(arr.contains(&json!(2)));
        assert!(arr.contains(&json!(3)));
    }

    #[test]
    fn test_jmespath_filter_equality() {
        let data = json!({"tags": [
            {"Key": "Name", "Value": "my-instance"},
            {"Key": "Env", "Value": "prod"},
            {"Key": "Team", "Value": "backend"}
        ]});
        let result = evaluate("tags[?Key=='Name']", &data).unwrap();
        assert_eq!(result, json!([{"Key": "Name", "Value": "my-instance"}]));
    }

    #[test]
    fn test_jmespath_filter_not_equal() {
        let data = json!({"items": [
            {"status": "active"},
            {"status": "inactive"},
            {"status": "active"}
        ]});
        let result = evaluate("items[?status!='active']", &data).unwrap();
        assert_eq!(result, json!([{"status": "inactive"}]));
    }

    #[test]
    fn test_jmespath_pipe() {
        let data = json!({"items": [
            {"name": "a", "val": 1},
            {"name": "b", "val": 2}
        ]});
        let result = evaluate("items[0] | name", &data).unwrap();
        assert_eq!(result, json!("a"));
    }

    #[test]
    fn test_jmespath_literal_string() {
        let data = json!({"x": 1});
        let result = evaluate("`\"hello\"`", &data).unwrap();
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn test_jmespath_literal_number() {
        let data = json!({"x": 1});
        let result = evaluate("`42`", &data).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn test_jmespath_complex_ec2_pattern() {
        let data = json!({
            "Reservations": [{
                "Instances": [{
                    "InstanceId": "i-1234567890abcdef0",
                    "State": {"Name": "running"}
                }]
            }]
        });
        let result = evaluate("Reservations[0].Instances[0].InstanceId", &data).unwrap();
        assert_eq!(result, json!("i-1234567890abcdef0"));
    }

    #[test]
    fn test_jmespath_flatten_nested_arrays() {
        let data = json!({"Reservations": [
            {"Instances": [{"Id": "i-1"}, {"Id": "i-2"}]},
            {"Instances": [{"Id": "i-3"}]}
        ]});
        let result = evaluate("Reservations[].Instances[].Id", &data).unwrap();
        assert_eq!(result, json!(["i-1", "i-2", "i-3"]));
    }

    #[test]
    fn test_jmespath_filter_then_field() {
        let data = json!({"tags": [
            {"Key": "Name", "Value": "my-instance"},
            {"Key": "Env", "Value": "prod"}
        ]});
        let result = evaluate("tags[?Key=='Name'].Value", &data).unwrap();
        assert_eq!(result, json!(["my-instance"]));
    }

    #[test]
    fn test_jmespath_list_wildcard_projection() {
        // [*] projects over elements, extracting a field from each
        let data = json!({
            "Buckets": [
                {"Name": "bucket-a", "CreationDate": "2024-01-01"},
                {"Name": "bucket-b", "CreationDate": "2024-02-01"}
            ]
        });
        let result = evaluate("Buckets[*].Name", &data).unwrap();
        assert_eq!(result, json!(["bucket-a", "bucket-b"]));
    }

    #[test]
    fn test_jmespath_list_wildcard_no_flatten() {
        // [*] should NOT flatten nested arrays (unlike [])
        let data = json!({"a": [[1, 2], [3, 4]]});
        let result_wildcard = evaluate("a[*]", &data).unwrap();
        assert_eq!(result_wildcard, json!([[1, 2], [3, 4]])); // preserved
        let result_flatten = evaluate("a[]", &data).unwrap();
        assert_eq!(result_flatten, json!([1, 2, 3, 4])); // flattened
    }

    #[test]
    fn test_jmespath_list_wildcard_empty_array() {
        // [*] on empty array should return empty array, not null
        let data = json!({"Users": []});
        let result = evaluate("Users[*].UserName", &data).unwrap();
        assert_eq!(result, json!([]));
    }

    #[test]
    fn test_jmespath_iam_list_users_pattern() {
        let data = json!({
            "Users": [
                {"UserName": "alice", "UserId": "AIDA1"},
                {"UserName": "bob", "UserId": "AIDA2"}
            ]
        });
        let result = evaluate("Users[].UserName", &data).unwrap();
        assert_eq!(result, json!(["alice", "bob"]));
    }

    #[test]
    fn test_jmespath_array_index_out_of_bounds() {
        let data = json!({"items": [1, 2]});
        let result = evaluate("items[5]", &data).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_jmespath_wildcard_with_projection() {
        let data = json!({
            "people": {
                "alice": {"age": 30},
                "bob": {"age": 25}
            }
        });
        let result = evaluate("people.*.age", &data).unwrap();
        assert!(result.is_array());
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr.contains(&json!(30)));
        assert!(arr.contains(&json!(25)));
    }

    #[test]
    fn test_jmespath_pipe_with_flatten() {
        let data = json!({
            "Reservations": [
                {"Instances": [{"Id": "i-1"}, {"Id": "i-2"}]},
                {"Instances": [{"Id": "i-3"}]}
            ]
        });
        let result = evaluate("Reservations[].Instances[] | [0].Id", &data).unwrap();
        assert_eq!(result, json!("i-1"));
    }

    #[test]
    fn test_jmespath_on_null_value() {
        let data = Value::Null;
        let result = evaluate("foo", &data).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_jmespath_quoted_field() {
        let data = json!({"foo bar": "value"});
        let result = evaluate("\"foo bar\"", &data).unwrap();
        assert_eq!(result, json!("value"));
    }
}
