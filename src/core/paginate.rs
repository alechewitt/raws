use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

/// Configuration for paginating a single operation, loaded from paginators-1.json.
#[derive(Debug, Clone)]
pub struct PaginatorConfig {
    /// The input parameter name(s) to set for the next page token.
    /// Usually a single string, but can be multiple (e.g., S3 ListMultipartUploads).
    pub input_token: Vec<String>,
    /// The output field name(s) containing the next page token.
    /// Parallel to input_token.
    pub output_token: Vec<String>,
    /// The output field(s) whose values should be aggregated (merged) across pages.
    pub result_key: Vec<String>,
    /// Optional: the input parameter name for limiting page size.
    pub limit_key: Option<String>,
    /// Optional: output field indicating if there are more results (e.g., "IsTruncated").
    pub more_results: Option<String>,
    /// Optional: keys that should NOT be aggregated — just use the value from the last page.
    pub non_aggregate_keys: Vec<String>,
}

/// Load paginator definitions from paginators-1.json in the given service version directory.
///
/// The `service_version_dir` should point to the directory containing service-2.json
/// (e.g., models/dynamodb/2012-08-10/).
pub fn load_paginators(service_version_dir: &Path) -> Result<HashMap<String, PaginatorConfig>> {
    let paginators_path = service_version_dir.join("paginators-1.json");
    if !paginators_path.exists() {
        return Ok(HashMap::new());
    }

    let content = std::fs::read_to_string(&paginators_path)
        .with_context(|| format!("Failed to read paginators file: {}", paginators_path.display()))?;

    parse_paginators(&content)
}

/// Parse the JSON content of a paginators-1.json file.
pub fn parse_paginators(json_str: &str) -> Result<HashMap<String, PaginatorConfig>> {
    let raw: Value = serde_json::from_str(json_str)
        .context("Failed to parse paginators JSON")?;

    let pagination = match raw.get("pagination").and_then(|v| v.as_object()) {
        Some(p) => p,
        None => return Ok(HashMap::new()),
    };

    let mut result = HashMap::new();

    for (op_name, config) in pagination {
        let input_token = parse_string_or_array(config, "input_token");
        let output_token = parse_string_or_array(config, "output_token");
        let result_key = parse_string_or_array(config, "result_key");
        let non_aggregate_keys = parse_string_or_array(config, "non_aggregate_keys");

        let limit_key = config
            .get("limit_key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let more_results = config
            .get("more_results")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Skip entries that don't have enough info to paginate
        if input_token.is_empty() || output_token.is_empty() {
            continue;
        }

        result.insert(
            op_name.clone(),
            PaginatorConfig {
                input_token,
                output_token,
                result_key,
                limit_key,
                more_results,
                non_aggregate_keys,
            },
        );
    }

    Ok(result)
}

/// Parse a field that can be either a single string or an array of strings.
fn parse_string_or_array(value: &Value, key: &str) -> Vec<String> {
    match value.get(key) {
        Some(Value::String(s)) => {
            // Some output_tokens have JMESPath expressions like "NextMarker || Contents[-1].Key"
            // We only take the first token (before ||) for simplicity
            let clean = s.split("||").next().unwrap_or(s).trim().to_string();
            vec![clean]
        }
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| {
                v.as_str().map(|s| {
                    s.split("||").next().unwrap_or(s).trim().to_string()
                })
            })
            .collect(),
        _ => vec![],
    }
}

/// Merge paginated results from multiple pages into a single response.
///
/// - `result_key` fields: arrays are concatenated, integers are summed
/// - `non_aggregate_keys`: values from the last page win
/// - All other fields: values from the last page win
/// - `output_token` fields are removed from the merged result (pagination is complete)
pub fn merge_pages(
    pages: &[Value],
    config: &PaginatorConfig,
) -> Value {
    if pages.is_empty() {
        return Value::Object(serde_json::Map::new());
    }
    if pages.len() == 1 {
        return pages[0].clone();
    }

    // Start with the last page as the base (non-aggregate keys come from the last page)
    let mut merged = match pages.last() {
        Some(v) => v.clone(),
        None => return Value::Object(serde_json::Map::new()),
    };

    let merged_obj = match merged.as_object_mut() {
        Some(obj) => obj,
        None => return merged,
    };

    // For result_key fields, aggregate across all pages
    for key in &config.result_key {
        let first_page_value = pages[0].get(key);

        // Determine if the value is an array or an integer
        match first_page_value {
            Some(Value::Array(_)) => {
                // Concatenate all arrays
                let mut combined = Vec::new();
                for page in pages {
                    if let Some(Value::Array(arr)) = page.get(key) {
                        combined.extend(arr.iter().cloned());
                    }
                }
                merged_obj.insert(key.clone(), Value::Array(combined));
            }
            Some(Value::Number(_)) => {
                // Sum all numbers (for things like Count, ScannedCount)
                let mut sum: i64 = 0;
                for page in pages {
                    if let Some(Value::Number(n)) = page.get(key) {
                        sum += n.as_i64().unwrap_or(0);
                    }
                }
                merged_obj.insert(key.clone(), Value::Number(serde_json::Number::from(sum)));
            }
            _ => {
                // For other types (including missing), just keep last page value
            }
        }
    }

    // Remove output_token fields from the final merged result
    // (they represent the pagination cursor, which is meaningless after full pagination)
    for token in &config.output_token {
        merged_obj.remove(token);
    }

    // Also remove more_results field if present
    if let Some(ref mr) = config.more_results {
        merged_obj.remove(mr);
    }

    merged
}

/// Check if the response indicates there are more pages to fetch.
///
/// Returns the next token values if pagination should continue, or None if done.
pub fn extract_next_tokens(
    response: &Value,
    config: &PaginatorConfig,
) -> Option<Vec<(String, String)>> {
    // Check more_results field first (e.g., IsTruncated for IAM/S3)
    if let Some(ref mr_key) = config.more_results {
        match response.get(mr_key) {
            Some(Value::Bool(false)) => return None,
            Some(Value::String(s)) if s == "false" => return None,
            _ => {}
        }
    }

    let mut tokens = Vec::new();

    for (input_tok, output_tok) in config.input_token.iter().zip(config.output_token.iter()) {
        match response.get(output_tok) {
            Some(Value::String(s)) if !s.is_empty() => {
                tokens.push((input_tok.clone(), s.clone()));
            }
            Some(Value::String(_)) | Some(Value::Null) | None => {
                // Empty string, null, or missing: pagination is done
                return None;
            }
            Some(other) => {
                // For non-string tokens (e.g., map-type LastEvaluatedKey in DynamoDB)
                let s = other.to_string();
                if s == "null" || s.is_empty() || s == "{}" {
                    return None;
                }
                tokens.push((input_tok.clone(), s));
            }
        }
    }

    if tokens.is_empty() {
        None
    } else {
        Some(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---------------------------------------------------------------
    // Paginator config loading tests
    // ---------------------------------------------------------------

    #[test]
    fn test_auto_paginate_parse_simple_paginator() {
        let json = r#"{
            "pagination": {
                "ListTables": {
                    "input_token": "ExclusiveStartTableName",
                    "output_token": "LastEvaluatedTableName",
                    "limit_key": "Limit",
                    "result_key": "TableNames"
                }
            }
        }"#;

        let paginators = parse_paginators(json).unwrap();
        assert_eq!(paginators.len(), 1);

        let config = &paginators["ListTables"];
        assert_eq!(config.input_token, vec!["ExclusiveStartTableName"]);
        assert_eq!(config.output_token, vec!["LastEvaluatedTableName"]);
        assert_eq!(config.result_key, vec!["TableNames"]);
        assert_eq!(config.limit_key, Some("Limit".to_string()));
        assert!(config.more_results.is_none());
        assert!(config.non_aggregate_keys.is_empty());
    }

    #[test]
    fn test_auto_paginate_parse_array_result_keys() {
        let json = r#"{
            "pagination": {
                "Query": {
                    "input_token": "ExclusiveStartKey",
                    "output_token": "LastEvaluatedKey",
                    "limit_key": "Limit",
                    "result_key": ["Items", "Count", "ScannedCount"],
                    "non_aggregate_keys": ["ConsumedCapacity"]
                }
            }
        }"#;

        let paginators = parse_paginators(json).unwrap();
        let config = &paginators["Query"];
        assert_eq!(config.result_key, vec!["Items", "Count", "ScannedCount"]);
        assert_eq!(config.non_aggregate_keys, vec!["ConsumedCapacity"]);
    }

    #[test]
    fn test_auto_paginate_parse_multi_token() {
        let json = r#"{
            "pagination": {
                "ListMultipartUploads": {
                    "limit_key": "MaxUploads",
                    "more_results": "IsTruncated",
                    "output_token": ["NextKeyMarker", "NextUploadIdMarker"],
                    "input_token": ["KeyMarker", "UploadIdMarker"],
                    "result_key": ["Uploads", "CommonPrefixes"]
                }
            }
        }"#;

        let paginators = parse_paginators(json).unwrap();
        let config = &paginators["ListMultipartUploads"];
        assert_eq!(config.input_token, vec!["KeyMarker", "UploadIdMarker"]);
        assert_eq!(
            config.output_token,
            vec!["NextKeyMarker", "NextUploadIdMarker"]
        );
        assert_eq!(config.more_results, Some("IsTruncated".to_string()));
    }

    #[test]
    fn test_auto_paginate_parse_more_results_field() {
        let json = r#"{
            "pagination": {
                "ListUsers": {
                    "input_token": "Marker",
                    "limit_key": "MaxItems",
                    "more_results": "IsTruncated",
                    "output_token": "Marker",
                    "result_key": "Users"
                }
            }
        }"#;

        let paginators = parse_paginators(json).unwrap();
        let config = &paginators["ListUsers"];
        assert_eq!(config.more_results, Some("IsTruncated".to_string()));
    }

    #[test]
    fn test_auto_paginate_parse_jmespath_output_token() {
        // S3 ListObjects has: "NextMarker || Contents[-1].Key"
        let json = r#"{
            "pagination": {
                "ListObjects": {
                    "more_results": "IsTruncated",
                    "limit_key": "MaxKeys",
                    "output_token": "NextMarker || Contents[-1].Key",
                    "input_token": "Marker",
                    "result_key": ["Contents", "CommonPrefixes"]
                }
            }
        }"#;

        let paginators = parse_paginators(json).unwrap();
        let config = &paginators["ListObjects"];
        // We take only the first part before ||
        assert_eq!(config.output_token, vec!["NextMarker"]);
    }

    #[test]
    fn test_auto_paginate_parse_empty_pagination() {
        let json = r#"{ "pagination": {} }"#;
        let paginators = parse_paginators(json).unwrap();
        assert!(paginators.is_empty());
    }

    #[test]
    fn test_auto_paginate_parse_no_pagination_key() {
        let json = r#"{ "version": "1.0" }"#;
        let paginators = parse_paginators(json).unwrap();
        assert!(paginators.is_empty());
    }

    #[test]
    fn test_auto_paginate_load_real_dynamodb_paginators() {
        let path = Path::new("models/dynamodb/2012-08-10");
        if !path.exists() {
            eprintln!("Skipping: DynamoDB model not copied yet");
            return;
        }
        let paginators = load_paginators(path).unwrap();
        assert!(paginators.contains_key("ListTables"));
        assert!(paginators.contains_key("Query"));
        assert!(paginators.contains_key("Scan"));

        let list_tables = &paginators["ListTables"];
        assert_eq!(list_tables.input_token, vec!["ExclusiveStartTableName"]);
        assert_eq!(list_tables.output_token, vec!["LastEvaluatedTableName"]);
        assert_eq!(list_tables.limit_key, Some("Limit".to_string()));
        assert_eq!(list_tables.result_key, vec!["TableNames"]);
    }

    #[test]
    fn test_auto_paginate_load_real_iam_paginators() {
        let path = Path::new("models/iam/2010-05-08");
        if !path.exists() {
            eprintln!("Skipping: IAM model not copied yet");
            return;
        }
        let paginators = load_paginators(path).unwrap();
        assert!(paginators.contains_key("ListUsers"));

        let list_users = &paginators["ListUsers"];
        assert_eq!(list_users.more_results, Some("IsTruncated".to_string()));
    }

    #[test]
    fn test_auto_paginate_load_real_s3_paginators() {
        let path = Path::new("models/s3/2006-03-01");
        if !path.exists() {
            eprintln!("Skipping: S3 model not copied yet");
            return;
        }
        let paginators = load_paginators(path).unwrap();
        assert!(paginators.contains_key("ListObjectsV2"));
        assert!(paginators.contains_key("ListBuckets"));

        // S3 ListMultipartUploads has multi-token
        if let Some(config) = paginators.get("ListMultipartUploads") {
            assert_eq!(config.input_token.len(), 2);
            assert_eq!(config.output_token.len(), 2);
        }
    }

    #[test]
    fn test_auto_paginate_load_nonexistent_dir() {
        let path = Path::new("models/nonexistent-service/9999-99-99");
        let paginators = load_paginators(path).unwrap();
        assert!(paginators.is_empty());
    }

    // ---------------------------------------------------------------
    // Result merging tests
    // ---------------------------------------------------------------

    fn simple_config() -> PaginatorConfig {
        PaginatorConfig {
            input_token: vec!["NextToken".to_string()],
            output_token: vec!["NextToken".to_string()],
            result_key: vec!["Items".to_string()],
            limit_key: None,
            more_results: None,
            non_aggregate_keys: vec![],
        }
    }

    #[test]
    fn test_auto_paginate_merge_single_page() {
        let config = simple_config();
        let page = json!({
            "Items": [{"id": 1}, {"id": 2}],
            "Count": 2
        });

        let merged = merge_pages(&[page.clone()], &config);
        assert_eq!(merged, page);
    }

    #[test]
    fn test_auto_paginate_merge_two_pages() {
        let config = simple_config();
        let page1 = json!({
            "Items": [{"id": 1}, {"id": 2}],
            "NextToken": "token1"
        });
        let page2 = json!({
            "Items": [{"id": 3}, {"id": 4}]
        });

        let merged = merge_pages(&[page1, page2], &config);

        // Items should be concatenated
        let items = merged["Items"].as_array().unwrap();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0]["id"], 1);
        assert_eq!(items[3]["id"], 4);

        // NextToken should be removed (output_token)
        assert!(merged.get("NextToken").is_none());
    }

    #[test]
    fn test_auto_paginate_merge_three_pages() {
        let config = simple_config();
        let page1 = json!({ "Items": ["a", "b"], "NextToken": "t1" });
        let page2 = json!({ "Items": ["c"], "NextToken": "t2" });
        let page3 = json!({ "Items": ["d", "e"] });

        let merged = merge_pages(&[page1, page2, page3], &config);
        let items = merged["Items"].as_array().unwrap();
        assert_eq!(items.len(), 5);
        assert_eq!(items[0], "a");
        assert_eq!(items[4], "e");
    }

    #[test]
    fn test_auto_paginate_merge_with_count_aggregation() {
        let config = PaginatorConfig {
            input_token: vec!["ExclusiveStartKey".to_string()],
            output_token: vec!["LastEvaluatedKey".to_string()],
            result_key: vec!["Items".to_string(), "Count".to_string(), "ScannedCount".to_string()],
            limit_key: None,
            more_results: None,
            non_aggregate_keys: vec!["ConsumedCapacity".to_string()],
        };

        let page1 = json!({
            "Items": [{"id": 1}],
            "Count": 1,
            "ScannedCount": 5,
            "LastEvaluatedKey": {"id": {"N": "1"}},
            "ConsumedCapacity": {"TableName": "test", "CapacityUnits": 1.0}
        });
        let page2 = json!({
            "Items": [{"id": 2}, {"id": 3}],
            "Count": 2,
            "ScannedCount": 10,
            "ConsumedCapacity": {"TableName": "test", "CapacityUnits": 2.0}
        });

        let merged = merge_pages(&[page1, page2], &config);

        // Items concatenated
        assert_eq!(merged["Items"].as_array().unwrap().len(), 3);
        // Count summed
        assert_eq!(merged["Count"].as_i64().unwrap(), 3);
        // ScannedCount summed
        assert_eq!(merged["ScannedCount"].as_i64().unwrap(), 15);
        // LastEvaluatedKey removed (output_token)
        assert!(merged.get("LastEvaluatedKey").is_none());
        // ConsumedCapacity from last page (non-aggregate)
        assert_eq!(merged["ConsumedCapacity"]["CapacityUnits"].as_f64().unwrap(), 2.0);
    }

    #[test]
    fn test_auto_paginate_merge_non_aggregate_keys() {
        let config = PaginatorConfig {
            input_token: vec!["Marker".to_string()],
            output_token: vec!["Marker".to_string()],
            result_key: vec!["Users".to_string()],
            limit_key: None,
            more_results: Some("IsTruncated".to_string()),
            non_aggregate_keys: vec!["Group".to_string()],
        };

        let page1 = json!({
            "Users": [{"name": "alice"}],
            "Group": {"GroupName": "admins"},
            "IsTruncated": true,
            "Marker": "abc"
        });
        let page2 = json!({
            "Users": [{"name": "bob"}],
            "Group": {"GroupName": "admins"},
            "IsTruncated": false
        });

        let merged = merge_pages(&[page1, page2], &config);

        // Users concatenated
        assert_eq!(merged["Users"].as_array().unwrap().len(), 2);
        // Group from last page
        assert_eq!(merged["Group"]["GroupName"], "admins");
        // Marker removed
        assert!(merged.get("Marker").is_none());
        // IsTruncated removed (more_results)
        assert!(merged.get("IsTruncated").is_none());
    }

    #[test]
    fn test_auto_paginate_merge_empty_pages() {
        let config = simple_config();
        let merged = merge_pages(&[], &config);
        assert_eq!(merged, json!({}));
    }

    #[test]
    fn test_auto_paginate_merge_multi_result_key() {
        let config = PaginatorConfig {
            input_token: vec!["KeyMarker".to_string(), "UploadIdMarker".to_string()],
            output_token: vec!["NextKeyMarker".to_string(), "NextUploadIdMarker".to_string()],
            result_key: vec!["Uploads".to_string(), "CommonPrefixes".to_string()],
            limit_key: None,
            more_results: Some("IsTruncated".to_string()),
            non_aggregate_keys: vec![],
        };

        let page1 = json!({
            "Uploads": [{"key": "file1.txt"}],
            "CommonPrefixes": [{"Prefix": "dir1/"}],
            "IsTruncated": true,
            "NextKeyMarker": "k1",
            "NextUploadIdMarker": "u1"
        });
        let page2 = json!({
            "Uploads": [{"key": "file2.txt"}],
            "CommonPrefixes": [{"Prefix": "dir2/"}],
            "IsTruncated": false
        });

        let merged = merge_pages(&[page1, page2], &config);
        assert_eq!(merged["Uploads"].as_array().unwrap().len(), 2);
        assert_eq!(merged["CommonPrefixes"].as_array().unwrap().len(), 2);
        assert!(merged.get("NextKeyMarker").is_none());
        assert!(merged.get("NextUploadIdMarker").is_none());
        assert!(merged.get("IsTruncated").is_none());
    }

    // ---------------------------------------------------------------
    // Token extraction tests
    // ---------------------------------------------------------------

    #[test]
    fn test_auto_paginate_extract_next_token_present() {
        let config = simple_config();
        let response = json!({
            "Items": [{"id": 1}],
            "NextToken": "abc123"
        });

        let tokens = extract_next_tokens(&response, &config);
        assert!(tokens.is_some());
        let tokens = tokens.unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], ("NextToken".to_string(), "abc123".to_string()));
    }

    #[test]
    fn test_auto_paginate_extract_next_token_null() {
        let config = simple_config();
        let response = json!({
            "Items": [{"id": 1}],
            "NextToken": null
        });

        assert!(extract_next_tokens(&response, &config).is_none());
    }

    #[test]
    fn test_auto_paginate_extract_next_token_missing() {
        let config = simple_config();
        let response = json!({
            "Items": [{"id": 1}]
        });

        assert!(extract_next_tokens(&response, &config).is_none());
    }

    #[test]
    fn test_auto_paginate_extract_next_token_empty_string() {
        let config = simple_config();
        let response = json!({
            "Items": [{"id": 1}],
            "NextToken": ""
        });

        assert!(extract_next_tokens(&response, &config).is_none());
    }

    #[test]
    fn test_auto_paginate_extract_with_is_truncated_false() {
        let config = PaginatorConfig {
            input_token: vec!["Marker".to_string()],
            output_token: vec!["Marker".to_string()],
            result_key: vec!["Users".to_string()],
            limit_key: None,
            more_results: Some("IsTruncated".to_string()),
            non_aggregate_keys: vec![],
        };

        let response = json!({
            "Users": [{"name": "alice"}],
            "Marker": "abc",
            "IsTruncated": false
        });

        // IsTruncated=false means no more pages even though Marker is present
        assert!(extract_next_tokens(&response, &config).is_none());
    }

    #[test]
    fn test_auto_paginate_extract_with_is_truncated_true() {
        let config = PaginatorConfig {
            input_token: vec!["Marker".to_string()],
            output_token: vec!["Marker".to_string()],
            result_key: vec!["Users".to_string()],
            limit_key: None,
            more_results: Some("IsTruncated".to_string()),
            non_aggregate_keys: vec![],
        };

        let response = json!({
            "Users": [{"name": "alice"}],
            "Marker": "abc",
            "IsTruncated": true
        });

        let tokens = extract_next_tokens(&response, &config);
        assert!(tokens.is_some());
        assert_eq!(tokens.unwrap()[0].1, "abc");
    }

    #[test]
    fn test_auto_paginate_extract_multi_token() {
        let config = PaginatorConfig {
            input_token: vec!["KeyMarker".to_string(), "UploadIdMarker".to_string()],
            output_token: vec!["NextKeyMarker".to_string(), "NextUploadIdMarker".to_string()],
            result_key: vec!["Uploads".to_string()],
            limit_key: None,
            more_results: Some("IsTruncated".to_string()),
            non_aggregate_keys: vec![],
        };

        let response = json!({
            "Uploads": [],
            "NextKeyMarker": "key1",
            "NextUploadIdMarker": "upload1",
            "IsTruncated": true
        });

        let tokens = extract_next_tokens(&response, &config).unwrap();
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0], ("KeyMarker".to_string(), "key1".to_string()));
        assert_eq!(tokens[1], ("UploadIdMarker".to_string(), "upload1".to_string()));
    }

    #[test]
    fn test_auto_paginate_extract_multi_token_partial_missing() {
        let config = PaginatorConfig {
            input_token: vec!["KeyMarker".to_string(), "UploadIdMarker".to_string()],
            output_token: vec!["NextKeyMarker".to_string(), "NextUploadIdMarker".to_string()],
            result_key: vec!["Uploads".to_string()],
            limit_key: None,
            more_results: None,
            non_aggregate_keys: vec![],
        };

        // One token present, one missing -> no more pages
        let response = json!({
            "Uploads": [],
            "NextKeyMarker": "key1"
        });

        assert!(extract_next_tokens(&response, &config).is_none());
    }
}
