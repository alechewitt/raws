// Query protocol serializer/parser
//
//  Serializer: builds a form-encoded body string for AWS Query-protocol requests.
//  Response parser: converts XML response bodies into serde_json::Value.
//  Error parser: extracts Code and Message from ErrorResponse XML.

use anyhow::{bail, Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use serde_json::Value;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Serializer
// ---------------------------------------------------------------------------

/// Serialize a query-protocol request body.
///
/// Returns a form-encoded string like:
///   Action=GetCallerIdentity&Version=2011-06-15&Param1=val1
pub fn serialize_query_request(
    operation_name: &str,
    api_version: &str,
    input: &Value,
    shapes: &HashMap<String, Value>,
    input_shape_name: &str,
) -> Result<String> {
    serialize_query_request_inner(operation_name, api_version, input, shapes, input_shape_name, false)
}

/// Serialize an EC2-protocol request body.
///
/// EC2 protocol is similar to query protocol, but with two differences:
///   1. Lists always use `Prefix.N` format (no `.member.` wrapper)
///   2. `locationName` values are capitalized (first letter uppercased)
pub fn serialize_ec2_request(
    operation_name: &str,
    api_version: &str,
    input: &Value,
    shapes: &HashMap<String, Value>,
    input_shape_name: &str,
) -> Result<String> {
    serialize_query_request_inner(operation_name, api_version, input, shapes, input_shape_name, true)
}

fn serialize_query_request_inner(
    operation_name: &str,
    api_version: &str,
    input: &Value,
    shapes: &HashMap<String, Value>,
    input_shape_name: &str,
    is_ec2: bool,
) -> Result<String> {
    let mut params: Vec<(String, String)> = Vec::new();
    params.push(("Action".to_string(), operation_name.to_string()));
    params.push(("Version".to_string(), api_version.to_string()));

    if let Some(shape_def) = shapes.get(input_shape_name) {
        serialize_value(&mut params, input, shape_def, shapes, "", is_ec2)?;
    }

    // Sort params by key for deterministic output (matching AWS SDK behavior for signing)
    params.sort_by(|a, b| a.0.cmp(&b.0));

    let encoded: Vec<String> = params
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                percent_encode_query(k),
                percent_encode_query(v)
            )
        })
        .collect();

    Ok(encoded.join("&"))
}

fn percent_encode_query(input: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
    // AWS requires encoding everything except unreserved characters: A-Z a-z 0-9 - _ . ~
    const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    utf8_percent_encode(input, ENCODE_SET).to_string()
}

fn serialize_value(
    params: &mut Vec<(String, String)>,
    value: &Value,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
    prefix: &str,
    is_ec2: bool,
) -> Result<()> {
    let shape_type = shape_def
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("string");

    match shape_type {
        "structure" => serialize_structure(params, value, shape_def, shapes, prefix, is_ec2),
        "list" => serialize_list(params, value, shape_def, shapes, prefix, is_ec2),
        "map" => serialize_map(params, value, shape_def, shapes, prefix, is_ec2),
        "string" | "timestamp" | "blob" => {
            if let Some(s) = value.as_str() {
                params.push((prefix.to_string(), s.to_string()));
            }
            Ok(())
        }
        "integer" | "long" => {
            if let Some(n) = value.as_i64() {
                params.push((prefix.to_string(), n.to_string()));
            }
            Ok(())
        }
        "float" | "double" => {
            if let Some(n) = value.as_f64() {
                params.push((prefix.to_string(), n.to_string()));
            }
            Ok(())
        }
        "boolean" => {
            if let Some(b) = value.as_bool() {
                params.push((
                    prefix.to_string(),
                    if b { "true" } else { "false" }.to_string(),
                ));
            }
            Ok(())
        }
        _ => {
            // Default: treat as string
            let s = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => value.to_string(),
            };
            params.push((prefix.to_string(), s));
            Ok(())
        }
    }
}

fn serialize_structure(
    params: &mut Vec<(String, String)>,
    value: &Value,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
    prefix: &str,
    is_ec2: bool,
) -> Result<()> {
    let members = match shape_def.get("members").and_then(|m| m.as_object()) {
        Some(m) => m,
        None => return Ok(()),
    };

    let obj = match value.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    for (key, val) in obj {
        // Look up the member definition
        let member_def = match members.get(key) {
            Some(m) => m,
            None => continue,
        };

        // The serialized name may come from locationName or queryName, otherwise use the key
        let serialized_name = if is_ec2 {
            get_ec2_serialized_name(member_def, key)
        } else {
            get_serialized_name(member_def, key)
        };

        let member_prefix = if prefix.is_empty() {
            serialized_name
        } else {
            format!("{}.{}", prefix, serialized_name)
        };

        // Resolve the target shape
        let target_shape_name = member_def
            .get("shape")
            .and_then(|s| s.as_str())
            .unwrap_or("");
        let target_shape = shapes.get(target_shape_name).cloned().unwrap_or(Value::Null);

        serialize_value(params, val, &target_shape, shapes, &member_prefix, is_ec2)?;
    }

    Ok(())
}

fn serialize_list(
    params: &mut Vec<(String, String)>,
    value: &Value,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
    prefix: &str,
    is_ec2: bool,
) -> Result<()> {
    let arr = match value.as_array() {
        Some(a) => a,
        None => return Ok(()),
    };

    if arr.is_empty() {
        // Query protocol serializes empty lists as empty string
        params.push((prefix.to_string(), String::new()));
        return Ok(());
    }

    let member_ref = shape_def.get("member").unwrap_or(&Value::Null);
    let member_shape_name = member_ref
        .get("shape")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let member_shape = shapes.get(member_shape_name).cloned().unwrap_or(Value::Null);

    // EC2 protocol always serializes lists as Prefix.N (no .member. wrapper)
    let list_prefix = if is_ec2 {
        prefix.to_string()
    } else {
        let flattened = shape_def
            .get("flattened")
            .and_then(|f| f.as_bool())
            .unwrap_or(false);

        if flattened {
            // For flattened lists, check if the member has a serialization name
            let member_name = member_ref
                .get("locationName")
                .and_then(|n| n.as_str());
            match member_name {
                Some(name) => {
                    // Replace the last component of prefix with member name
                    let parts: Vec<&str> = prefix.rsplitn(2, '.').collect();
                    if parts.len() == 2 {
                        format!("{}.{}", parts[1], name)
                    } else {
                        name.to_string()
                    }
                }
                None => prefix.to_string(),
            }
        } else {
            // Non-flattened: append the member name (defaults to "member")
            let member_name = member_ref
                .get("locationName")
                .and_then(|n| n.as_str())
                .unwrap_or("member");
            format!("{}.{}", prefix, member_name)
        }
    };

    for (i, element) in arr.iter().enumerate() {
        let element_prefix = format!("{}.{}", list_prefix, i + 1);
        serialize_value(params, element, &member_shape, shapes, &element_prefix, is_ec2)?;
    }

    Ok(())
}

fn serialize_map(
    params: &mut Vec<(String, String)>,
    value: &Value,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
    prefix: &str,
    is_ec2: bool,
) -> Result<()> {
    let obj = match value.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    let flattened = shape_def
        .get("flattened")
        .and_then(|f| f.as_bool())
        .unwrap_or(false);

    let full_prefix = if flattened {
        prefix.to_string()
    } else {
        format!("{}.entry", prefix)
    };

    let key_ref = shape_def.get("key").unwrap_or(&Value::Null);
    let value_ref = shape_def.get("value").unwrap_or(&Value::Null);

    let key_shape_name = key_ref.get("shape").and_then(|s| s.as_str()).unwrap_or("");
    let value_shape_name = value_ref
        .get("shape")
        .and_then(|s| s.as_str())
        .unwrap_or("");

    let key_shape = shapes.get(key_shape_name).cloned().unwrap_or(Value::Null);
    let value_shape = shapes
        .get(value_shape_name)
        .cloned()
        .unwrap_or(Value::Null);

    let key_suffix = get_serialized_name(key_ref, "key");
    let value_suffix = get_serialized_name(value_ref, "value");

    for (i, (k, v)) in obj.iter().enumerate() {
        let idx = i + 1;
        let key_prefix = format!("{}.{}.{}", full_prefix, idx, key_suffix);
        let value_prefix = format!("{}.{}.{}", full_prefix, idx, value_suffix);
        serialize_value(
            params,
            &Value::String(k.clone()),
            &key_shape,
            shapes,
            &key_prefix,
            is_ec2,
        )?;
        serialize_value(params, v, &value_shape, shapes, &value_prefix, is_ec2)?;
    }

    Ok(())
}

/// Get the serialized name for a member/key/value reference.
/// Uses "locationName" (or "queryName" for EC2) if present, otherwise the default.
fn get_serialized_name(member_ref: &Value, default_name: &str) -> String {
    // queryName takes precedence (for EC2 protocol)
    if let Some(qn) = member_ref.get("queryName").and_then(|n| n.as_str()) {
        return qn.to_string();
    }
    if let Some(name) = member_ref.get("locationName").and_then(|n| n.as_str()) {
        return name.to_string();
    }
    default_name.to_string()
}

/// Get the serialized name for EC2 protocol.
///
/// EC2 protocol differs from standard query:
///   - `queryName` takes highest precedence (same as query)
///   - `locationName` is capitalized (first letter uppercased)
///   - Falls back to the default name
fn get_ec2_serialized_name(member_ref: &Value, default_name: &str) -> String {
    if let Some(qn) = member_ref.get("queryName").and_then(|n| n.as_str()) {
        return qn.to_string();
    }
    if let Some(name) = member_ref.get("locationName").and_then(|n| n.as_str()) {
        // EC2 protocol capitalizes the first letter of locationName
        return capitalize_first(name);
    }
    default_name.to_string()
}

/// Capitalize the first character of a string.
fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            let upper: String = first.to_uppercase().collect();
            upper + chars.as_str()
        }
    }
}

// ---------------------------------------------------------------------------
// Response parser
// ---------------------------------------------------------------------------

/// Parse a query-protocol XML response into a serde_json::Value.
///
/// The XML structure is typically:
///   <OperationNameResponse>
///     <OperationNameResult>  (the resultWrapper)
///       ... fields ...
///     </OperationNameResult>
///     <ResponseMetadata>
///       <RequestId>...</RequestId>
///     </ResponseMetadata>
///   </OperationNameResponse>
pub fn parse_query_response(
    xml_body: &str,
    result_wrapper: Option<&str>,
    output_shape_name: &str,
    shapes: &HashMap<String, Value>,
) -> Result<Value> {
    let root = parse_xml_to_tree(xml_body)?;

    // Find the result wrapper element if specified
    let result_node = if let Some(wrapper_name) = result_wrapper {
        find_child_element(&root, wrapper_name)
            .with_context(|| format!("Result wrapper '{}' not found in XML response", wrapper_name))?
    } else {
        root.clone()
    };

    let shape_def = shapes
        .get(output_shape_name)
        .ok_or_else(|| anyhow::anyhow!("Output shape '{}' not found", output_shape_name))?;

    parse_shape_from_xml(&result_node, shape_def, shapes)
}

/// Parse a query-protocol XML error response.
///
/// Expected structure:
///   <ErrorResponse>
///     <Error>
///       <Code>...</Code>
///       <Message>...</Message>
///       <Type>...</Type>
///     </Error>
///     <RequestId>...</RequestId>
///   </ErrorResponse>
///
/// Returns (code, message).
pub fn parse_query_error(xml_body: &str) -> Result<(String, String)> {
    let root = parse_xml_to_tree(xml_body)?;

    // Navigate to <Error> child
    let error_node = find_child_element(&root, "Error")
        .context("No <Error> element found in error response")?;

    let code = find_child_text(&error_node, "Code")
        .unwrap_or_default();
    let message = find_child_text(&error_node, "Message")
        .unwrap_or_default();

    Ok((code, message))
}

/// Parse an EC2 query protocol XML error response.
///
/// EC2 uses a different error XML structure from standard query protocol:
///   <Response>
///     <Errors>
///       <Error>
///         <Code>...</Code>
///         <Message>...</Message>
///       </Error>
///     </Errors>
///     <RequestID>...</RequestID>
///   </Response>
///
/// Returns (code, message).
pub fn parse_ec2_error(xml_body: &str) -> Result<(String, String)> {
    let root = parse_xml_to_tree(xml_body)?;

    // Navigate to <Errors> -> <Error>
    let errors_node = find_child_element(&root, "Errors");
    let error_node = errors_node
        .as_ref()
        .and_then(|errors| find_child_element(errors, "Error"))
        .or_else(|| find_child_element(&root, "Error"));

    match error_node {
        Some(node) => {
            let code = find_child_text(&node, "Code").unwrap_or_default();
            let message = find_child_text(&node, "Message").unwrap_or_default();
            Ok((code, message))
        }
        None => {
            // Fallback: try to find Code/Message directly in root
            let code = find_child_text(&root, "Code").unwrap_or_default();
            let message = find_child_text(&root, "Message").unwrap_or_default();
            Ok((code, message))
        }
    }
}

// ---------------------------------------------------------------------------
// XML tree representation and parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct XmlNode {
    tag: String,
    text: Option<String>,
    children: Vec<XmlNode>,
}

impl XmlNode {
    fn new(tag: &str) -> Self {
        XmlNode {
            tag: tag.to_string(),
            text: None,
            children: Vec::new(),
        }
    }
}

/// Parse XML string into a simple tree structure.
fn parse_xml_to_tree(xml_str: &str) -> Result<XmlNode> {
    let mut reader = Reader::from_str(xml_str);
    let mut stack: Vec<XmlNode> = Vec::new();
    let mut root: Option<XmlNode> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let tag = strip_namespace(
                    std::str::from_utf8(e.name().as_ref())
                        .context("Invalid UTF-8 in XML tag")?,
                );
                stack.push(XmlNode::new(&tag));
            }
            Ok(Event::End(_)) => {
                let node = stack.pop().ok_or_else(|| anyhow::anyhow!("Unexpected end tag"))?;
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(node);
                } else {
                    root = Some(node);
                }
            }
            Ok(Event::Empty(ref e)) => {
                let tag = strip_namespace(
                    std::str::from_utf8(e.name().as_ref())
                        .context("Invalid UTF-8 in XML tag")?,
                );
                let node = XmlNode::new(&tag);
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(node);
                } else {
                    root = Some(node);
                }
            }
            Ok(Event::Text(ref e)) => {
                let text = e
                    .unescape()
                    .context("Failed to unescape XML text")?
                    .to_string();
                if !text.trim().is_empty() {
                    if let Some(current) = stack.last_mut() {
                        current.text = Some(text);
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {} // Skip comments, processing instructions, etc.
            Err(e) => bail!("XML parse error: {}", e),
        }
    }

    root.ok_or_else(|| anyhow::anyhow!("Empty or invalid XML document"))
}

/// Strip XML namespace prefix (e.g., "{https://...}Tag" -> "Tag" or "ns:Tag" -> "Tag")
fn strip_namespace(tag: &str) -> String {
    // Handle {uri}LocalName format
    if let Some(pos) = tag.rfind('}') {
        return tag[pos + 1..].to_string();
    }
    // Handle prefix:LocalName format
    if let Some(pos) = tag.rfind(':') {
        return tag[pos + 1..].to_string();
    }
    tag.to_string()
}

fn find_child_element(node: &XmlNode, name: &str) -> Option<XmlNode> {
    for child in &node.children {
        if child.tag == name {
            return Some(child.clone());
        }
    }
    None
}

fn find_child_text(node: &XmlNode, name: &str) -> Option<String> {
    for child in &node.children {
        if child.tag == name {
            return child.text.clone();
        }
    }
    None
}

/// Parse an XML node into a serde_json::Value based on the shape definition.
fn parse_shape_from_xml(
    node: &XmlNode,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
) -> Result<Value> {
    let shape_type = shape_def
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("string");

    match shape_type {
        "structure" => parse_structure_from_xml(node, shape_def, shapes),
        "list" => parse_list_from_xml(node, shape_def, shapes),
        "map" => parse_map_from_xml(node, shape_def, shapes),
        "string" | "blob" => {
            Ok(Value::String(node.text.clone().unwrap_or_default()))
        }
        "timestamp" => {
            let raw = node.text.clone().unwrap_or_default();
            Ok(Value::String(super::normalize_timestamp(&raw)))
        }
        "integer" | "long" => {
            let text = node.text.clone().unwrap_or_default();
            let n: i64 = text
                .parse()
                .with_context(|| format!("Failed to parse '{}' as integer", text))?;
            Ok(Value::Number(serde_json::Number::from(n)))
        }
        "float" | "double" => {
            let text = node.text.clone().unwrap_or_default();
            let n: f64 = text
                .parse()
                .with_context(|| format!("Failed to parse '{}' as float", text))?;
            Ok(serde_json::Number::from_f64(n)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        "boolean" => {
            let text = node.text.clone().unwrap_or_default();
            Ok(Value::Bool(text == "true"))
        }
        _ => {
            // Default: treat as string
            Ok(Value::String(node.text.clone().unwrap_or_default()))
        }
    }
}

fn parse_structure_from_xml(
    node: &XmlNode,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
) -> Result<Value> {
    let mut result = serde_json::Map::new();

    let members = match shape_def.get("members").and_then(|m| m.as_object()) {
        Some(m) => m,
        None => return Ok(Value::Object(result)),
    };

    // Build a name-to-children map: group children by tag name.
    // If multiple children share the same tag, aggregate into a Vec.
    let name_to_children = build_name_to_children(node);

    for (member_name, member_def) in members {
        let member_shape_name = member_def
            .get("shape")
            .and_then(|s| s.as_str())
            .unwrap_or("");
        let member_shape = match shapes.get(member_shape_name) {
            Some(s) => s,
            None => continue,
        };

        // Determine the XML element name for this member
        let xml_name = member_xml_name(member_def, member_shape, member_name);

        if let Some(child_nodes) = name_to_children.get(&xml_name) {
            let member_type = member_shape
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("string");

            // For list shapes that are flattened, pass all matching children
            let is_flattened_list = member_type == "list"
                && member_shape
                    .get("flattened")
                    .and_then(|f| f.as_bool())
                    .unwrap_or(false);

            if is_flattened_list {
                // Parse as flattened list: child_nodes are the list elements directly
                let member_ref = member_shape.get("member").unwrap_or(&Value::Null);
                let element_shape_name = member_ref
                    .get("shape")
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                let element_shape = shapes.get(element_shape_name).unwrap_or(&Value::Null);

                let mut arr = Vec::new();
                for child in child_nodes {
                    arr.push(parse_shape_from_xml(child, element_shape, shapes)?);
                }
                result.insert(member_name.clone(), Value::Array(arr));
            } else {
                // Use the first matching child node
                let child = &child_nodes[0];
                let parsed = parse_shape_from_xml(child, member_shape, shapes)?;
                result.insert(member_name.clone(), parsed);
            }
        }
    }

    Ok(Value::Object(result))
}

/// Determine the XML element name for a structure member.
///
/// For flattened lists with a member locationName, use that.
/// Otherwise use the member's own locationName, or fall back to the member name.
fn member_xml_name(member_def: &Value, member_shape: &Value, member_name: &str) -> String {
    let member_type = member_shape
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("");

    // Special case: flattened list with member serialization name
    if member_type == "list" {
        let is_flattened = member_shape
            .get("flattened")
            .and_then(|f| f.as_bool())
            .unwrap_or(false);
        if is_flattened {
            if let Some(list_member_name) = member_shape
                .get("member")
                .and_then(|m| m.get("locationName"))
                .and_then(|n| n.as_str())
            {
                return list_member_name.to_string();
            }
        }
    }

    // Use the member's locationName if present
    if let Some(name) = member_def.get("locationName").and_then(|n| n.as_str()) {
        return name.to_string();
    }

    member_name.to_string()
}

fn build_name_to_children(node: &XmlNode) -> HashMap<String, Vec<XmlNode>> {
    let mut map: HashMap<String, Vec<XmlNode>> = HashMap::new();
    for child in &node.children {
        map.entry(child.tag.clone())
            .or_default()
            .push(child.clone());
    }
    map
}

fn parse_list_from_xml(
    node: &XmlNode,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
) -> Result<Value> {
    let member_ref = shape_def.get("member").unwrap_or(&Value::Null);
    let member_shape_name = member_ref
        .get("shape")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let member_shape = shapes.get(member_shape_name).unwrap_or(&Value::Null);

    let member_tag = member_ref
        .get("locationName")
        .and_then(|n| n.as_str())
        .unwrap_or("member");

    let mut arr = Vec::new();
    for child in &node.children {
        if child.tag == member_tag {
            arr.push(parse_shape_from_xml(child, member_shape, shapes)?);
        }
    }

    Ok(Value::Array(arr))
}

fn parse_map_from_xml(
    node: &XmlNode,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
) -> Result<Value> {
    let key_ref = shape_def.get("key").unwrap_or(&Value::Null);
    let value_ref = shape_def.get("value").unwrap_or(&Value::Null);

    let key_shape_name = key_ref.get("shape").and_then(|s| s.as_str()).unwrap_or("");
    let value_shape_name = value_ref
        .get("shape")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let key_shape = shapes.get(key_shape_name).unwrap_or(&Value::Null);
    let value_shape = shapes.get(value_shape_name).unwrap_or(&Value::Null);

    let key_tag = key_ref
        .get("locationName")
        .and_then(|n| n.as_str())
        .unwrap_or("key");
    let value_tag = value_ref
        .get("locationName")
        .and_then(|n| n.as_str())
        .unwrap_or("value");

    let flattened = shape_def
        .get("flattened")
        .and_then(|f| f.as_bool())
        .unwrap_or(false);

    let mut map = serde_json::Map::new();

    // For flattened maps, <entry> elements are direct children of the current node
    // For non-flattened maps, there's a wrapper, and <entry> children are inside it
    let entry_nodes: Vec<&XmlNode> = if flattened {
        // Flattened: the node children *are* the entries
        node.children.iter().collect()
    } else {
        // Non-flattened: node's children are <entry> elements
        node.children.iter().filter(|c| c.tag == "entry").collect()
    };

    for entry in entry_nodes {
        let mut key_val: Option<String> = None;
        let mut val_val: Option<Value> = None;

        for child in &entry.children {
            if child.tag == key_tag {
                key_val = Some(
                    parse_shape_from_xml(child, key_shape, shapes)?
                        .as_str()
                        .unwrap_or("")
                        .to_string(),
                );
            } else if child.tag == value_tag {
                val_val = Some(parse_shape_from_xml(child, value_shape, shapes)?);
            }
        }

        if let (Some(k), Some(v)) = (key_val, val_val) {
            map.insert(k, v);
        }
    }

    Ok(Value::Object(map))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Helper to build a minimal shapes map for testing
    fn sts_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "GetCallerIdentityRequest".to_string(),
            json!({
                "type": "structure",
                "members": {}
            }),
        );
        shapes.insert(
            "GetCallerIdentityResponse".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "UserId": { "shape": "userIdType" },
                    "Account": { "shape": "accountType" },
                    "Arn": { "shape": "arnType" }
                }
            }),
        );
        shapes.insert("userIdType".to_string(), json!({"type": "string"}));
        shapes.insert("accountType".to_string(), json!({"type": "string"}));
        shapes.insert("arnType".to_string(), json!({"type": "string"}));
        shapes
    }

    // ---------------------------------------------------------------
    // Feature 1: query-serializer-basic
    // ---------------------------------------------------------------

    #[test]
    fn query_serialize_basic_empty_input() {
        let shapes = sts_shapes();
        let input = json!({});
        let result = serialize_query_request(
            "GetCallerIdentity",
            "2011-06-15",
            &input,
            &shapes,
            "GetCallerIdentityRequest",
        )
        .unwrap();

        assert!(result.contains("Action=GetCallerIdentity"));
        assert!(result.contains("Version=2011-06-15"));
        assert_eq!(
            result,
            "Action=GetCallerIdentity&Version=2011-06-15"
        );
    }

    #[test]
    fn query_serialize_basic_flat_members() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "ListUsersRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "PathPrefix": { "shape": "pathPrefixType" },
                    "MaxItems": { "shape": "maxItemsType" },
                    "Marker": { "shape": "markerType" }
                }
            }),
        );
        shapes.insert("pathPrefixType".to_string(), json!({"type": "string"}));
        shapes.insert("maxItemsType".to_string(), json!({"type": "integer"}));
        shapes.insert("markerType".to_string(), json!({"type": "string"}));

        let input = json!({
            "PathPrefix": "/division_abc/",
            "MaxItems": 10
        });

        let result = serialize_query_request(
            "ListUsers",
            "2010-05-08",
            &input,
            &shapes,
            "ListUsersRequest",
        )
        .unwrap();

        assert!(result.contains("Action=ListUsers"));
        assert!(result.contains("Version=2010-05-08"));
        assert!(result.contains("PathPrefix=%2Fdivision_abc%2F"));
        assert!(result.contains("MaxItems=10"));
    }

    #[test]
    fn query_serialize_basic_boolean() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "DryRun": { "shape": "Boolean" }
                }
            }),
        );
        shapes.insert("Boolean".to_string(), json!({"type": "boolean"}));

        let input = json!({"DryRun": true});
        let result = serialize_query_request(
            "TestOp",
            "2020-01-01",
            &input,
            &shapes,
            "TestRequest",
        )
        .unwrap();

        assert!(result.contains("DryRun=true"));
    }

    #[test]
    fn query_serialize_basic_special_chars() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Name": { "shape": "String" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({"Name": "hello world&foo=bar"});
        let result = serialize_query_request(
            "TestOp",
            "2020-01-01",
            &input,
            &shapes,
            "TestRequest",
        )
        .unwrap();

        // Space -> %20, & -> %26, = -> %3D
        assert!(result.contains("Name=hello%20world%26foo%3Dbar"));
    }

    // ---------------------------------------------------------------
    // Feature 2: query-serializer-nested
    // ---------------------------------------------------------------

    #[test]
    fn query_serialize_nested_structure() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TagUserRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "UserName": { "shape": "String" },
                    "Tags": { "shape": "tagListType" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes.insert(
            "tagListType".to_string(),
            json!({
                "type": "list",
                "member": { "shape": "Tag" }
            }),
        );
        shapes.insert(
            "Tag".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Key": { "shape": "String" },
                    "Value": { "shape": "String" }
                }
            }),
        );

        let input = json!({
            "UserName": "testuser",
            "Tags": [
                {"Key": "Department", "Value": "Engineering"},
                {"Key": "Team", "Value": "Backend"}
            ]
        });

        let result = serialize_query_request(
            "TagUser",
            "2010-05-08",
            &input,
            &shapes,
            "TagUserRequest",
        )
        .unwrap();

        // Non-flattened list uses .member.N notation
        assert!(result.contains("Tags.member.1.Key=Department"));
        assert!(result.contains("Tags.member.1.Value=Engineering"));
        assert!(result.contains("Tags.member.2.Key=Team"));
        assert!(result.contains("Tags.member.2.Value=Backend"));
        assert!(result.contains("UserName=testuser"));
    }

    #[test]
    fn query_serialize_nested_deep_structure() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "OuterRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Config": { "shape": "ConfigType" }
                }
            }),
        );
        shapes.insert(
            "ConfigType".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Name": { "shape": "String" },
                    "Settings": { "shape": "SettingsType" }
                }
            }),
        );
        shapes.insert(
            "SettingsType".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Enabled": { "shape": "Boolean" },
                    "Level": { "shape": "Integer" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes.insert("Boolean".to_string(), json!({"type": "boolean"}));
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));

        let input = json!({
            "Config": {
                "Name": "prod",
                "Settings": {
                    "Enabled": true,
                    "Level": 5
                }
            }
        });

        let result = serialize_query_request(
            "ConfigOp",
            "2020-01-01",
            &input,
            &shapes,
            "OuterRequest",
        )
        .unwrap();

        assert!(result.contains("Config.Name=prod"));
        assert!(result.contains("Config.Settings.Enabled=true"));
        assert!(result.contains("Config.Settings.Level=5"));
    }

    // ---------------------------------------------------------------
    // Feature 3: query-serializer-lists
    // ---------------------------------------------------------------

    #[test]
    fn query_serialize_list_simple() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "InstanceId": { "shape": "InstanceIdList" }
                }
            }),
        );
        shapes.insert(
            "InstanceIdList".to_string(),
            json!({
                "type": "list",
                "member": { "shape": "String" }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "InstanceId": ["i-123", "i-456"]
        });

        let result = serialize_query_request(
            "TestOp",
            "2020-01-01",
            &input,
            &shapes,
            "TestRequest",
        )
        .unwrap();

        // Non-flattened list: Prefix.member.N
        assert!(result.contains("InstanceId.member.1=i-123"));
        assert!(result.contains("InstanceId.member.2=i-456"));
    }

    #[test]
    fn query_serialize_list_flattened() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "InstanceId": { "shape": "InstanceIdList" }
                }
            }),
        );
        shapes.insert(
            "InstanceIdList".to_string(),
            json!({
                "type": "list",
                "flattened": true,
                "member": { "shape": "String" }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "InstanceId": ["i-123", "i-456"]
        });

        let result = serialize_query_request(
            "TestOp",
            "2020-01-01",
            &input,
            &shapes,
            "TestRequest",
        )
        .unwrap();

        // Flattened list: Prefix.N (no .member.)
        assert!(result.contains("InstanceId.1=i-123"));
        assert!(result.contains("InstanceId.2=i-456"));
        assert!(!result.contains("member"));
    }

    #[test]
    fn query_serialize_list_empty() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Items": { "shape": "ItemList" }
                }
            }),
        );
        shapes.insert(
            "ItemList".to_string(),
            json!({
                "type": "list",
                "member": { "shape": "String" }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "Items": []
        });

        let result = serialize_query_request(
            "TestOp",
            "2020-01-01",
            &input,
            &shapes,
            "TestRequest",
        )
        .unwrap();

        // Empty list serializes as Items=
        assert!(result.contains("Items="));
    }

    #[test]
    fn query_serialize_list_with_nested_structures() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "DescribeRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Filter": { "shape": "FilterList" }
                }
            }),
        );
        shapes.insert(
            "FilterList".to_string(),
            json!({
                "type": "list",
                "member": { "shape": "Filter" }
            }),
        );
        shapes.insert(
            "Filter".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Name": { "shape": "String" },
                    "Values": { "shape": "ValueList" }
                }
            }),
        );
        shapes.insert(
            "ValueList".to_string(),
            json!({
                "type": "list",
                "member": { "shape": "String" }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "Filter": [
                {
                    "Name": "instance-id",
                    "Values": ["i-123", "i-456"]
                }
            ]
        });

        let result = serialize_query_request(
            "DescribeInstances",
            "2016-11-15",
            &input,
            &shapes,
            "DescribeRequest",
        )
        .unwrap();

        assert!(result.contains("Filter.member.1.Name=instance-id"));
        assert!(result.contains("Filter.member.1.Values.member.1=i-123"));
        assert!(result.contains("Filter.member.1.Values.member.2=i-456"));
    }

    #[test]
    fn query_serialize_list_map() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Tags": { "shape": "TagMap" }
                }
            }),
        );
        shapes.insert(
            "TagMap".to_string(),
            json!({
                "type": "map",
                "key": { "shape": "String" },
                "value": { "shape": "String" }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "Tags": {
                "env": "prod",
                "team": "backend"
            }
        });

        let result = serialize_query_request(
            "TestOp",
            "2020-01-01",
            &input,
            &shapes,
            "TestRequest",
        )
        .unwrap();

        // Non-flattened map: Prefix.entry.N.key=K&Prefix.entry.N.value=V
        assert!(result.contains("Tags.entry."));
        assert!(result.contains(".key="));
        assert!(result.contains(".value="));
    }

    // ---------------------------------------------------------------
    // Feature 4: query-response-parser
    // ---------------------------------------------------------------

    #[test]
    fn query_parse_response_get_caller_identity() {
        let shapes = sts_shapes();

        let xml = r#"<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
    <Arn>arn:aws:iam::123456789012:user/Alice</Arn>
    <UserId>AKIAI44QH8DHBEXAMPLE</UserId>
    <Account>123456789012</Account>
  </GetCallerIdentityResult>
  <ResponseMetadata>
    <RequestId>01234567-89ab-cdef-0123-456789abcdef</RequestId>
  </ResponseMetadata>
</GetCallerIdentityResponse>"#;

        let result = parse_query_response(
            xml,
            Some("GetCallerIdentityResult"),
            "GetCallerIdentityResponse",
            &shapes,
        )
        .unwrap();

        assert_eq!(
            result["Arn"].as_str().unwrap(),
            "arn:aws:iam::123456789012:user/Alice"
        );
        assert_eq!(
            result["UserId"].as_str().unwrap(),
            "AKIAI44QH8DHBEXAMPLE"
        );
        assert_eq!(result["Account"].as_str().unwrap(), "123456789012");
    }

    #[test]
    fn query_parse_response_with_list() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "ListUsersResponse".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Users": { "shape": "userListType" },
                    "IsTruncated": { "shape": "booleanType" }
                }
            }),
        );
        shapes.insert(
            "userListType".to_string(),
            json!({
                "type": "list",
                "member": { "shape": "User" }
            }),
        );
        shapes.insert(
            "User".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "UserName": { "shape": "String" },
                    "UserId": { "shape": "String" },
                    "Path": { "shape": "String" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes.insert("booleanType".to_string(), json!({"type": "boolean"}));

        let xml = r#"<ListUsersResponse>
  <ListUsersResult>
    <Users>
      <member>
        <UserName>Alice</UserName>
        <UserId>AIDAI44QH8DHBEXAMPLE1</UserId>
        <Path>/</Path>
      </member>
      <member>
        <UserName>Bob</UserName>
        <UserId>AIDAI44QH8DHBEXAMPLE2</UserId>
        <Path>/admins/</Path>
      </member>
    </Users>
    <IsTruncated>false</IsTruncated>
  </ListUsersResult>
</ListUsersResponse>"#;

        let result = parse_query_response(
            xml,
            Some("ListUsersResult"),
            "ListUsersResponse",
            &shapes,
        )
        .unwrap();

        let users = result["Users"].as_array().unwrap();
        assert_eq!(users.len(), 2);
        assert_eq!(users[0]["UserName"].as_str().unwrap(), "Alice");
        assert_eq!(users[1]["UserName"].as_str().unwrap(), "Bob");
        assert_eq!(result["IsTruncated"].as_bool().unwrap(), false);
    }

    #[test]
    fn query_parse_response_no_wrapper() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "SimpleResponse".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Name": { "shape": "String" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let xml = r#"<SimpleResponse>
  <Name>test</Name>
</SimpleResponse>"#;

        let result =
            parse_query_response(xml, None, "SimpleResponse", &shapes).unwrap();

        assert_eq!(result["Name"].as_str().unwrap(), "test");
    }

    #[test]
    fn query_parse_response_integer_and_boolean() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestResponse".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Count": { "shape": "Integer" },
                    "Active": { "shape": "Boolean" }
                }
            }),
        );
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));
        shapes.insert("Boolean".to_string(), json!({"type": "boolean"}));

        let xml = r#"<TestResponse>
  <TestResult>
    <Count>42</Count>
    <Active>true</Active>
  </TestResult>
</TestResponse>"#;

        let result =
            parse_query_response(xml, Some("TestResult"), "TestResponse", &shapes)
                .unwrap();

        assert_eq!(result["Count"].as_i64().unwrap(), 42);
        assert_eq!(result["Active"].as_bool().unwrap(), true);
    }

    // ---------------------------------------------------------------
    // Feature 5: query-error-parser
    // ---------------------------------------------------------------

    #[test]
    fn query_parse_error_standard() {
        let xml = r#"<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>InvalidParameterValue</Code>
    <Message>The parameter value is invalid.</Message>
  </Error>
  <RequestId>01234567-89ab-cdef-0123-456789abcdef</RequestId>
</ErrorResponse>"#;

        let (code, message) = parse_query_error(xml).unwrap();
        assert_eq!(code, "InvalidParameterValue");
        assert_eq!(message, "The parameter value is invalid.");
    }

    #[test]
    fn query_parse_error_access_denied() {
        let xml = r#"<ErrorResponse>
  <Error>
    <Code>AccessDeniedException</Code>
    <Message>User is not authorized to perform this action.</Message>
  </Error>
  <RequestId>abc-123</RequestId>
</ErrorResponse>"#;

        let (code, message) = parse_query_error(xml).unwrap();
        assert_eq!(code, "AccessDeniedException");
        assert_eq!(
            message,
            "User is not authorized to perform this action."
        );
    }

    #[test]
    fn query_parse_error_expired_token() {
        let xml = r#"<ErrorResponse>
  <Error>
    <Type>Sender</Type>
    <Code>ExpiredTokenException</Code>
    <Message>The security token included in the request is expired</Message>
  </Error>
  <RequestId>req-456</RequestId>
</ErrorResponse>"#;

        let (code, message) = parse_query_error(xml).unwrap();
        assert_eq!(code, "ExpiredTokenException");
        assert_eq!(
            message,
            "The security token included in the request is expired"
        );
    }

    #[test]
    fn query_parse_error_missing_code() {
        // Edge case: error with no Code element
        let xml = r#"<ErrorResponse>
  <Error>
    <Message>Something went wrong</Message>
  </Error>
</ErrorResponse>"#;

        let (code, message) = parse_query_error(xml).unwrap();
        assert_eq!(code, "");
        assert_eq!(message, "Something went wrong");
    }

    // ---------------------------------------------------------------
    // Feature: ec2-query-variant error parsing
    // ---------------------------------------------------------------

    #[test]
    fn ec2_query_parse_error_standard() {
        let xml = r#"<Response>
  <Errors>
    <Error>
      <Code>InvalidParameterValue</Code>
      <Message>The parameter value is invalid.</Message>
    </Error>
  </Errors>
  <RequestID>01234567-89ab-cdef-0123-456789abcdef</RequestID>
</Response>"#;

        let (code, message) = parse_ec2_error(xml).unwrap();
        assert_eq!(code, "InvalidParameterValue");
        assert_eq!(message, "The parameter value is invalid.");
    }

    #[test]
    fn ec2_query_parse_error_auth_failure() {
        let xml = r#"<Response>
  <Errors>
    <Error>
      <Code>AuthFailure</Code>
      <Message>AWS was not able to validate the provided access credentials</Message>
    </Error>
  </Errors>
  <RequestID>abc-123</RequestID>
</Response>"#;

        let (code, message) = parse_ec2_error(xml).unwrap();
        assert_eq!(code, "AuthFailure");
        assert_eq!(
            message,
            "AWS was not able to validate the provided access credentials"
        );
    }

    #[test]
    fn ec2_query_parse_error_dry_run() {
        let xml = r#"<Response>
  <Errors>
    <Error>
      <Code>DryRunOperation</Code>
      <Message>Request would have succeeded, but DryRun flag is set.</Message>
    </Error>
  </Errors>
  <RequestID>req-789</RequestID>
</Response>"#;

        let (code, message) = parse_ec2_error(xml).unwrap();
        assert_eq!(code, "DryRunOperation");
        assert_eq!(
            message,
            "Request would have succeeded, but DryRun flag is set."
        );
    }

    #[test]
    fn ec2_query_parse_error_missing_code() {
        let xml = r#"<Response>
  <Errors>
    <Error>
      <Message>Something went wrong</Message>
    </Error>
  </Errors>
</Response>"#;

        let (code, message) = parse_ec2_error(xml).unwrap();
        assert_eq!(code, "");
        assert_eq!(message, "Something went wrong");
    }

    // ---------------------------------------------------------------
    // Feature: EC2 protocol serializer
    // ---------------------------------------------------------------

    /// Build shapes that mirror the EC2 DescribeImages Owners parameter.
    ///
    /// DescribeImagesRequest.Owners (locationName "Owner") -> OwnerStringList (list, NOT flattened)
    ///   -> member (String, locationName "Owner")
    fn ec2_describe_images_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "DescribeImagesRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Owners": {
                        "shape": "OwnerStringList",
                        "locationName": "Owner"
                    },
                    "DryRun": {
                        "shape": "Boolean",
                        "locationName": "dryRun"
                    },
                    "Filters": {
                        "shape": "FilterList",
                        "locationName": "Filter"
                    }
                }
            }),
        );
        shapes.insert(
            "OwnerStringList".to_string(),
            json!({
                "type": "list",
                "member": {
                    "shape": "String",
                    "locationName": "Owner"
                }
            }),
        );
        shapes.insert(
            "FilterList".to_string(),
            json!({
                "type": "list",
                "member": {
                    "shape": "Filter",
                    "locationName": "Filter"
                }
            }),
        );
        shapes.insert(
            "Filter".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Name": { "shape": "String" },
                    "Values": {
                        "shape": "ValueStringList",
                        "locationName": "Value"
                    }
                }
            }),
        );
        shapes.insert(
            "ValueStringList".to_string(),
            json!({
                "type": "list",
                "member": {
                    "shape": "String",
                    "locationName": "item"
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes.insert("Boolean".to_string(), json!({"type": "boolean"}));
        shapes
    }

    #[test]
    fn ec2_serialize_owners_self() {
        // EC2 DescribeImages --owners self should produce Owner.1=self
        let shapes = ec2_describe_images_shapes();
        let input = json!({
            "Owners": ["self"]
        });

        let result = serialize_ec2_request(
            "DescribeImages",
            "2016-11-15",
            &input,
            &shapes,
            "DescribeImagesRequest",
        )
        .unwrap();

        // EC2 protocol: no .member. wrapper, locationName capitalized
        assert!(result.contains("Owner.1=self"), "Expected Owner.1=self in: {result}");
        // Must NOT contain .member.
        assert!(!result.contains(".member."), "Should not contain .member. in: {result}");
    }

    #[test]
    fn ec2_serialize_owners_multiple() {
        let shapes = ec2_describe_images_shapes();
        let input = json!({
            "Owners": ["self", "amazon"]
        });

        let result = serialize_ec2_request(
            "DescribeImages",
            "2016-11-15",
            &input,
            &shapes,
            "DescribeImagesRequest",
        )
        .unwrap();

        assert!(result.contains("Owner.1=self"), "Expected Owner.1=self in: {result}");
        assert!(result.contains("Owner.2=amazon"), "Expected Owner.2=amazon in: {result}");
        assert!(!result.contains(".member."), "Should not contain .member. in: {result}");
    }

    #[test]
    fn ec2_serialize_location_name_capitalized() {
        // EC2 capitalizes the first letter of locationName.
        // DryRun has locationName "dryRun" which should become "DryRun" in EC2.
        let shapes = ec2_describe_images_shapes();
        let input = json!({
            "DryRun": true
        });

        let result = serialize_ec2_request(
            "DescribeImages",
            "2016-11-15",
            &input,
            &shapes,
            "DescribeImagesRequest",
        )
        .unwrap();

        assert!(result.contains("DryRun=true"), "Expected DryRun=true in: {result}");
        // Must not contain lowercase dryRun
        assert!(!result.contains("dryRun=true"), "Should not contain dryRun=true in: {result}");
    }

    #[test]
    fn ec2_serialize_filters() {
        // EC2 DescribeImages with Filters
        let shapes = ec2_describe_images_shapes();
        let input = json!({
            "Filters": [
                {
                    "Name": "is-public",
                    "Values": ["false"]
                }
            ]
        });

        let result = serialize_ec2_request(
            "DescribeImages",
            "2016-11-15",
            &input,
            &shapes,
            "DescribeImagesRequest",
        )
        .unwrap();

        // EC2 protocol: lists always use Prefix.N (no .member.)
        // Filter.1.Name=is-public and Filter.1.Value.1=false
        assert!(result.contains("Filter.1.Name=is-public"), "Expected Filter.1.Name=is-public in: {result}");
        assert!(result.contains("Filter.1.Value.1=false"), "Expected Filter.1.Value.1=false in: {result}");
        assert!(!result.contains(".member."), "Should not contain .member. in: {result}");
    }

    #[test]
    fn ec2_vs_query_list_serialization() {
        // Verify that the same shapes produce different results for EC2 vs query protocol.
        // EC2: Owner.1=self  (no .member. wrapper)
        // Query: Owner.Owner.1=self  (non-flattened uses member locationName)
        let shapes = ec2_describe_images_shapes();
        let input = json!({
            "Owners": ["self"]
        });

        let ec2_result = serialize_ec2_request(
            "DescribeImages",
            "2016-11-15",
            &input,
            &shapes,
            "DescribeImagesRequest",
        )
        .unwrap();

        let query_result = serialize_query_request(
            "DescribeImages",
            "2016-11-15",
            &input,
            &shapes,
            "DescribeImagesRequest",
        )
        .unwrap();

        // EC2 uses Owner.1=self
        assert!(ec2_result.contains("Owner.1=self"), "EC2 should produce Owner.1=self: {ec2_result}");

        // Standard query uses Owner.Owner.1=self (locationName "Owner" on member)
        assert!(query_result.contains("Owner.Owner.1=self"), "Query should produce Owner.Owner.1=self: {query_result}");
    }

    #[test]
    fn ec2_serialize_describe_instances_with_queryname() {
        // Test that queryName takes precedence for EC2 serialization.
        // EC2 DescribeInstances uses queryName for some members.
        let mut shapes = HashMap::new();
        shapes.insert(
            "DescribeInstancesRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "InstanceIds": {
                        "shape": "InstanceIdStringList",
                        "locationName": "InstanceId"
                    }
                }
            }),
        );
        shapes.insert(
            "InstanceIdStringList".to_string(),
            json!({
                "type": "list",
                "member": {
                    "shape": "String",
                    "locationName": "InstanceId"
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "InstanceIds": ["i-12345", "i-67890"]
        });

        let result = serialize_ec2_request(
            "DescribeInstances",
            "2016-11-15",
            &input,
            &shapes,
            "DescribeInstancesRequest",
        )
        .unwrap();

        // EC2: InstanceId.1=i-12345, InstanceId.2=i-67890 (no .member.)
        assert!(result.contains("InstanceId.1=i-12345"), "Expected InstanceId.1=i-12345 in: {result}");
        assert!(result.contains("InstanceId.2=i-67890"), "Expected InstanceId.2=i-67890 in: {result}");
        assert!(!result.contains(".member."), "Should not contain .member. in: {result}");
    }

    #[test]
    fn capitalize_first_basic() {
        assert_eq!(capitalize_first("dryRun"), "DryRun");
        assert_eq!(capitalize_first("Owner"), "Owner");
        assert_eq!(capitalize_first("a"), "A");
        assert_eq!(capitalize_first(""), "");
        assert_eq!(capitalize_first("ABC"), "ABC");
    }
}
