// REST-XML protocol serializer/parser
//
// Used by services like S3, Route53, CloudFront.
// - HTTP method from operation.http.method
// - URI template from operation.http.requestUri (e.g., /{Bucket}/{Key+})
// - Members with location=uri go into the URL path
// - Members with location=querystring go into query params
// - Members with location=header go into HTTP headers
// - Remaining non-streaming members serialized as XML body
// - If the shape has a `payload` member, only that member forms the body
//
// Response parsing:
// - Members with location=header come from response headers
// - Members with location=statusCode come from HTTP status
// - Body parsed as XML, using shape definitions for type-aware parsing
// - Error responses: <Error><Code>...</Code><Message>...</Message></Error>

use anyhow::{bail, Context, Result};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use quick_xml::events::Event;
use quick_xml::Reader;
use serde_json::Value;
use std::collections::HashMap;

/// The percent-encoding set for URI path segments.
const URI_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Serialized REST-XML request parts:
/// (resolved_uri, extra_headers, query_params, body_xml_string_or_none)
pub type RestXmlRequestParts = (
    String,
    Vec<(String, String)>,
    Vec<(String, String)>,
    Option<String>,
);

/// Build the REST-XML request: resolve URI template, extract headers/querystring,
/// serialize remaining members as XML body.
///
/// Returns (resolved_uri, extra_headers, query_params, body_xml_string_or_none)
pub fn serialize_rest_xml_request(
    uri_template: &str,
    input: &Value,
    input_shape_name: &str,
    shapes: &HashMap<String, Value>,
) -> Result<RestXmlRequestParts> {
    let shape_def = shapes
        .get(input_shape_name)
        .ok_or_else(|| anyhow::anyhow!("Input shape '{}' not found", input_shape_name))?;

    let members = shape_def
        .get("members")
        .and_then(|m| m.as_object())
        .cloned()
        .unwrap_or_default();

    let input_obj = input.as_object().cloned().unwrap_or_default();

    // Check if there is an explicit payload member
    let payload_member = shape_def.get("payload").and_then(|p| p.as_str());

    // Partition members by location
    let mut uri_params: HashMap<String, String> = HashMap::new();
    let mut query_params: Vec<(String, String)> = Vec::new();
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut body_members: Vec<(String, Value)> = Vec::new();

    for (member_name, member_def) in &members {
        let param_value = match input_obj.get(member_name) {
            Some(v) if !v.is_null() => v,
            _ => continue,
        };

        let location = member_def
            .get("location")
            .and_then(|l| l.as_str())
            .unwrap_or("");

        let location_name = member_def
            .get("locationName")
            .and_then(|n| n.as_str())
            .unwrap_or(member_name.as_str());

        match location {
            "uri" => {
                let str_val = value_to_string(param_value);
                uri_params.insert(location_name.to_string(), str_val);
            }
            "querystring" => {
                let str_val = value_to_string(param_value);
                query_params.push((location_name.to_string(), str_val));
            }
            "header" => {
                let str_val = value_to_string(param_value);
                headers.push((location_name.to_string(), str_val));
            }
            "headers" => {
                if let Some(obj) = param_value.as_object() {
                    for (k, v) in obj {
                        let header_name = format!("{}{}", location_name, k);
                        let header_val = value_to_string(v);
                        headers.push((header_name, header_val));
                    }
                }
            }
            _ => {
                // No location: candidate for body
                body_members.push((member_name.clone(), param_value.clone()));
            }
        }
    }

    // Resolve URI template
    let resolved_uri = render_uri_template(uri_template, &uri_params)?;

    // Build body
    let body = if let Some(payload_name) = payload_member {
        // Only the payload member goes in the body
        if let Some(param_value) = input_obj.get(payload_name) {
            if !param_value.is_null() {
                let member_def = members.get(payload_name);
                let member_shape_name = member_def
                    .and_then(|d| d.get("shape"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                let member_shape = shapes.get(member_shape_name);
                let member_type = member_shape
                    .and_then(|s| s.get("type"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("string");

                if member_type == "blob" || member_type == "string" {
                    // Streaming payload: raw value
                    Some(value_to_string(param_value))
                } else {
                    // Structure payload: serialize as XML
                    // The root element name comes from locationName on the member def,
                    // or the shape name
                    let root_name = member_def
                        .and_then(|d| d.get("locationName"))
                        .and_then(|n| n.as_str())
                        .unwrap_or(member_shape_name);

                    // Check for xmlNamespace on the member definition
                    let xml_ns = member_def.and_then(|d| d.get("xmlNamespace"));

                    let mut xml = String::new();
                    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
                    serialize_xml_value(
                        &mut xml,
                        param_value,
                        member_shape.unwrap_or(&Value::Null),
                        shapes,
                        root_name,
                        xml_ns,
                        0,
                    )?;
                    Some(xml)
                }
            } else {
                None
            }
        } else {
            None
        }
    } else if !body_members.is_empty() {
        // No payload: serialize all body members as XML under the input shape
        // The root element name comes from the shape's serialization name (xmlNamespace/locationName)
        // or the input shape name
        let root_name = shape_def
            .get("locationName")
            .and_then(|n| n.as_str())
            .unwrap_or(input_shape_name);

        let xml_ns = shape_def.get("xmlNamespace");

        // Build a JSON object of just the body members
        let mut body_obj = serde_json::Map::new();
        for (name, val) in &body_members {
            body_obj.insert(name.clone(), val.clone());
        }
        let body_value = Value::Object(body_obj);

        let mut xml = String::new();
        xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        serialize_xml_value(&mut xml, &body_value, shape_def, shapes, root_name, xml_ns, 0)?;
        Some(xml)
    } else {
        None
    };

    Ok((resolved_uri, headers, query_params, body))
}

// ---------------------------------------------------------------------------
// XML Serialization
// ---------------------------------------------------------------------------

/// Serialize a value as XML based on its shape definition.
fn serialize_xml_value(
    xml: &mut String,
    value: &Value,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
    element_name: &str,
    xml_ns: Option<&Value>,
    indent: usize,
) -> Result<()> {
    let shape_type = shape_def
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("string");

    match shape_type {
        "structure" => {
            serialize_xml_structure(xml, value, shape_def, shapes, element_name, xml_ns, indent)
        }
        "list" => serialize_xml_list(xml, value, shape_def, shapes, element_name, xml_ns, indent),
        "map" => serialize_xml_map(xml, value, shape_def, shapes, element_name, xml_ns, indent),
        "boolean" => {
            let bool_str = match value.as_bool() {
                Some(true) => "true",
                Some(false) => "false",
                None => value.as_str().unwrap_or("false"),
            };
            write_indent(xml, indent);
            write_open_tag(xml, element_name, xml_ns);
            xml.push_str(bool_str);
            write_close_tag(xml, element_name);
            Ok(())
        }
        _ => {
            // string, integer, long, float, double, timestamp, blob, etc.
            let text = value_to_string(value);
            write_indent(xml, indent);
            write_open_tag(xml, element_name, xml_ns);
            xml_escape_into(xml, &text);
            write_close_tag(xml, element_name);
            Ok(())
        }
    }
}

fn serialize_xml_structure(
    xml: &mut String,
    value: &Value,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
    element_name: &str,
    xml_ns: Option<&Value>,
    indent: usize,
) -> Result<()> {
    let obj = match value.as_object() {
        Some(o) => o,
        None => {
            // Empty structure
            write_indent(xml, indent);
            write_open_tag(xml, element_name, xml_ns);
            write_close_tag(xml, element_name);
            return Ok(());
        }
    };

    let members = shape_def
        .get("members")
        .and_then(|m| m.as_object())
        .cloned()
        .unwrap_or_default();

    write_indent(xml, indent);
    write_open_tag(xml, element_name, xml_ns);
    xml.push('\n');

    for (key, val) in obj {
        let member_def = match members.get(key) {
            Some(m) => m,
            None => continue,
        };

        // Skip members with a location (they go in URI/header/querystring)
        if member_def.get("location").is_some() {
            continue;
        }

        let member_shape_name = member_def
            .get("shape")
            .and_then(|s| s.as_str())
            .unwrap_or("");
        let member_shape = shapes.get(member_shape_name).unwrap_or(&Value::Null);

        // Determine element name: use locationName from member def, or member name
        let child_name = member_def
            .get("locationName")
            .and_then(|n| n.as_str())
            .unwrap_or(key.as_str());

        // Check xmlNamespace on member def
        let child_ns = member_def.get("xmlNamespace");

        let member_type = member_shape
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("string");

        // For flattened lists, we serialize each element directly (no wrapper)
        let is_flattened = member_type == "list"
            && member_shape
                .get("flattened")
                .and_then(|f| f.as_bool())
                .unwrap_or(false);

        if is_flattened {
            // Serialize each list element directly with the child_name
            if let Some(arr) = val.as_array() {
                let list_member_ref = member_shape.get("member").unwrap_or(&Value::Null);
                let element_shape_name = list_member_ref
                    .get("shape")
                    .and_then(|s| s.as_str())
                    .unwrap_or("");
                let element_shape = shapes.get(element_shape_name).unwrap_or(&Value::Null);

                // For flattened lists, use the member's locationName if available
                let flat_elem_name = list_member_ref
                    .get("locationName")
                    .and_then(|n| n.as_str())
                    .unwrap_or(child_name);

                for item in arr {
                    serialize_xml_value(
                        xml,
                        item,
                        element_shape,
                        shapes,
                        flat_elem_name,
                        None,
                        indent + 2,
                    )?;
                }
            }
        } else {
            serialize_xml_value(xml, val, member_shape, shapes, child_name, child_ns, indent + 2)?;
        }
    }

    write_indent(xml, indent);
    write_close_tag(xml, element_name);
    Ok(())
}

fn serialize_xml_list(
    xml: &mut String,
    value: &Value,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
    element_name: &str,
    xml_ns: Option<&Value>,
    indent: usize,
) -> Result<()> {
    let arr = match value.as_array() {
        Some(a) => a,
        None => return Ok(()),
    };

    let member_ref = shape_def.get("member").unwrap_or(&Value::Null);
    let member_shape_name = member_ref
        .get("shape")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let member_shape = shapes.get(member_shape_name).unwrap_or(&Value::Null);

    let flattened = shape_def
        .get("flattened")
        .and_then(|f| f.as_bool())
        .unwrap_or(false);

    if flattened {
        // Flattened: each item is a sibling element with element_name
        for item in arr {
            serialize_xml_value(xml, item, member_shape, shapes, element_name, xml_ns, indent)?;
        }
    } else {
        // Non-flattened: wrap items in element_name, each item uses member's locationName or "member"
        let child_name = member_ref
            .get("locationName")
            .and_then(|n| n.as_str())
            .unwrap_or("member");

        write_indent(xml, indent);
        write_open_tag(xml, element_name, xml_ns);
        xml.push('\n');

        for item in arr {
            serialize_xml_value(xml, item, member_shape, shapes, child_name, None, indent + 2)?;
        }

        write_indent(xml, indent);
        write_close_tag(xml, element_name);
    }

    Ok(())
}

fn serialize_xml_map(
    xml: &mut String,
    value: &Value,
    shape_def: &Value,
    shapes: &HashMap<String, Value>,
    element_name: &str,
    xml_ns: Option<&Value>,
    indent: usize,
) -> Result<()> {
    let obj = match value.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    let key_ref = shape_def.get("key").unwrap_or(&Value::Null);
    let value_ref = shape_def.get("value").unwrap_or(&Value::Null);

    let key_shape_name = key_ref
        .get("shape")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let value_shape_name = value_ref
        .get("shape")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let key_shape = shapes.get(key_shape_name).unwrap_or(&Value::Null);
    let value_shape = shapes.get(value_shape_name).unwrap_or(&Value::Null);

    let key_name = key_ref
        .get("locationName")
        .and_then(|n| n.as_str())
        .unwrap_or("key");
    let value_name = value_ref
        .get("locationName")
        .and_then(|n| n.as_str())
        .unwrap_or("value");

    let flattened = shape_def
        .get("flattened")
        .and_then(|f| f.as_bool())
        .unwrap_or(false);

    if flattened {
        // Flattened: each entry is a sibling with element_name
        for (k, v) in obj {
            write_indent(xml, indent);
            write_open_tag(xml, element_name, xml_ns);
            xml.push('\n');
            serialize_xml_value(
                xml,
                &Value::String(k.clone()),
                key_shape,
                shapes,
                key_name,
                None,
                indent + 2,
            )?;
            serialize_xml_value(xml, v, value_shape, shapes, value_name, None, indent + 2)?;
            write_indent(xml, indent);
            write_close_tag(xml, element_name);
        }
    } else {
        // Non-flattened: wrapper element, then <entry> for each
        write_indent(xml, indent);
        write_open_tag(xml, element_name, xml_ns);
        xml.push('\n');

        for (k, v) in obj {
            write_indent(xml, indent + 2);
            xml.push_str("<entry>\n");
            serialize_xml_value(
                xml,
                &Value::String(k.clone()),
                key_shape,
                shapes,
                key_name,
                None,
                indent + 4,
            )?;
            serialize_xml_value(xml, v, value_shape, shapes, value_name, None, indent + 4)?;
            write_indent(xml, indent + 2);
            xml.push_str("</entry>\n");
        }

        write_indent(xml, indent);
        write_close_tag(xml, element_name);
    }

    Ok(())
}

fn write_indent(xml: &mut String, indent: usize) {
    for _ in 0..indent {
        xml.push(' ');
    }
}

fn write_open_tag(xml: &mut String, name: &str, xml_ns: Option<&Value>) {
    xml.push('<');
    xml.push_str(name);
    if let Some(ns) = xml_ns {
        if let Some(ns_obj) = ns.as_object() {
            let uri = ns_obj.get("uri").and_then(|u| u.as_str()).unwrap_or("");
            let prefix = ns_obj.get("prefix").and_then(|p| p.as_str());
            if let Some(pfx) = prefix {
                xml.push_str(&format!(" xmlns:{}=\"{}\"", pfx, uri));
            } else {
                xml.push_str(&format!(" xmlns=\"{}\"", uri));
            }
        } else if let Some(uri) = ns.as_str() {
            xml.push_str(&format!(" xmlns=\"{}\"", uri));
        }
    }
    xml.push('>');
}

fn write_close_tag(xml: &mut String, name: &str) {
    xml.push_str("</");
    xml.push_str(name);
    xml.push_str(">\n");
}

fn xml_escape_into(xml: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            '&' => xml.push_str("&amp;"),
            '<' => xml.push_str("&lt;"),
            '>' => xml.push_str("&gt;"),
            '"' => xml.push_str("&quot;"),
            '\'' => xml.push_str("&apos;"),
            _ => xml.push(ch),
        }
    }
}

// ---------------------------------------------------------------------------
// URI Template rendering (same approach as rest_json)
// ---------------------------------------------------------------------------

fn render_uri_template(
    uri_template: &str,
    params: &HashMap<String, String>,
) -> Result<String> {
    let mut result = String::with_capacity(uri_template.len());
    let mut chars = uri_template.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '{' {
            let mut var_name = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                var_name.push(c);
            }

            let (lookup_name, greedy) = if var_name.ends_with('+') {
                (&var_name[..var_name.len() - 1], true)
            } else {
                (var_name.as_str(), false)
            };

            let value = params.get(lookup_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "URI template variable '{}' not found in input parameters",
                    lookup_name
                )
            })?;

            if greedy {
                let encoded: String = value
                    .split('/')
                    .map(|segment| utf8_percent_encode(segment, URI_ENCODE_SET).to_string())
                    .collect::<Vec<_>>()
                    .join("/");
                result.push_str(&encoded);
            } else {
                let encoded = utf8_percent_encode(value, URI_ENCODE_SET).to_string();
                result.push_str(&encoded);
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Null => String::new(),
        _ => value.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Response Parsing
// ---------------------------------------------------------------------------

/// Parse a REST-XML response body (plus headers and status) into serde_json::Value.
pub fn parse_rest_xml_response(
    body: &str,
    status_code: u16,
    response_headers: &HashMap<String, String>,
    output_shape_name: &str,
    shapes: &HashMap<String, Value>,
) -> Result<Value> {
    let shape_def = shapes
        .get(output_shape_name)
        .ok_or_else(|| anyhow::anyhow!("Output shape '{}' not found", output_shape_name))?;

    let members = shape_def
        .get("members")
        .and_then(|m| m.as_object())
        .cloned()
        .unwrap_or_default();

    let payload_member = shape_def.get("payload").and_then(|p| p.as_str());

    // Start building result
    let mut result = if let Some(payload_name) = payload_member {
        let mut map = serde_json::Map::new();
        if let Some(member_def) = members.get(payload_name) {
            let member_shape_name = member_def
                .get("shape")
                .and_then(|s| s.as_str())
                .unwrap_or("");
            let member_shape = shapes.get(member_shape_name);
            let member_type = member_shape
                .and_then(|s| s.get("type"))
                .and_then(|t| t.as_str())
                .unwrap_or("string");

            if member_type == "blob" || member_type == "string" {
                // Streaming payload: raw body
                map.insert(payload_name.to_string(), Value::String(body.to_string()));
            } else if !body.trim().is_empty() {
                // Parse XML body for the payload member
                let root = parse_xml_to_tree(body)?;
                let parsed =
                    parse_shape_from_xml(&root, member_shape.unwrap_or(&Value::Null), shapes)?;
                map.insert(payload_name.to_string(), parsed);
            }
        }
        map
    } else if !body.trim().is_empty() {
        // No payload: parse entire body as XML for the output shape
        let root = parse_xml_to_tree(body)?;
        let parsed = parse_shape_from_xml(&root, shape_def, shapes)?;
        match parsed {
            Value::Object(map) => map,
            _ => serde_json::Map::new(),
        }
    } else {
        serde_json::Map::new()
    };

    // Extract non-payload attributes (headers, statusCode)
    for (member_name, member_def) in &members {
        let location = member_def
            .get("location")
            .and_then(|l| l.as_str())
            .unwrap_or("");

        let location_name = member_def
            .get("locationName")
            .and_then(|n| n.as_str())
            .unwrap_or(member_name.as_str());

        match location {
            "statusCode" => {
                result.insert(
                    member_name.clone(),
                    Value::Number(serde_json::Number::from(status_code)),
                );
            }
            "header" => {
                let lower_location = location_name.to_lowercase();
                for (hdr_name, hdr_value) in response_headers {
                    if hdr_name.to_lowercase() == lower_location {
                        let member_shape_name = member_def
                            .get("shape")
                            .and_then(|s| s.as_str())
                            .unwrap_or("");
                        let member_shape = shapes.get(member_shape_name);
                        let parsed_value = parse_header_value(hdr_value, member_shape);
                        result.insert(member_name.clone(), parsed_value);
                        break;
                    }
                }
            }
            "headers" => {
                let prefix = location_name.to_lowercase();
                let mut header_map = serde_json::Map::new();
                for (hdr_name, hdr_value) in response_headers {
                    let lower_name = hdr_name.to_lowercase();
                    if lower_name.starts_with(&prefix) {
                        let key = hdr_name[prefix.len()..].to_string();
                        header_map.insert(key, Value::String(hdr_value.clone()));
                    }
                }
                if !header_map.is_empty() {
                    result.insert(member_name.clone(), Value::Object(header_map));
                }
            }
            _ => {
                // Body members already in result from XML parse
            }
        }
    }

    Ok(Value::Object(result))
}

/// Parse a header value into the appropriate JSON type based on the shape definition.
fn parse_header_value(value: &str, shape: Option<&Value>) -> Value {
    let shape_type = shape
        .and_then(|s| s.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("string");

    match shape_type {
        "integer" | "long" => value
            .parse::<i64>()
            .map(|n| Value::Number(serde_json::Number::from(n)))
            .unwrap_or_else(|_| Value::String(value.to_string())),
        "float" | "double" => value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string())),
        "boolean" => Value::Bool(value.eq_ignore_ascii_case("true")),
        _ => Value::String(value.to_string()),
    }
}

/// Parse a REST-XML error response. Returns (error_code, message).
///
/// Handles two error formats:
/// 1. S3-style: `<Error><Code>...</Code><Message>...</Message></Error>`
/// 2. Other REST-XML: `<ErrorResponse><Error><Code>...</Code><Message>...</Message></Error></ErrorResponse>`
pub fn parse_rest_xml_error(body: &str) -> Result<(String, String)> {
    let root = parse_xml_to_tree(body)?;

    // If root is <Error>, use it directly (S3 style)
    // If root is <ErrorResponse>, look for <Error> child
    let error_node = if root.tag == "Error" {
        root.clone()
    } else {
        find_child_element(&root, "Error")
            .unwrap_or(root.clone())
    };

    let code = find_child_text(&error_node, "Code").unwrap_or_default();
    let message = find_child_text(&error_node, "Message").unwrap_or_default();

    Ok((code, message))
}

// ---------------------------------------------------------------------------
// XML tree representation and parsing
// (Adapted from query.rs for REST-XML specifics)
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
                let node = stack
                    .pop()
                    .ok_or_else(|| anyhow::anyhow!("Unexpected end tag"))?;
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
    if let Some(pos) = tag.rfind('}') {
        return tag[pos + 1..].to_string();
    }
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
        _ => Ok(Value::String(node.text.clone().unwrap_or_default())),
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

    let name_to_children = build_name_to_children(node);

    for (member_name, member_def) in members {
        // Skip members with a location (they're handled separately for headers/status)
        if member_def.get("location").is_some() {
            continue;
        }

        let member_shape_name = member_def
            .get("shape")
            .and_then(|s| s.as_str())
            .unwrap_or("");
        let member_shape = match shapes.get(member_shape_name) {
            Some(s) => s,
            None => continue,
        };

        let xml_name = member_xml_name(member_def, member_shape, member_name);

        if let Some(child_nodes) = name_to_children.get(&xml_name) {
            let member_type = member_shape
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("string");

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
                let child = &child_nodes[0];
                let parsed = parse_shape_from_xml(child, member_shape, shapes)?;
                result.insert(member_name.clone(), parsed);
            }
        }
    }

    Ok(Value::Object(result))
}

/// Determine the XML element name for a structure member.
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

    let key_shape_name = key_ref
        .get("shape")
        .and_then(|s| s.as_str())
        .unwrap_or("");
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

    let entry_nodes: Vec<&XmlNode> = if flattened {
        node.children.iter().collect()
    } else {
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

    // ---------------------------------------------------------------
    // Helper: S3-like shapes for testing
    // ---------------------------------------------------------------

    fn s3_list_buckets_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "ListBucketsRequest".to_string(),
            json!({
                "type": "structure",
                "members": {}
            }),
        );
        shapes.insert(
            "ListBucketsOutput".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Buckets": { "shape": "Buckets" },
                    "Owner": { "shape": "Owner" }
                }
            }),
        );
        shapes.insert(
            "Buckets".to_string(),
            json!({
                "type": "list",
                "member": {
                    "shape": "Bucket",
                    "locationName": "Bucket"
                }
            }),
        );
        shapes.insert(
            "Bucket".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Name": { "shape": "BucketName" },
                    "CreationDate": { "shape": "CreationDate" }
                }
            }),
        );
        shapes.insert("BucketName".to_string(), json!({"type": "string"}));
        shapes.insert("CreationDate".to_string(), json!({"type": "timestamp"}));
        shapes.insert(
            "Owner".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "DisplayName": { "shape": "DisplayName" },
                    "ID": { "shape": "ID" }
                }
            }),
        );
        shapes.insert("DisplayName".to_string(), json!({"type": "string"}));
        shapes.insert("ID".to_string(), json!({"type": "string"}));
        shapes
    }

    fn s3_put_bucket_tagging_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "PutBucketTaggingRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Bucket": {
                        "shape": "BucketName",
                        "location": "uri",
                        "locationName": "Bucket"
                    },
                    "ContentMD5": {
                        "shape": "ContentMD5",
                        "location": "header",
                        "locationName": "Content-MD5"
                    },
                    "Tagging": {
                        "shape": "Tagging",
                        "locationName": "Tagging",
                        "xmlNamespace": {
                            "uri": "http://s3.amazonaws.com/doc/2006-03-01/"
                        }
                    }
                },
                "payload": "Tagging"
            }),
        );
        shapes.insert("BucketName".to_string(), json!({"type": "string"}));
        shapes.insert("ContentMD5".to_string(), json!({"type": "string"}));
        shapes.insert(
            "Tagging".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "TagSet": { "shape": "TagSet" }
                }
            }),
        );
        shapes.insert(
            "TagSet".to_string(),
            json!({
                "type": "list",
                "member": {
                    "shape": "Tag",
                    "locationName": "Tag"
                }
            }),
        );
        shapes.insert(
            "Tag".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Key": { "shape": "ObjectKey" },
                    "Value": { "shape": "TagValue" }
                }
            }),
        );
        shapes.insert("ObjectKey".to_string(), json!({"type": "string"}));
        shapes.insert("TagValue".to_string(), json!({"type": "string"}));
        shapes
    }

    fn route53_shapes() -> HashMap<String, Value> {
        let mut shapes = HashMap::new();
        shapes.insert(
            "ChangeResourceRecordSetsRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "HostedZoneId": {
                        "shape": "ResourceId",
                        "location": "uri",
                        "locationName": "Id"
                    },
                    "ChangeBatch": {
                        "shape": "ChangeBatch",
                        "locationName": "ChangeBatch",
                        "xmlNamespace": {
                            "uri": "https://route53.amazonaws.com/doc/2013-04-01/"
                        }
                    }
                },
                "payload": "ChangeBatch"
            }),
        );
        shapes.insert("ResourceId".to_string(), json!({"type": "string"}));
        shapes.insert(
            "ChangeBatch".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Comment": { "shape": "ResourceDescription" },
                    "Changes": { "shape": "Changes" }
                }
            }),
        );
        shapes.insert(
            "ResourceDescription".to_string(),
            json!({"type": "string"}),
        );
        shapes.insert(
            "Changes".to_string(),
            json!({
                "type": "list",
                "member": {
                    "shape": "Change",
                    "locationName": "Change"
                }
            }),
        );
        shapes.insert(
            "Change".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Action": { "shape": "ChangeAction" },
                    "ResourceRecordSet": { "shape": "ResourceRecordSet" }
                }
            }),
        );
        shapes.insert("ChangeAction".to_string(), json!({"type": "string"}));
        shapes.insert(
            "ResourceRecordSet".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Name": { "shape": "DNSName" },
                    "Type": { "shape": "RRType" }
                }
            }),
        );
        shapes.insert("DNSName".to_string(), json!({"type": "string"}));
        shapes.insert("RRType".to_string(), json!({"type": "string"}));
        shapes
    }

    // ---------------------------------------------------------------
    // Feature: rest-xml-protocol-serializer
    // ---------------------------------------------------------------

    #[test]
    fn rest_xml_serialize_empty_request() {
        let shapes = s3_list_buckets_shapes();
        let input = json!({});
        let (uri, headers, query, body) =
            serialize_rest_xml_request("/", &input, "ListBucketsRequest", &shapes).unwrap();

        assert_eq!(uri, "/");
        assert!(headers.is_empty());
        assert!(query.is_empty());
        assert!(body.is_none());
    }

    #[test]
    fn rest_xml_serialize_uri_template() {
        let shapes = s3_put_bucket_tagging_shapes();
        let input = json!({
            "Bucket": "my-bucket",
            "Tagging": {
                "TagSet": [
                    {"Key": "env", "Value": "prod"}
                ]
            }
        });

        let (uri, _headers, _query, body) = serialize_rest_xml_request(
            "/{Bucket}?tagging",
            &input,
            "PutBucketTaggingRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/my-bucket?tagging");
        assert!(body.is_some());
        let body_str = body.unwrap();
        assert!(body_str.contains("<Tagging"));
        assert!(body_str.contains("xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\""));
        assert!(body_str.contains("<TagSet>"));
        assert!(body_str.contains("<Tag>"));
        assert!(body_str.contains("<Key>env</Key>"));
        assert!(body_str.contains("<Value>prod</Value>"));
    }

    #[test]
    fn rest_xml_serialize_with_header() {
        let shapes = s3_put_bucket_tagging_shapes();
        let input = json!({
            "Bucket": "my-bucket",
            "ContentMD5": "abc123==",
            "Tagging": {
                "TagSet": []
            }
        });

        let (_uri, headers, _query, _body) = serialize_rest_xml_request(
            "/{Bucket}?tagging",
            &input,
            "PutBucketTaggingRequest",
            &shapes,
        )
        .unwrap();

        let header_map: HashMap<&str, &str> =
            headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert_eq!(header_map.get("Content-MD5"), Some(&"abc123=="));
    }

    #[test]
    fn rest_xml_serialize_payload_structure() {
        let shapes = route53_shapes();
        let input = json!({
            "HostedZoneId": "Z1234567890",
            "ChangeBatch": {
                "Comment": "Test change",
                "Changes": [
                    {
                        "Action": "CREATE",
                        "ResourceRecordSet": {
                            "Name": "example.com",
                            "Type": "A"
                        }
                    }
                ]
            }
        });

        let (uri, _headers, _query, body) = serialize_rest_xml_request(
            "/2013-04-01/hostedzone/{Id}/rrset",
            &input,
            "ChangeResourceRecordSetsRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/2013-04-01/hostedzone/Z1234567890/rrset");
        let body_str = body.unwrap();
        assert!(body_str.contains("<ChangeBatch"));
        assert!(body_str.contains("xmlns=\"https://route53.amazonaws.com/doc/2013-04-01/\""));
        assert!(body_str.contains("<Comment>Test change</Comment>"));
        assert!(body_str.contains("<Changes>"));
        assert!(body_str.contains("<Change>"));
        assert!(body_str.contains("<Action>CREATE</Action>"));
        assert!(body_str.contains("<Name>example.com</Name>"));
        assert!(body_str.contains("<Type>A</Type>"));
    }

    #[test]
    fn rest_xml_serialize_xml_escaping() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Data": {
                        "shape": "DataShape",
                        "locationName": "Data"
                    }
                },
                "payload": "Data"
            }),
        );
        shapes.insert(
            "DataShape".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Name": { "shape": "String" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "Data": {
                "Name": "foo & <bar>"
            }
        });

        let (_uri, _headers, _query, body) =
            serialize_rest_xml_request("/test", &input, "TestRequest", &shapes).unwrap();

        let body_str = body.unwrap();
        assert!(body_str.contains("<Name>foo &amp; &lt;bar&gt;</Name>"));
    }

    #[test]
    fn rest_xml_serialize_boolean_and_integer() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Config": {
                        "shape": "ConfigShape",
                        "locationName": "Config"
                    }
                },
                "payload": "Config"
            }),
        );
        shapes.insert(
            "ConfigShape".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Enabled": { "shape": "Boolean" },
                    "Count": { "shape": "Integer" }
                }
            }),
        );
        shapes.insert("Boolean".to_string(), json!({"type": "boolean"}));
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));

        let input = json!({
            "Config": {
                "Enabled": true,
                "Count": 42
            }
        });

        let (_uri, _headers, _query, body) =
            serialize_rest_xml_request("/test", &input, "TestRequest", &shapes).unwrap();

        let body_str = body.unwrap();
        assert!(body_str.contains("<Enabled>true</Enabled>"));
        assert!(body_str.contains("<Count>42</Count>"));
    }

    #[test]
    fn rest_xml_serialize_map() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Body": {
                        "shape": "BodyShape",
                        "locationName": "Body"
                    }
                },
                "payload": "Body"
            }),
        );
        shapes.insert(
            "BodyShape".to_string(),
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
            "Body": {
                "Tags": {
                    "env": "prod"
                }
            }
        });

        let (_uri, _headers, _query, body) =
            serialize_rest_xml_request("/test", &input, "TestRequest", &shapes).unwrap();

        let body_str = body.unwrap();
        assert!(body_str.contains("<Tags>"));
        assert!(body_str.contains("<entry>"));
        assert!(body_str.contains("<key>env</key>"));
        assert!(body_str.contains("<value>prod</value>"));
        assert!(body_str.contains("</entry>"));
        assert!(body_str.contains("</Tags>"));
    }

    #[test]
    fn rest_xml_serialize_flattened_list() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Body": {
                        "shape": "BodyShape",
                        "locationName": "Body"
                    }
                },
                "payload": "Body"
            }),
        );
        shapes.insert(
            "BodyShape".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Items": { "shape": "ItemList", "locationName": "Items" }
                }
            }),
        );
        shapes.insert(
            "ItemList".to_string(),
            json!({
                "type": "list",
                "flattened": true,
                "member": {
                    "shape": "String",
                    "locationName": "Item"
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "Body": {
                "Items": ["one", "two", "three"]
            }
        });

        let (_uri, _headers, _query, body) =
            serialize_rest_xml_request("/test", &input, "TestRequest", &shapes).unwrap();

        let body_str = body.unwrap();
        // Flattened: each element appears directly without wrapper
        assert!(body_str.contains("<Item>one</Item>"));
        assert!(body_str.contains("<Item>two</Item>"));
        assert!(body_str.contains("<Item>three</Item>"));
        // Should NOT have a wrapping <Items> element
        assert!(!body_str.contains("<Items>"));
    }

    #[test]
    fn rest_xml_serialize_greedy_uri_label() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "GetObjectRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Bucket": {
                        "shape": "String",
                        "location": "uri",
                        "locationName": "Bucket"
                    },
                    "Key": {
                        "shape": "String",
                        "location": "uri",
                        "locationName": "Key"
                    }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let input = json!({
            "Bucket": "my-bucket",
            "Key": "path/to/my file.txt"
        });

        let (uri, _headers, _query, _body) = serialize_rest_xml_request(
            "/{Bucket}/{Key+}",
            &input,
            "GetObjectRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/my-bucket/path/to/my%20file.txt");
    }

    #[test]
    fn rest_xml_serialize_querystring() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "ListObjectsRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Bucket": {
                        "shape": "String",
                        "location": "uri",
                        "locationName": "Bucket"
                    },
                    "Prefix": {
                        "shape": "String",
                        "location": "querystring",
                        "locationName": "prefix"
                    },
                    "MaxKeys": {
                        "shape": "Integer",
                        "location": "querystring",
                        "locationName": "max-keys"
                    }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));

        let input = json!({
            "Bucket": "my-bucket",
            "Prefix": "docs/",
            "MaxKeys": 100
        });

        let (uri, _headers, query, body) = serialize_rest_xml_request(
            "/{Bucket}",
            &input,
            "ListObjectsRequest",
            &shapes,
        )
        .unwrap();

        assert_eq!(uri, "/my-bucket");
        assert!(body.is_none());

        let query_map: HashMap<&str, &str> =
            query.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert_eq!(query_map.get("prefix"), Some(&"docs/"));
        assert_eq!(query_map.get("max-keys"), Some(&"100"));
    }

    #[test]
    fn rest_xml_serialize_body_without_payload() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestRequest".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Name": { "shape": "String" },
                    "Count": { "shape": "Integer" }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));

        let input = json!({
            "Name": "test",
            "Count": 5
        });

        let (_uri, _headers, _query, body) =
            serialize_rest_xml_request("/test", &input, "TestRequest", &shapes).unwrap();

        let body_str = body.unwrap();
        assert!(body_str.contains("<Name>test</Name>"));
        assert!(body_str.contains("<Count>5</Count>"));
    }

    // ---------------------------------------------------------------
    // Feature: rest-xml-protocol-parser
    // ---------------------------------------------------------------

    #[test]
    fn rest_xml_parse_s3_list_buckets() {
        let shapes = s3_list_buckets_shapes();

        let xml = r#"<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <DisplayName>webfile</DisplayName>
    <ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
  </Owner>
  <Buckets>
    <Bucket>
      <Name>quotes</Name>
      <CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
    </Bucket>
    <Bucket>
      <Name>samples</Name>
      <CreationDate>2006-02-03T16:41:58.000Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>"#;

        let result = parse_rest_xml_response(
            xml,
            200,
            &HashMap::new(),
            "ListBucketsOutput",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["Owner"]["DisplayName"].as_str().unwrap(), "webfile");
        assert_eq!(
            result["Owner"]["ID"].as_str().unwrap(),
            "bcaf1ffd86f461ca5fb16fd081034f"
        );

        let buckets = result["Buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0]["Name"].as_str().unwrap(), "quotes");
        assert_eq!(
            buckets[0]["CreationDate"].as_str().unwrap(),
            "2006-02-03T16:45:09+00:00"
        );
        assert_eq!(buckets[1]["Name"].as_str().unwrap(), "samples");
    }

    #[test]
    fn rest_xml_parse_with_headers_and_status() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "HeadObjectOutput".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "StatusCode": {
                        "shape": "Integer",
                        "location": "statusCode"
                    },
                    "ContentLength": {
                        "shape": "Long",
                        "location": "header",
                        "locationName": "Content-Length"
                    },
                    "ContentType": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "Content-Type"
                    },
                    "ETag": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "ETag"
                    }
                }
            }),
        );
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));
        shapes.insert("Long".to_string(), json!({"type": "long"}));
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let mut headers = HashMap::new();
        headers.insert("Content-Length".to_string(), "12345".to_string());
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("ETag".to_string(), "\"abc123\"".to_string());

        let result = parse_rest_xml_response(
            "",
            200,
            &headers,
            "HeadObjectOutput",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["StatusCode"].as_u64().unwrap(), 200);
        assert_eq!(result["ContentLength"].as_i64().unwrap(), 12345);
        assert_eq!(
            result["ContentType"].as_str().unwrap(),
            "application/json"
        );
        assert_eq!(result["ETag"].as_str().unwrap(), "\"abc123\"");
    }

    #[test]
    fn rest_xml_parse_with_payload_member() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "GetObjectOutput".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Body": {
                        "shape": "Blob"
                    },
                    "ContentType": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "Content-Type"
                    }
                },
                "payload": "Body"
            }),
        );
        shapes.insert("Blob".to_string(), json!({"type": "blob"}));
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "text/plain".to_string());

        let result = parse_rest_xml_response(
            "Hello, world!",
            200,
            &headers,
            "GetObjectOutput",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["Body"].as_str().unwrap(), "Hello, world!");
        assert_eq!(result["ContentType"].as_str().unwrap(), "text/plain");
    }

    #[test]
    fn rest_xml_parse_empty_body() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "DeleteObjectOutput".to_string(),
            json!({
                "type": "structure",
                "members": {}
            }),
        );

        let result = parse_rest_xml_response(
            "",
            204,
            &HashMap::new(),
            "DeleteObjectOutput",
            &shapes,
        )
        .unwrap();

        assert!(result.is_object());
    }

    #[test]
    fn rest_xml_parse_integer_and_boolean() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestOutput".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "Count": { "shape": "Integer" },
                    "Active": { "shape": "Boolean" },
                    "Rate": { "shape": "Double" }
                }
            }),
        );
        shapes.insert("Integer".to_string(), json!({"type": "integer"}));
        shapes.insert("Boolean".to_string(), json!({"type": "boolean"}));
        shapes.insert("Double".to_string(), json!({"type": "double"}));

        let xml = r#"<TestResult>
  <Count>42</Count>
  <Active>true</Active>
  <Rate>3.14</Rate>
</TestResult>"#;

        let result = parse_rest_xml_response(
            xml,
            200,
            &HashMap::new(),
            "TestOutput",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["Count"].as_i64().unwrap(), 42);
        assert_eq!(result["Active"].as_bool().unwrap(), true);
        assert!((result["Rate"].as_f64().unwrap() - 3.14).abs() < 0.001);
    }

    #[test]
    fn rest_xml_parse_flattened_list() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestOutput".to_string(),
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
                "flattened": true,
                "member": {
                    "shape": "String",
                    "locationName": "Item"
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let xml = r#"<TestResult>
  <Item>one</Item>
  <Item>two</Item>
  <Item>three</Item>
</TestResult>"#;

        let result = parse_rest_xml_response(
            xml,
            200,
            &HashMap::new(),
            "TestOutput",
            &shapes,
        )
        .unwrap();

        let items = result["Items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].as_str().unwrap(), "one");
        assert_eq!(items[1].as_str().unwrap(), "two");
        assert_eq!(items[2].as_str().unwrap(), "three");
    }

    #[test]
    fn rest_xml_parse_with_location_name() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestOutput".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "DisplayName": {
                        "shape": "String",
                        "locationName": "Name"
                    }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let xml = r#"<TestResult>
  <Name>hello</Name>
</TestResult>"#;

        let result = parse_rest_xml_response(
            xml,
            200,
            &HashMap::new(),
            "TestOutput",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["DisplayName"].as_str().unwrap(), "hello");
    }

    #[test]
    fn rest_xml_parse_map_response() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestOutput".to_string(),
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

        let xml = r#"<TestResult>
  <Tags>
    <entry>
      <key>env</key>
      <value>prod</value>
    </entry>
    <entry>
      <key>team</key>
      <value>backend</value>
    </entry>
  </Tags>
</TestResult>"#;

        let result = parse_rest_xml_response(
            xml,
            200,
            &HashMap::new(),
            "TestOutput",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["Tags"]["env"].as_str().unwrap(), "prod");
        assert_eq!(result["Tags"]["team"].as_str().unwrap(), "backend");
    }

    #[test]
    fn rest_xml_parse_case_insensitive_headers() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "TestOutput".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "RequestId": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "x-amz-request-id"
                    }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let mut headers = HashMap::new();
        headers.insert("X-Amz-Request-Id".to_string(), "abc-123".to_string());

        let result = parse_rest_xml_response(
            "",
            200,
            &headers,
            "TestOutput",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["RequestId"].as_str().unwrap(), "abc-123");
    }

    // ---------------------------------------------------------------
    // Error parsing
    // ---------------------------------------------------------------

    #[test]
    fn rest_xml_parse_error_s3_style() {
        let xml = r#"<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
  <BucketName>my-bucket</BucketName>
  <RequestId>abc-123</RequestId>
</Error>"#;

        let (code, message) = parse_rest_xml_error(xml).unwrap();
        assert_eq!(code, "NoSuchBucket");
        assert_eq!(message, "The specified bucket does not exist");
    }

    #[test]
    fn rest_xml_parse_error_standard_format() {
        let xml = r#"<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>InvalidInput</Code>
    <Message>Invalid resource type: foo</Message>
  </Error>
  <RequestId>req-456</RequestId>
</ErrorResponse>"#;

        let (code, message) = parse_rest_xml_error(xml).unwrap();
        assert_eq!(code, "InvalidInput");
        assert_eq!(message, "Invalid resource type: foo");
    }

    #[test]
    fn rest_xml_parse_error_access_denied() {
        let xml = r#"<Error>
  <Code>AccessDenied</Code>
  <Message>Access Denied</Message>
</Error>"#;

        let (code, message) = parse_rest_xml_error(xml).unwrap();
        assert_eq!(code, "AccessDenied");
        assert_eq!(message, "Access Denied");
    }

    #[test]
    fn rest_xml_parse_error_missing_fields() {
        let xml = r#"<Error>
  <Message>Something went wrong</Message>
</Error>"#;

        let (code, message) = parse_rest_xml_error(xml).unwrap();
        assert_eq!(code, "");
        assert_eq!(message, "Something went wrong");
    }

    #[test]
    fn rest_xml_parse_payload_xml_structure() {
        let mut shapes = HashMap::new();
        shapes.insert(
            "GetBucketTaggingOutput".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "TagSet": { "shape": "TagSet" }
                },
                "payload": "TagSet"
            }),
        );
        shapes.insert(
            "TagSet".to_string(),
            json!({
                "type": "list",
                "member": {
                    "shape": "Tag",
                    "locationName": "Tag"
                }
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
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let xml = r#"<TagSet>
  <Tag>
    <Key>env</Key>
    <Value>prod</Value>
  </Tag>
  <Tag>
    <Key>team</Key>
    <Value>backend</Value>
  </Tag>
</TagSet>"#;

        let result = parse_rest_xml_response(
            xml,
            200,
            &HashMap::new(),
            "GetBucketTaggingOutput",
            &shapes,
        )
        .unwrap();

        let tag_set = result["TagSet"].as_array().unwrap();
        assert_eq!(tag_set.len(), 2);
        assert_eq!(tag_set[0]["Key"].as_str().unwrap(), "env");
        assert_eq!(tag_set[0]["Value"].as_str().unwrap(), "prod");
        assert_eq!(tag_set[1]["Key"].as_str().unwrap(), "team");
        assert_eq!(tag_set[1]["Value"].as_str().unwrap(), "backend");
    }

    // ---------------------------------------------------------------
    // HEAD response / empty body tests (for HeadBucket etc.)
    // ---------------------------------------------------------------

    #[test]
    fn rest_xml_parse_head_bucket_success_empty_body() {
        // HeadBucket returns 200 with headers only, no body.
        // The output shape has all header-located members.
        let mut shapes = HashMap::new();
        shapes.insert(
            "HeadBucketOutput".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "BucketRegion": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "x-amz-bucket-region"
                    },
                    "AccessPointAlias": {
                        "shape": "Boolean",
                        "location": "header",
                        "locationName": "x-amz-access-point-alias"
                    }
                }
            }),
        );
        shapes.insert("String".to_string(), json!({"type": "string"}));
        shapes.insert("Boolean".to_string(), json!({"type": "boolean"}));

        let mut headers = HashMap::new();
        headers.insert("x-amz-bucket-region".to_string(), "us-east-1".to_string());
        headers.insert("x-amz-access-point-alias".to_string(), "false".to_string());

        let result = parse_rest_xml_response(
            "",
            200,
            &headers,
            "HeadBucketOutput",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["BucketRegion"].as_str().unwrap(), "us-east-1");
        assert_eq!(result["AccessPointAlias"].as_bool().unwrap(), false);
    }

    #[test]
    fn rest_xml_parse_head_object_success_no_body() {
        // HeadObject returns 200 with content-length, content-type, etag headers, no body
        let mut shapes = HashMap::new();
        shapes.insert(
            "HeadObjectOutput".to_string(),
            json!({
                "type": "structure",
                "members": {
                    "ContentLength": {
                        "shape": "Long",
                        "location": "header",
                        "locationName": "Content-Length"
                    },
                    "ContentType": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "Content-Type"
                    },
                    "ETag": {
                        "shape": "String",
                        "location": "header",
                        "locationName": "ETag"
                    }
                }
            }),
        );
        shapes.insert("Long".to_string(), json!({"type": "long"}));
        shapes.insert("String".to_string(), json!({"type": "string"}));

        let mut headers = HashMap::new();
        headers.insert("Content-Length".to_string(), "12345".to_string());
        headers.insert("Content-Type".to_string(), "text/plain".to_string());
        headers.insert("ETag".to_string(), "\"abcdef\"".to_string());

        let result = parse_rest_xml_response(
            "",
            200,
            &headers,
            "HeadObjectOutput",
            &shapes,
        )
        .unwrap();

        assert_eq!(result["ContentLength"].as_i64().unwrap(), 12345);
        assert_eq!(result["ContentType"].as_str().unwrap(), "text/plain");
        assert_eq!(result["ETag"].as_str().unwrap(), "\"abcdef\"");
    }

    #[test]
    fn rest_xml_parse_error_empty_body() {
        // parse_rest_xml_error should fail gracefully on empty body
        // (the caller in driver.rs handles this case by using error_from_status_code)
        let result = parse_rest_xml_error("");
        assert!(result.is_err(), "Parsing empty body as XML error should fail");
    }

    #[test]
    fn rest_xml_parse_error_whitespace_only_body() {
        let result = parse_rest_xml_error("   \n  ");
        assert!(result.is_err(), "Parsing whitespace body as XML error should fail");
    }
}
