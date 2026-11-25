use serde_json::{Map, Value};

#[derive(Clone, Debug, Default)]
pub struct Identifier {
    pub id: Option<String>,
    pub _id: Option<String>,
    pub name: Option<String>,
}

pub fn identifier_query(id: &Identifier) -> Option<Value> {
    if let Some(value) = id
        .id
        .as_ref()
        .filter(|s| !s.is_empty())
        .or_else(|| id._id.as_ref().filter(|s| !s.is_empty()))
    {
        Some(serde_json::json!({"_id": value}))
    } else {
        id.name
            .as_ref()
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::json!({"name":s}))
    }
}
pub fn find_document<'a>(docs: &'a [Value], id: &Identifier) -> Option<&'a Value> {
    if let Some(value) = id
        .id
        .as_ref()
        .filter(|s| !s.is_empty())
        .or_else(|| id._id.as_ref().filter(|s| !s.is_empty()))
    {
        docs.iter().find(|d| {
            d.get("id").and_then(Value::as_str) == Some(value)
                || d.get("_id").and_then(Value::as_str) == Some(value)
        })
    } else {
        id.name.as_ref().filter(|s| !s.is_empty()).and_then(|name| {
            docs.iter()
                .find(|d| d.get("name").and_then(Value::as_str) == Some(name))
        })
    }
}
pub fn where_to_server_query(filter: Option<&Map<String, Value>>) -> Value {
    Value::Object(
        filter
            .into_iter()
            .flat_map(|m| m.iter())
            .filter(|(k, v)| !k.contains('.') && (v.is_string() || v.is_boolean() || v.is_number()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    )
}
pub fn project_document(doc: &Value, fields: Option<&[String]>) -> Value {
    let Some(fields) = fields.filter(|f| !f.is_empty()) else {
        return doc.clone();
    };
    let Some(source) = doc.as_object() else {
        return doc.clone();
    };
    let mut out = Map::new();
    for field in fields.iter().map(String::as_str).chain(["_id", "name"]) {
        if let Some(v) = source.get(field) {
            out.insert(field.into(), v.clone());
            continue;
        }
        let path: Vec<_> = field.split('.').collect();
        let mut value = doc;
        let mut found = true;
        for segment in &path {
            match value.as_object().and_then(|o| o.get(*segment)) {
                Some(v) => value = v,
                None => {
                    found = false;
                    break;
                }
            }
        }
        if !found {
            continue;
        }
        let mut target = &mut out;
        for segment in &path[..path.len() - 1] {
            let v = target
                .entry(*segment)
                .or_insert_with(|| Value::Object(Map::new()));
            if !v.is_object() {
                *v = Value::Object(Map::new());
            }
            target = v.as_object_mut().unwrap();
        }
        target.insert(path[path.len() - 1].into(), value.clone());
    }
    Value::Object(out)
}
pub fn process_documents(
    docs: &[Value],
    filter: Option<&Map<String, Value>>,
    fields: Option<&[String]>,
    max_length: usize,
) -> Vec<Value> {
    let mut out: Vec<_> = docs
        .iter()
        .filter(|doc| {
            filter.is_none_or(|f| {
                f.iter().all(|(k, v)| match (doc.get(k), v) {
                    // JavaScript parses all JSON numbers as IEEE-754 doubles.
                    (Some(Value::Number(actual)), Value::Number(expected)) => {
                        actual.as_f64() == expected.as_f64()
                    }
                    (Some(actual), expected) if !expected.is_object() && !expected.is_array() => {
                        actual == expected
                    }
                    _ => false,
                })
            })
        })
        .map(|doc| project_document(doc, fields))
        .collect();
    if max_length > 0 && !out.is_empty() {
        let element_lengths: Vec<_> = out
            .iter()
            .map(|value| serde_json::to_vec(value).unwrap().len())
            .collect();
        let mut serialized_length =
            element_lengths
                .iter()
                .enumerate()
                .fold(2usize, |length, (index, element)| {
                    length
                        .saturating_add(*element)
                        .saturating_add(usize::from(index > 0))
                });
        let mut remaining = element_lengths.len();
        while !out.is_empty() && serialized_length > max_length {
            out.pop();
            remaining -= 1;
            if remaining == 0 {
                serialized_length = 2;
            } else {
                serialized_length =
                    serialized_length.saturating_sub(element_lengths[remaining].saturating_add(1));
            }
        }
    }
    out
}
fn js_string(v: Option<&Value>) -> String {
    match v {
        None => "undefined".into(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) => "null".into(),
        Some(Value::Object(_)) => "[object Object]".into(),
        Some(Value::Array(a)) => a
            .iter()
            .map(|v| {
                if v.is_null() {
                    String::new()
                } else {
                    js_string(Some(v))
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(v) => v.to_string(),
    }
}
pub fn summarize_packs(packs: &Value, document_name: Option<&str>) -> Vec<Value> {
    packs
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| {
            document_name.filter(|s| !s.is_empty()).is_none_or(|name| {
                p.get("type")
                    .filter(|v| !v.is_null())
                    .map(|v| js_string(Some(v)))
                    .unwrap_or_default()
                    .to_lowercase()
                    == name.to_lowercase()
            })
        })
        .map(|p| {
            let mut out = Map::new();
            for k in ["id", "label", "type"] {
                out.insert(
                    k.into(),
                    Value::String(
                        p.get(k)
                            .filter(|v| !v.is_null())
                            .map(|v| js_string(Some(v)))
                            .unwrap_or_default(),
                    ),
                );
            }
            for k in ["packageType", "packageName", "system"] {
                if let Some(v) = p.get(k) {
                    out.insert(k.into(), Value::String(js_string(Some(v))));
                }
            }
            Value::Object(out)
        })
        .collect()
}
pub fn validate_pack_access(packs: &Value, id: &str, kind: &str) -> Option<String> {
    match packs
        .as_array()
        .into_iter()
        .flatten()
        .find(|p| p.get("id").and_then(Value::as_str) == Some(id))
    {
        None => Some(format!(
            "Compendium pack \"{id}\" not found. Use get_compendiums to list the available packs."
        )),
        Some(p) if p.get("type").and_then(Value::as_str) != Some(kind) => Some(format!(
            "Compendium pack \"{id}\" holds {} documents, not {kind}.",
            js_string(p.get("type"))
        )),
        _ => None,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn projects_nested_and_literal_fields_without_mutation() {
        let doc =
            json!({"_id":"a","name":"A","system":{"hp":{"value":5,"max":10}},"system.hp.value":7});
        assert_eq!(
            project_document(
                &doc,
                Some(&["system.hp.value".into(), "system.hp.max".into()])
            ),
            json!({"_id":"a","name":"A","system.hp.value":7,"system":{"hp":{"max":10}}})
        );
        assert_eq!(doc["system"]["hp"]["value"], 5);
    }
    #[test]
    fn filters_before_projection_and_truncates_by_utf8_bytes() {
        let docs = vec![
            json!({"_id":"1","name":"é","kind":"x"}),
            json!({"_id":"2","name":"B","kind":"y"}),
        ];
        let f = json!({"kind":"x"}).as_object().unwrap().clone();
        assert_eq!(
            process_documents(&docs, Some(&f), Some(&["name".into()]), 25),
            vec![json!({"_id":"1","name":"é"})]
        );
        assert!(process_documents(&docs, Some(&f), None, 1).is_empty());
    }
    #[test]
    fn byte_limit_uses_exact_array_json_bytes_and_keeps_float_fidelity() {
        let docs = vec![
            json!({"text":"é\\n\\\"","value":1.0}),
            json!({"text":"🙂","value":-0.0}),
        ];
        let serialized = serde_json::to_vec(&docs).unwrap();
        assert_eq!(process_documents(&docs, None, None, serialized.len()), docs);
        let first = serde_json::to_vec(&docs[..1]).unwrap();
        assert_eq!(process_documents(&docs, None, None, first.len()), docs[..1]);
        assert_eq!(
            process_documents(&docs, None, None, first.len() - 1),
            Vec::<Value>::new()
        );
        assert_eq!(
            serde_json::to_vec(&process_documents(&docs, None, None, 0)).unwrap(),
            serialized
        );
    }

    #[test]
    fn byte_limit_matches_serialized_prefix_thresholds() {
        let docs = vec![
            json!({"s":"é\\n","f":1.0}),
            json!({"s":"🙂","f":-0.0}),
            json!({"s":"quote: \\\"","f":1e-200}),
        ];
        let full_len = serde_json::to_vec(&docs).unwrap().len();
        for max_length in 0..=full_len + 1 {
            let mut expected = docs.clone();
            while max_length > 0
                && !expected.is_empty()
                && serde_json::to_vec(&expected).unwrap().len() > max_length
            {
                expected.pop();
            }
            assert_eq!(
                process_documents(&docs, None, None, max_length),
                expected,
                "max_length={max_length}"
            );
        }
    }

    #[test]
    fn byte_limit_preserves_empty_array_and_tiny_limit_behavior() {
        assert!(process_documents(&[], None, None, 1).is_empty());
        assert!(process_documents(&[json!(1)], None, None, 1).is_empty());
        assert_eq!(
            serde_json::to_vec(&process_documents(&[], None, None, 1)).unwrap(),
            b"[]"
        );
    }

    #[test]
    fn numeric_filters_use_javascript_number_equality() {
        let docs = vec![
            json!({"n": 1}),
            json!({"n": 1.0}),
            json!({"n": "1"}),
            json!({"n": true}),
            json!({"n": 2}),
            json!({}),
        ];
        for filter in [json!({"n": 1}), json!({"n": 1.0})] {
            assert_eq!(
                process_documents(&docs, filter.as_object(), None, 0),
                docs[..2]
            );
        }
        let large = vec![
            json!({"n": 9007199254740992_u64}),
            json!({"n": 9007199254740993_u64}),
        ];
        let filter = json!({"n": 9007199254740992_u64});
        assert_eq!(
            process_documents(&large, filter.as_object(), None, 0),
            large
        );
        let zero = vec![json!({"n": 0}), json!({"n": -0.0})];
        assert_eq!(
            process_documents(&zero, json!({"n": -0.0}).as_object(), None, 0),
            zero
        );
    }
    #[test]
    fn identifier_query_and_lookup_use_upstream_precedence() {
        let id = Identifier {
            id: Some("a".into()),
            _id: Some("b".into()),
            name: Some("X".into()),
        };
        let docs = vec![json!({"id":"a"}), json!({"_id":"b"})];
        assert_eq!(identifier_query(&id), Some(json!({"_id":"a"})));
        assert_eq!(find_document(&docs, &id), Some(&docs[0]));
    }
    #[test]
    fn identifier_query_skips_empty_id_before_fallback() {
        let id = Identifier {
            id: Some(String::new()),
            _id: Some("fallback".into()),
            name: Some("Name".into()),
        };
        assert_eq!(identifier_query(&id), Some(json!({"_id":"fallback"})));
    }
    #[test]
    fn server_query_keeps_only_top_level_scalars() {
        let f = json!({"kind":"npc","active":true,"level":2,"system.hp":4,"none":null,"nested":{"a":1}});
        assert_eq!(
            where_to_server_query(f.as_object()),
            json!({"kind":"npc","active":true,"level":2})
        );
    }
    #[test]
    fn compendium_summary_and_validation() {
        let p = json!([{"id":"world.items","label":"Items","type":"Item","index":[1],"packageName":"world"}]);
        assert_eq!(
            summarize_packs(&p, Some("item")),
            vec![json!({"id":"world.items","label":"Items","type":"Item","packageName":"world"})]
        );
        assert_eq!(validate_pack_access(&p, "world.items", "Item"), None);
        assert!(validate_pack_access(&p, "world.items", "Actor")
            .unwrap()
            .contains("holds Item"));
        assert!(validate_pack_access(&p, "missing", "Item")
            .unwrap()
            .contains("not found"));
    }
}
