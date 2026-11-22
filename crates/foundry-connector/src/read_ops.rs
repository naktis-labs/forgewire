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
    while max_length > 0 && !out.is_empty() && serde_json::to_vec(&out).unwrap().len() > max_length
    {
        out.pop();
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
