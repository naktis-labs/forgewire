use anyhow::{anyhow, bail, Result};
use foundry_connector::{read_ops, FoundryClient, FoundryCredential};
use serde_json::{json, Map, Value};

const TYPES: [(&str, &str, &str); 13] = [
    ("actor", "actors", "actors"),
    ("item", "items", "items"),
    ("folder", "folders", "folders"),
    ("user", "users", "users"),
    ("scene", "scenes", "scenes"),
    ("journal", "journals", "journal"),
    ("macro", "macros", "macros"),
    ("card", "cards", "cards"),
    ("playlist", "playlists", "playlists"),
    ("table", "tables", "tables"),
    ("combat", "combats", "combats"),
    ("message", "messages", "messages"),
    ("setting", "settings", "settings"),
];

pub fn definitions() -> Vec<Value> {
    let mut tools = Vec::new();
    for (singular, plural, _) in TYPES {
        let fields = json!({"type":"array", "items":{"type":"string"}, "description":"Fields to return; supports dot notation. _id and name are always included."});
        let mut list = json!({"name":format!("get_{plural}"),"description":format!("Get all {plural} from FoundryVTT"),"inputSchema":{"type":"object","properties":{"max_length":{"type":"integer","description":"Maximum JSON response size in bytes; zero means unlimited."},"requested_fields":fields,"where":{"type":"object","additionalProperties":true}},"required":[]}});
        let mut get = json!({"name":format!("get_{singular}"),"description":format!("Get a {singular} by id, _id, or name"),"inputSchema":{"type":"object","properties":{"id":{"type":"string"},"_id":{"type":"string"},"name":{"type":"string"},"requested_fields":fields},"required":[]}});
        if document_type(singular).is_some() {
            let property = json!({"type":"string","description":"Compendium pack ID; list packs with get_compendiums."});
            list["inputSchema"]["properties"]["pack"] = property.clone();
            get["inputSchema"]["properties"]["pack"] = property;
        }
        tools.extend([list, get]);
    }
    tools.push(json!({"name":"get_compendiums","description":"List compendium packs, optionally filtered by document type.","inputSchema":{"type":"object","properties":{"type":{"type":"string"}},"required":[]}}));
    tools
}

fn document_type(singular: &str) -> Option<&'static str> {
    match singular {
        "actor" => Some("Actor"),
        "item" => Some("Item"),
        "scene" => Some("Scene"),
        "journal" => Some("JournalEntry"),
        "macro" => Some("Macro"),
        "card" => Some("Cards"),
        "playlist" => Some("Playlist"),
        "table" => Some("RollTable"),
        _ => None,
    }
}

pub fn recognizes(name: &str) -> bool {
    name == "get_compendiums"
        || TYPES.iter().any(|(single, plural, _)| {
            name == format!("get_{single}") || name == format!("get_{plural}")
        })
}

pub struct ReadRequest {
    pub collection: &'static str,
    single: bool,
    identifier: read_ops::Identifier,
    filter: Option<Map<String, Value>>,
    fields: Option<Vec<String>>,
    max_length: usize,
    pack: Option<String>,
    kind: Option<&'static str>,
}

pub fn parse(name: &str, args: &Value) -> Result<ReadRequest> {
    let (single, collection, kind) = TYPES
        .iter()
        .find_map(|(one, many, collection)| {
            if name == format!("get_{one}") {
                Some((true, *collection, document_type(one)))
            } else if name == format!("get_{many}") {
                Some((false, *collection, document_type(one)))
            } else {
                None
            }
        })
        .ok_or_else(|| anyhow!("unknown read tool"))?;
    let args = args
        .as_object()
        .ok_or_else(|| anyhow!("arguments must be an object"))?;
    let string = |key: &str| -> Result<Option<String>> {
        match args.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) => Ok(Some(s.clone())),
            _ => bail!("{key} must be a string"),
        }
    };
    let pack = string("pack")?;
    if pack.as_ref().is_some_and(|p| p.trim().is_empty()) {
        bail!("pack must not be empty");
    }
    if pack.is_some() && kind.is_none() {
        bail!("this document type cannot be stored in a compendium");
    }
    let identifier = read_ops::Identifier {
        id: string("id")?,
        _id: string("_id")?,
        name: string("name")?,
    };
    if single
        && [
            identifier.id.as_deref(),
            identifier._id.as_deref(),
            identifier.name.as_deref(),
        ]
        .iter()
        .all(|v| v.is_none_or(str::is_empty))
    {
        bail!("id, _id, or name is required");
    }
    let fields = match args.get("requested_fields") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => Some(
            values
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| anyhow!("requested_fields must contain strings"))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        _ => bail!("requested_fields must be an array of strings"),
    };
    let filter = match args.get("where") {
        None | Some(Value::Null) => None,
        Some(Value::Object(m)) => Some(m.clone()),
        _ => bail!("where must be an object"),
    };
    let max_length = match args.get("max_length") {
        None | Some(Value::Null) => 0,
        Some(v) => {
            let n = v
                .as_i64()
                .ok_or_else(|| anyhow!("max_length must be an integer"))?;
            if n < 0 {
                bail!("max_length must not be negative");
            }
            usize::try_from(n).unwrap_or(usize::MAX)
        }
    };
    Ok(ReadRequest {
        collection,
        single,
        identifier,
        filter,
        fields,
        max_length,
        pack,
        kind,
    })
}

fn apply_request(req: &ReadRequest, docs: Value) -> Result<Value> {
    let docs = docs
        .as_array()
        .ok_or_else(|| anyhow!("Foundry document response must be an array"))?;
    if req.single {
        let doc = read_ops::find_document(docs, &req.identifier)
            .ok_or_else(|| anyhow!("Document not found"))?;
        Ok(read_ops::project_document(doc, req.fields.as_deref()))
    } else {
        Ok(Value::Array(read_ops::process_documents(
            docs,
            req.filter.as_ref(),
            req.fields.as_deref(),
            req.max_length,
        )))
    }
}

#[cfg(test)]
pub fn apply(name: &str, args: &Value, docs: Value) -> Result<Value> {
    apply_request(&parse(name, args)?, docs)
}

fn pack_query(req: &ReadRequest) -> Value {
    if req.single {
        read_ops::identifier_query(&req.identifier).unwrap_or_else(|| json!({}))
    } else {
        read_ops::where_to_server_query(req.filter.as_ref())
    }
}

pub async fn call(
    client: &FoundryClient,
    credential: &FoundryCredential,
    name: &str,
    args: &Value,
) -> Result<Value> {
    if name == "get_compendiums" {
        let args = args
            .as_object()
            .ok_or_else(|| anyhow!("arguments must be an object"))?;
        let kind = match args.get("type") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.as_str()),
            _ => bail!("type must be a string"),
        };
        return client.get_compendiums(credential, kind).await;
    }
    let req = parse(name, args)?;
    let docs = if let Some(pack) = req.pack.as_deref() {
        let query = pack_query(&req);
        client
            .get_pack_documents(credential, req.kind.unwrap(), pack, query)
            .await?
    } else {
        client.list_documents(credential, req.collection).await?
    };
    apply_request(&req, docs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn definitions_cover_upstream_world_reads() {
        let tools = definitions();
        assert_eq!(tools.len(), 27);
        assert!(tools.iter().any(|t| t["name"] == "get_journals"));
        assert!(tools.iter().any(|t| t["name"] == "get_journal"));
        assert!(tools.iter().any(|t| t["name"] == "get_compendiums"));
        assert!(
            tools.iter().find(|t| t["name"] == "get_actors").unwrap()["inputSchema"]["properties"]
                .get("pack")
                .is_some()
        );
        assert!(
            tools.iter().find(|t| t["name"] == "get_users").unwrap()["inputSchema"]["properties"]
                .get("pack")
                .is_none()
        );
    }

    #[test]
    fn pack_list_query_pushes_supported_where_fields() {
        let req = parse("get_actors", &json!({"pack":"world.actors","where":{"type":"npc","active":true,"nested":{"x":1},"system.hp":4}})).unwrap();
        assert_eq!(pack_query(&req), json!({"type":"npc","active":true}));
        let single = parse("get_actor", &json!({"pack":"world.actors","id":"a"})).unwrap();
        assert_eq!(pack_query(&single), json!({"_id":"a"}));
    }

    #[test]
    fn list_filters_projects_and_limits_results() {
        let docs = json!([{"_id":"1","name":"One","type":"npc","extra":3},{"_id":"2","name":"Two","type":"pc"}]);
        let result = apply(
            "get_actors",
            &json!({"where":{"type":"npc"},"requested_fields":["name"]}),
            docs,
        )
        .unwrap();
        assert_eq!(result, json!([{"_id":"1","name":"One"}]));
    }

    #[test]
    fn singular_lookup_prefers_id_and_reports_missing() {
        let docs = json!([{"_id":"a","name":"One"},{"_id":"b","name":"Two"}]);
        assert_eq!(
            apply("get_actor", &json!({"id":"a","name":"Two"}), docs.clone()).unwrap()["_id"],
            "a"
        );
        assert!(apply("get_actor", &json!({"id":"missing"}), docs).is_err());
    }

    #[test]
    fn max_length_must_be_a_non_negative_integer() {
        assert!(parse("get_actors", &json!({"max_length":-1})).is_err());
        assert!(parse("get_actors", &json!({"max_length":-0.5})).is_err());
        assert!(parse("get_actors", &json!({"max_length":1.5})).is_err());
        assert!(parse("get_actors", &json!({"max_length":"10"})).is_err());
        assert_eq!(
            parse("get_actors", &json!({"max_length":0}))
                .unwrap()
                .max_length,
            0
        );
        assert_eq!(
            parse("get_actors", &json!({"max_length":4096}))
                .unwrap()
                .max_length,
            4096
        );
    }

    #[test]
    fn rejects_unsupported_pack_and_malformed_arguments() {
        assert!(parse("get_users", &json!({"pack":"world.foo"})).is_err());
        assert!(parse("get_actors", &json!({"pack":"world.foo"})).is_ok());
        assert!(parse("get_actors", &json!({"pack":""})).is_err());
        assert!(parse("get_actors", &json!({"requested_fields":[2]})).is_err());
        assert!(parse("get_actor", &json!({})).is_err());
    }
}
