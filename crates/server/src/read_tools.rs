use foundry_connector::FoundryCredential;
use serde_json::{json, Value};

use crate::operations::{self, Connector};

const TYPES: [(&str, &str); 13] = [
    ("actor", "actors"),
    ("item", "items"),
    ("folder", "folders"),
    ("user", "users"),
    ("scene", "scenes"),
    ("journal", "journals"),
    ("macro", "macros"),
    ("card", "cards"),
    ("playlist", "playlists"),
    ("table", "tables"),
    ("combat", "combats"),
    ("message", "messages"),
    ("setting", "settings"),
];

pub fn definitions() -> Vec<Value> {
    let mut tools = Vec::new();
    for (singular, plural) in TYPES {
        let fields = json!({"type":"array", "items":{"type":"string"}, "description":"Fields to return; supports dot notation. _id and name are always included."});
        let mut list = json!({"name":format!("get_{plural}"),"description":format!("Get all {plural} from FoundryVTT"),"inputSchema":{"type":"object","properties":{"max_length":{"type":"integer","description":"Maximum JSON response size in bytes; zero means unlimited."},"requested_fields":fields,"where":{"type":"object","additionalProperties":true}},"required":[]}});
        let mut get = json!({"name":format!("get_{singular}"),"description":format!("Get a {singular} by id, _id, or name"),"inputSchema":{"type":"object","properties":{"id":{"type":"string"},"_id":{"type":"string"},"name":{"type":"string"},"requested_fields":fields},"required":[]}});
        let packable = matches!(
            singular,
            "actor" | "item" | "scene" | "journal" | "macro" | "card" | "playlist" | "table"
        );
        if packable {
            let property = json!({"type":"string","description":"Compendium pack ID; list packs with get_compendiums."});
            list["inputSchema"]["properties"]["pack"] = property.clone();
            get["inputSchema"]["properties"]["pack"] = property;
        }
        tools.extend([list, get]);
    }
    tools.push(json!({"name":"get_compendiums","description":"List compendium packs, optionally filtered by document type.","inputSchema":{"type":"object","properties":{"type":{"type":"string"}},"required":[]}}));
    tools
}

pub fn recognizes(name: &str) -> bool {
    name == "get_compendiums"
        || TYPES
            .iter()
            .any(|(one, many)| name == format!("get_{one}") || name == format!("get_{many}"))
}

pub async fn call(
    client: &dyn Connector,
    credential: &FoundryCredential,
    name: &str,
    args: &Value,
) -> anyhow::Result<Value> {
    Ok(operations::read(client, credential, name, args).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn definitions_cover_all_mcp_read_workflows() {
        let tools = definitions();
        assert_eq!(tools.len(), 27);
        assert!(tools.iter().any(|t| t["name"] == "get_journals"));
        assert!(tools.iter().any(|t| t["name"] == "get_journal"));
        assert!(tools.iter().any(|t| t["name"] == "get_compendiums"));
        for tool in &tools {
            assert!(recognizes(tool["name"].as_str().unwrap()));
        }
    }
}
