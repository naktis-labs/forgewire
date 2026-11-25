use foundry_connector::FoundryCredential;
use serde_json::{json, Value};

use crate::operations::{self, Connector};

pub fn definitions() -> Vec<Value> {
    vec![
        json!({"name":"create_compendium","description":"Create a world compendium (requires FOUNDRY_ALLOW_WRITES=true).","inputSchema":{"type":"object","properties":{"label":{"type":"string"},"type":{"type":"string"}},"required":["label","type"]}}),
        json!({"name":"delete_compendium","description":"Permanently delete a world compendium and its contents (requires FOUNDRY_ALLOW_WRITES=true). Use the pack name, not its label.","inputSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}),
    ]
}

pub fn recognizes(name: &str) -> bool {
    matches!(name, "create_compendium" | "delete_compendium")
}

pub async fn call(
    client: &dyn Connector,
    credential: &FoundryCredential,
    policy: operations::WritePolicy,
    name: &str,
    args: &Value,
) -> anyhow::Result<Value> {
    Ok(operations::compendium(client, credential, policy, name, args).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn definitions_match_dispatch() {
        for definition in definitions() {
            assert!(recognizes(definition["name"].as_str().unwrap()));
        }
        assert!(!recognizes("other"));
    }
}
