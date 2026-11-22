use anyhow::{anyhow, bail, Result};
use foundry_connector::{FoundryClient, FoundryCredential};
use serde_json::{json, Value};

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
    client: &FoundryClient,
    credential: &FoundryCredential,
    allow_writes: bool,
    name: &str,
    args: &Value,
) -> Result<Value> {
    if !recognizes(name) {
        bail!("unknown compendium tool");
    }
    if !allow_writes {
        bail!("writes are disabled; set FOUNDRY_ALLOW_WRITES=true to enable");
    }
    let args = args
        .as_object()
        .ok_or_else(|| anyhow!("arguments must be an object"))?;
    let required = |key: &str| -> Result<&str> {
        args.get(key)
            .and_then(Value::as_str)
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| anyhow!("{key} must be a non-empty string"))
    };
    match name {
        "create_compendium" => {
            client
                .create_compendium(credential, required("label")?, required("type")?)
                .await
        }
        "delete_compendium" => {
            client
                .delete_compendium(credential, required("name")?)
                .await
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn credential() -> FoundryCredential {
        FoundryCredential {
            id: None,
            hostname: "localhost".into(),
            userid: None,
            username: None,
            password: String::new(),
        }
    }
    #[tokio::test]
    async fn writes_are_gated_before_validation_or_network() {
        let client = FoundryClient::new().unwrap();
        for name in ["create_compendium", "delete_compendium"] {
            assert!(call(&client, &credential(), false, name, &Value::Null)
                .await
                .unwrap_err()
                .to_string()
                .contains("disabled"));
        }
    }
    #[tokio::test]
    async fn invalid_arguments_fail_before_network() {
        let client = FoundryClient::new().unwrap();
        for (name, args) in [
            ("create_compendium", json!({"label":" ","type":"Actor"})),
            ("delete_compendium", json!({"name":1})),
        ] {
            assert!(call(&client, &credential(), true, name, &args)
                .await
                .unwrap_err()
                .to_string()
                .contains("non-empty string"));
        }
    }
    #[test]
    fn definitions_match_dispatch() {
        for def in definitions() {
            assert!(recognizes(def["name"].as_str().unwrap()));
        }
        assert!(!recognizes("other"));
    }
}
