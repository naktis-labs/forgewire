use foundry_connector::FoundryCredential;
use serde_json::{json, Value};

use crate::operations::{self, Connector};

pub fn definitions() -> Vec<Value> {
    vec![
        json!({"name":"browse_files","description":"Browse files in Foundry data storage","inputSchema":{"type":"object","properties":{"target":{"type":"string"},"type":{"type":"string"},"extensions":{"type":"array","items":{"type":"string"}}}}}),
        json!({"name":"upload_file","description":"Upload base64-encoded bytes to Foundry data storage (requires FOUNDRY_ALLOW_WRITES=true); remote URLs are not supported. Uploads may overwrite existing files.","inputSchema":{"type":"object","properties":{"target":{"type":"string"},"filename":{"type":"string"},"data_base64":{"type":"string"},"overwrite":{"type":"boolean","description":"Must be true to acknowledge that an existing file may be overwritten."}},"required":["target","filename","data_base64","overwrite"]}}),
    ]
}

pub fn recognizes(name: &str) -> bool {
    matches!(name, "browse_files" | "upload_file")
}

pub async fn call(
    client: &dyn Connector,
    credential: &FoundryCredential,
    policy: operations::WritePolicy,
    name: &str,
    args: &Value,
) -> Result<Value, operations::OperationError> {
    match name {
        "browse_files" => operations::browse_files(client, credential, args).await,
        "upload_file" => operations::upload_file(client, credential, policy, args).await,
        _ => Err(operations::OperationError::invalid("unknown file tool")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schema_requires_explicit_overwrite_acknowledgement() {
        let upload = &definitions()[1]["inputSchema"];
        assert!(upload["required"]
            .as_array()
            .unwrap()
            .contains(&json!("overwrite")));
        assert_eq!(upload["properties"]["overwrite"]["type"], "boolean");
    }
}
