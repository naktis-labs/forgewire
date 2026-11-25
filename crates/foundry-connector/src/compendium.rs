use anyhow::{anyhow, bail, Result};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::{read_ops, FoundryClient, FoundryCredential};

fn pack_operation(pack: &str, query: Value) -> Value {
    json!({"action":"get", "pack":pack, "query":query, "index":false, "broadcast":false})
}

fn management_payload(action: &str, data: Value) -> Value {
    let mut operation = json!({"action":action, "data":data});
    if action == "create" {
        operation["options"] = json!({});
    }
    json!(["manageCompendium", operation])
}

fn management_ack(frame: &str, id: u64, action: &str) -> Result<Option<Value>> {
    let prefix = format!("43{id}[");
    if !frame.starts_with(&prefix) {
        return Ok(None);
    }
    let ambiguous =
        || anyhow!("Invalid Foundry compendium response; the operation may have been applied");
    let values: Value =
        serde_json::from_str(&frame[2 + id.to_string().len()..]).map_err(|_| ambiguous())?;
    let response = values
        .as_array()
        .and_then(|v| v.first())
        .filter(|v| v.is_object())
        .ok_or_else(ambiguous)?;
    if response.pointer("/request/action").and_then(Value::as_str) != Some(action) {
        return Err(ambiguous());
    }
    if let Some(error) = response.get("error").filter(|v| !v.is_null()) {
        let message = error
            .as_str()
            .or_else(|| error.get("message").and_then(Value::as_str))
            .unwrap_or("unknown error");
        bail!("Foundry compendium {action} failed: {message}; the operation may have been applied");
    }
    let result = response.get("result").ok_or_else(ambiguous)?;
    let valid = if action == "create" {
        result.is_object() && result.as_object().is_some_and(|v| !v.is_empty())
    } else {
        result.as_str().is_some_and(|v| !v.trim().is_empty())
    };
    if !valid {
        return Err(ambiguous());
    }
    Ok(Some(response.clone()))
}

impl FoundryClient {
    async fn manage_compendium(
        &self,
        credential: &FoundryCredential,
        action: &str,
        data: Value,
    ) -> Result<Value> {
        let (session, generation, userid) = self.session(credential).await?;
        let request = self.socket_request(&credential.hostname, &session, generation)?;
        let mut socket = timeout(
            crate::SOCKET_CONNECT_TIMEOUT,
            crate::connect_socket(request),
        )
        .await
        .map_err(|_| anyhow!("Foundry websocket connection timed out"))?
        .map_err(|_| anyhow!("Foundry websocket connection failed"))?;
        timeout(
            crate::SESSION_TIMEOUT,
            crate::wait_for_session(&mut socket, &session, &userid),
        )
        .await
        .map_err(|_| anyhow!("Timed out waiting for Foundry session binding"))??;
        let payload = management_payload(action, data);
        crate::send_message(
            &mut socket,
            Message::Text(format!("421{payload}").into()),
            "sending compendium request; the operation may have been applied",
        )
        .await?;
        timeout(crate::RESPONSE_TIMEOUT, async {
            loop {
                let frame = socket.next().await.ok_or_else(|| anyhow!("Foundry socket closed; the operation may have been applied"))?
                    .map_err(|_| anyhow!("Foundry socket read failed; the operation may have been applied"))?;
                if let Message::Text(text) = frame {
                    if text == "2" {
                        crate::send_message(
                            &mut socket,
                            Message::Text("3".into()),
                            "sending Foundry pong; the operation may have been applied",
                        )
                        .await?;
                    } else if crate::is_namespace_disconnect_or_error(text.as_str()) {
                        bail!("Foundry Socket.IO namespace disconnected or rejected the connection; the operation may have been applied");
                    } else if let Some(response) = management_ack(text.as_str(), 1, action)? {
                        return Ok(response);
                    }
                }
            }
        }).await.map_err(|_| anyhow!("Timed out waiting for Foundry compendium response; the operation may have been applied"))?
    }

    pub async fn create_compendium(
        &self,
        credential: &FoundryCredential,
        label: &str,
        kind: &str,
    ) -> Result<Value> {
        if label.trim().is_empty() || kind.trim().is_empty() {
            bail!("label and type must not be empty");
        }
        self.manage_compendium(credential, "create", json!({"label":label,"type":kind}))
            .await
    }

    pub async fn delete_compendium(
        &self,
        credential: &FoundryCredential,
        name: &str,
    ) -> Result<Value> {
        if name.trim().is_empty() {
            bail!("name must not be empty");
        }
        self.manage_compendium(credential, "delete", json!(name))
            .await
    }

    pub async fn get_compendiums(
        &self,
        credential: &FoundryCredential,
        kind: Option<&str>,
    ) -> Result<Value> {
        let world = self.request(credential, "World", json!({})).await?;
        Ok(Value::Array(read_ops::summarize_packs(
            &world["packs"],
            kind,
        )))
    }

    pub async fn get_pack_documents(
        &self,
        credential: &FoundryCredential,
        kind: &str,
        pack: &str,
        query: Value,
    ) -> Result<Value> {
        let world = self.request(credential, "World", json!({})).await?;
        if let Some(error) = read_ops::validate_pack_access(&world["packs"], pack, kind) {
            bail!("{error}");
        }
        self.request(credential, kind, pack_operation(pack, query))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_protocol_and_acknowledgements() {
        assert_eq!(
            management_payload("create", json!({"label":"NPCs","type":"Actor"})),
            json!(["manageCompendium", {"action":"create","data":{"label":"NPCs","type":"Actor"},"options":{}}])
        );
        assert_eq!(
            management_payload("delete", json!("npcs")),
            json!(["manageCompendium", {"action":"delete","data":"npcs"}])
        );
        assert!(management_ack("432[{}]", 1, "create").unwrap().is_none());
        assert!(management_ack(
            "431[{\"request\":{\"action\":\"create\"},\"result\":{\"id\":\"world.npcs\"}}]",
            1,
            "create"
        )
        .unwrap()
        .is_some());
        assert!(management_ack(
            "431[{\"request\":{\"action\":\"delete\"},\"error\":\"denied\"}]",
            1,
            "delete"
        )
        .unwrap_err()
        .to_string()
        .contains("denied"));
        for frame in [
            "431[]",
            "431[{}]",
            "431[not-json]",
            "431[{\"request\":{\"action\":\"delete\"}}]",
        ] {
            assert!(management_ack(frame, 1, "delete")
                .unwrap_err()
                .to_string()
                .contains("may have been applied"));
        }
    }

    #[test]
    fn pack_read_operation_is_read_only_and_targets_pack() {
        assert_eq!(
            pack_operation("world.items", json!({"_id":"abc"})),
            json!({"action":"get","pack":"world.items","query":{"_id":"abc"},"index":false,"broadcast":false})
        );
    }
}
