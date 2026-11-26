use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::{read_ops, FoundryClient, FoundryCredential};

fn pack_operation(pack: &str, query: Value) -> Value {
    json!({"action":"get", "pack":pack, "query":query, "index":false, "broadcast":false})
}

impl FoundryClient {
    /// Create or delete a compendium over the identity's persistent
    /// connection. The acknowledgement id identifies the request, so a
    /// compendium change shares the connection with reads and with other
    /// changes instead of opening a socket per operation.
    async fn manage_compendium(
        &self,
        credential: &FoundryCredential,
        action: &str,
        data: Value,
    ) -> Result<Value> {
        let key = crate::identity_key(credential);
        self.connections
            .exchange(
                key,
                crate::socket_mux::Request::Compendium {
                    action: action.to_owned(),
                    data,
                },
                "compendium",
                || self.connect_for(credential),
            )
            .await
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
    fn pack_read_operation_is_read_only_and_targets_pack() {
        assert_eq!(
            pack_operation("world.items", json!({"_id":"abc"})),
            json!({"action":"get","pack":"world.items","query":{"_id":"abc"},"index":false,"broadcast":false})
        );
    }
}
