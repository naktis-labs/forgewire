use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::{read_ops, FoundryClient, FoundryCredential};

fn pack_operation(pack: &str, query: Value) -> Value {
    json!({"action":"get", "pack":pack, "query":query, "index":false, "broadcast":false})
}

/// The bare name a compendium delete takes, resolved against the packs the
/// world advertises.
///
/// A pack's id (`world.<name>`) names exactly one pack, and a world pack's
/// own name is reported next to that id, so both forms resolve to the same
/// pack. A package pack is refused: its name is only unique inside its
/// package, and the delete would reach a world pack of the same name. A pack
/// the world does not advertise is refused as well, so a delete never names
/// a pack the caller could not have seen.
pub fn resolve_pack_name(packs: &Value, requested: &str) -> Result<String> {
    let requested = requested.trim();
    if requested.is_empty() {
        return Err(crate::invalid_input("name must not be empty"));
    }
    let list = || packs.as_array().into_iter().flatten();

    // An id names exactly one pack.
    for pack in list() {
        if pack.get("id").and_then(Value::as_str) == Some(requested) {
            if !read_ops::is_world_pack(pack) {
                return Err(crate::invalid_input(format!(
                    "Compendium pack \"{requested}\" is not a world compendium and cannot be deleted."
                )));
            }
            let name = pack
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|name| !name.is_empty());
            return Ok(match name {
                Some(name) => name.to_owned(),
                None => requested
                    .strip_prefix("world.")
                    .unwrap_or(requested)
                    .to_owned(),
            });
        }
    }

    // A world pack's own name.
    for pack in list() {
        if read_ops::is_world_pack(pack)
            && pack.get("name").and_then(Value::as_str) == Some(requested)
        {
            return Ok(requested.to_owned());
        }
    }

    // A pack the world does not advertise is not deletable: the request
    // would reach the instance as a bare name, and a bare name the world
    // does not list may still name another pack.
    Err(crate::invalid_input(format!(
        "Compendium pack \"{requested}\" not found. Use get_compendiums to list the available packs."
    )))
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

    /// Delete a world compendium.
    ///
    /// Reads identify a pack by its id (`world.<name>`), while Foundry's
    /// `manageCompendium` delete expects the bare pack name and prefixes it
    /// itself. The name is resolved against the packs the world advertises,
    /// so both forms name the same pack and a package pack is never taken
    /// for a world pack.
    pub async fn delete_compendium(
        &self,
        credential: &FoundryCredential,
        name: &str,
    ) -> Result<Value> {
        if name.trim().is_empty() {
            return Err(crate::invalid_input("name must not be empty"));
        }
        let world = self.request(credential, "World", json!({})).await?;
        let name = resolve_pack_name(&world["packs"], name)?;
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

    /// The packs a world advertises: a world pack, a package pack, and a
    /// world pack whose own name starts with the world's prefix.
    fn packs() -> Value {
        json!([
            {"id":"world.npcs","name":"npcs","label":"NPCs","type":"Actor","packageType":"world"},
            {"id":"dnd5e.items","name":"items","label":"Items","type":"Item","packageType":"system"},
            {"id":"world.world.lore","name":"world.lore","label":"Lore","type":"JournalEntry","packageType":"world"},
        ])
    }

    #[test]
    fn a_pack_id_and_its_name_resolve_to_the_same_bare_name() {
        assert_eq!(
            resolve_pack_name(&packs(), "world.npcs").unwrap(),
            "npcs".to_owned()
        );
        assert_eq!(
            resolve_pack_name(&packs(), "npcs").unwrap(),
            "npcs".to_owned()
        );
    }

    #[test]
    fn a_pack_named_like_a_world_id_resolves_to_its_own_name() {
        // The id of the pack `npcs` is `world.npcs`, and the id of the pack
        // `world.lore` is `world.world.lore`. Neither may be shortened to
        // the other's name.
        assert_eq!(
            resolve_pack_name(&packs(), "world.lore").unwrap(),
            "world.lore".to_owned()
        );
        assert_eq!(
            resolve_pack_name(&packs(), "world.world.lore").unwrap(),
            "world.lore".to_owned()
        );
    }

    #[test]
    fn a_package_pack_is_not_deletable_by_its_name() {
        let error = resolve_pack_name(&packs(), "dnd5e.items")
            .unwrap_err()
            .to_string();
        assert!(error.contains("not a world compendium"), "{error}");
        // The bare name of a package pack repeats in the world scope, so it
        // must not reach a world pack of the same name either.
        let error = resolve_pack_name(&packs(), "items")
            .unwrap_err()
            .to_string();
        assert!(error.contains("not found"), "{error}");
    }

    #[test]
    fn an_empty_or_unknown_pack_name_is_refused() {
        assert!(resolve_pack_name(&packs(), "  ")
            .unwrap_err()
            .to_string()
            .contains("must not be empty"));
        assert!(resolve_pack_name(&packs(), "world.")
            .unwrap_err()
            .to_string()
            .contains("not found"));
        let error = resolve_pack_name(&packs(), "nothing")
            .unwrap_err()
            .to_string();
        assert!(error.contains("not found"), "{error}");
    }

    #[test]
    fn a_refused_pack_name_is_a_caller_mistake() {
        for requested in ["", "  ", "world.", "nothing", "dnd5e.items"] {
            let error = resolve_pack_name(&packs(), requested).unwrap_err();
            assert!(
                error.downcast_ref::<crate::InvalidInput>().is_some(),
                "{requested}: {error}"
            );
        }
    }

    #[test]
    fn a_pack_the_world_does_not_advertise_is_refused() {
        // The bare name of a pack the world does not list must not reach the
        // instance, where it could name another pack.
        let error = resolve_pack_name(&json!([]), "world.npcs")
            .unwrap_err()
            .to_string();
        assert!(error.contains("not found"), "{error}");
        let error = resolve_pack_name(&json!([]), "npcs")
            .unwrap_err()
            .to_string();
        assert!(error.contains("not found"), "{error}");
    }
}
