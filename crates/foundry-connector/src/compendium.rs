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
///
/// Two rules keep a destructive delete away from an unintended pack:
///
/// - A reference that is one pack's id *and* another pack's own name is
///   ambiguous, and it deletes neither.
/// - The bare name the delete carries must be the name of exactly one
///   advertised world pack. Nothing is inferred from the id's shape: the
///   target is always a pack the world has published.
pub fn resolve_pack_name(packs: &Value, requested: &str) -> Result<String> {
    let requested = requested.trim();
    if requested.is_empty() {
        return Err(crate::invalid_input("name must not be empty"));
    }
    let list: Vec<&Value> = packs.as_array().into_iter().flatten().collect();

    // The pack the reference names by its id, and the world pack it names
    // by its own name.
    let by_id: Vec<&Value> = list
        .iter()
        .copied()
        .filter(|pack| pack.get("id").and_then(Value::as_str) == Some(requested))
        .collect();
    let by_name: Vec<&Value> = list
        .iter()
        .copied()
        .filter(|pack| {
            read_ops::is_world_pack(pack)
                && pack.get("name").and_then(Value::as_str) == Some(requested)
        })
        .collect();

    if !by_id.is_empty() && !by_name.is_empty() {
        return Err(crate::invalid_input(format!(
            "Compendium pack \"{requested}\" is the id of one pack and the name of another. Delete it by a name that names exactly one pack."
        )));
    }
    if by_id.len() > 1 || by_name.len() > 1 {
        return Err(crate::invalid_input(format!(
            "Compendium pack \"{requested}\" names more than one pack. Delete it by a name that names exactly one pack."
        )));
    }

    // The bare name the delete has to carry.
    let candidate = if let Some(pack) = by_id.first() {
        if !read_ops::is_world_pack(pack) {
            return Err(crate::invalid_input(format!(
                "Compendium pack \"{requested}\" is not a world compendium and cannot be deleted."
            )));
        }
        match pack
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            Some(name) => name.to_owned(),
            // The world did not advertise the name, so the only candidate is
            // the id's own suffix. It is still verified below against the
            // advertised world packs, so it is a confirmation and not a
            // guess.
            None => requested
                .strip_prefix("world.")
                .unwrap_or(requested)
                .to_owned(),
        }
    } else if !by_name.is_empty() {
        requested.to_owned()
    } else {
        // A pack the world does not advertise is not deletable: the request
        // would reach the instance as a bare name, and a bare name the world
        // does not list may still name another pack.
        return Err(crate::pack_not_found(requested));
    };

    // The name the delete carries must be the name of exactly one
    // advertised world pack.
    let matching: Vec<&Value> = list
        .iter()
        .copied()
        .filter(|pack| {
            read_ops::is_world_pack(pack)
                && pack.get("name").and_then(Value::as_str) == Some(candidate.as_str())
        })
        .collect();
    match matching.len() {
        1 => Ok(candidate),
        0 => Err(crate::invalid_input(format!(
            "Compendium pack \"{requested}\" does not resolve to an advertised world compendium."
        ))),
        _ => Err(crate::invalid_input(format!(
            "Compendium pack \"{requested}\" resolves to more than one world compendium."
        ))),
    }
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
        read_ops::validate_pack_access(&world["packs"], pack, kind)?;
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

    /// A reference that is one pack's id and another pack's own name could
    /// delete either of them, so it deletes neither.
    #[test]
    fn a_reference_that_is_both_an_id_and_a_name_is_refused() {
        let colliding = json!([
            {"id":"world.lore","name":"lore","label":"Lore","type":"Actor","packageType":"world"},
            {"id":"world.world.lore","name":"world.lore","label":"Lore","type":"JournalEntry","packageType":"world"},
        ]);
        let error = resolve_pack_name(&colliding, "world.lore")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("the id of one pack and the name of another"),
            "{error}"
        );
        // Each pack is still reachable through its unambiguous reference.
        assert_eq!(
            resolve_pack_name(&colliding, "world.world.lore").unwrap(),
            "world.lore".to_owned()
        );
        assert_eq!(
            resolve_pack_name(&colliding, "lore").unwrap(),
            "lore".to_owned()
        );
    }

    /// Two world packs that share a name cannot be told apart, so the
    /// reference is refused instead of picking one.
    #[test]
    fn a_name_shared_by_two_world_packs_is_refused() {
        let duplicated = json!([
            {"id":"world.one","name":"shared","label":"One","type":"Actor","packageType":"world"},
            {"id":"world.two","name":"shared","label":"Two","type":"Actor","packageType":"world"},
        ]);
        let error = resolve_pack_name(&duplicated, "shared")
            .unwrap_err()
            .to_string();
        assert!(error.contains("more than one pack"), "{error}");
    }

    /// The delete's bare name is verified against the advertised packs: a
    /// pack whose name the world does not publish is not deletable, and the
    /// id's shape is never taken as proof of a name.
    #[test]
    fn a_pack_without_an_advertised_name_is_not_deletable() {
        let unnamed = json!([
            {"id":"world.ghost","label":"Ghost","type":"Actor","packageType":"world"},
        ]);
        let error = resolve_pack_name(&unnamed, "world.ghost")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("does not resolve to an advertised world compendium"),
            "{error}"
        );
    }

    /// A world pack whose id carries a name the world does advertise
    /// resolves through that name, and the name is confirmed rather than
    /// assumed.
    #[test]
    fn an_id_without_an_advertised_name_resolves_only_through_a_published_name() {
        let published = json!([
            {"id":"world.lore","label":"Lore","type":"Actor","packageType":"world"},
            {"id":"world.other","name":"lore","label":"Other","type":"Actor","packageType":"world"},
        ]);
        // The id names a pack the world does not publish a name for, but the
        // bare name it resolves to is an advertised world pack's name.
        assert_eq!(
            resolve_pack_name(&published, "world.lore").unwrap(),
            "lore".to_owned()
        );
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
                error.downcast_ref::<crate::InvalidInput>().is_some()
                    || error.downcast_ref::<crate::PackNotFound>().is_some(),
                "{requested}: {error}"
            );
        }
        // A pack the world does not advertise is a lookup miss, not a
        // malformed request.
        let error = resolve_pack_name(&packs(), "nothing").unwrap_err();
        assert!(
            error.downcast_ref::<crate::PackNotFound>().is_some(),
            "{error}"
        );
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
