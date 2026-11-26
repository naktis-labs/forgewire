//! Configured Foundry instances and instance targeting.
//!
//! Targeting rules by transport:
//!
//! - REST never mutates any selection state. A request names its instance
//!   explicitly (`?instance=<id|item_order>`) or targets the documented
//!   stable default — the first configured entry. Nothing an MCP client does
//!   can redirect a REST request.
//! - MCP selection is scoped to a server-issued session id ([`Selections`]):
//!   `initialize` issues it, later calls echo it, and one client's selection
//!   is invisible to every other client, session, and REST request.
//!
//! Both transports resolve the target once per request into an owned
//! credential snapshot, so later selections never retarget in-flight work,
//! and instance listing never discloses secrets (passwords or usernames).
//!
//! Every configured instance also has a canonical *scope* name — its `id`,
//! or its zero-based `item_order` when it has no `id`. The scope is what a
//! bearer token is issued for, so it names exactly one instance and is
//! resolved through the same configured set.

use crate::auth::Scope;
use foundry_connector::FoundryCredential;
use rand::RngCore;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// The canonical scope identifier of one configured instance: its configured
/// `id`, or its zero-based `item_order` when it has no `id`.
fn canonical_scope(index: usize, credential: &FoundryCredential) -> String {
    match credential.id.as_deref() {
        Some(id) if !id.is_empty() => id.to_owned(),
        _ => index.to_string(),
    }
}

/// The configured instances. Resolution is pure: it returns indices and owned
/// snapshots and stores nothing, so "selecting" an instance can never affect
/// another caller or a request already in flight.
#[derive(Clone)]
pub struct Instances {
    credentials: Arc<Vec<FoundryCredential>>,
}

impl Instances {
    pub fn new(credentials: Vec<FoundryCredential>) -> anyhow::Result<Self> {
        if credentials.is_empty() {
            anyhow::bail!("credentials file contains no entries");
        }
        Ok(Self {
            credentials: Arc::new(credentials),
        })
    }

    /// The documented stable default: the first configured entry.
    pub fn default_snapshot(&self) -> FoundryCredential {
        self.credentials[0].clone()
    }

    /// An owned snapshot of the entry at `index`, immune to later selections.
    pub fn credential(&self, index: usize) -> anyhow::Result<FoundryCredential> {
        self.credentials
            .get(index)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("instance not found"))
    }

    /// The canonical scope identifier of the instance at `index`: its
    /// configured `id`, or its zero-based `item_order` when it has no `id`.
    /// This is the name a bearer token's scope refers to, so it must be
    /// unique across the configured instances.
    pub fn canonical_scope(&self, index: usize) -> anyhow::Result<String> {
        let credential = self
            .credentials
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("instance not found"))?;
        Ok(canonical_scope(index, credential))
    }

    /// Whether the instance at `index` is the one `scope` names. A
    /// credential is issued for exactly one instance, so this is the check
    /// that keeps a request from reaching another one.
    pub fn matches_scope(&self, index: usize, scope: &Scope) -> anyhow::Result<bool> {
        Ok(self.canonical_scope(index)? == scope.as_str())
    }

    /// The index of the instance `scope` names, when the scope names one.
    pub fn index_for_scope(&self, scope: &Scope) -> Option<usize> {
        self.credentials
            .iter()
            .enumerate()
            .find(|(index, credential)| canonical_scope(*index, credential) == scope.as_str())
            .map(|(index, _)| index)
    }

    /// Resolve a scope name to the instance it names. A scope names exactly
    /// one configured instance: an unknown scope is an error, and a scope
    /// shared by two instances is ambiguous rather than silently resolved.
    pub fn resolve_scope(&self, scope: &str) -> anyhow::Result<usize> {
        let mut matches = self
            .credentials
            .iter()
            .enumerate()
            .filter(|(index, credential)| canonical_scope(*index, credential) == scope);
        if let Some((index, _)) = matches.next() {
            if matches.next().is_some() {
                anyhow::bail!("ambiguous instance scope");
            }
            return Ok(index);
        }
        anyhow::bail!("unknown instance scope")
    }

    /// Resolve a REST `?instance=` reference: a configured instance `id`
    /// first, else a zero-based `item_order`. Returns the index; callers take
    /// the snapshot themselves so resolution and use cannot be interleaved
    /// with a retarget.
    pub fn resolve_reference(&self, reference: &str) -> anyhow::Result<usize> {
        // A leading '+' is rejected rather than normalized: numeric parsing
        // would silently read "+1" as item_order 1, and instance addressing
        // must not silently rewrite the requested target.
        if reference.is_empty() || reference.starts_with('+') {
            anyhow::bail!("invalid instance reference");
        }
        let mut matches = self
            .credentials
            .iter()
            .enumerate()
            .filter(|(_, c)| c.id.as_deref() == Some(reference));
        if let Some((index, _)) = matches.next() {
            if matches.next().is_some() {
                anyhow::bail!("ambiguous instance identifier");
            }
            return Ok(index);
        }
        let index = reference
            .parse::<usize>()
            .map_err(|_| anyhow::anyhow!("instance not found"))?;
        if index >= self.credentials.len() {
            anyhow::bail!("instance not found");
        }
        Ok(index)
    }

    /// Resolve an MCP `choose_foundry_instance` request: exactly one of
    /// `_id`/`id` (unique) or `item_order`. Resolution reports selection
    /// only; it never verifies a connection and mutates nothing.
    pub fn resolve_selection(&self, args: &Value) -> anyhow::Result<usize> {
        let id = args.get("_id").or_else(|| args.get("id"));
        let order = args.get("item_order");
        if id.is_some() == order.is_some() {
            anyhow::bail!("provide exactly one of _id or item_order");
        }
        if let Some(id) = id {
            let id = id
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow::anyhow!("invalid instance identifier"))?;
            if args.get("_id").is_some() && args.get("id").is_some() {
                anyhow::bail!("provide only one identifier");
            }
            let mut matches = self
                .credentials
                .iter()
                .enumerate()
                .filter(|(_, c)| c.id.as_deref() == Some(id));
            let index = matches
                .next()
                .map(|(i, _)| i)
                .ok_or_else(|| anyhow::anyhow!("instance not found"))?;
            if matches.next().is_some() {
                anyhow::bail!("ambiguous instance identifier");
            }
            return Ok(index);
        }
        let index = order
            .and_then(Value::as_u64)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| anyhow::anyhow!("invalid item_order"))?;
        if index >= self.credentials.len() {
            anyhow::bail!("instance not found");
        }
        Ok(index)
    }

    /// List instances without secrets. `currently_active` flags the entry
    /// answering for the requesting scope: the stable default (or the
    /// `?instance=` reference) for REST, the session's own selection for MCP.
    /// Passwords and usernames are never emitted.
    pub fn show(&self, selected: usize) -> Value {
        Value::Array(
            self.credentials
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    json!({
                        "_id": c.id, "hostname": c.hostname, "userid": c.userid,
                        "item_order": i, "currently_active": i == selected
                    })
                })
                .collect(),
        )
    }
}

/// The selection report: selection only, never a verified connection.
pub fn selection_report(index: usize) -> Value {
    json!({"item_order": index, "selected": true})
}

/// Bounded table of server-issued MCP sessions and their per-client instance
/// selection.
///
/// `initialize` issues a random session id; requests carrying it (the
/// `Mcp-Session-Id` header) resolve targeting against this session's own
/// selection, never another client's. Resolution is atomic: [`Selections::scope`]
/// reads liveness and the selection under a single lock acquisition, so a
/// session that expires mid-request surfaces as expired instead of silently
/// degrading into the stable default. Sessions expire when the table is full
/// and the oldest is evicted, after which the client must initialize again.
/// A session starts with no selection, which means its calls use the stable
/// default.
#[derive(Clone)]
pub struct Selections {
    inner: Arc<Mutex<State>>,
}

struct State {
    sessions: HashMap<String, Option<usize>>,
    order: VecDeque<String>,
    capacity: usize,
}

/// The atomic resolution of a request's session scope: one lookup decides
/// both whether a presented session id is live and what it targets.
///
/// A request presenting an unknown or expired id must fail with the
/// initialize-again error — never silently retarget another instance,
/// cross into another client's scope, or run a write against the stable
/// default. Only a headerless (anonymous) caller bypasses sessions, and it
/// always targets the stable default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionScope {
    /// The id was never issued here or has expired: refuse the request.
    Expired,
    /// A live session that has not chosen: the stable default applies.
    Default,
    /// A live session with its own recorded selection.
    Selected(usize),
}

impl Selections {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_SESSION_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                sessions: HashMap::new(),
                order: VecDeque::new(),
                capacity: capacity.max(1),
            })),
        }
    }

    /// Issue a fresh session id with no selection yet.
    pub fn issue(&self) -> String {
        let id = session_id();
        let mut state = self.inner.lock().unwrap();
        while state.sessions.len() >= state.capacity {
            if let Some(oldest) = state.order.pop_front() {
                state.sessions.remove(&oldest);
            }
        }
        state.order.push_back(id.clone());
        state.sessions.insert(id.clone(), None);
        id
    }

    /// Record `index` as this session's selection and report whether the
    /// session was live. A session expired between identification and
    /// selection is not resurrected and nothing is stored; the caller must
    /// surface the miss so the client initializes again instead of trusting
    /// a phantom success report.
    #[must_use]
    pub fn select(&self, session: &str, index: usize) -> bool {
        let mut state = self.inner.lock().unwrap();
        match state.sessions.get_mut(session) {
            Some(slot) => {
                *slot = Some(index);
                true
            }
            None => false,
        }
    }

    /// Resolve `session` in a single atomic lookup, under one lock
    /// acquisition: whether the id is live and, if so, the session's own
    /// selection ([`SessionScope::Default`] when it has not chosen,
    /// [`SessionScope::Selected`] when it has).
    ///
    /// Combining liveness and targeting is the point: with separate
    /// contains-then-selection lookups, a session evicted between them made
    /// ordinary tool calls silently fall back to the stable default while
    /// `choose_foundry_instance` errored — the exact asymmetry this tri-state
    /// removes. An expired session is reported as [`SessionScope::Expired`]
    /// so the caller can refuse with the initialize-again error instead of
    /// retargeting; it is never resurrected and nothing is stored.
    pub fn scope(&self, session: &str) -> SessionScope {
        let state = self.inner.lock().unwrap();
        match state.sessions.get(session) {
            None => SessionScope::Expired,
            Some(None) => SessionScope::Default,
            Some(&Some(index)) => SessionScope::Selected(index),
        }
    }
}

impl Default for Selections {
    fn default() -> Self {
        Self::new()
    }
}

/// The default session table bound; tests inject smaller capacities so
/// oldest-first expiry is deterministic.
pub const DEFAULT_SESSION_CAPACITY: usize = 1024;

fn session_id() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credential(id: &str) -> FoundryCredential {
        FoundryCredential {
            id: Some(id.into()),
            hostname: format!("{id}.example"),
            userid: None,
            username: Some(format!("login-{id}")),
            password: format!("secret-{id}"),
        }
    }

    #[test]
    fn rest_targets_are_explicit_or_the_stable_default() {
        let instances = Instances::new(vec![credential("first"), credential("second")]).unwrap();
        assert_eq!(instances.default_snapshot().id.as_deref(), Some("first"));
        // Explicit references resolve by configured id first...
        assert_eq!(instances.resolve_reference("second").unwrap(), 1);
        // ...else by zero-based item_order.
        assert_eq!(instances.resolve_reference("1").unwrap(), 1);
        // A leading '+' would parse as a signed order; it is rejected, never
        // normalized into an index.
        for reference in ["missing", "9", "", "-1", "1x", "+1", "+second"] {
            assert!(
                instances.resolve_reference(reference).is_err(),
                "{reference:?} must not resolve"
            );
        }
        // Snapshots are independent clones: later resolutions never retarget
        // work that already holds one.
        let snapshot = instances.default_snapshot();
        let _ = instances.resolve_reference("second").unwrap();
        assert_eq!(snapshot.id.as_deref(), Some("first"));
    }

    #[test]
    fn selection_resolution_reports_selection_without_mutating_or_verifying() {
        let instances = Instances::new(vec![
            credential("first"),
            credential("first"),
            credential("second"),
        ])
        .unwrap();
        assert_eq!(
            instances
                .resolve_selection(&json!({"_id":"second"}))
                .unwrap(),
            2
        );
        assert_eq!(
            instances
                .resolve_selection(&json!({"item_order":0}))
                .unwrap(),
            0
        );
        // Duplicate ids are ambiguous...
        assert!(instances.resolve_selection(&json!({"id":"first"})).is_err());
        // ...and the request-shape rules still hold.
        for args in [
            json!({}),
            json!({"item_order":9}),
            json!({"_id":"second","item_order":0}),
            json!({"_id":"missing"}),
            json!({"_id":7}),
        ] {
            assert!(
                instances.resolve_selection(&args).is_err(),
                "{args} must not resolve"
            );
        }
        // Resolution is pure: nothing was stored or connected.
        assert_eq!(instances.default_snapshot().id.as_deref(), Some("first"));
        assert_eq!(selection_report(2), json!({"item_order":2,"selected":true}));
    }

    #[test]
    fn listing_flags_the_requested_scope_without_disclosing_secrets() {
        let instances = Instances::new(vec![credential("first"), credential("second")]).unwrap();
        let default = instances.show(0);
        assert_eq!(default[0]["currently_active"], true);
        assert_eq!(default[1]["currently_active"], false);
        let other = instances.show(1);
        assert_eq!(other[1]["currently_active"], true);
        let output = format!("{default}{other}");
        assert!(!output.contains("secret-first"));
        assert!(!output.contains("secret-second"));
        // Usernames are secrets too: listings carry id, hostname, and userid
        // only.
        assert!(!output.contains("login-first"));
        assert!(!output.contains("login-second"));
    }

    #[test]
    fn reference_resolution_reports_ambiguous_identifiers() {
        let instances = Instances::new(vec![
            credential("dup"),
            credential("dup"),
            credential("second"),
        ])
        .unwrap();
        let error = instances.resolve_reference("dup").unwrap_err().to_string();
        assert_eq!(error, "ambiguous instance identifier");
        // The ambiguity check precedes the item_order fallback: a duplicated
        // numeric id stays ambiguous even though an index with that number
        // exists, while unique ids and bare orders still resolve.
        let numeric =
            Instances::new(vec![credential("2"), credential("2"), credential("third")]).unwrap();
        assert_eq!(
            numeric.resolve_reference("2").unwrap_err().to_string(),
            "ambiguous instance identifier"
        );
        assert_eq!(numeric.resolve_reference("third").unwrap(), 2);
        assert_eq!(numeric.resolve_reference("1").unwrap(), 1);
    }

    #[test]
    fn sessions_scope_selections_per_client_and_expire_oldest() {
        let selections = Selections::with_capacity(2);
        let one = selections.issue();
        let two = selections.issue();
        assert_eq!(selections.scope(&one), SessionScope::Default);
        assert_eq!(selections.scope(&two), SessionScope::Default);
        assert_eq!(
            selections.scope("unknown"),
            SessionScope::Expired,
            "a never-issued id is expired, not defaulted"
        );

        assert!(
            selections.select(&two, 1),
            "a live session accepts its selection"
        );
        assert_eq!(selections.scope(&two), SessionScope::Selected(1));
        assert_eq!(
            selections.scope(&one),
            SessionScope::Default,
            "one session's choice is invisible to another"
        );

        // Issuing past capacity expires the oldest session.
        let three = selections.issue();
        assert_eq!(
            selections.scope(&one),
            SessionScope::Expired,
            "oldest session expired"
        );
        assert_eq!(selections.scope(&two), SessionScope::Selected(1));
        assert_eq!(selections.scope(&three), SessionScope::Default);

        // An expired session is not resurrected by any lookup and nothing is
        // stored for it.
        assert!(!selections.select(&one, 0));
        assert_eq!(selections.scope(&one), SessionScope::Expired);

        // Fresh ids do not collide.
        assert_ne!(selections.issue(), selections.issue());
    }

    #[test]
    fn scope_resolves_liveness_and_selection_atomically() {
        // The tri-state must never offer the racy middle ground the old
        // contains-then-selected pair allowed: a session is either live with
        // its own scope (Default or Selected) or Expired — never live while
        // silently degrading to the stable default after expiry.
        let selections = Selections::with_capacity(1);
        let victim = selections.issue();
        assert_eq!(selections.scope(&victim), SessionScope::Default);

        // The eviction that expires `victim` and every subsequent lookup of
        // its scope are ordered by the same mutex, so expiry is observed as
        // Expired — the caller refuses with "initialize again" instead of
        // running the request against the stable default.
        let fresh = selections.issue();
        assert_eq!(selections.scope(&fresh), SessionScope::Default);
        assert_eq!(selections.scope(&victim), SessionScope::Expired);
        assert!(
            selections.select(&fresh, 0),
            "a live session accepts its selection"
        );
        assert_eq!(selections.scope(&fresh), SessionScope::Selected(0));
        assert_eq!(selections.scope(&victim), SessionScope::Expired);
    }

    #[test]
    fn select_reports_whether_the_session_was_live() {
        let selections = Selections::with_capacity(1);
        let expired = selections.issue();
        let live = selections.issue();
        assert_eq!(selections.scope(&live), SessionScope::Default);
        assert_eq!(selections.scope(&expired), SessionScope::Expired);

        assert!(selections.select(&live, 0));
        assert_eq!(selections.scope(&live), SessionScope::Selected(0));

        // An expired session is never resurrected and never reports success:
        // a session evicted between a request's session check and its select
        // must surface as a miss the caller turns into "initialize again".
        assert!(!selections.select(&expired, 1));
        assert_eq!(selections.scope(&expired), SessionScope::Expired);
        assert!(!selections.select("never-issued", 0));
    }
}
