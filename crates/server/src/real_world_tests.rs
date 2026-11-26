//! Read and write checks against a real, disposable Foundry instance.
//!
//! These checks only run when `FORGEWIRE_REAL_WORLD_URL` is set, so the
//! ordinary suite never touches a real world. They drive the gateway's REST
//! and MCP adapters over the real connector, which is the path an operator
//! uses, and they exercise the instance rather than a protocol simulation.
//!
//! The instance must be a throwaway world: the write checks create, change
//! and delete documents, create and delete a compendium, and upload a file.
//! `FORGEWIRE_REAL_WORLD_USERNAME` and `FORGEWIRE_REAL_WORLD_PASSWORD` must
//! name a user of that world; `FORGEWIRE_REAL_WORLD_USERID` is optional and
//! only used when the user is known by id.
//!
//! The checks share one world, so they take a lock and run one at a time,
//! whether or not the test runner runs them in parallel.

use super::*;
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use foundry_connector::{FoundryClient, FoundryCredential};
use tower::ServiceExt;

/// The scope of the disposable instance's credential.
const SCOPE: &str = "real";

/// The lock the checks take one at a time.
///
/// They share one world, and a check that ran beside another could read or
/// change the documents the other one is working with.
static LIVE_WORLD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A one pixel PNG, so the upload carries real image bytes.
const PNG_1X1: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
    0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
    0x42, 0x60, 0x82,
];

/// The world's own directory in the instance's data storage.
///
/// The instance's data root holds one directory per world, so the world's
/// own directory is what a browse and an upload name.
fn world_data_target() -> String {
    std::env::var("FORGEWIRE_REAL_WORLD_DATA_TARGET")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "worlds/forgewire-test".to_owned())
}

/// The system the world runs, when the instance is expected to run one.
fn expected_system() -> Option<String> {
    std::env::var("FORGEWIRE_REAL_WORLD_SYSTEM")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// The disposable instance under test, when one is configured.
///
/// A missing credential is a mistake rather than a reason to skip: a run
/// that is meant to reach a real instance has to say so.
fn real_world() -> Option<(FoundryClient, FoundryCredential)> {
    let url = std::env::var("FORGEWIRE_REAL_WORLD_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())?;
    let username = std::env::var("FORGEWIRE_REAL_WORLD_USERNAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            panic!("FORGEWIRE_REAL_WORLD_USERNAME must be set when FORGEWIRE_REAL_WORLD_URL is set")
        });
    let password = std::env::var("FORGEWIRE_REAL_WORLD_PASSWORD").unwrap_or_default();
    let userid = std::env::var("FORGEWIRE_REAL_WORLD_USERID")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let credential = FoundryCredential {
        id: Some(SCOPE.to_owned()),
        hostname: url.clone(),
        userid,
        username: Some(username),
        password,
    };
    let client = FoundryClient::for_loopback(&url).expect("a loopback client");
    Some((client, credential))
}

/// The gateway over the real instance, with writes enabled for the disposable
/// world and a bearer token for the instance's scope.
///
/// The lock the checks share is returned with it, so the check holds it until
/// it is done.
async fn gateway() -> Option<(Router, String, tokio::sync::MutexGuard<'static, ()>)> {
    let (client, credential) = real_world()?;
    let app = Arc::new(App {
        client: Arc::new(client),
        instances: instances::Instances::new(vec![credential]).unwrap(),
        selections: instances::Selections::new(),
        allow_writes: true,
    });
    let token = auth::ScopedToken::new(auth::Scope::new(SCOPE).unwrap(), "real-secret".to_owned())
        .expect("a scoped token");
    let bearer = format!("{SCOPE}.real-secret");
    let guard = LIVE_WORLD.lock().await;
    Some((secured_router(app, Some(token)), bearer, guard))
}

/// A REST request with the instance's bearer credential.
async fn rest(
    router: &Router,
    bearer: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let builder = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "127.0.0.1:30000")
        .header("authorization", format!("Bearer {bearer}"));
    let request = match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// An MCP tool call with the instance's bearer credential.
async fn mcp(router: &Router, bearer: &str, name: &str, arguments: Value) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::post("/mcp")
                .header("host", "127.0.0.1:30000")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {bearer}"))
                .body(Body::from(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": name, "arguments": arguments},
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// The JSON payload an MCP tool call returned.
fn mcp_payload(value: &Value) -> Value {
    assert_eq!(
        value["result"]["isError"], false,
        "the MCP call failed: {value}"
    );
    serde_json::from_str(
        value["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or("null"),
    )
    .unwrap_or(Value::Null)
}

/// The documents of a collection, read over REST.
async fn documents_in(router: &Router, bearer: &str, name: &str) -> Vec<Value> {
    let (status, value) = rest(
        router,
        bearer,
        "GET",
        &format!("/api/documents/{name}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{name}: {value}");
    value.as_array().cloned().unwrap_or_default()
}

/// The id of the instance's first user, the author a chat message needs.
async fn user_id(router: &Router, bearer: &str) -> String {
    let users = documents_in(router, bearer, "users").await;
    users
        .first()
        .and_then(|user| user["_id"].as_str())
        .expect("a user id")
        .to_owned()
}

/// The suffix every document this run creates carries, so a run never
/// collides with the documents an earlier run left behind.
fn run_suffix() -> String {
    format!("{}", rand::random::<u32>())
}

/// Whether a document holds an embedded document of that id.
///
/// A parent document keeps its embedded documents in arrays of their own, so
/// the check walks the parent's whole body rather than naming the arrays,
/// which differ per parent kind.
fn holds_id(parent: &Value, id: &str) -> bool {
    match parent {
        Value::Array(items) => items.iter().any(|item| holds_id(item, id)),
        Value::Object(map) => {
            map.values().any(|value| holds_id(value, id))
                || map.get("_id").and_then(Value::as_str) == Some(id)
        }
        _ => false,
    }
}

/// The embedded document of that id inside a parent document, if the parent
/// holds one.
fn embedded(parent: &Value, id: &str) -> Option<Value> {
    match parent {
        Value::Array(items) => items.iter().find_map(|item| embedded(item, id)),
        Value::Object(map) => {
            if map.get("_id").and_then(Value::as_str) == Some(id) {
                return Some(parent.clone());
            }
            map.values().find_map(|value| embedded(value, id))
        }
        _ => None,
    }
}

#[tokio::test]
async fn the_real_instance_serves_reads_over_both_transports() {
    let Some((router, bearer, _world)) = gateway().await else {
        eprintln!("skipped: FORGEWIRE_REAL_WORLD_URL is not set");
        return;
    };

    // World metadata.
    let (status, world) = rest(&router, &bearer, "GET", "/api/world", None).await;
    assert_eq!(status, StatusCode::OK, "{world}");
    assert!(
        world["system"]
            .as_str()
            .is_some_and(|system| !system.is_empty()),
        "{world}"
    );
    if let Some(expected) = expected_system() {
        assert_eq!(world["system"], expected.as_str(), "{world}");
    }
    assert!(world["title"].is_string(), "{world}");
    assert!(world["id"].is_string(), "{world}");
    // The world descriptor the reference exposes and the gateway narrowed:
    // the world's kind, the core-version range it declares, the release
    // channel the instance runs, and how long it has been played.
    assert!(world["type"].is_string(), "{world}");
    assert!(world["compatibility"]["minimum"].is_string(), "{world}");
    assert!(world["compatibility"]["verified"].is_string(), "{world}");
    assert!(world["release_channel"].is_string(), "{world}");
    assert!(world["playtime"].is_number(), "{world}");
    assert!(world["last_played"].is_string(), "{world}");
    // The instance's own session state never reaches the metadata.
    assert!(world.get("users").is_none(), "{world}");
    assert!(world.get("session").is_none(), "{world}");
    assert!(world.get("flags").is_none(), "{world}");

    // The same read over MCP matches the REST read.
    let (status, mcp_world) = mcp(&router, &bearer, "get_world", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{mcp_world}");
    let mcp_world = mcp_payload(&mcp_world);
    assert_eq!(mcp_world["id"], world["id"], "{mcp_world}");
    assert_eq!(mcp_world["system"], world["system"], "{mcp_world}");
    assert_eq!(mcp_world["type"], world["type"], "{mcp_world}");
    assert_eq!(
        mcp_world["compatibility"], world["compatibility"],
        "{mcp_world}"
    );
    assert_eq!(
        mcp_world["release_channel"], world["release_channel"],
        "{mcp_world}"
    );

    // A collection read.
    let (status, actors) = rest(&router, &bearer, "GET", "/api/documents/actors", None).await;
    assert_eq!(status, StatusCode::OK, "{actors}");
    assert!(actors.is_array(), "{actors}");

    // The same collection over MCP.
    let (status, mcp_actors) = mcp(&router, &bearer, "get_actors", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{mcp_actors}");
    let mcp_actors = mcp_payload(&mcp_actors);
    assert_eq!(
        mcp_actors.as_array().map(|a| a.len()),
        actors.as_array().map(|a| a.len())
    );

    // Compendium listing.
    let (status, packs) = rest(&router, &bearer, "GET", "/api/compendiums", None).await;
    assert_eq!(status, StatusCode::OK, "{packs}");
    let packs = packs.as_array().cloned().unwrap_or_default();
    assert!(!packs.is_empty(), "the instance reported no compendiums");
    for pack in &packs {
        assert!(
            pack["id"].as_str().is_some_and(|id| !id.is_empty()),
            "{pack}"
        );
        assert!(
            pack["label"]
                .as_str()
                .is_some_and(|label| !label.is_empty()),
            "{pack}"
        );
    }
    // A world pack reports the bare name a delete takes next to its id.
    for pack in packs.iter().filter(|pack| {
        pack["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("world."))
    }) {
        let id = pack["id"].as_str().expect("a pack id");
        assert_eq!(pack["name"].as_str(), id.strip_prefix("world."), "{pack}");
    }
    // A package pack reports no name: its name is only unique inside its
    // package and would name a different pack in the world scope.
    let package_packs = packs
        .iter()
        .filter(|pack| {
            pack["id"]
                .as_str()
                .is_some_and(|id| !id.starts_with("world."))
        })
        .count();
    assert!(package_packs > 0, "the instance reported no package pack");
    for pack in packs.iter().filter(|pack| {
        pack["id"]
            .as_str()
            .is_some_and(|id| !id.starts_with("world."))
    }) {
        assert!(pack.get("name").is_none(), "{pack}");
    }
    let actor_packs: Vec<String> = packs
        .iter()
        .filter(|pack| pack["type"] == "Actor" && pack["id"].is_string())
        .map(|pack| pack["id"].as_str().expect("a pack id").to_owned())
        .collect();
    assert!(
        !actor_packs.is_empty(),
        "the instance reported no actor compendium: {packs:?}"
    );

    // A compendium document read. A pack may legitimately be empty, so the
    // first populated actor pack is the one that has to serve documents.
    let mut read_pack = None;
    for pack_id in &actor_packs {
        let (status, documents) = rest(
            &router,
            &bearer,
            "GET",
            &format!("/api/compendiums/{pack_id}/documents/actors"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{documents}");
        let documents = documents.as_array().cloned().unwrap_or_default();
        if !documents.is_empty() {
            read_pack = Some(documents);
            break;
        }
    }
    let documents = read_pack.expect("no actor compendium served documents");
    assert!(documents[0]["_id"].is_string(), "{documents:?}");

    // File browsing.
    let (status, files) = rest(
        &router,
        &bearer,
        "GET",
        &format!("/api/files?target={}", world_data_target()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{files}");
    assert!(files["dirs"].is_array(), "{files}");
    assert!(files["files"].is_array(), "{files}");
}

#[tokio::test]
async fn the_real_instance_serves_every_collection_over_both_transports() {
    let Some((router, bearer, _world)) = gateway().await else {
        eprintln!("skipped: FORGEWIRE_REAL_WORLD_URL is not set");
        return;
    };

    // Every collection the gateway reads, with the tool that reads it.
    let collections = [
        ("actors", "get_actors"),
        ("items", "get_items"),
        ("folders", "get_folders"),
        ("users", "get_users"),
        ("scenes", "get_scenes"),
        ("journal", "get_journals"),
        ("macros", "get_macros"),
        ("cards", "get_cards"),
        ("playlists", "get_playlists"),
        ("tables", "get_tables"),
        ("combats", "get_combats"),
        ("messages", "get_messages"),
        ("settings", "get_settings"),
    ];

    for (collection, tool) in collections {
        let (status, value) = rest(
            &router,
            &bearer,
            "GET",
            &format!("/api/documents/{collection}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{collection}: {value}");
        let documents = value.as_array().cloned().unwrap_or_default();

        // The same collection over MCP.
        let (status, value) = mcp(&router, &bearer, tool, json!({})).await;
        assert_eq!(status, StatusCode::OK, "{collection}: {value}");
        let mcp_documents = mcp_payload(&value);
        assert_eq!(
            mcp_documents.as_array().map(|d| d.len()),
            Some(documents.len()),
            "{collection}: {mcp_documents:?}"
        );
    }

    // The collection listing the gateway offers beside the reads.
    let (status, listed) = rest(&router, &bearer, "GET", "/api/collections/actors", None).await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert!(listed.is_array(), "{listed}");
}

#[tokio::test]
async fn the_real_instance_filters_projects_and_limits_reads() {
    let Some((router, bearer, _world)) = gateway().await else {
        eprintln!("skipped: FORGEWIRE_REAL_WORLD_URL is not set");
        return;
    };

    // A document to read back. The name is unique per run, so a filter on it
    // selects exactly one document even in a world that was used before.
    let name = format!("Forgewire Filter Actor {}", rand::random::<u32>());
    let (status, created) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"Actor","data":[{"name":name,"type":"npc"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let id = created[0]["_id"].as_str().expect("a created id").to_owned();

    // A filter selects by field.
    let (status, filtered) = mcp(
        &router,
        &bearer,
        "get_actors",
        json!({"where":{"name":name}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{filtered}");
    let filtered = mcp_payload(&filtered);
    let filtered = filtered.as_array().cloned().unwrap_or_default();
    assert_eq!(filtered.len(), 1, "{filtered:?}");
    assert_eq!(filtered[0]["_id"], id.as_str(), "{filtered:?}");

    // A single document by id, and by name.
    let (status, single) = mcp(&router, &bearer, "get_actor", json!({"id":id})).await;
    assert_eq!(status, StatusCode::OK, "{single}");
    let single = mcp_payload(&single);
    assert_eq!(single["name"], name, "{single}");

    let (status, by_name) = mcp(&router, &bearer, "get_actor", json!({"name":name})).await;
    assert_eq!(status, StatusCode::OK, "{by_name}");
    let by_name = mcp_payload(&by_name);
    assert_eq!(by_name["_id"], id.as_str(), "{by_name}");

    // A projection keeps the fields the caller asked for.
    let (status, projected) = mcp(
        &router,
        &bearer,
        "get_actor",
        json!({"id":id,"requested_fields":["img"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{projected}");
    let projected = mcp_payload(&projected);
    let keys: Vec<&str> = projected
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert!(keys.contains(&"img"), "{keys:?}");
    assert!(!keys.contains(&"prototypeToken"), "{keys:?}");

    // A byte limit is refused rather than answered with a partial document.
    let (status, limited) = mcp(
        &router,
        &bearer,
        "get_actor",
        json!({"id":id,"max_length":10}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{limited}");
    assert_eq!(limited["result"]["isError"], true, "{limited}");
    assert!(
        limited["result"]["content"][0]["text"]
            .as_str()
            .is_some_and(|text| text.contains("exceeds max_length")),
        "{limited}"
    );

    // Remove the document again.
    let (status, deleted) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/delete_document",
        Some(json!({"kind":"Actor","ids":[id]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
}

#[tokio::test]
async fn the_real_instance_accepts_writes_over_both_transports() {
    let Some((router, bearer, _world)) = gateway().await else {
        eprintln!("skipped: FORGEWIRE_REAL_WORLD_URL is not set");
        return;
    };

    // Create a document. The name carries the run's suffix, so a run never
    // collides with a document an earlier run left behind.
    let suffix = run_suffix();
    let name = format!("Forgewire Interop Actor {suffix}");
    let (status, created) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"Actor","data":[{"name":name,"type":"npc"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let id = created[0]["_id"].as_str().expect("a created id").to_owned();
    assert_eq!(created[0]["name"], name.as_str(), "{created}");

    // The change is visible to a read.
    let (status, actors) = rest(&router, &bearer, "GET", "/api/documents/actors", None).await;
    assert_eq!(status, StatusCode::OK, "{actors}");
    let actors = actors.as_array().cloned().unwrap_or_default();
    let found = actors
        .iter()
        .find(|actor| actor["_id"] == id.as_str())
        .unwrap_or_else(|| panic!("the created actor was not readable: {actors:?}"));
    assert_eq!(found["name"], name.as_str(), "{found}");

    // Change it.
    let renamed = format!("Forgewire Interop Actor (renamed) {suffix}");
    let (status, changed) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/modify_document",
        Some(json!({"kind":"Actor","id":id,"updates":[{"name":renamed}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{changed}");
    let (status, actors) = rest(&router, &bearer, "GET", "/api/documents/actors", None).await;
    assert_eq!(status, StatusCode::OK, "{actors}");
    let actors = actors.as_array().cloned().unwrap_or_default();
    let found = actors
        .iter()
        .find(|actor| actor["_id"] == id.as_str())
        .unwrap_or_else(|| panic!("the changed actor was not readable: {actors:?}"));
    assert_eq!(found["name"], renamed.as_str(), "{found}");

    // A read through MCP sees the same change.
    let (status, mcp_actor) = mcp(&router, &bearer, "get_actor", json!({"id":id})).await;
    assert_eq!(status, StatusCode::OK, "{mcp_actor}");
    let mcp_actor = mcp_payload(&mcp_actor);
    assert_eq!(mcp_actor["name"], renamed.as_str(), "{mcp_actor}");

    // Delete it.
    let (status, deleted) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/delete_document",
        Some(json!({"kind":"Actor","ids":[id]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    let (status, actors) = rest(&router, &bearer, "GET", "/api/documents/actors", None).await;
    assert_eq!(status, StatusCode::OK, "{actors}");
    let actors = actors.as_array().cloned().unwrap_or_default();
    assert!(
        !actors.iter().any(|actor| actor["_id"] == id.as_str()),
        "the deleted actor is still readable: {actors:?}"
    );
}

/// The document kinds the gateway can write, with the payload the real
/// instance accepts, the collection the document is read back from, and the
/// name the change is expected to leave behind.
///
/// Every name carries the run's own suffix, so a run never collides with the
/// documents an earlier run left behind.
fn writable_kinds(
    suffix: &str,
    scene_id: &str,
    author: &str,
) -> Vec<(String, &'static str, Value, Value, String)> {
    let named = |kind: &str| format!("Forgewire {kind} {suffix}");
    vec![
        (
            "Actor".to_owned(),
            "actors",
            json!([{"name":named("Actor"),"type":"npc"}]),
            json!([{"name":named("Actor renamed")}]),
            named("Actor renamed"),
        ),
        (
            "Item".to_owned(),
            "items",
            json!([{"name":named("Item"),"type":"consumable"}]),
            json!([{"name":named("Item renamed")}]),
            named("Item renamed"),
        ),
        (
            "Folder".to_owned(),
            "folders",
            json!([{"name":named("Folder"),"type":"Item"}]),
            json!([{"name":named("Folder renamed")}]),
            named("Folder renamed"),
        ),
        (
            "User".to_owned(),
            "users",
            json!([{"name":named("User")}]),
            json!([{"name":named("User renamed")}]),
            named("User renamed"),
        ),
        (
            "Scene".to_owned(),
            "scenes",
            json!([{"name":named("Scene")}]),
            json!([{"name":named("Scene renamed")}]),
            named("Scene renamed"),
        ),
        (
            "JournalEntry".to_owned(),
            "journal",
            json!([{"name":named("Journal")}]),
            json!([{"name":named("Journal renamed")}]),
            named("Journal renamed"),
        ),
        (
            "Macro".to_owned(),
            "macros",
            json!([{"name":named("Macro"),"command":"/noop"}]),
            json!([{"name":named("Macro renamed")}]),
            named("Macro renamed"),
        ),
        (
            "Cards".to_owned(),
            "cards",
            json!([{"name":named("Cards"),"type":"deck","cards":[{"name":"card","text":"text"}]}]),
            json!([{"name":named("Cards renamed")}]),
            named("Cards renamed"),
        ),
        (
            "Playlist".to_owned(),
            "playlists",
            json!([{"name":named("Playlist"),"songs":[]}]),
            json!([{"name":named("Playlist renamed")}]),
            named("Playlist renamed"),
        ),
        (
            "RollTable".to_owned(),
            "tables",
            json!([{"name":named("Table"),"results":[{"range":[1,6],"text":"1"}]}]),
            json!([{"name":named("Table renamed")}]),
            named("Table renamed"),
        ),
        (
            "Combat".to_owned(),
            "combats",
            json!([{"name":named("Combat"),"scene":scene_id}]),
            json!([{"name":named("Combat renamed")}]),
            named("Combat renamed"),
        ),
        (
            "ChatMessage".to_owned(),
            "messages",
            json!([{"content":"<p>Forgewire</p>","author":author}]),
            json!([{"content":"<p>Forgewire changed</p>"}]),
            String::new(),
        ),
        (
            "Setting".to_owned(),
            "settings",
            json!([{"key":format!("forgewire.setting.{suffix}"),"value":"1"}]),
            json!([{"value":"2"}]),
            String::new(),
        ),
    ]
}

#[tokio::test]
async fn the_real_instance_writes_every_document_kind() {
    let Some((router, bearer, _world)) = gateway().await else {
        eprintln!("skipped: FORGEWIRE_REAL_WORLD_URL is not set");
        return;
    };

    // A scene for the combat, and the user a chat message is authored by.
    let suffix = run_suffix();
    let (status, scene) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"Scene","data":[{"name":format!("Forgewire Kind Scene {suffix}")}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{scene}");
    let scene_id = scene[0]["_id"].as_str().expect("a scene id").to_owned();
    let author = user_id(&router, &bearer).await;

    for (kind, collection, data, updates, renamed) in writable_kinds(&suffix, &scene_id, &author) {
        // Create.
        let (status, created) = rest(
            &router,
            &bearer,
            "POST",
            "/api/write/create_document",
            Some(json!({"kind":kind,"data":data})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {created}");
        let id = created[0]["_id"].as_str().expect("a created id").to_owned();

        // Read back.
        let documents = documents_in(&router, &bearer, collection).await;
        let found = documents
            .iter()
            .find(|document| document["_id"] == id.as_str())
            .unwrap_or_else(|| panic!("{kind}: the created document is not readable"));
        assert_eq!(found["_id"], id.as_str(), "{kind}: {found}");

        // Change.
        let (status, changed) = rest(
            &router,
            &bearer,
            "POST",
            "/api/write/modify_document",
            Some(json!({"kind":kind,"id":id,"updates":updates})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {changed}");

        // Read the change back.
        let documents = documents_in(&router, &bearer, collection).await;
        let found = documents
            .iter()
            .find(|document| document["_id"] == id.as_str())
            .unwrap_or_else(|| panic!("{kind}: the changed document is not readable"));
        match kind.as_str() {
            "Setting" => assert_eq!(found["value"], "2", "{kind}: {found}"),
            "ChatMessage" => assert_eq!(found["content"], "<p>Forgewire changed</p>", "{kind}"),
            _ => assert_eq!(found["name"], renamed.as_str(), "{kind}: {found}"),
        }

        // Delete.
        let (status, deleted) = rest(
            &router,
            &bearer,
            "POST",
            "/api/write/delete_document",
            Some(json!({"kind":kind,"ids":[id]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {deleted}");

        // The document is gone.
        let documents = documents_in(&router, &bearer, collection).await;
        assert!(
            !documents
                .iter()
                .any(|document| document["_id"] == id.as_str()),
            "{kind}: the deleted document is still readable"
        );
    }

    // Remove the scene the combat referred to.
    let (status, deleted) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/delete_document",
        Some(json!({"kind":"Scene","ids":[scene_id]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
}

#[tokio::test]
async fn the_real_instance_writes_every_embedded_kind() {
    let Some((router, bearer, _world)) = gateway().await else {
        eprintln!("skipped: FORGEWIRE_REAL_WORLD_URL is not set");
        return;
    };

    // The parents the embedded documents hang from.
    let suffix = run_suffix();
    let (status, scene) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(
            json!({"kind":"Scene","data":[{"name":format!("Forgewire Embedded Scene {suffix}")}]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{scene}");
    let scene_id = scene[0]["_id"].as_str().expect("a scene id").to_owned();
    let scene_parent = format!("Scene.{scene_id}");

    let (status, actor) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"Actor","data":[{"name":format!("Forgewire Embedded Actor {suffix}"),"type":"npc"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{actor}");
    let actor_id = actor[0]["_id"].as_str().expect("an actor id").to_owned();
    let actor_parent = format!("Actor.{actor_id}");

    let (status, combat) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(
            json!({"kind":"Combat","data":[{"name":format!("Forgewire Embedded Combat {suffix}"),"scene":scene_id}]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{combat}");
    let combat_id = combat[0]["_id"].as_str().expect("a combat id").to_owned();
    let combat_parent = format!("Combat.{combat_id}");
    let author = user_id(&router, &bearer).await;

    // The embedded kinds the instance accepts, with the parent they hang
    // from, the collection the parent is read back from, the parent's id,
    // and the update the instance accepts for that kind. Each kind carries
    // its own field: a wall has no name, and a note has text instead.
    struct EmbeddedCase {
        kind: &'static str,
        data: Value,
        parent: String,
        parent_collection: &'static str,
        parent_id: String,
        update: Value,
        field: &'static str,
        expected: Value,
    }
    let kinds: Vec<EmbeddedCase> = vec![
        EmbeddedCase {
            kind: "Combatant",
            data: json!([{"name":"Forgewire Combatant"}]),
            parent: combat_parent.clone(),
            parent_collection: "combats",
            parent_id: combat_id.clone(),
            update: json!({"name":"Forgewire Combatant Updated"}),
            field: "name",
            expected: json!("Forgewire Combatant Updated"),
        },
        EmbeddedCase {
            kind: "ActiveEffect",
            data: json!([{"name":"Forgewire Effect"}]),
            parent: actor_parent.clone(),
            parent_collection: "actors",
            parent_id: actor_id.clone(),
            update: json!({"name":"Forgewire Effect Updated"}),
            field: "name",
            expected: json!("Forgewire Effect Updated"),
        },
        EmbeddedCase {
            kind: "Drawing",
            data: json!([{"x":0,"y":0,"author":author,"shape":{"type":"rect","width":100,"height":50}}]),
            parent: scene_parent.clone(),
            parent_collection: "scenes",
            parent_id: scene_id.clone(),
            update: json!({"x":25}),
            field: "x",
            expected: json!(25),
        },
        EmbeddedCase {
            kind: "Note",
            data: json!([{"text":"Forgewire Note","x":0,"y":0}]),
            parent: scene_parent.clone(),
            parent_collection: "scenes",
            parent_id: scene_id.clone(),
            update: json!({"text":"Forgewire Note Updated"}),
            field: "text",
            expected: json!("Forgewire Note Updated"),
        },
        EmbeddedCase {
            kind: "Tile",
            data: json!([{"x":0,"y":0,"width":100,"height":100,"text":"Forgewire Tile"}]),
            parent: scene_parent.clone(),
            parent_collection: "scenes",
            parent_id: scene_id.clone(),
            update: json!({"x":25}),
            field: "x",
            expected: json!(25),
        },
        EmbeddedCase {
            kind: "Token",
            data: json!([{"x":0,"y":0,"name":"Forgewire Token"}]),
            parent: scene_parent.clone(),
            parent_collection: "scenes",
            parent_id: scene_id.clone(),
            update: json!({"name":"Forgewire Token Updated"}),
            field: "name",
            expected: json!("Forgewire Token Updated"),
        },
        EmbeddedCase {
            kind: "Wall",
            data: json!([{"c":[0,0,100,0],"light":0,"sight":0,"sound":0,"door":0,"move":0}]),
            parent: scene_parent.clone(),
            parent_collection: "scenes",
            parent_id: scene_id.clone(),
            // A wall's sense types are enumerated, not booleans: 0 is none,
            // 10 limited, 20 normal.
            update: json!({"light":10}),
            field: "light",
            expected: json!(10),
        },
        EmbeddedCase {
            kind: "AmbientLight",
            data: json!([{"x":0,"y":0}]),
            parent: scene_parent.clone(),
            parent_collection: "scenes",
            parent_id: scene_id.clone(),
            update: json!({"x":25}),
            field: "x",
            expected: json!(25),
        },
        EmbeddedCase {
            kind: "AmbientSound",
            data: json!([{"x":0,"y":0}]),
            parent: scene_parent.clone(),
            parent_collection: "scenes",
            parent_id: scene_id.clone(),
            update: json!({"x":25}),
            field: "x",
            expected: json!(25),
        },
    ];

    for case in kinds {
        let kind = case.kind;
        let (status, created) = rest(
            &router,
            &bearer,
            "POST",
            "/api/write/create_document",
            Some(json!({"kind":kind,"data":case.data,"parent_uuid":case.parent})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {created}");
        let id = created[0]["_id"].as_str().expect("a created id").to_owned();

        // The document hangs from its parent, so the parent holds it.
        let parent_document = documents_in(&router, &bearer, case.parent_collection)
            .await
            .into_iter()
            .find(|document| document["_id"] == case.parent_id.as_str())
            .unwrap_or_else(|| panic!("{kind}: the parent was not readable"));
        assert!(
            holds_id(&parent_document, &id),
            "{kind}: the parent does not hold the created document"
        );

        // The document can be updated in place, and the parent shows it.
        let (status, updated) = rest(
            &router,
            &bearer,
            "POST",
            "/api/write/modify_document",
            Some(json!({"kind":kind,"id":id,"updates":[case.update],"parent_uuid":case.parent})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {updated}");
        let parent_document = documents_in(&router, &bearer, case.parent_collection)
            .await
            .into_iter()
            .find(|document| document["_id"] == case.parent_id.as_str())
            .unwrap_or_else(|| panic!("{kind}: the parent was not readable"));
        let embedded_document = embedded(&parent_document, &id)
            .unwrap_or_else(|| panic!("{kind}: the parent does not hold the updated document"));
        assert_eq!(
            embedded_document[case.field], case.expected,
            "{kind}: the update is not visible in the parent"
        );

        let (status, deleted) = rest(
            &router,
            &bearer,
            "POST",
            "/api/write/delete_document",
            Some(json!({"kind":kind,"ids":[id],"parent_uuid":case.parent})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {deleted}");

        // And the parent no longer holds it.
        let parent_document = documents_in(&router, &bearer, case.parent_collection)
            .await
            .into_iter()
            .find(|document| document["_id"] == case.parent_id.as_str())
            .unwrap_or_else(|| panic!("{kind}: the parent was not readable"));
        assert!(
            !holds_id(&parent_document, &id),
            "{kind}: the parent still holds the deleted document"
        );
    }

    // A kind the instance refuses without answering cannot be reported as
    // applied or refused, so the gateway has to leave the outcome open.
    let (status, refused) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"MeasuredTemplate","data":[{"t":"circle","distance":5}],"parent_uuid":scene_parent})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{refused}");
    assert_eq!(refused["outcome"], "possibly_applied", "{refused}");

    // Remove the parents again.
    for (kind, id) in [
        ("Combat", combat_id),
        ("Scene", scene_id),
        ("Actor", actor_id),
    ] {
        let (status, deleted) = rest(
            &router,
            &bearer,
            "POST",
            "/api/write/delete_document",
            Some(json!({"kind":kind,"ids":[id]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {deleted}");
    }
}

#[tokio::test]
async fn the_real_instance_accepts_compendium_and_upload_writes() {
    let Some((router, bearer, _world)) = gateway().await else {
        eprintln!("skipped: FORGEWIRE_REAL_WORLD_URL is not set");
        return;
    };

    // Create a compendium. A pack left behind by an aborted run would carry
    // the same id, so remove it first.
    let label = "Forgewire Interop";
    let (status, packs) = rest(&router, &bearer, "GET", "/api/compendiums", None).await;
    assert_eq!(status, StatusCode::OK, "{packs}");
    for entry in packs.as_array().cloned().unwrap_or_default() {
        if entry["label"] == label {
            let (status, removed) = rest(
                &router,
                &bearer,
                "DELETE",
                &format!("/api/compendiums/{}", entry["id"].as_str().unwrap()),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{removed}");
        }
    }

    let (status, pack) = rest(
        &router,
        &bearer,
        "POST",
        "/api/compendiums",
        Some(json!({"label":label,"type":"Actor"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{pack}");
    let created = &pack["result"];
    assert_eq!(created["label"], label, "{pack}");
    let pack_id = created["id"].as_str().expect("a pack id").to_owned();

    // The compendium is listed.
    let (status, packs) = rest(&router, &bearer, "GET", "/api/compendiums", None).await;
    assert_eq!(status, StatusCode::OK, "{packs}");
    let packs = packs.as_array().cloned().unwrap_or_default();
    assert!(
        packs.iter().any(|entry| entry["id"] == pack_id.as_str()),
        "{packs:?}"
    );

    // Delete it. The delete takes the pack id a read reports.
    let (status, deleted) = rest(
        &router,
        &bearer,
        "DELETE",
        &format!("/api/compendiums/{pack_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    let (status, packs) = rest(&router, &bearer, "GET", "/api/compendiums", None).await;
    assert_eq!(status, StatusCode::OK, "{packs}");
    let packs = packs.as_array().cloned().unwrap_or_default();
    assert!(
        !packs.iter().any(|entry| entry["id"] == pack_id.as_str()),
        "the deleted compendium is still listed: {packs:?}"
    );

    // A pack the world does not advertise is refused before it reaches the
    // instance, whether it is named by its own name or by a pack id. The
    // refusal is a lookup miss, so it is reported as "not found".
    for requested in ["forgewire-no-such-pack", "world.forgewire-no-such-pack"] {
        let (status, refused) = rest(
            &router,
            &bearer,
            "DELETE",
            &format!("/api/compendiums/{requested}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{requested}: {refused}");
        assert!(
            refused["error"]
                .as_str()
                .is_some_and(|error| error.contains("not found")),
            "{requested}: {refused}"
        );
    }

    // Upload bytes. The upload endpoint takes a directory relative to the
    // instance's data root, so the world's own directory is the target.
    let payload = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, PNG_1X1);
    let (status, uploaded) = rest(
        &router,
        &bearer,
        "POST",
        "/api/files",
        Some(json!({
            "target":world_data_target(),
            "filename":"forgewire-interop.png",
            "data_base64":payload,
            "overwrite":true,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{uploaded}");
    let path = uploaded["path"]
        .as_str()
        .expect("an upload path")
        .to_owned();
    assert!(path.contains("forgewire-interop.png"), "{uploaded}");

    // The file is browsable.
    let (status, files) = rest(
        &router,
        &bearer,
        "GET",
        &format!("/api/files?target={}", world_data_target()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{files}");
    let listed = files["files"].as_array().cloned().unwrap_or_default();
    assert!(
        listed.iter().any(|file| file
            .as_str()
            .is_some_and(|path| path.ends_with("forgewire-interop.png"))),
        "{files}"
    );

    // An upload into a directory the instance does not have is refused by
    // the instance, and the gateway reports the refusal rather than a
    // successful upload.
    let (status, refused) = rest(
        &router,
        &bearer,
        "POST",
        "/api/files",
        Some(json!({
            "target":"worlds/forgewire-does-not-exist",
            "filename":"forgewire-interop.png",
            "data_base64":payload,
            "overwrite":true,
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{refused}");
    assert!(
        refused["error"]
            .as_str()
            .is_some_and(|error| error.contains("does not exist")),
        "{refused}"
    );
    assert_eq!(refused["outcome"], "possibly_applied", "{refused}");
}

/// A compendium pack holds documents, and the gateway writes them the same
/// way it writes world documents: create, read back, update, read back,
/// delete, read back.
#[tokio::test]
async fn the_real_instance_mutates_documents_inside_a_compendium_pack() {
    let Some((router, bearer, _world)) = gateway().await else {
        eprintln!("skipped: FORGEWIRE_REAL_WORLD_URL is not set");
        return;
    };

    let suffix = run_suffix();
    let label = format!("Forgewire Pack {suffix}");
    let (status, pack) = rest(
        &router,
        &bearer,
        "POST",
        "/api/compendiums",
        Some(json!({"label":label,"type":"Actor"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{pack}");
    let pack_id = pack["result"]["id"].as_str().expect("a pack id").to_owned();

    // Create a document inside the pack.
    let name = format!("Forgewire Pack Actor {suffix}");
    let (status, created) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"Actor","data":[{"name":name,"type":"npc"}],"pack":pack_id})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let id = created[0]["_id"].as_str().expect("a created id").to_owned();

    // It is readable in the pack.
    let (status, documents) = rest(
        &router,
        &bearer,
        "GET",
        &format!("/api/compendiums/{pack_id}/documents/actors"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{documents}");
    let documents = documents.as_array().cloned().unwrap_or_default();
    assert!(
        documents
            .iter()
            .any(|document| document["_id"] == id.as_str()),
        "{documents:?}"
    );

    // Update it inside the pack.
    let updated_name = format!("Forgewire Pack Actor Updated {suffix}");
    let (status, updated) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/modify_document",
        Some(json!({"kind":"Actor","id":id,"updates":[{"name":updated_name}],"pack":pack_id})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");

    // The update is visible in the pack.
    let (status, documents) = rest(
        &router,
        &bearer,
        "GET",
        &format!("/api/compendiums/{pack_id}/documents/actors"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{documents}");
    let documents = documents.as_array().cloned().unwrap_or_default();
    assert!(
        documents
            .iter()
            .any(|document| document["_id"] == id.as_str()
                && document["name"] == updated_name.as_str()),
        "{documents:?}"
    );

    // Delete it from the pack.
    let (status, deleted) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/delete_document",
        Some(json!({"kind":"Actor","ids":[id],"pack":pack_id})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");

    // It is gone from the pack.
    let (status, documents) = rest(
        &router,
        &bearer,
        "GET",
        &format!("/api/compendiums/{pack_id}/documents/actors"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{documents}");
    let documents = documents.as_array().cloned().unwrap_or_default();
    assert!(
        !documents
            .iter()
            .any(|document| document["_id"] == id.as_str()),
        "{documents:?}"
    );

    // And the pack itself is removed.
    let (status, removed) = rest(
        &router,
        &bearer,
        "DELETE",
        &format!("/api/compendiums/{pack_id}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{removed}");
}

#[tokio::test]
async fn the_real_instance_reports_bad_requests() {
    let Some((router, bearer, _world)) = gateway().await else {
        eprintln!("skipped: FORGEWIRE_REAL_WORLD_URL is not set");
        return;
    };

    // A collection the gateway does not serve.
    let (status, refused) = rest(&router, &bearer, "GET", "/api/documents/planes", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");

    // A document kind the gateway does not serve.
    let (status, refused) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"Spaceship","data":[{"name":"nope"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");

    // A change without the data it needs.
    let (status, refused) = rest(
        &router,
        &bearer,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"Actor","data":[]})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");

    // A single read without an identifier.
    let (status, refused) = mcp(&router, &bearer, "get_actor", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{refused}");
    assert_eq!(refused["result"]["isError"], true, "{refused}");
}
