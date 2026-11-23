//! REST/MCP contract tests (ADR-0001 acceptance check: "Equivalent REST/MCP
//! calls match on successful results, rejected inputs, filtering/projection/
//! limits, embedded and compendium targeting, and errors").
//!
//! Both transports must reach the same application operations in
//! `operations.rs` and agree on results, validation, and gating. A canned
//! fake connector records every application call so tests can assert that
//! REST and MCP drove identical operations against the same target.

use super::*;
use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::sync::Mutex;
use tower::ServiceExt;

use axum::http::HeaderValue;

fn actor(id: &str, name: &str, kind: &str) -> Value {
    json!({"_id": id, "id": id, "name": name, "type": kind, "system": {"hp": {"value": 10}}})
}

/// Canned connector: returns fixed documents for every read and records the
/// operation, target identity, and query so tests can assert REST and MCP
/// issued identical application calls.
struct FakeConnector {
    actors: Vec<Value>,
    pack_actors: Vec<Value>,
    compendiums: Value,
    listing: Value,
    upload_result: Value,
    write_result: Value,
    calls: Mutex<Vec<String>>,
}

impl FakeConnector {
    fn new() -> Self {
        Self {
            actors: vec![
                actor("hero-1", "Hero One", "npc"),
                actor("hero-2", "Hero Two", "npc"),
                actor("hero-3", "Hero Three", "character"),
            ],
            pack_actors: vec![actor("pack-hero", "Pack Hero", "npc")],
            compendiums: json!([
                {"name": "pack-core", "label": "Core Pack", "type": "Actor", "package": "world"},
                {"name": "pack-spells", "label": "Spells", "type": "Item", "package": "world"}
            ]),
            listing: json!({"target": "data/", "dirs": ["data/audio"], "files": [{"name": "song.mp3", "size": 100}]}),
            upload_result: json!({"path": "data/upload/song.mp3"}),
            write_result: json!([{"_id": "new-1"}]),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, entry: String) {
        self.calls.lock().unwrap().push(entry);
    }

    fn recorded(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    /// The last `count` recorded calls, for asserting two transports matched.
    fn tail(&self, count: usize) -> Vec<String> {
        let calls = self.recorded();
        let skip = calls.len().saturating_sub(count);
        calls.iter().skip(skip).cloned().collect()
    }
}

#[async_trait]
impl operations::Connector for FakeConnector {
    async fn world(&self, target: &FoundryCredential) -> anyhow::Result<Value> {
        self.record(format!("world:{}", target.hostname));
        Ok(json!({"title": "Test World", "system": "dnd5e", "version": "13"}))
    }

    async fn list_documents(
        &self,
        target: &FoundryCredential,
        collection: &str,
    ) -> anyhow::Result<Value> {
        self.record(format!("list:{collection}:{}", target.hostname));
        if collection == "actors" {
            Ok(Value::Array(self.actors.clone()))
        } else {
            Ok(json!([]))
        }
    }

    async fn query_documents(
        &self,
        target: &FoundryCredential,
        collection: &str,
        query: Value,
    ) -> anyhow::Result<Value> {
        self.record(format!("query:{collection}:{}:{query}", target.hostname));
        if collection == "actors" {
            Ok(Value::Array(self.actors.clone()))
        } else {
            Ok(json!([]))
        }
    }

    async fn get_compendiums(
        &self,
        target: &FoundryCredential,
        kind: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.record(format!(
            "compendiums:{}:{}",
            target.hostname,
            kind.unwrap_or("")
        ));
        let packs = self.compendiums.as_array().unwrap();
        Ok(json!(packs
            .iter()
            .filter(|pack| kind.is_none_or(|kind| pack["type"] == json!(kind)))
            .cloned()
            .collect::<Vec<_>>()))
    }

    async fn get_pack_documents(
        &self,
        target: &FoundryCredential,
        kind: &str,
        pack: &str,
        query: Value,
    ) -> anyhow::Result<Value> {
        self.record(format!("pack:{kind}:{pack}:{}:{query}", target.hostname));
        // Mirror the connector's `validate_pack_access` wording so tests can
        // assert how the application layer classifies a pack miss.
        if pack == "pack-missing" {
            return Err(anyhow::anyhow!(
                "Compendium pack \"pack-missing\" not found. Use get_compendiums to list the available packs."
            ));
        }
        Ok(Value::Array(self.pack_actors.clone()))
    }

    async fn create_compendium(
        &self,
        target: &FoundryCredential,
        label: &str,
        kind: &str,
    ) -> anyhow::Result<Value> {
        self.record(format!(
            "create_compendium:{}:{label}:{kind}",
            target.hostname
        ));
        Ok(json!({"name": "pack-new", "label": label, "type": kind}))
    }

    async fn delete_compendium(
        &self,
        target: &FoundryCredential,
        name: &str,
    ) -> anyhow::Result<Value> {
        self.record(format!("delete_compendium:{}:{name}", target.hostname));
        Ok(json!({"deleted": name}))
    }

    async fn browse_files(
        &self,
        target: &FoundryCredential,
        path: &str,
        kind: Option<&str>,
        extensions: Option<&[String]>,
    ) -> anyhow::Result<Value> {
        self.record(format!(
            "browse:{}:{path}:{}:{}",
            target.hostname,
            kind.unwrap_or(""),
            extensions.map(|e| e.join(",")).unwrap_or_default()
        ));
        Ok(self.listing.clone())
    }

    async fn upload_file(
        &self,
        target: &FoundryCredential,
        path: &str,
        filename: &str,
        data: &[u8],
    ) -> anyhow::Result<Value> {
        self.record(format!(
            "upload:{}:{path}:{filename}:{}bytes",
            target.hostname,
            data.len()
        ));
        Ok(self.upload_result.clone())
    }

    async fn create_document(
        &self,
        target: &FoundryCredential,
        kind: &str,
        data: Vec<Value>,
        parent: Option<&str>,
        pack: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.record(format!(
            "create:{kind}:{}:{}:{parent:?}:{pack:?}",
            target.hostname,
            serde_json::to_string(&data).unwrap()
        ));
        Ok(self.write_result.clone())
    }

    async fn modify_document(
        &self,
        target: &FoundryCredential,
        kind: &str,
        id: &str,
        data: Vec<Value>,
        parent: Option<&str>,
        pack: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.record(format!(
            "modify:{kind}:{id}:{}:{}:{parent:?}:{pack:?}",
            target.hostname,
            serde_json::to_string(&data).unwrap()
        ));
        Ok(self.write_result.clone())
    }

    async fn delete_document(
        &self,
        target: &FoundryCredential,
        kind: &str,
        ids: Vec<String>,
        parent: Option<&str>,
        pack: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.record(format!(
            "delete:{kind}:{}:{ids:?}:{parent:?}:{pack:?}",
            target.hostname
        ));
        Ok(self.write_result.clone())
    }
}

fn credential(id: &str, hostname: &str) -> FoundryCredential {
    FoundryCredential {
        id: Some(id.into()),
        hostname: hostname.into(),
        userid: None,
        username: Some(format!("login-{id}")),
        password: format!("secret-{id}"),
    }
}

fn contract_app(allow_writes: bool) -> (Router, Arc<FakeConnector>) {
    contract_app_with_sessions(allow_writes, instances::DEFAULT_SESSION_CAPACITY)
}

/// Tests that exercise session expiry inject a small capacity so a second
/// `initialize` deterministically expires the oldest session.
fn contract_app_with_sessions(
    allow_writes: bool,
    session_capacity: usize,
) -> (Router, Arc<FakeConnector>) {
    let fake = Arc::new(FakeConnector::new());
    let instances = instances::Instances::new(vec![
        credential("one", "one.example"),
        credential("two", "two.example"),
    ])
    .unwrap();
    let app = App {
        client: fake.clone(),
        instances,
        selections: instances::Selections::with_capacity(session_capacity),
        allow_writes,
    };
    (router(Arc::new(app)), fake)
}

async fn rest_send(
    router: &Router,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "localhost")
        .header("content-type", "application/json")
        .body(Body::from(
            body.map(|body| body.to_string()).unwrap_or_default(),
        ))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn rest_get(router: &Router, path: &str) -> (StatusCode, Value) {
    rest_send(router, "GET", path, None).await
}

async fn mcp_call(router: &Router, name: &str, arguments: Value) -> (StatusCode, Value) {
    mcp_call_as(router, None, name, arguments).await
}

/// Call an MCP tool as a specific session (`Mcp-Session-Id` header) or as an
/// anonymous caller when `session` is `None`.
async fn mcp_call_as(
    router: &Router,
    session: Option<&str>,
    name: &str,
    arguments: Value,
) -> (StatusCode, Value) {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":arguments}});
    let mut request = Request::post("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json");
    if let Some(id) = session {
        request = request.header("mcp-session-id", id);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Initialize an MCP client session and return the server-issued session id.
async fn mcp_initialize(router: &Router) -> String {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}});
    let request = Request::post("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .expect("initialize must issue a session id")
        .to_owned()
}

/// Send a raw MCP request (any method) with an optional session header.
/// Returns the status, the parsed body, and the server-issued session header
/// when `initialize` answered.
async fn mcp_send(
    router: &Router,
    session: Option<&str>,
    body: Value,
) -> (StatusCode, Value, Option<String>) {
    let mut request = Request::post("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json");
    if let Some(id) = session {
        request = request.header("mcp-session-id", id);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let issued = response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        issued,
    )
}

/// The value an MCP tool result carries: `structuredContent` for objects, the
/// parsed text payload otherwise.
fn mcp_value(response: &Value) -> Value {
    if let Some(structured) = response.pointer("/result/structuredContent") {
        return structured.clone();
    }
    response["result"]["content"][0]["text"]
        .as_str()
        .and_then(|text| serde_json::from_str(text).ok())
        .unwrap_or(Value::Null)
}

fn mcp_error_text(response: &Value) -> String {
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// Assert the last two recorded application calls (one per transport) are
/// identical, proving REST and MCP reached the same operation with the same
/// shaped input and target.
fn assert_transports_matched(fake: &FakeConnector) {
    let tail = fake.tail(2);
    assert_eq!(tail.len(), 2, "expected one application call per transport");
    assert_eq!(
        tail[0], tail[1],
        "REST and MCP must drive identical application calls"
    );
}

#[tokio::test]
async fn rest_and_mcp_single_document_lookups_match() {
    let (router, fake) = contract_app(false);

    let (status, rest) = rest_get(&router, "/api/documents/actors?id=hero-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rest["name"], "Hero One");
    let (_, mcp) = mcp_call(&router, "get_actor", json!({"id":"hero-1"})).await;
    assert_eq!(mcp["result"]["isError"], false);
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);

    let (_, mcp) = mcp_call(&router, "get_actor", json!({"name":"Hero Two"})).await;
    let (status, rest) = rest_get(&router, "/api/documents/actors?name=Hero%20Two").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);

    // A missing document is the same application failure for both transports.
    let (status, rest) = rest_get(&router, "/api/documents/actors?id=missing").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(rest["error"], "Document not found");
    let (_, mcp) = mcp_call(&router, "get_actor", json!({"id":"missing"})).await;
    assert_eq!(mcp["result"]["isError"], true);
    assert_eq!(mcp_error_text(&mcp), "Document not found");

    // A filter that excludes the looked-up document excludes it everywhere.
    let (status, rest) = rest_get(
        &router,
        r#"/api/documents/actors?id=hero-3&where=%7B%22type%22%3A%22npc%22%7D"#,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(rest["error"], "Document not found");
    let (_, mcp) = mcp_call(
        &router,
        "get_actor",
        json!({"id":"hero-3","where":{"type":"npc"}}),
    )
    .await;
    assert_eq!(mcp_error_text(&mcp), "Document not found");

    // Matching filter succeeds on both.
    let (status, rest) = rest_get(
        &router,
        r#"/api/documents/actors?id=hero-1&where=%7B%22type%22%3A%22npc%22%7D"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(
        &router,
        "get_actor",
        json!({"id":"hero-1","where":{"type":"npc"}}),
    )
    .await;
    assert_eq!(rest, mcp_value(&mcp));

    // Byte limit below one document is rejected identically.
    let (status, rest) = rest_get(&router, "/api/documents/actors?id=hero-1&max_length=5").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rest["error"], "document exceeds max_length");
    let (_, mcp) = mcp_call(&router, "get_actor", json!({"id":"hero-1","max_length":5})).await;
    assert_eq!(mcp_error_text(&mcp), "document exceeds max_length");
}

/// An empty REST identifier (`id=`, `_id=`, `name=`) selects the singular
/// read, whose shared parse rejects it exactly like the MCP tool, instead of
/// silently degrading into a full listing.
#[tokio::test]
async fn rest_and_mcp_reject_empty_identifiers_identically() {
    let (router, fake) = contract_app(false);
    for (key, arguments) in [
        ("id", json!({"id":""})),
        ("_id", json!({"_id":""})),
        ("name", json!({"name":""})),
    ] {
        let (status, rest) = rest_get(&router, &format!("/api/documents/actors?{key}=")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{key}=");
        assert_eq!(rest["error"], "id, _id, or name is required", "{key}=");
        let (_, mcp) = mcp_call(&router, "get_actor", arguments).await;
        assert_eq!(mcp["result"]["isError"], true, "{key}=");
        assert_eq!(mcp_error_text(&mcp), rest["error"], "{key}=");
    }
    assert!(
        fake.recorded().is_empty(),
        "rejected identifier reads must not reach the connector"
    );

    // The same shared rule applies inside compendium packs.
    let (status, rest) = rest_get(&router, "/api/compendiums/pack-core/documents/actors?id=").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rest["error"], "id, _id, or name is required");
    let (_, mcp) = mcp_call(&router, "get_actor", json!({"pack":"pack-core","id":""})).await;
    assert_eq!(mcp_error_text(&mcp), "id, _id, or name is required");
}

#[tokio::test]
async fn rest_and_mcp_filtered_projected_and_limited_reads_match() {
    let (router, fake) = contract_app(false);

    let (_, mcp) = mcp_call(
        &router,
        "get_actors",
        json!({"where":{"type":"npc"},"requested_fields":["name"]}),
    )
    .await;
    let (status, rest) = rest_get(
        &router,
        r#"/api/documents/actors?where=%7B%22type%22%3A%22npc%22%7D&requested_fields=name"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rest, mcp_value(&mcp));
    let docs = rest.as_array().unwrap();
    assert_eq!(
        docs.len(),
        2,
        "filter and projection must apply through both transports"
    );
    assert!(docs
        .iter()
        .all(|doc| doc.get("system").is_none() && doc["name"].is_string()));
    assert_transports_matched(&fake);

    // Byte limits truncate identically with stable ordering: the same prefix
    // of the unbounded result survives on both transports.
    let (_, full) = rest_get(&router, "/api/documents/actors?requested_fields=name").await;
    let docs = full.as_array().unwrap();
    let limit = serde_json::to_vec(&[docs[0].clone(), docs[1].clone()])
        .unwrap()
        .len();
    let (status, rest) = rest_get(
        &router,
        &format!("/api/documents/actors?requested_fields=name&max_length={limit}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(
        &router,
        "get_actors",
        json!({"requested_fields":["name"],"max_length":limit}),
    )
    .await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_eq!(rest.as_array().unwrap().len(), 2);
    assert_eq!(&rest.as_array().unwrap()[0], &docs[0]);
    assert_eq!(&rest.as_array().unwrap()[1], &docs[1]);
    assert_transports_matched(&fake);

    // Plain collection listings match too.
    let (status, rest) = rest_get(&router, "/api/collections/actors").await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(&router, "list_documents", json!({"collection":"actors"})).await;
    assert_eq!(rest, mcp_value(&mcp));

    let (status, rest) = rest_get(&router, "/api/world").await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(&router, "get_world", json!({})).await;
    assert_eq!(rest, mcp_value(&mcp));
}

/// `/api/world` and `/api/collections/{name}` run through the same typed
/// error mapping as every other route: unknown collections are caller
/// mistakes (400 envelope), and the documented collection set — including
/// `journal`, whose plural tool name differs — is accepted identically by
/// both transports.
#[tokio::test]
async fn rest_world_and_collection_reads_share_validation_and_error_envelope() {
    let (router, fake) = contract_app(false);
    let (status, rest) = rest_get(&router, "/api/collections/no_such_collection").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rest["error"], "unsupported collection");
    let (_, mcp) = mcp_call(
        &router,
        "list_documents",
        json!({"collection":"no_such_collection"}),
    )
    .await;
    assert_eq!(mcp["result"]["isError"], true);
    assert_eq!(mcp_error_text(&mcp), rest["error"]);
    assert!(
        fake.recorded().is_empty(),
        "unknown collections must be rejected before any connector call"
    );

    for collection in ["actors", "journal"] {
        let (status, rest) = rest_get(&router, &format!("/api/collections/{collection}")).await;
        assert_eq!(status, StatusCode::OK, "{collection}");
        let (_, mcp) = mcp_call(&router, "list_documents", json!({"collection":collection})).await;
        assert_eq!(rest, mcp_value(&mcp), "{collection}");
    }
}

#[tokio::test]
async fn rest_and_mcp_compendium_reads_match() {
    let (router, fake) = contract_app(false);

    let (status, rest) = rest_get(&router, "/api/compendiums?type=Item").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rest.as_array().unwrap().len(), 1, "type filter must apply");
    let (_, mcp) = mcp_call(&router, "get_compendiums", json!({"type":"Item"})).await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);

    let (status, rest) = rest_get(&router, "/api/compendiums").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rest.as_array().unwrap().len(), 2);
    let (_, mcp) = mcp_call(&router, "get_compendiums", json!({})).await;
    assert_eq!(rest, mcp_value(&mcp));

    // Pack document lookup by id through both transports.
    let (status, rest) = rest_get(
        &router,
        "/api/compendiums/pack-core/documents/actors?id=pack-hero",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rest["name"], "Pack Hero");
    let (_, mcp) = mcp_call(
        &router,
        "get_actor",
        json!({"pack":"pack-core","id":"pack-hero"}),
    )
    .await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);

    // Plural pack listing through both transports.
    let (status, rest) = rest_get(&router, "/api/compendiums/pack-core/documents/actors").await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(&router, "get_actors", json!({"pack":"pack-core"})).await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);

    // Non-packable document types are rejected by both transports; the
    // messages differ because the transports describe different syntaxes.
    let (status, rest) = rest_get(&router, "/api/compendiums/pack-core/documents/users").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (_, mcp) = mcp_call(
        &router,
        "get_user",
        json!({"pack":"pack-core","id":"user-1"}),
    )
    .await;
    assert_eq!(mcp["result"]["isError"], true);
    assert_eq!(rest["error"], "unsupported compendium document type");
    assert_eq!(
        mcp_error_text(&mcp),
        "this document type cannot be stored in a compendium"
    );
}

/// An unknown compendium pack is a lookup miss, not an upstream fault: REST
/// answers 404 and MCP shows the same message, on both transports' shared
/// read path.
#[tokio::test]
async fn rest_and_mcp_map_unknown_packs_to_not_found() {
    let (router, fake) = contract_app(false);
    let (status, rest) = rest_get(
        &router,
        "/api/compendiums/pack-missing/documents/actors?id=hero",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        rest["error"],
        "Compendium pack \"pack-missing\" not found. Use get_compendiums to list the available packs."
    );
    let (_, mcp) = mcp_call(
        &router,
        "get_actor",
        json!({"pack":"pack-missing","id":"hero"}),
    )
    .await;
    assert_eq!(mcp["result"]["isError"], true);
    assert_eq!(mcp_error_text(&mcp), rest["error"]);
    assert_transports_matched(&fake);
}

#[tokio::test]
async fn rest_and_mcp_file_browsing_matches() {
    let (router, fake) = contract_app(false);

    let (status, rest) = rest_get(
        &router,
        "/api/files?target=data&type=folder&extensions=png,jpg",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(
        &router,
        "browse_files",
        json!({"target":"data","type":"folder","extensions":["png","jpg"]}),
    )
    .await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);

    // The `kind` alias is rejected with the same application message.
    let (status, rest) = rest_get(&router, "/api/files?kind=data").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rest["error"], "use type instead of kind");
    let (_, mcp) = mcp_call(&router, "browse_files", json!({"kind":"data"})).await;
    assert_eq!(mcp["result"]["isError"], true);
    assert_eq!(mcp_error_text(&mcp), "use type instead of kind");
}

#[tokio::test]
async fn rest_and_mcp_compendium_writes_share_gating_validation_and_effects() {
    // Disabled: both refuse before any connector contact.
    let (router, fake) = contract_app(false);
    let (status, rest) = rest_send(
        &router,
        "POST",
        "/api/compendiums",
        Some(json!({"label":"Pack","type":"Actor"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        rest["error"],
        "writes are disabled; set FOUNDRY_ALLOW_WRITES=true to enable"
    );
    let (_, mcp) = mcp_call(
        &router,
        "create_compendium",
        json!({"label":"Pack","type":"Actor"}),
    )
    .await;
    assert_eq!(mcp["result"]["isError"], true);
    assert_eq!(mcp_error_text(&mcp), rest["error"]);
    assert!(
        fake.recorded().is_empty(),
        "gating must precede any connector call"
    );

    // Enabled but invalid: identical validation failure, connector untouched.
    let (router, fake) = contract_app(true);
    let (status, rest) = rest_send(
        &router,
        "POST",
        "/api/compendiums",
        Some(json!({"type":"Actor"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rest["error"], "label must be a non-empty string");
    let (_, mcp) = mcp_call(&router, "create_compendium", json!({"type":"Actor"})).await;
    assert_eq!(mcp_error_text(&mcp), "label must be a non-empty string");
    assert!(fake.recorded().is_empty());

    // Enabled and valid: both drives reach the connector identically.
    let (status, rest) = rest_send(
        &router,
        "POST",
        "/api/compendiums",
        Some(json!({"label":"Pack","type":"Actor"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(
        &router,
        "create_compendium",
        json!({"label":"Pack","type":"Actor"}),
    )
    .await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);
    assert!(fake.tail(1)[0].starts_with("create_compendium:one.example:Pack:Actor"));

    let (status, rest) = rest_send(&router, "DELETE", "/api/compendiums/pack-old", None).await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(&router, "delete_compendium", json!({"name":"pack-old"})).await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);
}

#[tokio::test]
async fn rest_and_mcp_document_writes_share_gating_validation_and_effects() {
    let (router, fake) = contract_app(false);
    for (operation, args) in [
        (
            "create_document",
            json!({"kind":"Actor","data":[{"name":"New"}]}),
        ),
        (
            "modify_document",
            json!({"kind":"Actor","id":"hero-1","updates":[{"name":"Renamed"}]}),
        ),
        ("delete_document", json!({"kind":"Actor","ids":["hero-1"]})),
    ] {
        let (status, rest) = rest_send(
            &router,
            "POST",
            &format!("/api/write/{operation}"),
            Some(args.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{operation}");
        let (_, mcp) = mcp_call(&router, operation, args).await;
        assert_eq!(mcp["result"]["isError"], true, "{operation}");
        assert_eq!(mcp_error_text(&mcp), rest["error"]);
    }
    assert!(
        fake.recorded().is_empty(),
        "gating must precede any connector call"
    );

    // Enabled: identical validation, connector effects, and results.
    let (router, fake) = contract_app(true);
    let (status, rest) = rest_send(
        &router,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"Actor","data":[{"name":"New"}],"pack":"pack-core"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(
        &router,
        "create_document",
        json!({"kind":"Actor","data":[{"name":"New"}],"pack":"pack-core"}),
    )
    .await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);

    let (status, rest) = rest_send(
        &router,
        "POST",
        "/api/write/modify_document",
        Some(json!({"kind":"Actor","id":"hero-1","updates":[{"name":"Renamed"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(
        &router,
        "modify_document",
        json!({"kind":"Actor","id":"hero-1","updates":[{"name":"Renamed"}]}),
    )
    .await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);

    let (status, rest) = rest_send(
        &router,
        "POST",
        "/api/write/delete_document",
        Some(json!({"kind":"Actor","ids":["hero-1","hero-2"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(
        &router,
        "delete_document",
        json!({"kind":"Actor","ids":["hero-1","hero-2"]}),
    )
    .await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);

    // Invalid writes are rejected identically before any connector call.
    let before = fake.recorded().len();
    let (status, rest) = rest_send(
        &router,
        "POST",
        "/api/write/create_document",
        Some(json!({"kind":"Actor"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rest["error"], "data must be a non-empty array of objects");
    let (_, mcp) = mcp_call(&router, "create_document", json!({"kind":"Actor"})).await;
    assert_eq!(
        mcp_error_text(&mcp),
        "data must be a non-empty array of objects"
    );
    assert_eq!(fake.recorded().len(), before);
}

#[tokio::test]
async fn rest_and_mcp_uploads_share_gating_overwrite_and_byte_parity() {
    let payload = STANDARD.encode(b"hello world");
    let (router, fake) = contract_app(false);
    let args =
        json!({"target":"data/upload","filename":"a.png","data_base64":payload,"overwrite":true});
    let (status, rest) = rest_send(&router, "POST", "/api/files", Some(args.clone())).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (_, mcp) = mcp_call(&router, "upload_file", args).await;
    assert_eq!(mcp_error_text(&mcp), rest["error"]);
    assert!(fake.recorded().is_empty());

    // Enabled but the overwrite acknowledgement is missing, false, or not a
    // boolean: identical rejection on both transports.
    let (router, fake) = contract_app(true);
    for overwrite in [Value::Null, json!(false), json!("yes")] {
        let mut args = json!({"target":"data/upload","filename":"a.png","data_base64":payload});
        if !overwrite.is_null() {
            args["overwrite"] = overwrite;
        }
        let (status, rest) = rest_send(&router, "POST", "/api/files", Some(args.clone())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (_, mcp) = mcp_call(&router, "upload_file", args).await;
        assert_eq!(mcp["result"]["isError"], true);
        assert_eq!(mcp_error_text(&mcp), rest["error"]);
    }
    assert!(
        fake.recorded().is_empty(),
        "rejected uploads must not reach the connector"
    );

    // Enabled and acknowledged: identical decoded bytes reach the connector.
    let args =
        json!({"target":"data/upload","filename":"a.png","data_base64":payload,"overwrite":true});
    let (status, rest) = rest_send(&router, "POST", "/api/files", Some(args.clone())).await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(&router, "upload_file", args).await;
    assert_eq!(rest, mcp_value(&mcp));
    assert_transports_matched(&fake);
    assert!(
        fake.tail(1)[0].ends_with("a.png:11bytes"),
        "both transports must decode the same bytes"
    );

    // The application-level size cap rejects oversized uploads through the
    // same shared operation regardless of transport framing.
    let before = fake.recorded().len();
    let oversized = STANDARD.encode(vec![0u8; foundry_connector::files::MAX_UPLOAD_BYTES + 1]);
    let error = operations::upload_file(
        &*fake,
        &credential("one", "one.example"),
        true,
        &json!({"target":"data/upload","filename":"big.png","data_base64":oversized,"overwrite":true}),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("exceeds maximum size"), "{error}");
    assert_eq!(
        fake.recorded().len(),
        before,
        "oversized upload must not reach the connector"
    );
}

/// REST targeting is explicit or the documented stable default: without
/// `?instance=` every request hits the first configured entry, with it the
/// named instance is used, and unknown references are rejected before any
/// connector contact. Listing flags which entry a reference would target
/// without ever disclosing secrets.
#[tokio::test]
async fn rest_targets_the_stable_default_or_an_explicit_instance() {
    let (router, fake) = contract_app(true);

    // Default: the first configured entry.
    let (status, _) = rest_get(&router, "/api/world").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        fake.tail(1)[0],
        "world:one.example",
        "the default target is the first configured entry"
    );

    // Explicit by configured id...
    let (status, _) = rest_get(&router, "/api/world?instance=two").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fake.tail(1)[0], "world:two.example");
    // ...or by zero-based item_order.
    let (status, _) = rest_get(&router, "/api/world?instance=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fake.tail(1)[0], "world:two.example");

    // Writes target explicitly too.
    let (status, _) = rest_send(
        &router,
        "POST",
        "/api/write/create_document?instance=two",
        Some(json!({"kind":"Actor","data":[{"name":"New"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(fake.tail(1)[0].contains("create:Actor:two.example:"));

    // Unknown references are caller mistakes rejected before any contact.
    for reference in ["missing", "9"] {
        let (status, rest) = rest_get(&router, &format!("/api/world?instance={reference}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{reference}");
        assert_eq!(rest["error"], "instance not found", "{reference}");
    }
    assert_eq!(
        fake.recorded().len(),
        4,
        "rejected targets must not reach the connector"
    );

    // A leading '+' is malformed addressing, not a signed order: it is
    // refused, never normalized into item_order 1.
    let (status, rest) = rest_get(&router, "/api/world?instance=%2B1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(rest["error"], "invalid instance reference");
    assert_eq!(
        fake.recorded().len(),
        4,
        "a '+'-prefixed reference must not reach the connector"
    );

    // Listing flags which entry the requested reference would target.
    let (status, listing) = rest_get(&router, "/api/instances?instance=two").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listing[1]["currently_active"], true);
    assert_eq!(listing[0]["currently_active"], false);
    let listed = listing.to_string();
    assert!(
        !listed.contains("secret-two") && !listed.contains("login-two"),
        "instance listing must not disclose passwords or usernames"
    );
    let (status, listing) = rest_get(&router, "/api/instances").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        listing[0]["currently_active"], true,
        "the stable default is the first configured entry"
    );
    assert!(!listing.to_string().contains("login-one"));
}

/// MCP selection is scoped to the client's own session: one session's choice
/// is invisible to another session, to anonymous MCP callers, and to REST,
/// whose stable default never moves. Selection reports selection only, and
/// the old process-global REST selection endpoint is gone.
#[tokio::test]
async fn mcp_selection_is_session_scoped_and_never_redirects_rest() {
    let (router, fake) = contract_app(false);

    let session_a = mcp_initialize(&router).await;
    let session_b = mcp_initialize(&router).await;
    assert_ne!(session_a, session_b, "each client gets its own session");

    // Discovery parity between REST and anonymous MCP, without secrets.
    let (status, rest) = rest_get(&router, "/api/instances").await;
    assert_eq!(status, StatusCode::OK);
    let (_, mcp) = mcp_call(&router, "show_credentials", json!({})).await;
    assert_eq!(rest, mcp_value(&mcp));
    assert!(rest.to_string().contains("one.example"));
    assert!(!rest.to_string().contains("secret-one"));

    // Session A selects instance two; only A's own calls move.
    let (status, selected) = mcp_call_as(
        &router,
        Some(&session_a),
        "choose_foundry_instance",
        json!({"_id":"two"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        selected["result"]["structuredContent"],
        json!({"item_order": 1, "selected": true}),
        "selection reports selection only, not a verified connection"
    );
    let (_, mcp) = mcp_call_as(&router, Some(&session_a), "get_world", json!({})).await;
    assert_eq!(mcp["result"]["isError"], false);
    assert_eq!(
        fake.tail(1)[0],
        "world:two.example",
        "the selecting session reads from its own target"
    );

    // Another session, anonymous MCP callers, and REST are untouched.
    let (_, mcp) = mcp_call_as(&router, Some(&session_b), "get_world", json!({})).await;
    assert_eq!(mcp["result"]["isError"], false);
    assert_eq!(
        fake.tail(1)[0],
        "world:one.example",
        "another session must not inherit the selection"
    );
    let (_, mcp) = mcp_call(&router, "get_world", json!({})).await;
    assert_eq!(mcp["result"]["isError"], false);
    assert_eq!(
        fake.tail(1)[0],
        "world:one.example",
        "anonymous MCP calls use the stable default"
    );
    let (status, _) = rest_get(&router, "/api/world").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        fake.tail(1)[0],
        "world:one.example",
        "an MCP selection must not change REST's default target"
    );

    // Selecting without a session has no scope to record the choice in.
    let (_, mcp) = mcp_call(&router, "choose_foundry_instance", json!({"_id":"two"})).await;
    assert_eq!(mcp["result"]["isError"], true);
    assert!(mcp_error_text(&mcp).contains("session"));
    // Invalid selections inside a session fail without retargeting it.
    let (_, mcp) = mcp_call_as(
        &router,
        Some(&session_b),
        "choose_foundry_instance",
        json!({"item_order":9}),
    )
    .await;
    assert_eq!(mcp["result"]["isError"], true);
    let (_, mcp) = mcp_call_as(&router, Some(&session_b), "get_world", json!({})).await;
    assert_eq!(mcp["result"]["isError"], false);
    assert_eq!(fake.tail(1)[0], "world:one.example");

    // The old process-global selection endpoint is gone: no REST caller can
    // retarget anyone else anymore.
    let (status, _) = rest_send(
        &router,
        "PUT",
        "/api/instances/selection",
        Some(json!({"_id":"two"})),
    )
    .await;
    assert_ne!(status, StatusCode::OK);
}

/// Session capacity is injected so expiry is testable end to end: when the
/// table is full the oldest session expires, after which its requests are
/// refused with 404 and the client is told to initialize again. The expired
/// session is never resurrected — not by its own requests, not by
/// re-initializing while still carrying its header — and its old selection
/// never leaks into the sessions that follow. Writes are no exception: an
/// expired session's write fails with the initialize-again error instead of
/// silently running against the stable default instance.
#[tokio::test]
async fn expired_sessions_are_refused_until_the_client_initializes_again() {
    let (router, fake) = contract_app_with_sessions(true, 1);

    let session_a = mcp_initialize(&router).await;
    let (status, selected) = mcp_call_as(
        &router,
        Some(&session_a),
        "choose_foundry_instance",
        json!({"item_order":1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(selected["result"]["structuredContent"]["selected"], true);

    // The next initialize fills the table and expires session A.
    let session_b = mcp_initialize(&router).await;
    assert_ne!(session_a, session_b);

    // Every tool call — including a selection attempt — is refused with 404
    // and the initialize-again guidance instead of resurrecting the session.
    for name in ["get_world", "choose_foundry_instance", "show_credentials"] {
        let (status, body) =
            mcp_call_as(&router, Some(&session_a), name, json!({"item_order":1})).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{name}");
        assert_eq!(
            body["error"], "unknown or expired MCP session; initialize again",
            "{name}"
        );
    }

    // A write is refused the same way even though writes are enabled: the
    // expired session must not silently run the write against the stable
    // default instance (or anywhere else).
    let (status, body) = mcp_call_as(
        &router,
        Some(&session_a),
        "create_document",
        json!({"kind":"Actor","data":[{"name":"Written while expired"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body["error"], "unknown or expired MCP session; initialize again",
        "an expired session's write must fail with the initialize-again error"
    );
    assert!(
        fake.recorded().is_empty(),
        "expired-session refusals must not reach the connector"
    );

    // Re-initializing while still carrying the expired header must not revive
    // it: a brand new id is issued and the old one stays dead.
    let (status, body, issued) = mcp_send(
        &router,
        Some(&session_a),
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["protocolVersion"], "2025-06-18");
    let session_c = issued.expect("initialize must issue a fresh session id");
    assert_ne!(session_c, session_a, "the expired id must not be reused");
    let (status, body) = mcp_call_as(&router, Some(&session_a), "get_world", json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "the old id stays expired");
    assert_eq!(
        body["error"],
        "unknown or expired MCP session; initialize again"
    );

    // The fresh session works and starts on the stable default: the expired
    // session's selection did not leak into it.
    let (_, mcp) = mcp_call_as(&router, Some(&session_c), "get_world", json!({})).await;
    assert_eq!(mcp["result"]["isError"], false);
    assert_eq!(
        fake.tail(1)[0],
        "world:one.example",
        "an expired session's selection must not leak into later sessions"
    );

    // Positive control for the refusals above: a live session's write targets
    // its own selection, so the 404s are specifically about expiry, and a
    // session's writes follow its selection rather than the stable default.
    let (status, selected) = mcp_call_as(
        &router,
        Some(&session_c),
        "choose_foundry_instance",
        json!({"item_order":1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(selected["result"]["structuredContent"]["selected"], true);
    let (status, _) = mcp_call_as(
        &router,
        Some(&session_c),
        "create_document",
        json!({"kind":"Actor","data":[{"name":"Written while live"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        fake.tail(1)[0].starts_with("create:Actor:two.example:"),
        "a live session's write must target its own selection, got {}",
        fake.tail(1)[0]
    );
}

/// The `Mcp-Session-Id` header must be exactly one non-empty UTF-8 value:
/// duplicated, empty, or non-UTF-8 headers are caller mistakes refused with
/// 400 before any session lookup or connector contact.
#[tokio::test]
async fn duplicate_and_non_utf8_session_headers_are_refused() {
    let (router, fake) = contract_app(false);
    let session = mcp_initialize(&router).await;

    let send = |headers: Vec<HeaderValue>| {
        let router = router.clone();
        async move {
            let mut request = Request::post("/mcp")
                .header("host", "localhost")
                .header("content-type", "application/json");
            for value in headers {
                request = request.header("mcp-session-id", value);
            }
            let response = router
                .oneshot(
                    request
                        .body(Body::from(
                            json!({"jsonrpc":"2.0","id":1,"method":"ping"}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (
                status,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        }
    };

    // Two session headers: whose scope would apply is undefined, so refuse.
    let (status, body) = send(vec![
        HeaderValue::from_str(&session).unwrap(),
        HeaderValue::from_str("another-session").unwrap(),
    ])
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid MCP session header");

    // An empty value identifies no session.
    let (status, body) = send(vec![HeaderValue::from_str("").unwrap()]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid MCP session header");

    // 0xFF is a legal header byte but not UTF-8.
    let (status, body) = send(vec![HeaderValue::from_bytes(&[0xFF]).unwrap()]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "invalid MCP session header");

    // A well-formed header still works after the refusals.
    let (status, body) = send(vec![HeaderValue::from_str(&session).unwrap()]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], json!({}));

    assert!(
        fake.recorded().is_empty(),
        "refused headers must not reach the connector"
    );
}

/// Every Foundry-contacting REST route resolves `?instance=` the same way: a
/// valid reference targets that instance for that one request, and an invalid
/// reference is a 400 before the connector is contacted. The non-contacting
/// listing route honors the parameter too. An MCP session's selection never
/// moves REST: REST keeps its stable default and its own explicit targeting.
#[tokio::test]
async fn every_foundry_rest_route_honors_the_instance_parameter() {
    let (router, fake) = contract_app(true);

    let routes: [(&str, &str, Option<Value>); 10] = [
        ("GET", "/api/world", None),
        ("GET", "/api/collections/actors", None),
        ("GET", "/api/documents/actors?id=hero-1", None),
        ("GET", "/api/compendiums", None),
        (
            "GET",
            "/api/compendiums/pack-core/documents/actors?id=pack-hero",
            None,
        ),
        ("DELETE", "/api/compendiums/pack-core", None),
        (
            "POST",
            "/api/compendiums",
            Some(json!({"label":"Pack","type":"Actor"})),
        ),
        ("GET", "/api/files?target=data", None),
        (
            "POST",
            "/api/files",
            Some(
                json!({"target":"data/upload","filename":"a.png","data_base64":STANDARD.encode(b"x"),"overwrite":true}),
            ),
        ),
        (
            "POST",
            "/api/write/create_document",
            Some(json!({"kind":"Actor","data":[{"name":"New"}]})),
        ),
    ];
    let with_instance = |path: &str, reference: &str| {
        if path.contains('?') {
            format!("{path}&instance={reference}")
        } else {
            format!("{path}?instance={reference}")
        }
    };

    for (method, path, body) in routes {
        let before = fake.recorded().len();
        let (status, _) =
            rest_send(&router, method, &with_instance(path, "two"), body.clone()).await;
        assert_eq!(status, StatusCode::OK, "{method} {path}");
        let calls = fake.recorded();
        assert_eq!(calls.len(), before + 1, "{method} {path}");
        assert!(
            calls.last().unwrap().contains("two.example"),
            "{method} {path} must target the requested instance, got {}",
            calls.last().unwrap()
        );

        // An invalid reference is a caller mistake rejected before any
        // connector contact, on every route alike.
        let (status, error) = rest_send(
            &router,
            method,
            &with_instance(path, "missing"),
            body.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}");
        assert_eq!(error["error"], "instance not found", "{method} {path}");
        assert_eq!(
            fake.recorded().len(),
            before + 1,
            "{method} {path} must not reach the connector on a bad reference"
        );
    }

    // The listing route never contacts Foundry but honors the parameter and
    // rejects invalid references the same way, without disclosing secrets.
    let (status, listing) = rest_get(&router, "/api/instances?instance=two").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listing[1]["currently_active"], true);
    let listed = listing.to_string();
    assert!(!listed.contains("secret-two") && !listed.contains("login-two"));
    let (status, error) = rest_get(&router, "/api/instances?instance=missing").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["error"], "instance not found");

    // An MCP session's selection never affects REST: the session's own calls
    // follow its selection while REST keeps the stable default, and REST's
    // explicit references stay independent of anything MCP chose.
    let session = mcp_initialize(&router).await;
    let (status, selected) = mcp_call_as(
        &router,
        Some(&session),
        "choose_foundry_instance",
        json!({"_id":"two"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(selected["result"]["structuredContent"]["selected"], true);

    let (_, mcp) = mcp_call_as(&router, Some(&session), "get_world", json!({})).await;
    assert_eq!(mcp["result"]["isError"], false);
    assert_eq!(
        fake.tail(1)[0],
        "world:two.example",
        "the session's own calls honor its selection"
    );
    let (status, _) = rest_get(&router, "/api/world").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        fake.tail(1)[0],
        "world:one.example",
        "an MCP selection must not move REST's default"
    );
    let (status, _) = rest_get(&router, "/api/world?instance=two").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        fake.tail(1)[0],
        "world:two.example",
        "REST keeps its own explicit targeting"
    );
    let (status, _) = rest_get(&router, "/api/world").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        fake.tail(1)[0],
        "world:one.example",
        "REST's default stays stable after explicit and MCP targeting"
    );
}
