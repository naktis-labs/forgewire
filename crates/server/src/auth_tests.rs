//! The authentication contract of the gateway (ADR-0001).
//!
//! With a token configured, every route needs a valid credential, and the
//! credential must carry the instance scope the token was issued for: a
//! token minted for another instance is refused even when its secret
//! matches. A refusal never reaches Foundry and never discloses content.
//! The Host and Origin defences keep working unchanged next to it.

use super::*;
use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use foundry_connector::FoundryCredential;
use serde_json::{json, Value};
use std::sync::Mutex;
use tower::ServiceExt;

/// The scope of the first configured instance.
const SCOPE: &str = "one";
/// The scope of the second configured instance.
const OTHER_SCOPE: &str = "two";
/// The secret of the fixture token.
const SECRET: &str = "fixture-token";

/// A canned connector that records the calls it receives, so a test can
/// show that a refused request never reached Foundry.
struct RecordingConnector {
    calls: Mutex<Vec<String>>,
}

impl RecordingConnector {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, call: &str) {
        self.calls.lock().unwrap().push(call.to_owned());
    }

    fn recorded(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl operations::Connector for RecordingConnector {
    async fn world(&self, _target: &FoundryCredential) -> anyhow::Result<Value> {
        self.record("world");
        Ok(json!({"title": "Test World"}))
    }

    async fn list_documents(
        &self,
        _target: &FoundryCredential,
        _collection: &str,
    ) -> anyhow::Result<Value> {
        self.record("list_documents");
        Ok(json!([]))
    }

    async fn query_documents(
        &self,
        _target: &FoundryCredential,
        _collection: &str,
        _query: Value,
    ) -> anyhow::Result<Value> {
        self.record("query_documents");
        Ok(json!([]))
    }

    async fn get_compendiums(
        &self,
        _target: &FoundryCredential,
        _kind: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.record("get_compendiums");
        Ok(json!([]))
    }

    async fn get_pack_documents(
        &self,
        _target: &FoundryCredential,
        _kind: &str,
        _pack: &str,
        _query: Value,
    ) -> anyhow::Result<Value> {
        self.record("get_pack_documents");
        Ok(json!([]))
    }

    async fn create_compendium(
        &self,
        _target: &FoundryCredential,
        _label: &str,
        _kind: &str,
    ) -> anyhow::Result<Value> {
        self.record("create_compendium");
        Ok(json!({"name": "pack-new"}))
    }

    async fn delete_compendium(
        &self,
        _target: &FoundryCredential,
        _name: &str,
    ) -> anyhow::Result<Value> {
        self.record("delete_compendium");
        Ok(json!({"deleted": true}))
    }

    async fn browse_files(
        &self,
        _target: &FoundryCredential,
        _path: &str,
        _kind: Option<&str>,
        _extensions: Option<&[String]>,
    ) -> anyhow::Result<Value> {
        self.record("browse_files");
        Ok(json!({"files": []}))
    }

    async fn upload_file(
        &self,
        _target: &FoundryCredential,
        _path: &str,
        _filename: &str,
        _data: &[u8],
    ) -> anyhow::Result<Value> {
        self.record("upload_file");
        Ok(json!({"path": "data/upload"}))
    }

    async fn create_document(
        &self,
        _target: &FoundryCredential,
        kind: &str,
        _data: Vec<Value>,
        _parent: Option<&str>,
        _pack: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.record(&format!("create_document:{kind}"));
        Ok(json!([{"_id": "new-1"}]))
    }

    async fn modify_document(
        &self,
        _target: &FoundryCredential,
        kind: &str,
        id: &str,
        _data: Vec<Value>,
        _parent: Option<&str>,
        _pack: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.record(&format!("modify_document:{kind}:{id}"));
        Ok(json!([{"_id": id}]))
    }

    async fn delete_document(
        &self,
        _target: &FoundryCredential,
        kind: &str,
        ids: Vec<String>,
        _parent: Option<&str>,
        _pack: Option<&str>,
    ) -> anyhow::Result<Value> {
        self.record(&format!("delete_document:{kind}:{ids:?}"));
        Ok(json!([]))
    }
}

fn credential(id: &str, hostname: &str) -> FoundryCredential {
    FoundryCredential {
        id: Some(id.into()),
        hostname: hostname.into(),
        userid: None,
        username: None,
        password: format!("secret-{id}"),
    }
}

/// A token issued for `scope`.
fn token_for(scope: &str) -> auth::ScopedToken {
    auth::ScopedToken::new(auth::Scope::new(scope).unwrap(), SECRET.to_owned()).unwrap()
}

/// An app with two configured instances ("one" and "two") and, optionally,
/// a token issued for one of their scopes.
fn app(token: Option<auth::ScopedToken>, allow_writes: bool) -> (Router, Arc<RecordingConnector>) {
    let connector = Arc::new(RecordingConnector::new());
    let instances = instances::Instances::new(vec![
        credential("one", "one.example"),
        credential("two", "two.example"),
    ])
    .unwrap();
    let state = App {
        client: connector.clone(),
        instances,
        selections: instances::Selections::new(),
        allow_writes,
    };
    (secured_router(Arc::new(state), token), connector)
}

/// A REST request carrying `credential` (when given) as its bearer token.
async fn rest(
    router: &Router,
    method: &str,
    path: &str,
    body: Option<Value>,
    credential: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "localhost")
        .header("content-type", "application/json");
    if let Some(credential) = credential {
        request = request.header("authorization", format!("Bearer {credential}"));
    }
    let response = router
        .clone()
        .oneshot(
            request
                .body(Body::from(
                    body.map(|body| body.to_string()).unwrap_or_default(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// An MCP request carrying `credential` (when given) as its bearer token.
async fn mcp(router: &Router, body: Value, credential: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::post("/mcp")
        .header("host", "localhost")
        .header("content-type", "application/json");
    if let Some(credential) = credential {
        request = request.header("authorization", format!("Bearer {credential}"));
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

/// With a token configured, a request without a valid credential is refused:
/// on every REST route, on the MCP entry point, and before any Foundry
/// contact.
#[tokio::test]
async fn unauthenticated_requests_are_refused() {
    let (router, connector) = app(Some(token_for(SCOPE)), false);

    for (method, path) in [
        ("GET", "/health"),
        ("GET", "/api/world"),
        ("GET", "/api/collections/actors"),
        ("GET", "/api/documents/actors"),
        ("GET", "/api/compendiums"),
        ("GET", "/api/instances"),
    ] {
        let (status, body) = rest(&router, method, path, None, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
        assert_eq!(
            body,
            Value::Null,
            "{method} {path} must not disclose content"
        );
    }

    // The MCP entry point is refused the same way: no session is issued and
    // no tool runs, so nothing about the world can leak.
    let (status, body) = mcp(
        &router,
        json!({"jsonrpc":"2.0","id":1,"method":"initialize"}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, Value::Null);

    let (status, body) = mcp(
        &router,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_world","arguments":{}}}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, Value::Null);

    assert!(
        connector.recorded().is_empty(),
        "refused requests must not reach Foundry"
    );
}

/// The token is bound to its instance scope: the same secret presented for
/// another scope is refused, exactly like a wrong secret.
#[tokio::test]
async fn a_credential_for_another_instance_scope_is_refused() {
    let (router, connector) = app(Some(token_for(SCOPE)), false);

    for credential in [
        // The right secret, the wrong scope.
        format!("{OTHER_SCOPE}.{SECRET}"),
        // The right scope, the wrong secret.
        format!("{SCOPE}.other-token"),
        // No scope at all.
        SECRET.to_owned(),
    ] {
        let (status, body) = rest(&router, "GET", "/api/world", None, Some(&credential)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{credential}");
        assert_eq!(body, Value::Null, "{credential} must not disclose content");

        let (status, body) = mcp(
            &router,
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_world","arguments":{}}}),
            Some(&credential),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{credential}");
        assert_eq!(body, Value::Null, "{credential} must not disclose content");
    }

    assert!(
        connector.recorded().is_empty(),
        "a cross-scope credential must not reach Foundry"
    );
}

/// A credential carrying the configured scope and secret is accepted, on
/// both transports.
#[tokio::test]
async fn a_credential_for_the_configured_scope_is_accepted() {
    let (router, connector) = app(Some(token_for(SCOPE)), false);
    let credential = format!("{SCOPE}.{SECRET}");

    let (status, world) = rest(&router, "GET", "/api/world", None, Some(&credential)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(world["title"], "Test World");
    assert_eq!(connector.recorded(), vec!["world".to_owned()]);

    let (status, response) = mcp(
        &router,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_world","arguments":{}}}),
        Some(&credential),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["result"]["isError"], false);
    assert_eq!(
        response["result"]["content"][0]["text"],
        "{\"title\":\"Test World\"}"
    );
}

/// The Host and Origin defences keep working unchanged next to the token:
/// a valid credential does not buy a foreign Host header or a browser
/// origin.
#[tokio::test]
async fn host_and_origin_defences_hold_with_a_valid_credential() {
    let (router, connector) = app(Some(token_for(SCOPE)), false);
    let credential = format!("{SCOPE}.{SECRET}");

    for host in [
        "evil.example",
        "localhost.evil.example",
        "localhost@evil.example",
    ] {
        let request = Request::get("/api/world")
            .header("host", host)
            .header("authorization", format!("Bearer {credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{host}");
    }

    let request = Request::get("/api/world")
        .header("host", "localhost")
        .header("origin", "null")
        .header("authorization", format!("Bearer {credential}"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    assert!(
        connector.recorded().is_empty(),
        "a refused Host or Origin must not reach Foundry"
    );
}

/// Writes need an authenticated request as well as the operator's opt-in:
/// an unauthenticated write is refused outright, the opt-in still gates
/// authenticated writes, and a deployment without a token cannot write at
/// all.
#[tokio::test]
async fn writes_require_authentication_and_the_opt_in() {
    let write = json!({"kind":"Actor","data":[{"name":"New"}]});

    // Authentication configured: an unauthenticated write never reaches
    // the write gate.
    let (router, connector) = app(Some(token_for(SCOPE)), true);
    let (status, body) = rest(
        &router,
        "POST",
        "/api/write/create_document",
        Some(write.clone()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, Value::Null);
    let (status, body) = mcp(
        &router,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_document","arguments":write}}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, Value::Null);
    assert!(
        connector.recorded().is_empty(),
        "an unauthenticated write must not reach Foundry"
    );

    // Authenticated, but writes are not opted in.
    let (router, connector) = app(Some(token_for(SCOPE)), false);
    let credential = format!("{SCOPE}.{SECRET}");
    let (status, body) = rest(
        &router,
        "POST",
        "/api/write/create_document",
        Some(write.clone()),
        Some(&credential),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"], operations::WRITES_DISABLED);
    let (status, response) = mcp(
        &router,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_document","arguments":write}}),
        Some(&credential),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(
        response["result"]["content"][0]["text"],
        operations::WRITES_DISABLED
    );
    assert!(
        connector.recorded().is_empty(),
        "the opt-in must be checked before any Foundry contact"
    );

    // Authenticated and opted in: the write reaches Foundry.
    let (router, connector) = app(Some(token_for(SCOPE)), true);
    let (status, _) = rest(
        &router,
        "POST",
        "/api/write/create_document",
        Some(write.clone()),
        Some(&credential),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        connector.recorded(),
        vec!["create_document:Actor".to_owned()]
    );

    // No token configured: a write is refused even though the opt-in is on.
    let (router, connector) = app(None, true);
    let (status, body) = rest(
        &router,
        "POST",
        "/api/write/create_document",
        Some(write.clone()),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"], operations::WRITES_REQUIRE_AUTHENTICATION);
    let (status, response) = mcp(
        &router,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_document","arguments":write}}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(
        response["result"]["content"][0]["text"],
        operations::WRITES_REQUIRE_AUTHENTICATION
    );
    assert!(
        connector.recorded().is_empty(),
        "a refused write must not reach Foundry"
    );
}

/// The configured scope names exactly one configured instance: an unknown
/// or ambiguous scope is refused at startup rather than accepted.
#[test]
fn scopes_name_exactly_one_configured_instance() {
    let instances = instances::Instances::new(vec![
        credential("one", "one.example"),
        credential("two", "two.example"),
    ])
    .unwrap();
    assert_eq!(instances.resolve_scope("one").unwrap(), 0);
    assert_eq!(instances.resolve_scope("two").unwrap(), 1);
    assert_eq!(instances.canonical_scope(0).unwrap(), "one");
    assert_eq!(instances.canonical_scope(1).unwrap(), "two");
    assert!(instances.resolve_scope("missing").is_err());
    assert!(instances.resolve_scope("0").is_err());

    // An instance without an identifier is scoped by its item order.
    let anonymous = instances::Instances::new(vec![FoundryCredential {
        id: None,
        hostname: "one.example".into(),
        userid: None,
        username: None,
        password: String::new(),
    }])
    .unwrap();
    assert_eq!(anonymous.canonical_scope(0).unwrap(), "0");
    assert_eq!(anonymous.resolve_scope("0").unwrap(), 0);
    assert!(anonymous.resolve_scope("one").is_err());
}
