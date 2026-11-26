//! End-to-end MCP reads through the real connector and a loopback fake world.
use super::*;
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use foundry_connector::interop::{FakeWorld, WorldFixture};
use tower::ServiceExt;

async fn mcp(router: &Router, name: &str, arguments: Value) -> Value {
    let response = router.clone().oneshot(
        Request::post("/mcp").header("host", "localhost")
            .header("content-type", "application/json")
            .body(Body::from(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":arguments}}).to_string())).unwrap()
    ).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn mcp_journal_reads_reach_real_connector_and_disposable_world() {
    let world = FakeWorld::start_with_world(WorldFixture::journal_world()).await;
    let credential = FoundryCredential {
        id: Some("fixture".into()),
        hostname: world.base_url().into(),
        userid: Some("user-1".into()),
        username: Some("Interop User".into()),
        password: String::new(),
    };
    let app = Arc::new(App {
        client: Arc::new(FoundryClient::for_loopback(world.base_url()).unwrap()),
        instances: instances::Instances::new(vec![credential]).unwrap(),
        selections: instances::Selections::new(),
        allow_writes: false,
    });
    let router = secured_router(app, None);
    let list = mcp(&router, "get_journals", json!({})).await;
    assert_eq!(list["result"]["isError"], false, "{list}");
    let entries: Value =
        serde_json::from_str(list["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(entries.as_array().unwrap().len(), 2);
    let entry = mcp(&router, "get_journal", json!({"id":"journal-2"})).await;
    assert_eq!(entry["result"]["isError"], false, "{entry}");
    assert_eq!(
        entry["result"]["structuredContent"]["name"],
        "Interop Session Two"
    );
    let log = world.request_log();
    assert!(log.iter().any(|r| r == "GET /api/status"), "{log:?}");
    assert!(
        log.iter()
            .any(|r| r.starts_with("POST /join origin_ok=true cookie_ok=true auth_fields_ok=true")),
        "{log:?}"
    );
    assert!(
        log.iter().any(|r| r.starts_with("WS cookie_present=true")),
        "{log:?}"
    );
    let requests: Vec<_> = log
        .iter()
        .filter(|r| r.contains("modifyDocument"))
        .collect();
    assert_eq!(requests.len(), 2, "{log:?}");
    assert!(
        requests
            .iter()
            .all(|r| r.contains("JournalEntry") && r.contains("\"action\":\"get\"")),
        "{log:?}"
    );
    assert!(requests.iter().any(|r| r.contains("journal-2")), "{log:?}");
}

#[tokio::test]
async fn an_authenticated_change_reaches_the_real_connector_over_the_identity_connection() {
    let world = FakeWorld::start_with_world(WorldFixture::journal_world()).await;
    let credential = FoundryCredential {
        id: Some("fixture".into()),
        hostname: world.base_url().into(),
        userid: Some("user-1".into()),
        username: Some("Interop User".into()),
        password: String::new(),
    };
    let app = Arc::new(App {
        client: Arc::new(FoundryClient::for_loopback(world.base_url()).unwrap()),
        instances: instances::Instances::new(vec![credential]).unwrap(),
        selections: instances::Selections::new(),
        allow_writes: true,
    });
    let token = auth::ScopedToken::new(
        auth::Scope::new("fixture").unwrap(),
        "interop-secret".to_owned(),
    )
    .unwrap();
    let router = secured_router(app, Some(token));

    // A read first, so the identity's connection exists.
    let read = router
        .clone()
        .oneshot(
            Request::get("/api/documents/journal")
                .header("host", "localhost")
                .header("authorization", "Bearer fixture.interop-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::post("/api/write/create_document")
                .header("host", "localhost")
                .header("content-type", "application/json")
                .header("authorization", "Bearer fixture.interop-secret")
                .body(Body::from(
                    json!({"kind":"Actor","data":[{"name":"Interop Hero"}]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body[0]["_id"], "created-0", "{body}");

    // The read and the change shared the identity's one authenticated socket.
    let log = world.request_log();
    let sockets: std::collections::HashSet<_> = log
        .iter()
        .filter(|entry| entry.contains("modifyDocument"))
        .filter_map(|entry| entry.split("socket=").nth(1))
        .collect();
    assert_eq!(
        sockets.len(),
        1,
        "the change opened its own socket: {log:?}"
    );
    assert_eq!(
        log.iter()
            .filter(|entry| entry.contains("modifyDocument"))
            .count(),
        2,
        "both the read and the change should reach the world: {log:?}"
    );
}

#[tokio::test]
async fn a_change_that_loses_its_socket_is_reported_as_possibly_applied() {
    let world = FakeWorld::start_with_world(WorldFixture::journal_world()).await;
    world.drop_socket_on_mutation();
    let credential = FoundryCredential {
        id: Some("fixture".into()),
        hostname: world.base_url().into(),
        userid: Some("user-1".into()),
        username: Some("Interop User".into()),
        password: String::new(),
    };
    let app = Arc::new(App {
        client: Arc::new(FoundryClient::for_loopback(world.base_url()).unwrap()),
        instances: instances::Instances::new(vec![credential]).unwrap(),
        selections: instances::Selections::new(),
        allow_writes: true,
    });
    let token = auth::ScopedToken::new(
        auth::Scope::new("fixture").unwrap(),
        "interop-secret".to_owned(),
    )
    .unwrap();
    let router = secured_router(app, Some(token));

    let response = router
        .clone()
        .oneshot(
            Request::post("/api/write/create_document")
                .header("host", "localhost")
                .header("content-type", "application/json")
                .header("authorization", "Bearer fixture.interop-secret")
                .body(Body::from(
                    json!({"kind":"Actor","data":[{"name":"Interop Hero"}]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["outcome"], "possibly_applied", "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("may have been applied"),
        "{body}"
    );
}

#[tokio::test]
async fn an_upload_the_connector_refuses_is_a_caller_mistake() {
    // The connector refuses an upload target that escapes its directory
    // before it reaches Foundry. That is caller input, so the REST route
    // answers 400 instead of an upstream failure, and nothing was sent.
    let world = FakeWorld::start_with_world(WorldFixture::journal_world()).await;
    let credential = FoundryCredential {
        id: Some("fixture".into()),
        hostname: world.base_url().into(),
        userid: Some("user-1".into()),
        username: Some("Interop User".into()),
        password: String::new(),
    };
    let app = Arc::new(App {
        client: Arc::new(FoundryClient::for_loopback(world.base_url()).unwrap()),
        instances: instances::Instances::new(vec![credential]).unwrap(),
        selections: instances::Selections::new(),
        allow_writes: true,
    });
    let token = auth::ScopedToken::new(
        auth::Scope::new("fixture").unwrap(),
        "interop-secret".to_owned(),
    )
    .unwrap();
    let router = secured_router(app, Some(token));

    let response = router
        .clone()
        .oneshot(
            Request::post("/api/files")
                .header("host", "localhost")
                .header("content-type", "application/json")
                .header("authorization", "Bearer fixture.interop-secret")
                .body(Body::from(
                    json!({
                        "target": "../escape",
                        "filename": "a.png",
                        "data_base64": "aGVsbG8=",
                        "overwrite": true,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["error"], "invalid upload target", "{body}");
    assert!(body.get("outcome").is_none(), "{body}");

    // The request never reached the world.
    let log = world.request_log();
    assert!(
        !log.iter().any(|entry| entry.contains("upload")),
        "the refused upload reached the world: {log:?}"
    );
}
