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
