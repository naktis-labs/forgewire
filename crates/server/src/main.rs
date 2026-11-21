use axum::{
    extract::{Path, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use foundry_connector::{FoundryClient, FoundryCredential};
use serde_json::{json, Value};
use std::{env, net::SocketAddr, sync::Arc};

#[derive(Clone)]
struct App {
    client: FoundryClient,
    credential: FoundryCredential,
}

async fn health() -> Json<Value> {
    Json(json!({"status":"ok"}))
}
async fn world(State(app): State<Arc<App>>) -> Result<Json<Value>, (StatusCode, String)> {
    app.client
        .world(&app.credential)
        .await
        .map(|v| Json(serde_json::to_value(v).unwrap_or(Value::Null)))
        .map_err(internal)
}
async fn collection(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    app.client
        .list_documents(&app.credential, &name)
        .await
        .map(Json)
        .map_err(internal)
}
fn internal(e: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_GATEWAY, e.to_string())
}

async fn reject_origin(request: Request, next: Next) -> Response {
    let headers = request.headers();
    if headers.contains_key(header::ORIGIN) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let mut hosts = headers.get_all(header::HOST).iter();
    let valid_host = hosts
        .next()
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.parse::<http::uri::Authority>().ok())
        .is_some_and(|authority| {
            matches!(authority.host(), "localhost" | "127.0.0.1" | "[::1]")
                && authority.as_str().find('@').is_none()
        });
    if !valid_host || hosts.next().is_some() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    next.run(request).await
}

fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/world", get(world))
        .route("/api/collections/{name}", get(collection))
        .route("/mcp", post(mcp))
        .with_state(app)
        .layer(middleware::from_fn(reject_origin))
}

fn tool_success(id: Value, value: Value) -> Value {
    let mut result = json!({"content":[{"type":"text","text":value.to_string()}],"isError":false});
    if value.is_object() {
        result["structuredContent"] = value;
    }
    json!({"jsonrpc":"2.0","id":id,"result":result})
}

async fn mcp(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> impl IntoResponse {
    for version in headers.get_all("mcp-protocol-version") {
        if !matches!(version.to_str(), Ok("2025-03-26" | "2025-06-18"))
            || headers.get_all("mcp-protocol-version").iter().count() != 1
        {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"unsupported MCP protocol version"})),
            )
                .into_response();
        }
    }
    let valid_id = |id: &Value| id.is_string() || id.as_i64().is_some() || id.as_u64().is_some();
    let valid = input.is_object()
        && input.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
        && input.get("method").and_then(Value::as_str).is_some()
        && input.get("id").is_none_or(valid_id)
        && input
            .get("params")
            .is_none_or(|p| p.is_object() || p.is_array());
    if !valid {
        return (StatusCode::BAD_REQUEST, Json(json!({"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid request"}}))).into_response();
    }
    let method = input["method"].as_str().unwrap();
    let Some(id) = input.get("id").cloned() else {
        return StatusCode::ACCEPTED.into_response();
    };
    let response = match method {
        "initialize" => {
            let requested = input
                .get("params")
                .and_then(|p| p.get("protocolVersion"))
                .and_then(Value::as_str);
            let version = match requested {
                Some("2025-03-26") => "2025-03-26",
                Some("2025-06-18") => "2025-06-18",
                None => "2025-03-26",
                Some(_) => "2025-06-18",
            };
            json!({"jsonrpc":"2.0","id":id,"result":{"protocolVersion":version,"capabilities":{"tools":{}},"serverInfo":{"name":"forgewire","version":"0.1.0"}}})
        }
        "ping" => json!({"jsonrpc":"2.0","id":id,"result":{}}),
        "tools/list" => {
            json!({"jsonrpc":"2.0","id":id,"result":{"tools":[{"name":"get_world","description":"Read world metadata","inputSchema":{"type":"object","properties":{}}},{"name":"list_documents","description":"Read a Foundry world collection","inputSchema":{"type":"object","properties":{"collection":{"type":"string","enum":["actors","items","folders","users","scenes","journal","macros","cards","playlists","tables","combats","messages","settings"]}},"required":["collection"]}}]}})
        }
        "tools/call" => {
            let params = input.get("params").unwrap_or(&Value::Null);
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params.get("arguments").unwrap_or(&Value::Null);
            let result = match name {
                "get_world" => app
                    .client
                    .world(&app.credential)
                    .await
                    .map(|v| serde_json::to_value(v).unwrap_or(Value::Null)),
                "list_documents" => {
                    app.client
                        .list_documents(
                            &app.credential,
                            args.get("collection").and_then(Value::as_str).unwrap_or(""),
                        )
                        .await
                }
                _ => Err(anyhow::anyhow!("unknown tool")),
            };
            match result {
                Ok(value) => tool_success(id, value),
                Err(err) => {
                    json!({"jsonrpc":"2.0","id":id,"result":{"content":[{"type":"text","text":err.to_string()}],"isError":true}})
                }
            }
        }
        _ => json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}),
    };
    let mut response = Json(response).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    response
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();
    let credential_path = env::var("FOUNDRY_CREDENTIALS")
        .unwrap_or_else(|_| "config/foundry_credentials.json".into());
    let raw = tokio::fs::read_to_string(&credential_path).await?;
    let credentials: Vec<FoundryCredential> = serde_json::from_str(&raw)?;
    let credential = credentials
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("credentials file contains no entries"))?;
    let app = Arc::new(App {
        client: FoundryClient::new()?,
        credential,
    });
    let router = router(app);
    let addr: SocketAddr = env::var("BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8765".into())
        .parse()?;
    if !addr.ip().is_loopback() {
        tracing::warn!(%addr, "binding beyond loopback exposes this unauthenticated server; only loopback Host headers are accepted");
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "forgewire server listening");
    axum::serve(listener, router).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use tower::ServiceExt;

    fn app() -> Router {
        router(Arc::new(App {
            client: FoundryClient::new().unwrap(),
            credential: FoundryCredential {
                id: None,
                hostname: "localhost".into(),
                userid: None,
                username: None,
                password: String::new(),
            },
        }))
    }

    async fn post(body: Value, version: Option<&str>) -> (StatusCode, Value) {
        let mut req = Request::post("/mcp")
            .header("host", "localhost:8765")
            .header("content-type", "application/json");
        if let Some(v) = version {
            req = req.header("mcp-protocol-version", v);
        }
        let response = app()
            .oneshot(req.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn rejects_origins_and_untrusted_hosts_on_all_routes() {
        for path in ["/health", "/api/world", "/mcp"] {
            for host in [
                "evil.example",
                "localhost.evil.example",
                "localhost@evil.example",
                "localhost, evil.example",
            ] {
                let req = Request::get(path)
                    .header("host", host)
                    .body(Body::empty())
                    .unwrap();
                assert_eq!(
                    app().oneshot(req).await.unwrap().status(),
                    StatusCode::BAD_REQUEST
                );
            }
            let req = Request::get(path)
                .header("host", "localhost")
                .header("origin", "null")
                .body(Body::empty())
                .unwrap();
            assert_eq!(
                app().oneshot(req).await.unwrap().status(),
                StatusCode::FORBIDDEN
            );
        }
        let req = Request::get("/health")
            .header("host", "localhost")
            .header("host", "evil.example")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app().oneshot(req).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        for host in ["localhost", "127.0.0.1:8765", "[::1]:8765"] {
            let req = Request::get("/health")
                .header("host", host)
                .body(Body::empty())
                .unwrap();
            assert_eq!(app().oneshot(req).await.unwrap().status(), StatusCode::OK);
        }
    }

    #[test]
    fn structured_content_is_only_emitted_for_objects() {
        let array = tool_success(json!(1), json!([1, 2]));
        assert!(array["result"].get("structuredContent").is_none());
        assert_eq!(array["result"]["content"][0]["text"], "[1,2]");
        let object = tool_success(json!(1), json!({"name":"world"}));
        assert_eq!(object["result"]["structuredContent"]["name"], "world");
    }

    #[tokio::test]
    async fn negotiates_versions_and_validates_requests() {
        for (requested, expected) in [
            ("2025-03-26", "2025-03-26"),
            ("2025-06-18", "2025-06-18"),
            ("unknown", "2025-06-18"),
        ] {
            let (status, body) = post(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":requested}}), None).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["result"]["protocolVersion"], expected);
        }
        let (_, body) = post(json!({"jsonrpc":"2.0","id":1,"method":"initialize"}), None).await;
        assert_eq!(body["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(
            post(json!({"jsonrpc":"2.0","id":1,"method":"ping"}), Some("bad"))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        for body in [
            json!({}),
            json!({"jsonrpc":"2.0","id":null,"method":"ping"}),
            json!({"jsonrpc":"2.0","method":"ping","params":1}),
        ] {
            assert_eq!(post(body, None).await.0, StatusCode::BAD_REQUEST);
        }
        for method in [
            "notifications/initialized",
            "notifications/cancelled",
            "unknown",
            "ping",
        ] {
            assert_eq!(
                post(json!({"jsonrpc":"2.0","method":method}), None).await,
                (StatusCode::ACCEPTED, Value::Null)
            );
        }
        let (_, response) = post(
            json!({"jsonrpc":"2.0","id":1,"method":"notifications/initialized"}),
            None,
        )
        .await;
        assert_eq!(response["error"]["code"], -32601);
    }
}
