use axum::{
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use foundry_connector::{FoundryClient, FoundryCredential};
use serde_json::{json, Value};
use std::{env, net::SocketAddr, sync::Arc};

mod compendium_tools;
mod file_tools;
mod instances;
mod read_tools;

#[derive(Clone)]
struct App {
    client: FoundryClient,
    instances: instances::Instances,
    allow_writes: bool,
}

async fn health() -> Json<Value> {
    Json(json!({"status":"ok"}))
}
async fn world(State(app): State<Arc<App>>) -> Result<Json<Value>, (StatusCode, String)> {
    let credential = app.instances.snapshot();
    app.client
        .world(&credential)
        .await
        .map(|v| Json(serde_json::to_value(v).unwrap_or(Value::Null)))
        .map_err(internal)
}
async fn collection(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let credential = app.instances.snapshot();
    app.client
        .list_documents(&credential, &name)
        .await
        .map(Json)
        .map_err(internal)
}
fn internal(e: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_GATEWAY, e.to_string())
}

type WriteArguments<'a> = (
    &'a str,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    Vec<Value>,
    Vec<String>,
);

/// Document kinds accepted for write operations: the primary document types
/// plus the embedded document kinds from the upstream server-tools type list
/// (which require `parent_uuid` to address their parent document).
const WRITE_KINDS: [&str; 23] = [
    "Actor",
    "Item",
    "Folder",
    "User",
    "Scene",
    "JournalEntry",
    "Macro",
    "Cards",
    "Playlist",
    "RollTable",
    "Combat",
    "ChatMessage",
    "Setting",
    "Combatant",
    "ActiveEffect",
    "Drawing",
    "MeasuredTemplate",
    "Note",
    "Tile",
    "Token",
    "Wall",
    "AmbientLight",
    "AmbientSound",
];

fn write_arguments<'a>(args: &'a Value, operation: &str) -> anyhow::Result<WriteArguments<'a>> {
    let kind = args
        .get("kind")
        .and_then(Value::as_str)
        .filter(|s| WRITE_KINDS.contains(s))
        .ok_or_else(|| anyhow::anyhow!("invalid kind"))?;
    let optional = |key| -> anyhow::Result<Option<&str>> {
        match args.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) if !s.trim().is_empty() => Ok(Some(s)),
            _ => Err(anyhow::anyhow!("invalid {key}")),
        }
    };
    if operation == "create_document" && args.get("id").is_some() {
        anyhow::bail!("id must not be supplied to create_document");
    }
    let id = optional("id")?;
    let parent = optional("parent_uuid")?;
    let pack = optional("pack")?;
    let mut data = Vec::new();
    let mut ids = Vec::new();
    if operation == "delete_document" {
        let values = args
            .get("ids")
            .and_then(Value::as_array)
            .filter(|a| !a.is_empty())
            .ok_or_else(|| anyhow::anyhow!("ids must be a non-empty array"))?;
        for value in values {
            ids.push(
                value
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| anyhow::anyhow!("invalid id"))?
                    .to_owned(),
            );
        }
    } else {
        let field = if operation == "create_document" {
            "data"
        } else {
            "updates"
        };
        data = args
            .get(field)
            .and_then(Value::as_array)
            .filter(|a| !a.is_empty() && a.iter().all(Value::is_object))
            .ok_or_else(|| anyhow::anyhow!("{field} must be a non-empty array of objects"))?
            .clone();
        if operation == "modify_document" && id.is_none() {
            anyhow::bail!("id is required");
        }
    }
    Ok((kind, id, parent, pack, data, ids))
}

async fn write_document(
    app: &App,
    credential: &FoundryCredential,
    operation: &str,
    args: &Value,
) -> anyhow::Result<Value> {
    if !app.allow_writes {
        anyhow::bail!("writes are disabled; set FOUNDRY_ALLOW_WRITES=true to enable");
    }
    let (kind, id, parent, pack, data, ids) = write_arguments(args, operation)?;
    match operation {
        "create_document" => {
            app.client
                .create_document(credential, kind, data, parent, pack)
                .await
        }
        "modify_document" => {
            app.client
                .modify_document(credential, kind, id.unwrap(), data, parent, pack)
                .await
        }
        "delete_document" => {
            app.client
                .delete_document(credential, kind, ids, parent, pack)
                .await
        }
        _ => Err(anyhow::anyhow!("unknown operation")),
    }
}

async fn rest_write(
    State(app): State<Arc<App>>,
    Path(operation): Path<String>,
    Json(args): Json<Value>,
) -> Result<Json<Value>, (StatusCode, String)> {
    if !app.allow_writes {
        return Err((StatusCode::FORBIDDEN, "writes are disabled".into()));
    }
    if !matches!(
        operation.as_str(),
        "create_document" | "modify_document" | "delete_document"
    ) {
        return Err((StatusCode::NOT_FOUND, "unknown operation".into()));
    }
    write_arguments(&args, &operation).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let credential = app.instances.snapshot();
    write_document(&app, &credential, &operation, &args)
        .await
        .map(Json)
        .map_err(internal)
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
            matches!(
                authority.host().to_ascii_lowercase().as_str(),
                "localhost" | "127.0.0.1" | "[::1]"
            ) && authority.as_str().find('@').is_none()
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
        .route("/api/write/{operation}", post(rest_write))
        .route("/mcp", post(mcp))
        .with_state(app)
        .layer(DefaultBodyLimit::max(15 * 1024 * 1024))
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
            let mut tools = vec![
                json!({"name":"get_world","description":"Read world metadata","inputSchema":{"type":"object","properties":{}}}),
                json!({"name":"list_documents","description":"Read a Foundry world collection","inputSchema":{"type":"object","properties":{"collection":{"type":"string","enum":["actors","items","folders","users","scenes","journal","macros","cards","playlists","tables","combats","messages","settings"]}},"required":["collection"]}}),
            ];
            tools.extend([
                json!({"name":"show_credentials","description":"List configured Foundry instances without passwords","inputSchema":{"type":"object","properties":{}}}),
                json!({"name":"choose_foundry_instance","description":"Select an instance without verifying its connection","inputSchema":{"type":"object","properties":{"_id":{"type":"string"},"id":{"type":"string"},"item_order":{"type":"integer","minimum":0}}}}),
            ]);
            tools.extend(read_tools::definitions());
            tools.extend(file_tools::definitions());
            tools.extend(compendium_tools::definitions());
            for (name, field) in [
                ("create_document", "data"),
                ("modify_document", "updates"),
                ("delete_document", "ids"),
            ] {
                let mut required = vec!["kind", field];
                if name == "modify_document" {
                    required.push("id");
                }
                let kinds: Vec<Value> = WRITE_KINDS.iter().map(|kind| json!(kind)).collect();
                tools.push(json!({"name":name,"description":"Write Foundry documents (requires FOUNDRY_ALLOW_WRITES=true); embedded kinds such as Token or Wall require parent_uuid","inputSchema":{"type":"object","properties":{"kind":{"type":"string","enum":kinds},"id":{"type":"string","minLength":1},"parent_uuid":{"type":"string","minLength":1},"pack":{"type":"string","minLength":1},(field):{"type":"array","minItems":1,"items":{"type":if field == "ids" { "string" } else { "object" }}}},"required":required}}));
            }
            json!({"jsonrpc":"2.0","id":id,"result":{"tools":tools}})
        }
        "tools/call" => {
            let params = input.get("params").unwrap_or(&Value::Null);
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params.get("arguments").unwrap_or(&Value::Null);
            let credential = app.instances.snapshot();
            let result = match name {
                "show_credentials" => Ok(app.instances.show()),
                "choose_foundry_instance" => app.instances.select(args),
                "get_world" => app
                    .client
                    .world(&credential)
                    .await
                    .map(|v| serde_json::to_value(v).unwrap_or(Value::Null)),
                "list_documents" => {
                    app.client
                        .list_documents(
                            &credential,
                            args.get("collection").and_then(Value::as_str).unwrap_or(""),
                        )
                        .await
                }
                "create_document" | "modify_document" | "delete_document" => {
                    write_document(&app, &credential, name, args).await
                }
                _ if file_tools::recognizes(name) => {
                    file_tools::call(&app.client, &credential, app.allow_writes, name, args).await
                }
                _ if compendium_tools::recognizes(name) => {
                    compendium_tools::call(&app.client, &credential, app.allow_writes, name, args)
                        .await
                }
                _ if read_tools::recognizes(name) => {
                    read_tools::call(&app.client, &credential, name, args).await
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

fn validate_bind_addr(addr: SocketAddr) -> anyhow::Result<()> {
    if !addr.ip().is_loopback() {
        anyhow::bail!("BIND_ADDR must be a loopback address; this server is unauthenticated");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
        .init();
    let credential_path = env::var("FOUNDRY_CREDENTIALS")
        .unwrap_or_else(|_| "config/foundry_credentials.json".into());
    let raw = tokio::fs::read_to_string(&credential_path).await?;
    let mut entries: Vec<Value> = serde_json::from_str(&raw)?;
    for entry in &mut entries {
        let object = entry
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("invalid credential entry"))?;
        if let Some(id) = object.remove("_id") {
            if object.get("id").is_some_and(|existing| existing != &id) {
                anyhow::bail!("conflicting credential identifiers");
            }
            object.insert("id".into(), id);
        }
    }
    let credentials: Vec<FoundryCredential> = serde_json::from_value(Value::Array(entries))?;
    let app = Arc::new(App {
        client: FoundryClient::new()?,
        instances: instances::Instances::new(credentials)?,
        allow_writes: env::var("FOUNDRY_ALLOW_WRITES").as_deref() == Ok("true"),
    });
    let router = router(app);
    let addr: SocketAddr = env::var("BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8765".into())
        .parse()?;
    validate_bind_addr(addr)?;
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
            allow_writes: false,
            instances: instances::Instances::new(vec![FoundryCredential {
                id: None,
                hostname: "localhost".into(),
                userid: None,
                username: None,
                password: String::new(),
            }])
            .unwrap(),
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
        for (method, path) in [
            ("GET", "/health"),
            ("GET", "/api/world"),
            ("GET", "/mcp"),
            ("POST", "/api/write/create_document"),
        ] {
            for host in [
                "evil.example",
                "localhost.evil.example",
                "localhost@evil.example",
                "localhost, evil.example",
            ] {
                let req = Request::builder()
                    .method(method)
                    .uri(path)
                    .header("host", host)
                    .body(Body::empty())
                    .unwrap();
                assert_eq!(
                    app().oneshot(req).await.unwrap().status(),
                    StatusCode::BAD_REQUEST
                );
            }
            let req = Request::builder()
                .method(method)
                .uri(path)
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
        for host in [
            "localhost",
            "LocalHost:8765",
            "127.0.0.1:8765",
            "[::1]:8765",
        ] {
            let req = Request::get("/health")
                .header("host", host)
                .body(Body::empty())
                .unwrap();
            assert_eq!(app().oneshot(req).await.unwrap().status(), StatusCode::OK);
        }
    }

    #[test]
    fn only_loopback_bind_addresses_are_allowed() {
        for addr in ["127.0.0.1:8765", "[::1]:8765"] {
            assert!(validate_bind_addr(addr.parse().unwrap()).is_ok());
        }
        for addr in ["0.0.0.0:8765", "[::]:8765", "192.168.1.1:8765"] {
            assert!(validate_bind_addr(addr.parse().unwrap()).is_err());
        }
    }

    #[test]
    fn rejects_invalid_write_arguments() {
        for args in [
            json!({}),
            json!({"kind":"Actor","data":[]}),
            json!({"kind":"Actor","data":[1]}),
            json!({"kind":"Unknown","data":[{}]}),
            json!({"kind":"Actor","data":[{}],"pack":""}),
        ] {
            assert!(write_arguments(&args, "create_document").is_err());
        }
        assert!(
            write_arguments(&json!({"kind":"Actor","updates":[{}]}), "modify_document").is_err()
        );
        assert!(write_arguments(&json!({"kind":"Actor","ids":[""]}), "delete_document").is_err());
        assert!(write_arguments(&json!({"kind":"Actor","data":[{}]}), "create_document").is_ok());
    }

    #[test]
    fn create_document_rejects_supplied_id_and_accepts_embedded_kinds() {
        assert!(write_arguments(
            &json!({"kind":"Actor","data":[{}],"id":"abc"}),
            "create_document"
        )
        .is_err());
        assert!(write_arguments(
            &json!({"kind":"Actor","updates":[{}],"id":"abc"}),
            "modify_document"
        )
        .is_ok());
        for kind in [
            "Combatant",
            "ActiveEffect",
            "Drawing",
            "MeasuredTemplate",
            "Note",
            "Tile",
            "Token",
            "Wall",
            "AmbientLight",
            "AmbientSound",
        ] {
            assert!(
                write_arguments(
                    &json!({"kind":kind,"data":[{}],"parent_uuid":"Scene.abc"}),
                    "create_document"
                )
                .is_ok(),
                "{kind} should be a valid embedded write kind"
            );
        }
    }

    #[tokio::test]
    async fn writes_are_disabled_by_default_for_rest_and_mcp() {
        for name in ["create_document", "modify_document", "delete_document"] {
            let request = Request::post(format!("/api/write/{name}"))
                .header("host", "localhost")
                .header("content-type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap();
            assert_eq!(
                app().oneshot(request).await.unwrap().status(),
                StatusCode::FORBIDDEN
            );
            let (_, response) = post(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":{"collection":"actors","id":"test","document":{"name":"test"}}}}), None).await;
            assert_eq!(response["result"]["isError"], true);
            assert!(response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("disabled"));
        }
    }

    #[tokio::test]
    async fn enabled_writes_reject_invalid_input_before_connecting() {
        let mut state = App {
            client: FoundryClient::new().unwrap(),
            instances: instances::Instances::new(vec![FoundryCredential {
                id: None,
                hostname: "localhost".into(),
                userid: None,
                username: None,
                password: String::new(),
            }])
            .unwrap(),
            allow_writes: true,
        };
        for name in ["create_document", "modify_document", "delete_document"] {
            let request = Request::post(format!("/api/write/{name}"))
                .header("host", "localhost")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap();
            assert_eq!(
                router(Arc::new(state.clone()))
                    .oneshot(request)
                    .await
                    .unwrap()
                    .status(),
                StatusCode::BAD_REQUEST
            );
            assert!(
                write_document(&state, &state.instances.snapshot(), name, &json!({}))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("invalid kind")
            );
        }
        let request = Request::post("/api/write/not_an_operation")
            .header("host", "localhost")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(
            router(Arc::new(state.clone()))
                .oneshot(request)
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        state.allow_writes = false;
        assert!(write_document(
            &state,
            &state.instances.snapshot(),
            "create_document",
            &json!({})
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("disabled"));
        let (_, response) = post(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}), None).await;
        let tools = response["result"]["tools"].as_array().unwrap();
        for name in ["create_document", "modify_document", "delete_document"] {
            let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
            let field = match name {
                "create_document" => "data",
                "modify_document" => "updates",
                _ => "ids",
            };
            assert_eq!(tool["inputSchema"]["properties"][field]["type"], "array");
        }
        let kinds = tools
            .iter()
            .find(|tool| tool["name"] == "create_document")
            .unwrap()["inputSchema"]["properties"]["kind"]["enum"]
            .as_array()
            .unwrap()
            .clone();
        for kind in [
            "Actor",
            "Setting",
            "Combatant",
            "Token",
            "Wall",
            "AmbientSound",
        ] {
            assert!(
                kinds.contains(&json!(kind)),
                "{kind} missing from advertised kind enum"
            );
        }
    }

    #[tokio::test]
    async fn mcp_instance_selection_changes_the_active_credential_without_leaking_passwords() {
        let instances = instances::Instances::new(vec![
            FoundryCredential {
                id: Some("one".into()),
                hostname: "one.example".into(),
                userid: Some("user-one".into()),
                username: None,
                password: "secret-one".into(),
            },
            FoundryCredential {
                id: Some("two".into()),
                hostname: "two.example".into(),
                userid: Some("user-two".into()),
                username: None,
                password: "secret-two".into(),
            },
        ])
        .unwrap();
        let app = router(Arc::new(App {
            client: FoundryClient::new().unwrap(),
            instances: instances.clone(),
            allow_writes: false,
        }));
        let call = |name: &'static str, arguments: Value| {
            let app = app.clone();
            async move {
                let request = Request::post("/mcp")
                    .header("host", "localhost")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":arguments}}).to_string(),
                    ))
                    .unwrap();
                let response = app.oneshot(request).await.unwrap();
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                serde_json::from_slice::<Value>(&body).unwrap()
            }
        };

        let shown = call("show_credentials", json!({})).await;
        let text = shown["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(text.contains("one.example"));
        assert!(!text.contains("secret-one"));
        assert_eq!(instances.snapshot().id.as_deref(), Some("one"));

        let selected = call("choose_foundry_instance", json!({"_id":"two"})).await;
        assert_eq!(selected["result"]["structuredContent"]["item_order"], 1);
        assert_eq!(instances.snapshot().id.as_deref(), Some("two"));

        let selected = call("choose_foundry_instance", json!({"item_order":0})).await;
        assert_eq!(selected["result"]["structuredContent"]["item_order"], 0);
        assert_eq!(instances.snapshot().hostname, "one.example");
        assert!(
            call("choose_foundry_instance", json!({"item_order":9})).await["result"]["isError"]
                == true
        );
        assert!(
            call("choose_foundry_instance", json!({"id":"missing"})).await["result"]["isError"]
                == true
        );
    }

    #[tokio::test]
    async fn compendium_tools_are_advertised_and_write_gated() {
        let (_, response) = post(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}), None).await;
        let tools = response["result"]["tools"].as_array().unwrap();
        for name in ["create_compendium", "delete_compendium"] {
            assert!(tools.iter().any(|tool| tool["name"] == name));
            let (_, response) = post(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":{}}}), None).await;
            assert!(response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("disabled"));
        }
    }

    #[tokio::test]
    async fn file_tools_are_advertised_and_upload_is_write_gated() {
        let (_, response) = post(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}), None).await;
        let tools = response["result"]["tools"].as_array().unwrap();
        for name in ["browse_files", "upload_file"] {
            assert!(tools.iter().any(|tool| tool["name"] == name));
        }
        let (_, response) = post(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"upload_file","arguments":{}}}), None).await;
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("disabled"));
    }

    #[tokio::test]
    async fn request_body_limit_is_fifteen_mib() {
        for (size, expected) in [
            (3 * 1024 * 1024, StatusCode::OK),
            (15 * 1024 * 1024 + 1, StatusCode::PAYLOAD_TOO_LARGE),
        ] {
            let prefix = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\",\"padding\":\"";
            let suffix = "\"}";
            let body = format!(
                "{prefix}{}{suffix}",
                "a".repeat(size - prefix.len() - suffix.len())
            );
            let request = Request::post("/mcp")
                .header("host", "localhost")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            assert_eq!(app().oneshot(request).await.unwrap().status(), expected);
        }
    }

    #[tokio::test]
    async fn tools_call_without_id_is_accepted_without_execution() {
        // A tools/call without an id is a JSON-RPC notification: it must be
        // acknowledged with 202 and an empty body, and the tool must never run.
        let request = Request::post("/mcp")
            .header("host", "localhost:8765")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_world","arguments":{}}})
                    .to_string(),
            ))
            .unwrap();
        let response = app().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn read_tools_are_advertised_and_dispatched() {
        let (_, response) = post(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}), None).await;
        let tools = response["result"]["tools"].as_array().unwrap();
        for definition in read_tools::definitions() {
            assert!(tools.contains(&definition));
        }
        let (_, response) = post(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_actor","arguments":{}}}), None).await;
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("id, _id, or name is required"));
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
