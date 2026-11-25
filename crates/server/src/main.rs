use axum::{
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use foundry_connector::{FoundryClient, FoundryCredential};
use serde_json::{json, Value};
use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};

mod compendium_tools;
mod file_tools;
mod instances;
mod operations;
mod read_tools;

#[cfg(test)]
mod contract_tests;

#[cfg(test)]
mod packaging_tests;

#[derive(Clone)]
struct App {
    client: Arc<dyn operations::Connector>,
    instances: instances::Instances,
    selections: instances::Selections,
    allow_writes: bool,
}

/// Resolve a REST request's Foundry target: `?instance=<id|item_order>` when
/// given, else the documented stable default — the first configured entry.
/// The result is an owned credential snapshot taken once per request, so
/// neither MCP selections nor other REST requests can retarget work already
/// in flight; there is no process-global selection to inherit or mutate.
fn rest_target(
    instances: &instances::Instances,
    query: &HashMap<String, String>,
) -> Result<FoundryCredential, (StatusCode, Json<Value>)> {
    match query.get("instance") {
        Some(reference) => instances
            .resolve_reference(reference)
            .and_then(|index| instances.credential(index))
            .map_err(|e| rest_error(StatusCode::BAD_REQUEST, e)),
        None => Ok(instances.default_snapshot()),
    }
}

async fn health() -> Json<Value> {
    Json(json!({"status":"ok"}))
}
async fn world(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let credential = rest_target(&app.instances, &query)?;
    operations::world(&*app.client, &credential)
        .await
        .map(Json)
        .map_err(operation_status)
}
async fn collection(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let credential = rest_target(&app.instances, &query)?;
    operations::collection(&*app.client, &credential, &name)
        .await
        .map(Json)
        .map_err(operation_status)
}

fn rest_error(status: StatusCode, error: impl ToString) -> (StatusCode, Json<Value>) {
    (status, Json(json!({"error":error.to_string()})))
}

/// Map the shared typed operation error to REST status codes: caller mistakes
/// are 400/404, policy gates are 403, and only Foundry contact or response
/// failures are 502.
fn operation_status(error: operations::OperationError) -> (StatusCode, Json<Value>) {
    let status = match &error {
        operations::OperationError::Invalid(_) => StatusCode::BAD_REQUEST,
        operations::OperationError::NotFound(_) => StatusCode::NOT_FOUND,
        operations::OperationError::Forbidden(_) => StatusCode::FORBIDDEN,
        operations::OperationError::Upstream(_) => StatusCode::BAD_GATEWAY,
    };
    rest_error(status, error)
}

async fn rest_documents(
    State(app): State<Arc<App>>,
    Path(collection): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let args =
        operations::rest_read_args(&query).map_err(|e| rest_error(StatusCode::BAD_REQUEST, e))?;
    let (name, args) = operations::parse_rest_read(&collection, &args)
        .map_err(|e| rest_error(StatusCode::BAD_REQUEST, e))?;
    let req =
        operations::parse_read(&name, &args).map_err(|e| rest_error(StatusCode::BAD_REQUEST, e))?;
    let credential = rest_target(&app.instances, &query)?;
    operations::execute_read(&*app.client, &credential, &req)
        .await
        .map(Json)
        .map_err(operation_status)
}

async fn rest_compendiums(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let args = query
        .get("type")
        .map(|value| json!({"type":value}))
        .unwrap_or_else(|| json!({}));
    let credential = rest_target(&app.instances, &query)?;
    operations::read(&*app.client, &credential, "get_compendiums", &args)
        .await
        .map(Json)
        .map_err(operation_status)
}

async fn rest_compendium_documents(
    State(app): State<Arc<App>>,
    Path((pack, kind)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let args =
        operations::rest_read_args(&query).map_err(|e| rest_error(StatusCode::BAD_REQUEST, e))?;
    let (name, args) = operations::parse_rest_pack_read(&kind, &pack, &args)
        .map_err(|e| rest_error(StatusCode::BAD_REQUEST, e))?;
    let req =
        operations::parse_read(&name, &args).map_err(|e| rest_error(StatusCode::BAD_REQUEST, e))?;
    let credential = rest_target(&app.instances, &query)?;
    operations::execute_read(&*app.client, &credential, &req)
        .await
        .map(Json)
        .map_err(operation_status)
}

async fn rest_create_compendium(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
    Json(args): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let credential = rest_target(&app.instances, &query)?;
    operations::compendium(
        &*app.client,
        &credential,
        app.allow_writes,
        "create_compendium",
        &args,
    )
    .await
    .map(Json)
    .map_err(operation_status)
}

async fn rest_delete_compendium(
    State(app): State<Arc<App>>,
    Path(name): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let credential = rest_target(&app.instances, &query)?;
    operations::compendium(
        &*app.client,
        &credential,
        app.allow_writes,
        "delete_compendium",
        &json!({"name":name}),
    )
    .await
    .map(Json)
    .map_err(operation_status)
}

async fn rest_instances(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // Listing flags which entry the requested reference (or the stable
    // default) would target; it never exposes secrets or connection state.
    let selected = match query.get("instance") {
        Some(reference) => app
            .instances
            .resolve_reference(reference)
            .map_err(|e| rest_error(StatusCode::BAD_REQUEST, e))?,
        None => 0,
    };
    Ok(Json(app.instances.show(selected)))
}

async fn rest_browse_files(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut args = json!({});
    for key in ["target", "type", "kind"] {
        if let Some(value) = query.get(key) {
            args[key] = json!(value);
        }
    }
    if let Some(extensions) = query.get("extensions") {
        args["extensions"] = json!(extensions
            .split(',')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>());
    }
    let credential = rest_target(&app.instances, &query)?;
    operations::browse_files(&*app.client, &credential, &args)
        .await
        .map(Json)
        .map_err(operation_status)
}

async fn rest_upload_file(
    State(app): State<Arc<App>>,
    Query(query): Query<HashMap<String, String>>,
    Json(args): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let credential = rest_target(&app.instances, &query)?;
    operations::upload_file(&*app.client, &credential, app.allow_writes, &args)
        .await
        .map(Json)
        .map_err(operation_status)
}

async fn rest_write(
    State(app): State<Arc<App>>,
    Path(operation): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    Json(args): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if !matches!(
        operation.as_str(),
        "create_document" | "modify_document" | "delete_document"
    ) {
        return Err(rest_error(StatusCode::NOT_FOUND, "unknown operation"));
    }
    let credential = rest_target(&app.instances, &query)?;
    operations::write_document(
        &*app.client,
        &credential,
        app.allow_writes,
        &operation,
        &args,
    )
    .await
    .map(Json)
    .map_err(operation_status)
}

#[derive(Clone)]
struct BearerToken(String);

impl BearerToken {
    fn parse(value: String) -> anyhow::Result<Self> {
        // RFC 6750 b64token syntax, with padding allowed only at the end.
        let token = value.trim_end_matches('=');
        if token.is_empty()
            || !token.bytes().all(|b| {
                b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
            })
        {
            anyhow::bail!("invalid FORGEWIRE_BEARER_TOKEN configuration");
        }
        Ok(Self(value))
    }
}

async fn authenticate(
    State(token): State<Option<BearerToken>>,
    request: Request,
    next: Next,
) -> Response {
    use subtle::ConstantTimeEq;
    if let Some(token) = token {
        let mut values = request.headers().get_all(header::AUTHORIZATION).iter();
        let candidate = values
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                let (scheme, credential) = value.split_once(' ')?;
                if scheme.eq_ignore_ascii_case("bearer") {
                    Some(credential.as_bytes())
                } else {
                    None
                }
            });
        if values.next().is_some()
            || !candidate.is_some_and(|candidate| bool::from(candidate.ct_eq(token.0.as_bytes())))
        {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
            )
                .into_response();
        }
    }
    next.run(request).await
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

#[cfg(test)]
fn router(app: Arc<App>) -> Router {
    secured_router(app, None)
}

fn secured_router(app: Arc<App>, token: Option<BearerToken>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/world", get(world))
        .route("/api/collections/{name}", get(collection))
        .route("/api/documents/{collection}", get(rest_documents))
        .route(
            "/api/compendiums",
            get(rest_compendiums).post(rest_create_compendium),
        )
        .route(
            "/api/compendiums/{name}",
            axum::routing::delete(rest_delete_compendium),
        )
        .route(
            "/api/compendiums/{pack}/documents/{kind}",
            get(rest_compendium_documents),
        )
        .route("/api/instances", get(rest_instances))
        .route("/api/files", get(rest_browse_files).post(rest_upload_file))
        .route("/api/write/{operation}", post(rest_write))
        .route("/mcp", post(mcp))
        .with_state(app)
        .layer(DefaultBodyLimit::max(15 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(token, authenticate))
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
    // MCP sessions scope instance selection per client: `initialize` issues
    // the id (returned as `Mcp-Session-Id`), later requests echo it, and an
    // unknown or expired id is refused so the client reinitializes instead of
    // silently landing in another client's scope. Liveness and targeting are
    // resolved by one atomic lookup, so a session that expires mid-request
    // fails with the initialize-again error instead of silently running
    // ordinary tool calls against the stable default. Requests without a
    // header are anonymous: tools run against the stable default instance and
    // `choose_foundry_instance` has no scope to record a selection in.
    let mut session_headers = headers.get_all("mcp-session-id").iter();
    let session = match session_headers.next() {
        None => None,
        Some(value) => {
            let id = value
                .to_str()
                .ok()
                .filter(|id| !id.is_empty())
                .map(str::to_owned);
            if id.is_none() || session_headers.next().is_some() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"invalid MCP session header"})),
                )
                    .into_response();
            }
            id
        }
    };
    let mut scope = instances::SessionScope::Default;
    if let Some(id) = &session {
        if method != "initialize" {
            match app.selections.scope(id) {
                instances::SessionScope::Expired => {
                    return (
                        StatusCode::NOT_FOUND,
                        Json(json!({"error":"unknown or expired MCP session; initialize again"})),
                    )
                        .into_response();
                }
                resolved => scope = resolved,
            }
        }
    }
    let Some(id) = input.get("id").cloned() else {
        return StatusCode::ACCEPTED.into_response();
    };
    let mut issued_session = None;
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
            issued_session = Some(app.selections.issue());
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
                json!({"name":"choose_foundry_instance","description":"Select the instance for this MCP session (issued at initialize) without verifying its connection","inputSchema":{"type":"object","properties":{"_id":{"type":"string"},"id":{"type":"string"},"item_order":{"type":"integer","minimum":0}}}}),
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
                let kinds: Vec<Value> = operations::WRITE_KINDS
                    .iter()
                    .map(|kind| json!(kind))
                    .collect();
                tools.push(json!({"name":name,"description":"Write Foundry documents (requires FOUNDRY_ALLOW_WRITES=true); embedded kinds such as Token or Wall require parent_uuid","inputSchema":{"type":"object","properties":{"kind":{"type":"string","enum":kinds},"id":{"type":"string","minLength":1},"parent_uuid":{"type":"string","minLength":1},"pack":{"type":"string","minLength":1},(field):{"type":"array","minItems":1,"items":{"type":if field == "ids" { "string" } else { "object" }}}},"required":required}}));
            }
            json!({"jsonrpc":"2.0","id":id,"result":{"tools":tools}})
        }
        "tools/call" => {
            let params = input.get("params").unwrap_or(&Value::Null);
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params.get("arguments").unwrap_or(&Value::Null);
            // The target is the scope resolved atomically with the session
            // check above: the session's own selection, or the stable default
            // when it has not chosen (or the caller is anonymous). The
            // snapshot is owned, so in-flight work cannot be retargeted.
            let selected = match scope {
                instances::SessionScope::Selected(index) => Some(index),
                instances::SessionScope::Default | instances::SessionScope::Expired => None,
            };
            let result = match name {
                "show_credentials" => Ok(app.instances.show(selected.unwrap_or(0))),
                "choose_foundry_instance" => match &session {
                    Some(id) => app.instances.resolve_selection(args).and_then(|index| {
                        // `select` re-checks liveness under the lock that
                        // records the choice: a session evicted after this
                        // request's own session check (a concurrent
                        // initialize filling the table) must surface as an
                        // error instead of a phantom `selected: true`.
                        if app.selections.select(id, index) {
                            Ok(instances::selection_report(index))
                        } else {
                            Err(anyhow::anyhow!(
                                "unknown or expired MCP session; initialize again"
                            ))
                        }
                    }),
                    None => Err(anyhow::anyhow!(
                        "choose_foundry_instance requires an MCP session: initialize first and send the returned Mcp-Session-Id header"
                    )),
                },
                _ => {
                    let credential = match selected {
                        Some(index) => app.instances.credential(index),
                        None => Ok(app.instances.default_snapshot()),
                    };
                    match credential {
                        Ok(credential) => dispatch_tool(&app, &credential, name, args).await,
                        Err(err) => Err(err),
                    }
                }
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
    if let Some(session_id) = issued_session {
        if let Ok(value) = header::HeaderValue::from_str(&session_id) {
            response
                .headers_mut()
                .insert(header::HeaderName::from_static("mcp-session-id"), value);
        }
    }
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    response
}

/// Route a non-selection MCP tool to the shared application operations using
/// the caller's own target snapshot.
async fn dispatch_tool(
    app: &App,
    credential: &FoundryCredential,
    name: &str,
    args: &Value,
) -> anyhow::Result<Value> {
    match name {
        "get_world" => operations::world(&*app.client, credential)
            .await
            .map_err(anyhow::Error::from),
        "list_documents" => operations::collection(
            &*app.client,
            credential,
            args.get("collection").and_then(Value::as_str).unwrap_or(""),
        )
        .await
        .map_err(anyhow::Error::from),
        "create_document" | "modify_document" | "delete_document" => {
            operations::write_document(&*app.client, credential, app.allow_writes, name, args)
                .await
                .map_err(anyhow::Error::from)
        }
        _ if file_tools::recognizes(name) => {
            file_tools::call(&*app.client, credential, app.allow_writes, name, args).await
        }
        _ if compendium_tools::recognizes(name) => {
            compendium_tools::call(&*app.client, credential, app.allow_writes, name, args).await
        }
        _ if read_tools::recognizes(name) => {
            read_tools::call(&*app.client, credential, name, args).await
        }
        _ => Err(anyhow::anyhow!("unknown tool")),
    }
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
        client: Arc::new(FoundryClient::new()?),
        instances: instances::Instances::new(credentials)?,
        selections: instances::Selections::new(),
        allow_writes: env::var("FOUNDRY_ALLOW_WRITES").as_deref() == Ok("true"),
    });
    let token = match env::var("FORGEWIRE_BEARER_TOKEN") {
        Ok(value) => Some(BearerToken::parse(value)?),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            anyhow::bail!("invalid FORGEWIRE_BEARER_TOKEN configuration")
        }
    };
    let router = secured_router(app, token);
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
        test_app(None)
    }

    fn test_app(token: Option<BearerToken>) -> Router {
        secured_router(
            Arc::new(App {
                client: Arc::new(FoundryClient::new().unwrap()),
                allow_writes: false,
                instances: instances::Instances::new(vec![FoundryCredential {
                    id: None,
                    hostname: "localhost".into(),
                    userid: None,
                    username: None,
                    password: String::new(),
                }])
                .unwrap(),
                selections: instances::Selections::new(),
            }),
            token,
        )
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
    async fn bearer_auth_protects_rest_and_mcp() {
        let protected = test_app(Some(BearerToken::parse("fixture-token".into()).unwrap()));
        for (method, path) in [
            ("GET", "/health"),
            ("GET", "/api/instances"),
            ("POST", "/mcp"),
        ] {
            for value in [
                None,
                Some("Basic fixture-token"),
                Some("Bearer wrong"),
                Some("Bearer"),
                Some("Bearer fixture-token, Bearer fixture-token"),
                Some("Bearer fixture-token extra"),
            ] {
                let mut request = Request::builder()
                    .method(method)
                    .uri(path)
                    .header("host", "localhost");
                if let Some(value) = value {
                    request = request.header("authorization", value);
                }
                let response = protected
                    .clone()
                    .oneshot(request.body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    StatusCode::UNAUTHORIZED,
                    "{method} {path} {value:?}"
                );
                assert_eq!(
                    response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
                    "Bearer"
                );
            }
            let request = Request::builder()
                .method(method)
                .uri(path)
                .header("host", "localhost")
                .header("authorization", "Bearer fixture-token")
                .header("authorization", "Bearer fixture-token")
                .body(Body::empty())
                .unwrap();
            let response = protected.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(
                response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
                "Bearer"
            );
            let request = Request::builder()
                .method(method)
                .uri(path)
                .header("host", "localhost")
                .header("authorization", "Bearer fixture-token")
                .body(Body::empty())
                .unwrap();
            assert_ne!(
                protected.clone().oneshot(request).await.unwrap().status(),
                StatusCode::UNAUTHORIZED
            );
        }
    }

    #[tokio::test]
    async fn valid_bearer_reaches_mcp_handler() {
        let request = Request::post("/mcp")
            .header("host", "localhost")
            .header("authorization", "Bearer fixture-token")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}).to_string(),
            ))
            .unwrap();
        let response = test_app(Some(BearerToken::parse("fixture-token".into()).unwrap()))
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert!(body["result"]["tools"].is_array());
    }

    #[test]
    fn bearer_token_configuration_is_strict() {
        for token in [
            "",
            "=",
            " ",
            "token value",
            "token\n",
            "abc=def",
            "abc===def",
            "\u{00e9}",
        ] {
            assert!(BearerToken::parse(token.to_owned()).is_err());
        }
        for token in ["fixture-token", "a+/._~-=", "abc==="] {
            assert!(BearerToken::parse(token.to_owned()).is_ok());
        }
    }

    #[tokio::test]
    async fn configured_auth_preserves_host_and_origin_defenses() {
        let protected = test_app(Some(BearerToken::parse("fixture-token".into()).unwrap()));
        for (host, origin, expected) in [
            ("evil.example", None, StatusCode::BAD_REQUEST),
            ("localhost", Some("null"), StatusCode::FORBIDDEN),
        ] {
            let mut request = Request::get("/health")
                .header("host", host)
                .header("authorization", "Bearer fixture-token");
            if let Some(origin) = origin {
                request = request.header("origin", origin);
            }
            assert_eq!(
                protected
                    .clone()
                    .oneshot(request.body(Body::empty()).unwrap())
                    .await
                    .unwrap()
                    .status(),
                expected
            );
        }
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
    fn operation_errors_map_to_rest_statuses_with_the_json_envelope() {
        for (error, expected) in [
            (
                operations::OperationError::Invalid(anyhow::anyhow!("bad input")),
                StatusCode::BAD_REQUEST,
            ),
            (
                operations::OperationError::NotFound(anyhow::anyhow!("missing")),
                StatusCode::NOT_FOUND,
            ),
            (
                operations::OperationError::Forbidden(anyhow::anyhow!("disabled")),
                StatusCode::FORBIDDEN,
            ),
            (
                operations::OperationError::Upstream(anyhow::anyhow!("socket failed")),
                StatusCode::BAD_GATEWAY,
            ),
        ] {
            let (status, body) = operation_status(error);
            assert_eq!(status, expected);
            assert!(
                body.0.get("error").is_some(),
                "{status} must carry the error envelope"
            );
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
            assert!(operations::write_arguments(&args, "create_document").is_err());
        }
        assert!(operations::write_arguments(
            &json!({"kind":"Actor","updates":[{}]}),
            "modify_document"
        )
        .is_err());
        assert!(operations::write_arguments(
            &json!({"kind":"Actor","ids":[""]}),
            "delete_document"
        )
        .is_err());
        assert!(operations::write_arguments(
            &json!({"kind":"Actor","data":[{}]}),
            "create_document"
        )
        .is_ok());
    }

    #[test]
    fn create_document_rejects_supplied_id_and_accepts_embedded_kinds() {
        assert!(operations::write_arguments(
            &json!({"kind":"Actor","data":[{}],"id":"abc"}),
            "create_document"
        )
        .is_err());
        assert!(operations::write_arguments(
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
                operations::write_arguments(
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
    async fn rest_document_reads_share_mcp_validation() {
        let request = Request::get("/api/documents/actors?max_length=-1")
            .header("host", "localhost")
            .body(Body::empty())
            .unwrap();
        let response = app().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let rest_error: Value = serde_json::from_slice(&body).unwrap();

        let (_, mcp_response) = post(
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_actors","arguments":{"max_length":-1}}}),
            None,
        )
        .await;
        assert_eq!(mcp_response["result"]["isError"], true);
        assert_eq!(
            rest_error["error"],
            mcp_response["result"]["content"][0]["text"]
        );
    }

    #[tokio::test]
    async fn rest_exposes_compendium_instance_and_file_routes_with_write_gates() {
        let request = Request::get("/api/instances")
            .header("host", "localhost")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );

        for (method, path, body) in [
            (
                "POST",
                "/api/compendiums",
                r#"{"label":"Pack","type":"Actor"}"#,
            ),
            (
                "POST",
                "/api/files",
                r#"{"target":"","filename":"x","data_base64":"","overwrite":true}"#,
            ),
        ] {
            let request = Request::builder()
                .method(method)
                .uri(path)
                .header("host", "localhost")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            assert_eq!(
                app().oneshot(request).await.unwrap().status(),
                StatusCode::FORBIDDEN
            );
        }
    }

    #[tokio::test]
    async fn enabled_writes_reject_invalid_input_before_connecting() {
        let mut state = App {
            client: Arc::new(FoundryClient::new().unwrap()),
            instances: instances::Instances::new(vec![FoundryCredential {
                id: None,
                hostname: "localhost".into(),
                userid: None,
                username: None,
                password: String::new(),
            }])
            .unwrap(),
            selections: instances::Selections::new(),
            allow_writes: true,
        };
        for name in ["create_document", "modify_document", "delete_document"] {
            let request = Request::post(format!("/api/write/{name}"))
                .header("host", "localhost")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap();
            let rest_response = router(Arc::new(state.clone()))
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(rest_response.status(), StatusCode::BAD_REQUEST);
            let rest_body = to_bytes(rest_response.into_body(), usize::MAX)
                .await
                .unwrap();
            let rest_error = String::from_utf8(rest_body.to_vec()).unwrap();
            let request = Request::post("/mcp")
                .header("host", "localhost")
                .header("content-type", "application/json")
                .body(Body::from(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":{}}}).to_string()))
                .unwrap();
            let response = router(Arc::new(state.clone()))
                .oneshot(request)
                .await
                .unwrap();
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let response: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(response["result"]["isError"], true);
            let mcp_error = response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .to_owned();
            assert!(rest_error.contains("invalid kind"));
            assert_eq!(mcp_error, "invalid kind");
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
        assert!(operations::write_document(
            &*state.client,
            &state.instances.default_snapshot(),
            state.allow_writes,
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
    async fn mcp_instance_selection_is_scoped_to_its_session_without_leaking_passwords() {
        let app = router(Arc::new(App {
            client: Arc::new(FoundryClient::new().unwrap()),
            instances: instances::Instances::new(vec![
                FoundryCredential {
                    id: Some("one".into()),
                    hostname: "one.example".into(),
                    userid: Some("user-one".into()),
                    username: Some("login-one".into()),
                    password: "secret-one".into(),
                },
                FoundryCredential {
                    id: Some("two".into()),
                    hostname: "two.example".into(),
                    userid: Some("user-two".into()),
                    username: Some("login-two".into()),
                    password: "secret-two".into(),
                },
            ])
            .unwrap(),
            selections: instances::Selections::new(),
            allow_writes: false,
        }));
        let send = |session: Option<&str>, body: Value| {
            let app = app.clone();
            let session = session.map(str::to_owned);
            async move {
                let mut request = Request::post("/mcp")
                    .header("host", "localhost")
                    .header("content-type", "application/json");
                if let Some(id) = session.as_deref() {
                    request = request.header("mcp-session-id", id);
                }
                let response = app
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
                    serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
                    issued,
                )
            }
        };
        let call = |session: Option<&str>, name: &'static str, arguments: Value| {
            let app = app.clone();
            let session = session.map(str::to_owned);
            async move {
                let mut request = Request::post("/mcp")
                    .header("host", "localhost")
                    .header("content-type", "application/json");
                if let Some(id) = session.as_deref() {
                    request = request.header("mcp-session-id", id);
                }
                let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":arguments}});
                let response = app
                    .oneshot(request.body(Body::from(body.to_string())).unwrap())
                    .await
                    .unwrap();
                let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null)
            }
        };

        let initialize = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}});
        let session_a = send(None, initialize.clone())
            .await
            .2
            .expect("initialize must issue a session id");
        let session_b = send(None, initialize.clone())
            .await
            .2
            .expect("initialize must issue a session id");
        assert_ne!(session_a, session_b, "each client gets its own session");

        // Instance listings come back as a text payload (arrays carry no
        // structuredContent).
        let listed = |response: &Value| -> Value {
            serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap()
        };

        // Listing works without a session and never shows passwords or
        // usernames (the userid is the one identifier listings carry).
        let shown = call(None, "show_credentials", json!({})).await;
        let text = shown["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(text.contains("one.example"));
        assert!(text.contains("user-one"));
        assert!(!text.contains("secret-one"));
        assert!(!text.contains("login-one"));
        assert_eq!(listed(&shown)[0]["currently_active"], true);
        assert_eq!(listed(&shown)[1]["currently_active"], false);

        // A session's selection applies to that session only.
        let selected = call(
            Some(&session_a),
            "choose_foundry_instance",
            json!({"_id":"two"}),
        )
        .await;
        assert_eq!(selected["result"]["structuredContent"]["item_order"], 1);
        assert_eq!(
            selected["result"]["structuredContent"]["selected"], true,
            "selection reports selection only, not a verified connection"
        );
        let shown = call(Some(&session_a), "show_credentials", json!({})).await;
        assert_eq!(listed(&shown)[1]["currently_active"], true);
        let shown = call(Some(&session_b), "show_credentials", json!({})).await;
        assert_eq!(
            listed(&shown)[0]["currently_active"],
            true,
            "one client's selection must be invisible to another's session"
        );
        let shown = call(None, "show_credentials", json!({})).await;
        assert_eq!(listed(&shown)[0]["currently_active"], true);

        // Bad selections are refused without changing anything.
        for arguments in [json!({"item_order":9}), json!({"id":"missing"}), json!({})] {
            assert!(
                call(Some(&session_a), "choose_foundry_instance", arguments).await["result"]
                    ["isError"]
                    == true
            );
        }

        // Without a session there is no scope to remember a selection in.
        let response = call(None, "choose_foundry_instance", json!({"_id":"two"})).await;
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("session"));

        // Unknown or invented session ids are refused instead of silently
        // falling back to someone else's scope.
        let (status, _, _) = send(
            Some("bogus-session-id"),
            json!({"jsonrpc":"2.0","id":1,"method":"ping"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
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
