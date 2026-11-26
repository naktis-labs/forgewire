use std::future::Future;

use anyhow::{anyhow, bail, Result};
use futures_util::{Sink, SinkExt, StreamExt};
use reqwest::header::SET_COOKIE;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        client::IntoClientRequest,
        protocol::{Message, WebSocketConfig},
    },
};
use url::Url;

pub mod compendium;
pub mod connection;
pub mod errors;
pub mod files;
pub mod read_ops;
pub mod socket_mux;

pub use errors::{
    ambiguous_mutation, invalid_input, pack_kind_mismatch, pack_not_found, upstream_authentication,
    AmbiguousMutation, InvalidInput, PackKindMismatch, PackNotFound, UpstreamAuthentication,
};

/// The disposable local Foundry world used by the interoperability harness.
///
/// It is compiled for this crate's own tests and for builds that enable the
/// `loopback-interop` feature, so the interoperability tests in the server
/// crate drive the same fake world the connector's tests use.
#[cfg(any(test, feature = "loopback-interop"))]
pub mod interop;

#[derive(Clone, Deserialize)]
pub struct FoundryCredential {
    pub id: Option<String>,
    pub hostname: String,
    pub userid: Option<String>,
    pub username: Option<String>,
    pub password: String,
}

impl std::fmt::Debug for FoundryCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FoundryCredential")
            .field("id", &self.id)
            .field("hostname", &self.hostname)
            .field("userid", &self.userid)
            .field("username", &self.username.as_ref().map(|_| "[REDACTED]"))
            .field("password", &"[REDACTED]")
            .finish()
    }
}

const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const SOCKET_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn is_namespace_disconnect_or_error(frame: &str) -> bool {
    frame.starts_with("41") || frame.starts_with("44")
}

fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES))
}

pub(crate) async fn connect_socket(
    request: tokio_tungstenite::tungstenite::handshake::client::Request,
) -> Result<connection::Socket> {
    let (socket, _) = connect_async_with_config(request, Some(websocket_config()), false).await?;
    Ok(socket)
}

pub(crate) async fn write_with_deadline<F, T>(
    future: F,
    deadline: Duration,
    operation: &str,
) -> Result<T>
where
    F: Future<Output = T>,
{
    timeout(deadline, future)
        .await
        .map_err(|_| anyhow!("Timed out {operation}"))
}

pub(crate) async fn send_message<S>(sink: &mut S, message: Message, operation: &str) -> Result<()>
where
    S: Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    match write_with_deadline(sink.send(message), WRITE_TIMEOUT, operation).await? {
        Ok(()) => Ok(()),
        Err(error) => Err(anyhow!("Failed {operation}: {error}")),
    }
}

#[derive(Clone)]
pub struct FoundryClient {
    http: reqwest::Client,
    connections: std::sync::Arc<connection::ConnectionManager>,
    /// The loopback world the harness points the client at. `None` for the
    /// production client, which is HTTPS-only.
    #[cfg(any(test, feature = "loopback-interop"))]
    loopback_base_url: Option<Url>,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorldInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub title: Option<String>,
    pub system: Option<String>,
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub world_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_generation: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_build: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authors: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub modules: Vec<Value>,
}

fn generate_session_id() -> String {
    use rand::RngCore;
    let mut bytes = [0_u8; 12];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn join_payload(credential: &FoundryCredential) -> Value {
    let mut payload = json!({
        "password": credential.password,
        "action": "join"
    });
    if let Some(userid) = &credential.userid {
        payload["userid"] = json!(userid);
        payload["userId"] = json!(userid);
    }
    if let Some(username) = &credential.username {
        payload["username"] = json!(username);
    }
    payload
}

fn resolve_identity(credential: &mut FoundryCredential, users: &[Value]) -> Result<()> {
    let matches_id: Vec<_> = credential
        .userid
        .as_deref()
        .map(|id| users.iter().filter(|u| user_id(u) == Some(id)).collect())
        .unwrap_or_default();
    let matches_name: Vec<_> = credential
        .username
        .as_deref()
        .map(|name| {
            users
                .iter()
                .filter(|u| u.get("name").and_then(Value::as_str) == Some(name))
                .collect()
        })
        .unwrap_or_default();
    if credential
        .userid
        .as_deref()
        .is_some_and(|v| v.trim().is_empty())
        || credential
            .username
            .as_deref()
            .is_some_and(|v| v.trim().is_empty())
    {
        bail!("Foundry userid and username must not be blank");
    }
    if credential.userid.is_some()
        && credential.username.is_some()
        && !matches_id.is_empty()
        && matches_id
            .iter()
            .any(|user| user.get("name").and_then(Value::as_str) != credential.username.as_deref())
    {
        bail!("Foundry userid and username identify inconsistent users");
    }
    if credential.userid.is_none() && credential.username.is_none() {
        bail!("Foundry credential requires a userid or username");
    }
    let candidates = if credential.userid.is_some() {
        matches_id
    } else if !matches_name.is_empty() {
        matches_name
    } else {
        let username = credential.username.as_deref().unwrap();
        users
            .iter()
            .filter(|u| {
                u.get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| name.to_lowercase() == username.to_lowercase())
            })
            .collect()
    };
    if candidates.is_empty() {
        bail!("No Foundry user matches the configured identity");
    }
    if candidates.len() != 1 {
        bail!("Multiple Foundry users match the configured identity; specify userid");
    }
    let user = candidates[0];
    if credential.userid.is_none() {
        credential.userid = Some(
            user_id(user)
                .ok_or_else(|| anyhow!("Foundry join user is missing an id"))?
                .to_owned(),
        );
    }
    if credential.username.is_none() {
        credential.username = Some(
            user.get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("Foundry join user is missing a name"))?
                .to_owned(),
        );
    }
    Ok(())
}

fn user_id(user: &Value) -> Option<&str> {
    user.get("_id")
        .or_else(|| user.get("id"))
        .and_then(Value::as_str)
}

fn has_complete_identity(credential: &FoundryCredential) -> bool {
    credential
        .userid
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        && credential
            .username
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

/// The identity key of a configured credential. Two credentials that differ
/// in any part never share a connection, session or socket.
fn identity_key(credential: &FoundryCredential) -> connection::IdentityKey {
    connection::IdentityKey::new(&[
        credential.id.as_deref().unwrap_or(""),
        &credential.hostname,
        credential.userid.as_deref().unwrap_or(""),
        credential.username.as_deref().unwrap_or(""),
        &credential.password,
    ])
}

/// Whether an upstream status is worth another attempt. A refusal is not:
/// retrying it only repeats the same answer.
fn is_transient_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || matches!(status.as_u16(), 408 | 425 | 429)
}

/// Carry an upstream `Retry-After` hint into the reconnect loop.
///
/// Only the delta-seconds form is honored; an HTTP date is ignored rather
/// than guessed at. The hint never extends the reconnect deadline, so a
/// server cannot make the gateway wait longer than its own budget.
fn retry_after(headers: &reqwest::header::HeaderMap, error: anyhow::Error) -> anyhow::Error {
    let Some(seconds) = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
    else {
        return error;
    };
    let message = error.to_string();
    error.context(connection::RetryAfter {
        after: Duration::from_secs(seconds.min(MAX_RETRY_AFTER_SECONDS)),
        message,
    })
}

/// The longest `Retry-After` the gateway will wait for.
const MAX_RETRY_AFTER_SECONDS: u64 = 60;

fn socket_request(
    host: &str,
    session: &str,
    generation: Option<u64>,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    socket_request_for_url(
        FoundryClient::host_url(host, "/socket.io/?EIO=4&transport=websocket")?,
        session,
        generation,
    )
}

fn socket_request_for_url(
    mut url: Url,
    session: &str,
    generation: Option<u64>,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    let is_modern = generation.is_some_and(|g| g >= 14);
    if !is_modern {
        url.query_pairs_mut().append_pair("session", session);
    }
    url.set_scheme(if url.scheme() == "http" { "ws" } else { "wss" })
        .map_err(|_| anyhow!("invalid websocket scheme"))?;
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| anyhow!("invalid websocket URL"))?;
    if is_modern || generation.is_none() {
        request
            .headers_mut()
            .insert("Cookie", format!("session={session}").parse()?);
    }
    Ok(request)
}

async fn wait_for_session<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    session: &str,
    userid: &str,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let frame = socket
            .next()
            .await
            .ok_or_else(|| anyhow!("Foundry socket closed"))?
            .map_err(|error| anyhow!("Foundry socket read failed: {error}"))?;
        if let Message::Text(text) = frame {
            let text = text.as_str();
            if text.starts_with("0{") {
                send_message(
                    socket,
                    Message::Text("40".into()),
                    "sending Foundry namespace connect",
                )
                .await?;
            } else if text == "2" {
                send_message(socket, Message::Text("3".into()), "sending Foundry pong").await?;
            } else if is_namespace_disconnect_or_error(text) {
                bail!("Foundry Socket.IO namespace disconnected or rejected the connection");
            } else if let Some(binding) = session_event(text, session, userid) {
                binding?;
                return Ok(());
            }
        }
    }
}

/// Verifies the session binding. Some supported Foundry versions omit `userId`;
/// in that case readiness is session-scoped only and user identity cannot be
/// independently verified from this event.
fn session_event(frame: &str, expected_session: &str, expected_user: &str) -> Option<Result<()>> {
    let start = frame.find('[')?;
    let payload: Value = serde_json::from_str(&frame[start..]).ok()?;
    if payload.get(0)?.as_str()? != "session" {
        return None;
    }
    let binding = payload.get(1);
    let session_matches = binding
        .and_then(|value| value.get("sessionId"))
        .and_then(Value::as_str)
        == Some(expected_session);
    let user_matches = match binding.and_then(|value| value.get("userId")) {
        None => true,
        Some(Value::String(user)) => user == expected_user,
        Some(_) => false,
    };
    Some(if session_matches && user_matches {
        Ok(())
    } else {
        Err(connection::mark_terminal(anyhow!(
            "Foundry websocket session binding failed"
        )))
    })
}

#[derive(Default)]
struct DocumentOptions {
    parent_uuid: Option<String>,
    pack: Option<String>,
}

fn write_operation(
    action: &str,
    values: Value,
    options: &DocumentOptions,
    modified_time: u64,
) -> Result<Value> {
    let mut operation = json!({"action": action, "pack": options.pack, "modifiedTime": modified_time, "render": true});
    match action {
        "create" => {
            operation["data"] = values;
            operation["renderSheet"] = json!(true);
        }
        "update" => {
            operation["updates"] = values;
            operation["diff"] = json!(false);
            operation["recursive"] = json!(true);
        }
        "delete" => {
            operation["ids"] = values;
            operation["deleteAll"] = json!(false);
        }
        _ => bail!("unsupported document action"),
    }
    if let Some(parent) = &options.parent_uuid {
        operation["parentUuid"] = json!(parent);
    }
    Ok(operation)
}

fn validate_write_ack(response: &Value, action: &str, requested: &Value) -> Result<()> {
    if response.get("action").and_then(Value::as_str) != Some(action) {
        return Err(ambiguous_mutation(
            "Foundry document response has unexpected action, and the operation may have been applied",
        ));
    }
    let result = response
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ambiguous_mutation(
                "Foundry document response has invalid result, and the operation may have been applied",
            )
        })?;
    let requested = requested
        .as_array()
        .ok_or_else(|| anyhow!("invalid document request"))?;
    match action {
        "create" => {
            let mut seen = std::collections::HashSet::new();
            for value in result {
                let id = value.get("_id").and_then(Value::as_str);
                let valid = id.is_some_and(|id| !id.trim().is_empty())
                    && seen.insert(id.unwrap().to_owned());
                if !valid {
                    return Err(ambiguous_mutation(
                        "Foundry create response is incomplete, and the operation may have been applied",
                    ));
                }
            }
            if result.len() != requested.len() {
                return Err(ambiguous_mutation(
                    "Foundry create response is incomplete, and the operation may have been applied",
                ));
            }
        }
        "update" | "delete" => {
            let mut seen = std::collections::HashSet::new();
            for item in result {
                let id = if action == "update" {
                    item.get("_id").and_then(Value::as_str)
                } else {
                    item.as_str()
                }
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| {
                    ambiguous_mutation(format!(
                        "Foundry {action} response has malformed document id, and the operation may have been applied"
                    ))
                })?;
                if !seen.insert(id.to_owned()) {
                    return Err(ambiguous_mutation(format!(
                        "Foundry {action} response has duplicate document id, and the operation may have been applied"
                    )));
                }
            }
            if seen.len() != requested.len() {
                return Err(ambiguous_mutation(format!(
                    "Foundry {action} response has unexpected document id, and the operation may have been applied"
                )));
            }
            for value in requested {
                let id = if action == "update" {
                    value.get("_id").and_then(Value::as_str)
                } else {
                    value.as_str()
                }
                .ok_or_else(|| anyhow!("invalid requested document id"))?;
                if !seen.contains(id) {
                    return Err(ambiguous_mutation(format!(
                        "Foundry {action} response is missing requested document id, and the operation may have been applied"
                    )));
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn bounded_string(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let end = value
        .char_indices()
        .take_while(|(index, character)| index + character.len_utf8() <= 256)
        .map(|(index, character)| index + character.len_utf8())
        .last()
        .unwrap_or(0);
    Some(value[..end].to_owned())
}

fn safe_authors(value: Option<&Value>) -> Option<Value> {
    let authors = value?;
    let entries = authors
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(authors));
    let safe = entries
        .iter()
        .filter_map(|author| {
            let mut output = serde_json::Map::new();
            if let Some(name) = author.as_str().and_then(bounded_string) {
                output.insert("name".to_owned(), Value::String(name));
            } else if let Some(object) = author.as_object() {
                for key in ["name", "url"] {
                    if let Some(value) = object
                        .get(key)
                        .and_then(Value::as_str)
                        .and_then(bounded_string)
                    {
                        output.insert(key.to_owned(), Value::String(value));
                    }
                }
            }
            (!output.is_empty()).then_some(Value::Object(output))
        })
        .take(20)
        .collect();
    Some(Value::Array(safe))
}

fn safe_modules(value: Option<&Value>) -> Vec<Value> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|module| {
            let object = module.as_object()?;
            let mut output = serde_json::Map::new();
            for key in ["id", "title", "description", "url", "version"] {
                if let Some(value) = object
                    .get(key)
                    .and_then(Value::as_str)
                    .and_then(bounded_string)
                {
                    output.insert(key.to_owned(), Value::String(value));
                }
            }
            if let Some(authors) = safe_authors(object.get("authors")) {
                output.insert("authors".to_owned(), authors);
            }
            (!output.is_empty()).then_some(Value::Object(output))
        })
        .take(100)
        .collect()
}

fn world_info(result: &Value) -> WorldInfo {
    let meta = result
        .get("world")
        .or_else(|| result.get("meta"))
        .unwrap_or(result);
    WorldInfo {
        id: world_string(meta, "id"),
        title: world_string(meta, "title"),
        system: world_string(meta, "system"),
        version: world_string_fallback(meta, &["coreVersion", "version"]),
        world_version: world_string(meta, "coreVersion")
            .and_then(|_| world_string(meta, "version")),
        release_generation: bounded_number(
            result
                .get("release")
                .and_then(|release| release.get("generation")),
        ),
        release_build: bounded_number(
            result
                .get("release")
                .and_then(|release| release.get("build")),
        ),
        description: world_string(meta, "description"),
        authors: safe_authors(meta.get("authors")),
        url: world_string(meta, "url"),
        system_version: world_string(meta, "systemVersion"),
        background: world_string(meta, "background"),
        modules: safe_modules(result.get("modules")),
    }
}

fn bounded_number(value: Option<&Value>) -> Option<Value> {
    match value? {
        Value::Number(number) if number.is_u64() => Some(Value::Number(number.clone())),
        Value::String(value) => bounded_string(value).map(Value::String),
        _ => None,
    }
}

fn world_string(meta: &Value, key: &str) -> Option<String> {
    meta.get(key)
        .and_then(Value::as_str)
        .and_then(bounded_string)
}

fn world_string_fallback(meta: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| world_string(meta, key))
}

impl FoundryClient {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .https_only(true)
                .redirect(reqwest::redirect::Policy::none())
                .timeout(HTTP_TIMEOUT)
                .build()?,
            connections: std::sync::Arc::new(connection::ConnectionManager::default()),
            #[cfg(any(test, feature = "loopback-interop"))]
            loopback_base_url: None,
        })
    }

    /// Build a client for a disposable loopback Foundry world.
    ///
    /// The production client is HTTPS-only (`https_only(true)`, `wss://`), so
    /// it cannot reach a local world that speaks plaintext HTTP and
    /// WebSocket. This constructor exists for the interoperability harness
    /// and is refused for anything but a loopback address, so a harness
    /// client can never be pointed at a live Foundry host. It is compiled
    /// only for this crate's own tests and for builds that enable the
    /// `loopback-interop` feature.
    #[cfg(any(test, feature = "loopback-interop"))]
    pub fn for_loopback(base_url: &str) -> Result<Self> {
        let base_url = Url::parse(base_url)?;
        if base_url.scheme() != "http" || base_url.host_str() != Some("127.0.0.1") {
            bail!("loopback transport must use loopback HTTP");
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(HTTP_TIMEOUT)
                .build()?,
            connections: std::sync::Arc::new(connection::ConnectionManager::default()),
            loopback_base_url: Some(base_url),
        })
    }

    #[cfg(test)]
    fn for_test(base_url: &str) -> Result<Self> {
        Self::for_loopback(base_url)
    }

    /// A client with an explicit connection manager, so a test can drive
    /// the reconnect and world-slot budgets it needs.
    #[cfg(test)]
    fn for_test_with_connections(
        base_url: &str,
        connections: std::sync::Arc<connection::ConnectionManager>,
    ) -> Result<Self> {
        let mut client = Self::for_test(base_url)?;
        client.connections = connections;
        Ok(client)
    }

    fn url_for(&self, host: &str, path: &str) -> Result<Url> {
        #[cfg(any(test, feature = "loopback-interop"))]
        if let Some(base) = &self.loopback_base_url {
            return Ok(base.join(path.trim_start_matches('/'))?);
        }
        Self::host_url(host, path)
    }

    /// The websocket request for one session.
    ///
    /// The loopback world the harness points the client at is reached over
    /// plaintext `ws://`; every other target keeps the production `wss://`
    /// form. Both paths share the version-specific session binding.
    fn socket_request(
        &self,
        host: &str,
        session: &str,
        generation: Option<u64>,
    ) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
        #[cfg(any(test, feature = "loopback-interop"))]
        if let Some(base) = &self.loopback_base_url {
            return socket_request_for_url(
                base.join("socket.io/?EIO=4&transport=websocket")?,
                session,
                generation,
            );
        }
        socket_request(host, session, generation)
    }

    fn host_url(host: &str, path: &str) -> Result<Url> {
        let host = host
            .trim()
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/');
        if host.is_empty() || host.contains('/') || host.contains('@') {
            bail!("hostname must be a host, optionally with port");
        }
        let url = Url::parse(&format!("https://{host}{path}"))
            .map_err(|_| anyhow!("invalid Foundry hostname"))?;
        Ok(url)
    }

    async fn session(
        &self,
        credential: &FoundryCredential,
    ) -> Result<(String, Option<u64>, String)> {
        if credential
            .userid
            .as_deref()
            .is_none_or(|v| v.trim().is_empty())
            && credential
                .username
                .as_deref()
                .is_none_or(|v| v.trim().is_empty())
        {
            return Err(connection::mark_terminal(anyhow!(
                "Foundry credential requires a userid or username"
            )));
        }
        let status = match timeout(
            HTTP_TIMEOUT,
            self.http
                .get(self.url_for(&credential.hostname, "/api/status")?)
                .send(),
        )
        .await
        {
            Ok(Ok(response)) if response.status().is_success() => {
                response.json().await.unwrap_or(Value::Null)
            }
            _ => Value::Null,
        };
        let generation = status
            .get("release")
            .and_then(|v| v.get("generation"))
            .and_then(Value::as_u64)
            .or_else(|| {
                status
                    .get("version")
                    .and_then(Value::as_str)
                    .and_then(|v| v.split('.').next()?.parse().ok())
            });
        let join_url = self.url_for(&credential.hostname, "/join")?;
        let response = self
            .http
            .get(join_url.clone())
            .send()
            .await
            .map_err(|_| anyhow!("Foundry session request failed or timed out"))?;
        if !response.status().is_success() {
            let status = response.status();
            let message =
                format!("Foundry session request returned an unsuccessful status {status}");
            return Err(if is_transient_status(status) {
                retry_after(response.headers(), anyhow!("{message}"))
            } else {
                connection::mark_terminal(crate::upstream_authentication(message))
            });
        }
        let session = response
            .headers()
            .get_all(SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find_map(|v| {
                v.split(';')
                    .next()?
                    .strip_prefix("session=")
                    .map(str::to_owned)
            })
            .unwrap_or_else(generate_session_id);
        let mut credential = credential.clone();
        match self
            .join_users(&credential.hostname, &session, generation)
            .await?
        {
            Some(users) => {
                resolve_identity(&mut credential, &users).map_err(connection::mark_terminal)?
            }
            None if has_complete_identity(&credential) => {}
            None => {
                bail!("Foundry join user data is unavailable; cannot resolve the missing identity")
            }
        }
        let payload = join_payload(&credential);
        let auth = self
            .http
            .post(join_url)
            .header("cookie", format!("session={session}"))
            .header(
                "origin",
                self.url_for(&credential.hostname, "/")?
                    .origin()
                    .ascii_serialization(),
            )
            .json(&payload)
            .send()
            .await
            .map_err(|_| anyhow!("Foundry authentication request failed or timed out"))?;
        let status = auth.status();
        if !status.is_success() {
            if is_transient_status(status) {
                return Err(retry_after(
                    auth.headers(),
                    anyhow!("Foundry authentication request returned transient status {status}"),
                ));
            }
            return Err(connection::mark_terminal(crate::upstream_authentication(
                format!("Foundry authentication failed (HTTP status {status})"),
            )));
        }
        let body: Value = auth.json().await.unwrap_or(Value::Null);
        if body.get("status").and_then(Value::as_str) != Some("success") {
            return Err(connection::mark_terminal(crate::upstream_authentication(
                "Foundry authentication failed",
            )));
        }
        let userid = credential.userid.ok_or_else(|| {
            connection::mark_terminal(anyhow!("Foundry credential requires a userid"))
        })?;
        Ok((session, generation, userid))
    }

    async fn join_users(
        &self,
        host: &str,
        session: &str,
        generation: Option<u64>,
    ) -> Result<Option<Vec<Value>>> {
        let request = self.socket_request(host, session, generation)?;
        let mut socket = match timeout(SOCKET_CONNECT_TIMEOUT, connect_socket(request)).await {
            Ok(Ok(socket)) => socket,
            Ok(Err(_)) | Err(_) => return Ok(None),
        };
        let result = match timeout(SESSION_TIMEOUT, async {
            loop {
                let frame = match socket.next().await {
                    Some(Ok(frame)) => frame,
                    Some(Err(_)) | None => return Ok(None),
                };
                if let Message::Text(text) = frame {
                    if text.starts_with("0{") {
                        send_message(
                            &mut socket,
                            Message::Text("40".into()),
                            "sending Foundry namespace connect",
                        )
                        .await?;
                    } else if text == "2" {
                        send_message(
                            &mut socket,
                            Message::Text("3".into()),
                            "sending Foundry pong",
                        )
                        .await?;
                    } else if is_namespace_disconnect_or_error(text.as_str()) {
                        bail!(
                            "Foundry Socket.IO namespace disconnected or rejected the connection"
                        );
                    } else if text.starts_with("40") || text.starts_with("42[\"session\"") {
                        send_message(
                            &mut socket,
                            Message::Text("421[\"getJoinData\"]".into()),
                            "sending Foundry join-data request",
                        )
                        .await?;
                    } else if text.starts_with("431[") {
                        let data: Value = serde_json::from_str(&text[3..]).map_err(|_| {
                            connection::mark_terminal(anyhow!("invalid Foundry join data response"))
                        })?;
                        return data
                            .get(0)
                            .and_then(|v| v.get("users"))
                            .and_then(Value::as_array)
                            .cloned()
                            .map(Some)
                            .ok_or_else(|| {
                                connection::mark_terminal(anyhow!(
                                    "Foundry join data did not contain users"
                                ))
                            });
                    }
                }
            }
        })
        .await
        {
            Ok(result) => result?,
            Err(_) => None,
        };
        let _ = socket.close(None).await;
        Ok(result)
    }

    async fn request(
        &self,
        credential: &FoundryCredential,
        document: &str,
        operation: Value,
    ) -> Result<Value> {
        let key = identity_key(credential);
        if document == "World" {
            return self
                .connections
                .world(key, || self.connect_for(credential))
                .await;
        }
        let action = operation
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("get")
            .to_owned();
        let requested = match action.as_str() {
            "create" => operation.get("data").cloned(),
            "update" => operation.get("updates").cloned(),
            "delete" => operation.get("ids").cloned(),
            _ => None,
        };
        let response = self
            .connections
            .exchange(
                key,
                socket_mux::Request::Document {
                    document: document.to_owned(),
                    action: action.clone(),
                    operation,
                },
                "document",
                || self.connect_for(credential),
            )
            .await?;
        if action == "get" {
            return response
                .get("result")
                .filter(|value| value.is_array())
                .cloned()
                .ok_or_else(|| anyhow!("Foundry document response has invalid result"));
        }
        let requested = requested.ok_or_else(|| anyhow!("invalid document request"))?;
        validate_write_ack(&response, &action, &requested)?;
        Ok(response["result"].clone())
    }

    /// Authenticate and open the identity's websocket, verifying the session
    /// binding before the connection is handed to the connection manager.
    async fn connect_for(&self, credential: &FoundryCredential) -> Result<connection::Socket> {
        let (session, generation, userid) = self.session(credential).await?;
        let request = self.socket_request(&credential.hostname, &session, generation)?;
        let mut socket = timeout(SOCKET_CONNECT_TIMEOUT, connect_socket(request))
            .await
            .map_err(|_| anyhow!("Foundry websocket connection timed out"))?
            .map_err(|_| anyhow!("Foundry websocket connection failed"))?;
        timeout(
            SESSION_TIMEOUT,
            wait_for_session(&mut socket, &session, &userid),
        )
        .await
        .map_err(|_| anyhow!("Timed out waiting for Foundry session binding"))??;
        Ok(socket)
    }

    pub async fn list_documents(
        &self,
        credential: &FoundryCredential,
        collection: &str,
    ) -> Result<Value> {
        let kind = match collection {
            "actors" => "Actor",
            "items" => "Item",
            "folders" => "Folder",
            "users" => "User",
            "scenes" => "Scene",
            "journal" => "JournalEntry",
            "macros" => "Macro",
            "cards" => "Cards",
            "playlists" => "Playlist",
            "tables" => "RollTable",
            "combats" => "Combat",
            "messages" => "ChatMessage",
            "settings" => "Setting",
            _ => bail!("unsupported collection"),
        };
        self.request(
            credential,
            kind,
            json!({"action":"get", "query":{}, "index":false, "broadcast":false}),
        )
        .await
    }

    /// Read a world collection with a server-side scalar query.
    pub async fn query_documents(
        &self,
        credential: &FoundryCredential,
        collection: &str,
        query: Value,
    ) -> Result<Value> {
        let kind = match collection {
            "actors" => "Actor",
            "items" => "Item",
            "folders" => "Folder",
            "users" => "User",
            "scenes" => "Scene",
            "journal" => "JournalEntry",
            "macros" => "Macro",
            "cards" => "Cards",
            "playlists" => "Playlist",
            "tables" => "RollTable",
            "combats" => "Combat",
            "messages" => "ChatMessage",
            "settings" => "Setting",
            _ => bail!("unsupported collection"),
        };
        self.request(
            credential,
            kind,
            json!({"action":"get", "query":query, "index":false, "broadcast":false}),
        )
        .await
    }

    pub async fn create_document(
        &self,
        cred: &FoundryCredential,
        kind: &str,
        data: Vec<Value>,
        parent_uuid: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value> {
        if data.is_empty() || data.iter().any(|v| !v.is_object()) {
            bail!("document data must contain objects");
        }
        self.write_documents(cred, kind, "create", json!(data), parent_uuid, pack)
            .await
    }

    pub async fn modify_document(
        &self,
        cred: &FoundryCredential,
        kind: &str,
        id: &str,
        updates: Vec<Value>,
        parent_uuid: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value> {
        if id.trim().is_empty() {
            bail!("document id is required");
        }
        if updates.len() != 1 {
            bail!("exactly one update is required");
        }
        let updates: Vec<Value> = updates
            .into_iter()
            .map(|mut update| {
                let object = update
                    .as_object_mut()
                    .ok_or_else(|| anyhow!("document updates must be objects"))?;
                object.insert("_id".into(), json!(id));
                Ok(update)
            })
            .collect::<Result<_>>()?;
        self.write_documents(cred, kind, "update", json!(updates), parent_uuid, pack)
            .await
    }

    pub async fn delete_document(
        &self,
        cred: &FoundryCredential,
        kind: &str,
        ids: Vec<String>,
        parent_uuid: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value> {
        if ids.is_empty() || ids.iter().any(|id| id.trim().is_empty()) {
            bail!("document ids are required");
        }
        let mut unique = std::collections::HashSet::new();
        if ids.iter().any(|id| !unique.insert(id)) {
            bail!("duplicate document ids are not allowed");
        }
        self.write_documents(cred, kind, "delete", json!(ids), parent_uuid, pack)
            .await
    }

    async fn write_documents(
        &self,
        cred: &FoundryCredential,
        kind: &str,
        action: &str,
        values: Value,
        parent_uuid: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value> {
        if kind.trim().is_empty() || kind == "World" {
            bail!("invalid document type");
        }
        let options = DocumentOptions {
            parent_uuid: parent_uuid.map(str::to_owned),
            pack: pack.map(str::to_owned),
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        self.request(cred, kind, write_operation(action, values, &options, now)?)
            .await
    }

    pub async fn world(&self, credential: &FoundryCredential) -> Result<WorldInfo> {
        let result = self.request(credential, "World", json!({})).await?;
        Ok(world_info(&result))
    }
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_debug_output_redacts_password_and_username() {
        let credential = FoundryCredential {
            id: Some("config-1".into()),
            hostname: "example.org".into(),
            userid: Some("user-1".into()),
            username: Some("Player".into()),
            password: "super-secret-password".into(),
        };

        let debug = format!("{credential:?}");

        assert!(!debug.contains("super-secret-password"), "{debug}");
        assert!(!debug.contains("Player"), "{debug}");
        assert!(debug.contains("REDACTED"), "{debug}");
    }

    #[tokio::test]
    async fn write_deadline_expires_independently_of_response_wait() {
        assert_ne!(WRITE_TIMEOUT, RESPONSE_TIMEOUT);
        let result = write_with_deadline(
            tokio::time::sleep(Duration::from_secs(1)),
            Duration::from_millis(1),
            "sending test request",
        )
        .await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("sending test request"));
    }

    #[test]
    fn websocket_message_limit_preserves_large_foundry_reads() {
        assert_eq!(websocket_config().max_message_size, Some(64 * 1024 * 1024));
        assert_eq!(websocket_config().max_frame_size, Some(64 * 1024 * 1024));
    }

    #[tokio::test]
    async fn rejects_multiple_updates_and_duplicate_delete_ids_before_connecting() {
        let client = FoundryClient::new().unwrap();
        let credential = FoundryCredential {
            id: None,
            hostname: "example.org".into(),
            userid: None,
            username: None,
            password: String::new(),
        };
        let error = client
            .modify_document(
                &credential,
                "Actor",
                "a",
                vec![json!({}), json!({})],
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exactly one update"), "{error}");
        let error = client
            .delete_document(
                &credential,
                "Actor",
                vec!["a".into(), "a".into()],
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("duplicate"), "{error}");
    }

    #[test]
    fn exact_match_failures_carry_ambiguity_caveat() {
        let update = json!({"action":"update","result":[{"_id":"b"}]});
        let error = validate_write_ack(&update, "update", &json!([{"_id":"a"}]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("may have been applied"), "{error}");
        let delete = json!({"action":"delete","result":["a","c"]});
        let error = validate_write_ack(&delete, "delete", &json!(["a", "b"]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("may have been applied"), "{error}");
        let duplicate = json!({"action":"delete","result":["a","a"]});
        let error = validate_write_ack(&duplicate, "delete", &json!(["a"]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("may have been applied"), "{error}");
        let create = json!({"action":"create","result":[{"_id":""}]});
        let error = validate_write_ack(&create, "create", &json!([{}]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("may have been applied"), "{error}");
    }

    #[test]
    fn write_ack_rejects_extra_malformed_and_duplicate_ids() {
        for (action, requested, responses) in [
            (
                "update",
                json!([{"_id":"a"}]),
                vec![
                    json!([{"_id":"a"},{"_id":"b"}]),
                    json!([{"_id":"a"},{}]),
                    json!([{"_id":"a"},{"_id":"a"}]),
                ],
            ),
            (
                "delete",
                json!(["a", "b"]),
                vec![
                    json!(["a", "b", "c"]),
                    json!(["a", "b", null]),
                    json!(["a", "b", "a"]),
                    json!(["a", "a"]),
                ],
            ),
            (
                "create",
                json!([{}, {}]),
                vec![json!([{"_id":"a"},{"_id":"a"}])],
            ),
        ] {
            for result in responses {
                assert!(
                    validate_write_ack(
                        &json!({"action":action,"result":result}),
                        action,
                        &requested
                    )
                    .is_err(),
                    "{action} {result}"
                );
            }
        }
        assert!(validate_write_ack(
            &json!({"action":"delete","result":["b","a"]}),
            "delete",
            &json!(["a", "b"])
        )
        .is_ok());
    }

    #[test]
    fn create_ack_requires_nonempty_ids_and_exact_count() {
        let requested = json!([{}]);
        for result in [
            json!([{}]),
            json!([{"_id":""}]),
            json!([{"_id":"  "}]),
            json!([{"_id":1}]),
            json!([]),
            json!([{"_id":"a"},{"_id":"b"}]),
        ] {
            assert!(validate_write_ack(
                &json!({"action":"create","result":result}),
                "create",
                &requested
            )
            .is_err());
        }
        assert!(validate_write_ack(
            &json!({"action":"create","result":[{"_id":"a"}]}),
            "create",
            &requested
        )
        .is_ok());
    }

    #[test]
    fn rejects_path_in_hostname() {
        assert!(FoundryClient::host_url("example.com/world", "/join").is_err());
    }
    #[test]
    fn websocket_url_uses_wss_and_version_specific_session_binding() {
        let legacy = socket_request("https://example.org", "s-1", Some(13)).unwrap();
        assert_eq!(legacy.uri().scheme_str().unwrap(), "wss");
        assert_eq!(
            legacy.uri().query(),
            Some("EIO=4&transport=websocket&session=s-1")
        );
        let modern = socket_request("example.org", "s-1", Some(14)).unwrap();
        assert_eq!(modern.uri().scheme_str().unwrap(), "wss");
        assert_eq!(modern.headers().get("cookie").unwrap(), "session=s-1");
        assert_eq!(modern.uri().query(), Some("EIO=4&transport=websocket"));
        let unknown = socket_request("example.org", "s-1", None).unwrap();
        assert_eq!(unknown.uri().scheme_str().unwrap(), "wss");
        assert!(unknown.uri().query().unwrap().contains("session=s-1"));
        assert_eq!(unknown.headers().get("cookie").unwrap(), "session=s-1");
    }

    #[test]
    fn builds_upstream_auth_payload_with_user_id_aliases() {
        let credential = FoundryCredential {
            id: None,
            hostname: "example.org".into(),
            userid: Some("u1".into()),
            username: Some("Player".into()),
            password: "secret".into(),
        };
        assert_eq!(
            join_payload(&credential),
            json!({"userid":"u1","userId":"u1","username":"Player","password":"secret","action":"join"})
        );
    }

    #[test]
    fn rejects_credentials_without_any_identity_or_with_blank_identity() {
        let base = FoundryCredential {
            id: None,
            hostname: "example.org".into(),
            userid: None,
            username: None,
            password: "secret".into(),
        };
        assert!(resolve_identity(&mut base.clone(), &[]).is_err());
        let mut blank = FoundryCredential {
            username: Some("  ".into()),
            ..base
        };
        assert!(resolve_identity(&mut blank, &[])
            .unwrap_err()
            .to_string()
            .contains("blank"));
    }

    #[test]
    fn username_resolution_prefers_exact_then_unique_case_insensitive_match() {
        let mut exact = FoundryCredential {
            id: None,
            hostname: "example.org".into(),
            userid: None,
            username: Some("alex".into()),
            password: String::new(),
        };
        let users = [
            json!({"_id":"upper","name":"Alex"}),
            json!({"_id":"exact","name":"alex"}),
        ];
        resolve_identity(&mut exact, &users).unwrap();
        assert_eq!(exact.userid.as_deref(), Some("exact"));

        let mut case_insensitive = FoundryCredential {
            userid: None,
            username: Some("aLeX".into()),
            ..exact
        };
        resolve_identity(
            &mut case_insensitive,
            &[json!({"_id":"only","name":"Alex"})],
        )
        .unwrap();
        assert_eq!(case_insensitive.userid.as_deref(), Some("only"));

        let mut ambiguous = FoundryCredential {
            username: Some("aLeX".into()),
            userid: None,
            id: None,
            hostname: "example.org".into(),
            password: String::new(),
        };
        let error = resolve_identity(&mut ambiguous, &users)
            .unwrap_err()
            .to_string();
        assert!(error.contains("Multiple Foundry users"), "{error}");
    }

    #[test]
    fn verifies_session_event_session_and_optional_user_binding() {
        assert!(matches!(
            session_event(
                "42[\"session\",{\"sessionId\":\"s\",\"userId\":\"u\"}]",
                "s",
                "u"
            ),
            Some(Ok(()))
        ));
        assert!(matches!(
            session_event("42[\"session\",{\"sessionId\":\"s\"}]", "s", "u"),
            Some(Ok(()))
        ));
        assert!(session_event(
            "42[\"session\",{\"sessionId\":\"other\",\"userId\":\"u\"}]",
            "s",
            "u"
        )
        .unwrap()
        .is_err());
        assert!(session_event(
            "42[\"session\",{\"sessionId\":\"s\",\"userId\":null}]",
            "s",
            "u"
        )
        .unwrap()
        .is_err());
        assert!(session_event(
            "42[\"session\",{\"sessionId\":\"s\",\"userId\":7}]",
            "s",
            "u"
        )
        .unwrap()
        .is_err());
        assert!(session_event(
            "42[\"session\",{\"sessionId\":\"s\",\"userId\":\"other\"}]",
            "s",
            "u"
        )
        .unwrap()
        .is_err());
        assert!(session_event("42[\"session\",null]", "s", "u")
            .unwrap()
            .is_err());
        assert!(session_event("42[\"other\",null]", "s", "u").is_none());
    }

    #[test]
    fn builds_write_operations_with_reference_protocol_fields() {
        let options = DocumentOptions {
            parent_uuid: Some("Actor.a".into()),
            pack: Some("world.items".into()),
        };
        let create = write_operation("create", json!([{"name":"Sword"}]), &options, 123).unwrap();
        assert_eq!(
            create,
            json!({"action":"create","pack":"world.items","parentUuid":"Actor.a","data":[{"name":"Sword"}],"modifiedTime":123,"renderSheet":true,"render":true})
        );
        let update = write_operation(
            "update",
            json!([{"_id":"i","name":"A"}]),
            &DocumentOptions::default(),
            123,
        )
        .unwrap();
        assert_eq!(
            update,
            json!({"action":"update","pack":null,"updates":[{"_id":"i","name":"A"}],"modifiedTime":123,"diff":false,"recursive":true,"render":true})
        );
        let delete =
            write_operation("delete", json!(["i"]), &DocumentOptions::default(), 123).unwrap();
        assert_eq!(
            delete,
            json!({"action":"delete","pack":null,"ids":["i"],"modifiedTime":123,"deleteAll":false,"render":true})
        );
    }

    #[test]
    fn write_ack_requires_action_and_complete_results() {
        let update = json!([{"_id":"a"}, {"_id":"b"}]);
        assert!(validate_write_ack(
            &json!({"action":"delete","result":[{"_id":"a"},{"_id":"b"}]}),
            "update",
            &update
        )
        .is_err());
        assert!(validate_write_ack(
            &json!({"action":"update","result":[{"_id":"a"}]}),
            "update",
            &update
        )
        .is_err());
        assert!(validate_write_ack(
            &json!({"action":"delete","result":["a"]}),
            "delete",
            &json!(["a", "b"])
        )
        .is_err());
        assert!(validate_write_ack(
            &json!({"action":"create","result":[]}),
            "create",
            &json!([{}])
        )
        .is_err());
        assert!(validate_write_ack(
            &json!({"action":"create","result":[{"_id":"a"}]}),
            "create",
            &json!([{}])
        )
        .is_ok());
        assert!(validate_write_ack(
            &json!({"action":"delete","result":["a","b"]}),
            "delete",
            &json!(["a", "b"])
        )
        .is_ok());
        assert!(validate_write_ack(
            &json!({"action":"update","result":[{"_id":"a"},{"_id":"b"}]}),
            "update",
            &update
        )
        .is_ok());
    }

    #[test]
    fn extracts_live_v14_world_metadata() {
        let info = world_info(&json!({
            "world": {"title": "My World", "system": "mta", "coreVersion": "14.370"},
            "system": {"id": "mta", "version": "0.19.2"},
            "release": {"generation": 14, "build": 370}
        }));
        assert_eq!(info.title.as_deref(), Some("My World"));
        assert_eq!(info.system.as_deref(), Some("mta"));
        assert_eq!(info.version.as_deref(), Some("14.370"));
    }

    #[test]
    fn world_metadata_includes_safe_reference_fields_and_bounded_modules() {
        let info = world_info(&json!({
            "world": {
                "id": "summerlands",
                "title": " Summerlands ",
                "system": " dnd5e ",
                "coreVersion": "14.370",
                "version": " 2.1.0 ",
                "systemVersion": " 4.3.1 ",
                "description": " A world ",
                "authors": [{"name":" A. Author ","url":" https://author.test ","email":"private@example.test"}],
                "url": "https://example.test/world",
                "background": "worlds/summerlands/bg.webp",
                "actors": [{"name": "Must not leak"}],
                "users": [{"password": "secret"}],
                "session": {"userId": "secret"},
                "password": "secret"
            },
            "modules": [
                {"id":" module-a ","title":" Module A ","description":" A module ","authors":[{"name":" Dev ","email":"private@example.test"}],"url":" https://module.test ","version":" 1.2 ","token":"secret"},
                {"id":"module-b","title":"Module B","description":"B","version":"2"}
            ],
            "scenes": [{"name":"Must not leak"}],
            "settings": {"private":"secret"},
            "users": [{"password":"secret"}]
        }));

        assert_eq!(info.title.as_deref(), Some("Summerlands"));
        assert_eq!(info.system.as_deref(), Some("dnd5e"));
        assert_eq!(info.version.as_deref(), Some("14.370"));
        assert_eq!(info.world_version.as_deref(), Some("2.1.0"));
        assert_eq!(info.id.as_deref(), Some("summerlands"));
        assert_eq!(info.system_version.as_deref(), Some("4.3.1"));
        assert_eq!(info.description.as_deref(), Some("A world"));
        assert_eq!(
            info.authors,
            Some(json!([{"name":"A. Author","url":"https://author.test"}]))
        );
        assert_eq!(info.url.as_deref(), Some("https://example.test/world"));
        assert_eq!(
            info.background.as_deref(),
            Some("worlds/summerlands/bg.webp")
        );
        assert_eq!(info.modules.len(), 2);
        assert_eq!(info.modules[0]["id"], "module-a");
        assert_eq!(info.modules[0]["title"], "Module A");
        assert_eq!(info.modules[0]["description"], "A module");
        assert_eq!(info.modules[0]["version"], "1.2");
        assert_eq!(info.modules[0]["authors"], json!([{"name":"Dev"}]));
        assert!(info.modules[0].get("token").is_none());
        let output = serde_json::to_value(info).unwrap();
        for excluded in [
            "actors", "users", "session", "password", "scenes", "settings",
        ] {
            assert!(output.get(excluded).is_none(), "{excluded} leaked");
        }
        assert!(!output.to_string().contains("private@example.test"));
    }

    #[test]
    fn world_metadata_bounds_fields_and_filters_invalid_entries_before_caps() {
        let long = "é".repeat(400);
        let modules = std::iter::once(json!(null))
            .chain(std::iter::once(json!("invalid")))
            .chain(std::iter::once(json!({"token":"secret"})))
            .chain(std::iter::once(json!({"id":long})))
            .chain((0..150).map(|i| json!({"id":format!("module-{i}"),"title":long})))
            .collect::<Vec<_>>();
        let input = json!({
            "world": {
                "title": long,
                "authors": [null, 7, {"email":"private@example.test"}, " String Author ", {"name":long}]
            },
            "modules": modules,
            "release": {"generation": 14, "build": 370, "secret": "omit"}
        });
        let info = world_info(&input);
        assert_eq!(info.title.as_ref().unwrap().chars().count(), 128);
        assert_eq!(info.title.as_ref().unwrap().len(), 256);
        let authors = info.authors.as_ref().unwrap().as_array().unwrap();
        assert_eq!(authors.len(), 2);
        assert_eq!(authors[0]["name"], "String Author");
        assert_eq!(authors[1]["name"].as_str().unwrap().chars().count(), 128);
        assert_eq!(authors[1]["name"].as_str().unwrap().len(), 256);
        assert_eq!(info.modules.len(), 100);
        assert_eq!(info.modules[0]["id"].as_str().unwrap().chars().count(), 128);
        assert_eq!(info.modules[0]["id"].as_str().unwrap().len(), 256);
        assert_eq!(info.modules[1]["id"], "module-0");
        assert_eq!(info.release_generation, Some(json!(14)));
        assert_eq!(info.release_build, Some(json!(370)));
        let output = serde_json::to_value(&info).unwrap();
        assert!(output["title"].as_str().unwrap().len() < 512);
        assert_eq!(output["modules"].as_array().unwrap().len(), 100);
        assert!(!output.to_string().contains("private@example.test"));
        assert!(output["release"].is_null());
    }

    #[test]
    fn reads_world_metadata_from_the_world_payload() {
        let world = json!({"meta":{"title":"My World","system":"dnd5e","version":"1.0"}});
        let info = world_info(&world);
        assert_eq!(info.title.as_deref(), Some("My World"));
        assert_eq!(info.system.as_deref(), Some("dnd5e"));
    }
}
