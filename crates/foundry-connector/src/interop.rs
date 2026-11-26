//! A disposable local Foundry world for interoperability testing.
//!
//! The interoperability harness has to drive the *real* connector against a
//! server that speaks Foundry's HTTP and Socket.IO protocols, without live
//! credentials and without touching a real world. This module is that
//! server: an in-process HTTP + WebSocket endpoint that answers `/api/status`,
//! `/join`, and the Socket.IO frames the connector uses for world and
//! document reads.
//!
//! It is compiled for this crate's own tests and for builds that enable the
//! `loopback-interop` feature, so the interoperability tests in the server
//! crate drive exactly the same world the connector's tests use — there is
//! one fake world, not two mocking layers.
//!
//! Two worlds are available:
//!
//! - the *canned* world (`start_canned`) answers every document request with
//!   one synthetic document named after the requested type, and the world
//!   event with a fixed acknowledgement frame. It is what the connector's
//!   unit tests use to exercise protocol handling.
//! - the *fixture* world (`start_with_world`) serves a small, realistic
//!   world — journal entries, an actor, an item, a scene, and a compendium
//!   pack — and answers document queries with the matching documents. It is
//!   what the interoperability harness reads.
//!
//! Nothing here reaches the network: the world binds a loopback port and
//! refuses to be addressed by any other host.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

use axum::{
    extract::{
        ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, HeaderValue, Uri},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};

/// The contents of one disposable world.
#[derive(Clone, Debug)]
pub struct WorldFixture {
    /// The Foundry release generation the world reports, through
    /// `/api/status` and the `release` block of the world event. Generation
    /// 14 and later bind the websocket session with a cookie; earlier ones
    /// use a query parameter.
    pub generation: u64,
    /// The session id the world issues in its `session` cookie.
    pub session: String,
    /// The users the world reports through `getJoinData`.
    pub users: Vec<Value>,
    /// The world metadata the world event returns.
    pub world: Value,
    /// The compendium packs the world advertises.
    pub packs: Vec<Value>,
    /// The documents the world holds. Every document carries the Foundry
    /// document type in a `type` field; the field is the world's own
    /// bookkeeping and is not part of the documents it serves.
    pub documents: Vec<Value>,
}

impl WorldFixture {
    /// A small world with two journal entries, one actor, one item, and one
    /// scene: enough to exercise the journal read path and the read tools
    /// around it.
    pub fn journal_world() -> Self {
        Self {
            generation: 14,
            session: "interop-session".into(),
            users: vec![json!({"_id": "user-1", "name": "Interop User"})],
            world: json!({
                "id": "interop-world",
                "title": "Interop World",
                "system": "dnd5e",
                "coreVersion": "14.1",
                "description": "Disposable interoperability world",
                "authors": [{"name": "Interop Harness"}],
            }),
            packs: vec![json!({
                "id": "world.items",
                "label": "Items",
                "type": "Item",
                "packageName": "world"
            })],
            documents: vec![
                json!({
                    "_id": "journal-1",
                    "name": "Interop Session One",
                    "type": "JournalEntry",
                    "content": "<p>First disposable entry.</p>"
                }),
                json!({
                    "_id": "journal-2",
                    "name": "Interop Session Two",
                    "type": "JournalEntry",
                    "content": "<p>Second disposable entry.</p>"
                }),
                json!({"_id": "actor-1", "name": "Interop Hero", "type": "Actor"}),
                json!({"_id": "item-1", "name": "Interop Sword", "type": "Item"}),
                json!({"_id": "scene-1", "name": "Interop Square", "type": "Scene"}),
            ],
        }
    }

    /// The documents of one Foundry type that match a server-side query.
    /// The query holds top-level scalars only, the same shape the server
    /// sends.
    pub fn documents(&self, kind: &str, query: &Value) -> Vec<Value> {
        self.documents
            .iter()
            .filter(|document| document.get("type").and_then(Value::as_str) == Some(kind))
            .filter(|document| matches_query(document, query))
            .map(|document| {
                // `type` is the world's own bookkeeping, not part of the
                // document a real world would serve.
                let mut document = document.clone();
                if let Some(object) = document.as_object_mut() {
                    object.remove("type");
                }
                document
            })
            .collect()
    }
}

/// Match a document against a server-side query. Numbers compare the way
/// JavaScript compares them, so an integral and a fractional spelling of the
/// same number match.
fn matches_query(document: &Value, query: &Value) -> bool {
    let Some(query) = query.as_object() else {
        return true;
    };
    query.iter().all(|(key, expected)| {
        let Some(actual) = document.get(key) else {
            return false;
        };
        match (actual, expected) {
            (Value::Number(actual), Value::Number(expected)) => {
                actual.as_f64() == expected.as_f64()
            }
            (actual, expected) if !expected.is_object() && !expected.is_array() => {
                actual == expected
            }
            _ => false,
        }
    })
}

/// What the world answers with.
enum World {
    /// A canned world: every document request answers with one synthetic
    /// document named after the requested type, and the world event answers
    /// with a fixed acknowledgement frame.
    Canned { ack: String },
    /// A fixture world: documents, metadata, and users come from the
    /// fixture.
    Fixture(WorldFixture),
}

pub(crate) struct FakeState {
    pub(crate) requests: Mutex<Vec<String>>,
    pub(crate) generation: u64,
    pub(crate) session: String,
    pub(crate) binding: Value,
    pub(crate) join_data: Option<Value>,
    world: World,
    pub(crate) next_socket: AtomicUsize,
    pub(crate) disconnect_socket: AtomicUsize,
    pub(crate) fail_authentication: AtomicBool,
    pub(crate) auth_non_success_success_body: AtomicBool,
    pub(crate) auth_transient_status: AtomicUsize,
    pub(crate) auth_transient_attempts: AtomicUsize,
    pub(crate) join_data_transport_failures: AtomicUsize,
    pub(crate) world_delay_ms: AtomicUsize,
    pub(crate) hold_document_acks: AtomicBool,
    pub(crate) document_requests: tokio::sync::Semaphore,
    pub(crate) release_document_acks: tokio::sync::Semaphore,
    pub(crate) namespace_after_world: Mutex<Option<String>>,
    /// How long the authentication request takes, so a test can make the
    /// connection handshake slow.
    pub(crate) auth_delay_ms: AtomicUsize,
    /// Close the socket instead of answering a mutating request, so a test
    /// can observe the ambiguous outcome of a change that may have been
    /// applied.
    pub(crate) disconnect_on_mutation: AtomicBool,
    /// Refuse document requests for this document type, so a test can
    /// observe that an upstream refusal leaves the connection usable.
    pub(crate) error_document: Mutex<Option<String>>,
    /// The `Retry-After` hint, in seconds, sent with a transient
    /// authentication status.
    pub(crate) auth_retry_after_seconds: AtomicUsize,
    /// The status `GET /join` answers with. Two hundred means success.
    pub(crate) join_status: AtomicUsize,
    /// How many authentication requests hang instead of answering, so a test
    /// can make one attempt time out.
    pub(crate) auth_hang_attempts: AtomicUsize,
    /// How long a hanging authentication request waits before it answers.
    pub(crate) auth_hang_ms: AtomicUsize,
}

/// A running disposable Foundry world.
pub struct FakeWorld {
    pub(crate) base_url: String,
    pub(crate) state: Arc<FakeState>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for FakeWorld {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl FakeWorld {
    /// The base URL of the running world. It is always a loopback address.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Every request the world has served, in order. The harness asserts
    /// against this log to show that the real connector really spoke HTTP and
    /// WebSocket to the world.
    pub fn request_log(&self) -> Vec<String> {
        self.state.requests.lock().unwrap().clone()
    }

    /// Make the world drop the socket instead of answering a change, so a
    /// harness can observe the ambiguous outcome of a change that may have
    /// been applied.
    pub fn drop_socket_on_mutation(&self) {
        self.state
            .disconnect_on_mutation
            .store(true, Ordering::Release);
    }

    /// Start a canned world: one synthetic document per requested type and a
    /// fixed world acknowledgement.
    pub async fn start_canned(ack: &str, generation: u64, session: &str) -> Self {
        Self::start_canned_with_users(
            ack,
            generation,
            session,
            vec![json!({"_id":"user-1","name":"Test User"})],
        )
        .await
    }

    /// Start a canned world that reports the given users.
    pub async fn start_canned_with_users(
        ack: &str,
        generation: u64,
        session: &str,
        users: Vec<Value>,
    ) -> Self {
        Self::start_canned_with_join_data(ack, generation, session, Some(json!({"users": users})))
            .await
    }

    /// Start a canned world with the given join data (or none).
    pub async fn start_canned_with_join_data(
        ack: &str,
        generation: u64,
        session: &str,
        join_data: Option<Value>,
    ) -> Self {
        Self::start_canned_with_binding(
            ack,
            generation,
            session,
            join_data,
            json!({"sessionId":session,"userId":"user-1"}),
        )
        .await
    }

    /// Start a canned world with the given join data and session binding.
    pub async fn start_canned_with_binding(
        ack: &str,
        generation: u64,
        session: &str,
        join_data: Option<Value>,
        binding: Value,
    ) -> Self {
        let state = Arc::new(FakeState {
            requests: Mutex::new(Vec::new()),
            generation,
            session: session.into(),
            binding,
            join_data,
            world: World::Canned { ack: ack.into() },
            next_socket: AtomicUsize::new(1),
            disconnect_socket: AtomicUsize::new(0),
            fail_authentication: AtomicBool::new(false),
            auth_non_success_success_body: AtomicBool::new(false),
            auth_transient_status: AtomicUsize::new(0),
            auth_transient_attempts: AtomicUsize::new(0),
            join_data_transport_failures: AtomicUsize::new(0),
            world_delay_ms: AtomicUsize::new(0),
            hold_document_acks: AtomicBool::new(false),
            document_requests: tokio::sync::Semaphore::new(0),
            release_document_acks: tokio::sync::Semaphore::new(0),
            namespace_after_world: Mutex::new(None),
            auth_delay_ms: AtomicUsize::new(0),
            disconnect_on_mutation: AtomicBool::new(false),
            error_document: Mutex::new(None),
            auth_retry_after_seconds: AtomicUsize::new(0),
            join_status: AtomicUsize::new(200),
            auth_hang_attempts: AtomicUsize::new(0),
            auth_hang_ms: AtomicUsize::new(5000),
        });
        Self::serve(state).await
    }

    /// Start a world serving the given fixture.
    pub async fn start_with_world(fixture: WorldFixture) -> Self {
        let binding = match fixture.users.first() {
            Some(user) => json!({
                "sessionId": fixture.session,
                "userId": user.get("_id").and_then(Value::as_str).unwrap_or("user-1"),
            }),
            None => json!({"sessionId": fixture.session}),
        };
        let state = Arc::new(FakeState {
            requests: Mutex::new(Vec::new()),
            generation: fixture.generation,
            session: fixture.session.clone(),
            binding,
            join_data: Some(json!({"users": fixture.users.clone()})),
            world: World::Fixture(fixture),
            next_socket: AtomicUsize::new(1),
            disconnect_socket: AtomicUsize::new(0),
            fail_authentication: AtomicBool::new(false),
            auth_non_success_success_body: AtomicBool::new(false),
            auth_transient_status: AtomicUsize::new(0),
            auth_transient_attempts: AtomicUsize::new(0),
            join_data_transport_failures: AtomicUsize::new(0),
            world_delay_ms: AtomicUsize::new(0),
            hold_document_acks: AtomicBool::new(false),
            document_requests: tokio::sync::Semaphore::new(0),
            release_document_acks: tokio::sync::Semaphore::new(0),
            namespace_after_world: Mutex::new(None),
            auth_delay_ms: AtomicUsize::new(0),
            disconnect_on_mutation: AtomicBool::new(false),
            error_document: Mutex::new(None),
            auth_retry_after_seconds: AtomicUsize::new(0),
            join_status: AtomicUsize::new(200),
            auth_hang_attempts: AtomicUsize::new(0),
            auth_hang_ms: AtomicUsize::new(5000),
        });
        Self::serve(state).await
    }

    /// Bind the world to a loopback port and serve it.
    async fn serve(state: Arc<FakeState>) -> Self {
        let app = Router::new()
            .route("/api/status", get(status))
            .route("/join", get(join).post(authenticate))
            .route("/socket.io/", get(socket))
            .route("/upload", post(upload_failure))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            base_url: format!("http://{address}"),
            state,
            server,
        }
    }
}

/// The world event's acknowledgement frame.
fn world_ack(state: &FakeState) -> String {
    match &state.world {
        World::Canned { ack } => ack.clone(),
        World::Fixture(fixture) => format!(
            "430[{}]",
            json!({
                "world": fixture.world,
                "packs": fixture.packs,
                "release": {"generation": fixture.generation, "build": 370},
            })
        ),
    }
}

/// The documents one document request answers with.
fn document_result(state: &FakeState, kind: &str, query: &Value) -> Vec<Value> {
    match &state.world {
        World::Canned { .. } => vec![json!({"_id":"doc","name":kind})],
        World::Fixture(fixture) => fixture.documents(kind, query),
    }
}

/// Whether the authentication payload names a user the world knows.
///
/// The canned world checks the fixed test identity; the fixture world checks
/// the identity against its own users. Neither stores a password: the
/// harness credential is a throwaway value, and the world's job is to
/// exercise the protocol, not to authenticate a secret.
fn identity_matches(state: &FakeState, body: &Value) -> bool {
    let userid = body.get("userid").and_then(Value::as_str);
    let user_id = body.get("userId").and_then(Value::as_str);
    let username = body.get("username").and_then(Value::as_str);
    match &state.world {
        World::Canned { .. } => {
            userid == Some("user-1")
                && user_id == Some("user-1")
                && username == Some("Test User")
                && body.get("password").and_then(Value::as_str) == Some("test-password")
        }
        World::Fixture(fixture) => {
            let known = fixture.users.iter().any(|user| {
                let id = user.get("_id").and_then(Value::as_str);
                let name = user.get("name").and_then(Value::as_str);
                (userid.is_none() || id == userid)
                    && (user_id.is_none() || id == user_id)
                    && (username.is_none() || name == username)
            });
            known && body.get("password").is_some()
        }
    }
}

async fn upload_failure(
    State(state): State<Arc<FakeState>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    state
        .requests
        .lock()
        .unwrap()
        .push(format!("POST /upload bytes={}", body.len()));
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":"upload failed"})),
    )
}

async fn status(State(state): State<Arc<FakeState>>) -> Json<Value> {
    state
        .requests
        .lock()
        .unwrap()
        .push("GET /api/status".into());
    Json(json!({"release":{"generation":state.generation}}))
}

async fn join(State(state): State<Arc<FakeState>>) -> axum::response::Response {
    state.requests.lock().unwrap().push("GET /join".into());
    let status = axum::http::StatusCode::from_u16(state.join_status.load(Ordering::Acquire) as u16)
        .unwrap_or(axum::http::StatusCode::OK);
    if !status.is_success() {
        return (status, Json(json!({"status": "unavailable"}))).into_response();
    }
    let cookie = format!("session={}; Path=/; HttpOnly", state.session);
    (
        [(
            axum::http::header::SET_COOKIE,
            HeaderValue::from_str(&cookie).unwrap(),
        )],
        "",
    )
        .into_response()
}

async fn authenticate(
    State(state): State<Arc<FakeState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    let remaining_transient = state.auth_transient_attempts.load(Ordering::Acquire);
    if remaining_transient > 0
        && state
            .auth_transient_attempts
            .compare_exchange(
                remaining_transient,
                remaining_transient - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    {
        let status = axum::http::StatusCode::from_u16(
            state.auth_transient_status.load(Ordering::Acquire) as u16,
        )
        .unwrap();
        state
            .requests
            .lock()
            .unwrap()
            .push(format!("POST /join status={}", status.as_u16()));
        let mut response =
            (status, Json(json!({"status":"temporarily unavailable"}))).into_response();
        let seconds = state.auth_retry_after_seconds.load(Ordering::Acquire);
        if seconds > 0 {
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                response
                    .headers_mut()
                    .insert(axum::http::header::RETRY_AFTER, value);
            }
        }
        return response;
    }
    let remaining_hangs = state.auth_hang_attempts.load(Ordering::Acquire);
    if remaining_hangs > 0
        && state
            .auth_hang_attempts
            .compare_exchange(
                remaining_hangs,
                remaining_hangs - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    {
        // Answer long after any attempt timeout, so the client gives up on
        // this attempt.
        tokio::time::sleep(std::time::Duration::from_millis(
            state.auth_hang_ms.load(Ordering::Acquire) as u64,
        ))
        .await;
    }
    let delay = state.auth_delay_ms.load(Ordering::Relaxed);
    if delay > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(delay as u64)).await;
    }
    let origin = headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let cookie = headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let fields_match = identity_matches(&state, &body);
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let expected_origin = format!("http://{host}");
    let expected_cookie = format!("session={}", state.session);
    state.requests.lock().unwrap().push(format!(
        "POST /join origin_ok={} cookie_ok={} auth_fields_ok={}",
        origin == expected_origin,
        cookie == expected_cookie,
        fields_match,
    ));
    if state.auth_non_success_success_body.load(Ordering::Acquire) {
        (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(json!({"status":"success"})),
        )
            .into_response()
    } else if !state.fail_authentication.load(Ordering::Acquire)
        && origin == expected_origin
        && cookie == expected_cookie
        && fields_match
    {
        (
            axum::http::StatusCode::OK,
            Json(json!({"status":"success"})),
        )
            .into_response()
    } else {
        (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(json!({"status":"failure"})),
        )
            .into_response()
    }
}

async fn socket(
    State(state): State<Arc<FakeState>>,
    headers: HeaderMap,
    uri: Uri,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    let cookie = headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    state.requests.lock().unwrap().push(format!(
        "WS cookie_present={} uri={uri}",
        cookie == format!("session={}", state.session)
    ));
    let socket_id = state.next_socket.fetch_add(1, Ordering::Relaxed);
    upgrade.on_upgrade(move |socket| socket_session(socket, state, socket_id))
}

async fn socket_session(mut socket: WebSocket, state: Arc<FakeState>, socket_id: usize) {
    let _ = socket.send(AxumMessage::Text("0{}".into())).await;
    let (sink, mut stream) = socket.split();
    let sink = Arc::new(tokio::sync::Mutex::new(sink));
    while let Some(Ok(AxumMessage::Text(text))) = stream.next().await {
        if text == "40" {
            let _ = sink
                .lock()
                .await
                .send(AxumMessage::Text(
                    format!("42[\"session\",{}]", state.binding).into(),
                ))
                .await;
        } else if text == "420[\"world\"]" {
            state
                .requests
                .lock()
                .unwrap()
                .push(format!("WS {text} socket={socket_id}"));
            if state.disconnect_socket.load(Ordering::Acquire) == socket_id {
                state
                    .requests
                    .lock()
                    .unwrap()
                    .push(format!("WS-CLOSE socket={socket_id}"));
                let _ = sink.lock().await.send(AxumMessage::Close(None)).await;
                return;
            }
            let delay = state.world_delay_ms.load(Ordering::Relaxed);
            let sink = sink.clone();
            let state = state.clone();
            tokio::spawn(async move {
                if delay > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay as u64)).await;
                }
                state
                    .requests
                    .lock()
                    .unwrap()
                    .push(format!("WS-OUT world socket={socket_id}"));
                let _ = sink
                    .lock()
                    .await
                    .send(AxumMessage::Text(world_ack(&state).into()))
                    .await;
                let namespace_frame = state.namespace_after_world.lock().unwrap().take();
                if let Some(frame) = namespace_frame {
                    state
                        .requests
                        .lock()
                        .unwrap()
                        .push(format!("WS-OUT namespace={frame} socket={socket_id}"));
                    let _ = sink
                        .lock()
                        .await
                        .send(AxumMessage::Text(frame.into()))
                        .await;
                }
            });
        } else if text.contains("[\"getJoinData\"]") {
            state.requests.lock().unwrap().push(format!("WS {text}"));
            let remaining_failures = state.join_data_transport_failures.load(Ordering::Acquire);
            if remaining_failures > 0
                && state
                    .join_data_transport_failures
                    .compare_exchange(
                        remaining_failures,
                        remaining_failures - 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                let _ = sink.lock().await.send(AxumMessage::Close(None)).await;
                return;
            }
            let Some(join_data) = &state.join_data else {
                return;
            };
            let ack = text
                .strip_prefix("42")
                .and_then(|s| s.split('[').next())
                .unwrap_or("1");
            let reply = format!("43{ack}[{join_data}]");
            let _ = sink
                .lock()
                .await
                .send(AxumMessage::Text(reply.into()))
                .await;
        } else if text.contains("[\"modifyDocument\"") {
            state
                .requests
                .lock()
                .unwrap()
                .push(format!("WS {text} socket={socket_id}"));
            let (ack, payload) = text
                .strip_prefix("42")
                .and_then(|tail| {
                    let end = tail.find('[')?;
                    Some((&tail[..end], &tail[end..]))
                })
                .unwrap();
            let payload: Value = serde_json::from_str(payload).unwrap();
            let document = payload[1]["type"].as_str().unwrap().to_owned();
            let action = payload[1]["action"].as_str().unwrap_or("get").to_owned();
            let query = payload[1]["operation"]["query"].clone();
            let refused = state
                .error_document
                .lock()
                .unwrap()
                .as_deref()
                .is_some_and(|kind| kind == document);
            if refused {
                let response = format!(
                    "43{ack}[{}]",
                    json!({"type":document,"action":action,"error":{"message":"denied"}})
                );
                let _ = sink
                    .lock()
                    .await
                    .send(AxumMessage::Text(response.into()))
                    .await;
                continue;
            }
            if action != "get" && state.disconnect_on_mutation.load(Ordering::Acquire) {
                state
                    .requests
                    .lock()
                    .unwrap()
                    .push(format!("WS-CLOSE mutation socket={socket_id}"));
                let _ = sink.lock().await.send(AxumMessage::Close(None)).await;
                return;
            }
            let result = match action.as_str() {
                "create" => payload[1]["operation"]["data"]
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .enumerate()
                            .map(|(index, item)| {
                                let mut item = item.clone();
                                item["_id"] = json!(format!("created-{index}"));
                                item
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                "update" => payload[1]["operation"]["updates"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default(),
                "delete" => payload[1]["operation"]["ids"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default(),
                _ => document_result(&state, &document, &query),
            };
            let response = format!(
                "43{ack}[{}]",
                json!({"type":document,"action":action,"result":result})
            );
            let hold_ack = state.hold_document_acks.load(Ordering::Acquire);
            if hold_ack {
                state.document_requests.add_permits(1);
            }
            let sink = sink.clone();
            let state = state.clone();
            tokio::spawn(async move {
                if hold_ack {
                    let permit = state.release_document_acks.acquire().await.unwrap();
                    permit.forget();
                } else {
                    let delay = if document == "Actor" { 300 } else { 10 };
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
                state
                    .requests
                    .lock()
                    .unwrap()
                    .push(format!("WS-OUT {document} socket={socket_id}"));
                let _ = sink
                    .lock()
                    .await
                    .send(AxumMessage::Text(response.into()))
                    .await;
            });
        } else if text.contains("[\"manageCompendium\"") {
            state
                .requests
                .lock()
                .unwrap()
                .push(format!("WS {text} socket={socket_id}"));
            let (ack, payload) = text
                .strip_prefix("42")
                .and_then(|tail| {
                    let end = tail.find('[')?;
                    Some((&tail[..end], &tail[end..]))
                })
                .unwrap();
            let payload: Value = serde_json::from_str(payload).unwrap();
            let action = payload[1]["action"].as_str().unwrap_or("create").to_owned();
            let result = if action == "create" {
                json!({"id":"world.created-pack"})
            } else {
                json!("world.deleted-pack")
            };
            let response = format!(
                "43{ack}[{}]",
                json!({"request":{"action":action},"result":result})
            );
            let _ = sink
                .lock()
                .await
                .send(AxumMessage::Text(response.into()))
                .await;
        } else if text.contains("[\"manageFiles\"") {
            state
                .requests
                .lock()
                .unwrap()
                .push(format!("WS {text} socket={socket_id}"));
            let ack = text
                .strip_prefix("42")
                .and_then(|tail| tail.split('[').next())
                .unwrap_or("1");
            let response = format!(
                "43{ack}[{}]",
                json!({"dirs":[{"name":"worlds"}],"files":[{"name":"portrait.png"}]})
            );
            let _ = sink
                .lock()
                .await
                .send(AxumMessage::Text(response.into()))
                .await;
        } else {
            state.requests.lock().unwrap().push(format!("WS {text}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FoundryClient, FoundryCredential};

    fn credential(base_url: &str) -> FoundryCredential {
        FoundryCredential {
            id: Some("interop-id".into()),
            hostname: base_url.into(),
            userid: Some("user-1".into()),
            username: Some("Interop User".into()),
            password: "interop-password".into(),
        }
    }

    #[tokio::test]
    async fn fixture_world_serves_journal_entries_over_the_real_connector() {
        let world = FakeWorld::start_with_world(WorldFixture::journal_world()).await;
        let client = FoundryClient::for_loopback(world.base_url()).unwrap();

        let entries = client
            .list_documents(&credential(world.base_url()), "journal")
            .await
            .unwrap();

        assert_eq!(entries.as_array().unwrap().len(), 2);
        assert_eq!(entries[0]["_id"], "journal-1");
        assert_eq!(entries[0]["name"], "Interop Session One");
        assert!(
            entries[0].get("type").is_none(),
            "the world's bookkeeping must not leak into the document"
        );
    }

    #[tokio::test]
    async fn fixture_world_answers_a_server_side_journal_query() {
        let world = FakeWorld::start_with_world(WorldFixture::journal_world()).await;
        let client = FoundryClient::for_loopback(world.base_url()).unwrap();

        let entries = client
            .query_documents(
                &credential(world.base_url()),
                "journal",
                json!({"name": "Interop Session Two"}),
            )
            .await
            .unwrap();

        assert_eq!(entries.as_array().unwrap().len(), 1);
        assert_eq!(entries[0]["_id"], "journal-2");
    }

    #[tokio::test]
    async fn fixture_world_serves_world_metadata_and_compendium_packs() {
        let world = FakeWorld::start_with_world(WorldFixture::journal_world()).await;
        let client = FoundryClient::for_loopback(world.base_url()).unwrap();

        let info = client.world(&credential(world.base_url())).await.unwrap();
        assert_eq!(info.title.as_deref(), Some("Interop World"));
        assert_eq!(info.system.as_deref(), Some("dnd5e"));
        assert_eq!(info.release_generation, Some(json!(14)));

        let packs = client
            .get_compendiums(&credential(world.base_url()), None)
            .await
            .unwrap();
        assert_eq!(packs[0]["id"], "world.items");
    }

    #[tokio::test]
    async fn the_world_records_the_real_http_and_websocket_traffic() {
        let world = FakeWorld::start_with_world(WorldFixture::journal_world()).await;
        let client = FoundryClient::for_loopback(world.base_url()).unwrap();
        client
            .list_documents(&credential(world.base_url()), "journal")
            .await
            .unwrap();

        let log = world.request_log();
        assert!(
            log.iter().any(|entry| entry == "GET /api/status"),
            "{log:?}"
        );
        assert!(log.iter().any(|entry| entry == "GET /join"), "{log:?}");
        assert!(
            log.iter().any(
                |entry| entry == "POST /join origin_ok=true cookie_ok=true auth_fields_ok=true"
            ),
            "{log:?}"
        );
        assert!(
            log.iter()
                .any(|entry| entry.starts_with("WS cookie_present=true")
                    && entry.contains("/socket.io/?EIO=4&transport=websocket")),
            "{log:?}"
        );
        assert!(
            log.iter()
                .any(|entry| entry.contains("modifyDocument") && entry.contains("JournalEntry")),
            "{log:?}"
        );
    }

    #[tokio::test]
    async fn the_world_binds_the_loopback_address_only() {
        let world = FakeWorld::start_with_world(WorldFixture::journal_world()).await;
        assert!(
            world.base_url().starts_with("http://127.0.0.1:"),
            "{}",
            world.base_url()
        );
    }

    #[test]
    fn the_loopback_client_refuses_any_other_target() {
        for base_url in [
            "http://example.org",
            "http://localhost:3000",
            "https://127.0.0.1",
            "http://[::1]:3000",
            "http://127.0.0.1.example.org",
        ] {
            assert!(
                FoundryClient::for_loopback(base_url).is_err(),
                "{base_url} must be refused"
            );
        }
        assert!(FoundryClient::for_loopback("http://127.0.0.1:8080").is_ok());
    }

    #[test]
    fn fixture_documents_match_only_the_queried_scalars() {
        let fixture = WorldFixture::journal_world();
        assert_eq!(fixture.documents("JournalEntry", &json!({})).len(), 2);
        assert_eq!(
            fixture
                .documents("JournalEntry", &json!({"_id": "journal-2"}))
                .len(),
            1
        );
        assert!(fixture
            .documents("JournalEntry", &json!({"_id": "missing"}))
            .is_empty());
        assert!(fixture.documents("Unknown", &json!({})).is_empty());
    }
}
