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

use super::{FoundryClient, FoundryCredential};

struct FakeState {
    requests: Mutex<Vec<String>>,
    ack: String,
    generation: u64,
    session: String,
    binding: Value,
    join_data: Option<Value>,
    next_socket: AtomicUsize,
    disconnect_socket: AtomicUsize,
    fail_authentication: AtomicBool,
    auth_non_success_success_body: AtomicBool,
    auth_transient_status: AtomicUsize,
    auth_transient_attempts: AtomicUsize,
    join_data_transport_failures: AtomicUsize,
    world_delay_ms: AtomicUsize,
    hold_document_acks: AtomicBool,
    document_requests: tokio::sync::Semaphore,
    release_document_acks: tokio::sync::Semaphore,
    namespace_after_world: Mutex<Option<String>>,
}

struct FakeFoundry {
    base_url: String,
    requests: Arc<FakeState>,
}

impl FakeFoundry {
    async fn start(ack: &str, generation: u64, session: &str) -> Self {
        Self::start_with_users(
            ack,
            generation,
            session,
            vec![json!({"_id":"user-1","name":"Test User"})],
        )
        .await
    }

    async fn start_with_users(
        ack: &str,
        generation: u64,
        session: &str,
        users: Vec<Value>,
    ) -> Self {
        Self::start_with_join_data(ack, generation, session, Some(json!({"users": users}))).await
    }

    async fn start_with_join_data(
        ack: &str,
        generation: u64,
        session: &str,
        join_data: Option<Value>,
    ) -> Self {
        Self::start_with_binding(
            ack,
            generation,
            session,
            join_data,
            json!({"sessionId":session,"userId":"user-1"}),
        )
        .await
    }

    async fn start_with_binding(
        ack: &str,
        generation: u64,
        session: &str,
        join_data: Option<Value>,
        binding: Value,
    ) -> Self {
        let state = Arc::new(FakeState {
            requests: Mutex::new(Vec::new()),
            ack: ack.into(),
            generation,
            session: session.into(),
            binding,
            join_data,
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
        });
        let app = Router::new()
            .route("/api/status", get(status))
            .route("/join", get(join).post(authenticate))
            .route("/socket.io/", get(socket))
            .route("/upload", post(upload_failure))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            base_url: format!("http://{address}"),
            requests: state,
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

async fn join(State(state): State<Arc<FakeState>>) -> impl IntoResponse {
    state.requests.lock().unwrap().push("GET /join".into());
    let cookie = format!("session={}; Path=/; HttpOnly", state.session);
    (
        [(
            axum::http::header::SET_COOKIE,
            HeaderValue::from_str(&cookie).unwrap(),
        )],
        "",
    )
}

async fn authenticate(
    State(state): State<Arc<FakeState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
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
        return (status, Json(json!({"status":"temporarily unavailable"})));
    }
    let origin = headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let cookie = headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let fields_match = body.get("userid").and_then(Value::as_str) == Some("user-1")
        && body.get("userId").and_then(Value::as_str) == Some("user-1")
        && body.get("username").and_then(Value::as_str) == Some("Test User")
        && body.get("password").and_then(Value::as_str) == Some("test-password");
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
    } else if !state.fail_authentication.load(Ordering::Acquire)
        && origin == expected_origin
        && cookie == expected_cookie
        && fields_match
    {
        (
            axum::http::StatusCode::OK,
            Json(json!({"status":"success"})),
        )
    } else {
        (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(json!({"status":"failure"})),
        )
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
                    .send(AxumMessage::Text(state.ack.clone().into()))
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
            let response = format!(
                "43{ack}[{}]",
                json!({"type":document,"result":[{"_id":"doc","name":document}]})
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
        } else {
            state.requests.lock().unwrap().push(format!("WS {text}"));
        }
    }
}

#[tokio::test]
async fn mismatched_session_or_user_binding_blocks_world_and_document_requests() {
    for binding in [
        json!({"sessionId":"different-session","userId":"user-1"}),
        json!({"sessionId":"s-test","userId":"different-user"}),
    ] {
        let fake = FakeFoundry::start_with_binding(
            "430[{\"world\":{}}]",
            14,
            "s-test",
            Some(json!({"users":[{"_id":"user-1","name":"Test User"}]})),
            binding,
        )
        .await;
        let client = FoundryClient::for_test(&fake.base_url).unwrap();
        let credential = test_credential(&fake.base_url);

        let error = client.world(&credential).await.unwrap_err().to_string();
        assert!(error.contains("session binding failed"), "{error}");
        let error = client
            .list_documents(&credential, "actors")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("session binding failed"), "{error}");

        let requests = fake.requests.requests.lock().unwrap();
        assert!(!requests
            .iter()
            .any(|request| request.starts_with("WS 420[\"world\"]")));
        assert!(!requests
            .iter()
            .any(|request| request.contains("modifyDocument")));
    }
}

#[tokio::test]
async fn resolves_either_identity_from_join_users_before_authentication() {
    for credential in [
        FoundryCredential {
            userid: None,
            ..test_credential("unused")
        },
        FoundryCredential {
            username: None,
            ..test_credential("unused")
        },
    ] {
        let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
        let client = FoundryClient::for_test(&fake.base_url).unwrap();
        let credential = FoundryCredential {
            hostname: fake.base_url.clone(),
            ..credential
        };
        client.world(&credential).await.unwrap();
        assert!(fake
            .requests
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.contains("getJoinData")));
    }
}

#[tokio::test]
async fn upload_http_failure_reports_that_upload_may_have_been_applied() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let error = client
        .upload_file(&credential, "worlds/test", "image.png", b"image")
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("may have been applied"), "{error}");
    assert!(fake
        .requests
        .requests
        .lock()
        .unwrap()
        .iter()
        .any(|request| request.starts_with("POST /upload bytes=")));
}

#[tokio::test]
async fn rejects_unresolvable_ambiguous_and_inconsistent_join_identities() {
    for (credential, users, expected) in [
        (
            FoundryCredential {
                userid: Some("user-1".into()),
                username: Some(" ".into()),
                ..test_credential("unused")
            },
            vec![json!({"_id":"user-1","name":"Test User"})],
            "must not be blank",
        ),
        (
            FoundryCredential {
                userid: None,
                ..test_credential("unused")
            },
            vec![],
            "No Foundry user",
        ),
        (
            FoundryCredential {
                username: Some("Dup".into()),
                userid: None,
                ..test_credential("unused")
            },
            vec![
                json!({"_id":"a","name":"Dup"}),
                json!({"_id":"b","name":"Dup"}),
            ],
            "Multiple Foundry users",
        ),
        (
            FoundryCredential {
                username: Some("Other".into()),
                ..test_credential("unused")
            },
            vec![json!({"_id":"user-1","name":"Test User"})],
            "inconsistent",
        ),
    ] {
        let fake = FakeFoundry::start_with_users("430[{\"world\":{}}]", 14, "s-test", users).await;
        let client = FoundryClient::for_test(&fake.base_url).unwrap();
        let credential = FoundryCredential {
            hostname: fake.base_url.clone(),
            ..credential
        };
        let error = client.world(&credential).await.unwrap_err().to_string();
        assert!(
            error.contains(expected),
            "expected {expected:?}, got {error}"
        );
        let request_count = fake
            .requests
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| *request == "GET /join")
            .count();
        assert_eq!(request_count, 1, "terminal identity error retried: {error}");
    }
}

#[tokio::test]
async fn terminal_authentication_failure_is_not_retried_by_world_connection() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    fake.requests
        .fail_authentication
        .store(true, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let error = client.world(&credential).await.unwrap_err().to_string();
    assert!(error.contains("Foundry authentication failed"), "{error}");
    let requests = fake.requests.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| *request == "GET /join")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("POST /join "))
            .count(),
        1
    );
}

#[tokio::test]
async fn partial_identity_retries_after_join_data_transport_failure() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    fake.requests
        .join_data_transport_failures
        .store(1, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = FoundryCredential {
        userid: None,
        ..test_credential(&fake.base_url)
    };

    client.world(&credential).await.unwrap();

    let requests = fake.requests.requests.lock().unwrap();
    assert_eq!(requests.iter().filter(|r| *r == "GET /join").count(), 2);
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.starts_with("POST /join "))
            .count(),
        1
    );
}

#[tokio::test]
async fn authentication_non_success_status_with_success_body_is_rejected() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    fake.requests
        .auth_non_success_success_body
        .store(true, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();

    let error = client
        .world(&test_credential(&fake.base_url))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("Foundry authentication failed"), "{error}");
    let requests = fake.requests.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.starts_with("POST /join "))
            .count(),
        1,
        "non-success status must not be accepted or retried: {requests:?}"
    );
}

#[tokio::test]
async fn transient_authentication_statuses_are_retried() {
    for status in [408, 425, 429, 500, 503] {
        let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
        fake.requests
            .auth_transient_status
            .store(status, Ordering::Release);
        fake.requests
            .auth_transient_attempts
            .store(1, Ordering::Release);
        let client = FoundryClient::for_test(&fake.base_url).unwrap();

        client
            .world(&test_credential(&fake.base_url))
            .await
            .unwrap_or_else(|error| panic!("status {status} should retry: {error}"));

        let requests = fake.requests.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.starts_with("POST /join "))
                .count(),
            2,
            "status {status}: {requests:?}"
        );
        assert_eq!(
            requests.iter().filter(|r| *r == "GET /join").count(),
            2,
            "status {status}: {requests:?}"
        );
    }
}

#[tokio::test]
async fn malformed_join_data_is_not_retried() {
    let fake = FakeFoundry::start_with_join_data(
        "430[{\"world\":{}}]",
        14,
        "s-test",
        Some(json!({"notUsers": []})),
    )
    .await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();

    let error = client
        .world(&test_credential(&fake.base_url))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("did not contain users"), "{error}");
    let requests = fake.requests.requests.lock().unwrap();
    assert_eq!(requests.iter().filter(|r| *r == "GET /join").count(), 1);
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.starts_with("POST /join "))
            .count(),
        0
    );
}

#[tokio::test]
async fn foundry_client_world_read_reaches_fake_over_http_and_socket() {
    let fake = FakeFoundry::start(
        "430[{\"world\":{\"title\":\"Test World\",\"system\":\"dnd5e\",\"coreVersion\":\"14.1\"}}]",
        14,
        "s-test",
    )
    .await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = FoundryCredential {
        id: Some("test-id".into()),
        hostname: fake.base_url.clone(),
        userid: Some("user-1".into()),
        username: Some("Test User".into()),
        password: "test-password".into(),
    };
    let world = client.world(&credential).await.unwrap();
    assert_eq!(world.title.as_deref(), Some("Test World"));
    let requests = fake.requests.requests.lock().unwrap();
    assert!(requests.iter().any(|r| r == "GET /api/status"));
    assert!(requests.iter().any(|r| r == "GET /join"));
    assert!(requests
        .iter()
        .any(|r| r == "POST /join origin_ok=true cookie_ok=true auth_fields_ok=true"));
    assert!(requests
        .iter()
        .any(|r| r.starts_with("WS cookie_present=true")
            && r.contains("/socket.io/?EIO=4&transport=websocket")));
    assert!(requests.iter().any(|r| r.starts_with("WS 420[\"world\"]")));
}

#[tokio::test]
async fn disconnect_is_not_replayed_and_next_read_reconnects_with_bound_session() {
    let fake = FakeFoundry::start("430[{\"world\":{\"title\":\"fresh\"}}]", 14, "s-test").await;
    fake.requests.disconnect_socket.store(2, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let first_error = client.world(&credential).await.unwrap_err().to_string();
    assert!(first_error.contains("disconnected"), "{first_error}");
    let world = client.world(&credential).await.unwrap();
    assert_eq!(world.title.as_deref(), Some("fresh"));

    let requests = fake.requests.requests.lock().unwrap();
    let world_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.starts_with("WS 420[\"world\"]"))
        .collect();
    assert_eq!(world_requests.len(), 2, "{requests:?}");
    let first_socket = world_requests[0].split("socket=").nth(1).unwrap();
    let second_socket = world_requests[1].split("socket=").nth(1).unwrap();
    assert_ne!(first_socket, second_socket);
    assert_eq!(
        requests
            .iter()
            .filter(|request| *request == "GET /join")
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("POST /join "))
            .count(),
        2
    );
    assert!(requests
        .iter()
        .all(|request| { !request.starts_with("WS-CLOSE") || request == "WS-CLOSE socket=2" }));
}

#[tokio::test]
async fn sequential_world_requests_reuse_one_authenticated_websocket() {
    let fake =
        FakeFoundry::start("430[{\"world\":{\"title\":\"Test World\"}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    client.world(&credential).await.unwrap();
    client.world(&credential).await.unwrap();

    let requests = fake.requests.requests.lock().unwrap();
    assert_eq!(requests.iter().filter(|r| *r == "GET /join").count(), 1);
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.starts_with("POST /join "))
            .count(),
        1
    );
    let world_sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|r| r.starts_with("WS 420[\"world\"]"))
        .map(|r| r.split("socket=").nth(1).unwrap())
        .collect();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.starts_with("WS 420[\"world\"]"))
            .count(),
        2
    );
    assert_eq!(world_sockets.len(), 1);
}

#[tokio::test]
async fn namespace_disconnect_or_error_discards_the_shared_socket() {
    for frame in ["41", "44[\"session revoked\"]"] {
        let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
        *fake.requests.namespace_after_world.lock().unwrap() = Some(frame.to_owned());
        let client = FoundryClient::for_test(&fake.base_url).unwrap();
        let credential = test_credential(&fake.base_url);

        client.world(&credential).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        if let Err(error) = client.world(&credential).await {
            assert!(error.to_string().contains("disconnected"), "{error}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        client.world(&credential).await.unwrap();

        let requests = fake.requests.requests.lock().unwrap();
        let world_sockets: std::collections::HashSet<_> = requests
            .iter()
            .filter(|request| request.starts_with("WS 420[\"world\"]"))
            .map(|request| request.split("socket=").nth(1).unwrap())
            .collect();
        assert_eq!(
            world_sockets.len(),
            2,
            "{frame} must invalidate the shared socket: {requests:?}"
        );
        assert!(requests
            .iter()
            .any(|request| request.starts_with(&format!("WS-OUT namespace={frame}"))));
    }
}

#[tokio::test]
async fn concurrent_document_reads_share_one_authenticated_socket_and_interleave_acks() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let (actors, items) = tokio::join!(
        client.list_documents(&credential, "actors"),
        client.list_documents(&credential, "items"),
    );
    assert_eq!(actors.unwrap()[0]["name"], "Actor");
    assert_eq!(items.unwrap()[0]["name"], "Item");

    let requests = fake.requests.requests.lock().unwrap();
    let document_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.contains("modifyDocument"))
        .collect();
    assert_eq!(document_requests.len(), 2, "{requests:?}");
    let sockets: std::collections::HashSet<_> = document_requests
        .iter()
        .map(|request| request.split("socket=").nth(1).unwrap())
        .collect();
    assert_eq!(
        sockets.len(),
        1,
        "document GETs used separate sockets: {requests:?}"
    );
    let socket = *sockets.iter().next().unwrap();
    let item_ack = requests
        .iter()
        .position(|request| request == &format!("WS-OUT Item socket={socket}"))
        .unwrap();
    let actor_ack = requests
        .iter()
        .position(|request| request == &format!("WS-OUT Actor socket={socket}"))
        .unwrap();
    assert!(
        item_ack < actor_ack,
        "ACKs were not interleaved: {requests:?}"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| *request == "GET /join")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("POST /join "))
            .count(),
        1
    );
}

#[tokio::test]
async fn concurrent_world_and_document_reads_share_one_authenticated_socket() {
    let fake = FakeFoundry::start("430[{\"world\":{\"title\":\"Shared\"}}]", 14, "s-test").await;
    fake.requests.world_delay_ms.store(160, Ordering::Relaxed);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let (world, actors, items) = tokio::join!(
        client.world(&credential),
        client.list_documents(&credential, "actors"),
        client.list_documents(&credential, "items"),
    );
    assert_eq!(world.unwrap().title.as_deref(), Some("Shared"));
    assert_eq!(actors.unwrap()[0]["name"], "Actor");
    assert_eq!(items.unwrap()[0]["name"], "Item");

    let requests = fake.requests.requests.lock().unwrap();
    let sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|request| {
            request.starts_with("WS 420[\"world\"]") || request.contains("modifyDocument")
        })
        .filter_map(|request| request.split("socket=").nth(1))
        .collect();
    assert_eq!(
        sockets.len(),
        1,
        "all reads should share one authenticated socket: {requests:?}"
    );
    let shared_socket = *sockets.iter().next().unwrap();
    let item_ack = requests
        .iter()
        .position(|r| r == &format!("WS-OUT Item socket={shared_socket}"))
        .unwrap();
    let world_ack = requests
        .iter()
        .position(|r| r == &format!("WS-OUT world socket={shared_socket}"))
        .unwrap();
    let actor_ack = requests
        .iter()
        .position(|r| r == &format!("WS-OUT Actor socket={shared_socket}"))
        .unwrap();
    assert!(
        item_ack < world_ack && world_ack < actor_ack,
        "responses were not interleaved: {requests:?}"
    );
}

#[tokio::test]
async fn overlapping_world_calls_serialize_on_the_shared_socket() {
    let fake = FakeFoundry::start("430[{\"world\":{\"title\":\"Shared\"}}]", 14, "s-test").await;
    fake.requests.world_delay_ms.store(100, Ordering::Relaxed);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let (first, second) = tokio::join!(client.world(&credential), client.world(&credential),);
    assert_eq!(first.unwrap().title.as_deref(), Some("Shared"));
    assert_eq!(second.unwrap().title.as_deref(), Some("Shared"));

    let requests = fake.requests.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.starts_with("WS 420[\"world\"]"))
            .count(),
        2,
        "both world reads should be sent: {requests:?}"
    );
    let sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|r| r.starts_with("WS 420[\"world\"]"))
        .filter_map(|r| r.split("socket=").nth(1))
        .collect();
    assert_eq!(
        sockets.len(),
        1,
        "world reads should reuse the socket: {requests:?}"
    );
}

#[tokio::test]
async fn pending_cap_rejection_does_not_discard_transport_or_existing_reads() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    fake.requests
        .hold_document_acks
        .store(true, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let reads: Vec<_> = (0..128)
        .map(|_| {
            let client = client.clone();
            let credential = credential.clone();
            tokio::spawn(async move { client.list_documents(&credential, "actors").await })
        })
        .collect();
    for _ in 0..128 {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fake.requests.document_requests.acquire(),
        )
        .await
        .expect("all document requests should reach fake Foundry")
        .unwrap()
        .forget();
    }

    let world_error = client.world(&credential).await.unwrap_err().to_string();
    assert!(
        world_error.contains("pending request limit reached"),
        "{world_error}"
    );

    fake.requests.release_document_acks.add_permits(128);
    let results = futures_util::future::join_all(reads).await;
    assert!(results
        .iter()
        .all(|result| result.as_ref().unwrap().is_ok()));
    fake.requests
        .hold_document_acks
        .store(false, Ordering::Release);

    client.list_documents(&credential, "items").await.unwrap();

    let requests = fake.requests.requests.lock().unwrap();
    let sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|r| r.contains("modifyDocument"))
        .filter_map(|r| r.split("socket=").nth(1))
        .collect();
    assert_eq!(
        sockets.len(),
        1,
        "cap rejection must preserve transport: {requests:?}"
    );
    assert!(!requests.iter().any(|r| r.starts_with("WS 420[\"world\"]")));
}

#[tokio::test]
async fn cancelled_world_request_discards_socket_before_late_response() {
    let fake = FakeFoundry::start("430[{\"world\":{\"title\":\"fresh\"}}]", 14, "s-test").await;
    fake.requests.world_delay_ms.store(150, Ordering::Relaxed);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);
    let request = tokio::spawn({
        let client = client.clone();
        let credential = credential.clone();
        async move { client.world(&credential).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if fake
                .requests
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.starts_with("WS 420[\"world\"]"))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    request.abort();
    let _ = request.await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    assert_eq!(
        client.world(&credential).await.unwrap().title.as_deref(),
        Some("fresh")
    );
    let requests = fake.requests.requests.lock().unwrap();
    let world_sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|r| r.starts_with("WS 420[\"world\"]"))
        .map(|r| r.split("socket=").nth(1).unwrap())
        .collect();
    assert_eq!(
        world_sockets.len(),
        2,
        "cancelled socket must not be reused"
    );
}

#[tokio::test]
async fn cancelled_document_get_discards_socket_before_late_ack() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);
    let request = tokio::spawn({
        let client = client.clone();
        let credential = credential.clone();
        async move { client.list_documents(&credential, "actors").await }
    });

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if fake
                .requests
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| request.contains("modifyDocument"))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    request.abort();
    let _ = request.await;
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;

    assert_eq!(
        client.list_documents(&credential, "actors").await.unwrap()[0]["name"],
        "Actor"
    );
    let requests = fake.requests.requests.lock().unwrap();
    let document_sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|request| request.contains("modifyDocument"))
        .map(|request| request.split("socket=").nth(1).unwrap())
        .collect();
    assert_eq!(
        document_sockets.len(),
        2,
        "cancelled GET's socket must not be reused: {requests:?}"
    );
}

#[tokio::test]
async fn distinct_credential_identities_progress_concurrently() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    fake.requests.world_delay_ms.store(300, Ordering::Relaxed);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let first = test_credential(&fake.base_url);
    let second = FoundryCredential {
        userid: None,
        ..first.clone()
    };
    let started = tokio::time::Instant::now();
    let (a, b) = tokio::join!(client.world(&first), client.world(&second));
    a.unwrap();
    b.unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_millis(550),
        "identity requests serialized globally"
    );
}

#[tokio::test]
async fn document_reads_keep_configured_credential_identities_isolated() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let first = test_credential(&fake.base_url);
    let second = FoundryCredential {
        id: Some("another-configured-identity".into()),
        ..first.clone()
    };

    client.list_documents(&first, "actors").await.unwrap();
    client.list_documents(&second, "actors").await.unwrap();

    let requests = fake.requests.requests.lock().unwrap();
    let document_sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|request| request.contains("modifyDocument"))
        .map(|request| request.split("socket=").nth(1).unwrap())
        .collect();
    assert_eq!(
        document_sockets.len(),
        2,
        "different configured identities shared an authenticated socket: {requests:?}"
    );
}

#[tokio::test]
async fn username_only_and_complete_credentials_use_independent_sessions() {
    let fake = FakeFoundry::start("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let complete = test_credential(&fake.base_url);
    let username_only = FoundryCredential {
        userid: None,
        ..complete.clone()
    };
    let second_config = FoundryCredential {
        id: Some("another-configured-identity".into()),
        ..complete.clone()
    };
    client.world(&complete).await.unwrap();
    client.world(&username_only).await.unwrap();
    client.world(&second_config).await.unwrap();

    let requests = fake.requests.requests.lock().unwrap();
    let world_sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|request| request.starts_with("WS 420[\"world\"]"))
        .map(|request| request.split("socket=").nth(1).unwrap())
        .collect();
    assert_eq!(
        world_sockets.len(),
        3,
        "credential forms and configured identities must not share sessions"
    );
}

#[tokio::test]
async fn foundry_v13_binds_session_in_legacy_websocket_query() {
    let fake = FakeFoundry::start(
        "430[{\"world\":{\"title\":\"Legacy\"}}]",
        13,
        "legacy-session",
    )
    .await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);
    client.world(&credential).await.unwrap();
    let requests = fake.requests.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|r| r.starts_with("WS cookie_present=false")
            && r.contains("/socket.io/?EIO=4&transport=websocket&session=legacy-session")));
}

#[tokio::test]
async fn complete_identity_survives_unavailable_join_data_but_not_mismatch() {
    let fake = FakeFoundry::start_with_join_data("430[{\"world\":{}}]", 14, "s-test", None).await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    client
        .world(&test_credential(&fake.base_url))
        .await
        .unwrap();

    let fake = FakeFoundry::start_with_join_data("430[{\"world\":{}}]", 14, "s-test", None).await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let partial = FoundryCredential {
        userid: None,
        ..test_credential(&fake.base_url)
    };
    let error = client.world(&partial).await.unwrap_err().to_string();
    assert!(error.contains("unavailable"), "{error}");

    let fake = FakeFoundry::start_with_users(
        "430[{\"world\":{}}]",
        14,
        "s-test",
        vec![json!({"_id":"user-1","name":"Somebody Else"})],
    )
    .await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let error = client
        .world(&test_credential(&fake.base_url))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("inconsistent"), "{error}");
}

#[tokio::test]
async fn malformed_join_data_fails_closed_even_for_complete_identity() {
    let fake = FakeFoundry::start_with_join_data(
        "430[{\"world\":{}}]",
        14,
        "s-test",
        Some(json!({"notUsers": []})),
    )
    .await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let error = client
        .world(&test_credential(&fake.base_url))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("did not contain users"), "{error}");
}

fn test_credential(hostname: &str) -> FoundryCredential {
    FoundryCredential {
        id: Some("test-id".into()),
        hostname: hostname.into(),
        userid: Some("user-1".into()),
        username: Some("Test User".into()),
        password: "test-password".into(),
    }
}
