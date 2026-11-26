use super::interop::FakeWorld as FakeFoundry;
use super::{FoundryClient, FoundryCredential};
use serde_json::json;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn mismatched_session_or_user_binding_blocks_world_and_document_requests() {
    for binding in [
        json!({"sessionId":"different-session","userId":"user-1"}),
        json!({"sessionId":"s-test","userId":"different-user"}),
    ] {
        let fake = FakeFoundry::start_canned_with_binding(
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

        let requests = fake.state.requests.lock().unwrap();
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
        let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
        let client = FoundryClient::for_test(&fake.base_url).unwrap();
        let credential = FoundryCredential {
            hostname: fake.base_url.clone(),
            ..credential
        };
        client.world(&credential).await.unwrap();
        assert!(fake
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.contains("getJoinData")));
    }
}

#[tokio::test]
async fn upload_http_failure_reports_that_upload_may_have_been_applied() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let error = client
        .upload_file(&credential, "worlds/test", "image.png", b"image")
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("may have been applied"), "{error}");
    assert!(fake
        .state
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
        let fake =
            FakeFoundry::start_canned_with_users("430[{\"world\":{}}]", 14, "s-test", users).await;
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
            .state
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
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .fail_authentication
        .store(true, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let error = client.world(&credential).await.unwrap_err();
    // The failure is typed, so the server classifies it as an upstream
    // authentication refusal instead of a plain transport fault.
    assert!(
        error
            .downcast_ref::<crate::UpstreamAuthentication>()
            .is_some(),
        "the refusal lost its type: {error}"
    );
    let error = error.to_string();
    assert!(error.contains("Foundry authentication failed"), "{error}");
    let requests = fake.state.requests.lock().unwrap();
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
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .join_data_transport_failures
        .store(1, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = FoundryCredential {
        userid: None,
        ..test_credential(&fake.base_url)
    };

    client.world(&credential).await.unwrap();

    let requests = fake.state.requests.lock().unwrap();
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
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .auth_non_success_success_body
        .store(true, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();

    let error = client
        .world(&test_credential(&fake.base_url))
        .await
        .unwrap_err();
    // A non-success status is an upstream authentication refusal, and it
    // keeps its type through the connector's error context.
    assert!(
        error
            .downcast_ref::<crate::UpstreamAuthentication>()
            .is_some(),
        "the refusal lost its type: {error}"
    );
    let error = error.to_string();
    assert!(error.contains("Foundry authentication failed"), "{error}");
    let requests = fake.state.requests.lock().unwrap();
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
        let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
        fake.state
            .auth_transient_status
            .store(status, Ordering::Release);
        fake.state
            .auth_transient_attempts
            .store(1, Ordering::Release);
        let client = FoundryClient::for_test(&fake.base_url).unwrap();

        client
            .world(&test_credential(&fake.base_url))
            .await
            .unwrap_or_else(|error| panic!("status {status} should retry: {error}"));

        let requests = fake.state.requests.lock().unwrap();
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
    let fake = FakeFoundry::start_canned_with_join_data(
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
    let requests = fake.state.requests.lock().unwrap();
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
    let fake = FakeFoundry::start_canned(
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
    let requests = fake.state.requests.lock().unwrap();
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
    let fake =
        FakeFoundry::start_canned("430[{\"world\":{\"title\":\"fresh\"}}]", 14, "s-test").await;
    fake.state.disconnect_socket.store(2, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let first_error = client.world(&credential).await.unwrap_err().to_string();
    assert!(first_error.contains("disconnected"), "{first_error}");
    let world = client.world(&credential).await.unwrap();
    assert_eq!(world.title.as_deref(), Some("fresh"));

    let requests = fake.state.requests.lock().unwrap();
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
        FakeFoundry::start_canned("430[{\"world\":{\"title\":\"Test World\"}}]", 14, "s-test")
            .await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    client.world(&credential).await.unwrap();
    client.world(&credential).await.unwrap();

    let requests = fake.state.requests.lock().unwrap();
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
        let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
        *fake.state.namespace_after_world.lock().unwrap() = Some(frame.to_owned());
        let client = FoundryClient::for_test(&fake.base_url).unwrap();
        let credential = test_credential(&fake.base_url);

        client.world(&credential).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        if let Err(error) = client.world(&credential).await {
            assert!(error.to_string().contains("disconnected"), "{error}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        client.world(&credential).await.unwrap();

        let requests = fake.state.requests.lock().unwrap();
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
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let (actors, items) = tokio::join!(
        client.list_documents(&credential, "actors"),
        client.list_documents(&credential, "items"),
    );
    assert_eq!(actors.unwrap()[0]["name"], "Actor");
    assert_eq!(items.unwrap()[0]["name"], "Item");

    let requests = fake.state.requests.lock().unwrap();
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
    let fake =
        FakeFoundry::start_canned("430[{\"world\":{\"title\":\"Shared\"}}]", 14, "s-test").await;
    fake.state.world_delay_ms.store(160, Ordering::Relaxed);
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

    let requests = fake.state.requests.lock().unwrap();
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
    let fake =
        FakeFoundry::start_canned("430[{\"world\":{\"title\":\"Shared\"}}]", 14, "s-test").await;
    fake.state.world_delay_ms.store(100, Ordering::Relaxed);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let (first, second) = tokio::join!(client.world(&credential), client.world(&credential),);
    assert_eq!(first.unwrap().title.as_deref(), Some("Shared"));
    assert_eq!(second.unwrap().title.as_deref(), Some("Shared"));

    let requests = fake.state.requests.lock().unwrap();
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
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state.hold_document_acks.store(true, Ordering::Release);
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
            fake.state.document_requests.acquire(),
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

    fake.state.release_document_acks.add_permits(128);
    let results = futures_util::future::join_all(reads).await;
    assert!(results
        .iter()
        .all(|result| result.as_ref().unwrap().is_ok()));
    fake.state
        .hold_document_acks
        .store(false, Ordering::Release);

    client.list_documents(&credential, "items").await.unwrap();

    let requests = fake.state.requests.lock().unwrap();
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
    let fake =
        FakeFoundry::start_canned("430[{\"world\":{\"title\":\"fresh\"}}]", 14, "s-test").await;
    fake.state.world_delay_ms.store(150, Ordering::Relaxed);
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
                .state
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
    let requests = fake.state.requests.lock().unwrap();
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
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
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
                .state
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
    let requests = fake.state.requests.lock().unwrap();
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

/// A change whose request is cancelled after its frame reached the instance
/// is never replayed, and the cancelled connection is not reused.
///
/// The frame was written, so the change may have been applied: the point of
/// the guarantee is that the gateway never sends it a second time, and that
/// the next request starts from a connection that cannot still be carrying
/// the cancelled one.
#[tokio::test]
async fn a_cancelled_change_is_never_replayed_and_discards_the_socket() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state.hold_document_acks.store(true, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let change = tokio::spawn({
        let client = client.clone();
        let credential = credential.clone();
        async move {
            client
                .create_document(
                    &credential,
                    "Actor",
                    vec![json!({"name": "Hero"})],
                    None,
                    None,
                )
                .await
        }
    });
    // The change reached the instance: its frame was written and the
    // acknowledgement is held.
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fake.state.document_requests.acquire(),
    )
    .await
    .expect("the change should reach fake Foundry")
    .unwrap()
    .forget();

    // Cancel the request while the instance still owes an answer.
    change.abort();
    let _ = change.await;
    // The instance answers the cancelled request on the socket that carried
    // it, after the gateway has already moved on.
    fake.state.release_document_acks.add_permits(128);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // The next request runs on a connection of its own.
    client.list_documents(&credential, "items").await.unwrap();

    let requests = fake.state.requests.lock().unwrap();
    let changes: Vec<_> = requests
        .iter()
        .filter(|request| request.contains("modifyDocument"))
        .filter(|request| request.contains("\"action\":\"create\""))
        .collect();
    assert_eq!(
        changes.len(),
        1,
        "the change must not be replayed: {requests:?}"
    );
    let sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|request| request.contains("modifyDocument"))
        .filter_map(|request| request.split("socket=").nth(1))
        .collect();
    assert_eq!(
        sockets.len(),
        2,
        "the cancelled socket must not be reused: {requests:?}"
    );
}

/// A change whose connection is invalidated before the instance answers is
/// reported as an unknown outcome and is never replayed, and the stale
/// socket's teardown does not take the successor down with it.
#[tokio::test]
async fn a_change_on_an_invalidated_connection_is_ambiguous_and_never_replayed() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    // The instance closes the socket instead of answering the change.
    fake.state
        .disconnect_on_mutation
        .store(true, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let error = client
        .create_document(
            &credential,
            "Actor",
            vec![json!({"name": "Hero"})],
            None,
            None,
        )
        .await
        .unwrap_err();
    // The outcome is unknown, and the gateway says so with its own type.
    assert!(
        error.downcast_ref::<crate::AmbiguousMutation>().is_some(),
        "{error}"
    );
    assert!(
        error.to_string().contains("may have been applied"),
        "{error}"
    );

    // The successor connection is unaffected by the stale socket's teardown.
    fake.state
        .disconnect_on_mutation
        .store(false, Ordering::Release);
    let documents = client.list_documents(&credential, "items").await.unwrap();
    assert_eq!(documents[0]["name"], "Item");

    let requests = fake.state.requests.lock().unwrap();
    let changes: Vec<_> = requests
        .iter()
        .filter(|request| request.contains("modifyDocument"))
        .filter(|request| request.contains("\"action\":\"create\""))
        .collect();
    assert_eq!(
        changes.len(),
        1,
        "the change must not be replayed: {requests:?}"
    );
    let sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|request| request.contains("modifyDocument"))
        .filter_map(|request| request.split("socket=").nth(1))
        .collect();
    assert_eq!(
        sockets.len(),
        2,
        "the invalidated socket must not be reused: {requests:?}"
    );
}

#[tokio::test]
async fn distinct_credential_identities_progress_concurrently() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state.world_delay_ms.store(300, Ordering::Relaxed);
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
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let first = test_credential(&fake.base_url);
    let second = FoundryCredential {
        id: Some("another-configured-identity".into()),
        ..first.clone()
    };

    client.list_documents(&first, "actors").await.unwrap();
    client.list_documents(&second, "actors").await.unwrap();

    let requests = fake.state.requests.lock().unwrap();
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
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
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

    let requests = fake.state.requests.lock().unwrap();
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
    let fake = FakeFoundry::start_canned(
        "430[{\"world\":{\"title\":\"Legacy\"}}]",
        13,
        "legacy-session",
    )
    .await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);
    client.world(&credential).await.unwrap();
    let requests = fake.state.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|r| r.starts_with("WS cookie_present=false")
            && r.contains("/socket.io/?EIO=4&transport=websocket&session=legacy-session")));
}

#[tokio::test]
async fn complete_identity_survives_unavailable_join_data_but_not_mismatch() {
    let fake =
        FakeFoundry::start_canned_with_join_data("430[{\"world\":{}}]", 14, "s-test", None).await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    client
        .world(&test_credential(&fake.base_url))
        .await
        .unwrap();

    let fake =
        FakeFoundry::start_canned_with_join_data("430[{\"world\":{}}]", 14, "s-test", None).await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let partial = FoundryCredential {
        userid: None,
        ..test_credential(&fake.base_url)
    };
    let error = client.world(&partial).await.unwrap_err().to_string();
    assert!(error.contains("unavailable"), "{error}");

    let fake = FakeFoundry::start_canned_with_users(
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
    let fake = FakeFoundry::start_canned_with_join_data(
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

/// How many websockets the world has seen. Every connection establishment
/// opens one, so a second operation that does not raise this count reused
/// the identity's authenticated connection.
fn socket_count(fake: &FakeFoundry) -> usize {
    fake.state
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.starts_with("WS cookie_present="))
        .count()
}

#[tokio::test]
async fn mutations_share_the_identity_connection_instead_of_opening_a_socket_per_change() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    client.list_documents(&credential, "actors").await.unwrap();
    let after_read = socket_count(&fake);

    let created = client
        .create_document(
            &credential,
            "Actor",
            vec![json!({"name": "Interop Hero"})],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(created[0]["_id"], "created-0");

    let modified = client
        .modify_document(
            &credential,
            "Actor",
            "actor-1",
            vec![json!({"name": "Renamed"})],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(modified[0]["_id"], "actor-1");

    let deleted = client
        .delete_document(&credential, "Actor", vec!["actor-1".to_owned()], None, None)
        .await
        .unwrap();
    assert_eq!(deleted[0], "actor-1");

    assert_eq!(
        socket_count(&fake),
        after_read,
        "a change opened a socket instead of reusing the connection: {:?}",
        fake.request_log()
    );
}

#[tokio::test]
async fn compendium_management_shares_the_identity_connection() {
    // The world advertises the pack the delete names, because the connector
    // resolves the name against the world's packs.
    let fake = FakeFoundry::start_canned(
        "430[{\"world\":{},\"packs\":[{\"id\":\"world.npcs\",\"name\":\"npcs\",\"label\":\"NPCs\",\"type\":\"Actor\",\"packageType\":\"world\"}]}]",
        14,
        "s-test",
    )
    .await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    client.list_documents(&credential, "actors").await.unwrap();
    let after_read = socket_count(&fake);

    let created = client
        .create_compendium(&credential, "NPCs", "Actor")
        .await
        .unwrap();
    assert_eq!(created["result"]["id"], "world.created-pack");
    // The pack id a read reports is accepted; Foundry expects the bare name.
    let removed = client
        .delete_compendium(&credential, "world.npcs")
        .await
        .unwrap();
    assert_eq!(removed["result"], "world.deleted-pack");
    assert!(
        fake.request_log().iter().any(|entry| {
            entry.contains("manageCompendium") && entry.contains("\"data\":\"npcs\"")
        }),
        "the delete did not send the bare pack name: {:?}",
        fake.request_log()
    );

    // A pack the world does not advertise never reaches the instance.
    let error = client
        .delete_compendium(&credential, "world.absent")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("not found"), "{error}");
    assert!(
        !fake
            .request_log()
            .iter()
            .any(|entry| entry.contains("\"data\":\"absent\"")),
        "a refused delete reached the instance: {:?}",
        fake.request_log()
    );

    assert_eq!(
        socket_count(&fake),
        after_read,
        "a compendium change opened a socket: {:?}",
        fake.request_log()
    );
}

#[tokio::test]
async fn file_browsing_shares_the_identity_connection() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    client.list_documents(&credential, "actors").await.unwrap();
    let after_read = socket_count(&fake);

    let browsed = client
        .browse_files(&credential, "worlds/test", None, None)
        .await
        .unwrap();
    assert_eq!(browsed["dirs"][0]["name"], "worlds");

    assert_eq!(
        socket_count(&fake),
        after_read,
        "a browse opened a socket: {:?}",
        fake.request_log()
    );
}

#[tokio::test]
async fn a_read_and_a_change_in_flight_share_one_socket_and_answer_separately() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let (read, change) = tokio::join!(
        client.list_documents(&credential, "actors"),
        client.create_document(
            &credential,
            "Actor",
            vec![json!({"name": "Hero"})],
            None,
            None
        ),
    );
    assert_eq!(read.unwrap()[0]["name"], "Actor");
    assert_eq!(change.unwrap()[0]["_id"], "created-0");

    let requests = fake.state.requests.lock().unwrap();
    let sockets: std::collections::HashSet<_> = requests
        .iter()
        .filter(|request| request.contains("modifyDocument"))
        .filter_map(|request| request.split("socket=").nth(1))
        .collect();
    assert_eq!(
        sockets.len(),
        1,
        "a read and a change used separate sockets: {requests:?}"
    );
}

#[tokio::test]
async fn a_disconnect_during_a_change_reports_that_it_may_have_been_applied() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .disconnect_on_mutation
        .store(true, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let error = client
        .create_document(
            &credential,
            "Actor",
            vec![json!({"name": "Hero"})],
            None,
            None,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("may have been applied"), "{error}");
}

#[tokio::test]
async fn a_slow_connection_establishment_does_not_consume_the_world_request_budget() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state.auth_delay_ms.store(400, Ordering::Relaxed);
    let policy = crate::connection::ReconnectPolicy {
        attempts: 1,
        initial_delay: std::time::Duration::ZERO,
        max_delay: std::time::Duration::ZERO,
        max_jitter: std::time::Duration::ZERO,
        attempt_timeout: std::time::Duration::from_secs(30),
        total_timeout: std::time::Duration::from_secs(30),
        world_gate_timeout: std::time::Duration::from_millis(200),
    };
    let manager = std::sync::Arc::new(crate::connection::ConnectionManager::with_policy(
        4,
        std::time::Duration::from_secs(60),
        policy,
    ));
    let client = FoundryClient::for_test_with_connections(&fake.base_url, manager).unwrap();
    let credential = test_credential(&fake.base_url);

    let (first, second) = tokio::join!(client.world(&credential), client.world(&credential));

    assert!(
        first.is_ok(),
        "the first world request failed: {:?}",
        first.err()
    );
    assert!(
        second.is_ok(),
        "the second world request failed: {:?}",
        second.err()
    );
}

#[tokio::test]
async fn an_upstream_refusal_leaves_the_shared_connection_usable() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    *fake.state.error_document.lock().unwrap() = Some("Actor".to_owned());
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let error = client
        .list_documents(&credential, "actors")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("denied"), "{error}");
    assert!(
        !error.contains("may have been applied"),
        "a refused read must not claim to be applied: {error}"
    );

    let after = socket_count(&fake);
    let items = client.list_documents(&credential, "items").await.unwrap();
    assert_eq!(items[0]["name"], "Item");
    assert_eq!(
        socket_count(&fake),
        after,
        "an upstream refusal tore down the shared connection: {:?}",
        fake.request_log()
    );
}

#[tokio::test]
async fn a_retry_after_hint_is_honored_before_the_next_attempt() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .auth_transient_status
        .store(503, Ordering::Release);
    fake.state
        .auth_transient_attempts
        .store(1, Ordering::Release);
    fake.state
        .auth_retry_after_seconds
        .store(1, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let started = std::time::Instant::now();
    let documents = client.list_documents(&credential, "actors").await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(documents[0]["name"], "Actor");
    assert!(
        elapsed >= std::time::Duration::from_millis(950),
        "the upstream hint was not honored: {elapsed:?}"
    );
}

#[tokio::test]
async fn a_retry_after_hint_does_not_hide_the_upstream_failure() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .auth_transient_status
        .store(503, Ordering::Release);
    fake.state
        .auth_transient_attempts
        .store(5, Ordering::Release);
    fake.state
        .auth_retry_after_seconds
        .store(1, Ordering::Release);
    let policy = crate::connection::ReconnectPolicy {
        attempts: 1,
        initial_delay: std::time::Duration::ZERO,
        max_delay: std::time::Duration::ZERO,
        max_jitter: std::time::Duration::ZERO,
        attempt_timeout: std::time::Duration::from_secs(30),
        total_timeout: std::time::Duration::from_secs(30),
        world_gate_timeout: std::time::Duration::from_secs(30),
    };
    let manager = std::sync::Arc::new(crate::connection::ConnectionManager::with_policy(
        4,
        std::time::Duration::from_secs(60),
        policy,
    ));
    let client = FoundryClient::for_test_with_connections(&fake.base_url, manager).unwrap();
    let credential = test_credential(&fake.base_url);

    let error = client
        .list_documents(&credential, "actors")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("transient status 503"),
        "the hint hid the upstream failure: {error}"
    );
    assert!(error.contains("retry after"), "{error}");
}

#[tokio::test]
async fn an_unsuccessful_session_request_is_terminal_and_not_retried() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state.join_status.store(403, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let error = client
        .list_documents(&credential, "actors")
        .await
        .unwrap_err();
    // The refused session request is an upstream authentication refusal, and
    // the type survives the connection's error context.
    assert!(
        error
            .downcast_ref::<crate::UpstreamAuthentication>()
            .is_some(),
        "the refusal lost its type: {error}"
    );
    let error = error.to_string();
    assert!(error.contains("unsuccessful status 403"), "{error}");
    let joins = fake
        .request_log()
        .iter()
        .filter(|request| request.starts_with("GET /join"))
        .count();
    assert_eq!(joins, 1, "a refused session request was retried");
}

#[tokio::test]
async fn a_world_request_that_waited_through_a_broken_socket_retries_on_the_new_one() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state.world_delay_ms.store(400, Ordering::Release);
    fake.state
        .disconnect_on_mutation
        .store(true, Ordering::Release);
    let client = std::sync::Arc::new(FoundryClient::for_test(&fake.base_url).unwrap());
    let credential = test_credential(&fake.base_url);

    // The first world request holds the world slot of the connection while
    // its answer is delayed.
    let first = tokio::spawn({
        let client = client.clone();
        let credential = credential.clone();
        async move { client.world(&credential).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The second world request takes its snapshot of the connection and then
    // waits for the world slot.
    let second = tokio::spawn({
        let client = client.clone();
        let credential = credential.clone();
        async move { client.world(&credential).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // A change that loses the socket breaks the connection the two world
    // requests share.
    let broken = client
        .create_document(
            &credential,
            "Actor",
            vec![json!({"name": "Hero"})],
            None,
            None,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(broken.contains("may have been applied"), "{broken}");

    assert!(
        first.await.unwrap().is_err(),
        "the world request on the broken socket should fail"
    );
    let world = second.await.unwrap().expect("the waiting world request");
    assert_eq!(world.title.as_deref(), None, "{world:?}");
}

#[tokio::test]
async fn a_refusal_while_another_request_is_in_flight_leaves_that_request_alone() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    *fake.state.error_document.lock().unwrap() = Some("Actor".to_owned());
    fake.state.hold_document_acks.store(true, Ordering::Release);
    let client = std::sync::Arc::new(FoundryClient::for_test(&fake.base_url).unwrap());
    let credential = test_credential(&fake.base_url);

    // A read of another document type is in flight, and the world holds its
    // answer.
    let held = tokio::spawn({
        let client = client.clone();
        let credential = credential.clone();
        async move { client.list_documents(&credential, "items").await }
    });
    fake.state
        .document_requests
        .acquire()
        .await
        .unwrap()
        .forget();

    let sockets = socket_count(&fake);
    let error = client
        .list_documents(&credential, "actors")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("denied"), "{error}");
    assert_eq!(
        socket_count(&fake),
        sockets,
        "the refusal tore down the shared connection: {:?}",
        fake.request_log()
    );

    fake.state.release_document_acks.add_permits(128);
    let items = held.await.unwrap().expect("the request in flight");
    assert_eq!(items[0]["name"], "Item");
}

#[tokio::test]
async fn a_transport_failure_invalidates_the_connection_for_the_next_request() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .disconnect_on_mutation
        .store(true, Ordering::Release);
    let client = FoundryClient::for_test(&fake.base_url).unwrap();
    let credential = test_credential(&fake.base_url);

    let before = socket_count(&fake);
    let error = client
        .create_document(
            &credential,
            "Actor",
            vec![json!({"name": "Hero"})],
            None,
            None,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("may have been applied"), "{error}");

    let documents = client.list_documents(&credential, "actors").await.unwrap();
    assert_eq!(documents[0]["name"], "Actor");
    assert!(
        socket_count(&fake) > before,
        "the broken connection was kept: {:?}",
        fake.request_log()
    );
}

#[tokio::test(start_paused = true)]
async fn a_hint_from_an_earlier_attempt_does_not_delay_a_later_attempt() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .auth_transient_status
        .store(503, Ordering::Release);
    fake.state
        .auth_transient_attempts
        .store(1, Ordering::Release);
    fake.state
        .auth_retry_after_seconds
        .store(2, Ordering::Release);
    fake.state.auth_hang_attempts.store(1, Ordering::Release);
    let policy = crate::connection::ReconnectPolicy {
        attempts: 3,
        initial_delay: std::time::Duration::from_millis(50),
        max_delay: std::time::Duration::from_millis(50),
        max_jitter: std::time::Duration::ZERO,
        attempt_timeout: std::time::Duration::from_secs(1),
        total_timeout: std::time::Duration::from_secs(30),
        world_gate_timeout: std::time::Duration::from_secs(30),
    };
    let manager = std::sync::Arc::new(crate::connection::ConnectionManager::with_policy(
        4,
        std::time::Duration::from_secs(60),
        policy,
    ));
    let client = FoundryClient::for_test_with_connections(&fake.base_url, manager).unwrap();
    let credential = test_credential(&fake.base_url);

    // The clock is virtual, so the measured delay is the one the reconnect
    // loop asked for, not the machine's load.
    let started = tokio::time::Instant::now();
    let documents = client.list_documents(&credential, "actors").await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(documents[0]["name"], "Actor");
    assert!(
        elapsed < std::time::Duration::from_secs(4),
        "a hint from an earlier attempt delayed a later one: {elapsed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_retry_after_hint_longer_than_the_cap_waits_only_the_cap() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .auth_transient_status
        .store(503, Ordering::Release);
    fake.state
        .auth_transient_attempts
        .store(1, Ordering::Release);
    fake.state
        .auth_retry_after_seconds
        .store(3600, Ordering::Release);
    let policy = crate::connection::ReconnectPolicy {
        attempts: 2,
        initial_delay: std::time::Duration::from_millis(50),
        max_delay: std::time::Duration::from_millis(50),
        max_jitter: std::time::Duration::ZERO,
        attempt_timeout: std::time::Duration::from_secs(30),
        total_timeout: std::time::Duration::from_secs(300),
        world_gate_timeout: std::time::Duration::from_secs(30),
    };
    let manager = std::sync::Arc::new(crate::connection::ConnectionManager::with_policy(
        4,
        std::time::Duration::from_secs(60),
        policy,
    ));
    let client = FoundryClient::for_test_with_connections(&fake.base_url, manager).unwrap();
    let credential = test_credential(&fake.base_url);

    let started = tokio::time::Instant::now();
    let documents = client.list_documents(&credential, "actors").await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(documents[0]["name"], "Actor");
    assert!(
        elapsed < std::time::Duration::from_secs(120),
        "the upstream hint was not capped: {elapsed:?}"
    );
    assert!(
        elapsed >= std::time::Duration::from_secs(60),
        "the capped hint was not waited for: {elapsed:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn the_reconnect_stops_at_its_aggregate_deadline() {
    let fake = FakeFoundry::start_canned("430[{\"world\":{}}]", 14, "s-test").await;
    fake.state
        .auth_transient_status
        .store(503, Ordering::Release);
    fake.state
        .auth_transient_attempts
        .store(10, Ordering::Release);
    fake.state
        .auth_retry_after_seconds
        .store(60, Ordering::Release);
    let policy = crate::connection::ReconnectPolicy {
        attempts: 5,
        initial_delay: std::time::Duration::from_millis(50),
        max_delay: std::time::Duration::from_millis(50),
        max_jitter: std::time::Duration::ZERO,
        attempt_timeout: std::time::Duration::from_secs(30),
        total_timeout: std::time::Duration::from_secs(5),
        world_gate_timeout: std::time::Duration::from_secs(30),
    };
    let manager = std::sync::Arc::new(crate::connection::ConnectionManager::with_policy(
        4,
        std::time::Duration::from_secs(60),
        policy,
    ));
    let client = FoundryClient::for_test_with_connections(&fake.base_url, manager).unwrap();
    let credential = test_credential(&fake.base_url);

    let started = tokio::time::Instant::now();
    let error = client
        .list_documents(&credential, "actors")
        .await
        .unwrap_err()
        .to_string();
    let elapsed = started.elapsed();
    assert!(
        error.contains("deadline"),
        "the reconnect ignored its deadline: {error}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(30),
        "the reconnect outlasted its deadline: {elapsed:?}"
    );
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
