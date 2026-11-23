use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
pub enum Request {
    World,
    Document {
        document: String,
        action: String,
        operation: Value,
    },
}

struct State {
    pending: HashMap<u64, Pending>,
    world: Option<WorldPending>,
    world_quarantined: bool,
    events: VecDeque<String>,
    disconnected: bool,
}
struct Pending {
    tx: oneshot::Sender<Result<Value, String>>,
    mutation: bool,
    document: String,
    action: String,
}
struct WorldPending {
    tx: oneshot::Sender<Result<Value, String>>,
    sent: bool,
}
struct Inner {
    state: Mutex<State>,
    next: AtomicU64,
    max_pending: usize,
    max_events: usize,
}

#[derive(Clone)]
pub struct SocketMux(Arc<Inner>);
pub struct PendingRequest {
    id: u64,
    rx: Option<oneshot::Receiver<Result<Value, String>>>,
    inner: Weak<Inner>,
}

impl SocketMux {
    pub fn new(max_pending: usize, max_events: usize) -> Self {
        Self(Arc::new(Inner {
            state: Mutex::new(State {
                pending: HashMap::new(),
                world: None,
                world_quarantined: false,
                events: VecDeque::new(),
                disconnected: false,
            }),
            next: AtomicU64::new(1),
            max_pending,
            max_events,
        }))
    }
    pub fn begin(&self, request: Request) -> Result<(PendingRequest, String), String> {
        let mut state = self.0.state.lock().unwrap();
        if state.disconnected {
            return Err("socket disconnected".into());
        }
        let world_request = matches!(request, Request::World);
        if world_request && (state.world.is_some() || state.world_quarantined) {
            return Err("world response slot is occupied or quarantined".into());
        }
        if world_request && state.pending.len() >= self.0.max_pending {
            return Err("pending request limit reached".into());
        }
        if !world_request
            && state.pending.len() + usize::from(state.world.is_some()) >= self.0.max_pending
        {
            return Err("pending request limit reached".into());
        }
        let id = if world_request {
            0
        } else {
            next_id(&self.0, &state)?
        };
        let (tx, rx) = oneshot::channel();
        let (frame, pending) = match request {
            Request::World => ("420[\"world\"]".to_owned(), None),
            Request::Document {
                document,
                action,
                operation,
            } => {
                let frame = format!(
                    "42{id}{}",
                    json!(["modifyDocument", {"type":document,"action":action,"operation":operation}])
                );
                (frame, Some((document, action)))
            }
        };
        if world_request {
            state.world = Some(WorldPending { tx, sent: false });
        } else {
            let (document, action) = pending.unwrap();
            state.pending.insert(
                id,
                Pending {
                    tx,
                    mutation: action != "get",
                    document,
                    action,
                },
            );
        }
        Ok((
            PendingRequest {
                id,
                rx: Some(rx),
                inner: Arc::downgrade(&self.0),
            },
            frame,
        ))
    }
    /// Dispatch a Socket.IO frame. Returns true when the namespace is no
    /// longer usable and the owning connection must be invalidated.
    pub fn receive(&self, frame: &str) -> bool {
        if crate::is_namespace_disconnect_or_error(frame) {
            self.disconnect();
            return true;
        }
        if frame.starts_with("430[") {
            let payload = serde_json::from_str::<Value>(&frame[3..])
                .ok()
                .and_then(|values| values.get(0).cloned())
                .filter(Value::is_object)
                .ok_or_else(|| "invalid Foundry world response".to_owned());
            let mut state = self.0.state.lock().unwrap();
            if let Some(world) = state.world.take() {
                let _ = world.tx.send(payload);
            } else if state.world_quarantined {
                state.world_quarantined = false;
                push_event(&mut state.events, frame, self.0.max_events);
            } else {
                push_event(&mut state.events, frame, self.0.max_events);
            }
            return false;
        }
        let Some((id, response)) = parse_ack(frame) else {
            self.event(frame);
            return false;
        };
        let mut state = self.0.state.lock().unwrap();
        if let Some(pending) = state.pending.get(&id) {
            let actual_type = response.get("type").and_then(Value::as_str);
            let has_error = response.get("error").is_some_and(|error| !error.is_null());
            let matches_document = {
                (actual_type == Some(&pending.document) || (actual_type.is_none() && has_error))
                    && response
                        .get("action")
                        .and_then(Value::as_str)
                        .is_none_or(|action| action == pending.action)
            };
            if matches_document {
                let pending = state.pending.remove(&id).unwrap();
                let result = if has_error {
                    let message = response["error"]
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown error");
                    Err(if pending.mutation {
                        format!("Foundry document request failed: {message}, and the operation may have been applied")
                    } else {
                        format!("Foundry document request failed: {message}")
                    })
                } else {
                    Ok(response)
                };
                let _ = pending.tx.send(result);
            } else {
                push_event(&mut state.events, frame, self.0.max_events);
            }
        } else {
            push_event(&mut state.events, frame, self.0.max_events);
        }
        false
    }
    pub fn event(&self, frame: &str) {
        let mut state = self.0.state.lock().unwrap();
        push_event(&mut state.events, frame, self.0.max_events);
    }
    pub fn pop_event(&self) -> Option<String> {
        self.0.state.lock().unwrap().events.pop_front()
    }
    pub fn pending_count(&self) -> usize {
        let state = self.0.state.lock().unwrap();
        state.pending.len() + usize::from(state.world.is_some())
    }
    /// Mark immediately before attempting transport write. Mutation requests are
    /// conservatively treated as possibly sent from registration onward.
    /// Atomically authorize the transport write against disconnect. Call before
    /// beginning I/O and send only when this returns true.
    pub fn mark_sent(&self, id: u64) -> bool {
        let mut state = self.0.state.lock().unwrap();
        if state.disconnected {
            return false;
        }
        if id == 0 {
            if let Some(world) = state.world.as_mut() {
                world.sent = true;
                true
            } else {
                false
            }
        } else {
            state.pending.contains_key(&id)
        }
    }
    pub fn disconnect(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.disconnected = true;
        if let Some(world) = state.world.take() {
            let _ = world.tx.send(Err("socket disconnected".into()));
        }
        state.world_quarantined = false;
        for (_, pending) in state.pending.drain() {
            let message = if pending.mutation {
                "socket disconnected; operation may have been applied"
            } else {
                "socket disconnected"
            };
            let _ = pending.tx.send(Err(message.into()));
        }
    }
}

impl PendingRequest {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub async fn wait(mut self) -> Result<Value, String> {
        self.rx
            .take()
            .unwrap()
            .await
            .unwrap_or_else(|_| Err("request cancelled".into()))
    }
}
impl Drop for PendingRequest {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut state = inner.state.lock().unwrap();
            if self.id == 0 {
                if let Some(world) = state.world.take() {
                    state.world_quarantined = world.sent;
                }
            } else {
                state.pending.remove(&self.id);
            }
        }
    }
}
fn push_event(events: &mut VecDeque<String>, frame: &str, limit: usize) {
    if limit == 0 {
        return;
    }
    if events.len() == limit {
        events.pop_front();
    }
    const MAX_EVENT_BYTES: usize = 16 * 1024;
    if frame.len() <= MAX_EVENT_BYTES {
        events.push_back(frame.to_owned());
    } else {
        let end = (0..=MAX_EVENT_BYTES)
            .rev()
            .find(|index| frame.is_char_boundary(*index))
            .unwrap_or(0);
        events.push_back(format!("{}…[truncated]", &frame[..end]));
    }
}
fn next_id(inner: &Inner, state: &State) -> Result<u64, String> {
    for _ in 0..=state.pending.len() {
        let id = inner
            .next
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .map_err(|_| "acknowledgement id space exhausted".to_owned())?;
        if id != 0 && !state.pending.contains_key(&id) {
            return Ok(id);
        }
    }
    Err("no available acknowledgement id".into())
}
pub fn pong_for_ping(frame: &str) -> Option<&'static str> {
    (frame == "2").then_some("3")
}
fn parse_ack(frame: &str) -> Option<(u64, Value)> {
    let tail = frame.strip_prefix("43")?;
    let end = tail.find('[')?;
    let digits = &tail[..end];
    let id = digits.parse::<u64>().ok()?;
    if digits != id.to_string() {
        return None;
    }
    let values: Value = serde_json::from_str(&tail[end..]).ok()?;
    let value = values.get(0)?.clone();
    Some((id, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn correlates_interleaved_world_and_document_acknowledgements() {
        let mux = SocketMux::new(2, 2);
        let (world, world_frame) = mux.begin(Request::World).unwrap();
        let (doc, doc_frame) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: json!({"action":"get"}),
            })
            .unwrap();
        assert_eq!(world_frame, "420[\"world\"]");
        assert!(doc_frame.starts_with("42"));
        assert_ne!(world.id(), doc.id());
        mux.receive(&format!(
            "43{}[{{\"type\":\"Actor\",\"result\":[]}}]",
            doc.id()
        ));
        mux.receive("430[{\"world\":{\"title\":\"W\"}}]");
        assert_eq!(
            doc.wait().await.unwrap(),
            json!({"type":"Actor","result":[]})
        );
        assert_eq!(world.wait().await.unwrap(), json!({"world":{"title":"W"}}));
    }

    #[tokio::test]
    async fn cancellation_removes_pending_and_world_frame_is_correlated() {
        let mux = SocketMux::new(1, 1);
        let (request, frame) = mux.begin(Request::World).unwrap();
        assert_eq!(frame, "420[\"world\"]");
        drop(request);
        assert_eq!(mux.pending_count(), 0);
    }

    #[tokio::test]
    async fn cancelled_world_is_quarantined_until_late_430_arrives() {
        let mux = SocketMux::new(2, 2);
        let (world, frame) = mux.begin(Request::World).unwrap();
        assert_eq!(frame, "420[\"world\"]");
        mux.mark_sent(world.id());
        drop(world);
        assert!(mux.begin(Request::World).is_err());
        mux.receive("430[{\"world\":{\"title\":\"late\"}}]");
        assert_eq!(
            mux.pop_event().as_deref(),
            Some("430[{\"world\":{\"title\":\"late\"}}]")
        );
        let (next, _) = mux.begin(Request::World).unwrap();
        mux.receive("430[{\"world\":{\"title\":\"current\"}}]");
        assert_eq!(
            next.wait().await.unwrap(),
            json!({"world":{"title":"current"}})
        );
    }

    #[tokio::test]
    async fn ordinary_ack_ids_after_protocol_prefix_are_correlated() {
        let mux = SocketMux::new(1, 1);
        let (pending, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: json!({}),
            })
            .unwrap();
        mux.receive(&format!(
            "43{}[{{\"type\":\"Actor\",\"result\":[]}}]",
            pending.id()
        ));
        assert_eq!(pending.wait().await.unwrap()["type"], "Actor");
    }

    #[tokio::test]
    async fn wrong_document_type_or_action_does_not_consume_ack_slot() {
        let mux = SocketMux::new(2, 2);
        let (pending, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "update".into(),
                operation: json!({}),
            })
            .unwrap();
        let id = pending.id();
        mux.receive(&format!(
            "43{id}[{{\"type\":\"Item\",\"action\":\"update\",\"result\":[]}}]"
        ));
        mux.receive(&format!(
            "43{id}[{{\"type\":\"Actor\",\"action\":\"delete\",\"result\":[]}}]"
        ));
        assert_eq!(mux.pending_count(), 1);
        mux.receive(&format!(
            "43{id}[{{\"type\":\"Actor\",\"action\":\"update\",\"result\":[]}}]"
        ));
        assert_eq!(pending.wait().await.unwrap()["action"], "update");
    }

    #[tokio::test]
    async fn world_response_must_contain_object_metadata() {
        for frame in ["430[]", "430[null]", "430[1]", "430[\"x\"]"] {
            let mux = SocketMux::new(1, 1);
            let (world, _) = mux.begin(Request::World).unwrap();
            mux.mark_sent(world.id());
            mux.receive(frame);
            assert!(world.wait().await.is_err(), "{frame}");
        }
    }

    #[tokio::test]
    async fn unsent_world_drop_does_not_quarantine_but_sent_drop_does() {
        let mux = SocketMux::new(1, 1);
        let (unsent, _) = mux.begin(Request::World).unwrap();
        drop(unsent);
        let (sent, _) = mux.begin(Request::World).unwrap();
        mux.mark_sent(sent.id());
        drop(sent);
        assert!(mux.begin(Request::World).is_err());
        mux.receive("430[{\"world\":{}}]");
        assert!(mux.begin(Request::World).is_ok());
    }

    #[tokio::test]
    async fn disconnect_during_inflight_mutation_is_conservatively_ambiguous() {
        let mux = SocketMux::new(1, 1);
        let (mutation, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "update".into(),
                operation: json!({}),
            })
            .unwrap();
        // begin reserves the request before transport I/O; disconnect may race with send completion.
        mux.disconnect();
        assert!(mutation
            .wait()
            .await
            .unwrap_err()
            .contains("may have been applied"));
    }

    #[tokio::test]
    async fn leading_zero_ack_id_is_unsolicited_not_correlated() {
        let mux = SocketMux::new(1, 1);
        let (pending, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: json!({}),
            })
            .unwrap();
        let id = pending.id();
        mux.receive(&format!("430{id}[{{\"type\":\"Actor\",\"result\":[]}}]"));
        assert_eq!(mux.pending_count(), 1);
    }

    #[tokio::test]
    async fn matching_document_error_keeps_connector_error_and_ambiguity_semantics() {
        let mux = SocketMux::new(1, 1);
        let (pending, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "update".into(),
                operation: json!({}),
            })
            .unwrap();
        mux.receive(&format!(
            "43{}[{{\"error\":{{\"message\":\"denied\"}}}}]",
            pending.id()
        ));
        let error = pending.wait().await.unwrap_err();
        assert!(error.contains("denied"), "{error}");
        assert!(error.contains("may have been applied"), "{error}");
    }

    #[tokio::test]
    async fn send_authorization_fails_after_disconnect() {
        let mux = SocketMux::new(1, 1);
        let (request, _) = mux.begin(Request::World).unwrap();
        mux.disconnect();
        assert!(!mux.mark_sent(request.id()));
        assert!(request.wait().await.is_err());
    }

    #[tokio::test]
    async fn namespace_disconnect_and_error_invalidate_without_becoming_events() {
        for frame in ["41", "44[\"authentication failed\"]"] {
            let mux = SocketMux::new(1, 1);
            let (pending, _) = mux
                .begin(Request::Document {
                    document: "Actor".into(),
                    action: "update".into(),
                    operation: json!({"action":"update"}),
                })
                .unwrap();

            assert!(mux.receive(frame), "{frame} should invalidate the mux");
            assert_eq!(mux.pending_count(), 0, "{frame}");
            assert!(mux.pop_event().is_none(), "{frame} was treated as an event");
            assert!(pending
                .wait()
                .await
                .unwrap_err()
                .contains("may have been applied"));
            assert!(mux.begin(Request::World).is_err());
        }
    }

    #[tokio::test]
    async fn disconnect_conservatively_marks_registered_mutation_ambiguous() {
        let mux = SocketMux::new(1, 1);
        let (write, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "delete".into(),
                operation: json!({}),
            })
            .unwrap();
        mux.disconnect();
        let error = write.wait().await.unwrap_err();
        assert!(error.contains("may have been applied"), "{error}");
    }

    #[test]
    fn ping_is_answered_as_control_not_registered_as_request() {
        assert_eq!(pong_for_ping("2"), Some("3"));
        assert_eq!(pong_for_ping("42[\"event\"]"), None);
    }

    #[tokio::test]
    async fn bounds_pending_and_events_and_marks_sent_mutation_ambiguous_on_disconnect() {
        let mux = SocketMux::new(1, 1);
        let (write, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "update".into(),
                operation: json!({"action":"update"}),
            })
            .unwrap();
        mux.mark_sent(write.id());
        assert!(mux
            .begin(Request::Document {
                document: "Item".into(),
                action: "get".into(),
                operation: json!({}),
            })
            .is_err());
        mux.event("event-one");
        mux.event("event-two");
        assert_eq!(mux.pop_event().as_deref(), Some("event-two"));
        mux.event(&"x".repeat(20 * 1024));
        assert!(mux.pop_event().unwrap().len() < 17 * 1024);
        mux.disconnect();
        assert!(write
            .wait()
            .await
            .unwrap_err()
            .contains("may have been applied"));
        assert!(mux.begin(Request::World).is_err());
    }

    #[test]
    fn pending_limit_counts_world_and_document_requests_together() {
        let mux = SocketMux::new(1, 1);
        let (_world, _) = mux.begin(Request::World).unwrap();
        assert!(mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: json!({"action":"get"}),
            })
            .is_err());
    }

    #[test]
    fn pending_limit_applies_when_world_follows_document() {
        let mux = SocketMux::new(1, 1);
        let (_document, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: json!({"action":"get"}),
            })
            .unwrap();
        assert!(mux.begin(Request::World).is_err());
    }
}
