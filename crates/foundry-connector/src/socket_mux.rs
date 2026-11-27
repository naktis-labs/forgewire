use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::{AtomicU64, AtomicU8, Ordering},
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
    Compendium {
        action: String,
        data: Value,
    },
    Files {
        target: String,
        kind: String,
        extensions: Vec<String>,
    },
}

/// What a pending request expects its acknowledgement to contain. The
/// acknowledgement id already identifies the request, so the expectation only
/// decides whether a reply carrying that id is the answer or a frame that
/// must be left unclaimed.
#[derive(Clone, Debug)]
enum Expectation {
    Document { document: String, action: String },
    Compendium { action: String },
    Files,
}

/// Where a queued frame is on its way to the socket.
///
/// The state is shared by the request that owns the frame, the writer task
/// and the pending registry, so that all three agree on whether the frame
/// was handed to the socket. Only the writer may move a frame from
/// `Queued` to `Transmitting`, and only the owner of a request may move it
/// from `Queued` to `Cancelled`. Whichever happens first decides the fate
/// of the frame: a cancelled frame never reaches the instance, and a frame
/// the writer took may have reached it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameState {
    /// The frame is queued and the writer has not taken it yet.
    Queued,
    /// The writer took the frame and is writing it to the socket.
    Transmitting,
    /// The request gave up before the writer took the frame.
    Cancelled,
}

const FRAME_QUEUED: u8 = 0;
const FRAME_TRANSMITTING: u8 = 1;
const FRAME_CANCELLED: u8 = 2;

/// The shared lifecycle of one queued frame.
#[derive(Clone, Debug)]
pub struct FrameHandle(Arc<AtomicU8>);

impl FrameHandle {
    pub fn new() -> Self {
        FrameHandle(Arc::new(AtomicU8::new(FRAME_QUEUED)))
    }
    pub fn state(&self) -> FrameState {
        match self.0.load(Ordering::Acquire) {
            FRAME_TRANSMITTING => FrameState::Transmitting,
            FRAME_CANCELLED => FrameState::Cancelled,
            _ => FrameState::Queued,
        }
    }
    /// The writer takes the frame for transmission.
    ///
    /// It returns false when the request was cancelled first, in which case
    /// the frame must not be written.
    pub fn claim(&self) -> bool {
        self.0
            .compare_exchange(
                FRAME_QUEUED,
                FRAME_TRANSMITTING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
    /// The request gave up before the writer took the frame.
    ///
    /// It returns false when the writer already took the frame, in which case
    /// the frame may have reached the instance.
    pub fn cancel(&self) -> bool {
        self.0
            .compare_exchange(
                FRAME_QUEUED,
                FRAME_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
    /// Whether the frame may have reached the instance.
    pub fn may_have_been_transmitted(&self) -> bool {
        self.state() == FrameState::Transmitting
    }
}

impl Default for FrameHandle {
    fn default() -> Self {
        FrameHandle::new()
    }
}

struct State {
    pending: HashMap<u64, Pending>,
    world: Option<WorldPending>,
    world_quarantined: bool,
    events: VecDeque<String>,
    disconnected: bool,
}
/// Why a pending request failed.
///
/// The distinction decides the fate of the shared connection: the upstream
/// answered and refused the request, so the connection stays usable for
/// other requests; the transport failed, so the connection cannot carry
/// another request and is invalidated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestError {
    /// The upstream answered. The connection is healthy and stays in use.
    Answer(String),
    /// The transport failed. The connection cannot carry another request.
    Transport(String),
    /// The transport failed before the request's frame was handed to the
    /// socket: the operation was not applied.
    NotTransmitted(String),
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

impl RequestError {
    /// The failure message, whichever kind of failure it is.
    pub fn message(&self) -> &str {
        match self {
            RequestError::Answer(message)
            | RequestError::Transport(message)
            | RequestError::NotTransmitted(message) => message,
        }
    }
    /// Whether the failure leaves the connection unusable.
    pub fn is_transport(&self) -> bool {
        matches!(
            self,
            RequestError::Transport(_) | RequestError::NotTransmitted(_)
        )
    }
    /// Whether the outcome of the request is unknown.
    ///
    /// A request whose frame was never handed to the socket has a known
    /// outcome: it did not happen.
    pub fn is_ambiguous(&self) -> bool {
        !matches!(self, RequestError::NotTransmitted(_))
    }
}

struct Pending {
    tx: oneshot::Sender<Result<Value, RequestError>>,
    mutation: bool,
    expectation: Expectation,
    frame: FrameHandle,
}
struct WorldPending {
    tx: oneshot::Sender<Result<Value, RequestError>>,
    frame: FrameHandle,
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
    rx: Option<oneshot::Receiver<Result<Value, RequestError>>>,
    inner: Weak<Inner>,
    frame: FrameHandle,
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
        let frame = FrameHandle::new();
        let (frame_text, pending) = match request {
            Request::World => ("420[\"world\"]".to_owned(), None),
            Request::Document {
                document,
                action,
                operation,
            } => {
                let frame_text = format!(
                    "42{id}{}",
                    json!(["modifyDocument", {"type":document,"action":action,"operation":operation}])
                );
                let mutation = action != "get";
                (
                    frame_text,
                    Some((Expectation::Document { document, action }, mutation)),
                )
            }
            Request::Compendium { action, data } => {
                let mut operation = json!({"action":action, "data":data});
                if action == "create" {
                    operation["options"] = json!({});
                }
                let frame_text = format!("42{id}{}", json!(["manageCompendium", operation]));
                (frame_text, Some((Expectation::Compendium { action }, true)))
            }
            Request::Files {
                target,
                kind,
                extensions,
            } => {
                let frame_text = format!(
                    "42{id}{}",
                    json!(["manageFiles", {"action":"browseFiles", "storage":"data", "target":target}, {"type":kind, "extensions":extensions, "wildcard":false, "render":true}])
                );
                (frame_text, Some((Expectation::Files, false)))
            }
        };
        if world_request {
            state.world = Some(WorldPending {
                tx,
                frame: frame.clone(),
            });
        } else {
            let (expectation, mutation) = pending.unwrap();
            state.pending.insert(
                id,
                Pending {
                    tx,
                    mutation,
                    expectation,
                    frame: frame.clone(),
                },
            );
        }
        Ok((
            PendingRequest {
                id,
                rx: Some(rx),
                inner: Arc::downgrade(&self.0),
                frame,
            },
            frame_text,
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
                .ok_or_else(|| RequestError::Answer("invalid Foundry world response".to_owned()));
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
        let Some(id) = parse_ack_id(frame) else {
            self.event(frame);
            return false;
        };
        let mut state = self.0.state.lock().unwrap();
        if !state.pending.contains_key(&id) {
            // The answer belongs to a request that is no longer pending.
            push_event(&mut state.events, frame, self.0.max_events);
            return false;
        }
        let Some(response) = parse_ack(frame) else {
            // The frame carries this request's id but cannot be read as an
            // answer. The socket answered, so it stays usable; this request
            // fails instead of waiting for an answer that will not come.
            let pending = state.pending.remove(&id).unwrap();
            let _ = pending.tx.send(Err(RequestError::Answer(
                "invalid Foundry acknowledgement".to_owned(),
            )));
            return false;
        };
        let result = {
            let pending = state.pending.get(&id).unwrap();
            evaluate(pending, &response, frame.len())
        };
        let pending = state.pending.remove(&id).unwrap();
        let _ = pending.tx.send(result);
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
    /// Atomically authorize the transport write against disconnect. Call before
    /// beginning I/O and send only when this returns true.
    ///
    /// Authorization is not transmission: the writer takes the frame when it
    /// writes it, and a request that gives up before that takes the frame
    /// with it.
    pub fn mark_sent(&self, id: u64) -> bool {
        let state = self.0.state.lock().unwrap();
        if state.disconnected {
            return false;
        }
        if id == 0 {
            state.world.is_some()
        } else {
            state.pending.contains_key(&id)
        }
    }
    pub fn disconnect(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.disconnected = true;
        if let Some(world) = state.world.take() {
            // A world frame the writer never took cannot reach the instance.
            world.frame.cancel();
            let _ = world
                .tx
                .send(Err(RequestError::Transport("socket disconnected".into())));
        }
        state.world_quarantined = false;
        for (_, pending) in state.pending.drain() {
            // A frame the writer never took cannot reach the instance: the
            // operation certainly did not happen. A frame the writer took
            // may have reached it, so the outcome stays unknown.
            pending.frame.cancel();
            let error = if pending.frame.may_have_been_transmitted() {
                RequestError::Transport(if pending.mutation {
                    "socket disconnected; operation may have been applied".to_owned()
                } else {
                    "socket disconnected".to_owned()
                })
            } else if pending.mutation {
                RequestError::NotTransmitted(
                    "socket disconnected; the operation was not applied".to_owned(),
                )
            } else {
                RequestError::NotTransmitted("socket disconnected".to_owned())
            };
            let _ = pending.tx.send(Err(error));
        }
    }
}

/// Decide what the acknowledgement for a pending request means.
///
/// The acknowledgement id is unique per pending request, so an id that
/// matches is that request's answer. The expectations add the protocol shape
/// checks the reference performs on the reply, and the ambiguity wording a
/// mutation must carry when it fails. An answer that does not fit the
/// request fails it: the socket answered, so it stays usable.
fn evaluate(pending: &Pending, response: &Value, frame_len: usize) -> Result<Value, RequestError> {
    match &pending.expectation {
        Expectation::Document { document, action } => {
            let actual_type = response.get("type").and_then(Value::as_str);
            let has_error = response.get("error").is_some_and(|error| !error.is_null());
            let matches_document = {
                (actual_type == Some(document.as_str()) || (actual_type.is_none() && has_error))
                    && response
                        .get("action")
                        .and_then(Value::as_str)
                        .is_none_or(|value| value == action)
            };
            if !matches_document {
                return Err(RequestError::Answer(if pending.mutation {
                    format!(
                        "Foundry document response does not answer the {document} {action} request, and the operation may have been applied"
                    )
                } else {
                    format!(
                        "Foundry document response does not answer the {document} {action} request"
                    )
                }));
            }
            if has_error {
                let message = response["error"]
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error");
                return Err(RequestError::Answer(if pending.mutation {
                    format!(
                        "Foundry document request failed: {message}, and the operation may have been applied"
                    )
                } else {
                    format!("Foundry document request failed: {message}")
                }));
            }
            Ok(response.clone())
        }
        Expectation::Compendium { action } => {
            let ambiguous =
                "Invalid Foundry compendium response; the operation may have been applied"
                    .to_owned();
            if response.pointer("/request/action").and_then(Value::as_str) != Some(action.as_str())
            {
                return Err(RequestError::Answer(ambiguous));
            }
            if let Some(error) = response.get("error").filter(|value| !value.is_null()) {
                let message = error
                    .as_str()
                    .or_else(|| error.get("message").and_then(Value::as_str))
                    .unwrap_or("unknown error");
                return Err(RequestError::Answer(format!(
                    "Foundry compendium {action} failed: {message}; the operation may have been applied"
                )));
            }
            let result = response.get("result");
            let valid = if action == "create" {
                result.is_some_and(|value| {
                    value.is_object() && value.as_object().is_some_and(|map| !map.is_empty())
                })
            } else {
                result
                    .is_some_and(|value| value.as_str().is_some_and(|text| !text.trim().is_empty()))
            };
            if !valid {
                return Err(RequestError::Answer(ambiguous));
            }
            Ok(response.clone())
        }
        Expectation::Files => {
            if frame_len > crate::files::MAX_RESPONSE_BYTES {
                return Err(RequestError::Answer(
                    "Foundry browse response too large".to_owned(),
                ));
            }
            if let Some(error) = response
                .get("error")
                .filter(|value| !value.is_null() && *value != &json!(false))
            {
                return Err(RequestError::Answer(format!(
                    "Foundry browse failed: {error}"
                )));
            }
            if !response.get("dirs").is_some_and(Value::is_array) {
                return Err(RequestError::Answer(
                    "invalid Foundry browse response".to_owned(),
                ));
            }
            Ok(response.clone())
        }
    }
}

impl PendingRequest {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub async fn wait(mut self) -> Result<Value, RequestError> {
        self.rx
            .take()
            .unwrap()
            .await
            .unwrap_or_else(|_| Err(RequestError::Answer("request cancelled".into())))
    }
}
impl PendingRequest {
    /// The lifecycle of the frame this request owns.
    ///
    /// The writer takes the frame when it writes it, and the request takes
    /// it when it gives up: the first of the two decides whether the frame
    /// can still reach the instance.
    pub fn frame(&self) -> FrameHandle {
        self.frame.clone()
    }
}
impl Drop for PendingRequest {
    fn drop(&mut self) {
        // A request that gives up takes its frame with it: a frame the writer
        // has not taken must not reach the instance afterwards.
        self.frame.cancel();
        if let Some(inner) = self.inner.upgrade() {
            let mut state = inner.state.lock().unwrap();
            if self.id == 0 {
                if let Some(world) = state.world.take() {
                    // Only a frame the writer took can still be answered by a
                    // late response.
                    state.world_quarantined = world.frame.may_have_been_transmitted();
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
/// The acknowledgement id of a Socket.IO acknowledgement frame
/// (`43<id>[...]`), or None when the frame carries none.
fn parse_ack_id(frame: &str) -> Option<u64> {
    let tail = frame.strip_prefix("43")?;
    let end = tail.find('[')?;
    let digits = &tail[..end];
    let id = digits.parse::<u64>().ok()?;
    if digits != id.to_string() {
        return None;
    }
    Some(id)
}

/// The answer carried by an acknowledgement frame.
fn parse_ack(frame: &str) -> Option<Value> {
    let tail = frame.strip_prefix("43")?;
    let end = tail.find('[')?;
    let values: Value = serde_json::from_str(&tail[end..]).ok()?;
    Some(values.get(0)?.clone())
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
        // The writer took the frame, so the instance may still answer it.
        assert!(
            world.frame().claim(),
            "the writer takes the frame for transmission"
        );
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
    async fn an_answer_with_a_foreign_id_does_not_consume_the_pending_slot() {
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
            "43{}[{{\"type\":\"Actor\",\"action\":\"update\",\"result\":[]}}]",
            id + 1000
        ));
        assert_eq!(mux.pending_count(), 1);
        mux.receive(&format!(
            "43{id}[{{\"type\":\"Actor\",\"action\":\"update\",\"result\":[]}}]"
        ));
        assert_eq!(pending.wait().await.unwrap()["action"], "update");
    }

    #[tokio::test]
    async fn an_answer_that_does_not_fit_the_request_fails_it_instead_of_waiting() {
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
        assert_eq!(mux.pending_count(), 0, "the request stayed pending");
        let error = pending.wait().await.unwrap_err();
        assert!(
            error.message().contains("does not answer"),
            "{}",
            error.message()
        );
        assert!(error.message().contains("may have been applied"));
    }

    #[tokio::test]
    async fn a_malformed_answer_with_a_matching_id_fails_the_request() {
        let mux = SocketMux::new(2, 2);
        let (pending, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: json!({}),
            })
            .unwrap();
        let id = pending.id();
        mux.receive(&format!("43{id}[not-json]"));
        assert_eq!(mux.pending_count(), 0, "the request stayed pending");
        assert_eq!(
            pending.wait().await.unwrap_err().message(),
            "invalid Foundry acknowledgement"
        );
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
        assert!(
            sent.frame().claim(),
            "the writer takes the frame for transmission"
        );
        drop(sent);
        assert!(mux.begin(Request::World).is_err());
        mux.receive("430[{\"world\":{}}]");
        assert!(mux.begin(Request::World).is_ok());
    }

    #[tokio::test]
    async fn a_dropped_request_takes_its_frame_with_it() {
        let mux = SocketMux::new(2, 2);
        let (document, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "update".into(),
                operation: json!({"action":"update"}),
            })
            .unwrap();
        let frame = document.frame();
        drop(document);
        assert!(
            !frame.claim(),
            "a request that gave up takes its frame, so the writer cannot write it"
        );

        // The same holds for a world request, and its slot is not quarantined
        // either: no late answer can arrive for a frame that was never
        // written.
        let (world, _) = mux.begin(Request::World).unwrap();
        let frame = world.frame();
        drop(world);
        assert!(
            !frame.claim(),
            "a world request that gave up takes its frame"
        );
        assert!(
            mux.begin(Request::World).is_ok(),
            "the world slot is not quarantined for a frame that was never written"
        );
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
        // The writer took the frame, so the instance may have applied it.
        assert!(
            mutation.frame().claim(),
            "the writer takes the frame for transmission"
        );
        // begin reserves the request before transport I/O; disconnect may race with send completion.
        mux.disconnect();
        assert!(mutation
            .wait()
            .await
            .unwrap_err()
            .message()
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
        let error = error.message().to_owned();
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

            // The writer took the frame, so the instance may have applied it.
            assert!(
                pending.frame().claim(),
                "the writer takes the frame for transmission"
            );
            assert!(mux.receive(frame), "{frame} should invalidate the mux");
            assert_eq!(mux.pending_count(), 0, "{frame}");
            assert!(mux.pop_event().is_none(), "{frame} was treated as an event");
            assert!(pending
                .wait()
                .await
                .unwrap_err()
                .message()
                .contains("may have been applied"));
            assert!(mux.begin(Request::World).is_err());
        }
    }

    #[tokio::test]
    async fn disconnect_reports_an_untaken_mutation_as_not_applied() {
        let mux = SocketMux::new(1, 1);
        let (write, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "delete".into(),
                operation: json!({}),
            })
            .unwrap();
        // The writer never took the frame, so the instance cannot have seen
        // it: the operation certainly did not happen.
        mux.disconnect();
        let error = write.wait().await.unwrap_err();
        assert!(!error.is_ambiguous(), "the outcome of the request is known");
        let error = error.message().to_owned();
        assert!(error.contains("the operation was not applied"), "{error}");
    }

    #[tokio::test]
    async fn document_frames_keep_the_reference_operation_shape() {
        let mux = SocketMux::new(2, 2);
        let (pending, frame) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: json!({"action":"get", "query":{"folder":"f1"}, "index":false, "broadcast":false}),
            })
            .unwrap();
        assert_eq!(
            frame,
            format!(
                "42{}{}",
                pending.id(),
                json!(["modifyDocument", {"type":"Actor","action":"get","operation":{"action":"get","query":{"folder":"f1"},"index":false,"broadcast":false}}])
            )
        );
    }

    #[tokio::test]
    async fn compendium_frames_match_the_reference_and_correlate_their_acknowledgement() {
        let mux = SocketMux::new(4, 4);
        let (create, create_frame) = mux
            .begin(Request::Compendium {
                action: "create".into(),
                data: json!({"label":"NPCs","type":"Actor"}),
            })
            .unwrap();
        assert_eq!(
            create_frame,
            format!(
                "42{}{}",
                create.id(),
                json!(["manageCompendium", {"action":"create","data":{"label":"NPCs","type":"Actor"},"options":{}}])
            )
        );
        let (remove, remove_frame) = mux
            .begin(Request::Compendium {
                action: "delete".into(),
                data: json!("world.npcs"),
            })
            .unwrap();
        assert_eq!(
            remove_frame,
            format!(
                "42{}{}",
                remove.id(),
                json!(["manageCompendium", {"action":"delete","data":"world.npcs"}])
            )
        );
        assert_ne!(create.id(), remove.id());

        mux.receive(&format!(
            "43{}[{}]",
            create.id(),
            json!({"request":{"action":"create"},"result":{"id":"world.npcs"}})
        ));
        mux.receive(&format!(
            "43{}[{}]",
            remove.id(),
            json!({"request":{"action":"delete"},"result":"world.npcs"})
        ));
        assert_eq!(create.wait().await.unwrap()["result"]["id"], "world.npcs");
        assert_eq!(remove.wait().await.unwrap()["result"], "world.npcs");
    }

    #[tokio::test]
    async fn compendium_failures_report_the_ambiguity_of_a_possible_change() {
        let mux = SocketMux::new(4, 4);
        let (denied, _) = mux
            .begin(Request::Compendium {
                action: "delete".into(),
                data: json!("world.npcs"),
            })
            .unwrap();
        mux.receive(&format!(
            "43{}[{}]",
            denied.id(),
            json!({"request":{"action":"delete"},"error":"denied"})
        ));
        let error = denied.wait().await.unwrap_err();
        let error = error.message().to_owned();
        assert!(error.contains("denied"), "{error}");
        assert!(error.contains("may have been applied"), "{error}");

        let (incomplete, _) = mux
            .begin(Request::Compendium {
                action: "create".into(),
                data: json!({"label":"NPCs","type":"Actor"}),
            })
            .unwrap();
        mux.receive(&format!(
            "43{}[{}]",
            incomplete.id(),
            json!({"request":{"action":"create"},"result":{}})
        ));
        assert!(incomplete
            .wait()
            .await
            .unwrap_err()
            .message()
            .contains("may have been applied"));

        let (mismatched, _) = mux
            .begin(Request::Compendium {
                action: "delete".into(),
                data: json!("world.npcs"),
            })
            .unwrap();
        mux.receive(&format!(
            "43{}[{}]",
            mismatched.id(),
            json!({"request":{"action":"create"},"result":"world.npcs"})
        ));
        assert!(mismatched
            .wait()
            .await
            .unwrap_err()
            .message()
            .contains("may have been applied"));
    }

    #[tokio::test]
    async fn file_browsing_frames_match_the_reference_and_reject_bad_answers() {
        let mux = SocketMux::new(4, 4);
        let (browse, frame) = mux
            .begin(Request::Files {
                target: "worlds/test".into(),
                kind: "image".into(),
                extensions: vec![".png".into()],
            })
            .unwrap();
        assert_eq!(
            frame,
            format!(
                "42{}{}",
                browse.id(),
                json!(["manageFiles", {"action":"browseFiles","storage":"data","target":"worlds/test"}, {"type":"image","extensions":[".png"],"wildcard":false,"render":true}])
            )
        );
        mux.receive(&format!(
            "43{}[{}]",
            browse.id(),
            json!({"dirs":[],"files":[]})
        ));
        assert_eq!(browse.wait().await.unwrap()["dirs"], json!([]));

        let (denied, _) = mux
            .begin(Request::Files {
                target: "worlds/test".into(),
                kind: "image".into(),
                extensions: vec![".png".into()],
            })
            .unwrap();
        mux.receive(&format!("43{}[{}]", denied.id(), json!({"error":"denied"})));
        assert!(denied
            .wait()
            .await
            .unwrap_err()
            .message()
            .contains("denied"));

        let (malformed, _) = mux
            .begin(Request::Files {
                target: "worlds/test".into(),
                kind: "image".into(),
                extensions: vec![".png".into()],
            })
            .unwrap();
        mux.receive(&format!("43{}[{}]", malformed.id(), json!({"files":[]})));
        assert!(malformed
            .wait()
            .await
            .unwrap_err()
            .message()
            .contains("invalid Foundry browse response"));
    }

    #[tokio::test]
    async fn interleaved_document_and_compendium_acknowledgements_stay_separate() {
        let mux = SocketMux::new(4, 4);
        let (document, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: json!({"action":"get"}),
            })
            .unwrap();
        let (compendium, _) = mux
            .begin(Request::Compendium {
                action: "create".into(),
                data: json!({"label":"NPCs","type":"Actor"}),
            })
            .unwrap();
        mux.receive(&format!(
            "43{}[{}]",
            compendium.id(),
            json!({"request":{"action":"create"},"result":{"id":"world.npcs"}})
        ));
        mux.receive(&format!(
            "43{}[{}]",
            document.id(),
            json!({"type":"Actor","action":"get","result":[]})
        ));
        assert_eq!(
            compendium.wait().await.unwrap()["result"]["id"],
            "world.npcs"
        );
        assert_eq!(document.wait().await.unwrap()["type"], "Actor");
    }

    #[tokio::test]
    async fn acknowledgements_preserve_foundry_decimal_precision() {
        let mux = SocketMux::new(2, 2);
        let (pending, _) = mux
            .begin(Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: json!({"action":"get"}),
            })
            .unwrap();
        mux.receive(&format!(
            "43{}[{}]",
            pending.id(),
            json!({"type":"Actor","result":[{"scaleX":0.9615384615384615}]})
        ));
        let result = pending.wait().await.unwrap();
        assert_eq!(
            result["result"].to_string(),
            r#"[{"scaleX":0.9615384615384615}]"#
        );
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
        assert!(
            write.frame().claim(),
            "the writer takes the frame for transmission"
        );
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
            .message()
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
