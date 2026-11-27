use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock, Weak,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use ring::hmac;
use serde_json::Value;
use tokio::{
    sync::{mpsc, Mutex, Notify},
    time::timeout,
};
use tokio_tungstenite::{tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};

pub(crate) type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// A process-keyed digest identifies credentials without retaining secrets in
/// the manager's map. The key is not exposed through Debug or logs.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct IdentityKey([u8; 32]);

impl IdentityKey {
    pub(crate) fn new(parts: &[&str]) -> Self {
        static PROCESS_KEY: OnceLock<[u8; 32]> = OnceLock::new();
        let key = PROCESS_KEY.get_or_init(|| {
            let mut key = [0; 32];
            rand::thread_rng().fill_bytes(&mut key);
            key
        });
        let mut input = Vec::new();
        for part in parts {
            input.extend_from_slice(&(part.len() as u64).to_be_bytes());
            input.extend_from_slice(part.as_bytes());
        }
        let key = hmac::Key::new(hmac::HMAC_SHA256, key);
        Self(hmac::sign(&key, &input).as_ref().try_into().unwrap())
    }
}

struct Connection {
    transport: Mutex<Option<Transport>>,
    world_gate: Mutex<()>,
    epoch: Arc<std::sync::atomic::AtomicU64>,
    next_epoch: std::sync::atomic::AtomicU64,
    last_used: std::sync::atomic::AtomicU64,
}

struct Outbound {
    message: Message,
    written: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    /// The lifecycle of the frame, shared with the request that owns it.
    frame: crate::socket_mux::FrameHandle,
}

/// How a failed write is classified.
///
/// A frame the writer never took cannot have reached the instance, so the
/// operation certainly did not happen. A frame the writer took may have
/// reached it, so the outcome stays unknown.
#[derive(Debug)]
enum WriteFailure {
    NotTransmitted(anyhow::Error),
    Transmitted(anyhow::Error),
}

impl WriteFailure {
    fn into_error(self) -> anyhow::Error {
        match self {
            WriteFailure::NotTransmitted(error) | WriteFailure::Transmitted(error) => error,
        }
    }
    /// Whether the failure leaves the outcome of a mutation unknown.
    fn may_have_been_applied(&self) -> bool {
        matches!(self, WriteFailure::Transmitted(_))
    }
}

impl std::fmt::Display for WriteFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteFailure::NotTransmitted(error) | WriteFailure::Transmitted(error) => {
                write!(formatter, "{error}")
            }
        }
    }
}

/// What the writer did with one queued frame.
enum FrameWrite {
    /// The frame reached the socket.
    Written,
    /// The request gave up before the writer took the frame: the frame was
    /// not written and the socket stays usable.
    Skipped,
    /// The writer took the frame and the write failed: the socket is gone.
    Failed,
}

#[derive(Clone)]
struct Transport {
    sender: mpsc::Sender<Outbound>,
    mux: crate::socket_mux::SocketMux,
    epoch: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct ReconnectPolicy {
    pub(crate) attempts: usize,
    pub(crate) initial_delay: Duration,
    pub(crate) max_delay: Duration,
    pub(crate) max_jitter: Duration,
    pub(crate) attempt_timeout: Duration,
    pub(crate) total_timeout: Duration,
    /// How long a world request waits for the world slot of its connection.
    /// The world response carries no acknowledgement id, so only one world
    /// request may be in flight per connection. The budget covers the
    /// request and its response only: the connection handshake happens
    /// before the slot is taken.
    pub(crate) world_gate_timeout: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            max_jitter: Duration::from_millis(500),
            // A fresh identity setup can use five HTTP calls (15s each) and
            // two websocket connect/bind phases (10s + 10s each). The budget
            // leaves room for overhead without letting one attempt monopolize
            // an identity indefinitely.
            attempt_timeout: Duration::from_secs(90),
            // Three attempts plus bounded backoff; longer than one full setup.
            total_timeout: Duration::from_secs(300),
            // One world request and its response, nothing more.
            world_gate_timeout: crate::RESPONSE_TIMEOUT,
        }
    }
}

pub(crate) struct ConnectionManager {
    // The global map lock is never held over socket I/O; each identity has its
    // own transport lock; one reader task dispatches correlated responses.
    connections: Arc<Mutex<HashMap<IdentityKey, Arc<Connection>>>>,
    reconnect: ReconnectPolicy,
    capacity: usize,
    idle_timeout: Duration,
    reaper_started: AtomicBool,
    lifetime: Arc<()>,
    shutdown: Arc<Notify>,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::with_limits(64, Duration::from_secs(10 * 60))
    }
}

async fn retry_connect<T, F, Fut, J>(
    connect: &mut F,
    policy: ReconnectPolicy,
    mut jitter: J,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
    J: FnMut(Duration) -> Duration,
{
    let deadline = tokio::time::Instant::now() + policy.total_timeout;
    let mut delay = policy.initial_delay;
    let mut last_error = None;
    for attempt in 0..policy.attempts {
        // The hint belongs to the answer of this attempt only: a later
        // failure for another reason must not wait for it.
        let mut retry_after: Option<Duration> = None;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining.min(policy.attempt_timeout), connect()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => {
                if is_terminal_connect_error(&error) {
                    return Err(error);
                }
                retry_after = error.downcast_ref::<RetryAfter>().map(|hint| hint.after);
                last_error = Some(error);
            }
            Err(_) => {
                // The attempt timed out. It carries no upstream hint.
                let error = if let Some(previous) = last_error.take() {
                    anyhow!(
                        "Timed out establishing authenticated Foundry websocket after {:?}; previous attempt failed: {previous}",
                        policy.attempt_timeout
                    )
                } else {
                    anyhow!(
                        "Timed out establishing authenticated Foundry websocket after {:?}",
                        policy.attempt_timeout
                    )
                };
                if remaining <= policy.attempt_timeout {
                    return Err(error);
                }
                last_error = Some(error);
            }
        }
        if attempt + 1 < policy.attempts {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let jitter = jitter(policy.max_jitter);
            let wait = delay + jitter.min(policy.max_jitter);
            let wait = match retry_after {
                Some(hint) if hint > wait => hint,
                _ => wait,
            };
            tokio::time::sleep(wait.min(remaining)).await;
            delay = delay.saturating_mul(2).min(policy.max_delay);
        }
    }
    if tokio::time::Instant::now() >= deadline {
        return Err(match last_error {
            Some(error) => {
                anyhow!("Aggregate Foundry reconnect deadline expired; last error: {error}")
            }
            None => anyhow!("Timed out establishing authenticated Foundry websocket"),
        });
    }
    Err(last_error
        .unwrap_or_else(|| anyhow!("Timed out establishing authenticated Foundry websocket")))
}

async fn connect_transport<F, Fut>(
    connection: &Connection,
    connect: &mut F,
    policy: ReconnectPolicy,
) -> Result<Transport>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Socket>>,
{
    let socket = retry_connect(connect, policy, |max| {
        let jitter_limit = max.as_millis().min(u64::MAX as u128) as u64;
        if jitter_limit == 0 {
            Duration::ZERO
        } else {
            Duration::from_millis(rand::thread_rng().next_u64() % (jitter_limit + 1))
        }
    })
    .await?;
    let epoch = connection.next_epoch.fetch_add(1, Ordering::AcqRel);
    connection.epoch.store(epoch, Ordering::Release);
    Ok(Transport::start(socket, connection.epoch.clone(), epoch))
}

#[derive(Debug)]
pub(crate) struct TerminalConnectError(String);

/// An upstream `Retry-After` hint. The reconnect loop waits at least this
/// long before the next attempt; the aggregate deadline still bounds the
/// whole reconnect, so the hint can never extend it.
///
/// The hint carries the upstream message it belongs to: the message is what
/// the caller sees, and the hint must not hide it.
#[derive(Debug)]
pub(crate) struct RetryAfter {
    pub(crate) after: Duration,
    pub(crate) message: String,
}

impl std::fmt::Display for RetryAfter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}; upstream asked to retry after {:?}",
            self.message, self.after
        )
    }
}

impl std::fmt::Display for TerminalConnectError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for TerminalConnectError {}

pub(crate) fn mark_terminal(error: anyhow::Error) -> anyhow::Error {
    let message = error.to_string();
    error.context(TerminalConnectError(message))
}

fn is_terminal_connect_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<TerminalConnectError>().is_some()
}

fn invalidate_epoch(active: &std::sync::atomic::AtomicU64, epoch: u64) {
    let _ = active.compare_exchange(epoch, 0, Ordering::AcqRel, Ordering::Acquire);
}

fn monotonic_millis() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

struct InFlight<'a>(
    &'a std::sync::atomic::AtomicU64,
    u64,
    &'a std::sync::atomic::AtomicU64,
    bool,
);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if self.3 {
            // Cancellation can happen after send. Invalidate only this socket,
            // never a successor installed by a later request.
            invalidate_epoch(self.0, self.1);
            self.2.store(monotonic_millis(), Ordering::Relaxed);
        }
    }
}

impl ConnectionManager {
    fn with_limits(capacity: usize, idle_timeout: Duration) -> Self {
        Self::with_policy(capacity, idle_timeout, ReconnectPolicy::default())
    }

    pub(crate) fn with_policy(
        capacity: usize,
        idle_timeout: Duration,
        reconnect: ReconnectPolicy,
    ) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            reconnect,
            capacity: capacity.max(1),
            idle_timeout,
            reaper_started: AtomicBool::new(false),
            lifetime: Arc::new(()),
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn shutdown_reaper(&self) {
        self.shutdown.notify_one();
    }

    fn start_reaper(&self) {
        if self.reaper_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let connections = self.connections.clone();
        let lifetime: Weak<()> = Arc::downgrade(&self.lifetime);
        let shutdown = self.shutdown.clone();
        let interval = self.idle_timeout.max(Duration::from_millis(10));
        let idle_ms = self.idle_timeout.as_millis() as u64;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {},
                    _ = shutdown.notified() => break,
                }
                let Some(_alive) = lifetime.upgrade() else {
                    break;
                };
                cleanup_idle_entries(&connections, idle_ms).await;
            }
        });
    }

    async fn reserve(&self, key: IdentityKey) -> Result<Arc<Connection>> {
        self.start_reaper();
        let mut connections = self.connections.lock().await;
        cleanup_idle_locked(&mut connections, self.idle_timeout.as_millis() as u64);
        if let Some(connection) = connections.get(&key) {
            return Ok(connection.clone());
        }
        if connections.len() >= self.capacity {
            evict_oldest_idle(&mut connections);
        }
        if connections.len() >= self.capacity {
            return Err(anyhow!("Foundry connection capacity is full"));
        }
        let connection = Arc::new(Connection {
            transport: Mutex::new(None),
            world_gate: Mutex::new(()),
            epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            next_epoch: std::sync::atomic::AtomicU64::new(1),
            last_used: std::sync::atomic::AtomicU64::new(monotonic_millis()),
        });
        connections.insert(key, connection.clone());
        Ok(connection)
    }

    /// Adopt or establish the identity's authenticated connection.
    ///
    /// The transport lock is held while a connection is established, so
    /// concurrent requests for one identity share a single handshake. The
    /// returned transport carries the epoch it belongs to; a later request
    /// that finds a different epoch knows the connection was replaced.
    async fn ensure_transport<F, Fut>(
        &self,
        connection: &Arc<Connection>,
        connect: &mut F,
    ) -> Result<Transport>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Socket>>,
    {
        let mut slot = connection.transport.lock().await;
        if slot
            .as_ref()
            .is_some_and(|transport| connection.epoch.load(Ordering::Acquire) != transport.epoch)
        {
            slot.take();
        }
        if slot.is_none() {
            *slot = Some(connect_transport(connection, connect, self.reconnect).await?);
        }
        Ok(slot.as_ref().unwrap().clone())
    }

    /// Run one request on the identity's persistent connection.
    ///
    /// Every request kind — document read, document mutation, compendium
    /// management and file browsing — shares this path, so the connection is
    /// established once per identity and reused by later requests instead of
    /// being reopened per operation. Each request carries its own
    /// acknowledgement id, so concurrent requests on one connection are
    /// answered independently.
    pub(crate) async fn exchange<F, Fut>(
        &self,
        key: IdentityKey,
        request: crate::socket_mux::Request,
        label: &str,
        mut connect: F,
    ) -> Result<Value>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Socket>>,
    {
        let mutation = request_is_mutation(&request);
        let connection = self.reserve(key).await?;
        let transport = self.ensure_transport(&connection, &mut connect).await?;
        let (pending, frame) = match transport.mux.begin(request) {
            Ok(request) => request,
            Err(error) => return Err(anyhow!(error)),
        };
        let mut in_flight = InFlight(
            &connection.epoch,
            transport.epoch,
            &connection.last_used,
            true,
        );
        if !transport.mux.mark_sent(pending.id()) {
            // The frame was never written, so a change certainly did not
            // happen. The socket may be gone: drop it, so the next request
            // for this identity reconnects instead of reusing a dead one.
            transport.mux.disconnect();
            invalidate_epoch(&connection.epoch, transport.epoch);
            in_flight.3 = false;
            return Err(anyhow!(
                "Foundry {label} request was cancelled before writing"
            ));
        }
        match transport
            .write(Message::Text(frame.into()), pending.frame())
            .await
        {
            Ok(()) => {}
            Err(failure) => {
                let ambiguous = failure.may_have_been_applied();
                if ambiguous {
                    // The writer took the frame, so the socket may still be
                    // carrying it: the connection cannot be trusted any more.
                    transport.mux.disconnect();
                    invalidate_epoch(&connection.epoch, transport.epoch);
                }
                in_flight.3 = false;
                let error = failure.into_error();
                let message = if ambiguous {
                    format!(
                        "Failed writing Foundry {label} request{}: {error}",
                        ambiguity(mutation)
                    )
                } else if mutation {
                    format!(
                        "Failed writing Foundry {label} request: {error}; the request was not transmitted, so the operation was not applied"
                    )
                } else {
                    format!(
                        "Failed writing Foundry {label} request: {error}; the request was not transmitted"
                    )
                };
                return Err(if ambiguous && mutation {
                    crate::ambiguous_mutation(message)
                } else {
                    anyhow!(message)
                });
            }
        }
        match timeout(crate::RESPONSE_TIMEOUT, pending.wait()).await {
            Ok(Ok(value)) => {
                connection
                    .last_used
                    .store(monotonic_millis(), Ordering::Relaxed);
                in_flight.3 = false;
                Ok(value)
            }
            Ok(Err(error)) => {
                if error.is_transport() {
                    transport.mux.disconnect();
                    invalidate_epoch(&connection.epoch, transport.epoch);
                }
                // The upstream answered and refused this request: the socket
                // is healthy, so the connection stays in use for other
                // requests.
                in_flight.3 = false;
                let message = error.message().to_owned();
                Err(if mutation && error.is_ambiguous() {
                    crate::ambiguous_mutation(message)
                } else {
                    anyhow!(message)
                })
            }
            Err(_) => {
                transport.mux.disconnect();
                invalidate_epoch(&connection.epoch, transport.epoch);
                in_flight.3 = false;
                let message = format!(
                    "Timed out waiting for Foundry {label} response{}",
                    ambiguity(mutation)
                );
                Err(if mutation {
                    crate::ambiguous_mutation(message)
                } else {
                    anyhow!(message)
                })
            }
        }
    }

    /// Read the world through the identity's persistent connection.
    ///
    /// The world response carries no acknowledgement id, so only one world
    /// request may be in flight per connection. The connection is established
    /// *before* the world slot is taken: a slow handshake must not consume
    /// the world request's own response budget, and a second world request
    /// must wait for the first one's response, not for its handshake.
    pub(crate) async fn world<F, Fut>(&self, key: IdentityKey, mut connect: F) -> Result<Value>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Socket>>,
    {
        let connection = self.reserve(key).await?;
        let mut retries = 0;
        loop {
            let transport = self.ensure_transport(&connection, &mut connect).await?;
            let guard = timeout(
                self.reconnect.world_gate_timeout,
                connection.world_gate.lock(),
            )
            .await
            .map_err(|_| anyhow!("Timed out waiting for Foundry world request capacity"))?;
            if connection.epoch.load(Ordering::Acquire) != transport.epoch {
                // Another request replaced the connection while this one
                // waited for the world slot. Start again on the new one.
                drop(guard);
                retries += 1;
                if retries >= 2 {
                    return Err(anyhow!(
                        "Foundry world request could not keep a stable connection"
                    ));
                }
                continue;
            }
            let (pending, frame) = match transport.mux.begin(crate::socket_mux::Request::World) {
                Ok(request) => request,
                Err(error) => return Err(anyhow!(error)),
            };
            let mut in_flight = InFlight(
                &connection.epoch,
                transport.epoch,
                &connection.last_used,
                true,
            );
            let write = if transport.mux.mark_sent(pending.id()) {
                transport
                    .write(Message::Text(frame.into()), pending.frame())
                    .await
            } else {
                Err(WriteFailure::NotTransmitted(anyhow!(
                    "Foundry world request was cancelled before writing"
                )))
            };
            if let Err(failure) = write {
                if failure.may_have_been_applied() {
                    transport.mux.disconnect();
                    invalidate_epoch(&connection.epoch, transport.epoch);
                }
                in_flight.3 = false;
                return Err(anyhow!(
                    "Failed writing Foundry world request: {}",
                    failure.into_error()
                ));
            }
            return match timeout(crate::RESPONSE_TIMEOUT, pending.wait()).await {
                Ok(Ok(value)) => {
                    connection
                        .last_used
                        .store(monotonic_millis(), Ordering::Relaxed);
                    in_flight.3 = false;
                    Ok(value)
                }
                Ok(Err(error)) => {
                    if error.is_transport() {
                        transport.mux.disconnect();
                        invalidate_epoch(&connection.epoch, transport.epoch);
                    }
                    in_flight.3 = false;
                    Err(anyhow!(error.message().to_owned()))
                }
                Err(_) => {
                    transport.mux.disconnect();
                    invalidate_epoch(&connection.epoch, transport.epoch);
                    in_flight.3 = false;
                    Err(anyhow!("Timed out waiting for Foundry world response"))
                }
            };
        }
    }
}

/// Whether a request can change the world, and therefore has to report an
/// ambiguous outcome when it fails.
fn request_is_mutation(request: &crate::socket_mux::Request) -> bool {
    match request {
        crate::socket_mux::Request::Document { action, .. } => action != "get",
        crate::socket_mux::Request::Compendium { .. } => true,
        crate::socket_mux::Request::World | crate::socket_mux::Request::Files { .. } => false,
    }
}

/// The wording a failed mutation carries: a timeout or a broken socket does
/// not prove the operation was not applied.
fn ambiguity(mutation: bool) -> &'static str {
    if mutation {
        ", and the operation may have been applied"
    } else {
        ""
    }
}

impl Transport {
    fn start(socket: Socket, active_epoch: Arc<std::sync::atomic::AtomicU64>, epoch: u64) -> Self {
        let (sender, receiver) = mpsc::channel::<Outbound>(64);
        let mux = crate::socket_mux::SocketMux::new(128, 64);
        let reader_mux = mux.clone();
        let reader_epoch = active_epoch.clone();
        tokio::spawn(async move {
            let (sink, stream) = socket.split();
            writer_loop(sink, stream, receiver, reader_mux, reader_epoch, epoch).await;
        });
        Self { sender, mux, epoch }
    }

    async fn write(
        &self,
        message: Message,
        frame: crate::socket_mux::FrameHandle,
    ) -> Result<(), WriteFailure> {
        enqueue_with_deadline(&self.sender, message, frame, crate::WRITE_TIMEOUT).await
    }
}

/// Write one queued frame to the socket.
///
/// The frame is taken for transmission before the write begins. A request
/// that gives up while its frame is still queued takes the frame with it,
/// and the writer then drops the frame instead of handing a stale frame to
/// the instance.
async fn write_outbound<S>(sink: &mut S, outbound: Outbound) -> FrameWrite
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    if !outbound.frame.claim() {
        let _ = outbound
            .written
            .send(Err("cancelled before writing".to_owned()));
        return FrameWrite::Skipped;
    }
    let result: std::result::Result<(), String> =
        match timeout(crate::WRITE_TIMEOUT, sink.send(outbound.message)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("timed out writing Foundry websocket request".to_owned()),
        };
    let failed = result.is_err();
    let _ = outbound.written.send(result);
    if failed {
        FrameWrite::Failed
    } else {
        FrameWrite::Written
    }
}

/// The writer task: hand queued frames to the socket and dispatch the
/// answers that come back.
///
/// A frame whose request gave up is dropped, and the writer keeps running:
/// the socket is still usable for the other requests on the connection. Only
/// a failed write ends the writer.
async fn writer_loop<S, R, E>(
    mut sink: S,
    mut stream: R,
    mut receiver: mpsc::Receiver<Outbound>,
    reader_mux: crate::socket_mux::SocketMux,
    reader_epoch: Arc<std::sync::atomic::AtomicU64>,
    epoch: u64,
) where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
    R: futures_util::Stream<Item = std::result::Result<Message, E>> + Unpin,
{
    loop {
        tokio::select! {
            outgoing = receiver.recv() => match outgoing {
                Some(outbound) => {
                    match write_outbound(&mut sink, outbound).await {
                        FrameWrite::Written => {}
                        // The request gave up before the writer took
                        // the frame: the frame was not written and
                        // the socket stays usable.
                        FrameWrite::Skipped => {}
                        FrameWrite::Failed => break,
                    }
                }
                None => break,
            },
            incoming = stream.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    if let Some(pong) = crate::socket_mux::pong_for_ping(text.as_str()) {
                        let pong = timeout(crate::WRITE_TIMEOUT, sink.send(Message::Text(pong.into()))).await;
                        if !matches!(pong, Ok(Ok(()))) { break; }
                    } else if reader_mux.receive(text.as_str()) {
                        invalidate_epoch(&reader_epoch, epoch);
                        break;
                    }
                }
                Some(Ok(_)) => {},
                _ => break,
            }
        }
    }
    invalidate_epoch(&reader_epoch, epoch);
    reader_mux.disconnect();
}

/// Enqueue a frame and wait for the writer to report its outcome.
///
/// The frame is cancelled when this future is dropped before the writer took
/// it, so a request that gives up never leaves a stale frame behind for the
/// writer to send.
async fn enqueue_with_deadline(
    sender: &mpsc::Sender<Outbound>,
    message: Message,
    frame: crate::socket_mux::FrameHandle,
    deadline: Duration,
) -> Result<(), WriteFailure> {
    let guard = FrameGuard(frame.clone());
    let outcome = crate::write_with_deadline(
        async {
            let (written, result) = tokio::sync::oneshot::channel();
            sender
                .send(Outbound {
                    message,
                    written,
                    frame: frame.clone(),
                })
                .await
                .map_err(|_| anyhow!("Foundry websocket writer stopped"))?;
            result
                .await
                .map_err(|_| anyhow!("Foundry websocket writer stopped"))?
                .map_err(|error| anyhow!("{error}"))?;
            Ok::<(), anyhow::Error>(())
        },
        deadline,
        "writing Foundry websocket request",
    )
    .await;
    // The frame is given up before the writer took it, unless the writer
    // already took it: the frame's state decides whether the outcome of the
    // operation is known.
    drop(guard);
    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) | Err(error) => Err(if frame.may_have_been_transmitted() {
            WriteFailure::Transmitted(error)
        } else {
            WriteFailure::NotTransmitted(error)
        }),
    }
}

/// Gives up a frame that was queued but not taken by the writer.
struct FrameGuard(crate::socket_mux::FrameHandle);

impl Drop for FrameGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl Drop for ConnectionManager {
    fn drop(&mut self) {
        self.shutdown_reaper();
    }
}

async fn cleanup_idle_entries(
    connections: &Mutex<HashMap<IdentityKey, Arc<Connection>>>,
    idle_ms: u64,
) {
    let mut entries = connections.lock().await;
    cleanup_idle_locked(&mut entries, idle_ms);
}

fn cleanup_idle_locked(connections: &mut HashMap<IdentityKey, Arc<Connection>>, idle_ms: u64) {
    let now = monotonic_millis();
    let expired: Vec<_> = connections
        .iter()
        .filter_map(|(key, connection)| {
            let idle = now.saturating_sub(connection.last_used.load(Ordering::Relaxed));
            (idle >= idle_ms && safely_idle(connection)).then(|| key.clone())
        })
        .collect();
    for key in expired {
        connections.remove(&key);
    }
}

fn evict_oldest_idle(connections: &mut HashMap<IdentityKey, Arc<Connection>>) {
    let candidate = connections
        .iter()
        .filter(|(_, connection)| safely_idle(connection))
        .min_by_key(|(_, connection)| connection.last_used.load(Ordering::Relaxed))
        .map(|(key, _)| key.clone());
    if let Some(key) = candidate {
        connections.remove(&key);
    }
}

fn safely_idle(connection: &Arc<Connection>) -> bool {
    if Arc::strong_count(connection) != 1 {
        return false;
    }
    connection.transport.try_lock().is_ok()
}

#[cfg(test)]
mod tests {
    use super::{
        enqueue_with_deadline, invalidate_epoch, ConnectionManager, IdentityKey, Outbound,
        Transport,
    };
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::sync::{mpsc, oneshot};
    use tokio_tungstenite::tungstenite::protocol::Message;

    #[tokio::test]
    async fn stale_reader_cannot_invalidate_an_installed_successor() {
        let active_epoch = Arc::new(std::sync::atomic::AtomicU64::new(1));
        let stale_reader_epoch = active_epoch.clone();
        let (frame_seen, stale_frame) = tokio::sync::oneshot::channel();
        let stale_reader = tokio::spawn(async move {
            stale_frame.await.unwrap();
            invalidate_epoch(&stale_reader_epoch, 1);
        });

        active_epoch.store(2, Ordering::Release);
        frame_seen.send(()).unwrap();
        stale_reader.await.unwrap();

        assert_eq!(active_epoch.load(Ordering::Acquire), 2);
        invalidate_epoch(&active_epoch, 2);
        assert_eq!(active_epoch.load(Ordering::Acquire), 0);
    }

    /// A socket that records the frames that reach it.
    #[derive(Clone, Default)]
    struct RecordingSocket(Arc<std::sync::Mutex<Vec<Message>>>);

    impl RecordingSocket {
        fn frames(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|message| match message {
                    Message::Text(text) => text.as_str().to_owned(),
                    other => format!("{other:?}"),
                })
                .collect()
        }
    }

    impl futures_util::Sink<Message> for RecordingSocket {
        type Error = std::io::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(self: std::pin::Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.0.lock().unwrap().push(item);
            Ok(())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// Queue a mutation frame the way a request does, and hand the queued
    /// frame to the caller so that it can act as the writer.
    ///
    /// The frame is queued but not taken: the writer is prevented from
    /// transmitting it, which is the window a cancelled request's frame used
    /// to survive in.
    async fn queue_mutation_frame() -> (
        crate::socket_mux::SocketMux,
        crate::socket_mux::PendingRequest,
        mpsc::Receiver<Outbound>,
        tokio::task::JoinHandle<Result<(), super::WriteFailure>>,
    ) {
        let mux = crate::socket_mux::SocketMux::new(1, 1);
        let (pending, frame) = mux
            .begin(crate::socket_mux::Request::Document {
                document: "Actor".into(),
                action: "update".into(),
                operation: serde_json::json!({"action": "update"}),
            })
            .unwrap();
        let (sender, receiver) = mpsc::channel::<Outbound>(4);
        let request = tokio::spawn({
            let sender = sender.clone();
            let handle = pending.frame();
            async move {
                enqueue_with_deadline(
                    &sender,
                    Message::Text(frame.into()),
                    handle,
                    Duration::from_secs(30),
                )
                .await
            }
        });
        (mux, pending, receiver, request)
    }

    /// A mutation that was queued but not yet written must not reach the
    /// instance when its request is cancelled.
    #[tokio::test]
    async fn a_cancelled_request_never_transmits_its_queued_frame() {
        let (_mux, _pending, mut receiver, request) = queue_mutation_frame().await;
        // The frame was enqueued successfully and the writer has not taken
        // it: the writer is prevented from transmitting it.
        let outbound = receiver.recv().await.expect("the frame was enqueued");

        // The request is cancelled while its frame is still queued.
        request.abort();
        let _ = request.await;

        // The writer takes the frame afterwards: it must drop it instead of
        // handing a stale frame to the instance.
        let socket = RecordingSocket::default();
        let mut sink = socket.clone();
        assert!(
            matches!(
                super::write_outbound(&mut sink, outbound).await,
                super::FrameWrite::Skipped
            ),
            "the writer must skip a frame whose request was cancelled"
        );
        assert!(
            socket.frames().is_empty(),
            "the cancelled frame reached the socket: {:?}",
            socket.frames()
        );
    }

    /// A mutation that was queued but not yet written must not reach the
    /// instance when its connection is invalidated, and the request must be
    /// told that the operation did not happen.
    #[tokio::test]
    async fn an_invalidated_connection_never_transmits_its_queued_frame() {
        let (mux, pending, mut receiver, request) = queue_mutation_frame().await;
        let outbound = receiver.recv().await.expect("the frame was enqueued");

        // The connection is invalidated while the frame is still queued.
        mux.disconnect();

        let socket = RecordingSocket::default();
        let mut sink = socket.clone();
        assert!(
            matches!(
                super::write_outbound(&mut sink, outbound).await,
                super::FrameWrite::Skipped
            ),
            "the writer must skip a frame whose connection was invalidated"
        );
        assert!(
            socket.frames().is_empty(),
            "the stale frame reached the socket: {:?}",
            socket.frames()
        );

        // The request learns that the operation certainly did not happen.
        let failure = request.await.unwrap().unwrap_err();
        assert!(
            !failure.may_have_been_applied(),
            "the frame was never transmitted, so the outcome is known"
        );
        let error = pending.wait().await.unwrap_err();
        assert!(
            !error.is_ambiguous(),
            "the frame was never transmitted, so the outcome is known"
        );
    }

    /// A mutation whose frame the writer already took may have reached the
    /// instance: its outcome stays unknown.
    #[tokio::test]
    async fn a_frame_the_writer_took_may_have_been_applied() {
        let (mux, pending, mut receiver, request) = queue_mutation_frame().await;
        let outbound = receiver.recv().await.expect("the frame was enqueued");

        // The writer takes the frame and writes it to the socket.
        let socket = RecordingSocket::default();
        let mut sink = socket.clone();
        assert!(
            matches!(
                super::write_outbound(&mut sink, outbound).await,
                super::FrameWrite::Written
            ),
            "the writer must write a frame it took"
        );
        assert_eq!(
            socket.frames().len(),
            1,
            "the frame the writer took reaches the socket"
        );
        request.await.unwrap().unwrap();

        // The connection is invalidated before the instance answers: the
        // frame may have been applied, so the outcome stays unknown.
        mux.disconnect();
        let error = pending.wait().await.unwrap_err();
        assert!(
            error.is_ambiguous(),
            "the frame was transmitted, so the outcome is unknown"
        );
        assert!(
            error.message().contains("may have been applied"),
            "{}",
            error.message()
        );
    }

    /// A transport whose queue the test holds: no writer takes the frames, so
    /// a frame stays queued until the test lets a writer take it.
    fn queued_transport(epoch: u64) -> (Transport, mpsc::Receiver<Outbound>) {
        let (sender, receiver) = mpsc::channel::<Outbound>(8);
        (
            Transport {
                sender,
                mux: crate::socket_mux::SocketMux::new(8, 8),
                epoch,
            },
            receiver,
        )
    }

    /// A manager whose identity already owns a transport, so a request runs
    /// on it without connecting.
    async fn manager_with_queued_transport(
        epoch: u64,
    ) -> (
        Arc<ConnectionManager>,
        IdentityKey,
        mpsc::Receiver<Outbound>,
    ) {
        let manager = ConnectionManager::with_limits(1, Duration::from_secs(60));
        let key = IdentityKey::new(&["queued-transport"]);
        let connection = manager.reserve(key.clone()).await.unwrap();
        connection.epoch.store(epoch, Ordering::Release);
        let (transport, receiver) = queued_transport(epoch);
        *connection.transport.lock().await = Some(transport);
        (Arc::new(manager), key, receiver)
    }

    /// A change whose frame never reached the socket is a definite failure,
    /// not an ambiguous one: the caller can tell it apart from a change that
    /// may have been applied.
    #[tokio::test]
    async fn a_change_whose_frame_never_reached_the_socket_is_not_ambiguous() {
        let (manager, key, mut receiver) = manager_with_queued_transport(7).await;
        let request = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .exchange(
                        key,
                        crate::socket_mux::Request::Document {
                            document: "Actor".into(),
                            action: "update".into(),
                            operation: serde_json::json!({"action": "update"}),
                        },
                        "document",
                        || async { Err(anyhow::anyhow!("the transport is already installed")) },
                    )
                    .await
            }
        });
        // The frame is queued and no writer takes it.
        let outbound = receiver.recv().await.expect("the frame was enqueued");
        // The writer goes away without writing: the frame never reaches the
        // socket.
        drop(outbound);

        let error = request.await.unwrap().unwrap_err();
        assert!(
            error.downcast_ref::<crate::AmbiguousMutation>().is_none(),
            "the frame never reached the socket, so the outcome is known: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("not transmitted"),
            "the failure must say the request was not transmitted: {message}"
        );
        assert!(
            !message.contains("may have been applied"),
            "the change did not happen: {message}"
        );
    }

    /// A definite write failure leaves the other requests on the same
    /// connection alone: the shared socket is not torn down for a frame that
    /// never reached it.
    #[tokio::test]
    async fn a_definite_write_failure_leaves_another_request_alone() {
        let (manager, key, mut receiver) = manager_with_queued_transport(9).await;
        // Another request is in flight on the same connection.
        let connection = manager.reserve(key.clone()).await.unwrap();
        let transport = connection.transport.lock().await.clone().unwrap();
        let (pending, _) = transport
            .mux
            .begin(crate::socket_mux::Request::Document {
                document: "Actor".into(),
                action: "get".into(),
                operation: serde_json::json!({"action": "get"}),
            })
            .unwrap();

        let request = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .exchange(
                        key,
                        crate::socket_mux::Request::Document {
                            document: "Actor".into(),
                            action: "update".into(),
                            operation: serde_json::json!({"action": "update"}),
                        },
                        "document",
                        || async { Err(anyhow::anyhow!("the transport is already installed")) },
                    )
                    .await
            }
        });
        let outbound = receiver.recv().await.expect("the frame was enqueued");
        drop(outbound);
        let error = request.await.unwrap().unwrap_err();
        assert!(
            error.downcast_ref::<crate::AmbiguousMutation>().is_none(),
            "the frame never reached the socket: {error}"
        );

        // The other request is still pending and can still be answered: the
        // connection was not torn down.
        assert_eq!(transport.mux.pending_count(), 1);
        let id = pending.id();
        transport.mux.receive(&format!(
            "43{id}[{{\"type\":\"Actor\",\"action\":\"get\",\"result\":[]}}]"
        ));
        assert!(
            pending.wait().await.is_ok(),
            "the other request must still be answered"
        );
    }

    /// A cancelled request takes only its own frame: another request's frame
    /// on the same connection is still written, and the socket stays usable.
    #[tokio::test]
    async fn a_cancelled_request_takes_only_its_own_frame() {
        let mux = crate::socket_mux::SocketMux::new(2, 2);
        let (first, first_text) = mux
            .begin(crate::socket_mux::Request::Document {
                document: "Actor".into(),
                action: "update".into(),
                operation: serde_json::json!({"action": "update"}),
            })
            .unwrap();
        let (second, second_text) = mux
            .begin(crate::socket_mux::Request::Document {
                document: "Item".into(),
                action: "update".into(),
                operation: serde_json::json!({"action": "update"}),
            })
            .unwrap();
        let (sender, mut receiver) = mpsc::channel::<Outbound>(4);

        let first_request = tokio::spawn({
            let sender = sender.clone();
            let frame = first.frame();
            async move {
                enqueue_with_deadline(
                    &sender,
                    Message::Text(first_text.into()),
                    frame,
                    Duration::from_secs(30),
                )
                .await
            }
        });
        let first_outbound = receiver.recv().await.expect("the first frame");
        // The first request gives up while its frame is still queued.
        first_request.abort();
        let _ = first_request.await;

        let second_request = tokio::spawn({
            let sender = sender.clone();
            let frame = second.frame();
            async move {
                enqueue_with_deadline(
                    &sender,
                    Message::Text(second_text.into()),
                    frame,
                    Duration::from_secs(30),
                )
                .await
            }
        });
        let second_outbound = receiver.recv().await.expect("the second frame");

        // The writer takes both frames: the first one is dropped, and the
        // second one is written to the socket, which stays usable.
        let socket = RecordingSocket::default();
        let mut sink = socket.clone();
        assert!(
            matches!(
                super::write_outbound(&mut sink, first_outbound).await,
                super::FrameWrite::Skipped
            ),
            "the writer must skip a frame whose request gave up"
        );
        assert!(
            matches!(
                super::write_outbound(&mut sink, second_outbound).await,
                super::FrameWrite::Written
            ),
            "the writer must write the other request's frame"
        );
        assert_eq!(
            socket.frames().len(),
            1,
            "only the other request's frame reaches the socket"
        );
        second_request.await.unwrap().unwrap();
    }

    /// A write deadline that expires while the frame is still queued is a
    /// definite failure: the frame never reached the socket.
    #[tokio::test]
    async fn a_write_deadline_before_the_writer_took_the_frame_is_definite() {
        let (sender, _receiver) = mpsc::channel::<Outbound>(4);
        let frame = crate::socket_mux::FrameHandle::new();
        let failure = enqueue_with_deadline(
            &sender,
            Message::Text("never-written".into()),
            frame,
            Duration::from_millis(20),
        )
        .await
        .unwrap_err();
        assert!(
            !failure.may_have_been_applied(),
            "the frame never reached the socket, so the outcome is known"
        );
    }

    /// A write deadline that expires after the writer took the frame stays
    /// ambiguous: the frame may have reached the socket.
    #[tokio::test]
    async fn a_write_deadline_after_the_writer_took_the_frame_stays_ambiguous() {
        let (sender, mut receiver) = mpsc::channel::<Outbound>(4);
        let frame = crate::socket_mux::FrameHandle::new();
        let request = tokio::spawn({
            let sender = sender.clone();
            let frame = frame.clone();
            async move {
                enqueue_with_deadline(
                    &sender,
                    Message::Text("written-frame".into()),
                    frame,
                    Duration::from_millis(20),
                )
                .await
            }
        });
        let outbound = receiver.recv().await.expect("the frame was enqueued");
        // The writer took the frame before the deadline expired.
        assert!(
            outbound.frame.claim(),
            "the writer takes the frame for transmission"
        );

        let failure = request.await.unwrap().unwrap_err();
        assert!(
            failure.may_have_been_applied(),
            "the frame was taken for transmission, so the outcome is unknown"
        );
    }

    /// A change whose frame was already on its way keeps the ambiguous
    /// outcome: it may have been applied.
    #[tokio::test]
    async fn a_change_whose_frame_was_already_on_its_way_is_ambiguous() {
        let (manager, key, mut receiver) = manager_with_queued_transport(11).await;
        let request = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .exchange(
                        key,
                        crate::socket_mux::Request::Document {
                            document: "Actor".into(),
                            action: "update".into(),
                            operation: serde_json::json!({"action": "update"}),
                        },
                        "document",
                        || async { Err(anyhow::anyhow!("the transport is already installed")) },
                    )
                    .await
            }
        });
        let outbound = receiver.recv().await.expect("the frame was enqueued");
        // The writer took the frame for transmission, so it may have reached
        // the instance.
        assert!(
            outbound.frame.claim(),
            "the writer takes the frame for transmission"
        );
        drop(outbound);

        let error = request.await.unwrap().unwrap_err();
        assert!(
            error.downcast_ref::<crate::AmbiguousMutation>().is_some(),
            "the frame was on its way, so the outcome is unknown: {error}"
        );
        assert!(
            error.to_string().contains("may have been applied"),
            "{error}"
        );
    }

    /// The world read reports no outcome: it changes nothing, so it is never
    /// an ambiguous mutation.
    #[tokio::test]
    async fn a_world_read_that_never_reached_the_socket_reports_no_outcome() {
        let (manager, key, mut receiver) = manager_with_queued_transport(13).await;
        let request = tokio::spawn({
            let manager = manager.clone();
            async move {
                manager
                    .world(key, || async {
                        Err(anyhow::anyhow!("the transport is already installed"))
                    })
                    .await
            }
        });
        let outbound = receiver.recv().await.expect("the frame was enqueued");
        drop(outbound);

        let error = request.await.unwrap().unwrap_err();
        assert!(
            error.downcast_ref::<crate::AmbiguousMutation>().is_none(),
            "a read changes nothing, so its outcome is not ambiguous: {error}"
        );
        assert!(
            !error.to_string().contains("may have been applied"),
            "{error}"
        );
    }

    /// A frame whose request gave up is dropped, and the writer keeps
    /// running: the socket stays usable for the next frame.
    #[tokio::test]
    async fn a_skipped_frame_keeps_the_writer_running() {
        let (sender, receiver) = mpsc::channel::<Outbound>(4);
        let mux = crate::socket_mux::SocketMux::new(4, 4);
        let socket = RecordingSocket::default();
        let writer = tokio::spawn({
            let sink = socket.clone();
            let epoch = Arc::new(std::sync::atomic::AtomicU64::new(1));
            async move {
                super::writer_loop(
                    sink,
                    futures_util::stream::pending::<std::result::Result<Message, std::io::Error>>(),
                    receiver,
                    mux,
                    epoch,
                    1,
                )
                .await;
            }
        });

        // A frame whose request gave up is dropped, not written.
        let frame = crate::socket_mux::FrameHandle::new();
        let (written, outcome) = oneshot::channel();
        sender
            .send(Outbound {
                message: Message::Text("cancelled-frame".into()),
                written,
                frame: frame.clone(),
            })
            .await
            .unwrap();
        assert!(frame.cancel(), "the request takes its frame");
        assert_eq!(
            outcome.await.expect("the writer reports the skipped frame"),
            Err("cancelled before writing".to_owned())
        );

        // The writer is still running: the next frame reaches the socket.
        let (written, outcome) = oneshot::channel();
        sender
            .send(Outbound {
                message: Message::Text("written-frame".into()),
                written,
                frame: crate::socket_mux::FrameHandle::new(),
            })
            .await
            .unwrap();
        assert_eq!(outcome.await.expect("the writer reports the write"), Ok(()));
        assert_eq!(socket.frames(), vec!["written-frame".to_owned()]);

        drop(sender);
        let _ = writer.await;
    }

    #[tokio::test]
    async fn terminal_connect_error_is_attempted_once() {
        let policy = super::ReconnectPolicy {
            attempts: 3,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            max_jitter: Duration::ZERO,
            attempt_timeout: Duration::from_secs(1),
            total_timeout: Duration::from_secs(1),
            world_gate_timeout: Duration::from_secs(30),
        };
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let mut connect = || {
            observed.fetch_add(1, Ordering::Relaxed);
            async {
                Err(super::mark_terminal(anyhow::anyhow!(
                    "Foundry authentication failed"
                )))
            }
        };
        let result: std::result::Result<(), anyhow::Error> =
            super::retry_connect(&mut connect, policy, |_| Duration::ZERO).await;

        assert_eq!(
            result.err().unwrap().to_string(),
            "Foundry authentication failed"
        );
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_recovers_after_transient_failure() {
        let policy = super::ReconnectPolicy {
            attempts: 3,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(2),
            max_jitter: Duration::ZERO,
            attempt_timeout: Duration::from_secs(2),
            total_timeout: Duration::from_secs(5),
            world_gate_timeout: Duration::from_secs(30),
        };
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = attempts.clone();
        let mut connect = || {
            let attempt = observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                if attempt == 0 {
                    Err(anyhow::anyhow!("transient connect failure"))
                } else {
                    Ok(attempt)
                }
            }
        };
        let started = tokio::time::Instant::now();
        let result: std::result::Result<usize, anyhow::Error> =
            super::retry_connect(&mut connect, policy, |_| Duration::ZERO).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(started.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn slow_successful_attempt_can_finish_within_its_budget() {
        let policy = super::ReconnectPolicy {
            attempts: 3,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(2),
            max_jitter: Duration::ZERO,
            attempt_timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(10),
            world_gate_timeout: Duration::from_secs(30),
        };
        let mut connect = || async {
            tokio::time::sleep(Duration::from_secs(4)).await;
            Ok::<_, anyhow::Error>("connected")
        };
        let started = tokio::time::Instant::now();
        let result = super::retry_connect(&mut connect, policy, |_| Duration::ZERO).await;

        assert_eq!(result.unwrap(), "connected");
        assert_eq!(started.elapsed(), Duration::from_secs(4));
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_timeout_allows_retry_before_total_deadline() {
        let policy = super::ReconnectPolicy {
            attempts: 3,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(2),
            max_jitter: Duration::ZERO,
            attempt_timeout: Duration::from_secs(2),
            total_timeout: Duration::from_secs(7),
            world_gate_timeout: Duration::from_secs(30),
        };
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let mut connect = || {
            let attempt = observed.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt == 0 {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    Err(anyhow::anyhow!("slow transient failure"))
                } else {
                    Ok(attempt)
                }
            }
        };
        let started = tokio::time::Instant::now();
        let result = super::retry_connect(&mut connect, policy, |_| Duration::ZERO).await;

        assert_eq!(result.unwrap(), 1);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(started.elapsed(), Duration::from_secs(3));
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_attempts_and_backoff_share_an_aggregate_deadline() {
        let policy = super::ReconnectPolicy {
            attempts: 5,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(2),
            max_jitter: Duration::ZERO,
            attempt_timeout: Duration::from_secs(2),
            total_timeout: Duration::from_millis(2500),
            world_gate_timeout: Duration::from_secs(30),
        };
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let started = tokio::time::Instant::now();
        let mut connect = || {
            observed.fetch_add(1, Ordering::Relaxed);
            async { Err(anyhow::anyhow!("transient connect failure")) }
        };
        let result: std::result::Result<(), anyhow::Error> =
            super::retry_connect(&mut connect, policy, |_| Duration::ZERO).await;

        let error = result.err().unwrap().to_string();
        assert!(
            error.contains("Aggregate Foundry reconnect deadline expired"),
            "{error}"
        );
        assert!(
            error.contains("last error: transient connect failure"),
            "{error}"
        );
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(started.elapsed(), Duration::from_millis(2500));
    }

    #[test]
    fn credential_keys_are_stable_and_secret_sensitive() {
        assert!(
            IdentityKey::new(&["host", "user", "secret"])
                == IdentityKey::new(&["host", "user", "secret"])
        );
        assert!(
            IdentityKey::new(&["host", "user", "secret"])
                != IdentityKey::new(&["host", "user", "other"])
        );
        assert!(IdentityKey::new(&["ab", "c"]) != IdentityKey::new(&["a", "bc"]));
        assert!(
            IdentityKey::new(&["config-a", "host", "user", "secret"])
                != IdentityKey::new(&["config-b", "host", "user", "secret"])
        );
    }

    #[tokio::test]
    async fn enqueue_deadline_includes_sender_backpressure() {
        let (sender, _receiver) = mpsc::channel(1);
        let (written, _) = oneshot::channel();
        sender
            .send(Outbound {
                message: Message::Text("occupies-channel".into()),
                written,
                frame: super::super::socket_mux::FrameHandle::new(),
            })
            .await
            .unwrap();

        let error = enqueue_with_deadline(
            &sender,
            Message::Text("blocked-by-full-channel".into()),
            super::super::socket_mux::FrameHandle::new(),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("writing Foundry websocket request"));
        assert!(
            !error.may_have_been_applied(),
            "the frame never reached the socket, so the outcome is known"
        );
    }

    #[tokio::test]
    async fn a_cancelled_enqueue_drops_its_frame_instead_of_delivering_it() {
        // The outbound queue is where a cancelled request's frame would
        // otherwise still be handed to the socket: the frame must go with
        // the cancelled request, not with the writer.
        let (sender, mut receiver) = mpsc::channel(1);
        let (written, _) = oneshot::channel();
        sender
            .send(Outbound {
                message: Message::Text("occupies-channel".into()),
                written,
                frame: super::super::socket_mux::FrameHandle::new(),
            })
            .await
            .unwrap();

        let sender = sender.clone();
        let (started, started_rx) = oneshot::channel();
        let mut task = tokio::spawn(async move {
            // The signal fires before the enqueue is attempted, so the test
            // can tell "the enqueue was waiting for room" apart from "the
            // enqueue never ran".
            let _ = started.send(());
            enqueue_with_deadline(
                &sender,
                Message::Text("cancelled-frame".into()),
                super::super::socket_mux::FrameHandle::new(),
                Duration::from_secs(30),
            )
            .await
        });
        // Let the enqueue reach the blocked hand-over, then cancel it. The
        // queue is full, so the enqueue cannot finish: it is waiting for
        // room, and the abort cancels it there.
        started_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut task)
                .await
                .is_err(),
            "the enqueue must still be waiting for room in the queue"
        );
        task.abort();
        let _ = task.await;

        let received = receiver.recv().await.unwrap();
        let Message::Text(text) = &received.message else {
            panic!("expected a text frame");
        };
        assert_eq!(
            text.as_str(),
            "occupies-channel",
            "the queued frame must be the first one"
        );
        assert!(
            receiver.try_recv().is_err(),
            "the cancelled frame must never reach the writer"
        );
    }

    #[tokio::test]
    async fn manager_capacity_evicts_idle_entries_and_stays_bounded() {
        let manager = ConnectionManager::with_limits(2, Duration::from_secs(60));
        manager.reserve(IdentityKey::new(&["one"])).await.unwrap();
        manager.reserve(IdentityKey::new(&["two"])).await.unwrap();
        manager.reserve(IdentityKey::new(&["three"])).await.unwrap();
        assert_eq!(manager.connections.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn idle_cleanup_removes_expired_entries() {
        let manager = ConnectionManager::with_limits(4, Duration::from_millis(1));
        let connection = manager.reserve(IdentityKey::new(&["idle"])).await.unwrap();
        connection.last_used.store(
            super::monotonic_millis().saturating_sub(10),
            std::sync::atomic::Ordering::Relaxed,
        );
        drop(connection);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(manager.connections.lock().await.is_empty());
    }

    #[tokio::test]
    async fn capacity_cleanup_never_evicts_an_in_flight_connection() {
        let manager = ConnectionManager::with_limits(1, Duration::from_millis(1));
        let active = manager
            .reserve(IdentityKey::new(&["active"]))
            .await
            .unwrap();
        let socket_guard = active.transport.lock().await;
        active.last_used.store(
            super::monotonic_millis().saturating_sub(10),
            std::sync::atomic::Ordering::Relaxed,
        );
        assert!(manager.reserve(IdentityKey::new(&["next"])).await.is_err());
        assert_eq!(manager.connections.lock().await.len(), 1);
        drop(socket_guard);
        drop(active);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(manager.connections.lock().await.is_empty());
    }
}
