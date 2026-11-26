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
        if let Err(error) = transport.write(Message::Text(frame.into())).await {
            transport.mux.disconnect();
            invalidate_epoch(&connection.epoch, transport.epoch);
            in_flight.3 = false;
            let message = format!(
                "Failed writing Foundry {label} request{}: {error}",
                ambiguity(mutation)
            );
            return Err(if mutation {
                crate::ambiguous_mutation(message)
            } else {
                anyhow!(message)
            });
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
                Err(if mutation {
                    crate::ambiguous_mutation(error.message().to_owned())
                } else {
                    anyhow!(error.message().to_owned())
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
                transport.write(Message::Text(frame.into())).await
            } else {
                Err(anyhow!(
                    "Foundry world request was cancelled before writing"
                ))
            };
            if let Err(error) = write {
                transport.mux.disconnect();
                invalidate_epoch(&connection.epoch, transport.epoch);
                in_flight.3 = false;
                return Err(anyhow!("Failed writing Foundry world request: {error}"));
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
        let (sender, mut receiver) = mpsc::channel::<Outbound>(64);
        let mux = crate::socket_mux::SocketMux::new(128, 64);
        let reader_mux = mux.clone();
        let reader_epoch = active_epoch.clone();
        tokio::spawn(async move {
            let (mut sink, mut stream) = socket.split();
            loop {
                tokio::select! {
                    outgoing = receiver.recv() => match outgoing {
                        Some(outbound) => {
                            let result = match timeout(crate::WRITE_TIMEOUT, sink.send(outbound.message)).await {
                                Ok(Ok(())) => Ok(()),
                                Ok(Err(error)) => Err(error.to_string()),
                                Err(_) => Err("timed out writing Foundry websocket request".to_owned()),
                            };
                            let failed = result.is_err();
                            let _ = outbound.written.send(result);
                            if failed { break; }
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
        });
        Self { sender, mux, epoch }
    }

    async fn write(&self, message: Message) -> Result<()> {
        enqueue_with_deadline(&self.sender, message, crate::WRITE_TIMEOUT).await
    }
}

async fn enqueue_with_deadline(
    sender: &mpsc::Sender<Outbound>,
    message: Message,
    deadline: Duration,
) -> Result<()> {
    crate::write_with_deadline(
        async {
            let (written, result) = tokio::sync::oneshot::channel();
            sender
                .send(Outbound { message, written })
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
    .await?
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
            })
            .await
            .unwrap();

        let error = enqueue_with_deadline(
            &sender,
            Message::Text("blocked-by-full-channel".into()),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("writing Foundry websocket request"));
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
