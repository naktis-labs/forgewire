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
    invalid: Arc<AtomicBool>,
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
}

pub(crate) struct ConnectionManager {
    // The global map lock is never held over socket I/O; each identity has its
    // own transport lock; one reader task dispatches correlated responses.
    connections: Arc<Mutex<HashMap<IdentityKey, Arc<Connection>>>>,
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

fn monotonic_millis() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

struct InFlight<'a>(&'a AtomicBool, &'a std::sync::atomic::AtomicU64, bool);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if self.2 {
            // Cancellation can happen after send. Force the next caller to
            // discard this stream rather than consume an uncorrelated late 430.
            self.0.store(true, Ordering::Release);
            self.1.store(monotonic_millis(), Ordering::Relaxed);
        }
    }
}

impl ConnectionManager {
    fn with_limits(capacity: usize, idle_timeout: Duration) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
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
            invalid: Arc::new(AtomicBool::new(false)),
            last_used: std::sync::atomic::AtomicU64::new(monotonic_millis()),
        });
        connections.insert(key, connection.clone());
        Ok(connection)
    }

    pub(crate) async fn world<F, Fut>(&self, key: IdentityKey, connect: F) -> Result<Value>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Socket>>,
    {
        let connection = self.reserve(key).await?;
        let mut slot = connection.transport.lock().await;
        if connection.invalid.swap(false, Ordering::AcqRel) {
            slot.take();
        }
        if slot.is_none() {
            *slot = Some(Transport::start(
                connect().await?,
                connection.invalid.clone(),
            ));
        }
        let (pending, frame) = match slot
            .as_ref()
            .unwrap()
            .mux
            .begin(crate::socket_mux::Request::World)
        {
            Ok(request) => request,
            Err(error) => {
                slot.take();
                return Err(anyhow!(error));
            }
        };
        let transport = slot.as_ref().unwrap().clone();
        drop(slot);
        let mut in_flight = InFlight(&connection.invalid, &connection.last_used, true);
        let write = if transport.mux.mark_sent(pending.id()) {
            transport.write(Message::Text(frame.into())).await
        } else {
            Err(anyhow!(
                "Foundry world request was cancelled before writing"
            ))
        };
        if let Err(error) = write {
            transport.mux.disconnect();
            connection.invalid.store(true, Ordering::Release);
            in_flight.2 = false;
            return Err(anyhow!("Failed writing Foundry world request: {error}"));
        }
        match timeout(crate::RESPONSE_TIMEOUT, pending.wait()).await {
            Ok(Ok(value)) => {
                connection
                    .last_used
                    .store(monotonic_millis(), Ordering::Relaxed);
                in_flight.2 = false;
                Ok(value)
            }
            Ok(Err(error)) => {
                transport.mux.disconnect();
                connection.invalid.store(true, Ordering::Release);
                in_flight.2 = false;
                Err(anyhow!(error))
            }
            Err(_) => {
                transport.mux.disconnect();
                connection.invalid.store(true, Ordering::Release);
                in_flight.2 = false;
                Err(anyhow!("Timed out waiting for Foundry world response"))
            }
        }
    }

    pub(crate) async fn document_get<F, Fut>(
        &self,
        key: IdentityKey,
        document: String,
        operation: Value,
        connect: F,
    ) -> Result<Value>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Socket>>,
    {
        let connection = self.reserve(key).await?;
        let mut slot = connection.transport.lock().await;
        if connection.invalid.swap(false, Ordering::AcqRel) {
            slot.take();
        }
        if slot.is_none() {
            *slot = Some(Transport::start(
                connect().await?,
                connection.invalid.clone(),
            ));
        }
        let (pending, frame) =
            match slot
                .as_ref()
                .unwrap()
                .mux
                .begin(crate::socket_mux::Request::Document {
                    document,
                    action: "get".into(),
                    operation,
                }) {
                Ok(request) => request,
                Err(error) => {
                    slot.take();
                    return Err(anyhow!(error));
                }
            };
        let transport = slot.as_ref().unwrap().clone();
        drop(slot);
        let mut in_flight = InFlight(&connection.invalid, &connection.last_used, true);
        let write = if transport.mux.mark_sent(pending.id()) {
            transport.write(Message::Text(frame.into())).await
        } else {
            Err(anyhow!(
                "Foundry document request was cancelled before writing"
            ))
        };
        if let Err(error) = write {
            transport.mux.disconnect();
            connection.invalid.store(true, Ordering::Release);
            in_flight.2 = false;
            return Err(anyhow!("Failed writing Foundry document request: {error}"));
        }
        match timeout(crate::RESPONSE_TIMEOUT, pending.wait()).await {
            Ok(Ok(response)) => {
                let result = response
                    .get("result")
                    .filter(|value| value.is_array())
                    .cloned()
                    .ok_or_else(|| anyhow!("Foundry document response has invalid result"));
                connection
                    .last_used
                    .store(monotonic_millis(), Ordering::Relaxed);
                in_flight.2 = false;
                result
            }
            Ok(Err(error)) => {
                transport.mux.disconnect();
                connection.invalid.store(true, Ordering::Release);
                in_flight.2 = false;
                Err(anyhow!(error))
            }
            Err(_) => {
                transport.mux.disconnect();
                connection.invalid.store(true, Ordering::Release);
                in_flight.2 = false;
                Err(anyhow!("Timed out waiting for Foundry document response"))
            }
        }
    }
}

impl Transport {
    fn start(socket: Socket, invalid: Arc<AtomicBool>) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Outbound>(64);
        let mux = crate::socket_mux::SocketMux::new(128, 64);
        let reader_mux = mux.clone();
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
                                invalid.store(true, Ordering::Release);
                                break;
                            }
                        }
                        Some(Ok(_)) => {},
                        _ => break,
                    }
                }
            }
            invalid.store(true, Ordering::Release);
            reader_mux.disconnect();
        });
        Self { sender, mux }
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
    use super::{enqueue_with_deadline, ConnectionManager, IdentityKey, Outbound};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use tokio_tungstenite::tungstenite::protocol::Message;

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
