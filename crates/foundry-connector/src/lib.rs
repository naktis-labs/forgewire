use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::header::SET_COOKIE;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, protocol::Message},
};
use url::Url;

#[derive(Clone, Debug, Deserialize)]
pub struct FoundryCredential {
    pub id: Option<String>,
    pub hostname: String,
    pub userid: Option<String>,
    pub username: Option<String>,
    pub password: String,
}

const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const SOCKET_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_TIMEOUT: Duration = Duration::from_secs(10);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct FoundryClient {
    http: reqwest::Client,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorldInfo {
    pub title: Option<String>,
    pub system: Option<String>,
    pub version: Option<String>,
}

fn generate_session_id() -> String {
    use rand::RngCore;
    let mut bytes = [0_u8; 12];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn validate_identity(credential: &FoundryCredential) -> Result<()> {
    if !credential
        .userid
        .as_deref()
        .is_some_and(|v| !v.trim().is_empty())
        || !credential
            .username
            .as_deref()
            .is_some_and(|v| !v.trim().is_empty())
    {
        bail!("Foundry userid and username are both required");
    }
    Ok(())
}

fn join_payload(credential: &FoundryCredential) -> Value {
    let mut payload = json!({
        "userid": credential.userid,
        "userId": credential.userid,
        "password": credential.password,
        "action": "join"
    });
    if let Some(username) = &credential.username {
        payload["username"] = json!(username);
    }
    payload
}

fn socket_request(
    host: &str,
    session: &str,
    generation: Option<u64>,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    let mut url = FoundryClient::host_url(host, "/socket.io/?EIO=4&transport=websocket")?;
    let is_modern = generation.is_some_and(|g| g >= 14);
    if !is_modern {
        url.query_pairs_mut().append_pair("session", session);
    }
    url.set_scheme("wss")
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

async fn wait_for_session<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let frame = socket
            .next()
            .await
            .ok_or_else(|| anyhow!("Foundry socket closed"))?
            .map_err(|_| anyhow!("Foundry socket read failed"))?;
        if let Message::Text(text) = frame {
            let text = text.as_str();
            if text.starts_with("0{") {
                socket
                    .send(Message::Text("40".into()))
                    .await
                    .map_err(|_| anyhow!("Foundry namespace connect failed"))?;
            } else if text == "2" {
                socket
                    .send(Message::Text("3".into()))
                    .await
                    .map_err(|_| anyhow!("Foundry pong failed"))?;
            } else if let Some(binding) = session_event(text) {
                binding?;
                return Ok(());
            }
        }
    }
}

fn session_event(frame: &str) -> Option<Result<()>> {
    let start = frame.find('[')?;
    let payload: Value = serde_json::from_str(&frame[start..]).ok()?;
    if payload.get(0)?.as_str()? != "session" {
        return None;
    }
    Some(
        if payload
            .get(1)
            .and_then(|v| v.get("sessionId"))
            .and_then(Value::as_str)
            .is_some()
        {
            Ok(())
        } else {
            Err(anyhow!("Foundry websocket session binding failed"))
        },
    )
}

fn document_ack(frame: &str, id: u64, document: &str) -> Result<Option<Value>> {
    let expected = format!("43{id}[");
    if !frame.starts_with(&expected) {
        return Ok(None);
    }
    let start = frame
        .find('[')
        .ok_or_else(|| anyhow!("invalid Foundry ACK"))?;
    let values: Value = serde_json::from_str(&frame[start..])
        .map_err(|_| anyhow!("invalid Foundry ACK payload"))?;
    let response = values.get(0).ok_or_else(|| anyhow!("empty Foundry ACK"))?;
    if response.get("type").and_then(Value::as_str) != Some(document) {
        return Ok(None);
    }
    if let Some(error) = response.get("error") {
        bail!(
            "Foundry document request failed: {}",
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
        );
    }
    if !response.get("result").is_some_and(Value::is_array) {
        return Ok(None);
    }
    Ok(response.get("result").cloned())
}

fn document_request(ack: u64, document: &str, query: Value) -> String {
    let payload = json!(["modifyDocument", {"type": document, "action": "get", "operation": {"action":"get", "query": query, "index": false, "broadcast": false}}]);
    format!("42{ack}{payload}")
}

fn world_response(frame: &str) -> Result<Value> {
    if !frame.starts_with("430") {
        bail!("not a Foundry world response");
    }
    let values: Value =
        serde_json::from_str(&frame[3..]).map_err(|_| anyhow!("invalid Foundry world response"))?;
    values
        .get(0)
        .filter(|v| v.is_object())
        .cloned()
        .ok_or_else(|| anyhow!("Foundry world response did not contain metadata"))
}

fn world_info(result: &Value) -> WorldInfo {
    let meta = result.get("meta").unwrap_or(result);
    WorldInfo {
        title: meta.get("title").and_then(Value::as_str).map(str::to_owned),
        system: meta
            .get("system")
            .and_then(Value::as_str)
            .map(str::to_owned),
        version: meta
            .get("version")
            .and_then(Value::as_str)
            .map(str::to_owned),
    }
}

impl FoundryClient {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .https_only(true)
                .redirect(reqwest::redirect::Policy::none())
                .timeout(HTTP_TIMEOUT)
                .build()?,
        })
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

    async fn session(&self, credential: &FoundryCredential) -> Result<(String, Option<u64>)> {
        validate_identity(credential)?;
        let status = match timeout(
            HTTP_TIMEOUT,
            self.http
                .get(Self::host_url(&credential.hostname, "/api/status")?)
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
        let join_url = Self::host_url(&credential.hostname, "/join")?;
        let response = self
            .http
            .get(join_url.clone())
            .send()
            .await
            .map_err(|_| anyhow!("Foundry session request failed or timed out"))?;
        if !response.status().is_success() {
            bail!("Foundry session request returned an unsuccessful status");
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
        let payload = join_payload(credential);
        let auth = self
            .http
            .post(join_url)
            .header("cookie", format!("session={session}"))
            .header(
                "origin",
                format!(
                    "https://{}",
                    credential.hostname.trim_start_matches("https://")
                ),
            )
            .json(&payload)
            .send()
            .await
            .map_err(|_| anyhow!("Foundry authentication request failed or timed out"))?;
        let body: Value = if auth.status().is_success() {
            auth.json().await.unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        if body.get("status").and_then(Value::as_str) != Some("success") {
            bail!("Foundry authentication failed");
        }
        Ok((session, generation))
    }

    async fn request(
        &self,
        credential: &FoundryCredential,
        document: &str,
        query: Value,
    ) -> Result<Value> {
        let (session, generation) = self.session(credential).await?;
        let request = socket_request(&credential.hostname, &session, generation)?;
        let (mut socket, _) = timeout(SOCKET_CONNECT_TIMEOUT, connect_async(request))
            .await
            .map_err(|_| anyhow!("Foundry websocket connection timed out"))?
            .map_err(|_| anyhow!("Foundry websocket connection failed"))?;
        timeout(SESSION_TIMEOUT, wait_for_session(&mut socket))
            .await
            .map_err(|_| anyhow!("Timed out waiting for Foundry session binding"))??;
        if document == "World" {
            socket
                .send(Message::Text("420[\"world\"]".into()))
                .await
                .map_err(|_| anyhow!("Failed sending Foundry world request"))?;
            return timeout(RESPONSE_TIMEOUT, async {
                loop {
                    let frame = socket
                        .next()
                        .await
                        .ok_or_else(|| anyhow!("Foundry socket closed"))?
                        .map_err(|_| anyhow!("Foundry socket read failed"))?;
                    if let Message::Text(text) = frame {
                        if text == "2" {
                            socket
                                .send(Message::Text("3".into()))
                                .await
                                .map_err(|_| anyhow!("Foundry pong failed"))?;
                        } else if text.starts_with("430") {
                            return world_response(text.as_str());
                        }
                    }
                }
            })
            .await
            .map_err(|_| anyhow!("Timed out waiting for Foundry world response"))?;
        }
        let ack = 1;
        socket
            .send(Message::Text(document_request(ack, document, query).into()))
            .await
            .map_err(|_| anyhow!("Failed sending Foundry document request"))?;
        timeout(RESPONSE_TIMEOUT, async {
            loop {
                let frame = socket
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("Foundry socket closed"))?
                    .map_err(|_| anyhow!("Foundry socket read failed"))?;
                if let Message::Text(text) = frame {
                    if text == "2" {
                        socket
                            .send(Message::Text("3".into()))
                            .await
                            .map_err(|_| anyhow!("Foundry pong failed"))?;
                        continue;
                    }
                    if let Some(value) = document_ack(text.as_str(), ack, document)? {
                        return Ok(value);
                    }
                }
            }
        })
        .await
        .map_err(|_| anyhow!("Timed out waiting for Foundry document response"))?
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
        self.request(credential, kind, json!({})).await
    }

    pub async fn world(&self, credential: &FoundryCredential) -> Result<WorldInfo> {
        let result = self.request(credential, "World", json!({})).await?;
        Ok(world_info(&result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert!(validate_identity(&credential).is_ok());
        assert_eq!(
            join_payload(&credential),
            json!({"userid":"u1","userId":"u1","username":"Player","password":"secret","action":"join"})
        );
    }

    #[test]
    fn rejects_missing_or_blank_foundry_userid_and_username() {
        let base = FoundryCredential {
            id: None,
            hostname: "example.org".into(),
            userid: Some("u1".into()),
            username: Some("Player".into()),
            password: "secret".into(),
        };
        assert!(validate_identity(&FoundryCredential {
            userid: None,
            ..base.clone()
        })
        .is_err());
        assert!(validate_identity(&FoundryCredential {
            username: Some("  ".into()),
            ..base
        })
        .is_err());
    }

    #[test]
    fn recognizes_session_event_and_fails_on_null_binding() {
        assert!(matches!(
            session_event("42[\"session\",{\"sessionId\":\"s\",\"userId\":\"u\"}]"),
            Some(Ok(()))
        ));
        assert!(session_event("42[\"session\",null]").unwrap().is_err());
        assert!(session_event("42[\"other\",null]").is_none());
    }

    #[test]
    fn emits_upstream_modify_document_operation_shape() {
        let message = document_request(17, "Actor", json!({"folder":"f1"}));
        assert_eq!(message, "4217[\"modifyDocument\",{\"action\":\"get\",\"operation\":{\"action\":\"get\",\"broadcast\":false,\"index\":false,\"query\":{\"folder\":\"f1\"}},\"type\":\"Actor\"}]");
    }

    #[test]
    fn parses_ack_only_for_matching_id_and_extracts_result() {
        assert_eq!(
            document_ack(
                "431[ {\"type\":\"Actor\",\"result\":[{\"_id\":\"a\"}]}]",
                1,
                "Actor"
            )
            .unwrap(),
            Some(json!([{"_id":"a"}]))
        );
        assert_eq!(
            document_ack("432[{\"type\":\"Actor\",\"result\":[]}]", 1, "Actor").unwrap(),
            None
        );
        assert!(document_ack(
            "431[{\"type\":\"Actor\",\"error\":{\"message\":\"denied\"}}]",
            1,
            "Actor"
        )
        .is_err());
    }

    #[test]
    fn parses_upstream_world_response_and_world_metadata() {
        let world = world_response(
            "430[{\"meta\":{\"title\":\"My World\",\"system\":\"dnd5e\",\"version\":\"1.0\"}}]",
        )
        .unwrap();
        assert_eq!(
            world,
            json!({"meta":{"title":"My World","system":"dnd5e","version":"1.0"}})
        );
        let info = world_info(&world);
        assert_eq!(info.title.as_deref(), Some("My World"));
        assert!(world_response("430[]").is_err());
    }
}
