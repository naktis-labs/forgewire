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

pub mod compendium;
pub mod files;
pub mod read_ops;

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
        bail!("Foundry document response has unexpected action, and the operation may have been applied");
    }
    let result = response
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            anyhow!("Foundry document response has invalid result, and the operation may have been applied")
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
                    bail!("Foundry create response is incomplete, and the operation may have been applied");
                }
            }
            if result.len() != requested.len() {
                bail!("Foundry create response is incomplete, and the operation may have been applied");
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
                    anyhow!("Foundry {action} response has malformed document id, and the operation may have been applied")
                })?;
                if !seen.insert(id.to_owned()) {
                    bail!("Foundry {action} response has duplicate document id, and the operation may have been applied");
                }
            }
            if seen.len() != requested.len() {
                bail!("Foundry {action} response has unexpected document id, and the operation may have been applied");
            }
            for value in requested {
                let id = if action == "update" {
                    value.get("_id").and_then(Value::as_str)
                } else {
                    value.as_str()
                }
                .ok_or_else(|| anyhow!("invalid requested document id"))?;
                if !seen.contains(id) {
                    bail!("Foundry {action} response is missing requested document id, and the operation may have been applied");
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn document_ack_response(
    frame: &str,
    id: u64,
    document: &str,
    action: &str,
) -> Result<Option<Value>> {
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
    if response
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind != document)
    {
        return Ok(None);
    }
    let applied = if action == "get" {
        String::new()
    } else {
        ", and the operation may have been applied".to_string()
    };
    if let Some(error) = response.get("error").filter(|error| !error.is_null()) {
        bail!(
            "Foundry document request failed{}: {}",
            applied,
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
        );
    }
    if response.get("type").and_then(Value::as_str) != Some(document) {
        return Ok(None);
    }
    if !response.get("result").is_some_and(Value::is_array) {
        bail!("Foundry document response has invalid result{applied}");
    }
    Ok(Some(response.clone()))
}

#[cfg(test)]
fn document_ack(frame: &str, id: u64, document: &str) -> Result<Option<Value>> {
    Ok(document_ack_response(frame, id, document, "get")?
        .map(|response| response["result"].clone()))
}

#[cfg(test)]
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
    let meta = result
        .get("world")
        .or_else(|| result.get("meta"))
        .unwrap_or(result);
    WorldInfo {
        title: meta.get("title").and_then(Value::as_str).map(str::to_owned),
        system: meta
            .get("system")
            .and_then(Value::as_str)
            .map(str::to_owned),
        version: meta
            .get("coreVersion")
            .or_else(|| meta.get("version"))
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
        operation: Value,
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
        let action = operation
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing document action"))?;
        let payload =
            json!(["modifyDocument", {"type": document, "action": action, "operation": operation}]);
        socket
            .send(Message::Text(format!("42{ack}{payload}").into()))
            .await
            .map_err(|_| anyhow!("Failed sending Foundry document request"))?;
        timeout(RESPONSE_TIMEOUT, async {
            let applied = if action == "get" {
                String::new()
            } else {
                ", and the operation may have been applied".to_string()
            };
            loop {
                let frame = socket
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("Foundry socket closed{applied}"))?
                    .map_err(|_| anyhow!("Foundry socket read failed{applied}"))?;
                if let Message::Text(text) = frame {
                    if text == "2" {
                        socket
                            .send(Message::Text("3".into()))
                            .await
                            .map_err(|_| anyhow!("Foundry pong failed"))?;
                        continue;
                    }
                    if let Some(response) =
                        document_ack_response(text.as_str(), ack, document, action)?
                    {
                        if action != "get" {
                            let key = match action {
                                "create" => "data",
                                "update" => "updates",
                                "delete" => "ids",
                                _ => bail!("unsupported document action"),
                            };
                            validate_write_ack(&response, action, &operation[key])?;
                        }
                        return Ok(response["result"].clone());
                    }
                }
            }
        })
        .await
        .map_err(|_| {
            if action == "get" {
                anyhow!("Timed out waiting for Foundry document response")
            } else {
                anyhow!("Timed out waiting for Foundry document response, and the operation may have been applied")
            }
        })?
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
mod tests {
    use super::*;
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
    fn type_mismatch_keeps_waiting_and_read_errors_do_not_claim_applied() {
        assert!(
            document_ack_response("431[{\"type\":\"Item\",\"result\":[]}]", 1, "Actor", "get")
                .unwrap()
                .is_none()
        );
        let read_error = document_ack_response(
            "431[{\"type\":\"Actor\",\"error\":{\"message\":\"denied\"}}]",
            1,
            "Actor",
            "get",
        )
        .unwrap_err()
        .to_string();
        assert!(
            !read_error.contains("may have been applied"),
            "{read_error}"
        );
        let write_error = document_ack_response(
            "431[{\"type\":\"Actor\",\"error\":{\"message\":\"denied\"}}]",
            1,
            "Actor",
            "update",
        )
        .unwrap_err()
        .to_string();
        assert!(
            write_error.contains("may have been applied"),
            "{write_error}"
        );
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
    fn write_ack_reports_errors_without_type_and_rejects_invalid_result() {
        assert!(document_ack("431[{\"error\":{\"message\":\"denied\"}}]", 1, "Actor").is_err());
        assert!(document_ack("431[{\"type\":\"Actor\",\"result\":null}]", 1, "Actor").is_err());
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
    fn preserves_foundry_decimal_in_document_ack() {
        let frame = r#"431[{"type":"Actor","result":[{"scaleX":0.9615384615384615}]}]"#;
        let result = document_ack(frame, 1, "Actor").unwrap().unwrap();
        assert_eq!(result.to_string(), r#"[{"scaleX":0.9615384615384615}]"#);
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
