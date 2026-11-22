use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use serde_json::{json, Value};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::{
    socket_request, wait_for_session, FoundryClient, FoundryCredential, RESPONSE_TIMEOUT,
    SESSION_TIMEOUT, SOCKET_CONNECT_TIMEOUT,
};

pub const MAX_UPLOAD_BYTES: usize = 10 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const IMAGE_EXTENSIONS: &[&str] = &[
    ".apng", ".avif", ".bmp", ".gif", ".jpeg", ".jpg", ".png", ".svg", ".tiff", ".webp",
];

fn browse_request(
    id: u64,
    target: &str,
    kind: Option<&str>,
    extensions: Option<&[String]>,
) -> String {
    let extensions = extensions
        .map(|v| json!(v))
        .unwrap_or_else(|| json!(IMAGE_EXTENSIONS));
    format!(
        "42{id}{}",
        json!(["manageFiles", {"action":"browseFiles", "storage":"data", "target":target}, {"type":kind.unwrap_or("image"), "extensions":extensions, "wildcard":false, "render":true}])
    )
}

fn browse_ack(frame: &str, id: u64) -> Result<Option<Value>> {
    let prefix = format!("43{id}[");
    if !frame.starts_with(&prefix) {
        return Ok(None);
    }
    let values: Value = serde_json::from_str(&frame[prefix.len() - 1..])
        .map_err(|_| anyhow!("invalid Foundry browse ACK"))?;
    let response = values
        .get(0)
        .filter(|v| v.is_object())
        .ok_or_else(|| anyhow!("empty Foundry browse ACK"))?;
    if let Some(error) = response
        .get("error")
        .filter(|v| !v.is_null() && *v != &json!(false))
    {
        bail!("Foundry browse failed: {error}");
    }
    if !response.get("dirs").is_some_and(Value::is_array) {
        bail!("invalid Foundry browse response");
    }
    Ok(Some(response.clone()))
}

fn validate_upload(target: &str, filename: &str, size: usize) -> Result<()> {
    if target.len() > 4096
        || target.starts_with('/')
        || target.contains('\\')
        || target.chars().any(char::is_control)
        || target.split('/').any(|p| p == "." || p == "..")
    {
        bail!("invalid upload target");
    }
    if filename.is_empty()
        || filename.len() > 255
        || filename == "."
        || filename == ".."
        || filename
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\' | '"'))
    {
        bail!("invalid upload filename");
    }
    if size > MAX_UPLOAD_BYTES {
        bail!("upload exceeds maximum size");
    }
    Ok(())
}

fn content_type(filename: &str) -> &'static str {
    match filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "avif" => "image/avif",
        "apng" => "image/apng",
        "bmp" => "image/bmp",
        "tif" | "tiff" => "image/tiff",
        _ => "application/octet-stream",
    }
}

fn multipart_body(boundary: &str, target: &str, filename: &str, data: &[u8]) -> Vec<u8> {
    let mut body = format!("--{boundary}\r\nContent-Disposition: form-data; name=\"source\"\r\n\r\ndata\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"target\"\r\n\r\n{target}\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"upload\"; filename=\"{filename}\"\r\nContent-Type: {}\r\n\r\n", content_type(filename)).into_bytes();
    body.extend_from_slice(data);
    body.extend_from_slice(format!("\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"bucket\"\r\n\r\nnull\r\n--{boundary}--\r\n").as_bytes());
    body
}

fn parse_upload_response(bytes: &[u8]) -> Result<Value> {
    let parsed: Value =
        serde_json::from_slice(bytes).map_err(|_| anyhow!("invalid Foundry upload response"))?;
    if let Some(error) = parsed
        .get("error")
        .filter(|v| !v.is_null() && *v != &json!(false))
    {
        bail!("Foundry upload failed: {error}");
    }
    let path = parsed
        .get("path")
        .and_then(Value::as_str)
        .filter(|path| !path.trim().is_empty())
        .ok_or_else(|| anyhow!("Foundry upload response missing path"))?;
    Ok(json!({"path": path, "message": parsed.get("message").and_then(Value::as_str)}))
}

impl FoundryClient {
    pub async fn browse_files(
        &self,
        credential: &FoundryCredential,
        target: &str,
        kind: Option<&str>,
        extensions: Option<&[String]>,
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
        let id = 1;
        socket
            .send(Message::Text(
                browse_request(id, target, kind, extensions).into(),
            ))
            .await
            .map_err(|_| anyhow!("Failed sending Foundry browse request"))?;
        timeout(RESPONSE_TIMEOUT, async {
            loop {
                let frame = socket
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("Foundry socket closed"))?
                    .map_err(|_| anyhow!("Foundry socket read failed"))?;
                if let Message::Text(text) = frame {
                    if text.len() > MAX_RESPONSE_BYTES {
                        bail!("Foundry browse response too large");
                    }
                    if text == "2" {
                        socket
                            .send(Message::Text("3".into()))
                            .await
                            .map_err(|_| anyhow!("Foundry pong failed"))?;
                        continue;
                    }
                    if let Some(response) = browse_ack(text.as_str(), id)? {
                        return Ok(response);
                    }
                }
            }
        })
        .await
        .map_err(|_| anyhow!("Timed out waiting for Foundry browse response"))?
    }

    /// Upload already-decoded bytes. URL fetching is deliberately unsupported.
    pub async fn upload_file(
        &self,
        credential: &FoundryCredential,
        target: &str,
        filename: &str,
        data: &[u8],
    ) -> Result<Value> {
        validate_upload(target, filename, data.len())?;
        let (session, _) = self.session(credential).await?;
        let url = Self::host_url(&credential.hostname, "/upload")?;
        let origin = url.origin().ascii_serialization();
        let mut random = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut random);
        let boundary = format!(
            "----FoundryMCPBoundary{}",
            random
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>()
        );
        let body = multipart_body(&boundary, target, filename, data);
        let mut response = self
            .http
            .post(url)
            .header("cookie", format!("session={session}"))
            .header("origin", origin)
            .header(
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await
            .map_err(|_| anyhow!("Foundry upload request failed"))?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| anyhow!("Foundry upload response failed"))?
        {
            if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(bytes.len()) {
                bail!("Foundry upload response too large");
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            bail!("Foundry upload failed with status {status}");
        }
        parse_upload_response(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn browse_request_matches_upstream() {
        let message = browse_request(7, "worlds/test", None, None);
        assert_eq!(
            message,
            format!(
                "427{}",
                json!(["manageFiles", {"action":"browseFiles","storage":"data","target":"worlds/test"}, {"type":"image","extensions":[".apng",".avif",".bmp",".gif",".jpeg",".jpg",".png",".svg",".tiff",".webp"],"wildcard":false,"render":true}])
            )
        );
    }

    #[test]
    fn browse_ack_checks_id_errors_and_shape() {
        assert!(browse_ack("432[{\"dirs\":[]}]", 1).unwrap().is_none());
        assert!(browse_ack("431[{\"error\":\"denied\"}]", 1).is_err());
        assert!(browse_ack("431[{\"files\":[]}]", 1).is_err());
        assert_eq!(
            browse_ack("431[{\"dirs\":[],\"files\":[]}]", 1)
                .unwrap()
                .unwrap()["dirs"],
            json!([])
        );
    }

    #[test]
    fn validates_upload_paths_and_size() {
        for target in ["../x", "/root", "foo/../bar", "foo\\bar", "foo\r\nbar"] {
            assert!(validate_upload(target, "x.png", 1).is_err(), "{target}");
        }
        for name in ["", ".", "..", "../x", "x\\y", "x\".png", "x\r\ny"] {
            assert!(validate_upload("worlds/test", name, 1).is_err(), "{name}");
        }
        assert!(validate_upload("worlds/test", "image.png", MAX_UPLOAD_BYTES + 1).is_err());
        assert!(validate_upload("worlds/test", "image.png", MAX_UPLOAD_BYTES).is_ok());
    }

    #[test]
    fn upload_response_requires_a_real_nonempty_path() {
        for body in [
            "{}",
            "null",
            "not json",
            r#"{"path":""}"#,
            r#"{"path":"   "}"#,
            r#"{"path":42}"#,
        ] {
            assert!(parse_upload_response(body.as_bytes()).is_err(), "{body}");
        }
        assert!(parse_upload_response(br#"{"error":"denied","path":"x"}"#).is_err());
        assert_eq!(
            parse_upload_response(br#"{"path":"foo/x.png","message":"ok"}"#).unwrap()["path"],
            "foo/x.png"
        );
    }

    #[test]
    fn multipart_matches_upstream_fields_and_preserves_binary() {
        let body = multipart_body("boundary", "worlds/test", "x.png", &[0, 255]);
        let mut expected = b"--boundary\r\nContent-Disposition: form-data; name=\"source\"\r\n\r\ndata\r\n--boundary\r\nContent-Disposition: form-data; name=\"target\"\r\n\r\nworlds/test\r\n--boundary\r\nContent-Disposition: form-data; name=\"upload\"; filename=\"x.png\"\r\nContent-Type: image/png\r\n\r\n".to_vec();
        expected.extend_from_slice(&[0, 255]);
        expected.extend_from_slice(b"\r\n--boundary\r\nContent-Disposition: form-data; name=\"bucket\"\r\n\r\nnull\r\n--boundary--\r\n");
        assert_eq!(body, expected);
    }
}
