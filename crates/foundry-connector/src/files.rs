use anyhow::{anyhow, bail, Result};
use rand::RngCore;
use serde_json::{json, Value};

use crate::{FoundryClient, FoundryCredential};

pub const MAX_UPLOAD_BYTES: usize = 10 * 1024 * 1024;
/// The largest browse response the gateway accepts.
pub(crate) const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const IMAGE_EXTENSIONS: &[&str] = &[
    ".apng", ".avif", ".bmp", ".gif", ".jpeg", ".jpg", ".png", ".svg", ".tiff", ".webp",
];

fn validate_upload(target: &str, filename: &str, size: usize) -> Result<()> {
    if target.len() > 4096
        || target.starts_with('/')
        || target.contains('\\')
        || target.chars().any(char::is_control)
        || target.split('/').any(|p| p == "." || p == "..")
    {
        return Err(crate::invalid_input("invalid upload target"));
    }
    if filename.is_empty()
        || filename.len() > 255
        || filename == "."
        || filename == ".."
        || filename
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\' | '"'))
    {
        return Err(crate::invalid_input("invalid upload filename"));
    }
    if size > MAX_UPLOAD_BYTES {
        return Err(crate::invalid_input("upload exceeds maximum size"));
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
    /// Browse a Foundry file directory over the identity's persistent
    /// connection. The acknowledgement id identifies the request, so a browse
    /// shares the connection with reads and with other operations instead of
    /// opening a socket per operation.
    pub async fn browse_files(
        &self,
        credential: &FoundryCredential,
        target: &str,
        kind: Option<&str>,
        extensions: Option<&[String]>,
    ) -> Result<Value> {
        let extensions = extensions.map(<[String]>::to_vec).unwrap_or_else(|| {
            IMAGE_EXTENSIONS
                .iter()
                .map(|value| (*value).to_owned())
                .collect()
        });
        let key = crate::identity_key(credential);
        self.connections
            .exchange(
                key,
                crate::socket_mux::Request::Files {
                    target: target.to_owned(),
                    kind: kind.unwrap_or("image").to_owned(),
                    extensions,
                },
                "files",
                || self.connect_for(credential),
            )
            .await
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
        let (session, _, _) = self.session(credential).await?;
        let url = self.url_for(&credential.hostname, "/upload")?;
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
            .map_err(|_| {
                crate::ambiguous_mutation(
                    "Foundry upload request failed; the upload may have been applied",
                )
            })?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            crate::ambiguous_mutation(
                "Foundry upload response failed; the upload may have been applied",
            )
        })? {
            if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(bytes.len()) {
                return Err(crate::ambiguous_mutation(
                    "Foundry upload response too large; the upload may have been applied",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            return Err(crate::ambiguous_mutation(format!(
                "Foundry upload failed with status {status}; the upload may have been applied"
            )));
        }
        parse_upload_response(&bytes).map_err(|error| {
            crate::ambiguous_mutation(format!("{error}; the upload may have been applied"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_refused_upload_is_caller_input_not_an_upstream_fault() {
        // The caller is told it made a mistake, so the gateway answers 400
        // instead of an upstream failure. This is the only place the
        // connector produces that typed error.
        for (target, name, size) in [
            ("../x", "x.png", 1),
            ("worlds/test", "../x", 1),
            ("worlds/test", "image.png", MAX_UPLOAD_BYTES + 1),
        ] {
            let error = validate_upload(target, name, size).unwrap_err();
            assert!(
                error.downcast_ref::<crate::InvalidInput>().is_some(),
                "{target} {name}: {error}"
            );
            assert!(error.downcast_ref::<crate::AmbiguousMutation>().is_none());
        }
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
