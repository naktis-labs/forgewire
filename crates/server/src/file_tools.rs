use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use foundry_connector::{FoundryClient, FoundryCredential};
use serde_json::{json, Value};

const MAX_UPLOAD_BYTES: usize = foundry_connector::files::MAX_UPLOAD_BYTES;
const MAX_ENCODED_BYTES: usize = MAX_UPLOAD_BYTES.div_ceil(3) * 4;

pub fn definitions() -> Vec<Value> {
    vec![
        json!({"name":"browse_files","description":"Browse files in Foundry data storage","inputSchema":{"type":"object","properties":{"target":{"type":"string"},"type":{"type":"string"},"extensions":{"type":"array","items":{"type":"string"}}}}}),
        json!({"name":"upload_file","description":"Upload base64-encoded bytes to Foundry data storage (requires FOUNDRY_ALLOW_WRITES=true); remote URLs are not supported. Uploads may overwrite existing files.","inputSchema":{"type":"object","properties":{"target":{"type":"string"},"filename":{"type":"string"},"data_base64":{"type":"string","maxLength":MAX_ENCODED_BYTES},"overwrite":{"type":"boolean","description":"Must be true to acknowledge that an existing file may be overwritten."}},"required":["target","filename","data_base64","overwrite"]}}),
    ]
}

pub fn recognizes(name: &str) -> bool {
    matches!(name, "browse_files" | "upload_file")
}

fn decode_upload(encoded: &str) -> Result<Vec<u8>> {
    if encoded.len() > MAX_ENCODED_BYTES {
        bail!("encoded upload exceeds maximum size");
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| anyhow!("invalid base64 upload"))?;
    if bytes.len() > MAX_UPLOAD_BYTES {
        bail!("upload exceeds maximum size");
    }
    Ok(bytes)
}

pub async fn call(
    client: &FoundryClient,
    credential: &FoundryCredential,
    allow_writes: bool,
    name: &str,
    args: &Value,
) -> Result<Value> {
    if name == "upload_file" && !allow_writes {
        bail!("writes are disabled; set FOUNDRY_ALLOW_WRITES=true to enable");
    }
    let args = args
        .as_object()
        .ok_or_else(|| anyhow!("arguments must be an object"))?;
    let required = |key: &str| -> Result<&str> {
        args.get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("{key} must be a string"))
    };
    match name {
        "browse_files" => {
            if args.contains_key("kind") {
                bail!("use type instead of kind");
            }
            let target = match args.get("target") {
                None => "",
                Some(v) => v
                    .as_str()
                    .ok_or_else(|| anyhow!("target must be a string"))?,
            };
            let kind = match args.get("type") {
                None | Some(Value::Null) => None,
                Some(v) => Some(v.as_str().ok_or_else(|| anyhow!("type must be a string"))?),
            };
            let extensions = match args.get("extensions") {
                None | Some(Value::Null) => None,
                Some(Value::Array(values)) => Some(
                    values
                        .iter()
                        .map(|v| {
                            v.as_str()
                                .map(str::to_owned)
                                .ok_or_else(|| anyhow!("extensions must contain strings"))
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                _ => bail!("extensions must be an array of strings"),
            };
            client
                .browse_files(credential, target, kind, extensions.as_deref())
                .await
        }
        "upload_file" => {
            match args.get("overwrite") {
                Some(Value::Bool(true)) => {}
                Some(Value::Bool(false)) => bail!("overwrite must be true to acknowledge potential replacement"),
                _ => bail!("overwrite must be a boolean and must be true to acknowledge potential replacement"),
            }
            let target = required("target")?;
            let filename = required("filename")?;
            let data = decode_upload(required("data_base64")?)?;
            client
                .upload_file(credential, target, filename, &data)
                .await
        }
        _ => bail!("unknown file tool"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decodes_base64_and_rejects_bad_or_oversized_data() {
        assert_eq!(decode_upload("AP8=").unwrap(), vec![0, 255]);
        assert!(decode_upload("not base64!").is_err());
        assert!(decode_upload(&"A".repeat(MAX_ENCODED_BYTES + 1)).is_err());
        let oversized = STANDARD.encode(vec![0; MAX_UPLOAD_BYTES + 1]);
        assert!(decode_upload(&oversized).is_err());
    }

    #[tokio::test]
    async fn disabled_upload_is_rejected_before_parsing_or_network() {
        let client = FoundryClient::new().unwrap();
        let credential = FoundryCredential {
            id: None,
            hostname: "localhost".into(),
            userid: None,
            username: None,
            password: String::new(),
        };
        let error = call(&client, &credential, false, "upload_file", &json!({}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("disabled"));
    }

    #[test]
    fn schema_requires_explicit_overwrite_and_uses_type() {
        let defs = definitions();
        let browse = &defs[0]["inputSchema"]["properties"];
        assert_eq!(browse["type"]["type"], "string");
        assert!(browse.get("kind").is_none());
        let upload = &defs[1]["inputSchema"];
        assert_eq!(upload["properties"]["overwrite"]["type"], "boolean");
        assert!(upload["required"]
            .as_array()
            .unwrap()
            .contains(&json!("overwrite")));
    }

    #[tokio::test]
    async fn upload_requires_true_overwrite_acknowledgment_before_network() {
        let client = FoundryClient::new().unwrap();
        let credential = FoundryCredential {
            id: None,
            hostname: "localhost".into(),
            userid: None,
            username: None,
            password: String::new(),
        };
        for overwrite in [
            None,
            Some(json!(false)),
            Some(json!("true")),
            Some(Value::Null),
        ] {
            let mut args = json!({"target":"", "filename":"x", "data_base64":""});
            if let Some(value) = overwrite {
                args["overwrite"] = value;
            }
            let err = call(&client, &credential, true, "upload_file", &args)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("overwrite"), "{err}");
        }
    }

    #[test]
    fn definitions_match_dispatch() {
        for definition in definitions() {
            assert!(recognizes(definition["name"].as_str().unwrap()));
        }
        assert!(!recognizes("other"));
    }
}
