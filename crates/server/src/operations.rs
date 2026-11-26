//! Transport-independent application operations shared by REST and MCP.
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use foundry_connector::{read_ops, FoundryClient, FoundryCredential};
use serde_json::{json, Map, Value};

pub const WRITE_KINDS: [&str; 23] = [
    "Actor",
    "Item",
    "Folder",
    "User",
    "Scene",
    "JournalEntry",
    "Macro",
    "Cards",
    "Playlist",
    "RollTable",
    "Combat",
    "ChatMessage",
    "Setting",
    "Combatant",
    "ActiveEffect",
    "Drawing",
    "MeasuredTemplate",
    "Note",
    "Tile",
    "Token",
    "Wall",
    "AmbientLight",
    "AmbientSound",
];

/// The single write-gating message every transport surfaces, so REST and MCP
/// cannot drift apart on the safety wording.
pub const WRITES_DISABLED: &str = "writes are disabled; set FOUNDRY_ALLOW_WRITES=true to enable";

/// The single write-gating message for a request that carries no valid
/// authentication. Writes are the one capability that always requires an
/// authenticated caller, so the wording is shared exactly like
/// [`WRITES_DISABLED`].
pub const WRITES_REQUIRE_AUTHENTICATION: &str =
    "writes require authentication; configure FORGEWIRE_BEARER_TOKEN and send Authorization: Bearer <scope>.<token>";

/// The two independent gates every write must pass: the operator's explicit
/// opt-in and the request's own authentication. Keeping them in one value
/// stops the transports from passing them in the wrong order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WritePolicy {
    /// `FOUNDRY_ALLOW_WRITES=true`.
    pub allow_writes: bool,
    /// The request presented a valid, in-scope bearer token.
    pub authenticated: bool,
}

impl WritePolicy {
    /// A policy for a request that is not authenticated.
    pub fn unauthenticated(allow_writes: bool) -> Self {
        Self {
            allow_writes,
            authenticated: false,
        }
    }

    /// A policy for an authenticated request.
    pub fn authenticated(allow_writes: bool) -> Self {
        Self {
            allow_writes,
            authenticated: true,
        }
    }
}

const TYPES: [(&str, &str, &str, Option<&str>); 13] = [
    ("actor", "actors", "actors", Some("Actor")),
    ("item", "items", "items", Some("Item")),
    ("folder", "folders", "folders", None),
    ("user", "users", "users", None),
    ("scene", "scenes", "scenes", Some("Scene")),
    ("journal", "journals", "journal", Some("JournalEntry")),
    ("macro", "macros", "macros", Some("Macro")),
    ("card", "cards", "cards", Some("Cards")),
    ("playlist", "playlists", "playlists", Some("Playlist")),
    ("table", "tables", "tables", Some("RollTable")),
    ("combat", "combats", "combats", None),
    ("message", "messages", "messages", None),
    ("setting", "settings", "settings", None),
];

const MAX_UPLOAD_BYTES: usize = foundry_connector::files::MAX_UPLOAD_BYTES;
const MAX_ENCODED_BYTES: usize = MAX_UPLOAD_BYTES.div_ceil(3) * 4;

/// Connector boundary keeps the application layer testable without contacting Foundry.
#[async_trait]
pub trait Connector: Send + Sync {
    async fn world(&self, target: &FoundryCredential) -> Result<Value>;
    async fn list_documents(&self, target: &FoundryCredential, collection: &str) -> Result<Value>;
    async fn query_documents(
        &self,
        target: &FoundryCredential,
        collection: &str,
        query: Value,
    ) -> Result<Value>;
    async fn get_compendiums(
        &self,
        target: &FoundryCredential,
        kind: Option<&str>,
    ) -> Result<Value>;
    async fn get_pack_documents(
        &self,
        target: &FoundryCredential,
        kind: &str,
        pack: &str,
        query: Value,
    ) -> Result<Value>;
    async fn create_compendium(
        &self,
        target: &FoundryCredential,
        label: &str,
        kind: &str,
    ) -> Result<Value>;
    async fn delete_compendium(&self, target: &FoundryCredential, name: &str) -> Result<Value>;
    async fn browse_files(
        &self,
        target: &FoundryCredential,
        path: &str,
        kind: Option<&str>,
        extensions: Option<&[String]>,
    ) -> Result<Value>;
    async fn upload_file(
        &self,
        target: &FoundryCredential,
        path: &str,
        filename: &str,
        data: &[u8],
    ) -> Result<Value>;
    async fn create_document(
        &self,
        target: &FoundryCredential,
        kind: &str,
        data: Vec<Value>,
        parent: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value>;
    async fn modify_document(
        &self,
        target: &FoundryCredential,
        kind: &str,
        id: &str,
        data: Vec<Value>,
        parent: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value>;
    async fn delete_document(
        &self,
        target: &FoundryCredential,
        kind: &str,
        ids: Vec<String>,
        parent: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value>;
}

#[async_trait]
impl Connector for FoundryClient {
    async fn world(&self, target: &FoundryCredential) -> Result<Value> {
        Ok(serde_json::to_value(
            FoundryClient::world(self, target).await?,
        )?)
    }
    async fn list_documents(&self, target: &FoundryCredential, collection: &str) -> Result<Value> {
        FoundryClient::list_documents(self, target, collection).await
    }
    async fn query_documents(
        &self,
        target: &FoundryCredential,
        collection: &str,
        query: Value,
    ) -> Result<Value> {
        FoundryClient::query_documents(self, target, collection, query).await
    }
    async fn get_compendiums(
        &self,
        target: &FoundryCredential,
        kind: Option<&str>,
    ) -> Result<Value> {
        FoundryClient::get_compendiums(self, target, kind).await
    }
    async fn get_pack_documents(
        &self,
        target: &FoundryCredential,
        kind: &str,
        pack: &str,
        query: Value,
    ) -> Result<Value> {
        FoundryClient::get_pack_documents(self, target, kind, pack, query).await
    }
    async fn create_compendium(
        &self,
        target: &FoundryCredential,
        label: &str,
        kind: &str,
    ) -> Result<Value> {
        FoundryClient::create_compendium(self, target, label, kind).await
    }
    async fn delete_compendium(&self, target: &FoundryCredential, name: &str) -> Result<Value> {
        FoundryClient::delete_compendium(self, target, name).await
    }
    async fn browse_files(
        &self,
        target: &FoundryCredential,
        path: &str,
        kind: Option<&str>,
        extensions: Option<&[String]>,
    ) -> Result<Value> {
        FoundryClient::browse_files(self, target, path, kind, extensions).await
    }
    async fn upload_file(
        &self,
        target: &FoundryCredential,
        path: &str,
        filename: &str,
        data: &[u8],
    ) -> Result<Value> {
        FoundryClient::upload_file(self, target, path, filename, data).await
    }
    async fn create_document(
        &self,
        target: &FoundryCredential,
        kind: &str,
        data: Vec<Value>,
        parent: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value> {
        FoundryClient::create_document(self, target, kind, data, parent, pack).await
    }
    async fn modify_document(
        &self,
        target: &FoundryCredential,
        kind: &str,
        id: &str,
        data: Vec<Value>,
        parent: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value> {
        FoundryClient::modify_document(self, target, kind, id, data, parent, pack).await
    }
    async fn delete_document(
        &self,
        target: &FoundryCredential,
        kind: &str,
        ids: Vec<String>,
        parent: Option<&str>,
        pack: Option<&str>,
    ) -> Result<Value> {
        FoundryClient::delete_document(self, target, kind, ids, parent, pack).await
    }
}

pub type WriteArguments<'a> = (
    &'a str,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    Vec<Value>,
    Vec<String>,
);

pub fn write_arguments<'a>(args: &'a Value, operation: &str) -> Result<WriteArguments<'a>> {
    if !matches!(
        operation,
        "create_document" | "modify_document" | "delete_document"
    ) {
        bail!("unknown operation");
    }
    let kind = args
        .get("kind")
        .and_then(Value::as_str)
        .filter(|s| WRITE_KINDS.contains(s))
        .ok_or_else(|| anyhow!("invalid kind"))?;
    let optional = |key| -> Result<Option<&str>> {
        match args.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) if !s.trim().is_empty() => Ok(Some(s)),
            _ => Err(anyhow!("invalid {key}")),
        }
    };
    if operation == "create_document" && args.get("id").is_some() {
        bail!("id must not be supplied to create_document");
    }
    let id = optional("id")?;
    let parent = optional("parent_uuid")?;
    let pack = optional("pack")?;
    let mut data = Vec::new();
    let mut ids = Vec::new();
    if operation == "delete_document" {
        for value in args
            .get("ids")
            .and_then(Value::as_array)
            .filter(|a| !a.is_empty())
            .ok_or_else(|| anyhow!("ids must be a non-empty array"))?
        {
            ids.push(
                value
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| anyhow!("invalid id"))?
                    .to_owned(),
            );
        }
    } else {
        let field = if operation == "create_document" {
            "data"
        } else {
            "updates"
        };
        data = args
            .get(field)
            .and_then(Value::as_array)
            .filter(|a| !a.is_empty() && a.iter().all(Value::is_object))
            .ok_or_else(|| anyhow!("{field} must be a non-empty array of objects"))?
            .clone();
        if operation == "modify_document" && id.is_none() {
            bail!("id is required");
        }
    }
    Ok((kind, id, parent, pack, data, ids))
}

pub async fn world(
    client: &dyn Connector,
    target: &FoundryCredential,
) -> Result<Value, OperationError> {
    client.world(target).await.map_err(OperationError::Upstream)
}

/// List a world collection. The name is validated by the same shared parse
/// the `/api/documents` route uses, so `/api/collections/{name}` and MCP
/// `list_documents` reject unknown or missing collections identically and
/// before any connector contact.
pub async fn collection(
    client: &dyn Connector,
    target: &FoundryCredential,
    name: &str,
) -> Result<Value, OperationError> {
    let (tool, args) = parse_rest_read(name, &json!({})).map_err(OperationError::Invalid)?;
    let req = parse_read(&tool, &args).map_err(OperationError::Invalid)?;
    execute_read(client, target, &req).await
}

#[derive(Clone, Debug)]
pub struct ReadRequest {
    pub collection: &'static str,
    pub single: bool,
    pub identifier: read_ops::Identifier,
    pub filter: Option<Map<String, Value>>,
    pub fields: Option<Vec<String>>,
    pub max_length: usize,
    pub pack: Option<String>,
    pub kind: Option<&'static str>,
}

pub fn parse_read(name: &str, args: &Value) -> Result<ReadRequest> {
    let (single, collection, kind) = TYPES
        .iter()
        .find_map(|(one, many, collection, kind)| {
            if name == format!("get_{one}") {
                Some((true, *collection, *kind))
            } else if name == format!("get_{many}") {
                Some((false, *collection, *kind))
            } else {
                None
            }
        })
        .ok_or_else(|| anyhow!("unknown read tool"))?;
    let args = args
        .as_object()
        .ok_or_else(|| anyhow!("arguments must be an object"))?;
    let string = |key: &str| -> Result<Option<String>> {
        match args.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) => Ok(Some(s.clone())),
            _ => bail!("{key} must be a string"),
        }
    };
    let pack = string("pack")?;
    if pack.as_ref().is_some_and(|p| p.trim().is_empty()) {
        bail!("pack must not be empty");
    }
    if pack.is_some() && kind.is_none() {
        bail!("this document type cannot be stored in a compendium");
    }
    let identifier = read_ops::Identifier {
        id: string("id")?,
        _id: string("_id")?,
        name: string("name")?,
    };
    if single
        && [
            identifier.id.as_deref(),
            identifier._id.as_deref(),
            identifier.name.as_deref(),
        ]
        .iter()
        .all(|v| v.is_none_or(str::is_empty))
    {
        bail!("id, _id, or name is required");
    }
    let fields = match args.get("requested_fields") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => Some(
            values
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| anyhow!("requested_fields must contain strings"))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        _ => bail!("requested_fields must be an array of strings"),
    };
    let filter = match args.get("where") {
        None | Some(Value::Null) => None,
        Some(Value::Object(map)) => Some(map.clone()),
        _ => bail!("where must be an object"),
    };
    let max_length = match args.get("max_length") {
        None | Some(Value::Null) => 0,
        Some(value) => {
            let n = value
                .as_i64()
                .ok_or_else(|| anyhow!("max_length must be an integer"))?;
            if n < 0 {
                bail!("max_length must not be negative");
            }
            usize::try_from(n).unwrap_or(usize::MAX)
        }
    };
    Ok(ReadRequest {
        collection,
        single,
        identifier,
        filter,
        fields,
        max_length,
        pack,
        kind,
    })
}

/// Whether REST query arguments select the singular read. Presence alone
/// selects it, so an empty `id=` still reaches the shared [`parse_read`]
/// requirement and fails exactly like MCP (`id, _id, or name is required`)
/// instead of silently degrading into a full listing.
fn rest_identifies_single(args: &Value) -> bool {
    ["id", "_id", "name"].iter().any(|k| args.get(*k).is_some())
}

/// Map a REST collection path (`/api/documents/{collection}`, and the
/// collection listing behind [`collection`]) onto the shared read tool names.
/// The path is a collection name, so it matches the collection column — the
/// same set `/api/collections/{name}`, MCP `list_documents`, and the
/// connector accept (note `journal`, whose plural tool name differs) — and
/// all of them validate through this one parse.
pub fn parse_rest_read(collection: &str, args: &Value) -> Result<(String, Value)> {
    let (singular, plural, _, _) = TYPES
        .iter()
        .find(|(_, _, name, _)| *name == collection)
        .ok_or_else(|| anyhow!("unsupported collection"))?;
    let single = rest_identifies_single(args);
    Ok((
        format!("get_{}", if single { singular } else { plural }),
        args.clone(),
    ))
}

/// Map a REST compendium path (`pack` plus document `kind`) and query
/// arguments onto the shared read operation. Packable kinds and the
/// singular/plural mapping come from the same table MCP pack reads use, so
/// the transports cannot disagree on which document types are packable.
pub fn parse_rest_pack_read(kind: &str, pack: &str, args: &Value) -> Result<(String, Value)> {
    let (singular, plural, _, _) = TYPES
        .iter()
        .find(|(one, many, _, _)| *one == kind || *many == kind)
        .filter(|(_, _, _, packable)| packable.is_some())
        .ok_or_else(|| anyhow!("unsupported compendium document type"))?;
    let mut args = args.clone();
    args["pack"] = json!(pack);
    let single = rest_identifies_single(&args);
    Ok((
        format!("get_{}", if single { singular } else { plural }),
        args,
    ))
}

fn pack_query(req: &ReadRequest) -> Value {
    if req.single {
        read_ops::identifier_query(&req.identifier).unwrap_or_else(|| json!({}))
    } else {
        read_ops::where_to_server_query(req.filter.as_ref())
    }
}

fn apply_read(req: &ReadRequest, docs: Value) -> Result<Value, OperationError> {
    let docs = docs.as_array().ok_or_else(|| {
        OperationError::Upstream(anyhow!("Foundry document response must be an array"))
    })?;
    if req.single {
        let doc = read_ops::find_document(docs, &req.identifier)
            .ok_or_else(|| OperationError::NotFound(anyhow!("Document not found")))?;
        if req.filter.as_ref().is_some_and(|filter| {
            !read_ops::process_documents(std::slice::from_ref(doc), Some(filter), None, 0)
                .len()
                .eq(&1)
        }) {
            return Err(OperationError::NotFound(anyhow!("Document not found")));
        }
        let projected = read_ops::project_document(doc, req.fields.as_deref());
        if req.max_length > 0
            && serde_json::to_vec(&projected)
                .map_err(|e| OperationError::Upstream(anyhow!(e)))?
                .len()
                > req.max_length
        {
            return Err(OperationError::Invalid(anyhow!(
                "document exceeds max_length"
            )));
        }
        Ok(projected)
    } else {
        Ok(Value::Array(read_ops::process_documents(
            docs,
            req.filter.as_ref(),
            req.fields.as_deref(),
            req.max_length,
        )))
    }
}

/// Typed application outcomes let each transport map the same failure to its
/// own wire format: REST status codes, MCP `isError` text. The displayed
/// message is identical to the untyped error it replaces, and MCP renders
/// every variant the same way it always has.
#[derive(Debug)]
pub enum OperationError {
    /// The operation is disabled by policy (`FOUNDRY_ALLOW_WRITES`).
    Forbidden(anyhow::Error),
    /// Caller input failed validation, or the request as stated cannot be
    /// satisfied (for example a byte limit smaller than one document).
    Invalid(anyhow::Error),
    /// The lookup, filter, or limit excluded every candidate document.
    NotFound(anyhow::Error),
    /// Contacting Foundry or interpreting its response failed.
    Upstream(anyhow::Error),
    /// A change may have been applied. The request reached Foundry, but the
    /// answer does not prove whether the change took effect, so the caller
    /// learns the outcome by reading the affected document again. The
    /// change is never replayed.
    PotentiallyApplied(anyhow::Error),
}

impl OperationError {
    fn invalid(message: &str) -> Self {
        OperationError::Invalid(anyhow!("{message}"))
    }
}

impl std::fmt::Display for OperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationError::Forbidden(e)
            | OperationError::Invalid(e)
            | OperationError::NotFound(e)
            | OperationError::Upstream(e)
            | OperationError::PotentiallyApplied(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for OperationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OperationError::Forbidden(e)
            | OperationError::Invalid(e)
            | OperationError::NotFound(e)
            | OperationError::Upstream(e)
            | OperationError::PotentiallyApplied(e) => e.source(),
        }
    }
}

/// The write gate shared by every mutating operation: the operator's opt-in
/// first (the documented default is "disabled"), then the request's own
/// authentication. A write is refused outright when either gate fails, so
/// no mutating call ever reaches the connector.
fn ensure_writes_allowed(policy: WritePolicy) -> Result<(), OperationError> {
    if !policy.allow_writes {
        return Err(OperationError::Forbidden(anyhow!("{WRITES_DISABLED}")));
    }
    if !policy.authenticated {
        return Err(OperationError::Forbidden(anyhow!(
            "{WRITES_REQUIRE_AUTHENTICATION}"
        )));
    }
    Ok(())
}

/// Classify a failure from a change.
///
/// A change whose outcome is unknown is its own outcome, not a plain upstream
/// failure: the caller must learn whether the change took effect by reading
/// the document again, and the change is never replayed. Caller input the
/// connector refused never reached Foundry, so it is a caller mistake.
fn classify_change_failure(error: anyhow::Error) -> OperationError {
    if error
        .downcast_ref::<foundry_connector::AmbiguousMutation>()
        .is_some()
    {
        return OperationError::PotentiallyApplied(error);
    }
    if error
        .downcast_ref::<foundry_connector::InvalidInput>()
        .is_some()
    {
        return OperationError::Invalid(error);
    }
    OperationError::Upstream(error)
}

/// Classify a failure from the shared read path's connector call. An unknown
/// compendium pack reference is the caller's lookup miss (`NotFound`, 404 on
/// REST), not an upstream fault; every other connector failure stays
/// `Upstream`. The connector reports the miss as validation text from
/// `read_ops::validate_pack_access`, so this is the one place a read turns
/// that message into a typed outcome for both transports.
fn classify_read_failure(error: anyhow::Error) -> OperationError {
    let message = error.to_string();
    if message.starts_with("Compendium pack \"") && message.contains("not found") {
        OperationError::NotFound(error)
    } else {
        OperationError::Upstream(error)
    }
}

pub async fn read(
    client: &dyn Connector,
    target: &FoundryCredential,
    name: &str,
    args: &Value,
) -> Result<Value, OperationError> {
    if name == "get_compendiums" {
        let args = args
            .as_object()
            .ok_or_else(|| OperationError::invalid("arguments must be an object"))?;
        let kind = match args.get("type") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.as_str()),
            _ => return Err(OperationError::invalid("type must be a string")),
        };
        return client
            .get_compendiums(target, kind)
            .await
            .map_err(OperationError::Upstream);
    }
    let req = parse_read(name, args).map_err(OperationError::Invalid)?;
    execute_read(client, target, &req).await
}

/// Execute an already validated read request. REST handlers validate with
/// [`parse_read`] first (mapping rejection to 400) and then call this, so one
/// parse drives both transports instead of duplicated validation.
pub async fn execute_read(
    client: &dyn Connector,
    target: &FoundryCredential,
    req: &ReadRequest,
) -> Result<Value, OperationError> {
    let query = pack_query(req);
    let docs = if let Some(pack) = req.pack.as_deref() {
        client
            .get_pack_documents(target, req.kind.unwrap(), pack, query)
            .await
    } else if req.single || req.filter.is_some() {
        client.query_documents(target, req.collection, query).await
    } else {
        client.list_documents(target, req.collection).await
    }
    .map_err(classify_read_failure)?;
    apply_read(req, docs)
}

pub async fn write_document(
    client: &dyn Connector,
    target: &FoundryCredential,
    policy: WritePolicy,
    operation: &str,
    args: &Value,
) -> Result<Value, OperationError> {
    ensure_writes_allowed(policy)?;
    let (kind, id, parent, pack, data, ids) =
        write_arguments(args, operation).map_err(OperationError::Invalid)?;
    match operation {
        "create_document" => client
            .create_document(target, kind, data, parent, pack)
            .await
            .map_err(classify_change_failure),
        "modify_document" => client
            .modify_document(target, kind, id.unwrap(), data, parent, pack)
            .await
            .map_err(classify_change_failure),
        "delete_document" => client
            .delete_document(target, kind, ids, parent, pack)
            .await
            .map_err(classify_change_failure),
        _ => Err(OperationError::invalid("unknown operation")),
    }
}

fn object_args(args: &Value) -> Result<&Map<String, Value>> {
    args.as_object()
        .ok_or_else(|| anyhow!("arguments must be an object"))
}

fn required_string<'a>(args: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow!("{key} must be a non-empty string"))
}

pub async fn compendium(
    client: &dyn Connector,
    target: &FoundryCredential,
    policy: WritePolicy,
    name: &str,
    args: &Value,
) -> Result<Value, OperationError> {
    let args = object_args(args).map_err(OperationError::Invalid)?;
    match name {
        "create_compendium" | "delete_compendium" => {
            ensure_writes_allowed(policy)?;
            match name {
                "create_compendium" => client
                    .create_compendium(
                        target,
                        required_string(args, "label").map_err(OperationError::Invalid)?,
                        required_string(args, "type").map_err(OperationError::Invalid)?,
                    )
                    .await
                    .map_err(classify_change_failure),
                _ => client
                    .delete_compendium(
                        target,
                        required_string(args, "name").map_err(OperationError::Invalid)?,
                    )
                    .await
                    .map_err(classify_change_failure),
            }
        }
        _ => Err(OperationError::invalid("unknown compendium tool")),
    }
}

pub async fn browse_files(
    client: &dyn Connector,
    target: &FoundryCredential,
    args: &Value,
) -> Result<Value, OperationError> {
    let args = object_args(args).map_err(OperationError::Invalid)?;
    if args.contains_key("kind") {
        return Err(OperationError::invalid("use type instead of kind"));
    }
    let path = match args.get("target") {
        None => "",
        Some(v) => v
            .as_str()
            .ok_or_else(|| OperationError::invalid("target must be a string"))?,
    };
    let kind = match args.get("type") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_str()
                .ok_or_else(|| OperationError::invalid("type must be a string"))?,
        ),
    };
    let extensions = match args.get("extensions") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => Some(
            values
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| OperationError::invalid("extensions must contain strings"))
                })
                .collect::<Result<Vec<_>, OperationError>>()?,
        ),
        _ => {
            return Err(OperationError::invalid(
                "extensions must be an array of strings",
            ))
        }
    };
    client
        .browse_files(target, path, kind, extensions.as_deref())
        .await
        .map_err(OperationError::Upstream)
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

pub async fn upload_file(
    client: &dyn Connector,
    target: &FoundryCredential,
    policy: WritePolicy,
    args: &Value,
) -> Result<Value, OperationError> {
    ensure_writes_allowed(policy)?;
    let args = object_args(args).map_err(OperationError::Invalid)?;
    match args.get("overwrite") {
        Some(Value::Bool(true)) => {}
        Some(Value::Bool(false)) => {
            return Err(OperationError::invalid(
                "overwrite must be true to acknowledge potential replacement",
            ))
        }
        _ => {
            return Err(OperationError::invalid(
                "overwrite must be a boolean and must be true to acknowledge potential replacement",
            ))
        }
    }
    let path = required_string(args, "target").map_err(OperationError::Invalid)?;
    let filename = required_string(args, "filename").map_err(OperationError::Invalid)?;
    let data =
        decode_upload(required_string(args, "data_base64").map_err(OperationError::Invalid)?)
            .map_err(OperationError::Invalid)?;
    client
        .upload_file(target, path, filename, &data)
        .await
        .map_err(classify_change_failure)
}

pub fn rest_read_args(query: &std::collections::HashMap<String, String>) -> Result<Value> {
    let mut args = Map::new();
    for key in ["id", "_id", "name", "pack"] {
        if let Some(value) = query.get(key) {
            args.insert(key.into(), json!(value));
        }
    }
    if let Some(fields) = query.get("requested_fields") {
        args.insert(
            "requested_fields".into(),
            json!(fields
                .split(',')
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()),
        );
    }
    if let Some(length) = query.get("max_length") {
        let n: i64 = length
            .parse()
            .map_err(|_| anyhow!("max_length must be an integer"))?;
        args.insert("max_length".into(), json!(n));
    }
    if let Some(filter) = query.get("where") {
        let parsed: Value =
            serde_json::from_str(filter).map_err(|_| anyhow!("where must be valid JSON object"))?;
        args.insert("where".into(), parsed);
    }
    Ok(Value::Object(args))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn validates_collection_and_document_write_inputs_in_application_layer() {
        assert!(write_arguments(&json!({"kind":"Actor","data":[{}]}), "create_document").is_ok());
        assert!(write_arguments(
            &json!({"kind":"Actor","data":[{}],"id":"x"}),
            "create_document"
        )
        .is_err());
        assert!(write_arguments(&json!({"kind":"Actor","ids":[" "]}), "delete_document").is_err());
        assert!(
            write_arguments(&json!({"kind":"Actor","updates":[{}]}), "modify_document").is_err()
        );
    }

    #[test]
    fn rejects_unknown_write_operation_before_parsing_arguments() {
        assert_eq!(
            write_arguments(&Value::Null, "unknown")
                .unwrap_err()
                .to_string(),
            "unknown operation"
        );
    }

    #[test]
    fn rest_read_arguments_preserve_filters_projection_and_byte_limit() {
        let query = [
            ("where".into(), r#"{"type":"npc"}"#.into()),
            ("requested_fields".into(), "name,system.hp".into()),
            ("max_length".into(), "80".into()),
        ]
        .into();
        let args = rest_read_args(&query).unwrap();
        assert_eq!(args["where"], json!({"type":"npc"}));
        assert_eq!(args["requested_fields"], json!(["name", "system.hp"]));
        assert_eq!(args["max_length"], 80);
    }

    #[test]
    fn rest_pack_reads_derive_from_the_shared_packable_type_table() {
        // Identifiers select the singular read; absence selects the plural.
        for (kind, single, plural) in [
            ("actor", "get_actor", "get_actors"),
            ("actors", "get_actor", "get_actors"),
            ("table", "get_table", "get_tables"),
            ("tables", "get_table", "get_tables"),
        ] {
            let (name, args) =
                parse_rest_pack_read(kind, "pack-core", &json!({"id":"abc"})).unwrap();
            assert_eq!(name, single, "{kind} with identifier");
            assert_eq!(args["pack"], "pack-core");
            let (name, _) = parse_rest_pack_read(kind, "pack-core", &json!({})).unwrap();
            assert_eq!(name, plural, "{kind} without identifier");
        }
        // Non-packable and unknown document types are rejected.
        for kind in [
            "user", "users", "folder", "folders", "combat", "combats", "message", "messages",
            "setting", "settings", "creature",
        ] {
            assert!(
                parse_rest_pack_read(kind, "pack-core", &json!({"id":"abc"})).is_err(),
                "{kind}"
            );
        }
        // An explicit path pack overrides any pack from the query string.
        let (_, args) = parse_rest_pack_read(
            "actor",
            "path-pack",
            &json!({"pack":"query-pack","name":"Hero"}),
        )
        .unwrap();
        assert_eq!(args["pack"], "path-pack");
        assert_eq!(args["name"], "Hero");
    }

    #[test]
    fn write_gating_is_typed_as_forbidden_with_the_shared_message() {
        // The opt-in comes first: the documented default is "disabled".
        let error = ensure_writes_allowed(WritePolicy::unauthenticated(false)).unwrap_err();
        assert!(matches!(error, OperationError::Forbidden(_)));
        assert_eq!(error.to_string(), WRITES_DISABLED);
        // The opt-in alone is not enough: a write also needs an
        // authenticated request.
        let error = ensure_writes_allowed(WritePolicy::unauthenticated(true)).unwrap_err();
        assert!(matches!(error, OperationError::Forbidden(_)));
        assert_eq!(error.to_string(), WRITES_REQUIRE_AUTHENTICATION);
        // Both gates passed.
        assert!(ensure_writes_allowed(WritePolicy::authenticated(true)).is_ok());
        // An authenticated request is still gated by the opt-in.
        let error = ensure_writes_allowed(WritePolicy::authenticated(false)).unwrap_err();
        assert_eq!(error.to_string(), WRITES_DISABLED);
    }

    #[test]
    fn unknown_pack_references_classify_as_not_found_other_failures_upstream() {
        let miss = anyhow!(
            "Compendium pack \"world.missing\" not found. Use get_compendiums to list the available packs."
        );
        assert!(matches!(
            classify_read_failure(miss),
            OperationError::NotFound(_)
        ));
        for message in [
            "Timed out waiting Foundry document response",
            "Compendium pack \"world.items\" holds Item documents, not Actor.",
            "Foundry document response must be an array",
        ] {
            let error = classify_read_failure(anyhow!("{message}"));
            assert!(
                matches!(error, OperationError::Upstream(_)),
                "{message} must stay upstream"
            );
        }
    }

    #[test]
    fn rest_empty_identifiers_reach_the_shared_single_read_validation() {
        for key in ["id", "_id", "name"] {
            let query = HashMap::from([(key.to_string(), String::new())]);
            let args = rest_read_args(&query).unwrap();
            // Presence alone selects the singular read; the shared parse then
            // rejects it exactly like the MCP tool would.
            let (name, _) = parse_rest_read("actors", &args).unwrap();
            assert_eq!(name, "get_actor", "{key}=");
            let error = parse_read(&name, &args).unwrap_err().to_string();
            assert_eq!(error, "id, _id, or name is required", "{key}=");
        }
    }
}
