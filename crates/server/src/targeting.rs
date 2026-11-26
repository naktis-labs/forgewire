//! Transport-independent target resolution, credential snapshots, and
//! authorization.
//!
//! Both transports resolve the instance a request targets through this one
//! path, so they cannot drift apart on which instance a request may reach:
//!
//! - REST names the instance explicitly (`?instance=<id|item_order>`) or
//!   targets the documented stable default — the first configured entry.
//!   Nothing an MCP client does can redirect a REST request.
//! - MCP uses the session's own selection, or that same stable default when
//!   the session has not chosen (and for anonymous callers).
//!
//! The result is an owned credential snapshot taken once per request, so a
//! later selection can never retarget work that is already in flight, and
//! there is no process-global selection to inherit or mutate.
//!
//! When the deployment has a token configured, the request's credential is
//! issued for exactly one instance: a target outside that scope is refused
//! here, before any Foundry contact. With no token configured there is no
//! scope to enforce and the local posture is unchanged.

use crate::auth::Authentication;
use crate::instances::Instances;
use foundry_connector::FoundryCredential;

/// Why a target could not be resolved.
#[derive(Debug)]
pub enum TargetError {
    /// The reference names no configured instance.
    Unknown(String),
    /// The credential is not issued for the instance the request names.
    OutOfScope,
}

impl std::fmt::Display for TargetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message())
    }
}

impl TargetError {
    /// The message a caller reads.
    pub fn message(&self) -> String {
        match self {
            TargetError::Unknown(message) => message.clone(),
            TargetError::OutOfScope => crate::OUT_OF_SCOPE.to_owned(),
        }
    }
}

impl std::error::Error for TargetError {}

/// The instance a request asks for, before authorization.
#[derive(Clone, Copy, Debug)]
pub enum RequestedTarget<'a> {
    /// A configured instance `id`, or a zero-based `item_order`.
    Reference(&'a str),
    /// The documented stable default: the first configured entry.
    Default,
    /// The instance an MCP session has selected.
    Selected(usize),
}

/// Resolve the target of a request into an owned credential snapshot.
///
/// Resolution is pure: it stores nothing, so selecting an instance can
/// never affect another caller or a request already in flight.
pub fn resolve(
    instances: &Instances,
    requested: RequestedTarget<'_>,
    authentication: &Authentication,
) -> Result<FoundryCredential, TargetError> {
    match requested {
        RequestedTarget::Reference(reference) => {
            let index = instances
                .resolve_reference(reference)
                .map_err(|error| TargetError::Unknown(error.to_string()))?;
            let index = authorize(instances, index, authentication)?;
            instances
                .credential(index)
                .map_err(|error| TargetError::Unknown(error.to_string()))
        }
        RequestedTarget::Default => {
            // The documented stable default is the first configured entry,
            // and it is checked against the credential's scope like any
            // other target.
            authorize(instances, 0, authentication)?;
            Ok(instances.default_snapshot())
        }
        RequestedTarget::Selected(index) => {
            let index = authorize(instances, index, authentication)?;
            instances
                .credential(index)
                .map_err(|error| TargetError::Unknown(error.to_string()))
        }
    }
}

/// Check a resolved instance against the request's credential.
///
/// With no token configured there is no scope to check, and the local
/// posture is unchanged. With a token configured the credential is issued
/// for exactly one instance, so any other instance is refused before the
/// connector is reached.
pub fn authorize(
    instances: &Instances,
    index: usize,
    authentication: &Authentication,
) -> Result<usize, TargetError> {
    match authentication.scope() {
        Some(scope) => {
            let matches = instances
                .matches_scope(index, scope)
                .map_err(|error| TargetError::Unknown(error.to_string()))?;
            if !matches {
                return Err(TargetError::OutOfScope);
            }
            Ok(index)
        }
        None => Ok(index),
    }
}
