//! The connector's typed domain errors.
//!
//! A domain failure carries its kind, not just its wording: the application
//! layer classifies a failure by the type it downcasts to, never by matching
//! text. The wording is still what a caller reads, so every type renders the
//! message the untyped error used to carry.

use std::fmt;

/// Caller input the connector refused before it reached Foundry.
///
/// The request never left the gateway, so the failure is a caller mistake
/// and not an upstream fault.
#[derive(Debug)]
pub struct InvalidInput {
    message: String,
}

impl InvalidInput {
    /// The failure message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for InvalidInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for InvalidInput {}

/// Mark a failure as caller input the connector refused.
pub fn invalid_input(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(InvalidInput {
        message: message.into(),
    })
}

/// A change whose outcome is unknown.
///
/// The request may have reached Foundry, but the answer does not prove
/// whether the change was applied: a timeout, a broken socket and an
/// invalid acknowledgement all leave that open. The gateway reports this
/// instead of retrying the change, so a caller learns the outcome by
/// reading the affected document again.
#[derive(Debug)]
pub struct AmbiguousMutation {
    message: String,
}

impl AmbiguousMutation {
    /// The failure message, which states that the change may have been
    /// applied.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for AmbiguousMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AmbiguousMutation {}

/// Mark a failure as a change whose outcome is unknown. The message is
/// shown to the caller unchanged.
pub fn ambiguous_mutation(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(AmbiguousMutation {
        message: message.into(),
    })
}

/// A compendium pack the world does not advertise.
///
/// The reference is a lookup miss: the caller named a pack that does not
/// exist, so the outcome is "not found" rather than an upstream fault.
#[derive(Debug)]
pub struct PackNotFound {
    id: String,
}

impl PackNotFound {
    /// The pack reference the caller named.
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for PackNotFound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Compendium pack \"{}\" not found. Use get_compendiums to list the available packs.",
            self.id
        )
    }
}

impl std::error::Error for PackNotFound {}

/// Mark a failure as a compendium pack the world does not advertise.
pub fn pack_not_found(id: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(PackNotFound { id: id.into() })
}

/// A compendium pack that holds a different document type than the caller
/// asked for.
///
/// The pack exists, so this is not a lookup miss; the request itself is
/// wrong, which keeps it a caller mistake rather than an upstream fault.
#[derive(Debug)]
pub struct PackKindMismatch {
    id: String,
    holds: String,
    expected: String,
}

impl PackKindMismatch {
    /// The pack reference the caller named.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// The document type the pack actually holds.
    pub fn holds(&self) -> &str {
        &self.holds
    }
    /// The document type the caller asked for.
    pub fn expected(&self) -> &str {
        &self.expected
    }
}

impl fmt::Display for PackKindMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Compendium pack \"{}\" holds {} documents, not {}.",
            self.id, self.holds, self.expected
        )
    }
}

impl std::error::Error for PackKindMismatch {}

/// Mark a failure as a pack that holds another document type.
pub fn pack_kind_mismatch(
    id: impl Into<String>,
    holds: impl Into<String>,
    expected: impl Into<String>,
) -> anyhow::Error {
    anyhow::Error::new(PackKindMismatch {
        id: id.into(),
        holds: holds.into(),
        expected: expected.into(),
    })
}

/// Foundry refused the gateway's configured credentials.
///
/// The gateway authenticated the caller; the instance refused the gateway.
/// That is an upstream authentication failure, distinct from a transport
/// failure and from a caller mistake.
#[derive(Debug)]
pub struct UpstreamAuthentication {
    message: String,
}

impl UpstreamAuthentication {
    /// The failure message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for UpstreamAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for UpstreamAuthentication {}

/// Mark a failure as an upstream authentication refusal.
pub fn upstream_authentication(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(UpstreamAuthentication {
        message: message.into(),
    })
}
