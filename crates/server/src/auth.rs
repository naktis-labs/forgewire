//! Instance-scoped bearer authentication.
//!
//! The gateway may be configured with one bearer token
//! (`FORGEWIRE_BEARER_TOKEN`). The token is not a bare secret: it is issued
//! for one *scope* — one configured Foundry instance — and a request must
//! present that scope together with the secret. A token minted for a
//! different instance therefore does not authenticate here, even when its
//! secret is the same: the scope is checked explicitly, not folded into a
//! single opaque comparison.
//!
//! The presented credential is `<scope>.<secret>`:
//!
//! - `<scope>` is the canonical identifier of the configured instance the
//!   token was issued for (its configured `id`, or its zero-based
//!   `item_order` when it has no `id`). It may not contain `.`, which keeps
//!   the split unambiguous, and it is restricted to RFC 6750 b64token
//!   characters so the whole credential stays a valid b64token.
//! - `<secret>` is the configured `FORGEWIRE_BEARER_TOKEN` value, compared in
//!   constant time.
//!
//! Authentication is optional for reads: with no token configured the
//! gateway keeps its loopback-only, Host/Origin-checked local posture.
//! Writes always require a valid, in-scope token (see
//! [`operations::WRITES_REQUIRE_AUTHENTICATION`]).

use subtle::ConstantTimeEq;

/// The scope a bearer token is issued for: the canonical identifier of one
/// configured Foundry instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scope(String);

impl Scope {
    /// Validate a scope name. Scopes are dot-free so that `<scope>.<secret>`
    /// splits unambiguously at the first dot, and are restricted to b64token
    /// characters so the presented credential stays a valid RFC 6750
    /// b64token.
    pub fn new(value: &str) -> anyhow::Result<Self> {
        if value.is_empty() {
            anyhow::bail!("instance scope must not be empty");
        }
        if value.contains('.') {
            anyhow::bail!("instance scope must not contain '.'");
        }
        if !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'~' | b'+' | b'/'))
        {
            anyhow::bail!(
                "instance scope must consist of letters, digits, '-', '_', '~', '+' or '/'"
            );
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A bearer token bound to one instance scope.
#[derive(Clone, Debug)]
pub struct ScopedToken {
    scope: Scope,
    secret: String,
}

impl ScopedToken {
    /// Bind a secret to a scope. The secret follows RFC 6750 b64token
    /// syntax, with padding allowed only at the end. The configured value is
    /// kept verbatim: clients present exactly what the operator configured.
    pub fn new(scope: Scope, secret: String) -> anyhow::Result<Self> {
        let unpadded = secret.trim_end_matches('=');
        if unpadded.is_empty()
            || !unpadded.bytes().all(|b| {
                b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
            })
        {
            anyhow::bail!("invalid FORGEWIRE_BEARER_TOKEN configuration");
        }
        Ok(Self { scope, secret })
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// Verify a presented bearer credential. The credential must carry this
    /// token's scope and its secret: a credential issued for another
    /// instance is refused even when the secret matches.
    pub fn verify(&self, presented: &str) -> bool {
        let Some((scope, secret)) = presented.split_once('.') else {
            return false;
        };
        // The scope is an identifier, not a secret: it is compared as a
        // string. The secret is compared in constant time.
        scope == self.scope.as_str() && bool::from(secret.as_bytes().ct_eq(self.secret.as_bytes()))
    }
}

/// What the authentication layer decided about one request. The marker is
/// attached to the request so the shared application layer can require
/// authentication for writes without either transport adapter re-deciding
/// it, and so the verified *scope* travels with the request: a credential
/// issued for one instance may only reach that instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Authentication {
    /// The request presented a valid token, and it was issued for this
    /// scope.
    Authenticated(Scope),
    /// The request presented no token (the deployment has none configured).
    Unauthenticated,
}

impl Authentication {
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Authentication::Authenticated(_))
    }

    /// The verified scope of an authenticated request. It is `None` only
    /// when the deployment has no token configured, which keeps the local
    /// posture of unauthenticated reads unchanged.
    pub fn scope(&self) -> Option<&Scope> {
        match self {
            Authentication::Authenticated(scope) => Some(scope),
            Authentication::Unauthenticated => None,
        }
    }
}
