# ADR: Reference parity and core gateway architecture

- **Status:** Accepted for implementation
- **Date:** 2026-11-23
- **Project:** Forgewire
- **Scope:** Core Foundry connector, application operations, REST and MCP adapters
- **Reference baseline:** [TheStranjer/foundry-vtt-mcp at c117882](https://github.com/TheStranjer/foundry-vtt-mcp/tree/c117882be3922346e4df873d6268d65ddbc68997)
- **Forgewire baseline:** [`macho-moth` at 1bc7ce7](https://github.com/naktis-labs/forgewire/tree/1bc7ce79660d4cabd1c714e5e0becdebcb0fec28)
- **Related decision:** [Functional parity through shared application operations](./2026-11-22-functional-parity-adr.md). This ADR extends that proposal with connection lifecycle, instance isolation, explicit parity policy, and scope boundaries. Where the documents conflict on architecture or sequencing, this decision governs.

## Context

Forgewire replaces the TypeScript reference's Foundry-facing MCP integration with a Rust connector and an HTTP gateway. The reference is the behavioral source for supported Foundry operations and protocol details, **not** a requirement to reproduce its TypeScript structure, MCP-only surface, error messages, or security weaknesses.

The existing `macho-moth` implementation already handles Foundry v13/v14 session-binding variants, reads and writes through Socket.IO / Engine.IO v4, compendium management, file browsing and binary uploads, and MCP over HTTP. It has useful validation and acknowledgement tests and deliberately disables writes by default.

However, it authenticates and opens a new WebSocket for each operation, handles only the corresponding response, stores HTTP cookies in a shared client, and tracks a process-wide selected credential. Only a subset of the MCP operations has REST equivalents. Some reference capabilities and response semantics are missing, including resolving a user from either `userid` or `username` and returning useful world metadata beyond title, system, and version.

These differences impede parity and make the existing connector the wrong foundation for later *unsolicited* Foundry document updates. At the same time, the current phase is **not** an authorization to build a document cache, indexer, search service, or public change-feed API.

## Decision drivers

1. Preserve the reference's useful client workflows across supported Foundry versions and systems, while documenting intentional incompatibilities.
2. Make the Rust connector long-lived, concurrent, reconnectable, and able to route unsolicited protocol frames internally.
3. Make the application layer the single authority for validation, instance targeting, mutation gating, operation semantics, and domain errors.
4. Expose existing capabilities through REST and MCP without making either adapter call the other.
5. Prevent credential or operation mixing between Foundry identities and concurrent gateway clients.
6. Retain conservative defaults for destructive operations and network-facing surfaces.
7. Keep the implementation phase bounded to correctness, parity, and core architectural foundations; defer feature expansion.

## Decision

### D1. Use capability parity, not implementation or wire-schema parity

The pinned TypeScript reference defines the **baseline behaviors**. Track each operation as `equivalent`, `partial`, `missing`, or `intentional deviation`, separately for REST and MCP where appropriate. Equivalent client workflows may use different route names, parameter encodings, and envelopes, but must preserve relevant Foundry results, documented errors, and authorization semantics.

The reference is not an oracle on safety or correctness. Retain stricter Foundry write acknowledgement validation, no automatic retry of uncertain mutations, default-disabled writes, loopback-only unauthenticated binding, bounded binary uploads, and explicit overwrite acknowledgement. Document these as intentional deviations, not regressions.

Do not claim complete reference parity while a required capability is missing. Record remaining URL upload and optional insecure-TLS differences explicitly. New document kinds and operations beyond the reference are outside this phase.

### D2. Introduce a persistent connection manager per Foundry identity

The connector owns the HTTP authentication session and **one reusable, bound Socket.IO connection per active configured Foundry identity**, rather than opening a new connection for each operation. A connection is keyed by a stable credential/instance identifier, never by the gateway's mutable global selection alone.

A connection task:

- Performs version detection, `GET /join`, `POST /join`, socket upgrade, Engine.IO namespace connect, and verified `session` binding.
- Serializes outbound sends and assigns unique acknowledgement IDs for outstanding requests on that connection.
- Runs one inbound frame reader that handles Engine.IO ping/pong, Socket.IO control frames, requested responses, and unsolicited Foundry events.
- Correlates `43<ack>[...]` replies with pending requests through a bounded request registry; the `430` world reply is handled according to its actual protocol shape.
- Exposes unsolicited frames as **internal** typed or raw notifications without assuming they form a reliable event log. Unknown event types must not interrupt normal request dispatch.
- Applies per-operation deadlines, bounded pending queues, cancellation cleanup, and backpressure. A cancelled or timed-out waiter must be removed.
- Detects disconnects, fails or invalidates pending operations with correct ambiguity semantics, and reconnects using bounded exponential backoff with jitter.
- Uses a connection generation/epoch so late replies, old-socket closure callbacks, and stale reconnection attempts cannot complete requests or replace a newer connection.
- Revalidates session binding after reconnection before accepting work, and exposes connection readiness truthfully.

Do not replay a write automatically after a disconnect or timeout. A failed read can be retried only under a deliberate read-only retry policy. A mutation that might have been applied returns an explicit ambiguous-outcome error.

Incoming unsolicited messages are **routed and testable now**. Interpreting document updates, reconciling cached documents, persistence, indexing, or exposing public subscriptions are separate future decisions.

### D3. Separate credentials, connection state, and client selection

Do not share cookie jars between Foundry identities. Prefer explicit per-session cookie handling or per-identity HTTP clients, with no accidental reuse when two credentials target the same hostname. Never put passwords or session IDs in API responses or logs. For v13 session-in-query compatibility, avoid URL logging and document the residual proxy-log risk.

Restore the reference's ability to accept **either `userid` or `username`**, resolving the missing field using the Foundry join-user data where available. Reject missing, ambiguous, or contradictory identities clearly. Preserve strict validation and authenticated-session checks.

Connection ownership is keyed by Foundry identity; request targeting is explicit and immutable for the lifetime of an operation. REST selects the configured instance per request or resource namespace, not through a process-global mutable default. MCP's `choose_foundry_instance` workflow must retain equivalent behavior, but selection must be scoped to a client/session rather than silently redirecting other clients. Where transport sessions are required, implement the minimal session context needed for correct isolation; do not fake per-client isolation with a global lock. A configured default may be used only as an explicit fallback when no target is supplied.

A switch reports **selected** only after selection and **connected** only after successful authentication and binding. An in-flight operation remains bound to its original instance snapshot.

### D4. Put transport-neutral operations between the connector and both APIs

Introduce a shared application operations layer for:

- World metadata and world collection reads, single-object lookups, filtering, requested-field projection, and byte-limited results.
- Compendium discovery, filtering, document reads, and world compendium create/delete.
- World, embedded, and supported compendium document create/modify/delete.
- Configured-instance discovery and selection.
- Foundry file browsing and caller-provided byte uploads.

Each operation owns normalized inputs, Foundry target resolution, credential snapshot, write permission checks, connector invocation, validated results, and typed domain errors. REST and MCP adapters handle only transport parsing, schema negotiation, serialization, and status/error mapping. Neither adapter calls the other or bypasses shared operations for a Foundry action.

REST is the **primary public application API**. Existing endpoints should remain usable where feasible while the missing capabilities gain REST equivalents. MCP is a first-class, thin adapter to those same operations, not the internal source of truth. Do not invent new game-domain capabilities to fill out the REST surface.

### D5. Preserve efficient Foundry reads and meaningful metadata

Restore baseline behavior for `get_world`: return the reference's useful, non-collection world metadata, including relevant trimmed module metadata, rather than just `title`, `system`, and `version`. Explicitly exclude collection bodies and sensitive session/credential state.

World collection reads should push supported top-level scalar predicates into the Foundry `modifyDocument` `get` query, then apply the complete requested filter locally for parity. Single-document lookups should send a targeted query instead of fetching whole collections when supported. Keep compendium queries similarly targeted.

Field projection, filtering, and response-size limits must be consistent between REST and MCP. Avoid serializing and shrinking full output arrays repeatedly; use bounded construction or an equivalently tested linear-time strategy. Preserve deterministic truncation semantics and return a truthful indication when data is incomplete if the API contract supports it.

Preserve JSON numeric round-trip behavior where significant to existing Foundry payloads.

### D6. Make operational safety and errors explicit

Use typed errors that distinguish invalid input, missing resources, forbidden writes, authentication/connection failures, upstream failures, and **ambiguous mutation outcomes**. REST must not map caller validation errors to HTTP 502; MCP should preserve useful tool-level failure information. Timeout after sending a mutation must never be reported as definite failure or silently retried.

Preserve:

- `FOUNDRY_ALLOW_WRITES=true` opt-in for mutations.
- Acknowledgement of possible overwrite for uploads.
- Size, path, filename, and response bounds.
- Loopback-only listener, Host/Origin checks, and no implied remote authentication.
- Protection of credentials and session material in errors, traces, request logs, and tests.

Public non-loopback exposure, authorization delegation, and browser-origin access require a separate security decision. Untrusted URL fetching remains disabled until an explicit design covers SSRF, redirects, DNS changes and address pinning, proxy bypass, streaming limits, timeouts, and deployment egress controls.

## Baseline capability matrix

Symbols: **E** = implemented/equivalent in the indicated surface at a static-review level; **P** = partial or needs behavioral verification; **M** = missing; **D** = deliberate deviation. These are source-review judgments, **not** claims of successful live integration testing.

| Reference capability | Forgewire MCP | Forgewire REST | Current-phase disposition |
| --- | --- | --- | --- |
| Foundry v13/v14 login and WebSocket session binding | P | P | Retain and test; use shared persistent connection |
| User identity from either username or user ID | M | M | Restore parity |
| Reusable connection, heartbeat and reconnect | M | M | **Core architecture: D2** |
| Concurrent request/ACK multiplexing | P | P | **Core architecture: D2** |
| World metadata (`get_world`) | P | P | Restore useful reference payload |
| List 13 world document collections | E | E | Verify protocol and response parity |
| Get individual documents by ID or name | E | M | Add REST equivalent |
| Where filters, field projection, result byte cap | P | M | Query pushdown, shared processing, REST equivalent |
| List and filter compendium packs | E | M | Add REST equivalent |
| Read individual/listed compendium documents | E | M | Add REST equivalent |
| Create, modify and delete supported documents | P | P | Normalize validation and error semantics |
| Embedded/pack-targeted document mutations | P | P | Contract-test parent/pack targeting |
| List configured instances without secrets | E | M | Add REST equivalent |
| Switch Foundry instance | P | M | Verify connection and isolate selection |
| Browse Foundry data files | E | M | Add REST equivalent |
| Upload caller-supplied base64/bytes | D | M | Keep stricter bounds and overwrite gate; add REST |
| Fetch URL then upload | M | M | **Deferred security-sensitive parity gap** |
| Optional insecure TLS for trusted self-signed hosts | M | M | Document limitation; not required to weaken defaults |
| Upstream MCP stdio transport | D | D | Keep Forgewire HTTP/MCP transport |
| REST as an independent application API | N/A | P | **Core architecture: D4** |

The reference tool list and connector behavior are documented in [`src/server-tools.ts`](https://github.com/TheStranjer/foundry-vtt-mcp/blob/c117882be3922346e4df873d6268d65ddbc68997/src/server-tools.ts) and [`src/foundry-client.ts`](https://github.com/TheStranjer/foundry-vtt-mcp/blob/c117882be3922346e4df873d6268d65ddbc68997/src/foundry-client.ts). Forgewire's current implementation is in [`crates/foundry-connector`](https://github.com/naktis-labs/forgewire/tree/1bc7ce79660d4cabd1c714e5e0becdebcb0fec28/crates/foundry-connector) and [`crates/server`](https://github.com/naktis-labs/forgewire/tree/1bc7ce79660d4cabd1c714e5e0becdebcb0fec28/crates/server).

## Implementation sequence

1. **P0: Connection foundation (D2, D3).** Build per-identity authentication and persistent connection ownership, message dispatch, bounded ACK registry, heartbeat/reconnect handling, and session isolation. Preserve the existing testable protocol framing.
2. **P0: Shared application boundary (D4, D6).** Unify validation, authorization, target snapshots, typed errors, and ambiguous-write behavior. Retire connector calls originating independently in adapters.
3. **P1: Restore existing reference workflows (D1, D5).** Add REST equivalents for already implemented MCP operations; restore identity resolution and world metadata; restore targeted queries and efficient result limits. Retain explicit safety deviations.
4. **P1: Contract and lifecycle verification.** Execute fixture-backed and controlled integration tests for both adapters, connector lifecycle, and instance switching. Update README and capability matrix based on observed results, not intent.

Do not claim any phase complete because its types or endpoints compile. Completion depends on behavior under concurrent requests and disconnects.

## Acceptance criteria

- [ ] A single bound connection can serve concurrent Foundry reads, correlate interleaved ACK IDs, respond to ping, and route unrelated unsolicited frames without losing requests.
- [ ] Forced close, restart, failed session binding, and selection during reconnect cannot attach the wrong socket to an instance. Pending operations complete, fail, or report uncertainty correctly; no mutation is replayed.
- [ ] Two credentials at the same Foundry hostname never share authentication cookies or requests. Client A's selection cannot redirect client B's subsequent request.
- [ ] A configuration with only `userid` or only `username` can authenticate when the reference's supported identity lookup is available; ambiguous and invalid identities fail safely.
- [ ] REST and MCP invoke the same application operations. Contract tests compare valid results, malformed input, forbidden writes, pack and embedded targeting, upload bounds, and error classifications.
- [ ] World metadata, targeted collection/compendium reads, field projection, filters, and response caps match the baseline's meaningful behavior, with documented deviations.
- [ ] Existing Foundry writes stay disabled unless explicitly enabled; uncertain mutation outcomes are distinguishable and never auto-retried.
- [ ] HTTP remains loopback-only and unauthenticated by design for this phase; no secrets appear in public responses or logged URLs.
- [ ] Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and source/diff checks. Validate on a non-production Foundry world; no live mutations without separate approval.
- [ ] Document unresolved compatibility gaps explicitly; do not label the project "fully parity-complete" while URL fetching, insecure-TLS configuration, or any other reference capability remains missing.

## Explicitly deferred

The connection manager may **observe** unsolicited document messages, but this ADR does **not** authorize a synchronized document cache, durable event history, document reconciliation, REST/SSE/WebSocket event feeds, full-text or vector search, embeddings, hybrid rank fusion, an indexing pipeline, additional Foundry document kinds, or general-purpose Foundry feature expansion.

These require future ADRs covering ownership and correctness of derived state, visibility/permission changes, indexing consistency, reconnect catch-up, persistence, and resource budgets. In particular, unsolicited events are hints, **not** a guarantee of complete history.

URL-to-upload support and untrusted-network deployment are likewise deferred pending independent security decisions. Deferred parity gaps remain recorded rather than silently treated as completed.

## Alternatives considered

1. **Keep per-request connections and add features around them. Rejected.** Simpler in the short term, but prevents continuous unsolicited-event observation, duplicates authentication overhead, and complicates concurrency.
2. **Port the TypeScript implementation structurally to Rust. Rejected.** Preserves some behavior but also inherits per-message listener patterns and global selection semantics unsuitable for a multi-client gateway.
3. **Implement REST as an MCP proxy, or MCP as an HTTP client to REST. Rejected.** Couples transports and duplicates framing/error translation. Both should invoke the application layer directly.
4. **Use one global connection and selected instance. Rejected.** Conflates independent clients and risks cross-world operations.
5. **Build cache, search and public event streams immediately. Deferred.** Changes the task from reliable gateway parity to a new data product before the connector's semantics are stable.
6. **Copy upstream URL-fetch behavior without security review. Rejected.** A direct translation would introduce an avoidable SSRF and resource-consumption surface.

## Consequences and trade-offs

The persistent connection manager and instance-scoped session model require more concurrency control, cleanup, lifecycle tests, and potentially MCP session tracking. They eliminate repeated authentication and provide an internal protocol event boundary without prematurely deciding how future search or synchronization will work.

Shared application operations require a refactor and some endpoint migration but reduce long-term REST/MCP drift and centralize destructive-operation policy. REST is the primary API without requiring wire compatibility with upstream MCP schemas.

Forgewire will intentionally remain narrower than the reference for unsafe or unverified capabilities. These limitations are explicit and are not reasons to weaken existing safety measures merely to mark a matrix cell complete.

## Verification and follow-up

This ADR records architectural decisions and a **static** capability comparison; it does not assert that Rust tests, Clippy, end-to-end Foundry compatibility, or live mutations were executed as part of this document change.

The implementation should maintain the matrix as test evidence is accumulated, and add a separate ADR before introducing persistent derived state, semantic search, or a publicly exposed event stream.
