# ADR-0001: Foundry connector and gateway architecture

- **Status:** Accepted
- **Date:** 2026-11-23
- **Scope:** Reference parity and core infrastructure (not new features)
- **Reference:** [TheStranjer/foundry-vtt-mcp at c117882](https://github.com/TheStranjer/foundry-vtt-mcp/tree/c117882be3922346e4df873d6268d65ddbc68997)
- **Reviewed Forgewire baseline:** [`macho-moth` at 1bc7ce7](https://github.com/naktis-labs/forgewire/tree/1bc7ce79660d4cabd1c714e5e0becdebcb0fec28)

## Context

Forgewire is a Rust Foundry VTT integration gateway offering REST and MCP over HTTP. The TypeScript reference defines the *functional and protocol baseline*, not an architecture, wire-schema, or security policy that must be copied.

At the reviewed baseline, Forgewire implements version-sensitive Foundry session binding, document/compendium operations, file browsing and byte uploads, and an MCP adapter. It also deliberately gates writes. However, each operation opens a new authenticated WebSocket; one shared HTTP client retains cookies across credentials; instance selection is process-global; REST lacks many MCP capabilities; username-only and user-ID-only credentials are unsupported; and `get_world` returns only three fields. Unsolicited messages cannot be observed continuously.

This ADR **consolidates** the earlier functional-parity proposal (2026-11-22) and core-architecture decision (2026-11-23) into one authoritative decision. Their history remains in Git. No search, indexing, or derived-state features are approved by this ADR.

## Decision

### 1. Parity is behavioral, with explicit safety deviations

Preserve the reference's supported client workflows, including world/collection reads, ID/name lookups, field projection and filtering, compendium operations, world/embedded/pack mutations, credential selection, file browsing, and uploads of caller-provided bytes. Support these workflows in **both REST and MCP** through a shared application layer.

Equivalent workflows need not reuse upstream tool names, parameter syntax, or response envelopes. Test results and meaningful validation/authorization/mutation semantics instead. Record each difference as *equivalent*, *partial*, *missing*, or *intentional deviation*. The reference is not an authority for unsafe behavior.

Retain default-disabled writes, strict acknowledgement validation, bounded uploads, overwrite acknowledgement, and loopback-only unauthenticated HTTP. Do not describe these safeguards as parity defects. Do not claim complete reference parity while documented gaps remain.

### 2. Keep a persistent, identity-scoped Foundry connection

Replace per-operation socket creation with a reusable, authenticated Socket.IO / Engine.IO v4 connection per configured Foundry identity. Its lifecycle is independent of incoming REST or MCP requests.

- Authenticate with `/join`, detect v13/v14 binding differences, and verify the bound `session` event before marking a connection ready. Require an exact `sessionId`; if `userId` is present it must be a matching string, but accept its absence for Foundry compatibility. Without that field the event verifies the session, not the user identity.
- Run a **single inbound frame reader** and serialized sender. Handle Engine.IO ping/pong, protocol controls, world responses, and ACK-bearing replies.
- Assign distinct IDs to concurrent requests. Route `43<ack>[...]` replies through a bounded pending-request registry; accommodate the protocol's special `430` world response without confusing concurrent world requests.
- Forward unsolicited frames to an **internal** bounded event interface; unknown frames must not disrupt request handling. This is a protocol foundation, not a journal synchronization engine.
- Define request deadlines, cancellation cleanup, backpressure, and bounded reconnect with jitter. Identify sockets by connection epoch so late frames or obsolete reconnect tasks cannot affect a successor.
- On disconnect, settle outstanding operations correctly. Do **not** automatically replay mutations. If a request might have reached Foundry, report an ambiguous outcome.

Unsolicited frames are not a complete event log. Reconciliation, persistence, subscriptions, and event interpretation require a later decision.

### 3. Isolate identity, targeting, and selection

Each Foundry identity owns independent session-cookie and connection state, including when multiple credentials target the same hostname. Avoid shared cookie jars and logging of passwords, cookies, or session-bearing URLs. Document the residual session-query logging risk for legacy Foundry versions.

Accept either `userid` or `username` in configuration and resolve the missing identity against Foundry's join-user data when available; fail clearly for missing, inconsistent, or ambiguous identities. Never infer that selection alone proves a connection is healthy.

Every operation captures an immutable target and credential snapshot. REST targets an instance explicitly (or uses a documented default), **not** a mutable global active instance. Preserve MCP's `choose_foundry_instance` workflow with client/session-scoped selection so one client cannot redirect another. If session tracking is needed to deliver that behavior, implement it rather than pretending a process-wide lock provides isolation. Swapping instances must not retarget in-flight work.

### 4. Use shared application operations, with REST as the primary API

Introduce transport-independent operations responsible for:

- Input normalization and domain validation.
- Instance resolution and credential snapshot selection.
- Authorization/write gating and destructive-operation policy.
- Connector invocation, result shaping, and typed domain errors.

Cover the reference workflows: world/collections and single-document reads; filters, projection and byte caps; compendium list/read/create/delete; world, embedded and pack document writes; configured-instance discovery/selection; file browsing; and byte uploads. Existing REST routes remain usable where feasible.

REST and MCP handlers perform only protocol parsing, content negotiation, transport errors/status mapping, and serialization. **Neither adapter calls the other**, and neither accesses Foundry outside the shared application operations. Add REST equivalents to the existing MCP-only capabilities; don't add unrelated Foundry features in this phase.

### 5. Preserve efficient reads, correct errors, and safe defaults

`get_world` must expose the reference's useful non-collection metadata (including bounded/trimmed module information), not only title/system/version; exclude sensitive state and document collection bodies.

Push supported scalar filters and targeted ID/name lookups into Foundry `modifyDocument` read queries, then apply complete local filtering and requested-field projection for consistent results. Keep compendium queries targeted. Apply byte limits without repeatedly serializing/shrinking an entire array; retain stable ordering and truthful truncation semantics. Preserve significant JSON floating-point round trips.

Use typed errors for invalid input, forbidden writes, not found, authentication, transport/upstream failure, and *potentially applied mutation*. Invalid caller input must not surface as an upstream 502. Timeouts or invalid mutation acknowledgements do not prove the mutation failed.

Retain `FOUNDRY_ALLOW_WRITES=true` as explicit opt-in, upload size/path/name checks, affirmative overwrite acknowledgement, loopback-only binding, and Host/Origin checks. Any public listener, external authentication, or browser access requires a separate security decision. Live Foundry mutations and uploads require separate authorization.

**Do not implement URL uploads in this phase.** The reference supports URL-to-upload, but a new implementation needs a separately reviewed SSRF/egress policy: redirects, DNS rebinding and connection pinning, proxy behavior, stream/size/time bounds, credential separation, and deployment egress controls. Application-level address filtering alone is insufficient.

## Capability matrix at the reviewed baseline

**E** = implemented, pending live verification; **P** = partial or unverified behavior; **M** = missing; **D** = intentional difference. REST and MCP are scored by available client workflow, not by transport-internal code.

| Reference workflow | MCP | REST | Disposition |
| --- | --- | --- | --- |
| v13/v14 authentication and session binding | P | P | Test with persistent connections |
| Resolve `userid` or `username` | M | M | Restore parity |
| Reusable socket, heartbeat, reconnection, concurrent ACKs | M | M | Core connection architecture |
| World metadata | P | P | Restore reference-relevant fields |
| List 13 world collections | E | E | Verify results |
| Single lookup by ID/name | E | M | REST parity |
| Filters, field projection, byte-limited results | P | M | Query pushdown and REST parity |
| Compendium listing/filtering and document reads | E | M | REST parity |
| Compendium create/delete | E | M | REST parity |
| World, embedded and pack document writes | P | P | Shared validation and contract tests |
| List configured instances without secrets | E | M | REST parity |
| Select instance | P | M | Client isolation and connection verification |
| Browse Foundry files | E | M | REST parity |
| Upload caller-provided bytes | D | M | Preserve size/overwrite safeguards; REST parity |
| URL-to-upload | M | M | **Deferred** pending security ADR |
| Optional insecure TLS for trusted self-signed hosts | M | M | Document compatibility gap; secure default |
| MCP transport | D | N/A | Keep Streamable HTTP rather than upstream stdio |
| REST service layer | N/A | P | Make primary API using shared operations |

The matrix is a **static source review**, not proof of successful Foundry integration or a completed feature. Source baseline: [reference tool definitions](https://github.com/TheStranjer/foundry-vtt-mcp/blob/c117882be3922346e4df873d6268d65ddbc68997/src/server-tools.ts), [reference connector](https://github.com/TheStranjer/foundry-vtt-mcp/blob/c117882be3922346e4df873d6268d65ddbc68997/src/foundry-client.ts), [Forgewire connector](https://github.com/naktis-labs/forgewire/tree/1bc7ce79660d4cabd1c714e5e0becdebcb0fec28/crates/foundry-connector).

## Implementation and verification

1. **P0: Socket and identity lifecycle.** Implement per-identity connections, ACK multiplexing, unsolicited frame dispatch, reconnect epochs, credential isolation, and authenticated readiness.
2. **P0: Shared application boundary.** Move authorization, validation, target snapshots, typed errors, and write acknowledgement semantics out of both transport adapters.
3. **P1: Complete existing workflows.** Restore credential resolution, useful world metadata and targeted queries; implement REST equivalents of the reference-backed MCP operations.
4. **P1: Verify and document.** Add controlled fake-Foundry tests and cross-adapter contracts, update the matrix from observed evidence, and document remaining gaps.

**Acceptance checks:**

- Two credentials against the same host never share sessions; different clients cannot change each other's instance target; concurrent operations preserve their initial target.
- Interleaved ACKs and unsolicited frames dispatch correctly; ping/pong, cancellation, disconnection, stale reconnects and session rebinding are covered.
- Ambiguous mutation outcomes are reported and never retried automatically; forbidden writes remain forbidden through both transports.
- Equivalent REST/MCP calls match on successful results, rejected inputs, filtering/projection/limits, embedded and compendium targeting, and errors.
- World metadata and read query behavior match the pinned reference where intended. Deviations are explicit.
- Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` and `git diff --check`. Validate against a **non-production** Foundry world. Tests and live verification have **not** been run as part of this ADR migration.

## Out of scope

No document cache, reconciliation engine, persistent derived state, journal-specific event processing, public SSE/WebSocket feed, full-text or vector search, embeddings, indexing pipeline, expanded document types, or other new Foundry features. The connector only establishes the ability to *receive* unsolicited protocol events for future work.

Remote URL fetching and public-network deployment also require independent design and security review. Do not quietly treat deferred capabilities as delivered parity.

## Alternatives and consequences

- **Per-request sockets:** rejected because they discard unsolicited events and repeatedly authenticate.
- **Literal Rust port of TypeScript internals:** rejected because the reference's listeners and global selection do not fit a concurrent multi-client gateway.
- **Independent business logic in REST and MCP, or adapter-to-adapter proxies:** rejected to avoid validation and authorization drift.
- **Immediate search/index/event-feed implementation:** deferred until the connection and operation semantics are reliable.

This design costs more lifecycle and concurrency engineering upfront but produces one bounded, testable Foundry integration layer and one shared operation model. REST can evolve independently of MCP without divergent permissions or business logic. Remaining upstream differences are documented rather than hidden.
