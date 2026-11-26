# Foundry Gateway Architecture Implementation Plan

**Goal:** Implement the accepted connector and gateway architecture and provide the reference workflows through both REST and MCP.

**Architecture:** Maintain one authenticated connection per configured identity, with isolated HTTP cookies, a single socket reader and correlated requests. REST and MCP use transport-independent application operations with typed errors and immutable targets; REST is the primary API and MCP selection is client/session scoped.

**Tech stack:** Rust, Tokio, Axum, reqwest, tokio-tungstenite, serde_json.

**ADR:** `docs/adr/0001-foundry-gateway-architecture.md`. The pinned reference is commit `c117882be3922346e4df873d6268d65ddbc68997`; the ADR's capability matrix is the starting baseline, not evidence of completed work.

## Global constraints

- Preserve existing uncommitted work and existing REST routes where feasible. Do not commit unless requested.
- No live mutations or uploads without separate authorization; use a controlled fake Foundry server for mutation tests.
- Preserve default-disabled writes, strict mutation ACK checks, explicit upload overwrite acknowledgement, upload bounds, loopback binding, Host/Origin checks and JSON float round trips.
- Never automatically replay mutations after disconnect; report potentially applied outcomes when a request may have reached Foundry.
- Do not implement URL uploads in this phase. Record the gap and require a separate SSRF/egress security decision. Do not add search, indexing, public event feeds, persistence or other out-of-scope features.
- Use configured agents without model overrides; do not use the `general` agent. Independently review changed authority, destructive operations and concurrency behavior.

## Review focus

- Two credentials for the same hostname must never share cookie jars, sessions or sockets.
- Simultaneous or cancelled requests, `430` world responses, late ACKs and reconnect epochs must not misroute or leak results.
- Disconnect after sending a mutation must return an ambiguous outcome, never retry; a pre-send failure must not claim application.
- One MCP client selecting an instance must not redirect another client, a REST request or an in-flight operation.
- Both adapters must reject malformed inputs and disabled writes consistently while mapping domain errors to appropriate transport outcomes.

## Current execution status

The packages below are the original acceptance checklist, not a claim that every item is complete. Shared operations, instance isolation, persistent read sockets, bounded reconnect and fake-server/transport tests have been implemented and independently reviewed in increments. The capability inventory records partial metadata parity, deferred URL uploads, insecure TLS, and live verification gaps. Every operation kind — world and document reads, document changes, compendium management and file browsing — now runs on the identity's persistent connection with a distinct acknowledgement id per request. The latest authentication-status fix passed scoped review; the inventory records residual retry and coverage limitations. Live mutations and uploads have not been authorized or tested. Consult the inventory for current evidence rather than interpreting unchecked historical tasks as a fresh work queue.

## Work packages

Each package starts with focused failing tests, implements the smallest coherent change, and finishes with its targeted tests. Sequence the packages as shown where their interfaces depend on prior work. File paths for new modules are implementation boundaries, not a requirement to preserve current MCP-oriented names.

### 1. Fake Foundry and baseline inventory

**Files:** connector tests and test-support modules under `crates/foundry-connector`; gateway tests under `crates/server`; capability documentation in `README.md`.

- [ ] Inventory the reference workflows against the ADR matrix and current routes; record intended differences and existing client compatibility.
- [ ] Build a controlled local fake HTTP/Socket.IO Foundry supporting status, join/authentication, session binding, configurable world ACKs, and recorded request frames on an ephemeral loopback port. The initial client read test proves status, join and WebSocket traffic reach it; ping/pong and disconnect controls remain follow-up work.
- [ ] Add baseline tests demonstrating per-operation socket creation and current identity/selection limitations without exposing credentials in output.

### 2. Identity and authenticated connection lifecycle (P0)

**Files:** `crates/foundry-connector/src/lib.rs`, new identity/connection modules, connector tests and dependency configuration as needed.

- [ ] Define an identity-scoped connection manager and separate HTTP client/cookie store per configured credential, including credentials sharing a hostname. Resolve `userid` or `username` from join-user data; reject missing, ambiguous or inconsistent identities.
- [ ] Test v13/v14 session binding and require the correct bound `session` event before readiness. Ensure logs and errors omit passwords, cookies and session-bearing URLs; document legacy query logging risk.
- [ ] Implement one inbound socket reader and serialized sender per connection. Correlate distinct ACK IDs using a bounded pending registry; serialize or otherwise safely correlate special `430` world requests. Deliver unsolicited frames to a bounded internal event interface; ignore unknown frames without disrupting requests.
- [ ] Test interleaved ACKs, concurrent world requests, ping/pong, unsolicited frames, registry backpressure, timeouts and cancellation cleanup using the fake server.
- [ ] Add bounded reconnect with jitter, connection epochs and stale-task/frame rejection. On disconnect settle all waiters; distinguish definitely-unsent from potentially-applied mutations and never replay writes. Test disconnect at both sides of send, reconnection, stale epochs and session rebinding.

### 3. Shared application boundary and targets (P0)

**Files:** `crates/server/src/main.rs`, `crates/server/src/instances.rs`, new application/operation modules; existing `read_tools.rs`, `file_tools.rs`, `compendium_tools.rs` refactored as appropriate.

- [ ] Define transport-neutral operation inputs, immutable target snapshots, typed domain errors (invalid input, forbidden, not found, authentication, upstream/transport, potentially applied), and a mockable connector interface. Put validation, authorization and result shaping here, not in adapters.
- [ ] Refactor existing world, collection and document-write REST and MCP handlers through shared operations; remove duplicated REST write validation and direct adapter-to-connector calls. Preserve existing REST endpoints and test equivalent success, invalid input, forbidden write and ambiguous failure through both transports.
- [ ] Make REST use explicit instance targeting or a documented stable default, never MCP's mutable selection. Provide MCP client/session-scoped selection, including session creation, identification, expiry and unknown-session behavior; selection alone must not report a verified connection.
- [ ] With a paused fake connector, start an operation against A, select B concurrently, then assert the first operation uses A. Test two independent MCP sessions and REST target isolation; ensure instance listing never exposes secrets.

### 4. Reference workflows and REST parity (P1)

**Files:** shared application modules, `crates/server/src/main.rs`, read/file/compendium modules, connector read code, gateway contract tests and `README.md`.

- [ ] Expose shared individual lookup, filtered/projected and byte-limited reads, compendium listing and reads, compendium creation/deletion, instance discovery/selection, file browsing and byte uploads through REST and MCP. Define and document REST routes and input/response shapes; retain old routes where feasible.
- [ ] Add contract tests for each workflow through both adapters against the fake backend, including embedded and compendium targeting, upload limits/overwrite, disabled writes, not-found and malformed inputs. Assert matching application outcomes and typed errors, not identical transport envelopes.
- [ ] Restore reference-relevant world metadata with bounded module information, excluding sensitive state and collection bodies. Compare fixture results to pinned reference behavior.
- [ ] Push supported scalar filters and ID/name queries into world and pack reads; apply complete local filtering and projection afterward. Implement efficient byte-limiting with stable order and truthful truncation. Test nested filters, UTF-8 boundaries, ordering, numeric round trips and targeted query frames.
- [ ] Document the optional insecure TLS compatibility gap without weakening the secure default, and record remote URL upload as deferred rather than implemented.

### 5. Verification, review and documentation (P1)

**Files:** all affected code and tests; `README.md`; capability matrix or a linked maintained matrix.

- [ ] Independently review the actual identity isolation, connection lifecycle, shared write authorization, destructive behavior and concurrent targeting changes; resolve concrete findings.
- [ ] Update the matrix with observed evidence and classify each reference workflow as equivalent, partial, missing or intentional deviation for each transport. Explicitly disclose URL uploads and optional insecure TLS gaps, and unverified live mutations.
- [ ] Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and `git diff --check`. Report command outcomes and limitations.
- [ ] Where permitted, verify reads against a non-production Foundry instance. Do not perform live mutations or uploads without separate authorization; record any versions or paths not tested.
