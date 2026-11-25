# ADR-0001 implementation inventory

Reference: `TheStranjer/foundry-vtt-mcp` commit `c117882be3922346e4df873d6268d65ddbc68997`. This is a source inventory, not evidence that live Foundry operations work. E = exposed, P = partial, M = missing, D = intentionally restricted.

Updated after the shared-operations and connection-lifecycle increments. Evidence is fixture-based unless stated otherwise.

| Capability | MCP | REST | Evidence and remaining work |
| --- | --- | --- | --- |
| World metadata | P | P | Shared operation exposes selected world fields, safe author details and bounded module metadata. The allowlist intentionally omits author email and other potentially sensitive fields and excludes collection bodies; top-level release generation and build are exposed, while other release and world-package metadata remain unexposed; bounded descriptions may be truncated. Live payload compatibility remains unverified. |
| Collection reads | E | E | Shared operation; both call the connector. |
| Single lookup, filters, projection, byte limits | E | E | Shared `read` operation with contract tests asserting identical connector calls and results. |
| Compendium list and document reads | E | E | Shared operation; contract-tested. |
| Compendium create/delete | E | E | Shared operation, write gate enforced before validation and connector contact. |
| Document create/update/delete | P | P | Shared `write_document`; strict ACK validation, ambiguity reporting, live mutations unverified. |
| Instance listing/selection | P | P | Listing shared and secret-free. REST targets an instance explicitly with `?instance=<id|item_order>` or the stable first-entry default; there is no process-global REST selection endpoint. MCP selection is scoped to a server-issued `Mcp-Session-Id` session (bounded table, unknown sessions get 404, headerless selection is refused). Contract tests cover REST default and explicit targets, per-session MCP isolation, and that an MCP selection never moves REST's default. Selection is not connection-verified; live multi-instance behavior unverified. |
| Browse files | E | E | Shared operation; a REST/MCP divergence on the `kind` parameter was found and fixed. |
| Upload caller-provided bytes | D | D | Shared operation; bounded size, explicit overwrite acknowledgement, ambiguous-outcome reporting. |
| Upload from remote URL | M | M | Deferred by ADR-0001 pending a separate SSRF and egress security decision. |
| Username-only or userid-only credential | E | E | Resolved against Foundry join-user data, exact match preferred with unique case-insensitive fallback, ambiguous or inconsistent identities fail closed. |
| Persistent authenticated socket | P | P | Per-identity connections for world and document reads: bounded map, idle reaper, cancellation quarantine, write deadlines, namespace-disconnect handling, redacted credential `Debug`, 64 MiB frame limit. Bounded reconnect with jitter and connection epochs apply to connection establishment, not replay of in-flight requests. Mutations still use per-request sockets; bounded unsolicited events are collected internally. Compendium management and file browsing still use per-operation sockets. A world request waiting for another can time out before that request's connection attempts complete; stale-reader integration coverage remains timing-dependent. Retries do not honor `Retry-After`, and unsuccessful session GET responses are all treated as transient. |
| Optional insecure TLS | M | M | Documented compatibility gap; secure default retained. |

Evidence base: 144 workspace tests using a loopback fake Foundry (authentication fields, v13 and v14 session binding, identity resolution, interleaved ACKs, world `430` handling, cancellation and late responses). REST/MCP contract tests use a recording fake connector.

The current `FoundryClient` does not enable a reqwest cookie jar; the ADR baseline's shared-cookie-jar concern does not apply to this source. Session and connection state are isolated per identity by HMAC-keyed map entries.

Existing REST routes are preserved and extended: `GET /health`, `GET /api/world`, `GET /api/collections/{name}`, `POST /api/write/{operation}`, plus document, compendium, instance and file routes. MCP uses `POST /mcp`. All write paths and live Foundry behavior still require separately authorized verification.
