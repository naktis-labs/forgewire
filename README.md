# forgewire

A small Rust gateway between Foundry VTT and local clients. It provides a Foundry connector crate and an HTTP server with REST endpoints and MCP Streamable HTTP over a single `/mcp` endpoint.

## Status and scope

This is an initial implementation with document writes disabled by default. It supports Foundry's HTTPS `/api/status` version probe, `/join` session creation and authentication, then the Socket.IO / Engine.IO v4 websocket protocol used by `TheStranjer/foundry-vtt-mcp`: `40` namespace connect, Engine.IO ping/pong (`2`/`3`), `42["session", ...]` binding, and `42<ack>["modifyDocument", ...]` reads and changes acknowledged with `43<ack>[...]`, and `42<ack>["manageCompendium", ...]` and `42<ack>["manageFiles", ...]` likewise. Foundry v14+ binds the socket session via a Cookie header; earlier versions use the `session` query parameter. If version detection fails, the connector sends both, so the session can appear in Foundry or proxy URL logs.

REST and MCP share operations for world metadata, collection and document reads and writes, compendiums, file browsing and base64 uploads. MCP clients can select a configured instance for their own session. Every operation — world and document reads, document changes, compendium management and file browsing — runs on a persistent identity-scoped connection with bounded reconnect attempts; each request carries its own acknowledgement id, so concurrent requests share one authenticated socket without being confused for one another. A failed in-flight request is not replayed, and a change is never replayed automatically: a change whose outcome is unknown is reported as possibly applied, and its REST response carries `"outcome": "possibly_applied"` so a caller can tell it apart from a change that certainly did not happen and read the affected document instead of replaying the change. Input the connector refuses before it reaches Foundry — an invalid upload target, filename, or oversized payload — is a caller mistake (`400`), not an upstream fault. URL uploads and optional insecure TLS are not supported; persistence, search, vector indexing and real-time event synchronization are out of scope. Foundry 13 and earlier bind the WebSocket session in the socket.io URL query (`?session=…`), so that session can appear in the Foundry server's or an intermediary proxy's access logs; Forgewire v14 and later bind it with a cookie instead, and the gateway itself logs no passwords, cookies, or session-bearing URLs. Foundry server versions and systems vary; test against a non-production world first. See `docs/adr/0001-capability-inventory.md` for partial parity and verification limits.

## Build and run

Requires a recent stable Rust toolchain. Create `config/foundry_credentials.json`:

```json
[
  {
    "id": "campaign",
    "hostname": "foundry.example.org",
    "userid": "YOUR_FOUNDRY_USER_ID",
    "username": "YOUR_FOUNDRY_USER_NAME",
    "password": "YOUR_FOUNDRY_PASSWORD"
  }
]
```

Protect this file with filesystem permissions (`chmod 600 config/foundry_credentials.json`) and do not commit credentials. The default credentials path is gitignored; custom paths must be protected separately. Supply at least one non-empty `userid` or `username`; the connector resolves a missing identifier from Foundry join-user data when available and rejects ambiguous or inconsistent identities. REST requests target the first configured entry unless they name an instance explicitly (see instance targeting below), and MCP clients select an instance per session. Selecting an instance reports the selection only; it does not verify the instance's connection. No caller's selection ever retargets another client's requests or work already in flight. Set `FOUNDRY_CREDENTIALS` to use another file. `BIND_ADDR` defaults to `127.0.0.1:8765`; non-loopback addresses are rejected. Gateway authentication is optional for reads and required for writes. Set `FORGEWIRE_BEARER_TOKEN` to a non-empty RFC 6750 bearer token (letters, digits, `-._~+/`, optionally followed by `=` padding) to require `Authorization: Bearer <scope>.<token>` on every HTTP route, including `/health` and `/mcp`. The token is issued for one instance scope: `<scope>` is the identifier of the configured Foundry instance the token belongs to (its `id`, or its zero-based `item_order` when it has no `id`), and it must match the scope the gateway is configured with. By default that is the stable default instance, the first configured entry; set `FORGEWIRE_BEARER_TOKEN_SCOPE` to the `id` of another configured instance to issue the token for it. A credential issued for a different instance is refused even when its secret matches, and an unknown or ambiguous scope prevents startup. Invalid or empty configured values prevent startup. Keep the token secret, use a strong randomly generated value, and configure it through a protected environment; do not put real tokens in source control or command history. Both REST and MCP clients must send it with each request. Authentication does not authorize network exposure: a trusted TLS/proxy boundary remains unresolved, and loopback-only binding and Host and Origin restrictions remain in force. Requests carrying an `Origin` header are rejected, and `Host` must identify localhost or a loopback address; browser-based clients and remote hostnames are not supported. Writes always require authentication: without a configured token, or without a credential in the configured scope, a mutating call is refused even when `FOUNDRY_ALLOW_WRITES=true` is set.

```sh
cargo run -p forgewire-server
```

### Container image

A reproducible, rootless OCI definition for `forgewire-server` lives in `packaging/Dockerfile`. It compiles the release binary in a digest-pinned build stage, runs it as a non-root numeric user (UID 65532, no login shell), and keeps the final image to the binary plus the CA bundle. The runtime listens on loopback only (`BIND_ADDR=127.0.0.1:8765`, non-loopback addresses are rejected at startup) and carries no secrets: the bearer token is supplied through the environment at run time, and the Foundry credentials file is mounted read-only.

```sh
docker build --pull=false -f packaging/Dockerfile -t forgewire-server:0.1.0 .
```

See [`docs/packaging.md`](docs/packaging.md) for the full build, run and verification procedure, including how the bearer token and the Foundry instance are supplied.

## HTTP API

- `GET /health` returns `{"status":"ok"}`.
- `GET /api/world` returns world metadata.
- `GET /api/collections/{name}` retrieves a supported collection: `actors`, `items`, `folders`, `users`, `scenes`, `journal`, `macros`, `cards`, `playlists`, `tables`, `combats`, `messages`, or `settings`.
- `GET /api/documents/{collection}` retrieves documents from a collection with optional `id`/`_id`/`name`, `where` (JSON object), `requested_fields` and `max_length` query parameters.
- `GET /api/compendiums` lists compendiums (optional `type` filter); `GET /api/compendiums/{pack}/documents/{kind}` reads documents from a pack; `POST /api/compendiums` and `DELETE /api/compendiums/{name}` create and delete packs (write-gated).
- `GET /api/files` browses Foundry data directories (`target`, `type`, `extensions`); `POST /api/files` uploads a base64-encoded file (write-gated).
- `GET /api/instances` lists configured instances with their identifiers and hostnames; passwords are never included. With `?instance=`, `currently_active` flags the entry that reference resolves to; without it, it flags the stable default.
- `POST /mcp` implements JSON-RPC initialize, ping, tools/list and tools/call, including document reads and writes.
- `POST /api/write/{operation}` exposes `create_document`, `modify_document` and `delete_document`. Set `FOUNDRY_ALLOW_WRITES=true` explicitly to enable mutations; they are disabled by default. Mutations also require an authenticated request: the gateway must have a bearer token configured and the request must present it in the configured instance scope, so no local process can mutate a world the operator has not issued a token for. Delete operations are permanent. A timeout or invalid response after a mutation does not prove it failed: inspect Foundry before retrying to avoid duplicate creations or overwrites.

### Instance targeting (REST)

Every REST route that reaches Foundry accepts an optional `instance` query parameter naming a configured instance by its `id` (or legacy `_id`) or by its zero-based `item_order` (for example `GET /api/world?instance=two`). If the reference matches a configured `id`, it wins; otherwise it is parsed as an `item_order`. Without the parameter, requests use the documented stable default: the first configured entry. There is deliberately no process-global selection endpoint (a former `PUT /api/instances/selection` was rejected during design because it would have let one caller redirect every later request, on both transports): the target is resolved once per request into an immutable credential snapshot, so neither another REST caller nor an MCP selection can change the target of a request already in flight. Unknown or ambiguous references are rejected with `400` before any Foundry contact.

MCP clients should use Streamable HTTP at `http://127.0.0.1:8765/mcp`. The current implementation returns JSON responses for requests and HTTP 202 for notifications; it does not establish long-lived SSE streams. `initialize` issues a session id returned as an `Mcp-Session-Id` response header. Echoing that header on later requests scopes state to the client: `choose_foundry_instance` then records the selection for that session alone (the response reports the selection and does not verify a connection), and subsequent calls from that session target it. Selections are private per session: they never affect REST, other sessions, or anonymous MCP calls, which always use the stable default. Calling `choose_foundry_instance` without a session header is an error, and an unknown or expired session id (the bounded session table expires the oldest sessions) is answered with `404`; initialize again. Base64 uploads are limited to 10 MiB and require `overwrite: true` to acknowledge that an existing file could be replaced; remote URL uploads are not supported.

## Tests

Run `cargo test --workspace`.
