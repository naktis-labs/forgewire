# forgewire

A small Rust gateway between Foundry VTT and local clients. It provides a Foundry connector crate and an HTTP server with REST endpoints and MCP Streamable HTTP over a single `/mcp` endpoint.

## Status and scope

This is an initial implementation with document writes disabled by default. It supports Foundry's HTTPS `/api/status` version probe, `/join` session creation and authentication, then the Socket.IO / Engine.IO v4 websocket protocol used by `TheStranjer/foundry-vtt-mcp`: `40` namespace connect, Engine.IO ping/pong (`2`/`3`), `42["session", ...]` binding, and `42<ack>["modifyDocument", ...]` reads acknowledged with `43<ack>[...]`. Foundry v14+ binds the socket session via a Cookie header; earlier versions use the `session` query parameter. If version detection fails, the connector sends both, so the session can appear in Foundry or proxy URL logs.

The connector exposes world metadata, collection reads and document creation, updates and deletion. Compendium listing and document reads, file browsing and base64 uploads are also available through MCP. Compendium creation and deletion and instance switching are available through MCP. URL uploads, reconnects, TLS exceptions, persistence, search, vector indexing and real-time event synchronization are not yet implemented. Foundry server versions and systems vary; test against a non-production world first.

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

Protect this file with filesystem permissions (`chmod 600 config/foundry_credentials.json`) and do not commit credentials. The default credentials path is gitignored; custom paths must be protected separately. Both `userid` and `username` must be supplied and non-empty. The server initially uses the first configured entry; MCP clients can select another configured instance for subsequent requests. Switching does not verify its connection. Set `FOUNDRY_CREDENTIALS` to use another file. `BIND_ADDR` defaults to `127.0.0.1:8765`; non-loopback addresses are rejected. The server currently has no gateway authentication, so exposing it to an untrusted network is unsafe. Requests carrying an `Origin` header are rejected, and `Host` must identify localhost or a loopback address; browser-based clients and remote hostnames are not supported.

```sh
cargo run -p forgewire-server
```

## HTTP API

- `GET /health` returns `{"status":"ok"}`.
- `GET /api/world` returns world metadata.
- `GET /api/collections/{name}` retrieves a supported collection: `actors`, `items`, `folders`, `users`, `scenes`, `journal`, `macros`, `cards`, `playlists`, `tables`, `combats`, `messages`, or `settings`.
- `POST /mcp` implements JSON-RPC initialize, ping, tools/list and tools/call, including document reads and writes.
- `POST /api/write/{operation}` exposes `create_document`, `modify_document` and `delete_document`. Set `FOUNDRY_ALLOW_WRITES=true` explicitly to enable mutations; they are disabled by default. Delete operations are permanent. A timeout or invalid response after a mutation does not prove it failed: inspect Foundry before retrying to avoid duplicate creations or overwrites. The gateway has no authentication, so any local process can invoke enabled operations using the configured Foundry account.

MCP clients should use Streamable HTTP at `http://127.0.0.1:8765/mcp`. The current implementation returns JSON responses for requests and HTTP 202 for notifications; it does not establish long-lived SSE streams or MCP session IDs. Base64 uploads are limited to 10 MiB and require `overwrite: true` to acknowledge that an existing file could be replaced; remote URL uploads are not supported.

## Tests

Run `cargo test --workspace`.
