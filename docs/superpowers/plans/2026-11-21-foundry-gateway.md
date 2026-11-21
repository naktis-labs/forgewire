# Foundry Gateway Implementation Plan

**Goal:** Provide a Rust Foundry VTT connector with REST and MCP Streamable HTTP access.

**Architecture:** Keep Foundry authentication and Socket.IO protocol in an independent connector crate. Expose a small read-only gateway in a separate server crate; both REST and MCP use the same connector. Use the referenced project only to understand the Foundry protocol.

**Tech Stack:** Rust, Tokio, Axum, HTTP and WebSocket clients.

## Global Constraints

- Bind to localhost by default; document the absence of gateway authentication.
- Do not require a Foundry module or browser.
- Avoid destructive operations; document unsupported functionality.

## Tasks

- [x] Implement and test Foundry authentication, version detection, session binding and document reads against protocol fixtures.
- [x] Expose document and world reads through REST and MCP Streamable HTTP, with protocol and error handling tests.
- [x] Document configuration, security, supported versions and limitations; verify formatting and workspace tests.
- [x] Independently review protocol, transport and security behavior; address material findings.

Ruling: Unknown Foundry versions use both query and cookie session binding for compatibility with v13 and v14; this may expose the session in URL logs and is documented. Live Foundry and end-to-end MCP client verification remain outstanding.
