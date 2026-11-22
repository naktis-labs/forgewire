# Foundry Feature Parity Plan

Goal: add original server operations while retaining the Rust connector and REST/MCP gateway.

- [x] Implement and fixture-test document creation, modification and deletion.
- [x] Add original document lookup, projection, filtering and compendium reads.
- [x] Implement file browsing and uploads; compendium creation/deletion and instance selection remain unimplemented.
- [x] Expose operations over REST and MCP, with writes disabled by default and explicit opt-in.
- [x] Independently review security and destructive behavior, run workspace tests and document limitations.

Ruling: Writes require `FOUNDRY_ALLOW_WRITES=true`; uploads require explicit `overwrite: true`; non-loopback binds are rejected outright. Live writes and the end-to-end upload path remain unverified pending user authorization; the instance's `browse_files` permission denial was observed on the Test account. Modify accepts exactly one update per call.
