# Shared Operations Implementation Plan

**Goal:** Provide original Foundry capabilities on both REST and MCP via one application layer.

**Architecture:** Transport adapters invoke shared application operations which own validation, credential snapshots, write gating, connector calls and domain errors. Preserve existing REST routes. URL fetching is a separately secured follow-up and remains an explicit parity gap until its security prerequisites are satisfied.

**Tech Stack:** Rust, Axum, Tokio, serde_json, Foundry connector.

**Spec:** `docs/superpowers/specs/2026-11-22-functional-parity-adr.md`

## Constraints and review focus

No live writes or commits without permission. Preserve dirty worktree. Test disabled writes, malformed inputs, ambiguous mutation results, snapshot isolation and no password leakage across both adapters. Review destructive and concurrent behavior independently.

## Tasks

- [ ] Inventory existing capabilities and routes, and establish a transport-neutral operation dispatch and mockable tests.
- [ ] Route MCP and existing REST world, collection and document writes through shared dispatch. Verify invalid requests and disabled writes through both transports.
- [ ] Add REST access to shared document lookup/filter/projection, compendium reads and management, instance selection, file browsing and byte upload. Test successful and failing adapter equivalence with a fake backend.
- [ ] Test an in-flight operation retains its original credential after concurrent instance selection.
- [ ] Specify and implement URL fetching only with a defensible deployment egress boundary, destination validation and pinned connections, redirect and byte limits, and security tests. Until then document this as an open parity gap.
- [ ] Independently review authority, destructive and concurrency changes; run workspace tests, fmt, Clippy and diff checks; update capability matrix and README.
