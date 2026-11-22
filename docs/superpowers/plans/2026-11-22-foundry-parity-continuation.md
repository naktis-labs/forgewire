# Foundry Feature Parity Continuation

Goal: complete remaining original MCP operations while retaining the Rust gateway's safety controls.

- [x] Implement compendium creation and deletion with explicit write opt-in and ambiguous-result reporting.
- [x] Implement credential listing and concurrent instance selection using request snapshots without exposing passwords.
- [ ] URL uploads remain unsupported: robust SSRF protection requires deployment egress controls in addition to application checks.
- [ ] Complete independent review of instance switching and document remaining limitations.

No live mutations without explicit permission. A failed or timed-out mutation may have succeeded; do not automatically retry.
