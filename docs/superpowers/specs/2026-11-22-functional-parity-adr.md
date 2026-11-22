# ADR: Functional parity through shared application operations

- Status: Proposed
- Date: 2026-11-22
- Reference: https://github.com/TheStranjer/foundry-vtt-mcp (`master`, server tool definitions and handler)

## Context

Forgewire provides a Rust Foundry connector and a local HTTP gateway. The original server exposes Foundry operations over MCP. Forgewire currently exposes most of those capabilities over MCP but only world reads, collection reads, and document writes over REST. Some existing REST and MCP handlers call the connector directly, while others use MCP-oriented helper modules. This makes capability and behavior drift possible.

The goal is functional equivalence, not compatibility with the original tool names, API schemas, implementation, or architecture. REST and MCP must each provide the same capabilities and use the same internal application operations.

## Decision

Introduce a transport-independent application layer. Each logical operation owns argument validation, write authorization, selection of one credential snapshot per request, Foundry connector invocation, result semantics, and domain errors. REST and MCP adapt transport requests into these operations and translate results and errors into their respective protocols. Neither adapter calls the other, and neither bypasses the application layer for Foundry operations. Transport-specific malformed-request handling, protocol negotiation, serialization, and HTTP status mapping remain in the adapters.

The application layer may reuse and refactor the existing read, file, compendium, instance, and document-write modules; their current MCP-facing names do not define the boundary. Preserve in-flight request credential snapshots when another request changes the selected instance. Instance selection reports selection, not a verified connection, unless a connection is actually verified.

## Required capabilities

Both transports must offer:

1. World metadata and collection reads; individual lookup by identifier or name; filtering, field projection, response size limiting, and supported compendium document reads.
2. Listing and filtering compendium packs, and creating and deleting world compendiums.
3. Creating, updating, and deleting world, embedded, and supported compendium documents.
4. Listing configured instances without passwords and selecting the active instance.
5. Browsing Foundry files and uploading caller-provided file bytes.
6. Fetching a remote URL and uploading its content to Foundry, subject to the security requirements below.

Equivalent client workflows may use different HTTP routes, method names, argument representations, and result envelopes. The same meaningful inputs must produce equivalent application results and validation, authorization, and mutation semantics; transport-specific framing and status codes may differ. New functionality must be exposed through both adapters.

## Security and operational constraints

- Writes remain disabled unless `FOUNDRY_ALLOW_WRITES=true`. File uploads require an explicit overwrite acknowledgement; deletion is permanent.
- Never retry a mutation automatically after an ambiguous result. Report that it may have been applied on timeout or invalid acknowledgement.
- Preserve the loopback-only unauthenticated gateway policy and existing Host and Origin checks. Do not reveal passwords or session tokens in responses.
- Remote fetching is untrusted network access. Do not ship URL uploads without a separately documented and tested policy covering redirect validation, DNS rebinding and connection pinning, proxy bypass, bounded streaming, deadlines, credential isolation, and deployment egress controls. Do not claim address filtering alone guarantees protection from private network routing. Until that policy is implemented and configured, reject URL uploads explicitly; this is an unresolved parity gap, not completion.
- No live Foundry mutations or uploads without separate user authorization. Preserve existing uncommitted work; do not commit without request.

## Alternatives

- **Shared application operations (chosen):** consistent validation, authority, and behavior; requires a deliberate refactor and cross-adapter contract tests.
- **Duplicate REST and MCP implementations:** smaller initial patch but violates the shared-path requirement and risks diverging authorization and behavior.
- **REST proxy to MCP or MCP proxy to REST:** superficially shares behavior but couples application logic to transport framing and complicates errors, testing, and lifecycle.

## Acceptance and verification

- A capability matrix maps each original client capability to both interfaces and shared operation(s), with any residual restrictions explicitly recorded.
- Contract tests call both adapters using equivalent inputs and a controlled fake or mock backend, verifying equivalent successful results, validation failures, write gating, and ambiguous mutation errors. Tests cover selection during an in-flight operation and prove the original snapshot is used.
- Test supported read filters, field projection, compendium targeting, file operations, and instance selection through both interfaces. No test may require an unauthorized live write.
- Run workspace tests, formatting, Clippy with warnings denied, and `git diff --check`; independently review authority, destructive operations, concurrency, and URL-fetch controls when changed.
- Document endpoint usage, safety restrictions, any remaining gaps, and what was not verified live. Do not claim full parity while URL fetching or any other required capability is unavailable.

## Consequences

Shared operations add a layer and require adapter-contract tests, but give validation and authorization a single authority. URL upload remains blocked until its network security preconditions are met. Existing clients may require documented migration if REST schemas change; preserve existing routes where feasible.
