# syntax=docker/dockerfile:1.7
#
# forgewire-server — reproducible, rootless OCI packaging definition.
#
# Design notes
#   * Multi-stage: the Rust toolchain, the registry cache and the source tree
#     live only in the `builder` stage. The final stage contains the release
#     binary, the CA bundle and nothing else.
#   * Every base image is pinned by digest, so a rebuild of the same source
#     resolves to byte-identical base layers.
#   * The toolchain version is pinned by the builder image tag (1.99.1);
#     `cargo build --locked` additionally pins the dependency graph via
#     Cargo.lock.
#   * The process runs as a numeric, non-root UID that has no /etc/passwd
#     entry and therefore no login shell.
#   * The server binds 127.0.0.1 only. The image deliberately declares no
#     EXPOSE: the process is reached over host networking, never through a
#     published container port.
#   * No secret is baked in. FORGEWIRE_BEARER_TOKEN is supplied at runtime
#     through the environment; the Foundry credentials file is mounted
#     read-only and is never copied into any layer.
#
# Build (from the repository root):
#   docker build --pull=false -f packaging/Dockerfile -t forgewire-server:0.1.0 .
#
# Run:
#   docker run --rm --network host \
#     --env FORGEWIRE_BEARER_TOKEN \
#     --mount type=bind,src="$PWD/config/foundry_credentials.json",dst=/etc/forgewire/foundry_credentials.json,readonly \
#     forgewire-server:0.1.0
#
# See docs/packaging.md for the full build, run and verification procedure.

# ---------------------------------------------------------------------------
# Stage 1 of 2 — build.
# ---------------------------------------------------------------------------

# rust:1.99.1-bookworm, multi-arch OCI index (linux/amd64, linux/arm64, ...).
# The digest pins the index; the tag pins the Rust toolchain to 1.99.1.
ARG BUILDER_IMAGE=rust:1.99.1-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c

# debian:bookworm-slim, multi-arch OCI index. Provides libc6 and libgcc-s1
# (required by the Rust runtime) and nothing else of the build toolchain.
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:e67eeead736ff7bbe258ef088bff683635c561b1fcfefad0806c1df4ab49ce0e

FROM ${BUILDER_IMAGE} AS builder

WORKDIR /build

# Reproducibility knobs:
#   CARGO_INCREMENTAL=0   no incremental artifacts in the build tree
#   RUSTFLAGS             strip symbols and rewrite the build path, so the
#                         binary does not embed the absolute build directory
#   SOURCE_DATE_EPOCH     fixed timestamp for anything that would otherwise
#                         stamp "now"
#   LC_ALL / TZ           locale- and timezone-independent output
ENV CARGO_TERM_COLOR=never \
    CARGO_INCREMENTAL=0 \
    CARGO_NET_RETRY=5 \
    RUSTFLAGS="-C strip=symbols --remap-path-prefix=/build=/forgewire" \
    SOURCE_DATE_EPOCH=1704067200 \
    LC_ALL=C.UTF-8 \
    TZ=UTC

# Only the workspace sources are needed; .dockerignore keeps target/, .git/,
# .superpowers/, local credentials and docs build output out of the context.
COPY . .

# --locked: refuse to update Cargo.lock, so the dependency graph is fixed.
RUN cargo build --locked --release -p forgewire-server

# ---------------------------------------------------------------------------
# Stage 2 of 2 — runtime.
# ---------------------------------------------------------------------------

FROM ${RUNTIME_IMAGE} AS runtime

LABEL org.opencontainers.image.title="forgewire-server" \
      org.opencontainers.image.description="Read-only REST and MCP gateway in front of a Foundry VTT connector" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.licenses="MPL-2.0"

# Runtime configuration. None of these values is a secret.
#   BIND_ADDR            loopback only; the server refuses any non-loopback
#                        address at startup
#   RUST_LOG             log filter
#   FOUNDRY_CREDENTIALS  path of the credentials file, which is mounted
#                        read-only at run time and is never baked into the
#                        image
#   HOME                 the numeric run user has no home directory
#
# FORGEWIRE_BEARER_TOKEN is intentionally NOT set here. It is a secret and is
# supplied at run time through the environment (for example
# `--env FORGEWIRE_BEARER_TOKEN` or `--env-file`).
ENV HOME=/nonexistent \
    BIND_ADDR=127.0.0.1:8765 \
    RUST_LOG=info \
    FOUNDRY_CREDENTIALS=/etc/forgewire/foundry_credentials.json

# The read-only bind mount target for the credentials file. It is created
# here, owned by the run user, and never populated by the image build.
RUN mkdir -p /etc/forgewire \
 && chown 65532:65532 /etc/forgewire \
 && chmod 0750 /etc/forgewire

WORKDIR /srv/forgewire

# The only two artifacts that enter the final image: the release binary and
# the CA bundle needed for outbound TLS.
COPY --from=builder /build/target/release/forgewire-server /usr/local/bin/forgewire-server
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Non-root: numeric UID:GID, no /etc/passwd entry, hence no login shell.
USER 65532:65532

# No EXPOSE on purpose. The process listens on 127.0.0.1 inside the network
# namespace it runs in; a published container port would not reach it, and
# publishing it would be the wrong exposure model for a loopback-only
# gateway. Use host networking (see docs/packaging.md).
ENTRYPOINT ["/usr/local/bin/forgewire-server"]
