# Packaging forgewire-server as an OCI image

This document describes the reproducible, rootless container packaging for
`forgewire-server`. The definition lives in [`packaging/Dockerfile`](../packaging/Dockerfile)
and is verified by static tests that need no container daemon.

The packaging is an increment on top of the gateway itself: it changes no
authentication behaviour, no connector behaviour, and no request handling. It
only decides how the existing `forgewire-server` binary is built and run.

## What the image contains

The final image holds two artifacts and nothing else:

- `/usr/local/bin/forgewire-server` — the release binary.
- `/etc/ssl/certs/ca-certificates.crt` — the CA bundle used for outbound TLS.

The Rust toolchain, the Cargo registry cache, the workspace sources and every
build intermediate stay in the discarded build stage. The image contains no
shell for the run user, no compiler, and no package manager is invoked at
runtime.

## Properties

| Property | How it is achieved |
| --- | --- |
| Non-root | `USER 65532:65532` — a numeric UID with no `/etc/passwd` entry, therefore no login shell. |
| Loopback only | `BIND_ADDR=127.0.0.1:8765` in the image; the server itself refuses any non-loopback bind address at startup. |
| No secrets in layers | No secret value is written to any layer. The bearer token arrives through the environment at run time, the Foundry credentials are mounted read-only, and `.dockerignore` keeps local credential files out of the build context. |
| Reproducible | Both base images are pinned by digest, the Rust toolchain is pinned to 1.99.1 by the builder image tag, `cargo build --locked` pins the dependency graph through `Cargo.lock`, and the build path is rewritten so the binary does not embed the build directory. |

### Pinned base images

| Stage | Image | Digest |
| --- | --- | --- |
| build | `rust:1.99.1-bookworm` | `sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c` |
| runtime | `debian:bookworm-slim` | `sha256:e67eeead736ff7bbe258ef088bff683635c561b1fcfefad0806c1df4ab49ce0e` |

Both digests are multi-architecture OCI indexes and cover `linux/amd64` and
`linux/arm64`. The runtime image supplies `libc6` and `libgcc-s1`, which the
Rust runtime needs, and the CA bundle is copied over from the build stage.

## Build

Run from the repository root:

```sh
docker build --pull=false -f packaging/Dockerfile -t forgewire-server:0.1.0 .
```

- `--pull=false` keeps the pinned digests authoritative: the build uses the
  locally cached layers instead of re-resolving the tags.
- The build context is the repository root, so that `.dockerignore` applies.
  It excludes `target/`, `.git/`, `.superpowers/`, documentation build output
  and every local credential file.

Without a container daemon the definition can still be checked statically:

```sh
cargo test --locked --workspace
```

The packaging tests in `crates/server/src/packaging_tests.rs` parse the
Dockerfile and `.dockerignore` and assert the non-root user, the loopback
default, digest pinning, the absence of baked-in secrets and the run-time
secret path.

## Run

### 1. Prepare the Foundry credentials

The gateway reads its Foundry instances from a JSON file. By default the path
is `config/foundry_credentials.json`, which is gitignored and excluded from
the build context. Use the same format as a local run:

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

The file is mounted into the container read-only, so it must be readable by
the container's run user (UID 65532) and by nobody else:

```sh
sudo chown 65532:65532 config/foundry_credentials.json
sudo chmod 0400 config/foundry_credentials.json
```

### 2. Supply the bearer token through the environment

The bearer token is a secret, so it is never written into the image, a
`Dockerfile`, or a committed file. Put it in the environment of the shell that
starts the container and let the container inherit it:

```sh
export FORGEWIRE_BEARER_TOKEN="$(openssl rand -base64 32 | tr -d '/+=' | cut -c1-48)"
```

`FORGEWIRE_BEARER_TOKEN` must be a non-empty RFC 6750 b64token value
(letters, digits, `-._~+/`, optionally followed by `=` padding). An invalid or
empty value prevents startup. If the variable is unset the gateway starts
without authentication — which is only acceptable because it listens on
loopback.

A `--env-file` with mode `0600` works as well:

```sh
printf 'FORGEWIRE_BEARER_TOKEN=%s\n' "$FORGEWIRE_BEARER_TOKEN" > /tmp/forgewire.env
chmod 0600 /tmp/forgewire.env
```

### 3. Start the container

```sh
docker run --rm --network host \
  --env FORGEWIRE_BEARER_TOKEN \
  --mount type=bind,src="$PWD/config/foundry_credentials.json",dst=/etc/forgewire/foundry_credentials.json,readonly \
  forgewire-server:0.1.0
```

- `--network host` is required, and is the reason the image publishes no
  port. The process binds `127.0.0.1` inside its own network namespace, so a
  published container port (`-p 8765:8765`) would not reach it, and
  publishing it would be the wrong exposure model anyway. With host
  networking the gateway is reachable only from the host's loopback
  interface.
- `--env FORGEWIRE_BEARER_TOKEN` passes the value from the current
  environment without placing it in the command line or in shell history.
- The credentials file is mounted read-only. The image never contains it.

### 4. Check it

```sh
curl -sS -H "Host: localhost" http://127.0.0.1:8765/health
curl -sS -H "Host: localhost" -H "Authorization: Bearer $FORGEWIRE_BEARER_TOKEN" \
  http://127.0.0.1:8765/api/instances
```

The `Host` header must name localhost or a loopback address; the gateway
rejects other host names and rejects any request that carries an `Origin`
header.

## The runtime contract

| Variable | Default in the image | Meaning |
| --- | --- | --- |
| `BIND_ADDR` | `127.0.0.1:8765` | Listen address. Non-loopback addresses are rejected at startup. |
| `FORGEWIRE_BEARER_TOKEN` | not set | When set, every route including `/health` and `/mcp` requires `Authorization: Bearer <token>`. Supply at run time. |
| `FOUNDRY_CREDENTIALS` | `/etc/forgewire/foundry_credentials.json` | Path of the Foundry credentials file, mounted read-only. |
| `FOUNDRY_ALLOW_WRITES` | not set | Set to `true` to enable document mutations. Disabled by default. |
| `RUST_LOG` | `info` | Log filter. |

**The runtime listens on reachable everywhere.** `BIND_ADDR` defaults to
`127.0.0.1:8765`, the server validates the address at startup and refuses
anything that is not a loopback address, and the container is run with host
networking so no other host can reach the port. The gateway has no TLS
terminator of its own; the TLS boundary remains the operator's responsibility,
exactly as it is for a local run.

## What is not covered

- The image was **not built and the container was **not started** as part of
  the change that introduced this definition: the development environment had
  no container builder (`docker`, `podman`, `buildah`, `nerdctl` and `kaniko`
  are all absent). The definition is verified by reading it, not by producing
  an image. Build and run it in an environment that has a builder.
- The digests above were resolved from Docker Hub and the presence of the
  CA bundle and the shared libraries was confirmed by inspecting the image
  layers directly. That is a static check of the base images, not a build of
  the final image.
