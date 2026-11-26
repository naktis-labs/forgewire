# Foundry interoperability verification

## Automated disposable fake world

Run from the repository root, with the locked dependencies already available locally:

```sh
cargo test --locked --offline -p forgewire-server interop_tests
cargo test --locked --offline --workspace
```

The server test constructs the real `FoundryClient`, drives the MCP router with
`get_journals` and `get_journal`, and checks the fake world's HTTP and Socket.IO
request log. A second test drives an authenticated change through the REST
write route against the same disposable world and checks that the read and the
change shared one authenticated socket. A third test drops the socket during
a change and checks that the REST route reports the unknown outcome as
`502` with `"outcome": "possibly_applied"`. The fake world listens only on an
ephemeral `127.0.0.1` port; the loopback client ignores proxy settings and
cannot target a remote host. The fixture is entirely in memory and requires no
`FOUNDRY_*` variables. Dropping the world stops its listener. This is a limited
protocol simulation, not proof of compatibility with any real Foundry release.
It does not implement a full Foundry world, and it does not verify uploads or
arbitrary compendiums.

## Automated live instance (own, disposable)

`crates/server/src/real_world_tests.rs` drives the gateway's REST and MCP
routes over the real connector against a live Foundry instance. The checks are
skipped unless `FORGEWIRE_REAL_WORLD_URL` is set, so the ordinary suite never
touches a real world. They include writes, so the instance must be a world
that exists to be changed: the checks create, change and delete documents,
create and delete a compendium, and upload a file.

The instance used for the recorded run was Foundry VTT 14.370 on Node 24.22.1
with the D&D5e system 6.1.1, listening on loopback only, with a world named
`forgewire-test` and a game master account that could read and write it.

```sh
FORGEWIRE_REAL_WORLD_URL=http://127.0.0.1:30000 \
FORGEWIRE_REAL_WORLD_USERNAME=Gamemaster \
FORGEWIRE_REAL_WORLD_PASSWORD=THE_DISPOSABLE_PASSWORD \
  cargo test -p forgewire-server real_world -- --nocapture
```

`FORGEWIRE_REAL_WORLD_USERID` is optional and only used when the user is known
by id. The instance has to be reachable over plain HTTP: the loopback client
is compiled only for this harness and refuses anything but a loopback
address, so it cannot be pointed at a remote host.

Two more variables describe the instance when it is not the one used for the
recorded run:

- `FORGEWIRE_REAL_WORLD_DATA_TARGET` is the world's own directory relative to
  the instance's data root, which is what a browse and an upload name. It
  defaults to `worlds/forgewire-test`.
- `FORGEWIRE_REAL_WORLD_SYSTEM` is the system the world is expected to run.
  When it is set, the world's system has to match it; when it is not, the
  check only asserts that the world reports a system at all.

A URL without a username is a mistake rather than a reason to skip: the
checks fail loudly, so a run that was meant to reach a real instance cannot
pass without doing anything.

The checks share one world, so they take a lock and run one at a time, whether
or not the test runner runs them in parallel. The documents they create carry
a per-run suffix, so a run never collides with what an earlier run left
behind; the compendium keeps a fixed label and is removed again, including
one an earlier run left behind.

Things the recorded run settled that the fake world could not:

- Foundry only serves a world while that world is launched. A restarted
  instance answers `/api/status` with `"active": false` and every read and
  write fails with `Foundry join data did not contain users`, because the
  join data of a world that is not running carries no users. Launch the world
  again after every restart.
- The world data of Foundry 14 lives in a local key-value store under the
  instance's data directory, not in MongoDB, so no database has to be set up.
- Foundry's `manageCompendium` delete takes the pack's bare name, while
  every read identifies a pack by its id (`world.<name>`). The connector
  resolves the name against the packs the world advertises, so the id and the
  pack's own name both reach the same pack, and the pack listing reports a
  world pack's name next to its id. A package pack has no deletable name:
  its name repeats in the world scope and would name a different pack. A pack
  the world does not advertise is refused with `400` before the instance sees
  it, whether it is named by its own name or by a pack id.
- Foundry 14 refuses `MeasuredTemplate`, a document type it deprecated,
  without sending an acknowledgement. The gateway reports such a change as
  `502` with `"outcome": "possibly_applied"`, because there is no answer that
  proves the outcome.
- A browse and an upload take a directory relative to the instance's data
  root, so a world lives under `worlds/<world>`. The instance refuses an
  upload whose target directory does not exist.

## Real disposable world (manual, separate environment)


**Do not use a production world.** This procedure is for an operator with an
existing licensed Foundry installation in an isolated environment. Do not
perform it unless you can create and delete a throwaway world and have confirmed
its URL points only to that world. It is not part of the automated tests. The
gateway's connector requires HTTPS with a certificate trusted by the system;
plain HTTP and self-signed certificates without an installed trusted CA will not
work. Foundry itself or a local TLS reverse proxy must provide that HTTPS URL.
Do not expose the gateway publicly.

1. Using Foundry's setup UI, create a new world named `forgewire-interop`, using
   an installed game system. Launch it, create a disposable user and a journal
   entry named `Interop check` in that world, and ensure the user can read the
   journal. Record the disposable world's HTTPS hostname and optional port,
   plus the user's name or ID. These setup actions change only the new world;
   the gateway steps below perform reads only.
2. Build the gateway from this repository:

   ```sh
   cargo build --locked --release -p forgewire-server
   ```

3. In a private shell, create a directory outside the repository and edit a
   credentials file there. For example:

   ```sh
   umask 077
   mkdir -p "$HOME/.local/share/forgewire-interop"
   ${EDITOR:-vi} "$HOME/.local/share/forgewire-interop/credentials.json"
   chmod 600 "$HOME/.local/share/forgewire-interop/credentials.json"
   ```

   Its JSON format is an array. Replace the placeholders interactively; do not
   put real credentials in shell history, logs, source control, or shared files:

   ```json
   [{
     "id": "disposable",
     "hostname": "DISPOSABLE_HTTPS_HOST_AND_OPTIONAL_PORT",
     "userid": "DISPOSABLE_USER_ID",
     "username": "DISPOSABLE_USER_NAME",
     "password": "DISPOSABLE_USER_PASSWORD"
   }]
   ```

   Supply at least one of `userid` or `username`; omit an unknown field rather
   than using a placeholder. `id` is the gateway's instance identifier, not
   the Foundry user ID. The hostname must not contain a path.
4. In the gateway shell, explicitly disable writes and any inherited bearer
   configuration, then start it on loopback. `FOUNDRY_CREDENTIALS` is required
   here; `FOUNDRY_ALLOW_WRITES` must remain unset. `BIND_ADDR` is optional and
   defaults to the address shown. A bearer token is optional for local reads;
   if used, see `README.md` for `FORGEWIRE_BEARER_TOKEN` and
   `FORGEWIRE_BEARER_TOKEN_SCOPE` and send the scoped authorization header.

   ```sh
   unset FOUNDRY_ALLOW_WRITES FORGEWIRE_BEARER_TOKEN FORGEWIRE_BEARER_TOKEN_SCOPE
   export FOUNDRY_CREDENTIALS="$HOME/.local/share/forgewire-interop/credentials.json"
   export BIND_ADDR=127.0.0.1:8765
   ./target/release/forgewire-server
   ```

5. In a second local shell, read the journal through MCP. The text content of
   the JSON-RPC response should include `Interop check` and `isError` should be
   `false`. Journal data may be sensitive; do not publish the response.

   ```sh
   curl --fail-with-body -sS http://127.0.0.1:8765/mcp \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_journals","arguments":{}}}'
   ```

6. **Teardown:** stop the gateway with Ctrl-C. Stop the disposable world in
   Foundry and delete `forgewire-interop` using Foundry's setup UI, confirming
   the correct world before deletion. Remove the private credentials file and
   its now-empty directory, and unset the variables in the gateway shell:

   ```sh
   rm -f "$HOME/.local/share/forgewire-interop/credentials.json"
   rmdir "$HOME/.local/share/forgewire-interop"
   unset FOUNDRY_CREDENTIALS BIND_ADDR
   ```

   Follow your environment's secure deletion and backup policies; ordinary
   file removal does not guarantee physical erasure. Remove any disposable
   TLS proxy or CA configuration you created for this check separately.
