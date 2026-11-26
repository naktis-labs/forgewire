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
