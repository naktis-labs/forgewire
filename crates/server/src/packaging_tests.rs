//! Packaging definition tests for the `forgewire-server` OCI image.
//!
//! The image definition is a security boundary: it decides which user the
//! gateway runs as, which addresses it may listen on, and whether any secret
//! material ends up inside a layer. Those properties are asserted here by
//! reading the packaging definition as text, so the checks run anywhere
//! `cargo test --locked --workspace` runs and need no container daemon.
//!
//! The assertions are deliberately about the *contract* (non-root user,
//! loopback default, digest pinning, runtime-supplied secrets) rather than
//! about incidental formatting, so an unrelated edit to the Dockerfile does
//! not fail the suite, while a change that weakens the contract does.

use std::path::{Path, PathBuf};

const DOCKERFILE: &str = "packaging/Dockerfile";
const DOCKERIGNORE: &str = ".dockerignore";
const PACKAGING_DOC: &str = "docs/packaging.md";

/// The repository root, two levels above the server crate.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("the server crate lives two levels below the repository root")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {relative}: {e}"))
}

/// A single logical Dockerfile instruction. Line continuations are already
/// joined, and comments are dropped.
#[derive(Debug)]
struct Instruction {
    /// 1-based stage number; instructions before the first `FROM` are stage 0.
    stage: usize,
    keyword: String,
    argument: String,
}

impl Instruction {
    fn is_in_final_stage(&self, stages: usize) -> bool {
        self.stage == stages
    }
}

/// Join backslash-continued lines and drop comments and blank lines.
fn logical_lines(dockerfile: &str) -> Vec<String> {
    let mut logical = Vec::new();
    let mut buffer = String::new();
    for raw in dockerfile.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match line.strip_suffix('\\') {
            Some(continued) => {
                buffer.push_str(continued.trim_end());
                buffer.push(' ');
            }
            None => {
                buffer.push_str(line);
                logical.push(buffer.trim().to_string());
                buffer.clear();
            }
        }
    }
    if !buffer.trim().is_empty() {
        logical.push(buffer.trim().to_string());
    }
    logical
}

fn parse_dockerfile(dockerfile: &str) -> Vec<Instruction> {
    let mut stage = 0usize;
    let mut instructions = Vec::new();
    for line in logical_lines(dockerfile) {
        let (keyword, argument) = match line.split_once(char::is_whitespace) {
            Some((keyword, argument)) => (keyword.to_string(), argument.trim().to_string()),
            None => (line, String::new()),
        };
        if keyword == "FROM" {
            stage += 1;
        }
        instructions.push(Instruction {
            stage,
            keyword,
            argument,
        });
    }
    instructions
}

fn stage_count(instructions: &[Instruction]) -> usize {
    instructions.iter().map(|i| i.stage).max().unwrap_or(0)
}

fn final_stage(instructions: &[Instruction]) -> Vec<&Instruction> {
    let last = stage_count(instructions);
    instructions
        .iter()
        .filter(|i| i.is_in_final_stage(last))
        .collect()
}

/// Split an instruction argument into words, keeping double-quoted spans
/// (and the spaces inside them) together.
fn split_words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for ch in text.chars() {
        match ch {
            '"' => quoted = !quoted,
            c if c.is_whitespace() && !quoted => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// Parse an `ENV` argument into key/value pairs. Both the `KEY=VALUE` form
/// and the legacy `KEY VALUE` form are accepted.
fn env_pairs(argument: &str) -> Vec<(String, String)> {
    let words = split_words(argument);
    let mut pairs = Vec::new();
    let mut index = 0;
    while index < words.len() {
        let word = &words[index];
        match word.split_once('=') {
            Some((key, value)) => pairs.push((key.to_string(), value.to_string())),
            None => {
                let value = words[index + 1..].join(" ");
                pairs.push((word.to_string(), value));
                index = words.len();
                continue;
            }
        }
        index += 1;
    }
    pairs
}

/// The effective `ENV` map of the final stage.
fn final_stage_env(instructions: &[Instruction]) -> Vec<(String, String)> {
    final_stage(instructions)
        .iter()
        .filter(|i| i.keyword == "ENV")
        .flat_map(|i| env_pairs(&i.argument))
        .collect()
}

/// Global `ARG` defaults, used to resolve `FROM ${NAME}`.
fn global_args(instructions: &[Instruction]) -> Vec<(String, String)> {
    instructions
        .iter()
        .filter(|i| i.stage == 0 && i.keyword == "ARG")
        .filter_map(|i| {
            i.argument
                .split_once('=')
                .map(|(name, value)| (name.to_string(), value.to_string()))
        })
        .collect()
}

/// Replace `${NAME}` placeholders with the matching `ARG` default.
fn resolve(argument: &str, args: &[(String, String)]) -> String {
    let mut resolved = argument.to_string();
    for (name, value) in args {
        resolved = resolved.replace(&format!("${{{name}}}"), value);
    }
    resolved
}

/// The image reference of a `FROM`, with build arguments resolved.
fn from_reference(instruction: &Instruction, args: &[(String, String)]) -> String {
    let words = split_words(&instruction.argument);
    let first = words.first().cloned().unwrap_or_default();
    let first = resolve(&first, args);
    // `FROM <image> AS <name>`: the image is the first word.
    first
}

/// The source paths of a `COPY` instruction, ignoring flags such as
/// `--from=builder`.
fn copy_sources(argument: &str) -> Vec<String> {
    let words = split_words(argument);
    let positional: Vec<&String> = words
        .iter()
        .filter(|word| !word.starts_with("--"))
        .collect();
    positional
        .first()
        .map(|source| vec![(*source).clone()])
        .unwrap_or_default()
}

/// Does this environment variable name look like it carries a secret?
fn looks_secret(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    [
        "TOKEN",
        "SECRET",
        "PASSWORD",
        "PASSWD",
        "CREDENTIAL",
        "APIKEY",
        "API_KEY",
        "PRIVATE",
    ]
    .iter()
    .any(|needle| upper.contains(needle))
}

fn is_digest_pinned(reference: &str) -> bool {
    let Some((_, digest)) = reference.rsplit_once("@sha256:") else {
        return false;
    };
    digest.len() == 64 && digest.chars().all(|c| c.is_ascii_hexdigit())
}

/// The non-comment body of the Dockerfile, used for literal scans.
fn code_lines(dockerfile: &str) -> Vec<&str> {
    dockerfile
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

#[test]
fn image_is_built_in_multiple_digest_pinned_stages() {
    let dockerfile = read(DOCKERFILE);
    let instructions = parse_dockerfile(&dockerfile);
    let froms: Vec<&Instruction> = instructions
        .iter()
        .filter(|i| i.keyword == "FROM")
        .collect();

    assert!(
        froms.len() >= 2,
        "the image needs a build stage and a runtime stage, found {} FROM instructions",
        froms.len()
    );

    let args = global_args(&instructions);
    for from in &froms {
        let reference = from_reference(from, &args);
        assert!(
            is_digest_pinned(&reference),
            "base image must be pinned by digest, found {reference}"
        );
    }

    // The builder stage pins the Rust toolchain version through its image tag.
    let builder = froms
        .iter()
        .find(|from| {
            split_words(&from.argument)
                .windows(2)
                .any(|pair| pair[0].eq_ignore_ascii_case("as") && pair[1] == "builder")
        })
        .expect("a stage named builder");
    let builder_reference = from_reference(builder, &args);
    assert!(
        builder_reference.contains("rust:"),
        "the builder stage must pin a Rust image, found {builder_reference}"
    );
    let version = builder_reference
        .split(':')
        .nth(1)
        .and_then(|tag| tag.split('-').next())
        .unwrap_or_default();
    assert!(
        version.split('.').count() >= 3
            && version.split('.').all(|part| part.chars().all(|c| c.is_ascii_digit())),
        "the Rust toolchain must be pinned to an exact version, found {version}"
    );
}

#[test]
fn build_is_locked_to_the_committed_dependency_graph() {
    let dockerfile = read(DOCKERFILE);
    let instructions = parse_dockerfile(&dockerfile);
    let builds: Vec<&str> = instructions
        .iter()
        .filter(|i| i.keyword == "RUN")
        .map(|i| i.argument.as_str())
        .filter(|argument| argument.contains("cargo build"))
        .collect();

    assert!(
        !builds.is_empty(),
        "the build stage must compile the server with cargo"
    );
    for build in builds {
        assert!(
            build.contains("--locked"),
            "cargo build must use --locked so the image matches Cargo.lock, found {build}"
        );
        assert!(
            build.contains("-p forgewire-server"),
            "the build must target the forgewire-server package, found {build}"
        );
    }
}

#[test]
fn final_stage_runs_as_a_non_root_numeric_user() {
    let dockerfile = read(DOCKERFILE);
    let instructions = parse_dockerfile(&dockerfile);
    let final_stage = final_stage(&instructions);

    let users: Vec<&str> = final_stage
        .iter()
        .filter(|i| i.keyword == "USER")
        .map(|i| i.argument.as_str())
        .collect();

    assert_eq!(
        users.len(),
        1,
        "the runtime stage must set exactly one USER, found {users:?}"
    );
    let user = users[0];
    assert!(
        user.split(':').all(|part| !part.is_empty()
            && part.chars().all(|c| c.is_ascii_digit())),
        "the run user must be a numeric UID[:GID] with no shell, found {user}"
    );
    let uid: u32 = user
        .split(':')
        .next()
        .unwrap()
        .parse()
        .expect("numeric uid");
    assert_ne!(uid, 0, "the runtime stage must not run as root");
}

#[test]
fn runtime_listens_on_loopback_only() {
    let dockerfile = read(DOCKERFILE);
    let instructions = parse_dockerfile(&dockerfile);

    let env = final_stage_env(&instructions);
    let bind = env
        .iter()
        .find(|(key, _)| key == "BIND_ADDR")
        .map(|(_, value)| value.clone())
        .expect("the runtime stage must set BIND_ADDR");
    assert!(
        bind.starts_with("127.0.0.1:") || bind.starts_with("[::1]:"),
        "BIND_ADDR must default to a loopback address, found {bind}"
    );

    // No wildcard bind anywhere in the definition.
    for line in code_lines(&dockerfile) {
        assert!(
            !line.contains("0.0.0.0"),
            "the definition must not bind a wildcard address: {line}"
        );
    }

    // EXPOSE is deliberately absent: the process is reached over host
    // networking, so publishing a container port would be misleading.
    assert!(
        !instructions.iter().any(|i| i.keyword == "EXPOSE"),
        "the image must not declare EXPOSE for a loopback-only gateway"
    );

    let entrypoints: Vec<&str> = final_stage(&instructions)
        .iter()
        .filter(|i| i.keyword == "ENTRYPOINT")
        .map(|i| i.argument.as_str())
        .collect();
    assert_eq!(entrypoints.len(), 1, "the runtime stage needs one ENTRYPOINT");
    assert!(
        entrypoints[0].starts_with('['),
        "ENTRYPOINT must use exec form, found {}",
        entrypoints[0]
    );
    assert!(
        entrypoints[0].contains("forgewire-server"),
        "ENTRYPOINT must start forgewire-server, found {}",
        entrypoints[0]
    );
}

#[test]
fn no_secret_material_is_baked_into_the_image() {
    let dockerfile = read(DOCKERFILE);
    let instructions = parse_dockerfile(&dockerfile);

    // No environment variable may carry a literal secret. A secret-shaped
    // variable may only point at a runtime file.
    for (key, value) in instructions
        .iter()
        .filter(|i| i.keyword == "ENV" || i.keyword == "ARG")
        .flat_map(|i| env_pairs(&i.argument))
    {
        if !looks_secret(&key) {
            continue;
        }
        assert!(
            value.is_empty() || (value.starts_with('/') && !value.contains(char::is_whitespace)),
            "{key} must not carry an inline literal, found {value}"
        );
    }

    // The bearer token is supplied at run time, so the image must not define
    // it at all.
    let defined: Vec<String> = instructions
        .iter()
        .filter(|i| i.keyword == "ENV" || i.keyword == "ARG")
        .flat_map(|i| env_pairs(&i.argument))
        .map(|(key, _)| key)
        .collect();
    assert!(
        !defined.iter().any(|key| key == "FORGEWIRE_BEARER_TOKEN"),
        "FORGEWIRE_BEARER_TOKEN must be supplied at run time, not baked in"
    );

    // No key material, no inline credentials, anywhere in the definition.
    for line in dockerfile.lines() {
        let upper = line.to_ascii_uppercase();
        assert!(
            !upper.contains("PRIVATE KEY-----"),
            "the definition must not contain key material: {line}"
        );
        assert!(
            !upper.contains("AUTHORIZATION: BEARER "),
            "the definition must not contain an inline authorization header: {line}"
        );
        assert!(
            !upper.contains("\"PASSWORD\""),
            "the definition must not contain a password literal: {line}"
        );
    }
}

#[test]
fn bearer_token_and_foundry_instance_are_supplied_at_run_time() {
    let dockerfile = read(DOCKERFILE);
    let instructions = parse_dockerfile(&dockerfile);

    // The definition names the run-time environment contract.
    assert!(
        dockerfile.contains("FORGEWIRE_BEARER_TOKEN"),
        "the definition must name the run-time bearer token variable"
    );

    // The binary reads exactly these variables; the packaging and the server
    // must agree on their names.
    let source = read("crates/server/src/main.rs");
    for variable in [
        "FORGEWIRE_BEARER_TOKEN",
        "BIND_ADDR",
        "FOUNDRY_CREDENTIALS",
        "FOUNDRY_ALLOW_WRITES",
    ] {
        assert!(
            source.contains(variable),
            "the server must read {variable}, which the image documents"
        );
    }

    // The credentials file is referenced by path and is never copied in.
    let env = final_stage_env(&instructions);
    let credentials = env
        .iter()
        .find(|(key, _)| key == "FOUNDRY_CREDENTIALS")
        .map(|(_, value)| value.clone())
        .expect("the runtime stage must point at the credentials file");
    assert!(
        credentials.starts_with('/'),
        "the credentials path must be absolute, found {credentials}"
    );
    for instruction in final_stage(&instructions) {
        if instruction.keyword != "COPY" {
            continue;
        }
        let source = copy_sources(&instruction.argument);        for path in source {
            assert!(
                !path.contains("credentials"),
                "the credentials file must never be copied into the image: {path}"
            );
        }
    }
}

#[test]
fn final_stage_contains_only_the_binary_and_the_ca_bundle() {
    let dockerfile = read(DOCKERFILE);
    let instructions = parse_dockerfile(&dockerfile);
    let final_stage = final_stage(&instructions);

    let copies: Vec<&Instruction> = final_stage
        .iter()
        .copied()
        .filter(|i| i.keyword == "COPY")
        .collect();
    assert_eq!(
        copies.len(),
        2,
        "the runtime stage must copy exactly the binary and the CA bundle, found {copies:?}"
    );

    let sources: Vec<String> = copies
        .iter()
        .flat_map(|i| copy_sources(&i.argument))
        .collect();
    assert!(
        sources
            .iter()
            .any(|s| s.ends_with("target/release/forgewire-server")),
        "the release binary must be copied in, found {sources:?}"
    );
    assert!(
        sources
            .iter()
            .any(|s| s.ends_with("/etc/ssl/certs/ca-certificates.crt")),
        "the CA bundle must be copied in for outbound TLS, found {sources:?}"
    );

    // No toolchain, no package installation and no source tree in the
    // runtime stage.
    for instruction in &final_stage {
        if instruction.keyword == "RUN" {
            let argument = instruction.argument.to_ascii_lowercase();
            for tool in ["cargo", "rustc", "rustup", "apt-get", "apk ", "yum "] {
                assert!(
                    !argument.contains(tool),
                    "the runtime stage must not run {tool}: {}",
                    instruction.argument
                );
            }
        }
        if instruction.keyword == "COPY" {
            for path in copy_sources(&instruction.argument) {
                assert!(
                    path != "." && path != "crates" && !path.ends_with("/target"),
                    "the runtime stage must not carry build inputs: {path}"
                );
            }
        }
    }
}

#[test]
fn dockerignore_excludes_build_output_and_local_secrets() {
    let dockerignore = read(DOCKERIGNORE);
    let entries: Vec<&str> = dockerignore
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();

    let has = |wanted: &[&str]| {
        wanted
            .iter()
            .any(|candidate| entries.iter().any(|entry| entry == candidate))
    };

    assert!(
        has(&["target/", "**/target/"]),
        ".dockerignore must exclude the Cargo build directory, found {entries:?}"
    );
    assert!(
        has(&[".git/", "**/.git/"]),
        ".dockerignore must exclude the git directory, found {entries:?}"
    );
    assert!(
        has(&[".superpowers/", "**/.superpowers/"]),
        ".dockerignore must exclude local tooling state, found {entries:?}"
    );
    assert!(
        entries.iter().any(|entry| entry.starts_with("docs/")),
        ".dockerignore must exclude documentation build output, found {entries:?}"
    );
    assert!(
        has(&[
            "config/",
            "**/foundry_credentials.json",
            "credentials.json",
            "**/credentials.json",
        ]),
        ".dockerignore must exclude local credential files, found {entries:?}"
    );
    assert!(
        has(&[".env", ".env.*"]),
        ".dockerignore must exclude local environment files, found {entries:?}"
    );
    assert!(
        has(&["*.pem", "*.key"]),
        ".dockerignore must exclude key material, found {entries:?}"
    );
}

#[test]
fn packaging_documentation_states_the_build_run_and_loopback_contract() {
    let doc = read(PACKAGING_DOC);

    for needle in [
        "docker build",
        "docker run",
        "--network host",
        "127.0.0.1",
        "FORGEWIRE_BEARER_TOKEN",
        "FOUNDRY_CREDENTIALS",
        "loopback",
    ] {
        assert!(
            doc.contains(needle),
            "the packaging documentation must mention {needle}"
        );
    }

    let readme = read("README.md");
    assert!(
        readme.contains("docs/packaging.md"),
        "the README must point at the packaging documentation"
    );
}
