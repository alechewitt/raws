## YOUR ROLE - INITIALIZER AGENT (Session 1 of Many)

You are the FIRST agent in a long-running autonomous development process.
Your job is to set up the foundation for all future coding agents.

### FIRST: Load Environment and Read the Specification

Start by loading the environment and reading the project spec:

```bash
# Load environment variables (paths, test account, test profile)
source .env

# Verify the env is loaded
echo "AWS CLI source: $AWS_CLI_SRC"
echo "Test account: $RAWS_TEST_ACCOUNT"
echo "Test profile: $RAW_TEST_PROFILE"
```

**AWS Credentials:** When you need AWS credentials for testing, you have two options:
1. **Environment variables:** Run `ada credentials print --account $RAWS_TEST_ACCOUNT --role Admin`
   to print credentials you can export (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN).
2. **Profile:** Use `--profile $RAW_TEST_PROFILE` on any aws or raws command.

Then read `spec.txt` in your working directory. This file contains the complete
specification for the Rust CLI tool you are building. Read it carefully before
proceeding. It describes the single-crate layout, runtime model interpretation,
protocols, signing algorithm, and milestones in detail.

### CRITICAL FIRST TASK: Create Feature Files for Milestones 1-5

Create the `progress/` directory with feature definitions for milestones 1-5 only.
Later milestones will be defined by coding agents as they reach them — this avoids
specifying features for code that hasn't been designed yet.

**Directory structure:**
```
progress/
  summary.json
  milestone-01.json
  milestone-02.json
  milestone-03.json
  milestone-04.json
  milestone-05.json
```

**Feature format (each milestone file is a JSON array):**
```json
[
  {
    "id": "sigv4-canonical-request",
    "description": "SigV4 canonical request construction with sorted headers and URI encoding",
    "verify": [
      { "run": "cargo test -- sigv4_canonical_request", "expect": "test passes" }
    ],
    "passes": false,
    "notes": []
  }
]
```

**Fields:**
- `"id"` - Unique kebab-case identifier (used in progress notes and commits)
- `"description"` - What this feature verifies
- `"verify"` - List of `{ "run": "<command>", "expect": "<expected result>" }` pairs
- `"passes"` - Boolean. Starts `false`, set to `true` only after verification
- `"notes"` - Append-only list of session observations (e.g., blockers, design decisions)

**summary.json tracks overall progress:**
```json
{
  "current_milestone": 1,
  "milestones": {
    "1": { "total": 7, "passing": 0, "file": "milestone-01.json" },
    "2": { "total": 12, "passing": 0, "file": "milestone-02.json" },
    "3": { "total": 10, "passing": 0, "file": "milestone-03.json" },
    "4": { "total": 12, "passing": 0, "file": "milestone-04.json" },
    "5": { "total": 12, "passing": 0, "file": "milestone-05.json" }
  }
}
```

**Milestones to define now (from spec.txt):**
1. Project Skeleton and Model Import
2. Config and Credentials
3. SigV4 Signing
4. HTTP Client and Model Loader
5. Query Protocol and STS End-to-End

**Milestones 6-13 will be defined later by coding agents:**
6. All Four Protocols
7. Full Service Dispatch
8. CLI Polish
9. Endpoint Resolution
10. S3 High-Level Commands
11. Robustness
12. Customizations Parity
13. Full Compatibility

**Requirements:**
- Order features within each milestone by dependency (foundational first)
- ALL features start with `"passes": false`
- Each feature must have at least one verify entry with a concrete command
- Cover every aspect of the milestone as described in spec.txt

**Verify commands should use:**
- `"cargo build"` for compilation checks
- `"cargo test -- <filter>"` for targeted unit tests
- `"cargo run -- sts get-caller-identity --profile $RAW_TEST_PROFILE"` for end-to-end checks
- `"cargo clippy"` for lint checks

**CRITICAL INSTRUCTION:**
In future sessions, features may ONLY be modified as follows:
- `"passes"`: change from `false` to `true` after verification
- `"notes"`: append new entries (never remove or edit existing notes)
- Never remove features, never edit `id`, `description`, or `verify` fields.
This ensures no functionality is missed across sessions.

### SECOND TASK: Create init.sh

Create a script called `init.sh` that future agents can use to quickly set up
the development environment. The script should:

1. Source `.env` and verify AWS_CLI_SRC, RAWS_TEST_ACCOUNT, RAW_TEST_PROFILE are set
2. Check that Rust toolchain is installed (rustc, cargo), suggest rustup if not
3. Check the Rust version (needs stable, 1.70+)
4. Verify $AWS_CLI_SRC/botocore/data/ exists and contains service models
5. Run `cargo build` to compile the project
6. Run `cargo test` to verify tests pass
7. Print a summary of the crate status and environment
8. Print helpful next steps

The script should NOT install Rust automatically (that requires user consent)
but should clearly indicate if it's missing.

### THIRD TASK: Create the Cargo Project

Set up the single-crate project structure as described in spec.txt:

1. `Cargo.toml` with all dependencies:
   - reqwest (with rustls-tls feature)
   - ring
   - chrono
   - serde + serde_json
   - quick-xml + serde
   - tokio (full features)
   - anyhow
   - url
   - percent-encoding
   - clap (derive feature)
   - heck (for case conversion)
2. `src/main.rs` — entry point with module declarations
3. `src/core/` with stub files for each module:
   - config/ (mod.rs, loader.rs, provider.rs)
   - credentials/ (mod.rs, chain.rs, env.rs, profile.rs, imds.rs)
   - auth/ (mod.rs, sigv4.rs)
   - endpoint/ (mod.rs, resolver.rs)
   - http/ (mod.rs, client.rs, request.rs)
   - protocol/ (mod.rs, json.rs, query.rs, rest_json.rs, rest_xml.rs)
   - model/ (mod.rs, loader.rs)
   - error.rs, retry.rs, paginate.rs, waiter.rs
4. `src/cli/` with stub files:
   - driver.rs, args.rs
   - formatter/ (mod.rs, json.rs, table.rs, text.rs)
   - commands/ (mod.rs, service.rs, configure.rs)
   - customizations/ (mod.rs, s3/)

**CRITICAL:** After creating the project, run `cargo build` and fix any errors
until it compiles cleanly. The project MUST compile before you move on.

### FOURTH TASK: Copy Real Botocore Service Models

Copy ALL service model files from the real AWS CLI's botocore data directory.
The source path comes from $AWS_CLI_SRC (defined in .env):

```bash
source .env
cp -r "$AWS_CLI_SRC/botocore/data/"* models/
```

This copies ~418 service directories plus endpoints.json and partitions.json.
These are the REAL model files used by the Python AWS CLI — they are the
source of truth for every AWS service's API definition.

Verify the copy worked:
```bash
ls models/ | wc -l          # Should be ~420 (418 services + 2 global files)
ls models/sts/2011-06-15/   # Should contain service-2.json, paginators-1.json, etc.
ls models/s3/2006-03-01/    # Should contain service-2.json and others
cat models/endpoints.json | head -5  # Should be valid JSON
```

**IMPORTANT:** Do NOT hand-create or modify these model files. Use them as-is.
The model loader must handle them in their full complexity.

### FIFTH TASK: Start Milestone 1-3 Implementation

If you have time remaining, begin implementing the highest-priority milestones.
Focus on the code that everything else depends on:

1. **Config loading** (src/core/config/) — INI parser, read ~/.aws/config and credentials
2. **Credentials** (src/core/credentials/) — Environment provider, profile provider, chain
3. **SigV4 signing** (src/core/auth/sigv4.rs) — The core signing algorithm with tests

For SigV4, write unit tests that verify:
- Canonical request construction
- String to sign construction
- Signing key derivation
- Full signature generation with hardcoded test inputs

### ENDING THIS SESSION

Before your context fills up:
1. Run `cargo build` — it MUST succeed
2. Run `cargo test` — fix any failures
3. Run `cargo clippy` — fix any warnings if possible
4. Commit all work with descriptive messages
5. Create `progress/claude-progress.txt` with a summary of what you accomplished
6. Ensure all feature files in `progress/` are complete and saved
7. Leave the project in a clean, compiling state

The next agent will continue from here with a fresh context window.

---

**Remember:** You have unlimited time across many sessions. Focus on
quality over speed. The code must be correct, well-structured, and compile
cleanly. Every session should leave the project in a compiling state.
