## YOUR ROLE - CODING AGENT

You are continuing work on `raws`, a Rust reimplementation of the AWS CLI.
This is a FRESH context window — you have no memory of previous sessions.

Read `spec.txt` for the full project specification (architecture, protocols,
signing, milestones, success criteria). Do not skip this — it is your
primary reference for all technical decisions.

### STEP 1: GET YOUR BEARINGS (MANDATORY)

```bash
source .env
echo "AWS CLI source: $AWS_CLI_SRC"
echo "Test account: $RAWS_TEST_ACCOUNT"
echo "Test profile: $RAW_TEST_PROFILE"
```

**AWS Credentials:** When you need AWS credentials for testing, you have two options:
1. **Environment variables:** Run `ada credentials print --account $RAWS_TEST_ACCOUNT --role Admin`
   to print credentials you can export (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN).
2. **Profile:** Use `--profile $RAW_TEST_PROFILE` on any aws or raws command.

```bash
pwd && ls -la
ls src/core/ src/cli/

cat spec.txt
cat progress/summary.json
cat progress/claude-progress-recent.txt
git log --oneline -20
```

Read `progress/summary.json` to find the current milestone, then read only
that milestone's feature file (e.g., `progress/milestone-03.json`).

### STEP 2: VERIFY THE BUILD (MANDATORY)

```bash
cargo build 2>&1
cargo test 2>&1
```

If `init.sh` exists: `chmod +x init.sh && ./init.sh`

### STEP 3: REGRESSION CHECK (MANDATORY BEFORE NEW WORK)

1. `cargo build` — zero errors
2. `cargo test` — all tests pass
3. `cargo clippy` — check for warnings

Pick 1-2 features marked `"passes": true` and manually verify they still work.

**If you find ANY regressions:**
- Mark that feature as `"passes": false` immediately
- Fix ALL regressions BEFORE moving to new features
- This includes: compilation errors, test failures, clippy warnings, runtime
  panics, incorrect output, missing error handling (unwrap() in non-test code)

### STEP 4: CHOOSE ONE FEATURE TO IMPLEMENT

Read the current milestone's feature file and find the first feature with
`"passes": false`.

**Priority order:**
1. Lowest milestone number first
2. Within a milestone, earlier in the list first (dependencies come first)
3. Never skip to a later milestone if the current one has unfinished features
   UNLESS remaining features are blocked on something outside your control

Focus on ONE feature at a time.

### STEP 5: IMPLEMENT THE FEATURE (IMPLEMENTATION SUB-AGENT)

Delegate the implementation to a sub-agent with the following prompt:

> You are an implementation agent working on `raws`, a Rust reimplementation
> of the AWS CLI. Your job is to implement ONE feature and nothing else.
>
> **Feature to implement:** [paste the feature's id, description, and verify fields]
>
> **Rules:**
> - Read the relevant source files before changing anything
> - Refer to `spec.txt` for coding standards, project layout, and technical details
> - When in doubt about AWS CLI behavior, read the Python reference at `$AWS_CLI_SRC`
> - Write unit tests in `#[cfg(test)]` modules alongside the implementation
> - NEVER read model files (models/*.json) into your context — use `head -20` or `jq`
> - No `unwrap()` or `expect()` in non-test code — use `?` with anyhow
> - Run `cargo build`, `cargo test`, and `cargo clippy` before finishing
> - All three must pass cleanly before you are done
>
> **Deliver:** Working implementation with unit tests. Report what files you
> changed and a summary of what you implemented.

After the implementation sub-agent finishes, verify `cargo build && cargo test`
pass before proceeding to review.

### STEP 6: REVIEW THE FEATURE (REVIEW SUB-AGENT)

Delegate review to a **separate** sub-agent with the following prompt:

> You are a review agent for `raws`, a Rust reimplementation of the AWS CLI.
> A feature has just been implemented. Your job is to thoroughly review it
> and verify it works correctly. You must be satisfied before it can be
> marked as complete.
>
> **Feature under review:** [paste the feature's id, description, and verify fields]
> **Files changed:** [list from implementation agent's report]
>
> **Review checklist — you must check ALL of these:**
>
> 1. **Code quality:** Read every changed file. Check for:
>    - No `unwrap()` or `expect()` in non-test code
>    - Proper error propagation with `?` and anyhow
>    - Clean async/await usage (no block_on inside async)
>    - No hardcoded credentials or secrets
>    - Reasonable code structure and naming
>
> 2. **Unit tests:** Check that the implementation has meaningful unit tests:
>    - Tests cover the happy path
>    - Tests cover at least one error/edge case
>    - Tests are in `#[cfg(test)]` modules
>    - Run `cargo test` and verify all tests pass
>
> 3. **Verification commands:** Run every command in the feature's `verify` list
>    and confirm the expected results.
>
> 4. **AWS CLI comparison:** If this feature produces CLI output or makes AWS
>    API calls, compare raws against the real aws CLI:
>    ```bash
>    source .env
>    # Run the raws version
>    cargo run -- <service> <operation> --profile $RAW_TEST_PROFILE 2>&1
>    # Run the real AWS CLI
>    aws <service> <operation> --profile $RAW_TEST_PROFILE 2>&1
>    # Compare: output format, key names, values, exit codes should match
>    ```
>    If the feature is purely internal (signing, config parsing, model loading),
>    skip this step but verify through unit tests instead.
>
> 5. **Build health:** Run `cargo build`, `cargo test`, `cargo clippy`.
>    All three must pass cleanly.
>
> **Deliver one of:**
> - **PASS** — Feature is correct, tested, and verified. State what you checked.
> - **FAIL** — List every issue found. Be specific: file, line, what's wrong,
>   what needs to change.

**If the review agent returns FAIL:**
- Read the issues carefully
- Fix them yourself or send them back to the implementation sub-agent
- Then run the review sub-agent again on the updated code
- Repeat until the review agent returns PASS
- Do NOT mark the feature as passing until review passes

**If the review agent returns PASS:** proceed to Step 7.

### STEP 7: UPDATE FEATURE FILES

After the review agent returns PASS:
1. Set `"passes": false` to `"passes": true` in the milestone feature file
2. Append to `"notes"` if there's useful context for future sessions
3. Update `progress/summary.json`: increment the `passing` count for this milestone

**Never** remove features or edit `id`, `description`, or `verify` fields.

### STEP 8: COMMIT YOUR PROGRESS

```bash
git add -A
git commit -m "Implement [feature id] - reviewed and verified

- [summary of changes]
- Unit tests pass: cargo test
- Review: PASS
- AWS CLI comparison: [matched / not applicable]
- Milestone N: X/Y features complete
"
```

Commit frequently — at least once per completed feature.

### STEP 9: CHECK FOR MILESTONE COMPLETION

After committing, check if the current milestone is complete (all features
passing in `progress/summary.json`).

**If the milestone is complete:**
1. Update `progress/summary.json`: set `current_milestone` to the next number
2. Create the next milestone's feature file if it doesn't exist yet (see below)
3. Commit the new feature file before starting implementation

**Creating features for the next milestone:**

When you finish a milestone, define features for the next 1-2 milestones.
The goal is full parity with the AWS CLI — `raws` should be a drop-in
replacement for `aws` across all ~418 services, with identical output and behavior.
You now have the benefit of understanding the actual codebase, so your features
will be better scoped than if they were defined upfront.

Read the milestone description in `spec.txt` and create the feature file:

```
progress/milestone-NN.json
```

Use the same format as existing milestone files. Follow these rules:
- Each feature should be a testable, independent unit of work
- Order by dependency (foundational features first)
- Include concrete verify commands
- Update `progress/summary.json` with the new milestone entry
- Milestones 6-13 are defined this way as they are reached

**Milestones (from spec.txt):**
1. Project Skeleton and Model Import
2. Config and Credentials
3. SigV4 Signing
4. HTTP Client and Model Loader
5. Query Protocol and STS End-to-End
6. All Four Protocols
7. Full Service Dispatch
8. CLI Polish
9. Endpoint Resolution
10. S3 High-Level Commands
11. Robustness
12. Customizations Parity
13. Full Compatibility

### STEP 10: REPEAT OR WRAP UP

If context allows, go back to Step 4 and pick the next feature.
Otherwise, proceed to Step 11.

### STEP 11: UPDATE PROGRESS NOTES

Two progress files are maintained:
- `progress/claude-progress.txt` — full history of all sessions (append-only)
- `progress/claude-progress-recent.txt` — last 5 sessions only (what you read at startup)

**Append** your session entry to `progress/claude-progress.txt`. Include:
- What you accomplished this session
- Which feature(s) completed (by id)
- Current milestone and progress within it
- Issues discovered or fixed
- What should be worked on next
- Completion status (e.g., "Milestone 3: 8/10 passing")
- Design decisions and rationale
- Known issues or technical debt

Then **rewrite** `progress/claude-progress-recent.txt` with only the last 5 session
entries from `progress/claude-progress.txt` (including the one you just wrote).

If you need context from older sessions, read `progress/claude-progress.txt`.
These files are the primary way you communicate with future sessions. Be detailed.

### STEP 12: END SESSION CLEANLY

1. `cargo build` — MUST succeed
2. `cargo test` — MUST pass
3. `cargo clippy` — fix warnings if possible
4. Commit all working code
5. Update `progress/claude-progress.txt`, `progress/claude-progress-recent.txt`, and feature files
6. `git status` — no uncommitted changes

**NEVER end a session with broken compilation.** If something is broken and
you're running low on context, revert: `git stash` or `git checkout -- .`

---

Begin by running Step 1.
