## YOUR ROLE - VALIDATION AGENT

You are a BLACK-BOX validation agent for `raws`, a Rust reimplementation of the AWS CLI.
Your job is to systematically test every command of `raws` against the real AWS CLI and
verify they produce identical output.

### CRITICAL RULES

1. **Do NOT read or examine the raws source code.** Treat raws as a black box. You may
   only interact with it by running commands and observing output.
2. **The AWS CLI help output and documentation are your only references** for expected
   behavior. Do not look at spec.txt, milestone files, or any internal design documents.
3. **Never run destructive commands** (create, delete, put, update, start, stop, terminate,
   remove, deregister, revoke) against real AWS resources. Only run read-only commands
   (list, describe, get) and help/error comparisons.
4. **Use subagents for parallelism.** One explorer subagent discovers commands, multiple
   tester subagents validate them. Never have two agents working on the same service.

---

### STEP 1: ENVIRONMENT SETUP (MANDATORY)

```bash
# Load environment variables (assumes cwd is the raws project root)
source prompt/.env
echo "RAWS root: $RAWS_ROOT"
echo "Test account: $RAWS_TEST_ACCOUNT"
echo "Test profile: $RAW_TEST_PROFILE"
```

**AWS Credentials:** For commands that hit real AWS APIs, set credentials:
```bash
eval $(ada credentials print --account $RAWS_TEST_ACCOUNT --role Admin --format env)
```

**Two CLI binaries you will use throughout:**

| CLI  | Binary path / invocation |
|------|--------------------------|
| AWS  | `$AWS_CLI_VENV/bin/aws` |
| raws | `$RAWS_ROOT/target/release/raws` |

**IMPORTANT: Shell state does not persist between Bash tool calls.** Each Bash invocation
is a fresh shell. You MUST source `prompt/.env` and set convenience variables at the top of
each Bash call:
```bash
source prompt/.env
AWS=$AWS_CLI_VENV/bin/aws
RAWS=$RAWS_ROOT/target/release/raws
```

Test that both work:
```bash
source prompt/.env && $AWS_CLI_VENV/bin/aws --version
```

### STEP 2: BUILD RAWS (MANDATORY)

Build the release binary so tests run quickly:
```bash
cargo build --release 2>&1
```

If the build fails, stop. Do not proceed with validation on a broken build.

### STEP 3: INITIALIZE PROGRESS DIRECTORY

Create the progress tracking directory if it doesn't exist:
```bash
mkdir -p progress/validation
```

If `progress/validation/summary.json` already exists, read it to understand what has
already been discovered and tested. Resume from where the last session left off.

If it doesn't exist, create the initial summary:
```json
{
  "global_options": {
    "commands": "global_options.json",
    "commands_created": false,
    "commands_tested": false
  },
  "authentication": {
    "commands": "authentication.json",
    "commands_created": false,
    "commands_tested": false
  }
}
```

Service entries will be added by the explorer subagent as it discovers them.

---

### STEP 4: DISCOVERY PHASE (Explorer Subagent)

Spawn an **Explorer subagent** to discover all available commands. This agent runs first,
before any tester agents.

**Explorer subagent prompt:**

> You are a discovery agent. Your job is to catalog every service and command available
> in the AWS CLI by reading its help output. You do NOT test anything — you only discover
> and record.
>
> **Setup:**
> ```bash
> source prompt/.env
> source $AWS_CLI_VENV/bin/activate
> ```
>
> **Task 1: Discover services**
> Run `aws help` and extract the complete list of available services.
>
> **Task 2: Discover commands per service**
> For each service, run `aws <service> help` and extract all available commands
> (operations). Parse the "AVAILABLE COMMANDS" section from the help output.
>
> **Task 3: Create per-service JSON files**
> For each service, create `progress/validation/<service>.json` with an entry for
> every command:
> ```json
> [
>   {
>     "command": "raws <service> <operation> --region us-east-1",
>     "aws_command": "aws <service> <operation> --region us-east-1",
>     "category": "help|read|write|error",
>     "matches_aws_cli": null,
>     "tested": false,
>     "difference": null
>   }
> ]
> ```
>
> For each command, set the `category` field:
> - `"read"` — commands starting with list-, describe-, get-, search-, lookup-,
>   batch-get-, scan, query, check-, count-, detect-, estimate-, evaluate-,
>   export-, fetch-, preview-, poll-, select-, simulate-, test-, translate-,
>   validate-, verify-. Also: commands that are clearly read-only like
>   `get-caller-identity`, `decode-authorization-message`.
> - `"write"` — commands starting with create-, delete-, put-, update-, start-,
>   stop-, terminate-, remove-, deregister-, revoke-, attach-, detach-, enable-,
>   disable-, modify-, register-, run-, send-, set-, tag-, untag-, invoke,
>   publish, execute-, import-, add-, associate-, disassociate-, cancel-,
>   complete-, confirm-, accept-, reject-, reboot-, release-, reset-.
> - `"help"` — special entry for `<service> help` comparison (add one per service).
> - `"error"` — reserved for error case testing (tester agents will add these).
> - **Catch-all rule:** If a command does not match any prefix in the read or write
>   lists, classify it as `"write"` (safer default — prevents accidental execution
>   of unclassified mutating operations).
>
> **Task 4: Update summary.json**
> Add each discovered service to `progress/validation/summary.json`:
> ```json
> {
>   "service_<name>": {
>     "commands": "<name>.json",
>     "commands_created": true,
>     "commands_tested": false,
>     "total_commands": <N>,
>     "tested": 0,
>     "passed": 0,
>     "failed": 0
>   }
> }
> ```
>
> **Task 5: Create global_options.json and authentication.json**
>
> `global_options.json` — test entries for each global CLI option:
> ```json
> [
>   { "command": "raws sts get-caller-identity --output json --region us-east-1",
>     "aws_command": "aws sts get-caller-identity --output json --region us-east-1",
>     "category": "read", "matches_aws_cli": null, "tested": false, "difference": null },
>   { "command": "raws sts get-caller-identity --output text --region us-east-1",
>     "aws_command": "aws sts get-caller-identity --output text --region us-east-1",
>     "category": "read", "matches_aws_cli": null, "tested": false, "difference": null },
>   { "command": "raws sts get-caller-identity --output table --region us-east-1",
>     "aws_command": "aws sts get-caller-identity --output table --region us-east-1",
>     "category": "read", "matches_aws_cli": null, "tested": false, "difference": null },
>   { "command": "raws sts get-caller-identity --region eu-west-1",
>     "aws_command": "aws sts get-caller-identity --region eu-west-1",
>     "category": "read", "matches_aws_cli": null, "tested": false, "difference": null },
>   { "command": "raws sts get-caller-identity --no-paginate --region us-east-1",
>     "aws_command": "aws sts get-caller-identity --no-paginate --region us-east-1",
>     "category": "read", "matches_aws_cli": null, "tested": false, "difference": null },
>   { "command": "raws sts get-caller-identity --query Account --region us-east-1",
>     "aws_command": "aws sts get-caller-identity --query Account --region us-east-1",
>     "category": "read", "matches_aws_cli": null, "tested": false, "difference": null },
>   { "command": "raws ec2 describe-vpcs --no-sign-request --region us-east-1 2>&1",
>     "aws_command": "aws ec2 describe-vpcs --no-sign-request --region us-east-1 2>&1",
>     "category": "error", "matches_aws_cli": null, "tested": false, "difference": null }
> ]
> ```
> Include tests for: --output (json/text/table), --region, --no-paginate, --query,
> --no-sign-request, --endpoint-url, --debug, --cli-read-timeout, --profile,
> --no-cli-pager. Use simple read-only commands as the base (sts get-caller-identity,
> ec2 describe-vpcs, etc.).
>
> `authentication.json` — test entries for credential methods:
> ```json
> [
>   { "command": "raws sts get-caller-identity --profile alechewt_awscli_test --region us-east-1",
>     "aws_command": "aws sts get-caller-identity --profile alechewt_awscli_test --region us-east-1",
>     "category": "read", "matches_aws_cli": null, "tested": false, "difference": null },
>   { "command": "raws sts get-caller-identity --region us-east-1",
>     "aws_command": "aws sts get-caller-identity --region us-east-1",
>     "category": "read", "matches_aws_cli": null, "tested": false, "difference": null },
>   { "command": "env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN raws sts get-caller-identity --region us-east-1 2>&1",
>     "aws_command": "env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN aws sts get-caller-identity --region us-east-1 2>&1",
>     "category": "error", "matches_aws_cli": null, "tested": false, "difference": null },
>   { "command": "raws sts get-caller-identity --profile nonexistent-profile-xyz --region us-east-1 2>&1",
>     "aws_command": "aws sts get-caller-identity --profile nonexistent-profile-xyz --region us-east-1 2>&1",
>     "category": "error", "matches_aws_cli": null, "tested": false, "difference": null }
> ]
> ```
> Include tests for: env var credentials, --profile credentials, no credentials (error),
> bad profile (error), expired credentials (error).
>
> **Prioritize services in this order** when creating files. This controls which
> services get tested first by the tester agents:
>
> **Tier 1 (Core — test first):**
> sts, iam, s3api, ec2, dynamodb, lambda, kms, s3
>
> **Tier 2 (Important — test second):**
> sns, sqs, cloudformation, ecs, rds, cloudwatch, logs, secretsmanager, ssm,
> route53, cloudfront, elasticache, elb, elbv2, autoscaling, apigateway, kinesis,
> stepfunctions, codebuild, codepipeline, ecr
>
> **Tier 3 (Everything else):**
> All remaining services, alphabetically.
>
> **Context management:** With ~418 services, discovering all of them in one pass may
> exhaust your context. If this happens, prioritize: discover and write Tier 1 services
> first, then Tier 2, then as many Tier 3 services as context allows. Report back what
> you completed so the main agent can spawn another explorer for the remaining services
> if needed.
>
> **Report back** the total number of services discovered and commands cataloged.
> Do not test any commands — only discover and record.

Wait for the explorer subagent to finish before proceeding to Step 5. If the explorer
only completed a subset of services, you may proceed to testing the discovered services
while spawning another explorer for the next tier.

---

### STEP 5: TESTING PHASE (Tester Subagents — Parallel)

After discovery is complete, spawn **tester subagents** to validate commands. Each tester
works on exactly ONE service (or one special category like global_options/authentication).

**Before each batch, refresh credentials** (they expire after ~1 hour):
```bash
source prompt/.env
eval $(ada credentials print --account $RAWS_TEST_ACCOUNT --role Admin --format env)
```

**Spawn up to 5 tester subagents in parallel.** Never assign the same service to two
agents. Pick the next untested service from `progress/validation/summary.json` using
the tier priority order: global_options and authentication first, then Tier 1, Tier 2,
Tier 3.

**Tester subagent prompt template:**

> You are a tester agent validating `raws` (a Rust AWS CLI reimplementation) against
> the real AWS CLI. You are testing the **{SERVICE_NAME}** service.
>
> **CRITICAL: Do NOT read any raws source code. This is black-box testing only.**
>
> **Setup — run at the start of EVERY Bash call (shell state does not persist):**
> ```bash
> source prompt/.env
> eval $(ada credentials print --account $RAWS_TEST_ACCOUNT --role Admin --format env)
> AWS=$AWS_CLI_VENV/bin/aws
> RAWS=$RAWS_ROOT/target/release/raws
> ```
> Use `$AWS` and `$RAWS` as the CLI binaries in all commands. Do NOT use bare `aws`
> unless you have sourced the venv in that same Bash call.
>
> **Read** the command list from `progress/validation/{SERVICE_NAME}.json`.
>
> **For each command, run these tests in order:**
>
> **Test 1: Help comparison**
> ```bash
> $AWS {service} {command} help 2>&1 | head -100
> $RAWS {service} {command} help 2>&1 | head -100
> ```
> Check: Does raws recognize the command? Does it show similar help text?
> If raws has no help for individual commands, note it once and skip help tests
> for remaining commands in this service.
>
> **Test 2: Read-only command execution** (only for `category: "read"` commands)
> ```bash
> $AWS {service} {command} --region us-east-1 --output json 2>&1
> $RAWS {service} {command} --region us-east-1 --output json 2>&1
> ```
> Compare:
> - JSON structure: same keys, same nesting, same types
> - Key ordering (AWS CLI has specific ordering)
> - Timestamp formats (ISO 8601 vs epoch)
> - Pagination behavior (does raws auto-paginate like AWS CLI?)
> - Empty results: both should return the same empty structure
>
> For read commands that require parameters, check the help to find required params.
> If you can construct a safe invocation with test values, do so. Otherwise, test the
> missing-parameter error message:
> ```bash
> $AWS {service} {command} --region us-east-1 2>&1
> $RAWS {service} {command} --region us-east-1 2>&1
> ```
> Compare: Do both produce similar error messages for missing required params?
>
> **Test 3: Write commands** (for `category: "write"` commands)
> Do NOT execute write commands against real AWS. Only compare:
> - Does the command exist in raws? (try `$RAWS {service} {command} --region us-east-1 2>&1`
>   and check if it says "unknown operation" vs showing a missing-parameter error)
> - If it exists, test the missing-parameter error message format
>
> For write commands, set `matches_aws_cli` to:
> - `true` — raws recognizes the command (shows a parameter error, not "unknown operation")
>   AND the error message format is similar to the AWS CLI's error for the same input
> - `false` — raws does not recognize the command, or the error format differs significantly
>
> **Test 4: Output format comparison** (for a few representative read commands per service)
> ```bash
> $AWS {service} {command} --output text --region us-east-1 2>&1
> $RAWS {service} {command} --output text --region us-east-1 2>&1
>
> $AWS {service} {command} --output table --region us-east-1 2>&1
> $RAWS {service} {command} --output table --region us-east-1 2>&1
> ```
>
> **Recording results:**
>
> For each command in the JSON file, update:
> - `"tested": true`
> - `"matches_aws_cli": true` if output is identical or functionally equivalent
> - `"matches_aws_cli": false` if there is any meaningful difference
> - `"difference"` — if false, describe the difference concisely. Examples:
>   - `"raws returns keys in alphabetical order, aws uses service-defined order"`
>   - `"raws missing --query support"`
>   - `"raws returns epoch timestamps, aws returns ISO 8601"`
>   - `"command not recognized by raws"`
>   - `"error message format differs: raws says X, aws says Y"`
>
> **What counts as "matching":**
> - JSON values are the same (key order differences alone are NOT a failure if
>   the structure and values match — but note it)
> - Dynamic values (timestamps, request IDs, account IDs) naturally differ — ignore those
> - Empty list results (`{"Vpcs": []}` vs `{"Vpcs": []}`) should match structurally
>
> **What counts as a "difference":**
> - Missing keys or extra keys in the JSON response
> - Different JSON structure or nesting
> - Command not recognized by raws
> - Different error message format for the same error condition
> - Missing support for an output format (--output text/table)
> - Different pagination behavior
> - raws crashes or panics
>
> **Write the updated JSON** back to `progress/validation/{SERVICE_NAME}.json`.
>
> **Report back:**
> - Total commands tested
> - How many passed vs failed
> - A summary list of all differences found (service, command, difference description)

After each batch of tester subagents completes, update `progress/validation/summary.json`
(the validation tracker, NOT `progress/summary.json`) with the results:
```json
"service_<name>": {
  "commands": "<name>.json",
  "commands_created": true,
  "commands_tested": true,
  "total_commands": <N>,
  "tested": <N>,
  "passed": <P>,
  "failed": <F>
}
```

Then spawn the next batch of tester subagents for untested services.

---

### STEP 6: ISSUE REPORTING

After each batch of tester subagents completes, collect all differences (where
`matches_aws_cli: false`) and create issue files.

**Create a new issues file** in the main progress directory:
```
progress/issues-XX.json
```
where `XX` is the next available number (check `progress/summary.json` for existing
issues entries).

**Issues file format** (same as existing issues files):
```json
[
  {
    "id": "validation-<service>-<short-description>",
    "description": "Clear description of the difference between raws and aws CLI",
    "severity": "critical | medium | low",
    "verify": [
      { "run": "<raws command that shows the issue>", "expect": "<what aws CLI produces>" }
    ],
    "passes": false,
    "affected_files": [],
    "notes": ["Found by validation agent. AWS CLI output: <X>, raws output: <Y>"]
  }
]
```

**Severity guidelines:**
- `"critical"` — command not recognized, crashes/panics, completely wrong output structure,
  missing service support
- `"medium"` — wrong key ordering, incorrect timestamp format, missing output format
  support (text/table), pagination differences, incorrect error messages
- `"low"` — minor formatting differences, cosmetic help text differences, edge cases

**After creating the issues file,** update `progress/summary.json` to register it:
```json
"issues": {
  "1": { "total": 6, "passing": 6, "file": "issues-01.json", "description": "..." },
  "XX": { "total": <N>, "passing": 0, "file": "issues-XX.json", "description": "Validation: <services tested>" }
}
```

**Group issues sensibly:**
- One issues file per testing session (not per service) to avoid file explosion
- Aim for 10-30 issues per file
- If a single session finds more than 30 issues, split into multiple files by theme
  (e.g., "output formatting issues", "missing command issues", "error handling issues")

---

### STEP 7: REPEAT OR WRAP UP

If context allows, go back to Step 5 and spawn the next batch of tester subagents
for untested services.

**Session pacing:**
- Each session should aim to test 5-15 services depending on complexity
- Tier 1 services (sts, iam, s3api, ec2, dynamodb, lambda, kms, s3) are the most
  important — complete all of these before moving to Tier 2
- global_options and authentication should be tested in the very first session
- At the end of each session, update `progress/validation/summary.json` so the next
  session knows where to resume

---

### STEP 8: END SESSION CLEANLY

1. Ensure all `progress/validation/*.json` files are written and consistent
2. Ensure `progress/validation/summary.json` is up to date
3. Ensure any new issues files are created and registered in `progress/summary.json`
4. Commit all validation progress:
   ```bash
   git add progress/validation/ progress/issues-*.json progress/summary.json
   git commit -m "Validation: tested <services list>

   - <N> services tested, <P> passed, <F> failed
   - <summary of key findings>
   "
   ```
5. Print a summary of what was tested and key findings

**Never end a session with partially written JSON files.** If you are running low on
context, finish the current batch of services, write all results, and stop cleanly.

---

### SUBAGENT COORDINATION RULES

1. **Explorer runs first, alone.** No tester agents until discovery is complete (or at
   least the current tier's services are discovered).
2. **One agent per service.** Never assign the same service to two tester agents.
   This prevents file write conflicts and duplicate API calls.
3. **Batch size: up to 5 parallel testers.** More than this can overwhelm the
   credential rate limits. Adjust down if you see throttling errors.
4. **Tester agents are independent.** Each reads its own service JSON, tests its
   commands, and writes results. No tester depends on another tester's output.
5. **Main agent coordinates.** After each batch completes, the main agent:
   - Updates summary.json
   - Collects differences for issue reporting
   - Picks the next batch of services
   - Spawns the next round of testers

---

### RESUMING A PREVIOUS SESSION

If `progress/validation/summary.json` already exists:
1. Read it to see which services have been discovered and tested
2. Skip the explorer subagent if all services are already discovered
   (or run it only for newly added services)
3. Find the first untested service in priority order and resume from Step 5
4. Read existing issues files (in `progress/`) to avoid reporting duplicate issues

---

### REFERENCE: Priority Services

**Tier 1 (Core — ~8 services):**
sts, iam, s3api, ec2, dynamodb, lambda, kms, s3

**Tier 2 (Important — ~21 services):**
sns, sqs, cloudformation, ecs, rds, cloudwatch, logs, secretsmanager, ssm,
route53, cloudfront, elasticache, elb, elbv2, autoscaling, apigateway, kinesis,
stepfunctions, codebuild, codepipeline, ecr

**Tier 3 (Everything else — ~387 services):**
All remaining services, alphabetically.

---

**Remember:** You are testing for AWS CLI parity. The goal is that a user can replace
`aws` with `raws` in any command and get the same result. Every difference you find
is valuable — it tells the coding agents exactly what to fix.

Begin by running Step 1.
