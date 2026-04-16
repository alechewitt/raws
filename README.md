```
                                          ,%%%%%%%%,
    ____  ___ _       _______           ,%%/\%%%%/\%%
   / __ \/   | |     / / ___/          ,%%%\c "" J/%%%
  / /_/ / /| | | /| / /\__ \  %.       %%%%/ o  o \%%%
 / _, _/ ___ | |/ |/ /___/ /  `%%.     %%%%    _  |%%%
/_/ |_/_/  |_|__/|__//____/    `%%     `%%%%(__Y__)%%'
                               //       ;%%%%`\-/%%%'
       Rust AWS CLI           ((       /  `%%%%%%%'
                               \\    .'          |
                                \\  /       \  | |
                                 \\/         ) | |
                                  \         /_ | |__
                                  (___________))))))) 
```

A Rust reimplementation of the [AWS CLI](https://github.com/aws/aws-cli), built entirely by Claude Code running autonomously in a loop.

## What is this?

`raws` is a drop-in replacement for the `aws` CLI, supporting the full ~416 services covered by [botocore](https://github.com/boto/botocore) in a single binary.

```
$ raws s3 ls
2024-01-15 09:23:45 my-app-logs
2024-03-22 14:07:12 my-data-bucket
2024-06-01 11:30:00 my-static-assets

$ raws sts get-caller-identity
{
    "UserId": "AIDACKCEVSQ6C2EXAMPLE",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/Alec"
}
```

The entire codebase (~38,000 lines of Rust and 1,180 tests) was written by Claude Code. 

## How it was built

A shell script runs `claude --print` in an infinite loop. Each invocation gets a prompt, does as much work as it can within its context window, commits its progress, and exits. The next invocation picks up where the last one left off.

```bash
while true; do
    if $first_time; then
        prompt="initializer_prompt.md"
    else
        prompt="coding_prompt.md"
    fi
    claude --print "$(cat $prompt)"  # autonomous session: implement, test, commit
done
```

The actual script is at [`prompt/run_agent.sh`](prompt/run_agent.sh). It handles logging, dirty-repo recovery, and prompt selection.

This approach was inspired by Anthropic's [autonomous coding quickstart](https://github.com/anthropics/claude-quickstarts/tree/main/autonomous-coding) and their post on [building a C compiler with Claude](https://www.anthropic.com/engineering/building-c-compiler).

### The prompts

All prompts live in [`prompt/`](prompt/):

| File | Purpose |
|------|---------|
| [`initializer_prompt.md`](prompt/initializer_prompt.md) | First-run setup: creates the Cargo project, copies service models, defines milestones |
| [`coding_prompt.md`](prompt/coding_prompt.md) | Main loop: picks the next feature, implements it with a sub-agent, reviews it with another sub-agent, commits |
| [`validation_prompt.md`](prompt/validation_prompt.md) | Black-box testing: runs `raws` and `aws` side-by-side, compares output, files issues |
| [`spec.txt`](prompt/spec.txt) | Full project specification — architecture, protocols, signing, milestones, success criteria |

### Progress tracking

Each session reads and updates JSON files in `progress/` to know what's done and what to work on next. Features are defined upfront per milestone, and each one is marked `passes: true` only after implementation *and* review by a separate sub-agent.

## Installation

If you just want to use the tool, install it with Homebrew:

```bash
brew install alechewitt/tap/raws
```

## Building from source

```bash
cargo build --release
```

## Architecture

Single crate, single binary. The key modules:

- **`core/model/`** — Loads botocore JSON service models at runtime
- **`core/auth/`** — SigV4 request signing
- **`core/credentials/`** — Provider chain: env vars, profiles, SSO, IMDS, assume-role, credential_process
- **`core/protocol/`** — Serializers/parsers for all four AWS protocols (query, json, rest-json, rest-xml, ec2)
- **`core/endpoint/`** — Endpoint resolution with partition/region/FIPS/dualstack support
- **`cli/`** — Argument parsing, output formatting (json/text/table), pagination, waiters
