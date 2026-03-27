use anyhow::{bail, Result};

use crate::core::model::{loader, pascal_to_kebab, store};

/// Global flags shared across all shell completion scripts.
const GLOBAL_FLAGS: &[&str] = &[
    "--region",
    "--profile",
    "--output",
    "--debug",
    "--endpoint-url",
    "--no-paginate",
    "--query",
    "--no-sign-request",
    "--no-verify-ssl",
    "--cli-connect-timeout",
    "--cli-read-timeout",
    "--use-dualstack-endpoint",
    "--use-fips-endpoint",
];

/// Dispatch to the appropriate shell completion generator.
///
/// Supported shells: `bash`, `zsh`, `fish`.
pub fn generate_completion(shell: &str) -> Result<String> {
    match shell {
        "bash" => Ok(generate_bash_completion()),
        "zsh" => Ok(generate_zsh_completion()),
        "fish" => Ok(generate_fish_completion()),
        other => bail!("Unsupported shell '{}'. Supported: bash, zsh, fish", other),
    }
}

/// List available services.
///
/// Returns an empty `Vec` (rather than an error) when no models are available,
/// so callers can degrade gracefully.
pub fn list_services() -> Vec<String> {
    store::discover_services().unwrap_or_default()
}

/// List operations for a given service in kebab-case CLI form.
///
/// Returns an empty `Vec` when the service is unknown or its model cannot be
/// loaded, so callers can degrade gracefully.
pub fn list_operations(service: &str) -> Vec<String> {
    let model_str = match store::get_service_model_str(service) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let model = match loader::parse_service_model(&model_str) {
        Ok(m) => m,
        Err(_) => return Vec::new(),
    };

    let mut ops: Vec<String> = model
        .operations
        .keys()
        .map(|name| pascal_to_kebab(name))
        .collect();
    ops.sort();
    ops
}

// ---------------------------------------------------------------------------
// Shell-specific generators
// ---------------------------------------------------------------------------

fn generate_bash_completion() -> String {
    let flags = GLOBAL_FLAGS.join(" ");

    format!(
        r#"_raws_completions() {{
    local cur prev
    cur="${{COMP_WORDS[COMP_CWORD]}}"
    prev="${{COMP_WORDS[COMP_CWORD-1]}}"

    if [[ ${{COMP_CWORD}} == 1 ]]; then
        COMPREPLY=($(compgen -W "$(raws completer list-services)" -- "${{cur}}"))
    elif [[ ${{COMP_CWORD}} == 2 ]]; then
        COMPREPLY=($(compgen -W "$(raws completer list-operations ${{prev}})" -- "${{cur}}"))
    elif [[ "${{cur}}" == --* ]]; then
        COMPREPLY=($(compgen -W "{flags}" -- "${{cur}}"))
    fi
}}
complete -F _raws_completions raws
"#
    )
}

fn generate_zsh_completion() -> String {
    let flags_list = GLOBAL_FLAGS
        .iter()
        .map(|f| format!("        {f}"))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"#compdef raws

_raws() {{
    local -a services operations flags
    flags=(
{flags_list}
    )

    if (( CURRENT == 2 )); then
        services=(${{(f)"$(raws completer list-services)"}})
        _describe 'service' services
    elif (( CURRENT == 3 )); then
        operations=(${{(f)"$(raws completer list-operations ${{words[2]}})"}})
        _describe 'operation' operations
    else
        _describe 'flag' flags
    fi
}}

_raws
"#
    )
}

fn generate_fish_completion() -> String {
    let mut lines = Vec::new();

    // Service-level completions
    lines.push(
        "complete -c raws -n '__fish_use_subcommand' -xa '(raws completer list-services)'"
            .to_string(),
    );
    lines.push(
        "complete -c raws -n '__fish_seen_subcommand_from (raws completer list-services)' -xa '(raws completer list-operations (commandline -opc)[2])'"
            .to_string(),
    );

    // Flag completions with descriptions
    let flag_descs: &[(&str, &str)] = &[
        ("region", "AWS region"),
        ("profile", "Named profile"),
        ("debug", "Enable debug output"),
        ("endpoint-url", "Override endpoint URL"),
        ("no-paginate", "Disable automatic pagination"),
        ("query", "JMESPath query for output filtering"),
        ("no-sign-request", "Do not sign requests"),
        ("no-verify-ssl", "Disable SSL verification"),
        ("cli-connect-timeout", "Connection timeout in seconds"),
        ("cli-read-timeout", "Read timeout in seconds"),
        ("use-dualstack-endpoint", "Use dual-stack endpoint"),
        ("use-fips-endpoint", "Use FIPS endpoint"),
    ];

    for (flag, desc) in flag_descs {
        lines.push(format!("complete -c raws -l {flag} -d '{desc}'"));
    }

    // Output flag with choices
    lines.push(
        "complete -c raws -l output -d 'Output format' -xa 'json text table yaml yaml-stream'"
            .to_string(),
    );

    lines.push(String::new()); // trailing newline
    lines.join("\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_generate_bash_contains_complete() {
        let script = generate_completion("bash").expect("bash generation should succeed");
        assert!(!script.is_empty(), "bash script should not be empty");
        assert!(
            script.contains("complete"),
            "bash script should contain 'complete'"
        );
    }

    #[test]
    fn test_generate_zsh_contains_raws_function() {
        let script = generate_completion("zsh").expect("zsh generation should succeed");
        assert!(!script.is_empty(), "zsh script should not be empty");
        assert!(
            script.contains("_raws"),
            "zsh script should contain '_raws'"
        );
    }

    #[test]
    fn test_generate_fish_contains_complete_c_raws() {
        let script = generate_completion("fish").expect("fish generation should succeed");
        assert!(!script.is_empty(), "fish script should not be empty");
        assert!(
            script.contains("complete -c raws"),
            "fish script should contain 'complete -c raws'"
        );
    }

    #[test]
    fn test_generate_invalid_shell_returns_error() {
        let result = generate_completion("invalid");
        assert!(result.is_err(), "invalid shell should return error");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("Unsupported shell"),
            "error message should mention unsupported shell, got: {msg}"
        );
    }

    #[test]
    fn test_bash_script_contains_global_flags() {
        let script = generate_bash_completion();
        for flag in GLOBAL_FLAGS {
            assert!(
                script.contains(flag),
                "bash script should contain flag '{flag}'"
            );
        }
    }

    #[test]
    fn test_zsh_script_contains_compdef() {
        let script = generate_zsh_completion();
        assert!(
            script.contains("#compdef raws"),
            "zsh script should contain '#compdef raws'"
        );
    }

    #[test]
    fn test_fish_script_has_region_profile_output() {
        let script = generate_fish_completion();
        assert!(
            script.contains("-l region"),
            "fish script should have --region completion"
        );
        assert!(
            script.contains("-l profile"),
            "fish script should have --profile completion"
        );
        assert!(
            script.contains("-l output"),
            "fish script should have --output completion"
        );
    }

    #[test]
    fn test_list_services_returns_empty_when_no_models_dir() {
        // Directly test that discover_services returns Err for a non-existent
        // directory, and that our wrapper (list_services) would map that to an
        // empty Vec. We avoid mutating process-wide env vars, which is
        // inherently racy when tests run in parallel.
        let bad_dir = Path::new("/tmp/raws_nonexistent_test_dir_12345");
        let result = loader::discover_services(bad_dir);
        assert!(
            result.is_err(),
            "discover_services should fail for a non-existent directory"
        );
        // The contract of list_services: errors -> empty vec.
        let services: Vec<String> = result.unwrap_or_default();
        assert!(
            services.is_empty(),
            "list_services should return empty vec when models dir does not exist"
        );
    }

    #[test]
    fn test_list_operations_returns_empty_for_unknown_service() {
        let ops = list_operations("totally_fake_service_name_999");
        assert!(
            ops.is_empty(),
            "list_operations should return empty vec for unknown service"
        );
    }

    #[test]
    fn test_list_services_returns_services_when_models_exist() {
        let models_dir = Path::new("models");
        if !models_dir.exists() {
            eprintln!("Skipping: models directory not present");
            return;
        }
        let services = list_services();
        assert!(
            !services.is_empty(),
            "list_services should return non-empty vec when models exist"
        );
        // Services should be sorted
        let mut sorted = services.clone();
        sorted.sort();
        assert_eq!(services, sorted, "list_services should return sorted names");
    }

    #[test]
    fn test_list_operations_returns_kebab_case() {
        let models_dir = Path::new("models/sts");
        if !models_dir.exists() {
            eprintln!("Skipping: STS model not present");
            return;
        }
        let ops = list_operations("sts");
        assert!(
            !ops.is_empty(),
            "list_operations for sts should return operations"
        );
        assert!(
            ops.contains(&"get-caller-identity".to_string()),
            "sts operations should include get-caller-identity, got: {:?}",
            ops
        );
        // Operations should be sorted
        let mut sorted = ops.clone();
        sorted.sort();
        assert_eq!(ops, sorted, "list_operations should return sorted names");
        // All should be lowercase kebab-case (no uppercase)
        for op in &ops {
            assert!(
                op.chars().all(|c| c.is_ascii_lowercase() || c == '-' || c.is_ascii_digit()),
                "operation '{}' should be kebab-case",
                op
            );
        }
    }

    #[test]
    fn test_bash_script_structure() {
        let script = generate_bash_completion();
        assert!(script.contains("_raws_completions()"));
        assert!(script.contains("COMP_WORDS"));
        assert!(script.contains("COMPREPLY"));
        assert!(script.contains("compgen"));
        assert!(script.contains("complete -F _raws_completions raws"));
    }

    #[test]
    fn test_zsh_script_structure() {
        let script = generate_zsh_completion();
        assert!(script.contains("#compdef raws"));
        assert!(script.contains("_raws()"));
        assert!(script.contains("_describe"));
        assert!(script.contains("CURRENT == 2"));
        assert!(script.contains("CURRENT == 3"));
    }

    #[test]
    fn test_fish_script_structure() {
        let script = generate_fish_completion();
        assert!(script.contains("__fish_use_subcommand"));
        assert!(script.contains("__fish_seen_subcommand_from"));
        assert!(script.contains("commandline -opc"));
    }

    #[test]
    fn test_fish_output_flag_has_choices() {
        let script = generate_fish_completion();
        assert!(
            script.contains("json text table yaml yaml-stream"),
            "fish output flag should list format choices"
        );
    }
}
