use anyhow::{bail, Result};
use clap::Parser;

use crate::cli::args::GlobalArgs;

pub async fn run() -> Result<()> {
    let args = GlobalArgs::parse();

    let service = match &args.service {
        Some(s) => s,
        None => {
            bail!("Usage: raws <service> <operation> [--params...]\n\nRun 'raws --help' for more information.");
        }
    };

    let operation = match &args.operation {
        Some(o) => o,
        None => {
            bail!("Usage: raws {service} <operation> [--params...]\n\nMissing operation name.");
        }
    };

    if args.debug {
        eprintln!("[debug] service={service} operation={operation}");
        eprintln!("[debug] region={:?} profile={:?} output={}", args.region, args.profile, args.output);
    }

    // TODO: Wire up the full pipeline in milestone 5
    // 1. Load service model
    // 2. Find operation
    // 3. Parse operation arguments
    // 4. Resolve credentials
    // 5. Build request using protocol serializer
    // 6. Sign request
    // 7. Send HTTP request
    // 8. Parse response
    // 9. Format and print output

    bail!("Service dispatch not yet implemented. Run 'cargo test' to verify the build.");
}
