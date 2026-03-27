mod core;
mod cli;

use std::process;

#[tokio::main]
async fn main() {
    // Install SIGINT handler: exit with 130 (matching AWS CLI behavior)
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() {
            process::exit(130);
        }
    });

    match cli::driver::run().await {
        Ok(()) => process::exit(0),
        Err(e) => {
            let code = crate::core::error::classify_exit_code(&e);
            eprintln!("raws: [ERROR]: {e:#}");
            process::exit(code);
        }
    }
}
