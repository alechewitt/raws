mod core;
mod cli;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    cli::driver::run().await
}
