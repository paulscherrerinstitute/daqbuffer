#![allow(non_snake_case)]
#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use daqbuffer::cmdopts::*;
use daqbuffer::errors::*;

pub async fn main2() -> Result<(), Error> {
  use clap::Clap;
  let opts = Opts::parse();
  daqbuffer::main_with_cli_parsed(&opts).await?;
  Ok(())
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt()
  .with_env_filter(tracing_subscriber::EnvFilter::new("daqbuffer=trace,tokio_postgres=info"))
  .init();
  main2().await.unwrap();
}
