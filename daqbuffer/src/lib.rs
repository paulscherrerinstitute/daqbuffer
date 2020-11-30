pub mod errors;
pub mod cmdopts;
pub mod node;
pub mod dbcon;
pub mod config;
pub mod diskformatv0;
pub mod scan;
pub mod utils;
pub mod proxy;
pub mod netpod;
pub mod backends;

use tracing::info;
use errors::*;
use crate::cmdopts::Opts;

pub async fn main_with_cli_parsed(opts: &Opts) -> Result<(), Error> {
  use crate::cmdopts::SubCmd;
  match &opts.subcmd {
    SubCmd::Version => {
      use clap::crate_version;
      eprintln!("{}", crate_version!())
    }
    SubCmd::Import(import) => {
      use crate::cmdopts::ImportEnum::*;
      match &import.subcmd {
        Channels(_k) => scan::update_db_with_channel_names_for_current_node().await?,
        Configs(_k) => {
          scan::update_db_with_channel_names_for_current_node().await?;
          scan::update_db_with_all_channel_configs_for_current_node().await?;
          info!("Done with channel names and configs");
        },
        Datafiles(_k) => scan::update_db_with_all_channel_datafiles_for_current_node().await?,
      }
    }
    SubCmd::Proxy(proxy) => {
      use crate::cmdopts::ProxyEnum;
      match &proxy.subcmd {
        ProxyEnum::Lib => proxy::proxy_lib().await?,
        ProxyEnum::DataApi => proxy::data_api().await?,
      }
    }
  }
  Ok(())
}
