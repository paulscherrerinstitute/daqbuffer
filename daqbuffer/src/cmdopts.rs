use clap::{Clap, crate_version};

#[derive(Debug, Clap)]
#[clap(name="daqbuffer", author="Dominik Werder <dominik.werder@gmail.com>", version=crate_version!())]
pub struct Opts {
  // default_value="..."
  #[clap(short, long, parse(from_occurrences))]
  pub verbose: i32,
  #[clap(subcommand)]
  pub subcmd: SubCmd,
}

#[derive(Debug, Clap)]
pub enum SubCmd {
  Version,
  Import(Import),
  Proxy(Proxy),
}

#[derive(Debug, Clap)]
pub struct Import {
  #[clap(subcommand)]
  pub subcmd: ImportEnum,
}

#[derive(Debug, Clap)]
pub enum ImportEnum {
  //#[clap()]
  Channels(ImportChannels),
  Configs(ImportConfigs),
  Datafiles(ImportDatafiles),
}

#[derive(Debug, Clap)]
pub struct ImportChannels {
}

#[derive(Debug, Clap)]
pub struct ImportConfigs {
}

#[derive(Debug, Clap)]
pub struct ImportDatafiles {
}

#[derive(Debug, Clap)]
pub struct Proxy {
  #[clap(subcommand)]
  pub subcmd: ProxyEnum,
}

#[derive(Debug, Clap)]
pub enum ProxyEnum {
  Lib,
  DataApi,
}
