#![allow(non_snake_case)]
#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use crate::errors::*;
pub use tokio_postgres::Client as DbClient;
use snafu::ResultExt;

pub struct DbInfo {
  host: String,
  user: String,
  pass: String,
}

impl DbInfo {
  pub fn gls_archive() -> Self {
    Self {
      host: "127.0.0.1".into(),
      user: "daqbuffer".into(),
      pass: "daqbuffer".into(),
    }
  }
  pub fn hipa_archive() -> Self {
    Self {
      host: "127.0.0.1".into(),
      user: "daqbuffer".into(),
      pass: "daqbuffer".into(),
    }
  }
  pub fn sf_databuffer() -> Self {
    Self {
      host: "sf-daqbuf-33".into(),
      user: "daqbuffer".into(),
      pass: "daqbuffer".into(),
    }
  }
  pub fn sf_imagebuffer() -> Self {
    Self {
      host: "sf-daqbuf-33".into(),
      user: "daqbuffer".into(),
      pass: "daqbuffer".into(),
    }
  }
}

pub async fn connect_db(info: &DbInfo) -> Result<DbClient, Error> {
  let (client, connection) = tokio_postgres::connect(&format!("host={} user={} password={}", info.host, info.user, info.pass), tokio_postgres::NoTls).await
  .context(DbConnect{})?;
  tokio::spawn(async move {
    if let Err(e) = connection.await {
      error!("db connection error: {}", e);
    }
  });
  Ok(client)
}
