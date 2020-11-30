#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
#[allow(unused_imports)]
use snafu::{ResultExt, ensure};
use std::path::{Path};
use crate::errors::*;
use crate::dbcon;
use std::rc::Rc;
use std::cell::RefCell;
use super::node;
use std::pin::Pin;
use std::future::Future;


pub async fn update_db_with_channel_names_for_current_node() -> Result<(), Error> {
  match super::backends::Facility::from_current_hostname().await {
    Ok(fac) => {
      update_db_with_channel_names(&fac.facility, fac.base_dir, &fac.dbinfo).await?;
    }
    Err(e) => {
      let hostname = super::node::get_hostname();
      error!("no fixed conf for host {}  {:?}", hostname, e);
      ensure!(false, Bad { msg: format!("bad host: {}", hostname) });
      unreachable!();
    }
  }
  Ok(())
}

pub async fn update_db_with_channel_names(facility: &str, base_dir: impl AsRef<Path>, dbinfo: &dbcon::DbInfo) -> Result<(), Error> {
  let c1 = Rc::new(RefCell::new(0u32));
  let dbc = dbcon::connect_db(dbinfo).await?;
  let rows = dbc.query("select rowid from facilities where name = $1", &[&facility]).await?;
  ensure!(rows.len() == 1, Bad{msg:format!("can not find facility {}", facility)});
  let facility: i64 = rows[0].try_get(0)?;
  dbc.query("begin", &[]).await?;
  let dbc = Rc::new(dbc);
  super::diskformatv0::find_channel_names_from_config(base_dir, |ch| {
    let ch = ch.to_owned();
    let dbc = dbc.clone();
    let c1 = c1.clone();
    async move {
      super::utils::delay_io_short().await;
      dbc.query("insert into channels (facility, name) values ($1, $2) on conflict do nothing", &[&facility, &ch]).await?;
      *c1.borrow_mut() += 1;
      let c2 = *c1.borrow();
      if c2 % 200 == 0 {
        trace!("channels {:6}  current {}", c2, ch);
        dbc.query("commit", &[]).await?;
        super::utils::delay_io_medium().await;
        dbc.query("begin", &[]).await?;
      }
      Ok(())
    }
  }).await?;
  dbc.query("commit", &[]).await.context(SE!(DbError))?;
  Ok(())
}


pub async fn update_db_with_all_channel_configs_for_current_node() -> Result<(), Error> {
  let fac = crate::backends::Facility::from_current_hostname().await?;
  update_db_with_all_channel_configs(&fac.node, &fac.base_dir, &fac.ks_prefix, &fac.dbinfo).await?;
  Ok(())
}

pub async fn update_db_with_all_channel_configs(node: &node::Node, base_dir: impl AsRef<Path>, ks_prefix: &str, dbinfo: &crate::dbcon::DbInfo) -> Result<(), Error> {
  let dbc = dbcon::connect_db(dbinfo).await?;
  let rows = dbc.query("select rowid, facility, name from channels where facility = $1 order by facility, name", &[&node.facility()]).await?;
  let mut c1 = 0;
  dbc.query("begin", &[]).await?;
  let mut count_inserted = 0;
  let mut count_updated = 0;
  for row in rows {
    let rowid: i64 = row.try_get(0)?;
    let _facility: i64 = row.try_get(1)?;
    let channel: String = row.try_get(2)?;
    match update_db_with_channel_config(node, &base_dir, ks_prefix, rowid, &channel, &dbc, &mut count_inserted, &mut count_updated).await {
      Err(Error::ChannelConfigdirNotFound{..}) => {
        warn!("can not find channel config {}", channel);
      }
      Err(e) => {
        error!("{:?}", e);
      }
      _ => {
        c1 += 1;
        if c1 % 200 == 0 {
          trace!("channel no {:6}  inserted {:6}  updated {:6}", c1, count_inserted, count_updated);
          dbc.query("commit", &[]).await?;
          dbc.query("begin", &[]).await?;
        }
      }
    }
  }
  dbc.query("commit", &[]).await?;
  info!("updating cache...");
  dbc.query("select update_cache()", &[]).await?;
  info!("updating cache done");
  Ok(())
}

/**
Parse the config of the given channel and update database.
*/
pub async fn update_db_with_channel_config(
  node: &node::Node,
  base_dir: impl AsRef<Path>,
  _ks_prefix: &str,
  channel_id: i64,
  channel: &str,
  dbc: &dbcon::DbClient,
  count_inserted: &mut usize,
  count_updated: &mut usize,
) -> Result<(), Error>
{
  let mut path = base_dir.as_ref().to_owned();
  path.push("config");
  path.push(channel);
  path.push("latest");
  path.push("00000_Config");
  let meta = tokio::fs::metadata(&path).await.context(ChannelConfigdirNotFound{path:&path})?;
  ensure!(meta.len() < 8 * 1024 * 1024, ConfigFileTooLarge{path});
  let rows = dbc.query("select rowid, fileSize, parsedUntil, channel from configs where node = $1 and channel = $2", &[&node.rowid(), &channel_id]).await?;
  ensure!(rows.len() <= 1, ExpectMaxOneRow);
  let (config_id, do_parse) = if rows.len() > 0 {
    let row = &rows[0];
    let rowid: i64 = row.get(0);
    let file_size: u32 = row.get::<_, i64>(1) as u32;
    let parsed_until: u32 = row.get::<_, i64>(2) as u32;
    let _channel_id = row.get::<_, i64>(2) as i64;
    ensure!(meta.len() >= file_size as u64, ConfigFileOnDiskShrunk{path});
    ensure!(meta.len() >= parsed_until as u64, ConfigFileOnDiskShrunk{path});
    (Some(rowid), meta.len() != parsed_until as u64)
  }
  else {
    (None, true)
  };
  if do_parse {
    let buf = tokio::fs::read(&path).await?;
    let config = parsersdaq::configfile::parseConfig(&buf)?;
    match config_id {
      None => {
        dbc.query(
          "insert into configs (node, channel, fileSize, parsedUntil, config) values ($1, $2, $3, $4, $5)",
          &[&node.rowid(), &channel_id, &(meta.len() as i64), &(buf.len() as i64), &serde_json::to_value(config)?],
        ).await?;
        *count_inserted += 1;
      }
      Some(_config_id_2) => {
        dbc.query(
          "insert into configs (node, channel, fileSize, parsedUntil, config) values ($1, $2, $3, $4, $5) on conflict (node, channel) do update set fileSize = $3, parsedUntil = $4, config = $5",
          &[&node.rowid(), &channel_id, &(meta.len() as i64), &(buf.len() as i64), &serde_json::to_value(config)?],
        ).await?;
        *count_updated += 1;
      }
    }
  }
  Ok(())
}




pub async fn update_db_with_all_channel_datafiles_for_current_node() -> Result<(), Error> {
  let fac = crate::backends::Facility::from_current_hostname().await?;
  update_db_with_all_channel_datafiles(&fac.node, &fac.base_dir, &fac.ks_prefix, &fac.dbinfo).await?;
  Ok(())
}


pub async fn update_db_with_all_channel_datafiles(node: &node::Node, base_dir: impl AsRef<Path>, ks_prefix: &str, dbinfo: &crate::dbcon::DbInfo) -> Result<(), Error> {
  let dbc = Rc::new(dbcon::connect_db(dbinfo).await?);
  let rows = dbc.query("select rowid, facility, name from channels where facility = $1 order by facility, name", &[&node.facility()]).await?;
  let mut c1 = 0;
  dbc.query("begin", &[]).await?;
  for row in rows {
    let rowid: i64 = row.try_get(0)?;
    let _facility: i64 = row.try_get(1)?;
    let channel: String = row.try_get(2)?;
    update_db_with_channel_datafiles(node, &base_dir, ks_prefix, rowid, &channel, dbc.clone()).await?;
    c1 += 1;
    if c1 % 40 == 0 {
      trace!("import datafiles  {}  {}", c1, channel);
      dbc.query("commit", &[]).await?;
      dbc.query("begin", &[]).await?;
    }
    if false && c1 >= 30 {
      break;
    }
  }
  dbc.query("commit", &[]).await?;
  Ok(())
}

struct DatafileDbWriter {
  channel_id: i64,
  node_id: i64,
  dbc: Rc<dbcon::DbClient>,
  c1: Rc<RefCell<u32>>,
}

impl super::diskformatv0::ChannelDatafileDescSink for DatafileDbWriter {
  fn sink(&self, k: super::diskformatv0::ChannelDatafileDesc) -> Pin<Box<dyn Future<Output=Result<(), Error>>>> {
    let dbc = self.dbc.clone();
    let c1 = self.c1.clone();
    let channel_id = self.channel_id;
    let node_id = self.node_id;
    Box::pin(async move {
      dbc.query(
        "insert into datafiles (node, channel, tsbeg, tsend, props) values ($1, $2, $3, $4, $5) on conflict do nothing",
        &[
          &node_id,
          &channel_id,
          &(k.timebin() as i64 * k.binsize() as i64),
          &((k.timebin() + 1) as i64 * k.binsize() as i64),
          &serde_json::to_value(k)?,
        ]
      ).await?;
      *c1.try_borrow_mut().unwrap() += 1;
      Ok(())
    })
  }
}

pub async fn update_db_with_channel_datafiles(
  node: &node::Node,
  base_dir: impl AsRef<Path>,
  ks_prefix: &str,
  channel_id: i64,
  channel: &str,
  dbc: Rc<dbcon::DbClient>,
) -> Result<(), Error>
{
  let writer = DatafileDbWriter {
    node_id: node.rowid(),
    channel_id: channel_id,
    dbc: dbc.clone(),
    c1: Rc::new(RefCell::new(0)),
  };
  let mut n_nothing = 0;
  for ks in &[2, 3, 4] {
    match super::diskformatv0::find_channel_datafiles_in_ks(
      &base_dir,
      ks_prefix,
      *ks,
      channel,
      &writer,
    ).await {
      Err(Error::ChannelDatadirNotFound{..}) => {
        n_nothing += 1;
      }
      x => x?
    }
    if false && *writer.c1.borrow() >= 10 {
      break;
    }
  }
  if n_nothing >= 3 {
    //warn!("No datafile directories in any keyspace  writer got {:5}  n_nothing {}  channel {}", writer.c1.borrow(), n_nothing, channel);
  }
  Ok(())
}
