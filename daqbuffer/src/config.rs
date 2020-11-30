#![allow(non_snake_case)]
#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use crate::errors::*;
use crate::node::{get_node, Node};
use crate::dbcon::*;
use std::path::{Path, PathBuf};
use tokio::fs::{metadata, File};
use tokio::io::AsyncReadExt;
use std::io::ErrorKind;
use parsersdaq::configfile::*;

#[derive(Debug)]
pub struct ConfigCurrentStatus {
  rowid: i64,
  channel: i64,
  fileSize: u64,
  parsedUntil: u64,
}

async fn parseConfigFile(path: &Path, flen: u64, buf1: &mut Vec<u8>) -> Result<(u64, Config), Error> {
  match File::open(&path).await {
    Err(e) => {
      warn!("can not open config file {:?} {:?}", path, e);
      Err(e.into())
    }
    Ok(mut f1) => {
      let n1 = f1.read_exact(&mut buf1[..flen as usize]).await?;
      match parseConfig(&buf1[..n1]) {
        Err(e) => {
          error!("can not parse config {:?}  {:?}", path, e);
          Err(e.into())
        }
        Ok(config) => {
          Ok((n1 as u64, config))
        }
      }
    }
  }
}

async fn checkConfig(node: &Node, basedir: &str, channel: i64, channelName: &str, current: &Option<ConfigCurrentStatus>, buf1: &mut Vec<u8>, db: &DbClient) -> Result<(), Error> {
  let mut path = PathBuf::from(basedir);
  path.push("config");
  path.push(channelName);
  path.push("latest");
  path.push("00000_Config");
  //trace!("try to open config file at {:?}", path);
  match metadata(&path).await {
    Err(e) => {
      if e.kind() == ErrorKind::NotFound {
        warn!("can not stat config file, not found {:?}", path);
      }
      else {
        warn!("can not stat config file {:?}  {:?}", path, e);
      }
      return Ok(());
    },
    Ok(meta) => {
      if meta.len() > 64 * 1024 || meta.len() as usize > buf1.len() {
        error!("config file too large  {}  {:?}", meta.len(), path);
        return Ok(());
      }
      else {
        let fileSizeStat = meta.len() as u64;
        let update = match current {
          None => {
            match parseConfigFile(&path, fileSizeStat, buf1).await {
              Err(e) => {
                error!("can not parse config file {:?}  {:?}", path, e);
                None
              }
              Ok(k) => Some(k)
            }
          }
          Some(cur) => {
            if cur.parsedUntil > fileSizeStat {
              error!("config file for channel shrunk  {}  {}  {:?}", cur.parsedUntil, fileSizeStat, path);
              None
            }
            else if cur.parsedUntil == fileSizeStat {
              // something to do?
              None
            }
            else {
              match parseConfigFile(&path, fileSizeStat, buf1).await {
                Err(e) => {
                  error!("can not parse config file {:?}  {:?}", path, e);
                  None
                }
                Ok(k) => Some(k)
              }
            }
          }
        };
        if let Some((parsedUntil, config)) = update {
          trace!("update config db {} {} {}", node.rowid, channel, channelName);
          let rows = db.query("insert into configs (node, channel, filesize, parseduntil) values ($1, $2, $3, $4)
            on conflict (node, channel) do update set filesize = $3, parseduntil = $4
            returning rowid",
            &[&node.rowid, &channel, &(fileSizeStat as i64), &(parsedUntil as i64)]
          ).await?;
          let configRowid: i64 = rows[0].get(0);
          for entry in &config.entries {
            let compr = match &entry.compressionMethod { None => -1i16, Some(k) => k.to_i16() };
            let shape2: Option<Vec<_>> = match &entry.shape {
              None => None,
              Some(a) => Some(a.iter().map(|x| *x as i16).collect())
            };
            let rows = db.query("insert into configentries (
                config, ts, pulse, ks, bs, splitcount, dtype, compression, isarray, shape, byteorder, sourcename
              ) values (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
              )
              on conflict do nothing
              returning rowid",
              &[
                &configRowid, &entry.ts, &entry.pulse, &(entry.ks as i16), &(entry.bs as i32), &(entry.splitCount as i16), &entry.dtype.to_i16(),
                &(compr as i16),
                & if entry.isArray { 1i16 } else { 0 },
                &shape2,
                & if entry.isBigEndian { 1i16 } else { 0 },
                &entry.sourceName,
              ]
            ).await?;
            if rows.len() == 1 {
              //trace!("INSERTED NEW");
            }
            else {
              //trace!("NOTHING NEW");
            }
          }
        }
        return Ok(());
      }
    }
  }
}

async fn getNodesCurrentChannelConfig(node: &Node, channel: i64, db: &DbClient) -> Result<Option<ConfigCurrentStatus>, Error> {
  let rows = db.query("select rowid, filesize, parseduntil from configs where node=$1 and channel=$2", &[&node.rowid, &channel]).await?;
  if rows.len() <= 0 { return Ok(None); }
  let row = &rows[0];
  Ok(Some(ConfigCurrentStatus {
    rowid: row.get(0),
    channel,
    fileSize: row.get::<_, i64>(1) as u64,
    parsedUntil: row.get::<_, i64>(2) as u64,
  }))
}

pub async fn scanConfig(basedir: &str, dbinfo: &crate::dbcon::DbInfo) -> Result<(), Error> {
  let mut buf1 = vec![0; 512 * 1024];
  let facility = "sf-databuffer";
  let node = get_node(facility, dbinfo).await?;
  // TODO
  let con1 = connect_db(&super::dbcon::DbInfo::sf_databuffer()).await?;
  if false {
    warn!("########################   clear configs and confentries   #####################");
    con1.query("delete from configentries;", &[]).await?;
    con1.query("delete from configs;", &[]).await?;
  }
  let mut c1 = 0 as i64;
  let mut maxRowid = 0 as i64;
  let mut run = true;
  while run {
    let rows = con1.query("select rowid, facility, name from channels where rowid > $1 order by rowid limit 4", &[&maxRowid]).await?;
    if rows.len() <= 0 {
      break;
    }
    for i1 in 0..rows.len() {
      let row = &rows[i1];
      let channel = row.get(0);
      let channelName = row.get(2);
      let currentConfig = getNodesCurrentChannelConfig(&node, channel, &con1).await?;
      checkConfig(&node, basedir, channel, channelName, &currentConfig, &mut buf1, &con1).await?;
      tokio::time::delay_for(std::time::Duration::from_millis(5)).await;
      maxRowid = channel;
      c1 += 1;
      if c1 % 200 == 0 {
        trace!("scan config loop channel {}", c1);
      }
      if c1 >= 10 {
        warn!("stop before end of input");
        run = false;
        break;
      }
    }
  }
  Ok(())
}
