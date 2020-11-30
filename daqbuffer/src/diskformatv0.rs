#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
#[allow(unused_imports)]
use snafu::{ResultExt, ensure};
use std::path::{Path, PathBuf};
use std::os::unix::ffi::OsStringExt;
use crate::errors::*;
use std::future::Future;
use std::pin::Pin;
use serde_derive::Serialize;
use chrono::DateTime;
use chrono::Utc;

pub async fn find_channel_names_from_config<F, Fut>(
  base_dir: impl AsRef<Path>,
  mut cb: F,
) -> Result<(), Error>
where
  F: FnMut(&str) -> Fut,
  Fut: Future<Output=Result<(), Error>>,
{
  let mut path2: PathBuf = base_dir.as_ref().into();
  path2.push("config");
  let mut rd = tokio::fs::read_dir(&path2).await.context(OpenPath{path:path2})?;
  while let Ok(Some(entry)) = rd.next_entry().await {
    let fname = String::from_utf8(entry.file_name().into_vec())?;
    cb(&fname).await?;
  }
  Ok(())
}

#[derive(Debug, Serialize)]
pub struct ChannelDesc {
  name: String,
}

#[derive(Debug, Serialize)]
pub struct ChannelDatafileDesc {
  channel: ChannelDesc,
  ks: u32,
  tb: u32,
  sp: u32,
  bs: u32,
  fs: u64,
  mt: DateTime<Utc>,
  ix_fs: Option<u64>,
  ix_mt: Option<DateTime<Utc>>,
}

impl ChannelDatafileDesc {
  pub fn timebin(&self) -> u32 { self.tb }
  pub fn binsize(&self) -> u32 { self.bs }
  pub fn keyspace(&self) -> u32 { self.ks }
  pub fn split(&self) -> u32 { self.sp }
}

pub trait ChannelDatafileDescSink {
  fn sink(&self, k: ChannelDatafileDesc) -> Pin<Box<dyn Future<Output=Result<(), Error>>>>;
}

pub async fn find_channel_datafiles_in_ks(
  base_dir: impl AsRef<Path>,
  ks_prefix: &str,
  ks: u32,
  channel: &str,
  cb: &dyn ChannelDatafileDescSink,
) -> Result<(), Error>
{
  let mut path2: PathBuf = base_dir.as_ref().into();
  path2.push(format!("{}_{}", ks_prefix, ks));
  path2.push("byTime");
  path2.push(channel);
  let re1 = regex::Regex::new(r"^\d{19}$")?;
  let re2 = regex::Regex::new(r"^\d{10}$")?;
  let re4 = regex::Regex::new(r"^(\d{19})_0{5}_Data$")?;
  let mut rd = tokio::fs::read_dir(&path2).await.context(ChannelDatadirNotFound{path:&path2})?;
  while let Ok(Some(entry)) = rd.next_entry().await {
    let fname = String::from_utf8(entry.file_name().into_vec())?;
    if !re1.is_match(&fname) {
      warn!("unexpected file  {}", fname);
      continue;
    }
    let timebin: u32 = fname.parse()?;
    let mut path = path2.clone();
    path.push(fname);
    let mut rd = tokio::fs::read_dir(&path).await.context(OpenPath{path:&path})?;
    while let Ok(Some(entry)) = rd.next_entry().await {
      let fname = String::from_utf8(entry.file_name().into_vec())?;
      if !re2.is_match(&fname) {
        warn!("unexpected file  {}", fname);
        continue;
      }
      let split: u32 = fname.parse()?;
      let mut path = path.clone();
      path.push(fname);
      let mut rd = tokio::fs::read_dir(&path).await.context(OpenPath{path:&path})?;
      while let Ok(Some(entry)) = rd.next_entry().await {
        let fname = String::from_utf8(entry.file_name().into_vec())?;
        if let Some(m) = re4.captures(&fname) {
          let binsize: u32 = m.get(1).unwrap().as_str().parse()?;
          path.push(&fname);
          let meta = tokio::fs::metadata(&path).await?;
          path.pop();
          path.push(format!("{}_Index", fname));
          let (ix_size, ix_tmod) = if let Ok(meta) = tokio::fs::metadata(&path).await {
            (Some(meta.len()), Some(meta.modified().unwrap().into()))
          }
          else { (None, None) };
          path.pop();
          cb.sink(ChannelDatafileDesc {
            channel: ChannelDesc {
              name: channel.into(),
            },
            ks: ks,
            tb: timebin,
            sp: split,
            bs: binsize,
            fs: meta.len(),
            ix_fs: ix_size,
            ix_mt: ix_tmod,
            mt: meta.modified().unwrap().into(),
          }).await?;
        }
      }
    }
  }
  Ok(())
}
