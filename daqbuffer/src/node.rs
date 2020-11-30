#![allow(non_snake_case)]
#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use snafu::ensure;
use crate::errors::*;
use crate::SE;
use crate::dbcon::connect_db;
use serde_derive::Serialize;

#[derive(Debug, Serialize)]
pub struct Node {
  pub rowid: i64,
  pub facility: i64,
  pub split: i32,
  pub hostname: String,
}

impl Node {
  pub fn rowid(&self) -> i64 { self.rowid }
  pub fn facility(&self) -> i64 { self.facility }
}

pub fn get_hostname() -> String {
  let out = std::process::Command::new("hostname").output().expect("FATAL can not query our hostname");
  String::from_utf8(out.stdout[..out.stdout.len()-1].to_vec()).expect("FATAL can not obtain our hostname")
}

pub async fn get_node(facility: &str, dbinfo: &crate::dbcon::DbInfo) -> Result<Node, Error> {
  let hostname = get_hostname();
  let con1 = connect_db(dbinfo).await.ctxb(SE!(AddPos))?;
  let rows = con1.query("select nodes.rowid, facility, split, hostname from nodes, facilities where facilities.name = $1 and facility = facilities.rowid and hostname = $2", &[&facility, &hostname]).await.context(SE!(DbError))?;
  ensure!(rows.len() > 0, Bad { msg: format!("could not find hostname {}  facility {}", hostname, facility) });
  let row = &rows[0];
  Ok(Node {
    rowid: row.get(0),
    facility: row.get(1),
    split: row.get(2),
    hostname: row.get(3),
  })
}
