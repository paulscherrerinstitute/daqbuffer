#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
#[allow(unused_imports)]
use snafu::{ResultExt, ensure};
use crate::dbcon::DbInfo;
use crate::node::{Node};
use crate::errors::*;

// TODO misnomer: this represents a specific member of the cluster of that facility/backend.
pub struct Facility {
  pub hostname: String,
  pub node: Node,
  pub dbinfo: DbInfo,
  pub facility: String,
  pub base_dir: String,
  pub ks_prefix: String,
}

impl Facility {
  pub async fn from_current_hostname() -> Result<Self, Error> {
    let hostname = crate::node::get_hostname();
    if ["hipa-daq-01.psi.ch", "hipa-data-api.psi.ch"].contains(&hostname.as_str()) {
      let facility = "hipa-archive";
      let dbinfo = DbInfo::hipa_archive();
      let node = crate::node::get_node(facility, &dbinfo).await?;
      Ok(Facility {
        hostname,
        node,
        dbinfo,
        facility: facility.into(),
        base_dir: "/hipa/daq/hipa-archive/daq_local".into(),
        ks_prefix: "daq_local".into(),
      })
    }
    else if ["gls-01.psi.ch", "gls-data-api.psi.ch"].contains(&hostname.as_ref()) {
      let facility = "gls-archive";
      let dbinfo = DbInfo::gls_archive();
      let node = crate::node::get_node(facility, &dbinfo).await?;
      Ok(Facility {
        hostname,
        node,
        dbinfo,
        facility: facility.into(),
        base_dir: "/gls_data/gls-archive/daq_local".into(),
        ks_prefix: "daq_local".into(),
      })
    }
    else if ["sf-daq-5.psi.ch", "sf-daq-6.psi.ch"].contains(&hostname.as_ref()) {
      let facility = "sf-imagebuffer";
      let dbinfo = DbInfo::sf_imagebuffer();
      let node = crate::node::get_node(facility, &dbinfo).await?;
      Ok(Facility {
        hostname,
        node,
        dbinfo,
        facility: facility.into(),
        base_dir: "/gpfs/sf-data/sf-imagebuffer/daq_swissfel".into(),
        ks_prefix: "daq_swissfel".into(),
      })
    }
    else if [
      "sf-daqbuf-21.psi.ch",
      "sf-daqbuf-22.psi.ch",
      "sf-daqbuf-23.psi.ch",
      "sf-daqbuf-24.psi.ch",
      "sf-daqbuf-25.psi.ch",
      "sf-daqbuf-26.psi.ch",
      "sf-daqbuf-27.psi.ch",
      "sf-daqbuf-28.psi.ch",
      "sf-daqbuf-29.psi.ch",
      "sf-daqbuf-30.psi.ch",
      "sf-daqbuf-31.psi.ch",
      "sf-daqbuf-32.psi.ch",
      "sf-daqbuf-33.psi.ch",
    ].contains(&hostname.as_ref()) {
      let facility = "sf-databuffer";
      let dbinfo = DbInfo::sf_databuffer();
      let node = crate::node::get_node(facility, &dbinfo).await?;
      Ok(Facility {
        hostname,
        node,
        dbinfo,
        facility: facility.into(),
        base_dir: "/data/sf-databuffer/daq_swissfel".into(),
        ks_prefix: "daq_swissfel".into(),
      })
    }
    else {
      error!("no fixed conf for host {:?}", hostname);
      ensure!(false, Bad { msg: format!("bad host: {}", hostname) });
      panic!();
    }
  }
}
