use crate::spawn_test_hosts;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use disk::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use disk::agg::streams::{Bins, StatsItem, StreamItem};
use disk::binned::RangeCompletableItem;
use disk::cache::BinnedQuery;
use disk::frame::inmem::InMemoryFrameAsyncReadStream;
use disk::streamlog::Streamlog;
use err::Error;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::{AggKind, Channel, Cluster, Database, HostPort, NanoRange, Node, PerfOpts};
use std::future::ready;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncRead;
use tokio::task::JoinHandle;

pub mod json;

struct RunningHosts {
    cluster: Cluster,
    _jhs: Vec<JoinHandle<Result<(), Error>>>,
}

lazy_static::lazy_static! {
    static ref HOSTS_RUNNING: Mutex<Option<Arc<RunningHosts>>> = Mutex::new(None);
}

fn require_test_hosts_running() -> Result<Arc<RunningHosts>, Error> {
    let mut g = HOSTS_RUNNING.lock().unwrap();
    match g.as_ref() {
        None => {
            let cluster = test_cluster();
            let jhs = spawn_test_hosts(cluster.clone());
            let ret = RunningHosts {
                cluster: cluster.clone(),
                _jhs: jhs,
            };
            let a = Arc::new(ret);
            *g = Some(a.clone());
            Ok(a)
        }
        Some(gg) => Ok(gg.clone()),
    }
}

fn test_cluster() -> Cluster {
    let nodes = (0..3)
        .into_iter()
        .map(|id| Node {
            host: "localhost".into(),
            listen: "0.0.0.0".into(),
            port: 8360 + id as u16,
            port_raw: 8360 + id as u16 + 100,
            data_base_path: format!("../tmpdata/node{:02}", id).into(),
            ksprefix: "ks".into(),
            split: id,
            backend: "testbackend".into(),
        })
        .collect();
    Cluster {
        nodes: nodes,
        database: Database {
            name: "daqbuffer".into(),
            host: "localhost".into(),
            user: "daqbuffer".into(),
            pass: "daqbuffer".into(),
        },
    }
}

#[test]
fn get_binned_binary() {
    taskrun::run(get_binned_binary_inner()).unwrap();
}

async fn get_binned_binary_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    if true {
        get_binned_channel(
            "wave-f64-be-n21",
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T00:20:30.000Z",
            2,
            cluster,
            true,
            2,
        )
        .await?;
    }
    if true {
        get_binned_channel(
            "wave-u16-le-n77",
            "1970-01-01T01:11:00.000Z",
            "1970-01-01T01:35:00.000Z",
            7,
            cluster,
            true,
            24,
        )
        .await?;
    }
    if true {
        get_binned_channel(
            "wave-u16-le-n77",
            "1970-01-01T01:42:00.000Z",
            "1970-01-01T03:55:00.000Z",
            2,
            cluster,
            true,
            3,
        )
        .await?;
    }
    Ok(())
}

async fn get_binned_channel<S>(
    channel_name: &str,
    beg_date: S,
    end_date: S,
    bin_count: u32,
    cluster: &Cluster,
    expect_range_complete: bool,
    expect_bin_count: u64,
) -> Result<BinnedResponse, Error>
where
    S: AsRef<str>,
{
    let t1 = Utc::now();
    let agg_kind = AggKind::DimXBins1;
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.as_ref().parse()?;
    let end_date: DateTime<Utc> = end_date.as_ref().parse()?;
    let channel_backend = "testbackend";
    let perf_opts = PerfOpts { inmem_bufcap: 512 };
    let channel = Channel {
        backend: channel_backend.into(),
        name: channel_name.into(),
    };
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = BinnedQuery::new(channel, range, bin_count, agg_kind);
    let hp = HostPort::from_node(node0);
    let url = query.url(&hp);
    info!("get_binned_channel  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url)
        .header("accept", "application/octet-stream")
        .body(Body::empty())?;
    let client = hyper::Client::new();
    let res = client.request(req).await?;
    if res.status() != StatusCode::OK {
        error!("client response {:?}", res);
    }
    let s1 = disk::cache::HttpBodyAsAsyncRead::new(res);
    let s2 = InMemoryFrameAsyncReadStream::new(s1, perf_opts.inmem_bufcap);
    let res = consume_binned_response(s2).await?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    info!("get_cached_0  DONE  bin_count {}  time {} ms", res.bin_count, ms);
    if !res.is_valid() {
        Err(Error::with_msg(format!("invalid response: {:?}", res)))
    } else if res.range_complete_count == 0 && expect_range_complete {
        Err(Error::with_msg(format!("expect range complete: {:?}", res)))
    } else if res.bin_count != expect_bin_count {
        Err(Error::with_msg(format!("bin count mismatch: {:?}", res)))
    } else {
        Ok(res)
    }
}

#[derive(Debug)]
pub struct BinnedResponse {
    bin_count: u64,
    err_item_count: u64,
    data_item_count: u64,
    bytes_read: u64,
    range_complete_count: u64,
    log_item_count: u64,
    stats_item_count: u64,
}

impl BinnedResponse {
    pub fn new() -> Self {
        Self {
            bin_count: 0,
            err_item_count: 0,
            data_item_count: 0,
            bytes_read: 0,
            range_complete_count: 0,
            log_item_count: 0,
            stats_item_count: 0,
        }
    }

    pub fn is_valid(&self) -> bool {
        if self.range_complete_count > 1 {
            false
        } else {
            true
        }
    }
}

async fn consume_binned_response<T>(inp: InMemoryFrameAsyncReadStream<T>) -> Result<BinnedResponse, Error>
where
    T: AsyncRead + Unpin,
{
    let s1 = inp
        .map_err(|e| error!("TEST GOT ERROR {:?}", e))
        .filter_map(|item| {
            let g = match item {
                Ok(item) => match item {
                    StreamItem::Log(item) => {
                        Streamlog::emit(&item);
                        None
                    }
                    StreamItem::Stats(item) => {
                        info!("Stats: {:?}", item);
                        None
                    }
                    StreamItem::DataItem(frame) => {
                        type ExpectedType = Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarBinBatch>>, Error>;
                        match bincode::deserialize::<ExpectedType>(frame.buf()) {
                            Ok(item) => match item {
                                Ok(item) => match item {
                                    StreamItem::Log(item) => {
                                        Streamlog::emit(&item);
                                        Some(Ok(StreamItem::Log(item)))
                                    }
                                    item => {
                                        info!("TEST GOT ITEM {:?}", item);
                                        Some(Ok(item))
                                    }
                                },
                                Err(e) => {
                                    error!("TEST GOT ERROR FRAME: {:?}", e);
                                    Some(Err(e))
                                }
                            },
                            Err(e) => {
                                error!("bincode error: {:?}", e);
                                Some(Err(e.into()))
                            }
                        }
                    }
                },
                Err(e) => Some(Err(Error::with_msg(format!("WEIRD EMPTY ERROR {:?}", e)))),
            };
            ready(g)
        })
        .fold(BinnedResponse::new(), |mut a, k| {
            let g = match k {
                Ok(StreamItem::Log(_item)) => {
                    a.log_item_count += 1;
                    a
                }
                Ok(StreamItem::Stats(item)) => match item {
                    StatsItem::EventDataReadStats(item) => {
                        a.bytes_read += item.parsed_bytes;
                        a
                    }
                },
                Ok(StreamItem::DataItem(item)) => match item {
                    RangeCompletableItem::RangeComplete => {
                        a.range_complete_count += 1;
                        a
                    }
                    RangeCompletableItem::Data(item) => {
                        a.data_item_count += 1;
                        a.bin_count += item.bin_count() as u64;
                        a
                    }
                },
                Err(_e) => {
                    a.err_item_count += 1;
                    a
                }
            };
            ready(g)
        });
    let ret = s1.await;
    info!("BinnedResponse: {:?}", ret);
    Ok(ret)
}

#[test]
fn bufs() {
    use bytes::{Buf, BufMut};
    let mut buf = BytesMut::with_capacity(1024);
    assert!(buf.as_mut().len() == 0);
    buf.put_u32_le(123);
    assert!(buf.as_mut().len() == 4);
    let mut b2 = buf.split_to(4);
    assert!(b2.capacity() == 4);
    b2.advance(2);
    assert!(b2.capacity() == 2);
    b2.advance(2);
    assert!(b2.capacity() == 0);
    assert!(buf.capacity() == 1020);
    assert!(buf.remaining() == 0);
    assert!(buf.remaining_mut() >= 1020);
    assert!(buf.capacity() == 1020);
}