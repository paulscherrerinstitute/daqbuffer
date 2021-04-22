use crate::agg::{IntoBinnedT, MinMaxAvgScalarBinBatch, MinMaxAvgScalarEventBatch};
use crate::merge::MergedMinMaxAvgScalarStream;
use crate::raw::EventsQuery;
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, FutureExt, StreamExt, TryStreamExt};
use netpod::{
    AggKind, BinSpecDimT, Channel, Cluster, NanoRange, NodeConfig, PreBinnedPatchCoord, PreBinnedPatchIterator,
    PreBinnedPatchRange, ToNanos,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tiny_keccak::Hasher;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Query {
    range: NanoRange,
    count: u64,
    agg_kind: AggKind,
    channel: Channel,
}

impl Query {
    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let beg_date = params.get("beg_date").ok_or(Error::with_msg("missing beg_date"))?;
        let end_date = params.get("end_date").ok_or(Error::with_msg("missing end_date"))?;
        let ret = Query {
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            count: params
                .get("bin_count")
                .ok_or(Error::with_msg("missing beg_date"))?
                .parse()
                .unwrap(),
            agg_kind: AggKind::DimXBins1,
            channel: Channel {
                backend: params.get("channel_backend").unwrap().into(),
                name: params.get("channel_name").unwrap().into(),
            },
        };
        info!("Query::from_request  {:?}", ret);
        Ok(ret)
    }
}

pub async fn binned_bytes_for_http(
    node_config: Arc<NodeConfig>,
    query: &Query,
) -> Result<BinnedBytesForHttpStream, Error> {
    let agg_kind = AggKind::DimXBins1;

    let channel_config = super::channelconfig::read_local_config(&query.channel, node_config.clone()).await?;
    let entry;
    {
        let mut ixs = vec![];
        for i1 in 0..channel_config.entries.len() {
            let e1 = &channel_config.entries[i1];
            if i1 + 1 < channel_config.entries.len() {
                let e2 = &channel_config.entries[i1 + 1];
                if e1.ts < query.range.end && e2.ts >= query.range.beg {
                    ixs.push(i1);
                }
            } else {
                if e1.ts < query.range.end {
                    ixs.push(i1);
                }
            }
        }
        if ixs.len() == 0 {
            return Err(Error::with_msg(format!("no config entries found")));
        } else if ixs.len() > 1 {
            return Err(Error::with_msg(format!("too many config entries found: {}", ixs.len())));
        }
        entry = &channel_config.entries[ixs[0]];
    }

    info!("found config entry {:?}", entry);

    let grid = PreBinnedPatchRange::covering_range(query.range.clone(), query.count);
    match grid {
        Some(spec) => {
            info!("GOT  PreBinnedPatchGridSpec:    {:?}", spec);
            warn!("Pass here to BinnedStream what kind of Agg, range, ...");
            let s1 = BinnedStream::new(
                PreBinnedPatchIterator::from_range(spec),
                query.channel.clone(),
                agg_kind,
                node_config.clone(),
            );
            let ret = BinnedBytesForHttpStream::new(s1);
            Ok(ret)
        }
        None => {
            // Merge raw data
            error!("binned_bytes_for_http  TODO merge raw data");
            todo!()
        }
    }
}

pub struct BinnedBytesForHttpStream {
    inp: BinnedStream,
}

impl BinnedBytesForHttpStream {
    pub fn new(inp: BinnedStream) -> Self {
        Self { inp }
    }
}

impl Stream for BinnedBytesForHttpStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(_k))) => {
                error!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!          BinnedBytesForHttpStream  TODO  serialize to bytes");
                let mut buf = BytesMut::with_capacity(250);
                buf.put(&b"BinnedBytesForHttpStream  TODO  serialize to bytes\n"[..]);
                Ready(Some(Ok(buf.freeze())))
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PreBinnedQuery {
    pub patch: PreBinnedPatchCoord,
    agg_kind: AggKind,
    channel: Channel,
}

impl PreBinnedQuery {
    pub fn from_request(req: &http::request::Parts) -> Result<Self, Error> {
        let params = netpod::query_params(req.uri.query());
        let ret = PreBinnedQuery {
            patch: PreBinnedPatchCoord::from_query_params(&params),
            agg_kind: AggKind::DimXBins1,
            channel: Channel {
                backend: params.get("channel_backend").unwrap().into(),
                name: params.get("channel_name").unwrap().into(),
            },
        };
        info!("PreBinnedQuery::from_request  {:?}", ret);
        Ok(ret)
    }
}

// NOTE  This answers a request for a single valid pre-binned patch.
// A user must first make sure that the grid spec is valid, and that this node is responsible for it.
// Otherwise it is an error.
pub fn pre_binned_bytes_for_http(
    node_config: Arc<NodeConfig>,
    query: &PreBinnedQuery,
) -> Result<PreBinnedValueByteStream, Error> {
    info!("pre_binned_bytes_for_http  {:?}  {:?}", query, node_config.node);
    let ret = PreBinnedValueByteStream::new(
        query.patch.clone(),
        query.channel.clone(),
        query.agg_kind.clone(),
        node_config,
    );
    Ok(ret)
}

pub struct PreBinnedValueByteStream {
    inp: PreBinnedValueStream,
}

impl PreBinnedValueByteStream {
    pub fn new(patch: PreBinnedPatchCoord, channel: Channel, agg_kind: AggKind, node_config: Arc<NodeConfig>) -> Self {
        warn!("PreBinnedValueByteStream");
        Self {
            inp: PreBinnedValueStream::new(patch, channel, agg_kind, node_config),
        }
    }
}

impl Stream for PreBinnedValueByteStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(_k))) => {
                error!("TODO convert item to Bytes");
                let buf = Bytes::new();
                Ready(Some(Ok(buf)))
            }
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct PreBinnedValueStream {
    patch_coord: PreBinnedPatchCoord,
    channel: Channel,
    agg_kind: AggKind,
    node_config: Arc<NodeConfig>,
    open_check_local_file: Option<Pin<Box<dyn Future<Output = Result<tokio::fs::File, std::io::Error>> + Send>>>,
    fut2: Option<Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatch, Error>> + Send>>>,
}

impl PreBinnedValueStream {
    pub fn new(
        patch_coord: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        node_config: Arc<NodeConfig>,
    ) -> Self {
        let node_ix = node_ix_for_patch(&patch_coord, &channel, &node_config.cluster);
        assert!(node_ix == node_config.node.id);
        Self {
            patch_coord,
            channel,
            agg_kind,
            node_config,
            open_check_local_file: None,
            fut2: None,
        }
    }

    fn try_setup_fetch_prebinned_higher_res(&mut self) {
        info!("try to find a next better granularity for {:?}", self.patch_coord);
        let g = self.patch_coord.bin_t_len();
        let range = NanoRange {
            beg: self.patch_coord.patch_beg(),
            end: self.patch_coord.patch_end(),
        };
        match PreBinnedPatchRange::covering_range(range, self.patch_coord.bin_count() + 1) {
            Some(range) => {
                let h = range.grid_spec.bin_t_len();
                info!(
                    "FOUND NEXT GRAN  g {}  h {}  ratio {}  mod {}  {:?}",
                    g,
                    h,
                    g / h,
                    g % h,
                    range
                );
                assert!(g / h > 1);
                assert!(g / h < 20);
                assert!(g % h == 0);
                let bin_size = range.grid_spec.bin_t_len();
                let channel = self.channel.clone();
                let agg_kind = self.agg_kind.clone();
                let node_config = self.node_config.clone();
                let patch_it = PreBinnedPatchIterator::from_range(range);
                let s = futures_util::stream::iter(patch_it)
                    .map(move |coord| {
                        PreBinnedValueFetchedStream::new(coord, channel.clone(), agg_kind.clone(), node_config.clone())
                    })
                    .flatten()
                    .map(move |k| {
                        error!("NOTE NOTE NOTE  try_setup_fetch_prebinned_higher_res   ITEM  from sub res bin_size {}   {:?}", bin_size, k);
                        k
                    });
                self.fut2 = Some(Box::pin(s));
            }
            None => {
                warn!("no better resolution found for  g {}", g);
                let evq = EventsQuery {
                    channel: self.channel.clone(),
                    range: NanoRange {
                        beg: self.patch_coord.patch_beg(),
                        end: self.patch_coord.patch_end(),
                    },
                    agg_kind: self.agg_kind.clone(),
                };
                assert!(self.patch_coord.patch_t_len() % self.patch_coord.bin_t_len() == 0);
                let count = self.patch_coord.patch_t_len() / self.patch_coord.bin_t_len();
                let spec = BinSpecDimT {
                    bs: self.patch_coord.bin_t_len(),
                    ts1: self.patch_coord.patch_beg(),
                    ts2: self.patch_coord.patch_end(),
                    count,
                };
                let evq = Arc::new(evq);
                error!("try_setup_fetch_prebinned_higher_res  apply all requested transformations and T-binning");
                let s1 = MergedFromRemotes::new(evq, self.node_config.cluster.clone());
                let s2 = s1
                    .map(|k| {
                        trace!("MergedFromRemotes  emitted some item");
                        k
                    })
                    .into_binned_t(spec)
                    .map_ok({
                        let mut a = MinMaxAvgScalarBinBatch::empty();
                        move |k| {
                            error!("try_setup_fetch_prebinned_higher_res  TODO  emit actual value");
                            a.push_single(&k);
                            if a.len() > 0 {
                                let z = std::mem::replace(&mut a, MinMaxAvgScalarBinBatch::empty());
                                Some(z)
                            } else {
                                None
                            }
                        }
                    })
                    .filter(|k| {
                        use std::future::ready;
                        ready(k.is_ok() && k.as_ref().unwrap().is_some())
                    })
                    .map_ok(Option::unwrap);
                self.fut2 = Some(Box::pin(s2));
            }
        }
    }
}

impl Stream for PreBinnedValueStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if let Some(fut) = self.fut2.as_mut() {
                info!("PreBinnedValueStream  ---------------------------------------------------------  fut2 poll");
                fut.poll_next_unpin(cx)
            } else if let Some(fut) = self.open_check_local_file.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(Ok(_file)) => err::todoval(),
                    Ready(Err(e)) => match e.kind() {
                        std::io::ErrorKind::NotFound => {
                            error!("TODO LOCAL CACHE FILE NOT FOUND");
                            self.try_setup_fetch_prebinned_higher_res();
                            continue 'outer;
                        }
                        _ => {
                            error!("File I/O error: {:?}", e);
                            Ready(Some(Err(e.into())))
                        }
                    },
                    Pending => Pending,
                }
            } else {
                #[allow(unused_imports)]
                use std::os::unix::fs::OpenOptionsExt;
                let mut opts = std::fs::OpenOptions::new();
                opts.read(true);
                let fut = async { tokio::fs::OpenOptions::from(opts).open("/DOESNOTEXIST").await };
                self.open_check_local_file = Some(Box::pin(fut));
                continue 'outer;
            };
        }
    }
}

pub struct PreBinnedValueFetchedStream {
    uri: http::Uri,
    resfut: Option<hyper::client::ResponseFuture>,
    res: Option<hyper::Response<hyper::Body>>,
}

impl PreBinnedValueFetchedStream {
    pub fn new(
        patch_coord: PreBinnedPatchCoord,
        channel: Channel,
        agg_kind: AggKind,
        node_config: Arc<NodeConfig>,
    ) -> Self {
        let nodeix = node_ix_for_patch(&patch_coord, &channel, &node_config.cluster);
        let node = &node_config.cluster.nodes[nodeix as usize];
        warn!("TODO defining property of a PreBinnedPatchCoord? patchlen + ix? binsize + patchix? binsize + patchsize + patchix?");
        // TODO encapsulate uri creation, how to express aggregation kind?
        let uri: hyper::Uri = format!(
            "http://{}:{}/api/1/prebinned?{}&channel_backend={}&channel_name={}&agg_kind={:?}",
            node.host,
            node.port,
            patch_coord.to_url_params_strings(),
            channel.backend,
            channel.name,
            agg_kind,
        )
        .parse()
        .unwrap();
        Self {
            uri,
            resfut: None,
            res: None,
        }
    }
}

impl Stream for PreBinnedValueFetchedStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if let Some(res) = self.res.as_mut() {
                pin_mut!(res);
                use hyper::body::HttpBody;
                match res.poll_data(cx) {
                    Ready(Some(Ok(_))) => {
                        error!("TODO   PreBinnedValueFetchedStream   received value, now do something");
                        Pending
                    }
                    Ready(Some(Err(e))) => Ready(Some(Err(e.into()))),
                    Ready(None) => Ready(None),
                    Pending => Pending,
                }
            } else if let Some(resfut) = self.resfut.as_mut() {
                match resfut.poll_unpin(cx) {
                    Ready(res) => match res {
                        Ok(res) => {
                            info!("GOT result from SUB REQUEST: {:?}", res);
                            self.res = Some(res);
                            continue 'outer;
                        }
                        Err(e) => {
                            error!("PreBinnedValueStream  error in stream {:?}", e);
                            Ready(Some(Err(e.into())))
                        }
                    },
                    Pending => Pending,
                }
            } else {
                let req = hyper::Request::builder()
                    .method(http::Method::GET)
                    .uri(&self.uri)
                    .body(hyper::Body::empty())?;
                let client = hyper::Client::new();
                info!("START REQUEST FOR {:?}", req);
                self.resfut = Some(client.request(req));
                continue 'outer;
            };
        }
    }
}

type T001 = Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarEventBatch, Error>> + Send>>;
type T002 = Pin<Box<dyn Future<Output = Result<T001, Error>> + Send>>;
pub struct MergedFromRemotes {
    tcp_establish_futs: Vec<T002>,
    nodein: Vec<Option<T001>>,
    merged: Option<T001>,
}

impl MergedFromRemotes {
    pub fn new(evq: Arc<EventsQuery>, cluster: Arc<Cluster>) -> Self {
        let mut tcp_establish_futs = vec![];
        for node in &cluster.nodes {
            let f = super::raw::x_processed_stream_from_node(evq.clone(), node.clone());
            let f: T002 = Box::pin(f);
            tcp_establish_futs.push(f);
        }
        let n = tcp_establish_futs.len();
        Self {
            tcp_establish_futs,
            nodein: (0..n).into_iter().map(|_| None).collect(),
            merged: None,
        }
    }
}

impl Stream for MergedFromRemotes {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<MinMaxAvgScalarEventBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        trace!("MergedFromRemotes   MAIN POLL");
        use Poll::*;
        'outer: loop {
            break if let Some(fut) = &mut self.merged {
                debug!(
                    "MergedFromRemotes  »»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»»   MergedFromRemotes   POLL merged"
                );
                match fut.poll_next_unpin(cx) {
                    Ready(Some(Ok(k))) => {
                        info!("MergedFromRemotes  »»»»»»»»»»»»»»  Ready Some Ok");
                        Ready(Some(Ok(k)))
                    }
                    Ready(Some(Err(e))) => {
                        info!("MergedFromRemotes  »»»»»»»»»»»»»»  Ready Some Err");
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        info!("MergedFromRemotes  »»»»»»»»»»»»»»  Ready None");
                        Ready(None)
                    }
                    Pending => {
                        info!("MergedFromRemotes  »»»»»»»»»»»»»»  Pending");
                        Pending
                    }
                }
            } else {
                trace!("MergedFromRemotes   PHASE SETUP");
                let mut pend = false;
                let mut c1 = 0;
                for i1 in 0..self.tcp_establish_futs.len() {
                    if self.nodein[i1].is_none() {
                        let f = &mut self.tcp_establish_futs[i1];
                        pin_mut!(f);
                        info!("MergedFromRemotes  tcp_establish_futs  POLLING INPUT ESTAB  {}", i1);
                        match f.poll(cx) {
                            Ready(Ok(k)) => {
                                info!("MergedFromRemotes  tcp_establish_futs  ESTABLISHED INPUT {}", i1);
                                self.nodein[i1] = Some(k);
                            }
                            Ready(Err(e)) => return Ready(Some(Err(e))),
                            Pending => {
                                pend = true;
                            }
                        }
                    } else {
                        c1 += 1;
                    }
                }
                if pend {
                    Pending
                } else {
                    if c1 == self.tcp_establish_futs.len() {
                        debug!("MergedFromRemotes  SETTING UP MERGED STREAM");
                        let inps = self.nodein.iter_mut().map(|k| k.take().unwrap()).collect();
                        let s1 = MergedMinMaxAvgScalarStream::new(inps);
                        self.merged = Some(Box::pin(s1));
                    } else {
                        info!(
                            "MergedFromRemotes  conn / estab  {}  {}",
                            c1,
                            self.tcp_establish_futs.len()
                        );
                    }
                    continue 'outer;
                }
            };
        }
    }
}

pub struct BinnedStream {
    inp: Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatch, Error>> + Send>>,
}

impl BinnedStream {
    pub fn new(
        patch_it: PreBinnedPatchIterator,
        channel: Channel,
        agg_kind: AggKind,
        node_config: Arc<NodeConfig>,
    ) -> Self {
        warn!("BinnedStream will open a PreBinnedValueStream");
        let inp = futures_util::stream::iter(patch_it)
            .map(move |coord| {
                PreBinnedValueFetchedStream::new(coord, channel.clone(), agg_kind.clone(), node_config.clone())
            })
            .flatten()
            .map(|k| {
                match k {
                    Ok(ref k) => {
                        info!("BinnedStream  got good item {:?}", k);
                    }
                    Err(_) => {
                        error!("BinnedStream  got error")
                    }
                }
                k
            });
        Self { inp: Box::pin(inp) }
    }
}

impl Stream for BinnedStream {
    // TODO make this generic over all possible things
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => Ready(Some(Ok(k))),
            Ready(Some(Err(e))) => Ready(Some(Err(e))),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct SomeReturnThing {}

impl From<SomeReturnThing> for Bytes {
    fn from(_k: SomeReturnThing) -> Self {
        error!("TODO convert result to octets");
        todo!("TODO convert result to octets")
    }
}

pub fn node_ix_for_patch(patch_coord: &PreBinnedPatchCoord, channel: &Channel, cluster: &Cluster) -> u32 {
    let mut hash = tiny_keccak::Sha3::v256();
    hash.update(channel.backend.as_bytes());
    hash.update(channel.name.as_bytes());
    hash.update(&patch_coord.patch_beg().to_le_bytes());
    hash.update(&patch_coord.patch_end().to_le_bytes());
    hash.update(&patch_coord.bin_t_len().to_le_bytes());
    let mut out = [0; 32];
    hash.finalize(&mut out);
    let a = [out[0], out[1], out[2], out[3]];
    let ix = u32::from_le_bytes(a) % cluster.nodes.len() as u32;
    info!("node_ix_for_patch  {}", ix);
    ix
}
