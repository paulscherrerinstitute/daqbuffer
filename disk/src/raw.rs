/*!
Delivers event data.

Delivers event data (not yet time-binned) from local storage and provides client functions
to request such data from nodes.
*/

use crate::agg::MinMaxAvgScalarEventBatch;
use bytes::{BufMut, Bytes, BytesMut};
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, StreamExt};
use netpod::{AggKind, Channel, NanoRange, Node, NodeConfig};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;
#[allow(unused_imports)]
use tracing::{debug, error, info, span, trace, warn, Level};

/**
Query parameters to request (optionally) X-processed, but not T-processed events.
*/
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventsQuery {
    pub channel: Channel,
    pub range: NanoRange,
    pub agg_kind: AggKind,
}

pub async fn x_processed_stream_from_node(
    query: Arc<EventsQuery>,
    node: Arc<Node>,
) -> Result<Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarEventBatch, Error>> + Send>>, Error> {
    let net = TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;
    let qjs = serde_json::to_vec(query.as_ref())?;
    let (netin, mut netout) = net.into_split();

    // TODO  this incorrect magic MUST bubble up into the final result and be reported.

    netout.write_u32_le(INMEM_FRAME_MAGIC - 1).await?;
    netout.write_u32_le(qjs.len() as u32).await?;
    netout.write_u32_le(0).await?;
    netout.write_all(&qjs).await?;
    netout.write_u32_le(INMEM_FRAME_MAGIC).await?;
    netout.write_u32_le(0).await?;
    netout.write_u32_le(0).await?;
    netout.flush().await?;
    netout.forget();
    debug!("x_processed_stream_from_node   WRITTEN");
    let frames = InMemoryFrameAsyncReadStream::new(netin);
    let s2 = MinMaxAvgScalarEventBatchStreamFromFrames::new(frames);
    debug!("x_processed_stream_from_node   HAVE STREAM INSTANCE");
    let s3: Pin<Box<dyn Stream<Item = Result<_, Error>> + Send>> = Box::pin(s2);
    debug!("x_processed_stream_from_node   RETURN");
    Ok(s3)
}

pub struct MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    inp: InMemoryFrameAsyncReadStream<T>,
}

impl<T> MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: InMemoryFrameAsyncReadStream<T>) -> Self {
        Self { inp }
    }
}

impl<T> Stream for MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<MinMaxAvgScalarEventBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let j = &mut self.inp;
            pin_mut!(j);
            break match j.poll_next(cx) {
                Ready(Some(Ok(buf))) => {
                    info!(
                        "MinMaxAvgScalarEventBatchStreamFromFrames  got full frame buf  {}",
                        buf.len()
                    );
                    //let item = MinMaxAvgScalarEventBatch::from_full_frame(&buf);
                    match bincode::deserialize::<RawConnOut>(buf.as_ref()) {
                        Ok(item) => match item {
                            Ok(item) => Ready(Some(Ok(item))),
                            Err(e) => Ready(Some(Err(e))),
                        },
                        Err(e) => Ready(Some(Err(e.into()))),
                    }
                }
                Ready(Some(Err(e))) => Ready(Some(Err(e))),
                Ready(None) => Ready(None),
                Pending => Pending,
            };
        }
    }
}

pub const INMEM_FRAME_HEAD: usize = 12;
pub const INMEM_FRAME_MAGIC: u32 = 0xc6c3b73d;

/**
Interprets a byte stream as length-delimited frames.

Emits each frame as a single item. Therefore, each item must fit easily into memory.
*/
pub struct InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    inp: T,
    buf: BytesMut,
    wp: usize,
    tryparse: bool,
    errored: bool,
    completed: bool,
    inp_bytes_consumed: u64,
}

impl<T> InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: T) -> Self {
        // TODO make start cap adjustable
        let mut buf = BytesMut::with_capacity(1024);
        buf.resize(buf.capacity(), 0);
        Self {
            inp,
            buf,
            wp: 0,
            tryparse: false,
            errored: false,
            completed: false,
            inp_bytes_consumed: 0,
        }
    }

    fn tryparse(&mut self) -> Option<Option<Result<Bytes, Error>>> {
        const HEAD: usize = INMEM_FRAME_HEAD;
        let mut buf = std::mem::replace(&mut self.buf, BytesMut::new());
        if self.wp >= HEAD {
            let magic = u32::from_le_bytes(*arrayref::array_ref![buf, 0, 4]);
            let len = u32::from_le_bytes(*arrayref::array_ref![buf, 4, 4]);
            let _tyid = u32::from_le_bytes(*arrayref::array_ref![buf, 8, 4]);
            if magic != INMEM_FRAME_MAGIC {
                error!("InMemoryFrameAsyncReadStream  tryparse  incorrect magic: {}", magic);
                return Some(Some(Err(Error::with_msg(format!(
                    "InMemoryFrameAsyncReadStream  tryparse  incorrect magic: {}",
                    magic
                )))));
            }
            if len == 0 {
                if self.wp != HEAD {
                    return Some(Some(Err(Error::with_msg(format!(
                        "InMemoryFrameAsyncReadStream  tryparse  unexpected amount left {}",
                        self.wp
                    )))));
                }
                self.buf = buf;
                Some(None)
            } else {
                if len > 1024 * 32 {
                    warn!("InMemoryFrameAsyncReadStream  big len received  {}", len);
                }
                if len > 1024 * 1024 * 2 {
                    error!("InMemoryFrameAsyncReadStream  too long len {}", len);
                    return Some(Some(Err(Error::with_msg(format!(
                        "InMemoryFrameAsyncReadStream  tryparse  hug buffer  len {}  self.inp_bytes_consumed {}",
                        len, self.inp_bytes_consumed
                    )))));
                }
                assert!(len > 0 && len < 1024 * 512);
                let nl = len as usize + HEAD;
                if buf.capacity() < nl {
                    buf.resize(nl, 0);
                } else {
                    // nothing to do
                }
                if self.wp >= nl {
                    let mut buf3 = BytesMut::with_capacity(buf.capacity());
                    // TODO make stats of copied bytes and warn if ratio is too bad.
                    buf3.put(buf[nl..self.wp].as_ref());
                    buf3.resize(buf3.capacity(), 0);
                    use bytes::Buf;
                    buf.truncate(nl);
                    buf.advance(HEAD);
                    self.wp = self.wp - nl;
                    self.buf = buf3;
                    self.inp_bytes_consumed += nl as u64;
                    Some(Some(Ok(buf.freeze())))
                } else {
                    self.buf = buf;
                    None
                }
            }
        } else {
            self.buf = buf;
            None
        }
    }
}

impl<T> Stream for InMemoryFrameAsyncReadStream<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        assert!(!self.completed);
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        'outer: loop {
            if self.tryparse {
                let r = self.tryparse();
                break match r {
                    None => {
                        self.tryparse = false;
                        continue 'outer;
                    }
                    Some(None) => {
                        self.tryparse = false;
                        self.completed = true;
                        Ready(None)
                    }
                    Some(Some(Ok(k))) => Ready(Some(Ok(k))),
                    Some(Some(Err(e))) => {
                        self.tryparse = false;
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                };
            } else {
                let mut buf0 = std::mem::replace(&mut self.buf, BytesMut::new());
                if buf0.as_mut().len() != buf0.capacity() {
                    error!("-------   {}  {}", buf0.as_mut().len(), buf0.capacity());
                    panic!();
                }
                let mut buf2 = ReadBuf::new(buf0.as_mut()[self.wp..].as_mut());
                assert!(buf2.filled().len() == 0);
                assert!(buf2.capacity() > 0);
                assert!(buf2.remaining() > 0);
                let j = &mut self.inp;
                pin_mut!(j);
                break match AsyncRead::poll_read(j, cx, &mut buf2) {
                    Ready(Ok(())) => {
                        let n1 = buf2.filled().len();
                        if n1 == 0 {
                            if self.wp != 0 {
                                error!(
                                    "InMemoryFrameAsyncReadStream  self.wp != 0  wp {}  consumed {}",
                                    self.wp, self.inp_bytes_consumed
                                );
                            }
                            self.buf = buf0;
                            self.completed = true;
                            Ready(None)
                        } else {
                            self.wp += n1;
                            self.buf = buf0;
                            self.tryparse = true;
                            continue 'outer;
                        }
                    }
                    Ready(Err(e)) => Ready(Some(Err(e.into()))),
                    Pending => {
                        self.buf = buf0;
                        Pending
                    }
                };
            }
        }
    }
}

// TODO build a stream from disk data to batched event data.
#[allow(dead_code)]
async fn local_unpacked_test() {
    let query = err::todoval();
    let node = err::todoval();
    // TODO open and parse the channel config.
    // TODO find the matching config entry. (bonus: fuse consecutive compatible entries)
    use crate::agg::IntoDim1F32Stream;
    let _stream = crate::EventBlobsComplete::new(&query, query.channel_config.clone(), node).into_dim_1_f32_stream();
}

/**
Can be serialized as a length-delimited frame.
*/
pub trait Frameable {
    fn serialized(&self) -> Bytes;
}

pub async fn raw_service(node_config: Arc<NodeConfig>) -> Result<(), Error> {
    let addr = format!("{}:{}", node_config.node.listen, node_config.node.port_raw);
    let lis = tokio::net::TcpListener::bind(addr).await?;
    loop {
        match lis.accept().await {
            Ok((stream, addr)) => {
                taskrun::spawn(raw_conn_handler(stream, addr));
            }
            Err(e) => Err(e)?,
        }
    }
}

async fn raw_conn_handler(stream: TcpStream, addr: SocketAddr) -> Result<(), Error> {
    //use tracing_futures::Instrument;
    let span1 = span!(Level::INFO, "raw::raw_conn_handler");
    raw_conn_handler_inner(stream, addr).instrument(span1).await
}

type RawConnOut = Result<MinMaxAvgScalarEventBatch, Error>;

async fn raw_conn_handler_inner(stream: TcpStream, addr: SocketAddr) -> Result<(), Error> {
    match raw_conn_handler_inner_try(stream, addr).await {
        Ok(_) => (),
        Err(mut ce) => {
            let ret: RawConnOut = Err(ce.err);
            let enc = bincode::serialize(&ret)?;
            // TODO optimize
            let mut buf = BytesMut::with_capacity(enc.len() + 32);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(0);
            buf.put(enc.as_ref());
            match ce.netout.write(&buf).await {
                Ok(_) => (),
                Err(e) => return Err(e)?,
            }
        }
    }
    Ok(())
}

struct ConnErr {
    err: Error,
    netout: OwnedWriteHalf,
}

impl From<(Error, OwnedWriteHalf)> for ConnErr {
    fn from((err, netout): (Error, OwnedWriteHalf)) -> Self {
        Self { err, netout }
    }
}

impl From<(std::io::Error, OwnedWriteHalf)> for ConnErr {
    fn from((err, netout): (std::io::Error, OwnedWriteHalf)) -> Self {
        Self {
            err: err.into(),
            netout,
        }
    }
}

async fn raw_conn_handler_inner_try(stream: TcpStream, addr: SocketAddr) -> Result<(), ConnErr> {
    info!("raw_conn_handler   SPAWNED   for {:?}", addr);
    let (netin, mut netout) = stream.into_split();
    let mut h = InMemoryFrameAsyncReadStream::new(netin);
    let mut frames = vec![];
    while let Some(k) = h
        .next()
        .instrument(span!(Level::INFO, "raw_conn_handler  INPUT STREAM READ"))
        .await
    {
        match k {
            Ok(_) => {
                info!(". . . . . . . . . . . . . . . . . . . . . . . . . .   raw_conn_handler  FRAME RECV");
                frames.push(k);
            }
            Err(e) => {
                return Err((e, netout))?;
            }
        }
    }
    if frames.len() != 1 {
        error!("expect a command frame");
        return Err((Error::with_msg("expect a command frame"), netout))?;
    }
    error!("TODO decide on response content based on the parsed json query");
    let mut batch = MinMaxAvgScalarEventBatch::empty();
    batch.tss.push(42);
    batch.tss.push(43);
    batch.mins.push(7.1);
    batch.mins.push(7.2);
    batch.maxs.push(8.3);
    batch.maxs.push(8.4);
    batch.avgs.push(9.5);
    batch.avgs.push(9.6);
    let mut s1 = futures_util::stream::iter(vec![batch]);
    while let Some(item) = s1.next().await {
        let fr = item.serialized();
        let mut buf = BytesMut::with_capacity(fr.len() + 32);
        buf.put_u32_le(INMEM_FRAME_MAGIC);
        buf.put_u32_le(fr.len() as u32);
        buf.put_u32_le(0);
        buf.put(fr.as_ref());
        match netout.write(&buf).await {
            Ok(_) => {}
            Err(e) => return Err((e, netout))?,
        }
    }
    let mut buf = BytesMut::with_capacity(32);
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(0);
    buf.put_u32_le(0);
    match netout.write(&buf).await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    match netout.flush().await {
        Ok(_) => (),
        Err(e) => return Err((e, netout))?,
    }
    Ok(())
}
