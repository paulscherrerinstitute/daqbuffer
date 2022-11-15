use crate::channelconfig::chconf_from_events_json;
use crate::err::Error;
use crate::response;
use bytes::Bytes;
use disk::channelexec::channel_exec;
use disk::channelexec::ChannelExecFunction;
use disk::decode::Endianness;
use disk::decode::EventValueFromBytes;
use disk::decode::EventValueShape;
use disk::decode::NumFromBytes;
use disk::merge::mergedfromremotes::MergedFromRemotes;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::TryStreamExt;
use http::{Method, StatusCode};
use hyper::{Body, Request, Response};
use items::numops::NumOps;
use items::streams::collect_plain_events_json;
use items::streams::Collectable;
use items::Clearable;
use items::EventsNodeProcessor;
use items::Framable;
use items::FrameType;
use items::PushableIndex;
use items::Sitemty;
use items::TimeBinnableType;
use netpod::log::*;
use netpod::query::{PlainEventsQuery, RawEventsQuery};
use netpod::{AggKind, Channel, FromUrl, NanoRange, NodeConfigCached, PerfOpts, ScalarType, Shape};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::pin::Pin;
use std::time::Duration;
use url::Url;

pub struct EventInfoScan {}

impl EventInfoScan {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with("/api/4/event/info") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        info!("EventInfoScan::handle");
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let (head, _body) = req.into_parts();
        let url = Url::parse(&format!("dummy:{}", head.uri))?;
        let query = PlainEventsQuery::from_url(&url)?;
        let ret = match Self::exec(&query, node_config).await {
            Ok(stream) => {
                //
                let stream = stream.map_ok(|_| Bytes::new());
                response(StatusCode::OK).body(Body::wrap_stream(stream))?
            }
            Err(e) => response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(format!("{:?}", e)))?,
        };
        Ok(ret)
    }

    pub async fn exec(
        query: &PlainEventsQuery,
        node_config: &NodeConfigCached,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>, Error> {
        let chconf = chconf_from_events_json(&query, node_config).await?;
        let ret = channel_exec(
            EvInfoFunc::new(
                query.clone(),
                query.timeout(),
                query.events_max().unwrap_or(u64::MAX),
                node_config.clone(),
            ),
            query.channel(),
            query.range(),
            chconf.scalar_type,
            chconf.shape,
            AggKind::Stats1,
            node_config,
        )
        .await?;
        Ok(Box::pin(ret.map_err(Error::from)))
    }
}

pub struct EvInfoFunc {
    query: PlainEventsQuery,
    timeout: Duration,
    node_config: NodeConfigCached,
    events_max: u64,
}

impl EvInfoFunc {
    pub fn new(query: PlainEventsQuery, timeout: Duration, events_max: u64, node_config: NodeConfigCached) -> Self {
        Self {
            query,
            timeout,
            events_max,
            node_config,
        }
    }

    pub fn channel(&self) -> &Channel {
        &self.query.channel()
    }

    pub fn range(&self) -> &NanoRange {
        &self.query.range()
    }
}

impl ChannelExecFunction for EvInfoFunc {
    type Output = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

    fn exec<NTY, END, EVS, ENP>(
        self,
        byte_order: END,
        _scalar_type: ScalarType,
        _shape: Shape,
        event_value_shape: EVS,
        _events_node_proc: ENP,
    ) -> Result<Self::Output, ::err::Error>
    where
        NTY: NumOps + NumFromBytes<NTY, END> + 'static,
        END: Endianness + 'static,
        EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END> + 'static,
        ENP: EventsNodeProcessor<Input = <EVS as EventValueFromBytes<NTY, END>>::Batch> + 'static,
        // TODO require these things in general?
        <ENP as EventsNodeProcessor>::Output: Debug + Collectable + PushableIndex + Clearable,
        <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output: Debug
            + TimeBinnableType<Output = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>
            + Collectable
            + Unpin,
        Sitemty<<ENP as EventsNodeProcessor>::Output>: FrameType + Framable + 'static,
        Sitemty<<<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output>:
            FrameType + Framable + DeserializeOwned,
    {
        let _ = byte_order;
        let _ = event_value_shape;
        let perf_opts = PerfOpts { inmem_bufcap: 4096 };
        // TODO let PlainEventsJsonQuery provide the tune and pass to RawEventsQuery:
        let evq = RawEventsQuery::new(self.query.channel().clone(), self.query.range().clone(), AggKind::Plain);

        // TODO Use a Merged-From-Multiple-Local-Splits.
        // TODO Pass the read buffer size from query parameter: GPFS needs a larger buffer..
        // TODO Must issue multiple reads to GPFS, keep futures in a ordered queue.

        let s = MergedFromRemotes::<ENP>::new(evq, perf_opts, self.node_config.node_config.cluster);
        let f = collect_plain_events_json(s, self.timeout, 0, self.events_max, self.query.do_log());
        let f = FutureExt::map(f, |item| match item {
            Ok(item) => {
                // TODO add channel entry info here?
                //let obj = item.as_object_mut().unwrap();
                //obj.insert("channelName", JsonValue::String(en));
                Ok(Bytes::from(serde_json::to_vec(&item)?))
            }
            Err(e) => Err(e.into()),
        });
        let s = futures_util::stream::once(f);
        Ok(Box::pin(s))
    }

    fn empty() -> Self::Output {
        Box::pin(futures_util::stream::empty())
    }
}
