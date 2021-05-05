use crate::agg::binnedt::IntoBinnedT;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatchStreamItem;
use crate::cache::pbvfs::{PreBinnedItem, PreBinnedValueFetchedStream};
use crate::cache::{node_ix_for_patch, MergedFromRemotes, PreBinnedQuery};
use crate::frame::makeframe::make_frame;
use crate::raw::EventsQuery;
use bytes::Bytes;
use err::Error;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use netpod::log::*;
use netpod::streamext::SCC;
use netpod::{BinnedRange, NodeConfigCached, PreBinnedPatchIterator, PreBinnedPatchRange};
use std::future::{ready, Future};
use std::pin::Pin;
use std::task::{Context, Poll};

pub type PreBinnedValueByteStream = SCC<PreBinnedValueByteStreamInner>;

pub struct PreBinnedValueByteStreamInner {
    inp: PreBinnedValueStream,
}

pub fn pre_binned_value_byte_stream_new(
    query: &PreBinnedQuery,
    node_config: &NodeConfigCached,
) -> PreBinnedValueByteStream {
    let s1 = PreBinnedValueStream::new(query.clone(), node_config);
    let s2 = PreBinnedValueByteStreamInner { inp: s1 };
    SCC::new(s2)
}

impl Stream for PreBinnedValueByteStreamInner {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => match make_frame::<Result<PreBinnedItem, Error>>(&item) {
                Ok(buf) => Ready(Some(Ok(buf.freeze()))),
                Err(e) => Ready(Some(Err(e.into()))),
            },
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct PreBinnedValueStream {
    query: PreBinnedQuery,
    node_config: NodeConfigCached,
    open_check_local_file: Option<Pin<Box<dyn Future<Output = Result<tokio::fs::File, std::io::Error>> + Send>>>,
    fut2: Option<Pin<Box<dyn Stream<Item = Result<PreBinnedItem, Error>> + Send>>>,
    errored: bool,
    completed: bool,
}

impl PreBinnedValueStream {
    pub fn new(query: PreBinnedQuery, node_config: &NodeConfigCached) -> Self {
        // TODO check that we are the correct node.
        let _node_ix = node_ix_for_patch(&query.patch, &query.channel, &node_config.node_config.cluster);
        Self {
            query,
            node_config: node_config.clone(),
            open_check_local_file: None,
            fut2: None,
            errored: false,
            completed: false,
        }
    }

    fn try_setup_fetch_prebinned_higher_res(&mut self) {
        info!("try_setup_fetch_prebinned_higher_res  for {:?}", self.query.patch);
        let g = self.query.patch.bin_t_len();
        let range = self.query.patch.patch_range();
        match PreBinnedPatchRange::covering_range(range, self.query.patch.bin_count() + 1) {
            Some(range) => {
                let h = range.grid_spec.bin_t_len();
                info!(
                    "try_setup_fetch_prebinned_higher_res  found  g {}  h {}  ratio {}  mod {}  {:?}",
                    g,
                    h,
                    g / h,
                    g % h,
                    range,
                );
                if g / h <= 1 {
                    error!("try_setup_fetch_prebinned_higher_res  g {}  h {}", g, h);
                    return;
                }
                if g / h > 200 {
                    error!("try_setup_fetch_prebinned_higher_res  g {}  h {}", g, h);
                    return;
                }
                if g % h != 0 {
                    error!("try_setup_fetch_prebinned_higher_res  g {}  h {}", g, h);
                    return;
                }
                let node_config = self.node_config.clone();
                let patch_it = PreBinnedPatchIterator::from_range(range);
                let s = futures_util::stream::iter(patch_it)
                    .map({
                        let q2 = self.query.clone();
                        move |patch| {
                            let query = PreBinnedQuery {
                                patch,
                                channel: q2.channel.clone(),
                                agg_kind: q2.agg_kind.clone(),
                                cache_usage: q2.cache_usage.clone(),
                            };
                            PreBinnedValueFetchedStream::new(&query, &node_config)
                        }
                    })
                    .filter_map(|k| match k {
                        Ok(k) => ready(Some(k)),
                        Err(e) => {
                            // TODO Reconsider error handling here:
                            error!("{:?}", e);
                            ready(None)
                        }
                    })
                    .flatten();
                self.fut2 = Some(Box::pin(s));
            }
            None => {
                warn!("no better resolution found for  g {}", g);
                let evq = EventsQuery {
                    channel: self.query.channel.clone(),
                    range: self.query.patch.patch_range(),
                    agg_kind: self.query.agg_kind.clone(),
                };
                if self.query.patch.patch_t_len() % self.query.patch.bin_t_len() != 0 {
                    error!(
                        "Patch length inconsistency  {}  {}",
                        self.query.patch.patch_t_len(),
                        self.query.patch.bin_t_len()
                    );
                    return;
                }
                error!("try_setup_fetch_prebinned_higher_res  apply all requested transformations and T-binning");
                let count = self.query.patch.patch_t_len() / self.query.patch.bin_t_len();
                let range = BinnedRange::covering_range(evq.range.clone(), count).unwrap();
                let s1 = MergedFromRemotes::new(evq, self.node_config.node_config.cluster.clone());
                let s2 = s1.into_binned_t(range).map(|k| match k {
                    Ok(MinMaxAvgScalarBinBatchStreamItem::Values(k)) => Ok(PreBinnedItem::Batch(k)),
                    Ok(MinMaxAvgScalarBinBatchStreamItem::RangeComplete) => Ok(PreBinnedItem::RangeComplete),
                    Ok(MinMaxAvgScalarBinBatchStreamItem::EventDataReadStats(stats)) => {
                        Ok(PreBinnedItem::EventDataReadStats(stats))
                    }
                    Err(e) => Err(e),
                });
                self.fut2 = Some(Box::pin(s2));
            }
        }
    }
}

impl Stream for PreBinnedValueStream {
    // TODO need this generic for scalar and array (when wave is not binned down to a single scalar point)
    type Item = Result<PreBinnedItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("PreBinnedValueStream  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        'outer: loop {
            break if let Some(fut) = self.fut2.as_mut() {
                match fut.poll_next_unpin(cx) {
                    Ready(Some(k)) => match k {
                        Ok(k) => Ready(Some(Ok(k))),
                        Err(e) => {
                            self.errored = true;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(None) => Ready(None),
                    Pending => Pending,
                }
            } else if let Some(fut) = self.open_check_local_file.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(Ok(_file)) => {
                        let e = Err(Error::with_msg(format!("TODO use the cached data from file")));
                        self.errored = true;
                        Ready(Some(e))
                    }
                    Ready(Err(e)) => match e.kind() {
                        std::io::ErrorKind::NotFound => {
                            error!("TODO LOCAL CACHE FILE NOT FOUND");
                            self.try_setup_fetch_prebinned_higher_res();
                            if self.fut2.is_none() {
                                let e = Err(Error::with_msg(format!("try_setup_fetch_prebinned_higher_res  failed")));
                                self.errored = true;
                                Ready(Some(e))
                            } else {
                                continue 'outer;
                            }
                        }
                        _ => {
                            error!("File I/O error: {:?}", e);
                            self.errored = true;
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
