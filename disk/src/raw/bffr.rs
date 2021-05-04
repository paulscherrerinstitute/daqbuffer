use crate::agg::MinMaxAvgScalarEventBatchStreamItem;
use crate::frame::inmem::InMemoryFrameAsyncReadStream;
use crate::frame::makeframe::decode_frame;
use crate::raw::conn::RawConnOut;
use err::Error;
use futures_core::Stream;
use futures_util::pin_mut;
#[allow(unused_imports)]
use netpod::log::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

pub struct MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    inp: InMemoryFrameAsyncReadStream<T>,
    errored: bool,
    completed: bool,
}

impl<T> MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    pub fn new(inp: InMemoryFrameAsyncReadStream<T>) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
        }
    }
}

impl<T> Stream for MinMaxAvgScalarEventBatchStreamFromFrames<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<MinMaxAvgScalarEventBatchStreamItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("MinMaxAvgScalarEventBatchStreamFromFrames  poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        loop {
            let j = &mut self.inp;
            pin_mut!(j);
            break match j.poll_next(cx) {
                Ready(Some(Ok(frame))) => {
                    type ExpectedType = RawConnOut;
                    trace!(
                        "MinMaxAvgScalarEventBatchStreamFromFrames  got full frame buf  {}",
                        frame.buf().len()
                    );
                    match decode_frame::<ExpectedType>(&frame) {
                        Ok(item) => match item {
                            Ok(item) => Ready(Some(Ok(item))),
                            Err(e) => {
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                        },
                        Err(e) => {
                            error!(
                                "MinMaxAvgScalarEventBatchStreamFromFrames  ~~~~~~~~   ERROR on frame payload {}",
                                frame.buf().len(),
                            );
                            self.errored = true;
                            Ready(Some(Err(e)))
                        }
                    }
                }
                Ready(Some(Err(e))) => {
                    self.errored = true;
                    Ready(Some(Err(e)))
                }
                Ready(None) => {
                    self.completed = true;
                    Ready(None)
                }
                Pending => Pending,
            };
        }
    }
}