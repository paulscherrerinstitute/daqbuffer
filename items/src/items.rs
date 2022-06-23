pub mod binnedevents;
pub mod binsdim0;
pub mod binsdim1;
pub mod eventsitem;
pub mod frame;
pub mod inmem;
pub mod numops;
pub mod plainevents;
pub mod scalarevents;
pub mod statsevents;
pub mod streams;
pub mod waveevents;
pub mod xbinnedscalarevents;
pub mod xbinnedwaveevents;

use crate::frame::make_frame_2;
use crate::numops::BoolNum;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use err::Error;
use frame::make_error_frame;
#[allow(unused)]
use netpod::log::*;
use netpod::timeunits::{MS, SEC};
use netpod::{log::Level, AggKind, EventDataReadStats, EventQueryJsonStringFrame, NanoRange, Shape};
use netpod::{DiskStats, RangeFilterStats};
use numops::StringNum;
use serde::de::{self, DeserializeOwned, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{AsyncRead, ReadBuf};

pub const TERM_FRAME_TYPE_ID: u32 = 0x01;
pub const ERROR_FRAME_TYPE_ID: u32 = 0x02;
pub const EVENT_QUERY_JSON_STRING_FRAME: u32 = 0x100;
pub const EVENT_VALUES_FRAME_TYPE_ID: u32 = 0x500;
pub const WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0x800;
pub const X_BINNED_SCALAR_EVENTS_FRAME_TYPE_ID: u32 = 0x8800;
pub const X_BINNED_WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0x900;
pub const MIN_MAX_AVG_WAVE_BINS: u32 = 0xa00;
pub const MIN_MAX_AVG_DIM_0_BINS_FRAME_TYPE_ID: u32 = 0x700;
pub const MIN_MAX_AVG_DIM_1_BINS_FRAME_TYPE_ID: u32 = 0xb00;
pub const EVENT_FULL_FRAME_TYPE_ID: u32 = 0x2200;
pub const EVENTS_ITEM_FRAME_TYPE_ID: u32 = 0x2300;
pub const STATS_EVENTS_FRAME_TYPE_ID: u32 = 0x2400;

pub fn bool_is_false(j: &bool) -> bool {
    *j == false
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RangeCompletableItem<T> {
    RangeComplete,
    Data(T),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StatsItem {
    EventDataReadStats(EventDataReadStats),
    RangeFilterStats(RangeFilterStats),
    DiskStats(DiskStats),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StreamItem<T> {
    DataItem(T),
    Log(LogItem),
    Stats(StatsItem),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogItem {
    pub node_ix: u32,
    #[serde(with = "levelserde")]
    pub level: Level,
    pub msg: String,
}

impl LogItem {
    pub fn quick(level: Level, msg: String) -> Self {
        Self {
            level,
            msg,
            node_ix: 42,
        }
    }
}

pub type Sitemty<T> = Result<StreamItem<RangeCompletableItem<T>>, Error>;

struct VisitLevel;

impl<'de> Visitor<'de> for VisitLevel {
    type Value = u32;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "expect u32 Level code")
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }
}

mod levelserde {
    use super::Level;
    use super::VisitLevel;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(t: &Level, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let g = match *t {
            Level::ERROR => 1,
            Level::WARN => 2,
            Level::INFO => 3,
            Level::DEBUG => 4,
            Level::TRACE => 5,
        };
        s.serialize_u32(g)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        match d.deserialize_u32(VisitLevel) {
            Ok(level) => {
                let g = if level == 1 {
                    Level::ERROR
                } else if level == 2 {
                    Level::WARN
                } else if level == 3 {
                    Level::INFO
                } else if level == 4 {
                    Level::DEBUG
                } else if level == 5 {
                    Level::TRACE
                } else {
                    Level::TRACE
                };
                Ok(g)
            }
            Err(e) => Err(e),
        }
    }
}

pub const INMEM_FRAME_ENCID: u32 = 0x12121212;
pub const INMEM_FRAME_HEAD: usize = 20;
pub const INMEM_FRAME_FOOT: usize = 4;
pub const INMEM_FRAME_MAGIC: u32 = 0xc6c3b73d;

pub trait SubFrId {
    const SUB: u32;
}

impl SubFrId for u8 {
    const SUB: u32 = 3;
}

impl SubFrId for u16 {
    const SUB: u32 = 5;
}

impl SubFrId for u32 {
    const SUB: u32 = 8;
}

impl SubFrId for u64 {
    const SUB: u32 = 10;
}

impl SubFrId for i8 {
    const SUB: u32 = 2;
}

impl SubFrId for i16 {
    const SUB: u32 = 4;
}

impl SubFrId for i32 {
    const SUB: u32 = 7;
}

impl SubFrId for i64 {
    const SUB: u32 = 9;
}

impl SubFrId for f32 {
    const SUB: u32 = 11;
}

impl SubFrId for f64 {
    const SUB: u32 = 12;
}

impl SubFrId for StringNum {
    const SUB: u32 = 13;
}

impl SubFrId for BoolNum {
    const SUB: u32 = 14;
}

// To be implemented by the data containers, i.e. the T's in Sitemty<T>, e.g. ScalarEvents.
// TODO rename this, since it is misleading because it is not meanto to be implemented by Sitemty.
pub trait SitemtyFrameType {
    //const FRAME_TYPE_ID: u32;
    fn frame_type_id(&self) -> u32;
}

pub trait FrameTypeStatic {
    const FRAME_TYPE_ID: u32;
    fn from_error(x: ::err::Error) -> Self;
}

// Meant to be implemented by Sitemty.
pub trait FrameType {
    fn frame_type_id(&self) -> u32;
    fn is_err(&self) -> bool;
    fn err(&self) -> Option<&::err::Error>;
}

impl FrameTypeStatic for EventQueryJsonStringFrame {
    const FRAME_TYPE_ID: u32 = EVENT_QUERY_JSON_STRING_FRAME;

    fn from_error(_x: err::Error) -> Self {
        error!("FrameTypeStatic::from_error todo");
        todo!()
    }
}

impl<T: FrameTypeStatic> FrameTypeStatic for Sitemty<T> {
    const FRAME_TYPE_ID: u32 = <T as FrameTypeStatic>::FRAME_TYPE_ID;

    fn from_error(_: err::Error) -> Self {
        // TODO remove this method.
        panic!()
    }
}

impl<T> FrameType for Box<T>
where
    T: FrameType,
{
    fn frame_type_id(&self) -> u32 {
        self.as_ref().frame_type_id()
    }

    fn is_err(&self) -> bool {
        self.as_ref().is_err()
    }

    fn err(&self) -> Option<&::err::Error> {
        self.as_ref().err()
    }
}

impl<T> FrameType for Sitemty<T>
where
    // SitemtyFrameType
    T: FrameTypeStatic,
{
    fn frame_type_id(&self) -> u32 {
        <T as FrameTypeStatic>::FRAME_TYPE_ID
    }

    fn is_err(&self) -> bool {
        match self {
            Ok(_) => false,
            Err(_) => true,
        }
    }

    fn err(&self) -> Option<&::err::Error> {
        match self {
            Ok(_) => None,
            Err(e) => Some(e),
        }
    }
}

impl FrameType for EventQueryJsonStringFrame {
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeStatic>::FRAME_TYPE_ID
    }

    fn is_err(&self) -> bool {
        false
    }

    fn err(&self) -> Option<&::err::Error> {
        None
    }
}

impl SitemtyFrameType for Box<dyn TimeBinned> {
    fn frame_type_id(&self) -> u32 {
        self.as_time_binnable_dyn().frame_type_id()
    }
}

// TODO do we need Send here?
pub trait Framable {
    fn make_frame(&self) -> Result<BytesMut, Error>;
}

// erased_serde::Serialize
pub trait FramableInner: SitemtyFrameType + Send {
    fn _dummy(&self);
}

// erased_serde::Serialize`
impl<T: SitemtyFrameType + Send> FramableInner for T {
    fn _dummy(&self) {}
}

//impl<T: SitemtyFrameType + Serialize + Send> FramableInner for Box<T> {}

// TODO need also Framable for those types defined in other crates.
// TODO not all T have FrameTypeStatic, e.g. Box<dyn TimeBinned>
impl<T> Framable for Sitemty<T>
//where
//Self: erased_serde::Serialize,
//T: FramableInner + FrameTypeStatic,
//T: Sized,
{
    fn make_frame(&self) -> Result<BytesMut, Error> {
        todo!()
    }

    /*fn make_frame(&self) -> Result<BytesMut, Error> {
        //trace!("make_frame");
        match self {
            Ok(_) => make_frame_2(
                self,
                //T::FRAME_TYPE_ID
                self.frame_type_id(),
            ),
            Err(e) => make_error_frame(e),
        }
    }*/
}

impl<T> Framable for Box<T>
where
    T: Framable + ?Sized,
{
    fn make_frame(&self) -> Result<BytesMut, Error> {
        self.as_ref().make_frame()
    }
}

pub trait EventsNodeProcessor: Send + Unpin {
    type Input;
    type Output: Send + Unpin + DeserializeOwned + WithTimestamps + TimeBinnableType + ByteEstimate;
    fn create(shape: Shape, agg_kind: AggKind) -> Self;
    fn process(&self, inp: Self::Input) -> Self::Output;
}

pub trait EventsTypeAliases {
    type TimeBinOutput;
}

impl<ENP> EventsTypeAliases for ENP
where
    ENP: EventsNodeProcessor,
    <ENP as EventsNodeProcessor>::Output: TimeBinnableType,
{
    type TimeBinOutput = <<ENP as EventsNodeProcessor>::Output as TimeBinnableType>::Output;
}

#[derive(Clone, Debug, Deserialize)]
pub struct IsoDateTime(chrono::DateTime<Utc>);

impl Serialize for IsoDateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string())
    }
}

pub fn make_iso_ts(tss: &[u64]) -> Vec<IsoDateTime> {
    tss.iter()
        .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
        .collect()
}

pub enum Fits {
    Empty,
    Lower,
    Greater,
    Inside,
    PartlyLower,
    PartlyGreater,
    PartlyLowerAndGreater,
}

pub trait WithLen {
    fn len(&self) -> usize;
}

pub trait WithTimestamps {
    fn ts(&self, ix: usize) -> u64;
}

pub trait ByteEstimate {
    fn byte_estimate(&self) -> u64;
}

pub trait RangeOverlapInfo {
    fn ends_before(&self, range: NanoRange) -> bool;
    fn ends_after(&self, range: NanoRange) -> bool;
    fn starts_after(&self, range: NanoRange) -> bool;
}

pub trait FitsInside {
    fn fits_inside(&self, range: NanoRange) -> Fits;
}

pub trait FilterFittingInside: Sized {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self>;
}

pub trait PushableIndex {
    // TODO check whether it makes sense to allow a move out of src. Or use a deque for src type and pop?
    fn push_index(&mut self, src: &Self, ix: usize);
}

pub trait Appendable: WithLen {
    fn empty_like_self(&self) -> Self;
    fn append(&mut self, src: &Self);
}

pub trait Clearable {
    fn clear(&mut self);
}

pub trait EventAppendable
where
    Self: Sized,
{
    type Value;
    fn append_event(ret: Option<Self>, ts: u64, pulse: u64, value: Self::Value) -> Self;
}

pub trait TimeBins: Send + Unpin + WithLen + Appendable + FilterFittingInside {
    fn ts1s(&self) -> &Vec<u64>;
    fn ts2s(&self) -> &Vec<u64>;
}

pub trait TimeBinnableType:
    Send + Unpin + RangeOverlapInfo + FilterFittingInside + Appendable + Serialize + ReadableFromFile + FrameTypeStatic
{
    type Output: TimeBinnableType;
    type Aggregator: TimeBinnableTypeAggregator<Input = Self, Output = Self::Output> + Send + Unpin;
    fn aggregator(range: NanoRange, bin_count: usize, do_time_weight: bool) -> Self::Aggregator;
}

/// Provides a time-binned representation of the implementing type.
/// In contrast to `TimeBinnableType` this is meant for trait objects.

// TODO should not require Sync!
// TODO SitemtyFrameType is already supertrait of FramableInner.
pub trait TimeBinnableDyn: FramableInner + SitemtyFrameType + Sync + Send {
    fn aggregator_new(&self) -> Box<dyn TimeBinnableDynAggregator>;
}

pub trait TimeBinnableDynAggregator: Send {
    fn ingest(&mut self, item: &dyn TimeBinnableDyn);
    fn result(&mut self) -> Box<dyn TimeBinned>;
}

/// Container of some form of events, for use as trait object.
pub trait EventsDyn: TimeBinnableDyn {}

/// Data in time-binned form.
pub trait TimeBinned: TimeBinnableDyn {
    fn as_time_binnable_dyn(&self) -> &dyn TimeBinnableDyn;
    fn workaround_clone(&self) -> Box<dyn TimeBinned>;
    fn dummy_test_i32(&self) -> i32;
}

// TODO this impl is already covered by the generic one:
/*impl FramableInner for Box<dyn TimeBinned> {
    fn _dummy(&self) {}
}*/

impl TimeBinnableDyn for Box<dyn TimeBinned> {
    fn aggregator_new(&self) -> Box<dyn TimeBinnableDynAggregator> {
        self.as_time_binnable_dyn().aggregator_new()
    }
}

// TODO should get I/O and tokio dependence out of this crate
pub trait ReadableFromFile: Sized {
    fn read_from_file(file: File) -> Result<ReadPbv<Self>, Error>;
    // TODO should not need this:
    fn from_buf(buf: &[u8]) -> Result<Self, Error>;
}

// TODO should get I/O and tokio dependence out of this crate
pub struct ReadPbv<T>
where
    T: ReadableFromFile,
{
    buf: Vec<u8>,
    all: Vec<u8>,
    file: Option<File>,
    _m1: PhantomData<T>,
}

impl<T> ReadPbv<T>
where
    T: ReadableFromFile,
{
    fn new(file: File) -> Self {
        Self {
            // TODO make buffer size a parameter:
            buf: vec![0; 1024 * 32],
            all: vec![],
            file: Some(file),
            _m1: PhantomData,
        }
    }
}

impl<T> Future for ReadPbv<T>
where
    T: ReadableFromFile + Unpin,
{
    type Output = Result<StreamItem<RangeCompletableItem<T>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let mut buf = std::mem::replace(&mut self.buf, Vec::new());
        let ret = 'outer: loop {
            let mut dst = ReadBuf::new(&mut buf);
            if dst.remaining() == 0 || dst.capacity() == 0 {
                break Ready(Err(Error::with_msg("bad read buffer")));
            }
            let fp = self.file.as_mut().unwrap();
            let f = Pin::new(fp);
            break match File::poll_read(f, cx, &mut dst) {
                Ready(res) => match res {
                    Ok(_) => {
                        if dst.filled().len() > 0 {
                            self.all.extend_from_slice(dst.filled());
                            continue 'outer;
                        } else {
                            match T::from_buf(&mut self.all) {
                                Ok(item) => Ready(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))),
                                Err(e) => Ready(Err(e)),
                            }
                        }
                    }
                    Err(e) => Ready(Err(e.into())),
                },
                Pending => Pending,
            };
        };
        self.buf = buf;
        ret
    }
}

pub fn ts_offs_from_abs(tss: &[u64]) -> (u64, Vec<u64>, Vec<u64>) {
    let ts_anchor_sec = tss.first().map_or(0, |&k| k) / SEC;
    let ts_anchor_ns = ts_anchor_sec * SEC;
    let ts_off_ms: Vec<_> = tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
    let ts_off_ns = tss
        .iter()
        .zip(ts_off_ms.iter().map(|&k| k * MS))
        .map(|(&j, k)| (j - ts_anchor_ns - k))
        .collect();
    (ts_anchor_sec, ts_off_ms, ts_off_ns)
}

pub fn pulse_offs_from_abs(pulse: &[u64]) -> (u64, Vec<u64>) {
    let pulse_anchor = pulse.first().map_or(0, |k| *k);
    let pulse_off: Vec<_> = pulse.iter().map(|k| *k - pulse_anchor).collect();
    (pulse_anchor, pulse_off)
}

pub trait TimeBinnableTypeAggregator: Send {
    type Input: TimeBinnableType;
    type Output: TimeBinnableType;
    fn range(&self) -> &NanoRange;
    fn ingest(&mut self, item: &Self::Input);
    fn result_reset(&mut self, range: NanoRange, expand: bool) -> Self::Output;
}

pub trait TimestampInspectable: WithTimestamps + WithLen {}

impl<T> TimestampInspectable for T where T: WithTimestamps + WithLen {}

pub fn inspect_timestamps(events: &dyn TimestampInspectable, range: NanoRange) -> String {
    use fmt::Write;
    let rd = range.delta();
    let mut buf = String::new();
    let n = events.len();
    for i in 0..n {
        if i < 3 || i > (n - 4) {
            let ts = events.ts(i);
            let z = ts - range.beg;
            let z = z as f64 / rd as f64 * 2.0 - 1.0;
            write!(&mut buf, "i  {:3}  tt {:6.3}\n", i, z).unwrap();
        }
    }
    buf
}