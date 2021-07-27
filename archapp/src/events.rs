use crate::parse::PbFileReader;
use crate::{EventsItem, PlainEvents, ScalarPlainEvents, WavePlainEvents, XBinnedEvents};
use chrono::{TimeZone, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::eventvalues::EventValues;
use items::waveevents::{WaveEvents, WaveXBinner};
use items::xbinnedscalarevents::XBinnedScalarEvents;
use items::xbinnedwaveevents::XBinnedWaveEvents;
use items::RangeCompletableItem::RangeComplete;
use items::{EventsNodeProcessor, Framable, RangeCompletableItem, Sitemty, SitemtyFrameType, StreamItem};
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::timeunits::{DAY, SEC};
use netpod::{AggKind, ArchiverAppliance, Channel, ChannelInfo, HasScalarType, HasShape, NanoRange, ScalarType, Shape};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::{read_dir, File};

pub struct DataFilename {
    year: u32,
    month: u32,
}

pub fn parse_data_filename(s: &str) -> Result<DataFilename, Error> {
    if !s.ends_with(".pb") {
        return Err(Error::with_msg_no_trace("not a .pb file"));
    }
    if s.len() < 12 {
        return Err(Error::with_msg_no_trace("filename too short"));
    }
    let j = &s[s.len() - 11..];
    if &j[0..1] != ":" {
        return Err(Error::with_msg_no_trace("no colon"));
    }
    if &j[5..6] != "_" {
        return Err(Error::with_msg_no_trace("no underscore"));
    }
    let year: u32 = j[1..5].parse()?;
    let month: u32 = j[6..8].parse()?;
    let ret = DataFilename { year, month };
    Ok(ret)
}

struct StorageMerge {
    inps: Vec<Pin<Box<dyn Stream<Item = Sitemty<EventsItem>> + Send>>>,
    completed_inps: Vec<bool>,
    current_inp_item: Vec<Option<EventsItem>>,
    inprng: usize,
}

impl StorageMerge {
    fn refill_if_needed(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<(Pin<&mut Self>, bool), Error> {
        use Poll::*;
        let mut is_pending = false;
        for i in 0..self.inps.len() {
            if self.current_inp_item[i].is_none() && self.completed_inps[i] == false {
                match self.inps[i].poll_next_unpin(cx) {
                    Ready(j) => {
                        //
                        match j {
                            Some(j) => match j {
                                Ok(j) => match j {
                                    StreamItem::DataItem(j) => match j {
                                        RangeCompletableItem::Data(j) => {
                                            self.current_inp_item[i] = Some(j);
                                        }
                                        RangeCompletableItem::RangeComplete => {}
                                    },
                                    StreamItem::Log(_) => {}
                                    StreamItem::Stats(_) => {}
                                },
                                Err(e) => {
                                    self.completed_inps[i] = true;
                                    error!("inp err {:?}", e);
                                }
                            },
                            None => {
                                //
                                self.completed_inps[i] = true;
                            }
                        }
                    }
                    Pending => {
                        is_pending = true;
                    }
                }
            }
        }
        Ok((self, is_pending))
    }

    fn decide_next_item(&mut self) -> Result<Option<Sitemty<EventsItem>>, Error> {
        let not_found = 999;
        let mut i1 = self.inprng;
        let mut j1 = not_found;
        let mut tsmin = u64::MAX;
        use items::{WithLen, WithTimestamps};
        loop {
            if self.completed_inps[i1] {
            } else {
                match self.current_inp_item[i1].as_ref() {
                    None => panic!(),
                    Some(j) => {
                        if j.len() == 0 {
                            j1 = i1;
                            break;
                        } else {
                            let ts = j.ts(0);
                            if ts < tsmin {
                                tsmin = ts;
                                j1 = i1;
                                self.inprng = i1;
                            } else {
                            }
                        }
                    }
                }
            }
            i1 -= 1;
            if i1 == 0 {
                break;
            }
        }
        if j1 >= not_found {
            Ok(None)
        } else {
            let j = self.current_inp_item[j1]
                .take()
                .map(|j| Ok(StreamItem::DataItem(RangeCompletableItem::Data(j))));
            Ok(j)
        }
    }
}

impl Stream for StorageMerge {
    type Item = Sitemty<EventsItem>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let (mut self2, is_pending) = self.refill_if_needed(cx).unwrap();
        if is_pending {
            Pending
        } else {
            match self2.decide_next_item() {
                Ok(j) => Ready(j),
                Err(e) => {
                    error!("impl Stream for StorageMerge  {:?}", e);
                    panic!()
                }
            }
        }
    }
}

trait FrameMakerTrait: Send {
    fn make_frame(&self, ei: Sitemty<EventsItem>) -> Box<dyn Framable>;
}

struct FrameMaker {
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
}

impl FrameMaker {
    fn make_frame_gen<T>(item: Sitemty<EventsItem>) -> Box<dyn Framable>
    where
        T: SitemtyFrameType + Serialize + Send + 'static,
    {
        match item {
            Ok(_) => err::todoval(),
            Err(e) => {
                //let t = Ok(StreamItem::DataItem(RangeCompletableItem::Data()))
                let t: Sitemty<T> = Err(e);
                Box::new(t)
            }
        }
    }
}

macro_rules! events_item_to_sitemty {
    ($ei:expr, $t1:ident, $t2:ident, $t3:ident) => {{
        let ret = match $ei {
            Ok(k) => match k {
                StreamItem::DataItem(k) => match k {
                    RangeCompletableItem::Data(k) => {
                        //
                        match k {
                            EventsItem::Plain(h) => {
                                //
                                match h {
                                    PlainEvents::$t1(h) => {
                                        //
                                        match h {
                                            $t2::$t3(h) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(h))),
                                            _ => panic!(),
                                        }
                                    }
                                    _ => panic!(),
                                }
                            }
                            _ => panic!(),
                        }
                    }
                    RangeCompletableItem::RangeComplete => {
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    }
                },
                StreamItem::Log(j) => Ok(StreamItem::Log(j)),
                StreamItem::Stats(j) => Ok(StreamItem::Stats(j)),
            },
            Err(e) => Err(e),
        };
        Box::new(ret)
    }};
}

macro_rules! arm1 {
    ($item:expr, $sty:ident, $shape:expr, $ak:expr) => {{
        match $shape {
            Shape::Scalar => match $ak {
                AggKind::Plain => Self::make_frame_gen::<EventValues<$sty>>($item),
                AggKind::DimXBins1 => Self::make_frame_gen::<EventValues<$sty>>($item),
                AggKind::DimXBinsN(_) => Self::make_frame_gen::<EventValues<$sty>>($item),
            },
            Shape::Wave(_) => match $ak {
                AggKind::Plain => Self::make_frame_gen::<WaveEvents<$sty>>($item),
                AggKind::DimXBins1 => Self::make_frame_gen::<XBinnedScalarEvents<$sty>>($item),
                AggKind::DimXBinsN(_) => Self::make_frame_gen::<XBinnedWaveEvents<$sty>>($item),
            },
        }
    }};
}

impl FrameMakerTrait for FrameMaker {
    fn make_frame(&self, item: Sitemty<EventsItem>) -> Box<dyn Framable> {
        // Take from `self` the expected inner type.
        // If `ei` is not some data, then I can't dynamically determine the expected T of Sitemty.
        // Therefore, I need to decide that based on given parameters.
        // see also channel_info in this mod.
        match self.scalar_type {
            ScalarType::I8 => arm1!(item, i8, self.shape, self.agg_kind),
            ScalarType::I16 => arm1!(item, i16, self.shape, self.agg_kind),
            ScalarType::I32 => arm1!(item, i32, self.shape, self.agg_kind),
            ScalarType::F32 => arm1!(item, f32, self.shape, self.agg_kind),
            ScalarType::F64 => arm1!(item, f64, self.shape, self.agg_kind),
            _ => err::todoval(),
        }
    }
}

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    aa: &ArchiverAppliance,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    let ci = channel_info(&evq.channel, aa).await?;
    let mut inps = vec![];
    for p1 in &aa.data_base_paths {
        let p2 = p1.clone();
        let p3 = make_single_event_pipe(evq, p2).await?;
        inps.push(p3);
    }
    let sm = StorageMerge {
        inprng: inps.len() - 1,
        current_inp_item: (0..inps.len()).into_iter().map(|_| None).collect(),
        completed_inps: vec![false; inps.len()],
        inps,
    };
    let frame_maker = Box::new(FrameMaker {
        scalar_type: ci.scalar_type.clone(),
        shape: ci.shape.clone(),
        agg_kind: evq.agg_kind.clone(),
    }) as Box<dyn FrameMakerTrait>;
    let ret = sm.map(move |j| frame_maker.make_frame(j));
    Ok(Box::pin(ret))
}

pub async fn make_single_event_pipe(
    evq: &RawEventsQuery,
    base_path: PathBuf,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventsItem>> + Send>>, Error> {
    // TODO must apply the proper x-binning depending on the requested AggKind.

    info!("make_event_pipe  {:?}", evq);
    let evq = evq.clone();
    let DirAndPrefix { dir, prefix } = directory_for_channel_files(&evq.channel, base_path)?;
    //let dtbeg = Utc.timestamp((evq.range.beg / 1000000000) as i64, (evq.range.beg % 1000000000) as u32);
    let (tx, rx) = async_channel::bounded(16);
    let block1 = async move {
        trace!("++++++++++++++++++++++++++++");
        info!("start read of {:?}", dir);

        // TODO first collect all matching filenames, then sort, then open files.
        // TODO if dir does not exist, should notify client but not log as error.
        let mut rd = tokio::fs::read_dir(&dir).await?;
        while let Some(de) = rd.next_entry().await? {
            let s = de.file_name().to_string_lossy().into_owned();
            if s.starts_with(&prefix) && s.ends_with(".pb") {
                match parse_data_filename(&s) {
                    Ok(df) => {
                        info!("parse went ok: {} {}", df.year, df.month);
                        let ts0 = Utc.ymd(df.year as i32, df.month, 1).and_hms(0, 0, 0);
                        let ts1 = ts0.timestamp() as u64 * SEC + ts0.timestamp_subsec_nanos() as u64;
                        info!("file    {}   {}", ts1, ts1 + DAY * 27);
                        info!("range   {}   {}", evq.range.beg, evq.range.end);
                        if evq.range.beg < ts1 + DAY * 27 && evq.range.end > ts1 {
                            info!("••••••••••••••••••••••••••  file matches requested range");
                            let f1 = File::open(de.path()).await?;
                            info!("opened {:?}", de.path());
                            let mut pbr = PbFileReader::new(f1).await;
                            pbr.read_header().await?;
                            info!("✓ read header {:?}", pbr.payload_type());
                            loop {
                                match pbr.read_msg().await {
                                    Ok(ei) => {
                                        info!("read msg from file");
                                        let ei2 = ei.x_aggregate(&evq.agg_kind);
                                        let g = Ok(StreamItem::DataItem(RangeCompletableItem::Data(ei2)));
                                        tx.send(g).await?;
                                    }
                                    Err(e) => {
                                        error!("error while reading msg  {:?}", e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("bad filename parse {:?}", e);
                    }
                }
            } else {
                info!("prefix {}  s {}", prefix, s);
            }
        }
        Ok::<_, Error>(())
    };
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => {
                error!("{:?}", e);
            }
        }
    };
    tokio::task::spawn(block2);
    Ok(Box::pin(rx))
}

#[allow(unused)]
fn events_item_to_framable(ei: EventsItem) -> Result<Box<dyn Framable + Send>, Error> {
    match ei {
        EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::Int(h))) => {
            let range: NanoRange = err::todoval();
            let (x, y) = h
                .tss
                .into_iter()
                .zip(h.values.into_iter())
                .filter_map(|(j, k)| {
                    if j < range.beg || j >= range.end {
                        None
                    } else {
                        Some((j, k))
                    }
                })
                .fold((vec![], vec![]), |(mut a, mut b), (j, k)| {
                    a.push(j);
                    b.push(k);
                    (a, b)
                });
            let b = EventValues { tss: x, values: y };
            let b = Ok(StreamItem::DataItem(RangeCompletableItem::Data(b)));
            let ret = Box::new(b);
            Ok(ret)
        }
        _ => {
            error!("case not covered");
            Err(Error::with_msg_no_trace("todo"))
        }
    }
}

struct DirAndPrefix {
    dir: PathBuf,
    prefix: String,
}

fn directory_for_channel_files(channel: &Channel, base_path: PathBuf) -> Result<DirAndPrefix, Error> {
    // SARUN11/CVME/DBLM546/IOC_CPU_LOAD
    // SARUN11-CVME-DBLM546:IOC_CPU_LOAD
    let a: Vec<_> = channel.name.split("-").map(|s| s.split(":")).flatten().collect();
    let path = base_path;
    let path = a.iter().take(a.len() - 1).fold(path, |a, &x| a.join(x));
    let ret = DirAndPrefix {
        dir: path,
        prefix: a
            .last()
            .ok_or_else(|| Error::with_msg_no_trace("no prefix in file"))?
            .to_string(),
    };
    Ok(ret)
}

pub async fn channel_info(channel: &Channel, aa: &ArchiverAppliance) -> Result<ChannelInfo, Error> {
    let DirAndPrefix { dir, prefix } =
        directory_for_channel_files(channel, aa.data_base_paths.last().unwrap().clone())?;
    let mut msgs = vec![];
    msgs.push(format!("path: {}", dir.to_string_lossy()));
    let mut scalar_type = None;
    let mut shape = None;
    let mut rd = read_dir(&dir).await?;
    while let Some(de) = rd.next_entry().await? {
        let s = de.file_name().to_string_lossy().into_owned();
        if s.starts_with(&prefix) && s.ends_with(".pb") {
            msgs.push(s);
            let f1 = File::open(de.path()).await?;
            let mut pbr = PbFileReader::new(f1).await;
            pbr.read_header().await?;
            msgs.push(format!("got header {}", pbr.channel_name()));
            let ev = pbr.read_msg().await;
            match ev {
                Ok(item) => {
                    msgs.push(format!("got event {:?}", item));
                    shape = Some(item.shape());
                    // These type mappings are defined by the protobuffer schema.
                    scalar_type = Some(item.scalar_type());
                    break;
                }
                Err(e) => {
                    msgs.push(format!("can not read event! {:?}", e));
                }
            }
            msgs.push(format!("got header {}", pbr.channel_name()));
        }
    }
    let shape = shape.ok_or_else(|| Error::with_msg(format!("could not determine shape  {:?}", msgs)))?;
    let scalar_type =
        scalar_type.ok_or_else(|| Error::with_msg(format!("could not determine scalar_type  {:?}", msgs)))?;
    let ret = ChannelInfo {
        scalar_type,
        byte_order: None,
        shape,
        msg: JsonValue::Array(msgs.into_iter().map(JsonValue::String).collect()),
    };
    Ok(ret)
}
