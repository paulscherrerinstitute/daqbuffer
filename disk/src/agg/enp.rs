use crate::agg::binnedt::{TimeBinnableType, TimeBinnableTypeAggregator};
use crate::agg::streams::{Appendable, Collectable, Collector};
use crate::agg::{Fits, FitsInside};
use crate::binned::dim1::MinMaxAvgDim1Bins;
use crate::binned::{
    Bool, EventsNodeProcessor, FilterFittingInside, MinMaxAvgBins, MinMaxAvgWaveBins, NumOps, PushableIndex,
    RangeOverlapInfo, ReadPbv, ReadableFromFile, WithLen, WithTimestamps,
};
use crate::decode::EventValues;
use err::Error;
use netpod::log::*;
use netpod::timeunits::{MS, SEC};
use netpod::{x_bin_count, AggKind, NanoRange, Shape};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use tokio::fs::File;

pub struct Identity<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for Identity<NTY>
where
    NTY: NumOps,
{
    type Input = NTY;
    type Output = EventValues<NTY>;

    fn create(_shape: Shape, _agg_kind: AggKind) -> Self {
        Self { _m1: PhantomData }
    }

    fn process(&self, inp: EventValues<Self::Input>) -> Self::Output {
        inp
    }
}

// TODO rename Scalar -> Dim0
#[derive(Debug, Serialize, Deserialize)]
pub struct XBinnedScalarEvents<NTY> {
    tss: Vec<u64>,
    mins: Vec<NTY>,
    maxs: Vec<NTY>,
    avgs: Vec<f32>,
    xbincount: Vec<u32>,
}

impl<NTY> XBinnedScalarEvents<NTY> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
            xbincount: vec![],
        }
    }
}

impl<NTY> WithLen for XBinnedScalarEvents<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<NTY> WithTimestamps for XBinnedScalarEvents<NTY> {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<NTY> RangeOverlapInfo for XBinnedScalarEvents<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts < range.beg,
            None => true,
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        match self.tss.first() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }
}

impl<NTY> FitsInside for XBinnedScalarEvents<NTY> {
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if self.tss.is_empty() {
            Fits::Empty
        } else {
            let t1 = *self.tss.first().unwrap();
            let t2 = *self.tss.last().unwrap();
            if t2 < range.beg {
                Fits::Lower
            } else if t1 > range.end {
                Fits::Greater
            } else if t1 < range.beg && t2 > range.end {
                Fits::PartlyLowerAndGreater
            } else if t1 < range.beg {
                Fits::PartlyLower
            } else if t2 > range.end {
                Fits::PartlyGreater
            } else {
                Fits::Inside
            }
        }
    }
}

impl<NTY> FilterFittingInside for XBinnedScalarEvents<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> PushableIndex for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        self.xbincount.push(src.xbincount[ix]);
        self.mins.push(src.mins[ix]);
        self.maxs.push(src.maxs[ix]);
        self.avgs.push(src.avgs[ix]);
    }
}

impl<NTY> Appendable for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.xbincount.extend_from_slice(&src.xbincount);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
    }
}

impl<NTY> ReadableFromFile for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    fn read_from_file(_file: File) -> Result<ReadPbv<Self>, Error> {
        // TODO refactor types such that this impl is not needed.
        panic!()
    }

    fn from_buf(_buf: &[u8]) -> Result<Self, Error> {
        panic!()
    }
}

impl<NTY> TimeBinnableType for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgBins<NTY>;
    type Aggregator = XBinnedScalarEventsAggregator<NTY>;

    fn aggregator(range: NanoRange, _x_bin_count: usize) -> Self::Aggregator {
        Self::Aggregator::new(range)
    }
}

pub struct XBinnedScalarEventsAggregator<NTY>
where
    NTY: NumOps,
{
    range: NanoRange,
    count: u64,
    min: Option<NTY>,
    max: Option<NTY>,
    sumc: u64,
    sum: f32,
}

impl<NTY> XBinnedScalarEventsAggregator<NTY>
where
    NTY: NumOps,
{
    pub fn new(range: NanoRange) -> Self {
        Self {
            range,
            count: 0,
            min: None,
            max: None,
            sumc: 0,
            sum: 0f32,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for XBinnedScalarEventsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedScalarEvents<NTY>;
    type Output = MinMaxAvgBins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            if ts < self.range.beg {
                continue;
            } else if ts >= self.range.end {
                continue;
            } else {
                self.min = match self.min {
                    None => Some(item.mins[i1]),
                    Some(min) => {
                        if item.mins[i1] < min {
                            Some(item.mins[i1])
                        } else {
                            Some(min)
                        }
                    }
                };
                self.max = match self.max {
                    None => Some(item.maxs[i1]),
                    Some(max) => {
                        if item.maxs[i1] > max {
                            Some(item.maxs[i1])
                        } else {
                            Some(max)
                        }
                    }
                };
                let x = item.avgs[i1];
                if x.is_nan() {
                } else {
                    self.sum += x;
                    self.sumc += 1;
                }
                self.count += 1;
            }
        }
    }

    fn result(self) -> Self::Output {
        let avg = if self.sumc == 0 {
            None
        } else {
            Some(self.sum / self.sumc as f32)
        };
        Self::Output {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min],
            maxs: vec![self.max],
            avgs: vec![avg],
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct XBinnedScalarEventsCollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    mins: Vec<NTY>,
    maxs: Vec<NTY>,
    avgs: Vec<f32>,
    #[serde(skip_serializing_if = "Bool::is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Bool::is_false", rename = "timedOut")]
    timed_out: bool,
}

pub struct XBinnedScalarEventsCollector<NTY> {
    vals: XBinnedScalarEvents<NTY>,
    finalised_range: bool,
    timed_out: bool,
    #[allow(dead_code)]
    bin_count_exp: u32,
}

impl<NTY> XBinnedScalarEventsCollector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            finalised_range: false,
            timed_out: false,
            vals: XBinnedScalarEvents::empty(),
            bin_count_exp,
        }
    }
}

impl<NTY> WithLen for XBinnedScalarEventsCollector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
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

impl<NTY> Collector for XBinnedScalarEventsCollector<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedScalarEvents<NTY>;
    type Output = XBinnedScalarEventsCollectedResult<NTY>;

    fn ingest(&mut self, src: &Self::Input) {
        self.vals.append(src);
    }

    fn set_range_complete(&mut self) {
        self.finalised_range = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(self) -> Result<Self::Output, Error> {
        let tst = ts_offs_from_abs(&self.vals.tss);
        let ret = Self::Output {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            mins: self.vals.mins,
            maxs: self.vals.maxs,
            avgs: self.vals.avgs,
            finalised_range: self.finalised_range,
            timed_out: self.timed_out,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for XBinnedScalarEvents<NTY>
where
    NTY: NumOps,
{
    type Collector = XBinnedScalarEventsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}

// TODO  rename Wave -> Dim1
#[derive(Debug, Serialize, Deserialize)]
pub struct XBinnedWaveEvents<NTY> {
    tss: Vec<u64>,
    mins: Vec<Vec<NTY>>,
    maxs: Vec<Vec<NTY>>,
    avgs: Vec<Vec<f32>>,
}

impl<NTY> XBinnedWaveEvents<NTY> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }
}

impl<NTY> WithLen for XBinnedWaveEvents<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<NTY> WithTimestamps for XBinnedWaveEvents<NTY> {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<NTY> RangeOverlapInfo for XBinnedWaveEvents<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts < range.beg,
            None => true,
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        match self.tss.first() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }
}

impl<NTY> FitsInside for XBinnedWaveEvents<NTY> {
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if self.tss.is_empty() {
            Fits::Empty
        } else {
            let t1 = *self.tss.first().unwrap();
            let t2 = *self.tss.last().unwrap();
            if t2 < range.beg {
                Fits::Lower
            } else if t1 > range.end {
                Fits::Greater
            } else if t1 < range.beg && t2 > range.end {
                Fits::PartlyLowerAndGreater
            } else if t1 < range.beg {
                Fits::PartlyLower
            } else if t2 > range.end {
                Fits::PartlyGreater
            } else {
                Fits::Inside
            }
        }
    }
}

impl<NTY> FilterFittingInside for XBinnedWaveEvents<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> PushableIndex for XBinnedWaveEvents<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        // TODO not nice.
        self.mins.push(src.mins[ix].clone());
        self.maxs.push(src.maxs[ix].clone());
        self.avgs.push(src.avgs[ix].clone());
    }
}

impl<NTY> Appendable for XBinnedWaveEvents<NTY>
where
    NTY: NumOps,
{
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.mins.extend_from_slice(&src.mins);
        self.maxs.extend_from_slice(&src.maxs);
        self.avgs.extend_from_slice(&src.avgs);
    }
}

impl<NTY> ReadableFromFile for XBinnedWaveEvents<NTY>
where
    NTY: NumOps,
{
    fn read_from_file(_file: File) -> Result<ReadPbv<Self>, Error> {
        // TODO refactor types such that this impl is not needed.
        panic!()
    }

    fn from_buf(_buf: &[u8]) -> Result<Self, Error> {
        panic!()
    }
}

impl<NTY> TimeBinnableType for XBinnedWaveEvents<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgWaveBins<NTY>;
    type Aggregator = XBinnedWaveEventsAggregator<NTY>;

    fn aggregator(range: NanoRange, bin_count: usize) -> Self::Aggregator {
        Self::Aggregator::new(range, bin_count)
    }
}

pub struct XBinnedWaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    range: NanoRange,
    count: u64,
    min: Vec<NTY>,
    max: Vec<NTY>,
    sum: Vec<f32>,
    sumc: u64,
}

impl<NTY> XBinnedWaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    pub fn new(range: NanoRange, bin_count: usize) -> Self {
        if bin_count == 0 {
            panic!("bin_count == 0");
        }
        Self {
            range,
            count: 0,
            min: vec![NTY::max_or_nan(); bin_count],
            max: vec![NTY::min_or_nan(); bin_count],
            sum: vec![0f32; bin_count],
            sumc: 0,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for XBinnedWaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedWaveEvents<NTY>;
    type Output = MinMaxAvgWaveBins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        //info!("XBinnedWaveEventsAggregator  ingest  item {:?}", item);
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            if ts < self.range.beg {
                continue;
            } else if ts >= self.range.end {
                continue;
            } else {
                for (i2, &v) in item.mins[i1].iter().enumerate() {
                    if v < self.min[i2] || self.min[i2].is_nan() {
                        self.min[i2] = v;
                    }
                }
                for (i2, &v) in item.maxs[i1].iter().enumerate() {
                    if v > self.max[i2] || self.max[i2].is_nan() {
                        self.max[i2] = v;
                    }
                }
                for (i2, &v) in item.avgs[i1].iter().enumerate() {
                    if v.is_nan() {
                    } else {
                        self.sum[i2] += v;
                    }
                }
                self.sumc += 1;
                self.count += 1;
            }
        }
    }

    fn result(self) -> Self::Output {
        if self.sumc == 0 {
            Self::Output {
                ts1s: vec![self.range.beg],
                ts2s: vec![self.range.end],
                counts: vec![self.count],
                mins: vec![None],
                maxs: vec![None],
                avgs: vec![None],
            }
        } else {
            let avg = self.sum.iter().map(|k| *k / self.sumc as f32).collect();
            let ret = Self::Output {
                ts1s: vec![self.range.beg],
                ts2s: vec![self.range.end],
                counts: vec![self.count],
                mins: vec![Some(self.min)],
                maxs: vec![Some(self.max)],
                avgs: vec![Some(avg)],
            };
            if ret.ts1s[0] < 1300 {
                info!("XBinnedWaveEventsAggregator  result  {:?}", ret);
            }
            ret
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct XBinnedWaveEventsCollectedResult<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    mins: Vec<Vec<NTY>>,
    maxs: Vec<Vec<NTY>>,
    avgs: Vec<Vec<f32>>,
    #[serde(skip_serializing_if = "Bool::is_false", rename = "finalisedRange")]
    finalised_range: bool,
    #[serde(skip_serializing_if = "Bool::is_false", rename = "timedOut")]
    timed_out: bool,
}

pub struct XBinnedWaveEventsCollector<NTY> {
    vals: XBinnedWaveEvents<NTY>,
    finalised_range: bool,
    timed_out: bool,
    #[allow(dead_code)]
    bin_count_exp: u32,
}

impl<NTY> XBinnedWaveEventsCollector<NTY> {
    pub fn new(bin_count_exp: u32) -> Self {
        Self {
            finalised_range: false,
            timed_out: false,
            vals: XBinnedWaveEvents::empty(),
            bin_count_exp,
        }
    }
}

impl<NTY> WithLen for XBinnedWaveEventsCollector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

impl<NTY> Collector for XBinnedWaveEventsCollector<NTY>
where
    NTY: NumOps,
{
    type Input = XBinnedWaveEvents<NTY>;
    type Output = XBinnedWaveEventsCollectedResult<NTY>;

    fn ingest(&mut self, src: &Self::Input) {
        self.vals.append(src);
    }

    fn set_range_complete(&mut self) {
        self.finalised_range = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(self) -> Result<Self::Output, Error> {
        let ts_anchor_sec = self.vals.tss.first().map_or(0, |&k| k) / SEC;
        let ts_anchor_ns = ts_anchor_sec * SEC;
        let ts_off_ms: Vec<_> = self.vals.tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
        let ts_off_ns = self
            .vals
            .tss
            .iter()
            .zip(ts_off_ms.iter().map(|&k| k * MS))
            .map(|(&j, k)| (j - ts_anchor_ns - k))
            .collect();
        let ret = Self::Output {
            finalised_range: self.finalised_range,
            timed_out: self.timed_out,
            ts_anchor_sec,
            ts_off_ms,
            ts_off_ns,
            mins: self.vals.mins,
            maxs: self.vals.maxs,
            avgs: self.vals.avgs,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for XBinnedWaveEvents<NTY>
where
    NTY: NumOps,
{
    type Collector = XBinnedWaveEventsCollector<NTY>;

    fn new_collector(bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new(bin_count_exp)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WaveEvents<NTY> {
    pub tss: Vec<u64>,
    pub vals: Vec<Vec<NTY>>,
}

impl<NTY> WaveEvents<NTY> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            vals: vec![],
        }
    }
}

impl<NTY> WithLen for WaveEvents<NTY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<NTY> WithTimestamps for WaveEvents<NTY> {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<NTY> RangeOverlapInfo for WaveEvents<NTY> {
    fn ends_before(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts < range.beg,
            None => true,
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        match self.tss.first() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }
}

impl<NTY> FitsInside for WaveEvents<NTY> {
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if self.tss.is_empty() {
            Fits::Empty
        } else {
            let t1 = *self.tss.first().unwrap();
            let t2 = *self.tss.last().unwrap();
            if t2 < range.beg {
                Fits::Lower
            } else if t1 > range.end {
                Fits::Greater
            } else if t1 < range.beg && t2 > range.end {
                Fits::PartlyLowerAndGreater
            } else if t1 < range.beg {
                Fits::PartlyLower
            } else if t2 > range.end {
                Fits::PartlyGreater
            } else {
                Fits::Inside
            }
        }
    }
}

impl<NTY> FilterFittingInside for WaveEvents<NTY> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> PushableIndex for WaveEvents<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        // TODO trait should allow to move from source.
        self.vals.push(src.vals[ix].clone());
    }
}

impl<NTY> Appendable for WaveEvents<NTY>
where
    NTY: NumOps,
{
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.vals.extend_from_slice(&src.vals);
    }
}

impl<NTY> ReadableFromFile for WaveEvents<NTY>
where
    NTY: NumOps,
{
    fn read_from_file(_file: File) -> Result<ReadPbv<Self>, Error> {
        // TODO refactor types such that this impl is not needed.
        panic!()
    }

    fn from_buf(_buf: &[u8]) -> Result<Self, Error> {
        panic!()
    }
}

impl<NTY> TimeBinnableType for WaveEvents<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgDim1Bins<NTY>;
    type Aggregator = WaveEventsAggregator<NTY>;

    fn aggregator(range: NanoRange, bin_count: usize) -> Self::Aggregator {
        Self::Aggregator::new(range, bin_count)
    }
}

pub struct WaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    range: NanoRange,
    count: u64,
    min: Option<Vec<NTY>>,
    max: Option<Vec<NTY>>,
    sumc: u64,
    sum: Option<Vec<f32>>,
}

impl<NTY> WaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    pub fn new(range: NanoRange, _x_bin_count: usize) -> Self {
        Self {
            range,
            count: 0,
            // TODO create the right number of bins right here:
            min: err::todoval(),
            max: None,
            sumc: 0,
            sum: None,
        }
    }
}

impl<NTY> TimeBinnableTypeAggregator for WaveEventsAggregator<NTY>
where
    NTY: NumOps,
{
    type Input = WaveEvents<NTY>;
    type Output = MinMaxAvgDim1Bins<NTY>;

    fn range(&self) -> &NanoRange {
        &self.range
    }

    fn ingest(&mut self, item: &Self::Input) {
        for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            if ts < self.range.beg {
                continue;
            } else if ts >= self.range.end {
                continue;
            } else {
                match &mut self.min {
                    None => self.min = Some(item.vals[i1].clone()),
                    Some(min) => {
                        for (a, b) in min.iter_mut().zip(item.vals[i1].iter()) {
                            if b < a {
                                *a = *b;
                            }
                        }
                    }
                };
                match &mut self.max {
                    None => self.max = Some(item.vals[i1].clone()),
                    Some(max) => {
                        for (a, b) in max.iter_mut().zip(item.vals[i1].iter()) {
                            if b < a {
                                *a = *b;
                            }
                        }
                    }
                };
                match self.sum.as_mut() {
                    None => {
                        self.sum = Some(item.vals[i1].iter().map(|k| k.as_()).collect());
                    }
                    Some(sum) => {
                        for (a, b) in sum.iter_mut().zip(item.vals[i1].iter()) {
                            let vf = b.as_();
                            if vf.is_nan() {
                            } else {
                                *a += vf;
                            }
                        }
                    }
                }
                self.sumc += 1;
                self.count += 1;
            }
        }
    }

    fn result(self) -> Self::Output {
        let avg = if self.sumc == 0 {
            None
        } else {
            let avg = self
                .sum
                .as_ref()
                .unwrap()
                .iter()
                .map(|item| item / self.sumc as f32)
                .collect();
            Some(avg)
        };
        Self::Output {
            ts1s: vec![self.range.beg],
            ts2s: vec![self.range.end],
            counts: vec![self.count],
            mins: vec![self.min],
            maxs: vec![self.max],
            avgs: vec![avg],
        }
    }
}

pub struct WaveXBinner<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for WaveXBinner<NTY>
where
    NTY: NumOps,
{
    type Input = Vec<NTY>;
    type Output = XBinnedScalarEvents<NTY>;

    fn create(_shape: Shape, _agg_kind: AggKind) -> Self {
        Self { _m1: PhantomData }
    }

    fn process(&self, inp: EventValues<Self::Input>) -> Self::Output {
        let nev = inp.tss.len();
        let mut ret = Self::Output {
            tss: inp.tss,
            xbincount: Vec::with_capacity(nev),
            mins: Vec::with_capacity(nev),
            maxs: Vec::with_capacity(nev),
            avgs: Vec::with_capacity(nev),
        };
        for i1 in 0..nev {
            // TODO why do I work here with Option?
            err::todo();
            let mut min = None;
            let mut max = None;
            let mut sum = 0f32;
            let mut count = 0;
            let vals = &inp.values[i1];
            for i2 in 0..vals.len() {
                let v = vals[i2];
                min = match min {
                    None => Some(v),
                    Some(min) => {
                        if v < min {
                            Some(v)
                        } else {
                            Some(min)
                        }
                    }
                };
                max = match max {
                    None => Some(v),
                    Some(max) => {
                        if v > max {
                            Some(v)
                        } else {
                            Some(max)
                        }
                    }
                };
                let vf = v.as_();
                if vf.is_nan() {
                } else {
                    sum += vf;
                    count += 1;
                }
            }
            // TODO while X-binning I expect values, otherwise it is illegal input.
            ret.xbincount.push(nev as u32);
            ret.mins.push(min.unwrap());
            ret.maxs.push(max.unwrap());
            if count == 0 {
                ret.avgs.push(f32::NAN);
            } else {
                ret.avgs.push(sum / count as f32);
            }
        }
        ret
    }
}

pub struct WaveNBinner<NTY> {
    shape_bin_count: usize,
    x_bin_count: usize,
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for WaveNBinner<NTY>
where
    NTY: NumOps,
{
    type Input = Vec<NTY>;
    type Output = XBinnedWaveEvents<NTY>;

    fn create(shape: Shape, agg_kind: AggKind) -> Self {
        info!("WaveNBinner::create");
        // TODO get rid of panic potential
        let shape_bin_count = if let Shape::Wave(n) = shape { n } else { panic!() } as usize;
        let x_bin_count = x_bin_count(&shape, &agg_kind);
        info!("shape_bin_count {}  x_bin_count {}", shape_bin_count, x_bin_count);
        Self {
            shape_bin_count,
            x_bin_count,
            _m1: PhantomData,
        }
    }

    fn process(&self, inp: EventValues<Self::Input>) -> Self::Output {
        let nev = inp.tss.len();
        let mut ret = Self::Output {
            // TODO get rid of this clone:
            tss: inp.tss.clone(),
            mins: Vec::with_capacity(nev),
            maxs: Vec::with_capacity(nev),
            avgs: Vec::with_capacity(nev),
        };
        for i1 in 0..nev {
            let mut min = vec![NTY::max_or_nan(); self.x_bin_count];
            let mut max = vec![NTY::min_or_nan(); self.x_bin_count];
            let mut sum = vec![0f32; self.x_bin_count];
            let mut sumc = vec![0u64; self.x_bin_count];
            for (i2, &v) in inp.values[i1].iter().enumerate() {
                let i3 = i2 * self.x_bin_count / self.shape_bin_count;
                if v < min[i3] || min[i3].is_nan() {
                    min[i3] = v;
                }
                if v > max[i3] || max[i3].is_nan() {
                    max[i3] = v;
                }
                if v.is_nan() {
                } else {
                    sum[i3] += v.as_();
                    sumc[i3] += 1;
                }
            }
            // TODO
            if false && inp.tss[0] < 1300 {
                info!("WaveNBinner  process  push min  {:?}", min);
            }
            ret.mins.push(min);
            ret.maxs.push(max);
            let avg = sum
                .into_iter()
                .zip(sumc.into_iter())
                .map(|(j, k)| if k > 0 { j / k as f32 } else { f32::NAN })
                .collect();
            ret.avgs.push(avg);
        }
        ret
    }
}

pub struct WavePlainProc<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for WavePlainProc<NTY>
where
    NTY: NumOps,
{
    type Input = Vec<NTY>;
    type Output = WaveEvents<NTY>;

    fn create(_shape: Shape, _agg_kind: AggKind) -> Self {
        Self { _m1: PhantomData }
    }

    fn process(&self, inp: EventValues<Self::Input>) -> Self::Output {
        if false {
            let n = if inp.values.len() > 0 { inp.values[0].len() } else { 0 };
            let n = if n > 5 { 5 } else { n };
            WaveEvents {
                tss: inp.tss,
                vals: inp.values.iter().map(|k| k[..n].to_vec()).collect(),
            }
        } else {
            WaveEvents {
                tss: inp.tss,
                vals: inp.values,
            }
        }
    }
}
