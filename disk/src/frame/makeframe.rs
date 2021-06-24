use crate::agg::enp::{WaveEvents, XBinnedScalarEvents, XBinnedWaveEvents};
use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::StreamItem;
use crate::binned::dim1::MinMaxAvgDim1Bins;
use crate::binned::{MinMaxAvgBins, MinMaxAvgWaveBins, NumOps, RangeCompletableItem};
use crate::decode::EventValues;
use crate::frame::inmem::InMemoryFrame;
use crate::raw::EventQueryJsonStringFrame;
use crate::Sitemty;
use bytes::{BufMut, BytesMut};
use err::Error;
use netpod::BoolNum;
use serde::{de::DeserializeOwned, Serialize};

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

impl SubFrId for BoolNum {
    const SUB: u32 = 13;
}

pub trait FrameType {
    const FRAME_TYPE_ID: u32;
}

impl FrameType for EventQueryJsonStringFrame {
    const FRAME_TYPE_ID: u32 = 0x100;
}

impl FrameType for Sitemty<MinMaxAvgScalarBinBatch> {
    const FRAME_TYPE_ID: u32 = 0x200;
}

impl FrameType for Sitemty<MinMaxAvgScalarEventBatch> {
    const FRAME_TYPE_ID: u32 = 0x300;
}

impl<NTY> FrameType for Sitemty<EventValues<NTY>>
where
    NTY: NumOps,
{
    const FRAME_TYPE_ID: u32 = 0x500 + NTY::SUB;
}

impl<NTY> FrameType for Sitemty<XBinnedScalarEvents<NTY>>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = 0x600 + NTY::SUB;
}

impl<NTY> FrameType for Sitemty<MinMaxAvgBins<NTY>>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = 0x700 + NTY::SUB;
}

impl<NTY> FrameType for Sitemty<WaveEvents<NTY>>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = 0x800 + NTY::SUB;
}

impl<NTY> FrameType for Sitemty<XBinnedWaveEvents<NTY>>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = 0x900 + NTY::SUB;
}

impl<NTY> FrameType for Sitemty<MinMaxAvgWaveBins<NTY>>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = 0xa00 + NTY::SUB;
}

impl<NTY> FrameType for Sitemty<MinMaxAvgDim1Bins<NTY>>
where
    NTY: SubFrId,
{
    const FRAME_TYPE_ID: u32 = 0xb00 + NTY::SUB;
}

pub trait ProvidesFrameType {
    fn frame_type_id(&self) -> u32;
}

pub trait Framable: Send {
    fn typeid(&self) -> u32;
    fn make_frame(&self) -> Result<BytesMut, Error>;
}

impl Framable for Sitemty<serde_json::Value> {
    fn typeid(&self) -> u32 {
        EventQueryJsonStringFrame::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        panic!()
    }
}

impl Framable for Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarBinBatch>>, Error> {
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl Framable for Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatch>>, Error> {
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Result<StreamItem<RangeCompletableItem<EventValues<NTY>>>, err::Error>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Result<StreamItem<RangeCompletableItem<XBinnedScalarEvents<NTY>>>, err::Error>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<MinMaxAvgBins<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<WaveEvents<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<XBinnedWaveEvents<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<MinMaxAvgWaveBins<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> Framable for Sitemty<MinMaxAvgDim1Bins<NTY>>
where
    NTY: NumOps + Serialize,
{
    fn typeid(&self) -> u32 {
        Self::FRAME_TYPE_ID
    }
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

pub fn make_frame<FT>(item: &FT) -> Result<BytesMut, Error>
where
    FT: FrameType + Serialize,
{
    match bincode::serialize(item) {
        Ok(enc) => {
            if enc.len() > u32::MAX as usize {
                return Err(Error::with_msg(format!("too long payload {}", enc.len())));
            }
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let mut buf = BytesMut::with_capacity(enc.len() + INMEM_FRAME_HEAD);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
            buf.put_u32_le(FT::FRAME_TYPE_ID);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(payload_crc);
            buf.put(enc.as_ref());
            let mut h = crc32fast::Hasher::new();
            h.update(&buf);
            let frame_crc = h.finalize();
            buf.put_u32_le(frame_crc);
            Ok(buf)
        }
        Err(e) => Err(e)?,
    }
}

pub fn make_term_frame() -> BytesMut {
    let mut h = crc32fast::Hasher::new();
    h.update(&[]);
    let payload_crc = h.finalize();
    let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(INMEM_FRAME_ENCID);
    buf.put_u32_le(0x01);
    buf.put_u32_le(0);
    buf.put_u32_le(payload_crc);
    let mut h = crc32fast::Hasher::new();
    h.update(&buf);
    let frame_crc = h.finalize();
    buf.put_u32_le(frame_crc);
    buf
}

pub fn decode_frame<T>(frame: &InMemoryFrame) -> Result<T, Error>
where
    T: FrameType + DeserializeOwned,
{
    if frame.encid() != INMEM_FRAME_ENCID {
        return Err(Error::with_msg(format!("unknown encoder id {:?}", frame)));
    }
    if frame.tyid() != <T as FrameType>::FRAME_TYPE_ID {
        return Err(Error::with_msg(format!(
            "type id mismatch  expect {:x}  found {:?}",
            <T as FrameType>::FRAME_TYPE_ID,
            frame
        )));
    }
    if frame.len() as usize != frame.buf().len() {
        return Err(Error::with_msg(format!(
            "buf mismatch  {}  vs  {}  in {:?}",
            frame.len(),
            frame.buf().len(),
            frame
        )));
    }
    match bincode::deserialize(frame.buf()) {
        Ok(item) => Ok(item),
        Err(e) => Err(e.into()),
    }
}

pub fn crchex<T>(t: T) -> String
where
    T: AsRef<[u8]>,
{
    let mut h = crc32fast::Hasher::new();
    h.update(t.as_ref());
    let crc = h.finalize();
    format!("{:08x}", crc)
}
