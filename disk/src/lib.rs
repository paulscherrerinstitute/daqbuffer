#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::task::{Context, Poll};
use std::pin::Pin;
use tokio::io::AsyncRead;
use tokio::fs::File;
use std::future::Future;
use futures_core::Stream;
use futures_util::future::FusedFuture;
use futures_util::{pin_mut, StreamExt};
use bytes::{Bytes, BytesMut, BufMut, Buf};
use std::path::PathBuf;


pub async fn read_test_1(query: &netpod::AggQuerySingleChannel) -> Result<netpod::BodyStream, Error> {
    let pre = "/data/sf-databuffer/daq_swissfel";
    let path = format!("{}/{}_{}/byTime/{}/{:019}/{:010}/{:019}_00000_Data", pre, query.ksprefix, query.keyspace, query.channel.name(), query.timebin, query.split, query.tbsize);
    debug!("try path: {}", path);
    let fin = tokio::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .await?;
    let meta = fin.metadata().await;
    debug!("file meta {:?}", meta);
    let stream = netpod::BodyStream {
        inner: Box::new(FileReader {
            file: fin,
            nreads: 0,
            buffer_size: query.buffer_size,
        }),
    };
    Ok(stream)
}

struct FileReader {
    file: tokio::fs::File,
    nreads: u32,
    buffer_size: u32,
}

impl Stream for FileReader {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let blen = self.buffer_size as usize;
        let mut buf2 = BytesMut::with_capacity(blen);
        buf2.resize(buf2.capacity(), 0);
        if buf2.as_mut().len() != blen {
            panic!("logic");
        }
        let mut buf = tokio::io::ReadBuf::new(buf2.as_mut());
        if buf.filled().len() != 0 {
            panic!("logic");
        }
        match Pin::new(&mut self.file).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(_)) => {
                let rlen = buf.filled().len();
                if rlen == 0 {
                    Poll::Ready(None)
                }
                else {
                    if rlen != blen {
                        info!("short read  {} of {}", buf.filled().len(), blen);
                    }
                    self.nreads += 1;
                    Poll::Ready(Some(Ok(buf2.freeze())))
                }
            }
            Poll::Ready(Err(e)) => {
                Poll::Ready(Some(Err(Error::from(e))))
            }
            Poll::Pending => Poll::Pending
        }
    }

}


#[allow(dead_code)]
struct Fopen1 {
    opts: tokio::fs::OpenOptions,
    fut: Pin<Box<dyn Future<Output=Result<tokio::fs::File, std::io::Error>>>>,
    term: bool,
}

impl Fopen1 {

    pub fn new(path: PathBuf) -> Self {
        let fut = Box::pin(async {
            let mut o1 = tokio::fs::OpenOptions::new();
            let o2 = o1.read(true);
            let res = o2.open(path);
            //() == res;
            //todo!()
            res.await
        }) as Pin<Box<dyn Future<Output=std::io::Result<tokio::fs::File>>>>;
        let _fut2: Box<dyn Future<Output=u32>> = Box::new(async {
            123
        });
        Self {
            opts: tokio::fs::OpenOptions::new(),
            fut,
            term: false,
        }
    }

}

impl Future for Fopen1 {
    type Output = Result<tokio::fs::File, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let g = self.fut.as_mut();
        match g.poll(cx) {
            Poll::Ready(Ok(k)) => {
                self.term = true;
                Poll::Ready(Ok(k))
            },
            Poll::Ready(Err(k)) => {
                self.term = true;
                Poll::Ready(Err(k.into()))
            },
            Poll::Pending => Poll::Pending,
        }
    }

}


impl FusedFuture for Fopen1 {
    fn is_terminated(&self) -> bool {
        self.term
    }
}

unsafe impl Send for Fopen1 {}


pub fn raw_concat_channel_read_stream_try_open_in_background(query: &netpod::AggQuerySingleChannel) -> impl Stream<Item=Result<Bytes, Error>> + Send {
    use futures_util::{FutureExt, select};
    use tokio::io::AsyncReadExt;
    let mut query = query.clone();
    async_stream::stream! {
        let mut fopen = None;
        let mut fopen_avail = false;
        let mut file_prep: Option<File> = None;
        let mut file: Option<File> = None;
        let mut reading = None;
        let mut i1 = 0;
        let mut i9 = 0;
        loop {
            let blen = query.buffer_size as usize;
            {
                if !fopen_avail && file_prep.is_none() && i1 < 16 {
                    query.timebin = 18700 + i1;
                    info!("Prepare open task for next file {}", query.timebin);
                    fopen.replace(Fopen1::new(datapath(&query)));
                    fopen_avail = true;
                    i1 += 1;
                }
            }
            if !fopen_avail && file_prep.is_none() && file.is_none() && reading.is_none() {
                info!("Nothing more to do");
                break;
            }
            // TODO
            // When the file is available, I can prepare the next reading.
            // But next iteration, the file is not available, but reading is, so I should read!
            // I can not simply drop the reading future, that would lose the request.

            if reading.is_some() {
                let k: Result<(tokio::fs::File, BytesMut), Error> = reading.as_mut().unwrap().await;
                if k.is_err() {
                    error!("LONELY READ ERROR");
                }
                let k = k.unwrap();
                reading.take();
                file = Some(k.0);
                yield Ok(k.1.freeze());
            }
            else if fopen.is_some() {
                if file.is_some() {
                    if reading.is_none() {
                        let mut buf = BytesMut::with_capacity(blen);
                        let mut file2 = file.take().unwrap();
                        let a = async move {
                            file2.read_buf(&mut buf).await?;
                            Ok::<_, Error>((file2, buf))
                        };
                        let a = Box::pin(a);
                        reading = Some(a.fuse());
                    }
                    // TODO do I really have to take out the future while waiting on it?
                    // I think the issue is now with the mutex guard, can I get rid of the mutex again?
                    let mut fopen3 = fopen.take().unwrap();
                    let bufres = select! {
                        // TODO can I avoid the unwraps via matching already above?
                        f = fopen3 => {
                            fopen_avail = false;
                            // TODO feed out the potential error:
                            file_prep = Some(f.unwrap());
                            None
                        }
                        k = reading.as_mut().unwrap() => {
                            info!("COMBI  read chunk");
                            reading = None;
                            // TODO handle the error somehow here...
                            if k.is_err() {
                                error!("READ ERROR IN COMBI");
                            }
                            let k = k.unwrap();
                            file = Some(k.0);
                            Some(k.1)
                        }
                    };
                    if fopen_avail {
                        fopen.replace(fopen3);
                    }
                    if let Some(k) = bufres {
                        yield Ok(k.freeze());
                    }
                }
                else {
                    info!("-----------------   no file open yet, await only opening of the file");
                    // TODO try to avoid this duplicated code:
                    if fopen.is_none() {
                        error!("logic  BB");
                    }
                    let fopen3 = fopen.take().unwrap();
                    let f = fopen3.await?;
                    info!("opened next file SOLO");
                    fopen_avail = false;
                    file = Some(f);
                }
            }
            else if file.is_some() {
                loop {
                    let mut buf = BytesMut::with_capacity(blen);
                    let mut file2 = file.take().unwrap();
                    let n1 = file2.read_buf(&mut buf).await?;
                    if n1 == 0 {
                        if file_prep.is_some() {
                            file.replace(file_prep.take().unwrap());
                        }
                        else {
                            info!("After read loop, next file not yet ready");
                        }
                        break;
                    }
                    else {
                        file.replace(file2);
                        yield Ok(buf.freeze());
                    }
                }
            }
            i9 += 1;
            if i9 > 100 {
                break;
            }
        }
    }
}


pub fn raw_concat_channel_read_stream_file_pipe(query: &netpod::AggQuerySingleChannel) -> impl Stream<Item=Result<BytesMut, Error>> + Send {
    let query = query.clone();
    async_stream::stream! {
        let chrx = open_files(&query);
        while let Ok(file) = chrx.recv().await {
            let mut file = match file {
                Ok(k) => k,
                Err(_) => break
            };
            loop {
                let mut buf = BytesMut::with_capacity(query.buffer_size as usize);
                use tokio::io::AsyncReadExt;
                let n1 = file.read_buf(&mut buf).await?;
                if n1 == 0 {
                    info!("file EOF");
                    break;
                }
                else {
                    yield Ok(buf);
                }
            }
        }
    }
}

fn open_files(query: &netpod::AggQuerySingleChannel) -> async_channel::Receiver<Result<tokio::fs::File, Error>> {
    let (chtx, chrx) = async_channel::bounded(2);
    let mut query = query.clone();
    tokio::spawn(async move {
        let tb0 = query.timebin;
        for i1 in 0..16 {
            query.timebin = tb0 + i1;
            let path = datapath(&query);
            let fileres = tokio::fs::OpenOptions::new()
                .read(true)
                .open(path)
                .await;
            match fileres {
                Ok(k) => {
                    match chtx.send(Ok(k)).await {
                        Ok(_) => (),
                        Err(_) => break
                    }
                }
                Err(e) => {
                    match chtx.send(Err(e.into())).await {
                        Ok(_) => (),
                        Err(_) => break
                    }
                }
            }
        }
    });
    chrx
}


pub fn file_content_stream(mut file: tokio::fs::File, buffer_size: usize) -> impl Stream<Item=Result<BytesMut, Error>> + Send {
    async_stream::stream! {
        use tokio::io::AsyncReadExt;
        loop {
            let mut buf = BytesMut::with_capacity(buffer_size);
            let n1 = file.read_buf(&mut buf).await?;
            if n1 == 0 {
                info!("file EOF");
                break;
            }
            else {
                yield Ok(buf);
            }
        }
    }
}


pub fn parsed1(query: &netpod::AggQuerySingleChannel) -> impl Stream<Item=Result<Bytes, Error>> + Send {
    let query = query.clone();
    async_stream::stream! {
        let filerx = open_files(&query);
        while let Ok(fileres) = filerx.recv().await {
            match fileres {
                Ok(file) => {
                    let inp = Box::pin(file_content_stream(file, query.buffer_size as usize));
                    let mut chunker = EventChunker::new(inp);
                    while let Some(evres) = chunker.next().await {
                        match evres {
                            Ok(evres) => {
                                let mut buf = BytesMut::with_capacity(16);

                                // TODO put some interesting information to test
                                buf.put_u64_le(0xcafecafe);

                                yield Ok(buf.freeze())
                            }
                            Err(e) => {
                                yield Err(e)
                            }
                        }
                    }
                }
                Err(e) => {
                    yield Err(e);
                }
            }
        }
    }
}


pub struct EventChunker {
    inp: NeedMinBuffer,
    had_channel: bool,
    polled: u32,
    state: DataFileState,
}

enum DataFileState {
    FileHeader,
    Event,
}

impl EventChunker {

    pub fn new(inp: Pin<Box<dyn Stream<Item=Result<BytesMut, Error>> + Send>>) -> Self {
        let mut inp = NeedMinBuffer::new(inp);
        inp.set_need_min(6);
        Self {
            inp: inp,
            had_channel: false,
            polled: 0,
            state: DataFileState::FileHeader,
        }
    }

    fn parse_buf(&mut self, buf: &mut BytesMut) -> Result<ParseResult, Error> {
        // must communicate to caller:
        // what I've found in the buffer
        // what I've consumed from the buffer
        // how many bytes I need min to make progress
        let mut ret = EventFull::dummy();
        let mut need_min = 0;
        use byteorder::{BE, ReadBytesExt};
        //info!("parse_buf  rb {}", buf.len());
        let mut i1 = 0;
        loop {
            //info!("parse_buf LOOP {}", i1);
            match self.state {
                DataFileState::FileHeader => {
                    assert!(buf.len() >= 6, "logic");
                    let mut sl = std::io::Cursor::new(buf.as_ref());
                    let fver = sl.read_i16::<BE>().unwrap();
                    assert!(fver == 0, "unexpected file version");
                    let len = sl.read_i32::<BE>().unwrap();
                    assert!(len > 0 && len < 128, "unexpected data file header");
                    let totlen = len as usize + 2;
                    if buf.len() < totlen {
                        info!("parse_buf not enough A");
                        need_min = totlen as u32;
                        break;
                    }
                    else {
                        sl.advance(len as usize - 8);
                        let len2 = sl.read_i32::<BE>().unwrap();
                        assert!(len == len2, "len mismatch");
                        let s1 = String::from_utf8(buf.as_ref()[6..(len as usize + 6 - 8)].to_vec()).unwrap();
                        info!("channel name {}  len {}  len2 {}", s1, len, len2);
                        self.state = DataFileState::Event;
                        need_min = 4;
                        buf.advance(totlen);
                    }
                }
                DataFileState::Event => {
                    assert!(buf.len() >= 4, "logic");
                    let mut sl = std::io::Cursor::new(buf.as_ref());
                    let len = sl.read_i32::<BE>().unwrap();
                    //info!("event len {}", len);
                    if (buf.len() as u32) < len as u32 {
                        // TODO gather stats about this
                        //info!("parse_buf not enough B");
                        need_min = len as u32;
                        break;
                    }
                    else {
                        let mut sl = std::io::Cursor::new(buf.as_ref());
                        let len1b = sl.read_i32::<BE>().unwrap();
                        sl.read_i64::<BE>().unwrap();
                        let ts = sl.read_i64::<BE>().unwrap();
                        let pulse = sl.read_i64::<BE>().unwrap();
                        //info!("len {}  len1b {}  ts {}  pulse {}", len, len1b, ts, pulse);
                        need_min = 4;
                        buf.advance(len as usize);
                    }
                }
            }
            i1 += 1;
        }
        Ok(ParseResult {
            events: ret,
            need_min: need_min,
        })
    }

}

struct ParseResult {
    events: EventFull,
    need_min: u32,
}

impl Stream for EventChunker {
    type Item = Result<EventFull, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polled += 1;
        if self.polled >= 20000 {
            warn!("EventChunker poll limit reached");
            return Poll::Ready(None);
        }
        let g = &mut self.inp;
        pin_mut!(g);
        match g.poll_next(cx) {
            Poll::Ready(Some(Ok(mut buf))) => {
                //info!("EventChunker got buffer  len {}", buf.len());
                match self.parse_buf(&mut buf) {
                    Ok(res) => {
                        if buf.len() > 0 {

                            // TODO gather stats about this:
                            //info!("parse_buf returned {} leftover bytes to me", buf.len());
                            self.inp.put_back(buf);
                        }
                        self.inp.set_need_min(res.need_min);
                        Poll::Ready(Some(Ok(res.events)))
                    }
                    Err(e) => Poll::Ready(Some(Err(e.into())))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

}

pub struct EventFull {

    // TODO add structures to hold list of events

}

impl EventFull {

    pub fn dummy() -> Self {
        Self {
        }
    }

}




pub struct NeedMinBuffer {
    inp: Pin<Box<dyn Stream<Item=Result<BytesMut, Error>> + Send>>,
    need_min: u32,
    left: Option<BytesMut>,
}

impl NeedMinBuffer {

    pub fn new(inp: Pin<Box<dyn Stream<Item=Result<BytesMut, Error>> + Send>>) -> Self {
        Self {
            inp: inp,
            need_min: 1,
            left: None,
        }
    }

    pub fn put_back(&mut self, buf: BytesMut) {
        assert!(self.left.is_none());
    }

    pub fn set_need_min(&mut self, need_min: u32) {
        self.need_min = need_min;
    }

}

impl Stream for NeedMinBuffer {
    type Item = Result<BytesMut, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut again = false;
            let g = &mut self.inp;
            pin_mut!(g);
            let z = match g.poll_next(cx) {
                Poll::Ready(Some(Ok(buf))) => {
                    match self.left.take() {
                        Some(mut left) => {
                            left.unsplit(buf);
                            let buf = left;
                            if buf.len() as u32 >= self.need_min {
                                Poll::Ready(Some(Ok(buf)))
                            }
                            else {
                                self.left.replace(buf);
                                again = true;
                                Poll::Pending
                            }
                        }
                        None => {
                            if buf.len() as u32 >= self.need_min {
                                Poll::Ready(Some(Ok(buf)))
                            }
                            else {
                                self.left.replace(buf);
                                again = true;
                                Poll::Pending
                            }
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
            if !again {
                break z;
            }
        }
    }

}



pub fn raw_concat_channel_read_stream(query: &netpod::AggQuerySingleChannel) -> impl Stream<Item=Result<Bytes, Error>> + Send {
    let mut query = query.clone();
    async_stream::stream! {
        let mut i1 = 0;
        loop {
            let timebin = 18700 + i1;
            query.timebin = timebin;
            let s2 = raw_concat_channel_read_stream_timebin(&query);
            pin_mut!(s2);
            while let Some(item) = s2.next().await {
                yield item;
            }
            i1 += 1;
            if i1 > 15 {
                break;
            }
        }
    }
}


pub fn raw_concat_channel_read_stream_timebin(query: &netpod::AggQuerySingleChannel) -> impl Stream<Item=Result<Bytes, Error>> {
    let query = query.clone();
    let pre = "/data/sf-databuffer/daq_swissfel";
    let path = format!("{}/{}_{}/byTime/{}/{:019}/{:010}/{:019}_00000_Data", pre, query.ksprefix, query.keyspace, query.channel.name(), query.timebin, query.split, query.tbsize);
    async_stream::stream! {
        debug!("try path: {}", path);
        let mut fin = tokio::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .await?;
        let meta = fin.metadata().await?;
        debug!("file meta {:?}", meta);
        let blen = query.buffer_size as usize;
        use tokio::io::AsyncReadExt;
        loop {
            let mut buf = BytesMut::with_capacity(blen);
            assert!(buf.is_empty());
            if false {
                buf.resize(buf.capacity(), 0);
                if buf.as_mut().len() != blen {
                    panic!("logic");
                }
            }
            let n1 = fin.read_buf(&mut buf).await?;
            if n1 == 0 {
                break;
            }
            yield Ok(buf.freeze());
        }
    }
}


fn datapath(query: &netpod::AggQuerySingleChannel) -> PathBuf {
    let pre = "/data/sf-databuffer/daq_swissfel";
    let path = format!("{}/{}_{}/byTime/{}/{:019}/{:010}/{:019}_00000_Data", pre, query.ksprefix, query.keyspace, query.channel.name(), query.timebin, query.split, query.tbsize);
    path.into()
}


pub async fn raw_concat_channel_read(query: &netpod::AggQuerySingleChannel) -> Result<netpod::BodyStream, Error> {
    let _reader = RawConcatChannelReader {
        ksprefix: query.ksprefix.clone(),
        keyspace: query.keyspace,
        channel: query.channel.clone(),
        split: query.split,
        tbsize: query.tbsize,
        buffer_size: query.buffer_size,
        tb: 18714,
        //file_reader: None,
        fopen: None,
    };
    todo!()
}

/**
Read all events from all timebins for the given channel and split.
*/
#[allow(dead_code)]
pub struct RawConcatChannelReader {
    ksprefix: String,
    keyspace: u32,
    channel: netpod::Channel,
    split: u32,
    tbsize: u32,
    buffer_size: u32,
    tb: u32,
    //file_reader: Option<FileReader>,

    // TODO
    // Not enough to store a simple future here.
    // That will only resolve to a single output.
    // • How can I transition between Stream and async world?
    // • I guess I must not poll a completed Future which comes from some async fn again after it completed.
    // • relevant crates:  async-stream, tokio-stream
    fopen: Option<Box<dyn Future<Output=Option<Result<Bytes, Error>>> + Send>>,
}

impl RawConcatChannelReader {

    pub fn read(self) -> Result<netpod::BodyStream, Error> {
        let res = netpod::BodyStream {
            inner: Box::new(self),
        };
        Ok(res)
    }

}

impl futures_core::Stream for RawConcatChannelReader {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }

}
