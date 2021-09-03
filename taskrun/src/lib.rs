use crate::log::*;
use err::Error;
use std::future::Future;
use std::panic;
use std::sync::Mutex;
use tokio::task::JoinHandle;

pub mod log {
    #[allow(unused_imports)]
    pub use tracing::{debug, error, info, trace, warn};
}

pub fn run<T, F: std::future::Future<Output = Result<T, Error>>>(f: F) -> Result<T, Error> {
    tracing_init();
    let res = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(12)
        .max_blocking_threads(256)
        .enable_all()
        .on_thread_start(|| {
            let _old = panic::take_hook();
            panic::set_hook(Box::new(move |info| {
                let payload = if let Some(k) = info.payload().downcast_ref::<Error>() {
                    format!("{:?}", k)
                }
                else if let Some(k) = info.payload().downcast_ref::<String>() {
                    k.into()
                }
                else if let Some(&k) = info.payload().downcast_ref::<&str>() {
                    k.into()
                }
                else {
                    format!("unknown payload type")
                };
                error!(
                    "✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗     panicking\n{:?}\nLOCATION: {:?}\nPAYLOAD: {:?}\ninfo object: {:?}\nerr: {:?}",
                    Error::with_msg("catched panic in taskrun::run"),
                    info.location(),
                    info.payload(),
                    info,
                    payload,
                );
                //old(info);
            }));
        })
        .build()
        .unwrap()
        .block_on(async { f.await });
    match res {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("{:?}", e);
            Err(e)
        }
    }
}

lazy_static::lazy_static! {
    pub static ref INITMX: Mutex<u32> = Mutex::new(0);
}

pub fn tracing_init() {
    let mut g = INITMX.lock().unwrap();
    if *g == 0 {
        tracing_subscriber::fmt()
            //.with_timer(tracing_subscriber::fmt::time::uptime())
            .with_target(true)
            .with_thread_names(true)
            //.with_max_level(tracing::Level::INFO)
            .with_env_filter(tracing_subscriber::EnvFilter::new(
                "info,daqbuffer=trace,daqbuffer::test=trace,disk::raw::conn=info",
            ))
            .init();
        *g = 1;
    }
}

pub fn spawn<T>(task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(task)
}

pub fn test_cluster() -> netpod::Cluster {
    let nodes = (0..3)
        .into_iter()
        .map(|id| netpod::Node {
            host: "localhost".into(),
            listen: "0.0.0.0".into(),
            port: 8360 + id as u16,
            port_raw: 8360 + id as u16 + 100,
            data_base_path: format!("tmpdata/node{:02}", id).into(),
            cache_base_path: format!("tmpdata/node{:02}", id).into(),
            ksprefix: "ks".into(),
            split: id,
            backend: "testbackend".into(),
            archiver_appliance: None,
        })
        .collect();
    netpod::Cluster {
        nodes: nodes,
        database: netpod::Database {
            name: "daqbuffer".into(),
            host: "localhost".into(),
            user: "daqbuffer".into(),
            pass: "daqbuffer".into(),
        },
    }
}
