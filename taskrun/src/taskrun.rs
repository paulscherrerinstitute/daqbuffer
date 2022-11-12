pub mod append;

use crate::log::*;
use err::Error;
use std::future::Future;
use std::panic;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

pub mod log {
    #[allow(unused_imports)]
    pub use tracing::{debug, error, info, trace, warn};
}

lazy_static::lazy_static! {
    static ref RUNTIME: Mutex<Option<Arc<Runtime>>> = Mutex::new(None);
}

pub fn get_runtime() -> Arc<Runtime> {
    get_runtime_opts(24, 128)
}

pub fn get_runtime_opts(nworkers: usize, nblocking: usize) -> Arc<Runtime> {
    let mut g = RUNTIME.lock().unwrap();
    match g.as_ref() {
        None => {
            tracing_init();
            let res = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(nworkers)
                .max_blocking_threads(nblocking)
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
                .unwrap();
            let a = Arc::new(res);
            *g = Some(a.clone());
            a
        }
        Some(g) => g.clone(),
    }
}

pub fn run<T, F>(f: F) -> Result<T, Error>
where
    F: std::future::Future<Output = Result<T, Error>>,
{
    let runtime = get_runtime();
    let res = runtime.block_on(async { f.await });
    match res {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("Catched: {:?}", e);
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
        //use tracing_subscriber::fmt::time::FormatTime;
        let fmtstr = "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z";
        //let format = tracing_subscriber::fmt::format().with_timer(timer);
        let timer = tracing_subscriber::fmt::time::UtcTime::new(time::format_description::parse(fmtstr).unwrap());
        //use tracing_subscriber::prelude::*;
        //let trsub = tracing_subscriber::fmt::layer();
        //let console_layer = console_subscriber::spawn();
        //tracing_subscriber::registry().with(console_layer).with(trsub).init();
        //console_subscriber::init();
        #[allow(unused)]
        let log_filter = tracing_subscriber::EnvFilter::new(
            [
                //"tokio=trace",
                //"runtime=trace",
                "warn",
                "disk::binned::pbv=trace",
                "[log_span_d]=debug",
                "[log_span_t]=trace",
            ]
            .join(","),
        );
        tracing_subscriber::fmt()
            .with_timer(timer)
            .with_target(true)
            .with_thread_names(true)
            //.with_max_level(tracing::Level::INFO)
            //.with_env_filter(log_filter)
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
        *g = 1;
        //warn!("tracing_init  done");
    }
}

pub fn spawn<T>(task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(task)
}
