use crate::errors::*;
#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
#[allow(unused_imports)]
use snafu::{NoneError, ensure};
use std::net::SocketAddr;
use http::Method;
use hyper::{Body, Request, Response, Server, Client};
use hyper::service::{make_service_fn, service_fn};
use std::pin::Pin;
use std::future::Future;
use serde_json::{Value as SerdeValue};

async fn data_api_proxy(req: Request<Body>) -> Result<Response<Body>, Error> {
  match data_api_proxy_inner(req).await {
    Ok(k) => { Ok(k) }
    Err(e) => {
      error!("{:?}", e);
      Err(e)
    }
  }
}

fn response<T>(status: T) -> http::response::Builder
where
  http::StatusCode: std::convert::TryFrom<T>,
  <http::StatusCode as std::convert::TryFrom<T>>::Error: Into<http::Error>,
{
  Response::builder().status(status)
  .header("access-control-allow-origin", "*")
  .header("access-control-allow-headers", "*")
}

async fn data_api_proxy_inner(req: Request<Body>) -> Result<Response<Body>, Error> {
  if req.method() == "OPTIONS" {
    return Ok(response(200).body(Body::empty())?);
  }
  let path = req.uri().path();
  if path == "/api/1/channels" {
    Ok(channels_list(req).await?)
  }
  else if path == "/api/1/channels/config" {
    Ok(channels_config(req).await?)
  }
  else if path == "/api/1/stats/version" {
    Ok(stats_version(req).await?)
  }
  else if path == "/" {
    Ok(Response::builder().status(307).header("Location", "/web/index.html").body(Body::empty())?)
  }
  else if path.starts_with("/web/") {
    Ok(web_content(req).await?)
  }
  else {
    //let body = format!("404 Not Found  {}  {}", req.method(), req.uri());
    Ok(Response::builder().status(404).body(Body::empty())?)
  }
}

async fn web_content(req: Request<Body>) -> Result<Response<Body>, Error> {
  let path = req.uri().path();
  if path.contains("..") || path.contains("//") {
    Ok(Response::builder().status(404).body(Body::empty())?)
  }
  else {
    use std::path::PathBuf;
    use tokio::fs::read as fsread;
    let path = &path[5..];
    let mut path_buf: PathBuf = "/opt/daqbuffer/web".into();
    path_buf.push(path);
    info!("try to open {:?}   {:?}", path_buf, path);
    let buf = fsread(&path_buf).await?;
    let body = if path.ends_with(".html") || path.ends_with(".css") || path.ends_with(".js") {
      let str = String::from_utf8(buf)?;
      use tera::{Tera, Context};
      let mut ctx = Context::new();
      ctx.insert("mark1", "Information");
      let doc = Tera::one_off(&str, &ctx, false)?;
      doc.as_bytes().to_vec()
    }
    else {
      buf
    };
    let mime = if path.ends_with(".html") { "text/html; charset=utf-8" }
    else if path.ends_with(".css") { "text/css; charset=utf-8" }
    else if path.ends_with(".js") { "text/javascript; charset=utf-8" }
    else if path.ends_with(".json") { "application/json" }
    else { "application/octet-stream" };
    let res = response(200)
    .header("Content-Type", mime)
    .body(body.into())?;
    Ok(res)
  }
}

fn get_backends() -> [(&'static str, &'static str, u16); 6] {
  [
    ("gls-archive", "gls-data-api.psi.ch", 8371),
    ("hipa-archive", "hipa-data-api.psi.ch", 8082),
    ("sf-databuffer", "sf-daqbuf-33.psi.ch", 8371),
    ("sf-imagebuffer", "sf-daq-5.psi.ch", 8371),
    ("timeout", "sf-daqbuf-33.psi.ch", 8371),
    ("error500", "sf-daqbuf-33.psi.ch", 8371),
  ]
}

type TT0 = ((&'static str, &'static str, u16), http::response::Parts, hyper::body::Bytes);
type TT1 = Result<TT0, Error>;
type TT2 = tokio::task::JoinHandle<TT1>;
type TT3 = Result<TT1, tokio::task::JoinError>;
type TT4 = Result<TT3, tokio::time::Elapsed>;
type TT7 = Pin<Box<dyn Future<Output=TT4> + Send>>;
type TT8 = (&'static str, TT7);

fn subreq(backends_req: &[&str], endp: &str, subq_maker: &dyn Fn(&str) -> SerdeValue) -> Result<Vec<TT8>, Error> {
  let backends = get_backends();
  let mut spawned = vec![];
  for back in &backends {
    if backends_req.contains(&back.0) {
      let back = back.clone();
      let q = subq_maker(back.0);
      let endp = match back.0 {
        "timeout" => "channels_timeout",
        "error500" => "channels_error500",
        _ => endp
      };
      let uri = format!("http://{}:{}{}/{}", back.1, back.2, "/api/1", endp);
      let req = Request::builder().method(Method::POST).uri(uri)
      .header("content-type", "application/json")
      .body(Body::from(serde_json::to_string(&q)?))?;
      let jh: TT2 = tokio::spawn(async move {
        let res = Client::new().request(req).await?;
        let (pre, body) = res.into_parts();
        info!("Answer from {}  status {}", back.1, pre.status);
        //info!("{:?}", pre.headers);
        //info!("{:?}", pre.version);
        //info!("{:?}", pre.status);
        let body_all = hyper::body::to_bytes(body).await?;
        info!("Got {} bytes from {}", body_all.len(), back.1);
        Ok::<_, Error>((back, pre, body_all))
      });
      let jh = tokio::time::timeout(std::time::Duration::from_millis(5000), jh);
      let bx: Pin<Box<dyn Future<Output=TT4> + Send>> = Box::pin(jh);
      spawned.push((back.0, bx));
    }
  }
  Ok(spawned)
}

//fn extr<'a, T: crate::netpod::BackendAware + crate::netpod::FromErrorCode + serde::Deserialize<'a>>(results: Vec<(&str, TT4)>) -> Vec<T> {
fn extr<T: crate::netpod::BackendAware + crate::netpod::FromErrorCode + for<'a> serde::Deserialize<'a>>(results: Vec<(&str, TT4)>) -> Vec<T> {
  let mut ret = vec![];
  for (backend, r) in results {
    if let Ok(r20) = r {
      if let Ok(r30) = r20 {
        if let Ok(r2) = r30 {
          if r2.1.status == 200 {
            let inp_res: Result<Vec<T>, _> = serde_json::from_slice(&r2.2);
            if let Ok(inp) = inp_res {
              if inp.len() > 1 {
                error!("more than one result item from {:?}", r2.0);
              }
              else {
                for inp2 in inp {
                  if inp2.backend() == r2.0.0 {
                    ret.push(inp2);
                  }
                }
              }
            }
            else {
              error!("malformed answer from {:?}", r2.0);
              ret.push(T::from_error_code(backend, crate::netpod::ErrorCode::Error));
            }
          }
          else {
            error!("bad answer from {:?}", r2.0);
            ret.push(T::from_error_code(backend, crate::netpod::ErrorCode::Error));
          }
        }
        else {
          error!("bad answer from {:?}", r30);
          ret.push(T::from_error_code(backend, crate::netpod::ErrorCode::Error));
        }
      }
      else {
        error!("subrequest join handle error {:?}", r20);
        ret.push(T::from_error_code(backend, crate::netpod::ErrorCode::Error));
      }
    }
    else {
      error!("subrequest timeout {:?}", r);
      ret.push(T::from_error_code(backend, crate::netpod::ErrorCode::Timeout));
    }
  }
  ret
}

async fn channels_list(req: Request<Body>) -> Result<Response<Body>, Error> {
  use crate::netpod::{ChannelSearchQuery, ChannelSearchResult};
  let reqbody = req.into_body();
  let bodyslice = hyper::body::to_bytes(reqbody).await?;
  let query: ChannelSearchQuery = serde_json::from_slice(&bodyslice)?;
  let subq_maker = |backend: &str| -> SerdeValue {
    serde_json::to_value(ChannelSearchQuery {
      regex: query.regex.clone(),
      source_regex: query.source_regex.clone(),
      description_regex: query.description_regex.clone(),
      backends: vec![backend.into()],
      ordering: query.ordering.clone(),
    }).unwrap()
  };
  let back2: Vec<_> = query.backends.iter().map(|x|x.as_str()).collect();
  let spawned = subreq(&back2[..], "channels", &subq_maker)?;
  let mut res = vec![];
  for (backend, s) in spawned {
    res.push((backend, s.await));
  }
  let res2 = ChannelSearchResult(extr(res));
  let body = serde_json::to_string(&res2.0)?;
  let res = response(200)
  .body(body.into())?;
  Ok(res)
}

async fn channels_config(req: Request<Body>) -> Result<Response<Body>, Error> {
  use crate::netpod::{ChannelConfigsQuery, ChannelConfigsResponse};
  let reqbody = req.into_body();
  let bodyslice = hyper::body::to_bytes(reqbody).await?;
  let query: ChannelConfigsQuery = serde_json::from_slice(&bodyslice)?;
  let subq_maker = |backend: &str| -> SerdeValue {
    serde_json::to_value(ChannelConfigsQuery {
      regex: query.regex.clone(),
      source_regex: query.source_regex.clone(),
      description_regex: query.description_regex.clone(),
      backends: vec![backend.into()],
      ordering: query.ordering.clone(),
    }).unwrap()
  };
  let back2: Vec<_> = query.backends.iter().map(|x|x.as_str()).collect();
  let spawned = subreq(&back2[..], "channels/config", &subq_maker)?;
  let mut res = vec![];
  for (backend, s) in spawned {
    res.push((backend, s.await));
  }
  let res2 = ChannelConfigsResponse(extr(res));
  let body = serde_json::to_string(&res2.0)?;
  let res = response(200)
  .body(body.into())?;
  Ok(res)
}


async fn stats_version(_req: Request<Body>) -> Result<Response<Body>, Error> {
  use serde_json::Value;
  use snafu::IntoError;
  let mut spawned = vec![];
  let mut hosts = vec![];
  for i1 in 21..34 {
    hosts.push((format!("sf-daqbuf-{}", i1), 8371));
  }
  for i1 in 5..7 {
    hosts.push((format!("sf-daq-{}", i1), 8371));
  }
  hosts.push(("hipa-data-api".to_string(), 8082));
  hosts.push(("gls-data-api".to_string(), 8371));
  for host in hosts.into_iter() {
    spawned.push((host.clone(), tokio::spawn(async move {
      let uri = format!("http://{}:{}{}", host.0, host.1, "/stats/version");
      let req = Request::builder().method(Method::GET).uri(uri)
      //.header("content-type", "application/json")
      //.body(Body::from(serde_json::to_string(&q)?))?;
      .body(Body::empty())?;
      use tokio::time::delay_for;
      use tokio::select;
      select! {
        _ = delay_for(std::time::Duration::from_millis(1000)) => {
          use snafu::{IntoError, NoneError};
          //Err(Bad{msg:format!("")}.into_error(NoneError))
          Err(SE!(Timeout).into_error(NoneError))
        }
        res = Client::new().request(req) => {
          let res = res?;
          let (pre, body) = res.into_parts();
          if pre.status != 200 {
            Err(Bad{msg:format!("API error")}.into_error(NoneError)).ctxb(SE!(AddPos))
          }
          else {
            // aggregate returns a hyper Buf which is not Read
            let body_all = hyper::body::to_bytes(body).await?;
            let ver = String::from_utf8(body_all.to_vec())?;
            Ok::<_, Error>(Value::String(ver))
          }
        }
      }
    })));
  }
  use serde_json::{Map};
  let mut m = Map::new();
  for h in spawned {
    let res = match h.1.await {
      Ok(k) => {
        match k {
          Ok(k) => k,
          Err(_e) => Value::String(format!("ERROR"))
        }
      }
      Err(_e) => Value::String(format!("ERROR"))
    };
    m.insert(format!("{}:{}", h.0.0, h.0.1), res);
  }
  let res = response(200)
  .header("Content-Type", "application/json")
  .body(serde_json::to_string(&m)?.into())?;
  return Ok(res)
}


pub async fn data_api() -> Result<(), Error> {
  let addr = SocketAddr::from(([0, 0, 0, 0], 8371));
  let make_service = make_service_fn(|_conn| async {
    Ok::<_, Error>(service_fn(data_api_proxy))
  });
  Server::bind(&addr).serve(make_service).await?;
  Ok(())
}


async fn proxy_lib_service(req: Request<Body>) -> Result<Response<Body>, Error> {
  let path = req.uri().path();
  ensure!(!path.contains(".."), Bad{msg:format!("bad path {:?}", path)});
  ensure!(!path.contains("//"), Bad{msg:format!("bad path {:?}", path)});
  let path_prefix = "/api/__testing/lib/";
  if req.uri().path().starts_with(path_prefix) {
    match tokio::fs::read(format!("/opt/daqbuffer/lib/{}", &path[path_prefix.len()..])).await {
      Ok(buf) => {
        let body = hyper::Body::from(buf);
        Ok(response(200).body(body)?)
      }
      Err(_) => {
        Ok(response(500).body(Body::empty())?)
      }
    }
  }
  else {
    Ok(response(500).body(Body::empty())?)
  }
}

pub async fn proxy_lib() -> Result<(), Error> {
  let addr = SocketAddr::from(([0, 0, 0, 0], 8372));
  let make_service = make_service_fn(|_conn| async {
    Ok::<_, Error>(service_fn(proxy_lib_service))
  });
  Server::bind(&addr).serve(make_service).await?;
  Ok(())
}
