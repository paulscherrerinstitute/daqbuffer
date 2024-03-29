use crate::channelconfig::chconf_from_events_quorum;
use crate::err::Error;
use crate::response;
use crate::response_err;
use crate::ToPublicResponse;
use futures_util::stream;
use futures_util::TryStreamExt;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use netpod::APP_OCTET;
use query::api4::events::PlainEventsQuery;
use url::Url;

pub struct EventsHandler {}

impl EventsHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/events" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        match plain_events(req, node_config).await {
            Ok(ret) => Ok(ret),
            Err(e) => {
                error!("EventsHandler sees: {e}");
                Ok(e.to_public_response())
            }
        }
    }
}

async fn plain_events(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let accept_def = APP_JSON;
    let accept = req
        .headers()
        .get(http::header::ACCEPT)
        .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
    let url = {
        let s1 = format!("dummy:{}", req.uri());
        Url::parse(&s1)
            .map_err(Error::from)
            .map_err(|e| e.add_public_msg(format!("Can not parse query url")))?
    };
    if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
        Ok(plain_events_json(url, req, node_config).await?)
    } else if accept == APP_OCTET {
        Ok(plain_events_binary(url, req, node_config).await?)
    } else {
        let ret = response_err(StatusCode::NOT_ACCEPTABLE, format!("Unsupported Accept: {:?}", accept))?;
        Ok(ret)
    }
}

async fn plain_events_binary(
    url: Url,
    req: Request<Body>,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, Error> {
    debug!("{:?}", req);
    let query = PlainEventsQuery::from_url(&url).map_err(|e| e.add_public_msg(format!("Can not understand query")))?;
    let ch_conf = chconf_from_events_quorum(&query, node_config).await?;
    info!("plain_events_binary  chconf_from_events_quorum: {ch_conf:?}");
    let s = stream::iter([Ok::<_, Error>(String::from("TODO_PREBINNED_BINARY_STREAM"))]);
    let ret = response(StatusCode::OK).body(Body::wrap_stream(s.map_err(Error::from)))?;
    Ok(ret)
}

async fn plain_events_json(
    url: Url,
    req: Request<Body>,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, Error> {
    let reqid = crate::status_board()?.new_status_id();
    info!("plain_events_json  req: {:?}", req);
    let (_head, _body) = req.into_parts();
    let query = PlainEventsQuery::from_url(&url)?;
    info!("plain_events_json  query {query:?}");
    // TODO handle None case better and return 404
    let ch_conf = chconf_from_events_quorum(&query, node_config)
        .await
        .map_err(Error::from)?
        .ok_or_else(|| Error::with_msg_no_trace("channel not found"))?;
    info!("plain_events_json  chconf_from_events_quorum: {ch_conf:?}");
    let item =
        streams::plaineventsjson::plain_events_json(&query, ch_conf, reqid, &node_config.node_config.cluster).await;
    let item = match item {
        Ok(item) => item,
        Err(e) => {
            error!("got error from streams::plaineventsjson::plain_events_json {e:?}");
            return Err(e.into());
        }
    };
    let buf = serde_json::to_vec(&item)?;
    let ret = response(StatusCode::OK).body(Body::from(buf))?;
    Ok(ret)
}
