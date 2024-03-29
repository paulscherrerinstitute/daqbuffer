use crate::bodystream::response;
use crate::bodystream::ToPublicResponse;
use crate::channelconfig::ch_conf_from_binned;
use crate::err::Error;
use crate::response_err;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use query::api4::binned::BinnedQuery;
use tracing::Instrument;
use url::Url;

async fn binned_json(url: Url, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    debug!("{:?}", req);
    let reqid = crate::status_board()
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?
        .new_status_id();
    let (_head, _body) = req.into_parts();
    let query = BinnedQuery::from_url(&url).map_err(|e| {
        error!("binned_json: {e:?}");
        let msg = format!("can not parse query: {}", e.msg());
        e.add_public_msg(msg)
    })?;
    // TODO handle None case better and return 404
    let ch_conf = ch_conf_from_binned(&query, node_config)
        .await?
        .ok_or_else(|| Error::with_msg_no_trace("channel not found"))?;
    let span1 = span!(
        Level::INFO,
        "httpret::binned",
        reqid,
        beg = query.range().beg_u64() / SEC,
        end = query.range().end_u64() / SEC,
        ch = query.channel().name().clone(),
    );
    span1.in_scope(|| {
        debug!("begin");
    });
    let item = streams::timebinnedjson::timebinned_json(query, ch_conf, reqid, node_config.node_config.cluster.clone())
        .instrument(span1)
        .await?;
    let buf = serde_json::to_vec(&item)?;
    let ret = response(StatusCode::OK).body(Body::from(buf))?;
    Ok(ret)
}

async fn binned(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let url = {
        let s1 = format!("dummy:{}", req.uri());
        Url::parse(&s1)
            .map_err(Error::from)
            .map_err(|e| e.add_public_msg(format!("Can not parse query url")))?
    };
    if req
        .uri()
        .path_and_query()
        .map_or(false, |x| x.as_str().contains("DOERR"))
    {
        Err(Error::with_msg_no_trace("hidden message").add_public_msg("PublicMessage"))?;
    }
    if crate::accepts_json(&req.headers()) {
        Ok(binned_json(url, req, node_config).await?)
    } else if crate::accepts_octets(&req.headers()) {
        Ok(response_err(
            StatusCode::NOT_ACCEPTABLE,
            format!("binary binned data not yet available"),
        )?)
    } else {
        let ret = response_err(
            StatusCode::NOT_ACCEPTABLE,
            format!("Unsupported Accept: {:?}", req.headers()),
        )?;
        Ok(ret)
    }
}

pub struct BinnedHandler {}

impl BinnedHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/binned" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        match binned(req, node_config).await {
            Ok(ret) => Ok(ret),
            Err(e) => {
                warn!("BinnedHandler handle sees: {e}");
                Ok(e.to_public_response())
            }
        }
    }
}
