use crate::response;
use err::Error;
use hyper::{Body, Request, Response, StatusCode};
use netpod::{ChannelSearchQuery, NodeConfigCached};

pub async fn channel_search(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    match head.headers.get("accept") {
        Some(v) if v == "application/json" => {
            let query = ChannelSearchQuery::from_request(head.uri.query())?;
            let res = dbconn::search::search_channel(query, node_config).await?;
            let body = Body::from(serde_json::to_string(&res)?);
            let ret = super::response(StatusCode::OK).body(body)?;
            Ok(ret)
        }
        _ => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}
