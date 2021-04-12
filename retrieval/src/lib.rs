#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use tokio::task::JoinHandle;
use netpod::{Node, Cluster};
use hyper::Body;

pub mod cli;

#[test]
fn get_cached_0() {
    taskrun::run(get_cached_0_inner()).unwrap();
}

#[cfg(test)]
async fn get_cached_0_inner() -> Result<(), Error> {
    let t1 = chrono::Utc::now();
    let cluster = test_cluster();
    let node0 = &cluster.nodes[0];
    let hosts = spawn_test_hosts(&cluster);
    let req = hyper::Request::builder()
    .method(http::Method::GET)
    .uri(format!("http://{}:{}/api/1/binned?beg_date=1970-01-01T00:00:01.4253Z&end_date=1970-01-01T00:00:04.000Z", node0.host, node0.port))
    .body(Body::empty())?;
    let client = hyper::Client::new();
    let res = client.request(req).await?;
    info!("client response {:?}", res);
    let mut res_body = res.into_body();
    use hyper::body::HttpBody;
    let mut ntot = 0 as u64;
    loop {
        match res_body.data().await {
            Some(Ok(k)) => {
                //info!("packet..  len {}", k.len());
                ntot += k.len() as u64;
            }
            Some(Err(e)) => {
                error!("{:?}", e);
            }
            None => {
                info!("response stream exhausted");
                break;
            }
        }
    }
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    let throughput = ntot / 1024 * 1000 / ms;
    info!("get_cached_0 DONE  total download {} MB   throughput {:5} kB/s", ntot / 1024 / 1024, throughput);
    //Err::<(), _>(format!("test error").into())
    Ok(())
}


fn test_cluster() -> Cluster {
    let nodes = (0..1).into_iter().map(|k| {
        Node {
            host: "localhost".into(),
            port: 8360 + k,
            data_base_path: format!("../tmpdata/node{:02}", k).into(),
            ksprefix: "ks".into(),
            split: 0,
        }
    })
    .collect();
    Cluster {
        nodes: nodes,
    }
}

fn spawn_test_hosts(cluster: &Cluster) -> Vec<JoinHandle<Result<(), Error>>> {
    let mut ret = vec![];
    for node in &cluster.nodes {
        let h = tokio::spawn(httpret::host(node.clone(), cluster.clone()));
        ret.push(h);
    }
    ret
}
