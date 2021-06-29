use tokio::task::JoinHandle;

use err::Error;
use netpod::{Cluster, NodeConfig, NodeConfigCached, ProxyConfig};

pub mod client;
pub mod nodes;
#[cfg(test)]
pub mod test;

pub fn spawn_test_hosts(cluster: Cluster) -> Vec<JoinHandle<Result<(), Error>>> {
    let mut ret = vec![];
    for node in &cluster.nodes {
        let node_config = NodeConfig {
            cluster: cluster.clone(),
            name: format!("{}:{}", node.host, node.port),
        };
        let node_config: Result<NodeConfigCached, Error> = node_config.into();
        let node_config = node_config.unwrap();
        let h = tokio::spawn(httpret::host(node_config));
        ret.push(h);
    }
    ret
}

pub async fn run_node(node_config: NodeConfigCached) -> Result<(), Error> {
    httpret::host(node_config).await?;
    Ok(())
}

pub async fn run_proxy(proxy_config: ProxyConfig) -> Result<(), Error> {
    httpret::proxy::proxy(proxy_config).await?;
    Ok(())
}
