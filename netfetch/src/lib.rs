use async_channel::{bounded, Receiver};
use bytes::{BufMut, BytesMut};
use err::Error;
use futures_util::FutureExt;
use netpod::NodeConfigCached;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    cmd: u16,
    payload_len: u16,
    type_type: u16,
    data_len: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FetchItem {
    Log(String),
    Message(Message),
}

#[cfg(test)]
mod test {
    use futures_util::StreamExt;
    use netpod::log::*;
    use netpod::{Cluster, Database, Node, NodeConfig, NodeConfigCached};
    use std::collections::BTreeMap;
    use std::iter::FromIterator;

    #[test]
    fn ca_connect_1() {
        taskrun::run(async {
            let it = vec![(String::new(), String::new())].into_iter();
            let pairs = BTreeMap::from_iter(it);
            let node_config = NodeConfigCached {
                node: Node {
                    host: "".into(),
                    bin_grain_kind: 0,
                    port: 123,
                    port_raw: 123,
                    backend: "".into(),
                    split: 0,
                    data_base_path: "".into(),
                    listen: "".into(),
                    ksprefix: "".into(),
                },
                node_config: NodeConfig {
                    name: "".into(),
                    cluster: Cluster {
                        nodes: vec![],
                        database: Database {
                            host: "".into(),
                            name: "".into(),
                            user: "".into(),
                            pass: "".into(),
                        },
                    },
                },
                ix: 0,
            };
            let mut rx = super::ca_connect_1(pairs, &node_config).await?;
            while let Some(item) = rx.next().await {
                info!("got next: {:?}", item);
            }
            Ok(())
        })
        .unwrap();
    }
}

pub async fn ca_connect_1(
    _pairs: BTreeMap<String, String>,
    _node_config: &NodeConfigCached,
) -> Result<Receiver<Result<FetchItem, Error>>, Error> {
    let (tx, rx) = bounded(16);
    let tx2 = tx.clone();
    tokio::task::spawn(
        async move {
            let mut conn = tokio::net::TcpStream::connect("S30CB06-CVME-LLRF2.psi.ch:5064").await?;
            let (mut inp, mut out) = conn.split();
            tx.send(Ok(FetchItem::Log(format!("connected")))).await?;
            let mut buf = [0; 64];

            let mut b2 = BytesMut::with_capacity(128);
            b2.put_u16(0x00);
            b2.put_u16(0);
            b2.put_u16(0);
            b2.put_u16(0xb);
            b2.put_u32(0);
            b2.put_u32(0);
            out.write_all(&b2).await?;
            tx.send(Ok(FetchItem::Log(format!("written")))).await?;
            let n1 = inp.read(&mut buf).await?;
            tx.send(Ok(FetchItem::Log(format!("received: {} {:?}", n1, buf))))
                .await?;

            // Search to get cid:
            let chn = b"SATCB01-DBPM220:Y2";
            b2.clear();
            b2.put_u16(0x06);
            b2.put_u16((16 + chn.len()) as u16);
            b2.put_u16(0x00);
            b2.put_u16(0x0b);
            b2.put_u32(0x71803472);
            b2.put_u32(0x71803472);
            b2.put_slice(chn);
            out.write_all(&b2).await?;
            tx.send(Ok(FetchItem::Log(format!("written")))).await?;
            let n1 = inp.read(&mut buf).await?;
            tx.send(Ok(FetchItem::Log(format!("received: {} {:?}", n1, buf))))
                .await?;

            Ok::<_, Error>(())
        }
        .then({
            move |item| async move {
                match item {
                    Ok(_) => {}
                    Err(e) => {
                        tx2.send(Ok(FetchItem::Log(format!("Seeing error: {:?}", e)))).await?;
                    }
                }
                Ok::<_, Error>(())
            }
        }),
    );
    Ok(rx)
}
