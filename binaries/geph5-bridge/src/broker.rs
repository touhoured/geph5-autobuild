//! HTTP transport shared by registration (IPv4 and IPv6) and statistics.
use std::{env::VarError, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use async_trait::async_trait;
use geph5_broker_protocol::BrokerClient;
use nanorpc::{JrpcRequest, JrpcResponse, RpcTransport};

const DEFAULT_BROKER_URL: &str = "https://broker.geph.io/";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub type Client = Arc<BrokerClient<HttpTransport>>;

pub fn client_from_env() -> anyhow::Result<Client> {
    let url = match std::env::var("GEPH5_BROKER_URL") {
        Ok(url) => url,
        Err(VarError::NotPresent) => DEFAULT_BROKER_URL.to_owned(),
        Err(err) => return Err(err).context("Invalid GEPH5_BROKER_URL"),
    };
    Ok(Arc::new(BrokerClient(HttpTransport::new(&url)?)))
}

pub struct HttpTransport {
    url: reqwest::Url,
    client: reqwest::Client,
}

impl HttpTransport {
    fn new(url: &str) -> anyhow::Result<Self> {
        let url = reqwest::Url::parse(url).context("Invalid broker URL")?;
        ensure!(
            matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
            "Broker URL must use HTTP or HTTPS and include a host"
        );
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(REQUEST_TIMEOUT)
            // Broker URLs identify the RPC endpoint directly. Do not forward
            // authenticated registration bodies through HTTP redirects.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { url, client })
    }
}

#[async_trait]
impl RpcTransport for HttpTransport {
    type Error = anyhow::Error;

    async fn call_raw(&self, req: JrpcRequest) -> anyhow::Result<JrpcResponse> {
        let response = self.client.post(self.url.clone()).json(&req).send().await?;
        ensure!(
            response.status().is_success(),
            "Broker HTTP status {}",
            response.status()
        );
        let response: JrpcResponse = response
            .json()
            .await
            .context("Invalid broker JSON-RPC response")?;
        ensure!(
            response.jsonrpc == "2.0" && response.id == req.id,
            "Broker JSON-RPC response version or ID mismatch"
        );
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geph5_broker_protocol::{BridgeDescriptor, Mac, StatEvent};
    use nanorpc::JrpcId;
    use serde_json::{Value, json};
    use std::io::{BufRead, BufReader, Read, Write};

    // Exercise real HTTP serialization and the generated BrokerClient without
    // registering a fake bridge in the production database.
    fn server(
        count: usize,
        handler: impl Fn(Value) -> (u16, String) + Send + 'static,
    ) -> (String, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/rpc", listener.local_addr().unwrap());
        let task = std::thread::spawn(move || {
            for _ in 0..count {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(&stream);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert_eq!(line, "POST /rpc HTTP/1.1\r\n");
                let mut length = None;
                let mut json_content = false;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        length = Some(value.trim().parse::<usize>().unwrap());
                    }
                    if lower.trim() == "content-type: application/json" {
                        json_content = true;
                    }
                }
                assert!(json_content);
                let mut body = vec![0; length.unwrap()];
                reader.read_exact(&mut body).unwrap();
                let (status, response) = handler(serde_json::from_slice(&body).unwrap());
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                );
            }
        });
        (url, task)
    }

    fn request() -> JrpcRequest {
        JrpcRequest {
            jsonrpc: "2.0".into(),
            id: JrpcId::String("test".into()),
            method: "report_stats".into(),
            params: vec![],
        }
    }

    #[test]
    fn registration_and_stats_preserve_authenticated_payloads() {
        let key = *blake3::hash(b"test-token").as_bytes();
        let (url, server) = server(2, move |req| {
            assert_eq!(req["jsonrpc"], "2.0");
            assert_eq!(req["params"].as_array().unwrap().len(), 1);
            match req["method"].as_str().unwrap() {
                "insert_bridge" => {
                    let signed: Mac<BridgeDescriptor> =
                        serde_json::from_value(req["params"][0].clone()).unwrap();
                    let desc = signed.verify(&key).unwrap();
                    assert_eq!(desc.control_listen, "[2001:db8::1]:1234".parse().unwrap());
                    assert_eq!(desc.control_cookie, "test-cookie");
                    assert_eq!(desc.pool, "test_ipv6");
                    assert_eq!(desc.expiry, 123456);
                }
                "report_stats" => {
                    let signed: Mac<Vec<StatEvent>> =
                        serde_json::from_value(req["params"][0].clone()).unwrap();
                    assert_eq!(signed.verify(&key).unwrap().len(), 1);
                }
                other => panic!("unexpected method {other}"),
            }
            (
                200,
                json!({"jsonrpc":"2.0", "id":req["id"], "result":null}).to_string(),
            )
        });
        geph5_rt::block_on(async {
            let client = BrokerClient(HttpTransport::new(&url).unwrap());
            client
                .insert_bridge(Mac::new(
                    BridgeDescriptor {
                        control_listen: "[2001:db8::1]:1234".parse().unwrap(),
                        control_cookie: "test-cookie".into(),
                        pool: "test_ipv6".into(),
                        expiry: 123456,
                    },
                    &key,
                ))
                .await
                .unwrap()
                .unwrap();
            client
                .report_stats(Mac::new(vec![StatEvent::gauge("test", &[], 1.0)], &key))
                .await
                .unwrap()
                .unwrap();
        });
        server.join().unwrap();
    }

    #[test]
    fn rejects_http_errors_bad_json_and_mismatched_ids() {
        for (status, body) in [
            (
                503,
                json!({"jsonrpc":"2.0", "id":"test", "result":null}).to_string(),
            ),
            (302, "redirect".into()),
            (200, "not JSON".into()),
            (
                200,
                json!({"jsonrpc":"2.0", "id":"wrong", "result":null}).to_string(),
            ),
        ] {
            let (url, server) = server(1, move |_| (status, body.clone()));
            geph5_rt::block_on(async {
                assert!(
                    HttpTransport::new(&url)
                        .unwrap()
                        .call_raw(request())
                        .await
                        .is_err()
                );
            });
            server.join().unwrap();
        }
    }

    #[test]
    fn preserves_rpc_errors() {
        let (url, server) = server(2, |req| {
            (
                200,
                json!({
                    "jsonrpc":"2.0", "id":req["id"],
                    "error":{"code":123, "message":"rejected", "data":"test"}
                })
                .to_string(),
            )
        });
        geph5_rt::block_on(async {
            let response = HttpTransport::new(&url)
                .unwrap()
                .call_raw(request())
                .await
                .unwrap();
            assert_eq!(response.error.unwrap().code, 123);
            assert!(response.result.is_none());
            let client = BrokerClient(HttpTransport::new(&url).unwrap());
            let error = client
                .report_stats(Mac::new(Vec::new(), &[0; 32]))
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(error.0, "test");
        });
        server.join().unwrap();
    }

    #[test]
    fn slow_response_is_bounded_by_timeout() {
        let (url, server) = server(1, |req| {
            std::thread::sleep(Duration::from_millis(200));
            (
                200,
                json!({"jsonrpc":"2.0", "id":req["id"], "result":null}).to_string(),
            )
        });
        geph5_rt::block_on(async {
            let mut transport = HttpTransport::new(&url).unwrap();
            transport.client = reqwest::Client::builder()
                .timeout(Duration::from_millis(50))
                .build()
                .unwrap();
            let err = transport.call_raw(request()).await.unwrap_err();
            assert!(err.downcast_ref::<reqwest::Error>().unwrap().is_timeout());
        });
        server.join().unwrap();
    }

    #[test]
    fn rejects_non_http_urls() {
        for url in ["127.0.0.1:18888", "ftp://example.org/", ""] {
            assert!(HttpTransport::new(url).is_err());
        }
    }
}
