use std::{str::FromStr};
use std::env;
use iroh::{Endpoint, PublicKey, endpoint::presets::{self}, protocol::{ProtocolHandler, Router}};

static ALPN: &[u8] = b"benchmark";

#[tokio::main]
async fn main() {
    let endpoint = Endpoint::bind(presets::N0).await.expect("Failed to create endpoint");
    endpoint.online().await;

    println!("endpoint: {}", endpoint.id().to_string());

    let router = Router::builder(endpoint.clone())
        .accept(ALPN, Echo)
        .spawn();

    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let remote_endpoint = PublicKey::from_str(&args[1]).expect("Key was invalid");

        let conn = endpoint.connect(remote_endpoint, ALPN).await.expect("Failed to connect");

        let (mut send, mut recv) = conn.open_bi().await.expect("Failed to open connection");

        send.write_all(b"Hello, world!").await.expect("Failed to send bytes");

        send.finish().expect("Failed to finish connection");

        let response = recv.read_to_end(1000).await.expect("Failed to read bytes");

        assert_eq!(&response, b"Hello, world!");

        conn.close(0u32.into(), b"bye!");

        endpoint.close().await;
    }

    tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl-C");

    router.shutdown().await.expect("Failed to shutdown");
}

#[derive(Debug)]
pub struct Echo;

impl ProtocolHandler for Echo {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let endpoint_id = connection.remote_id();
        println!("accepted connection from {endpoint_id}");

        let (mut send, mut recv) = connection.accept_bi().await?;

        let bytes_sent = tokio::io::copy(&mut recv, &mut send).await?;
        println!("Copied over {bytes_sent} byte(s)");

        send.finish()?;

        connection.closed().await;

        Result::Ok(())
    }
}