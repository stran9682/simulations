use iroh::{
    Endpoint, PublicKey,
    endpoint::presets::{self},
    protocol::Router,
};
use simulations::rtp_session_manager::RtpConnectionManager;
use tokio_util::task::TaskTracker;
use std::{env, sync::Arc};
use std::str::FromStr;

static ALPN: &[u8] = b"benchmark";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;

    println!("endpoint: {}", endpoint.id().to_string());

    let connection_manager = Arc::new(RtpConnectionManager::new());

    let router = Router::builder(endpoint.clone())
        .accept(ALPN, Arc::clone(&connection_manager))
        .spawn();

    let args: Vec<String> = env::args().collect();

    if args.len() > 1 {
        let remote_endpoint = PublicKey::from_str(&args[1])?;

        let conn = endpoint.connect(remote_endpoint, ALPN).await?;

        let (mut send, mut recv) = conn.open_bi().await?;

        todo!("Implement starting a connection")
    }

    let connection_manager = Arc::clone(&connection_manager);

    let send_tasks = TaskTracker::new();
    send_tasks.spawn(send_packets(connection_manager, send_audio));

    send_tasks.wait().await;

    endpoint.close().await;

    router.shutdown().await?;

    Ok(())
}

async fn send_packets(connection_manager: Arc<RtpConnectionManager>, send_handler: impl AsyncFn() -> ())
{
    loop {
        let connections = connection_manager.connections();

        if connections.is_empty() {
            continue;
        }

        for _ in connections {
            send_handler().await;
        }
    }
}

async fn send_audio () {
    todo!()
}
