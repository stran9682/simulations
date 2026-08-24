use bytes::Bytes;
use iroh::{
    Endpoint, PublicKey,
    endpoint::{
        Connection,
        presets::{self},
    },
    protocol::Router,
};
use simulations::{
    rtp_sender::{send_audio, send_video},
    rtp_session_manager::RtpConnectionManager,
};
use std::{env, sync::Arc};
use std::{str::FromStr, time::Duration};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::mpsc::{self, Receiver, Sender},
};
use tokio_util::task::TaskTracker;

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

    let manager = Arc::clone(&connection_manager);

    let send_tasks = TaskTracker::new();

    let (audio_tx, audio_rx) = mpsc::channel::<Bytes>(200);
    send_tasks.spawn(send_packets(manager, audio_rx, send_audio));

    let (frame_tx, frame_rx) = mpsc::channel::<Bytes>(200);
    send_tasks.spawn(generate_video_frame(frame_tx));
    send_tasks.spawn(send_packets(connection_manager, frame_rx, send_video));

    send_tasks.wait().await;

    endpoint.close().await;

    router.shutdown().await?;

    Ok(())
}

async fn send_packets(
    connection_manager: Arc<RtpConnectionManager>,
    mut rx: Receiver<Bytes>,
    send_handler: impl AsyncFn(&Bytes, Vec<Connection>) -> (),
) {
    while let Some(bytes) = rx.recv().await {
        let connections = connection_manager.connections();

        if connections.is_empty() {
            continue;
        }

        send_handler(&bytes, connections).await;
    }
}

async fn generate_video_frame(tx: Sender<Bytes>) -> anyhow::Result<()> {
    let mut file = File::open("output.h264").await?;

    loop {
        let mut avcc_start_code: [u8; 4] = [0; 4];

        let _ = file.read_exact(&mut avcc_start_code).await?;

        let nal_unit_length = u32::from_be_bytes(avcc_start_code) as usize;

        let mut buffer = vec![0; nal_unit_length];
        file.read_buf(&mut buffer).await?;

        tx.send(Bytes::from(buffer)).await?;

        tokio::time::sleep(Duration::from_secs_f32(1.0 / 30.0)).await;
    }
}
