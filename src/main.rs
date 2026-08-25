use bytes::Bytes;
use iroh::{
    Endpoint, PublicKey,
    endpoint::presets::{self},
    protocol::Router,
};
use simulations::{
    rtp::rtp_sender::{PacketType, send_packets},
    rtp_connection_manager::RtpConnectionManager,
};

use std::{env, sync::Arc, time::Instant};
use std::{str::FromStr, time::Duration};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::mpsc::{self, Sender},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

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

    let send_tasks = TaskTracker::new();

    // Audio
    let manager = Arc::clone(&connection_manager);
    start_send_tasks(&send_tasks, manager, PacketType::Audio);

    // Video
    start_send_tasks(&send_tasks, connection_manager, PacketType::Video);

    send_tasks.wait().await;

    endpoint.close().await;

    router.shutdown().await?;

    Ok(())
}

async fn generate_video_frame(tx: Sender<(Bytes, u32)>, clock: Instant) -> anyhow::Result<()> {
    let mut file = File::open("output.h264").await?;

    loop {
        let mut avcc_start_code: [u8; 4] = [0; 4];

        let _ = file.read_exact(&mut avcc_start_code).await?;

        let nal_unit_length = u32::from_be_bytes(avcc_start_code) as usize;

        let mut buffer = vec![0; nal_unit_length];
        let bytes_read = file.read_buf(&mut buffer).await?;

        if bytes_read == 0 {
            return Ok(());
        }

        let elapsed = (clock.elapsed().as_secs() * 90_000) as u32;

        tx.send((Bytes::copy_from_slice(&buffer[..bytes_read]), elapsed))
            .await?;

        tokio::time::sleep(Duration::from_secs_f32(1.0 / 30.0)).await;
    }
}

async fn generate_audio_sample(tx: Sender<(Bytes, u32)>, clock: Instant) -> anyhow::Result<()> {
    todo!();
}

fn start_send_tasks(
    send_task_tracker: &TaskTracker,
    connection_manager: Arc<RtpConnectionManager>,
    packet_type: PacketType,
) {
    let (tx, rx) = mpsc::channel::<(Bytes, u32)>(200);
    let token = CancellationToken::new();
    let sender_token = token.child_token();
    let clock = connection_manager.clock;

    send_task_tracker.spawn(async move {
        tokio::select! {
            _ = sender_token.cancelled() => { return; }
            _ = send_packets(connection_manager, rx, packet_type) => (),
        }
    });

    send_task_tracker.spawn(async move {
        let _ = match packet_type {
            PacketType::Audio => generate_audio_sample(tx, clock).await.ok(),
            PacketType::Video => generate_video_frame(tx, clock).await.ok(),
        };
        token.cancel();
    });
}
