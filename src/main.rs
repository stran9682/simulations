use bytes::Bytes;
use iroh::{
    Endpoint, PublicKey,
    endpoint::presets::{self},
    protocol::Router,
};
use simulations::{
    rtp_packet_header::RTPSession,
    rtp_sender::{PacketType, send_packets},
    rtp_session_manager::RtpConnectionManager,
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

    let manager = Arc::clone(&connection_manager);

    let send_tasks = TaskTracker::new();
    let clock = Instant::now();

    let (audio_tx, audio_rx) = mpsc::channel::<(Bytes, u32)>(200);
    let rtp_audio_session = RTPSession::new(connection_manager.audio_ssrc());
    let token = CancellationToken::new();
    let sender_token = token.child_token();

    send_tasks.spawn(async move {
        tokio::select! {
            _ = send_packets(manager, audio_rx, rtp_audio_session, PacketType::Audio) => (),
            _ = sender_token.cancelled() => { return; }
        }
    });

    send_tasks.spawn(async move {
        generate_audio_sample(audio_tx, clock).await.ok();

        token.cancel();
    });


    let (frame_tx, frame_rx) = mpsc::channel::<(Bytes, u32)>(200);
    let rtp_video_session = RTPSession::new(connection_manager.video_ssrc());
    let token = CancellationToken::new();
    let sender_token = token.child_token();

    send_tasks.spawn(async move {
        tokio::select! {
            _ = sender_token.cancelled() => {
                return
            }
            _ = send_packets(connection_manager, frame_rx, rtp_video_session, PacketType::Video) => ()
        }
    });
    send_tasks.spawn(async move {
        generate_video_frame(frame_tx, clock).await.ok();

        token.cancel();
    });

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

        if bytes_read == 0 { return Ok(()) }

        let elapsed = (clock.elapsed().as_secs() * 90_000) as u32;

        tx.send((Bytes::copy_from_slice(&buffer[..bytes_read]), elapsed)).await?;

        tokio::time::sleep(Duration::from_secs_f32(1.0 / 30.0)).await;
    }
}

async fn generate_audio_sample(tx: Sender<(Bytes, u32)>, clock: Instant) -> anyhow::Result<()> {
    todo!();
}
