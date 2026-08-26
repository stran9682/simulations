use bytes::Bytes;
use iroh::{
    Endpoint, PublicKey,
    endpoint::presets::{self},
    protocol::Router,
};
use simulations::{
    rtp::rtp_sender::{PacketType, send_packets},
    rtp_connection_manager::{RtpConnectionManager, SessionInfo},
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

    println!("endpoint: {}", endpoint.id());

    let connection_manager = Arc::new(RtpConnectionManager::new());

    let router = Router::builder(endpoint.clone())
        .accept(ALPN, Arc::clone(&connection_manager))
        .spawn();

    let args: Vec<String> = env::args().collect();

    let send_tasks = TaskTracker::new();

    // Audio
    let manager = Arc::clone(&connection_manager);
    start_send_tasks(&send_tasks, manager, PacketType::Audio);

    // Video
    let manager = Arc::clone(&connection_manager);
    start_send_tasks(&send_tasks, manager, PacketType::Video);

    if args.len() > 1 {
        let peers = connect(&endpoint, &args[1], &connection_manager).await?;

        for peer in peers {
            connect(&endpoint, &peer, &connection_manager).await?;
        }
    }

    send_tasks.wait().await;

    endpoint.close().await;

    router.shutdown().await?;

    Ok(())
}

async fn generate_video_frame(tx: Sender<(Bytes, u32)>, clock: Instant) -> anyhow::Result<()> {
    loop {
        let mut file = File::open("output.h264").await?;

        loop {
            let mut avcc_start_code: [u8; 4] = [0; 4];

            if file.read_exact(&mut avcc_start_code).await.is_err() {
                break;
            }

            let nal_unit_length = u32::from_be_bytes(avcc_start_code) as usize;

            let mut buffer = vec![0; nal_unit_length];
            let bytes_read = file.read_buf(&mut buffer).await?;

            if bytes_read == 0 {
                break;
            }

            let elapsed = (clock.elapsed().as_secs() * 90_000) as u32;

            tx.send((Bytes::copy_from_slice(&buffer[..bytes_read]), elapsed))
                .await?;

            tokio::time::sleep(Duration::from_secs_f32(1.0 / 30.0)).await;
        }
    }
}

async fn generate_audio_sample(tx: Sender<(Bytes, u32)>, clock: Instant) -> anyhow::Result<()> {
    loop {
        let mut file = File::open("output.opus").await?;
        let mut opus_data = Vec::new();
        file.read_to_end(&mut opus_data).await?;

        let packets = parse_ogg_opus_packets(&opus_data)?;

        for packet in packets {
            let elapsed = (clock.elapsed().as_secs() * 48_000) as u32;

            tx.send((Bytes::copy_from_slice(&packet), elapsed)).await?;

            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

fn parse_ogg_opus_packets(file: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut offset = 0;
    let mut packets = Vec::new();

    while offset + 27 < file.len() {
        if &file[offset..offset + 4] != b"OggS" {
            break;
        }

        let page_segments = file[offset + 26] as usize;
        let segment_table_start = offset + 27;
        let segment_table_end = segment_table_start + page_segments;

        if segment_table_end > file.len() {
            break;
        }

        let mut packet_start = segment_table_end;
        for segment_size in &file[segment_table_start..segment_table_end] {
            let size = *segment_size as usize;
            let packet_end = packet_start + size;

            if packet_end > file.len() {
                break;
            }

            let packet = &file[packet_start..packet_end];
            if !packet.starts_with(b"OpusHead") && !packet.starts_with(b"OpusTags") {
                packets.push(packet.to_vec());
            }

            packet_start = packet_end;
        }

        offset = packet_start;
    }

    Ok(packets)
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

async fn connect(
    endpoint: &Endpoint,
    remote_id: &str,
    connection_manager: &Arc<RtpConnectionManager>,
) -> anyhow::Result<Vec<String>> {
    let remote_endpoint = PublicKey::from_str(remote_id)?;

    let conn = endpoint.connect(remote_endpoint, ALPN).await?;

    let (mut send, mut recv) = conn.open_bi().await?;

    let request = connection_manager.session_info();
    let request_bytes = serde_json::to_vec(&request)?;

    send.write_all(&request_bytes).await?;
    send.finish()?;

    let bytes = recv.read_to_end(1000).await?;

    let response: SessionInfo = serde_json::from_slice(&bytes)?;

    connection_manager.add_connection(conn.clone());

    connection_manager.spawn_receivers(conn, &response).await;

    Ok(response.peers)
}
