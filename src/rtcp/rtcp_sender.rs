use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use bytes::{BufMut, BytesMut};
use iroh::endpoint::Connection;
use rand::RngExt;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::{
    rtcp::{
        rtcp_packet_header::{PacketType, RTCPHeader},
        sender_report::SenderReport,
    },
    rtp::{rtp_packet_header::RTPSession, rtp_receiver::Peer},
};

pub async fn rtcp_sender(
    connection: Connection,
    rtp_session: Arc<RTPSession>,
    clock: Instant,
    clock_rate: u64,
    peer: Arc<Mutex<Peer>>,
    cancellation_token: CancellationToken,
) {
    let mut first_packet = true;

    tokio::select! {
        _ = cancellation_token.cancelled() => return,
        _ = { async move {
                loop {
                    let mut interval = 5.0;

                    interval = {
                        let mut rng = rand::rng();
                        rng.random_range(0.5..=1.5) * interval
                    };

                    if first_packet {
                        interval *= 0.5;
                        first_packet = false;
                    }

                    sleep(Duration::from_secs_f64(interval)).await;

                    let now = SystemTime::now();
                    let time_since_epoch = now.duration_since(SystemTime::UNIX_EPOCH).unwrap();

                    let seconds = time_since_epoch.as_secs() + 2_208_988_800;
                    let fraction =
                        ((time_since_epoch.subsec_micros() + 1) as f64 * (1u64 << 32) as f64 * 1.0e-6) as u32;
                    let ntp = seconds << 32 | (fraction as u64);

                    let sender_report = SenderReport {
                        ssrc: rtp_session.ssrc,
                        ntp_time: ntp,
                        rtp_time: (clock.elapsed().as_secs() * clock_rate) as u32,
                        packet_count: rtp_session.get_num_packets_generated(),
                        octet_count: rtp_session.get_num_octets_sent(),
                        reports: peer
                            .lock()
                            .map_or_else(|_| vec![], |p| vec![p.reception_report()]),
                    };

                    let header = RTCPHeader {
                        padding: false,
                        packet_type: PacketType::SenderReport,
                        count: sender_report.reports.len() as u8,
                        length: sender_report.length(),
                    };

                    let mut packet = BytesMut::with_capacity(4 + sender_report.length() as usize);
                    packet.put(header.serialize());
                    packet.put(sender_report.serialize());

                    let packet = packet.freeze();

                    match connection.send_datagram_wait(packet.clone()).await {
                        Ok(_) => {}
                        Err(e) => eprintln!("Failed to send RTCP to {}: {}", connection.remote_id(), e),
                    }
                }
            }
        } => (),
    };
}
