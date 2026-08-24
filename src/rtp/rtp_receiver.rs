use crate::rtp::{rtp_packet_header::RTPHeader, rtp_sender::PacketType};
use bytes::Bytes;
use std::{collections::VecDeque, time::Instant};
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct Peer {
    ///  variance in arrival time
    jitter: u32,

    /// highest sequence number currently received from this peer         
    max_sequence_number: u16,

    /// first sequence number received         
    initial_sequence_number: Option<u16>,

    /// number of packets received from this peer,
    /// can differ from max-initial when packets are lost
    packets_received: u32,

    /// number of times the sequence number has rolled over from max u16 value          
    wrap_around_count: u32,

    /// Stores the arrival time of the WINDOW_SIZE most recent packets
    window: VecDeque<u32>,

    /// packet in window with the earliest arrival time
    min_window: u32,

    /// middle 32 bytes of the NTP timestamp as received of the last SR from this peer
    last_sr_timestamp: u32,

    /// Time since the last SR has been received
    delay_since_last_sr: Option<Instant>,

    /// the expected number of packets received when the last SR was sent
    expected_prior: u32,

    /// the received number of packets when the last SR was sent
    received_prior: u32,
    // skew_calculator: PeerDelay,

    // buffer where frames with the same timestamp are grouped together
    // playout_buffer: Vec<PlayoutBufferNode>,
}

pub async fn packet_receiver(
    mut rx: Receiver<(RTPHeader, Bytes)>,
    packet_type: PacketType,
    cancellation_token: CancellationToken,
) {
    let instant = Instant::now();

    let media_clock_rate = match packet_type {
        PacketType::Audio => 48_000,
        PacketType::Video => 90_000,
    };

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                return
            }
            Some((header, bytes)) = rx.recv() => {
                let arrival_time = instant.elapsed();
                let arrival_time = arrival_time.as_millis() as u32 * (media_clock_rate / 1000);
                let difference = arrival_time.wrapping_sub(header.timestamp);

                let offset = peer_manager.peer_get_min_window(header.ssrc, difference, stream_type)?;

            }
        }
    }
}

fn calculate_playout_time() {}
