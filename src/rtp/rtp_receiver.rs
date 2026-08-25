use crate::rtp::{rtp_packet_header::RTPHeader, rtp_sender::PacketType};
use bytes::Bytes;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

static WINDOW_SIZE: usize = 50;

#[derive(Debug)]
pub struct PlayoutBufferNode {
    pub rtp_timestamp: u32,
    pub playout_time: u32,
    pub coded_data: Vec<Fragment>,
}

#[derive(Debug)]
pub struct Fragment {
    pub extended_sequence_num: u32,
    pub sequence_num: u16,
    pub data: Bytes,
}

impl Fragment {
    pub fn new(sequence_num: u16, data: Bytes) -> Self {
        Self {
            sequence_num,
            data,
            extended_sequence_num: 0,
        }
    }
}

#[derive(Debug)]
pub struct Peer {

    pub ssrc: u32, 

    ///  variance in arrival time
    pub jitter: u32,

    /// highest sequence number currently received from this peer         
    pub max_sequence_number: u16,

    /// first sequence number received         
    pub initial_sequence_number: Option<u16>,

    /// number of packets received from this peer,
    /// can differ from max-initial when packets are lost
    pub packets_received: u32,

    /// number of times the sequence number has rolled over from max u16 value          
    pub wrap_around_count: u32,

    /// Stores the arrival time of the WINDOW_SIZE most recent packets
    pub window: VecDeque<u32>,

    /// packet in window with the earliest arrival time
    pub min_window: u32,

    /// middle 32 bytes of the NTP timestamp as received of the last SR from this peer
    pub last_sr_timestamp: u32,

    /// Time since the last SR has been received
    pub delay_since_last_sr: Option<Instant>,

    /// the expected number of packets received when the last SR was sent
    pub expected_prior: u32,

    /// the received number of packets when the last SR was sent
    pub received_prior: u32,
    // skew_calculator: PeerDelay,

    // buffer where frames with the same timestamp are grouped together
    // playout_buffer: Vec<PlayoutBufferNode>,
}

impl Peer {
    pub fn new(ssrc: u32) -> Self {
        Self {
            ssrc,
            jitter: 0,
            delay_since_last_sr: None,
            last_sr_timestamp: 0,
            packets_received: 0,
            wrap_around_count: 0,
            max_sequence_number: 0,
            initial_sequence_number: None,
            window: VecDeque::new(),
            min_window: u32::MAX,
            // playout_buffer: Vec::with_capacity(100),
            // swift_peer_model,
            expected_prior: 0,
            received_prior: 0,
            // skew_calculator: PeerDelay::new(skew_threshold),
        }
    }

    pub fn max_extended_sequence_num(&self) -> u32 {
        let max_sequence = self.max_sequence_number;
        max_sequence as u32 + (65536 * self.wrap_around_count)
    }

    pub fn expected_num_packets(&self) -> u32 {
        // I'm actually cheating a bit here,
        // according to Perkin's, you should use the last received sequence number, not highest one
        self.max_extended_sequence_num() - self.initial_sequence_number.unwrap_or(0) as u32
    }

    pub fn calculate_fraction_lost(&self) -> u8 {
        let expected_interval = self.expected_num_packets() - self.expected_prior;
        let received_inteval = self.packets_received - self.received_prior;
        let lost_inteval = expected_interval as i32 - received_inteval as i32;

        if expected_interval == 0 || lost_inteval <= 0 {
            return 0;
        }

        ((lost_inteval << 8) / expected_interval as i32) as u8
    }

    pub fn update_reception_stats(&mut self, difference: u32, header: RTPHeader) {
        self.packets_received += 1;

        self.window.push_front(difference);
        let d = difference.wrapping_sub(self.window[0]) as i32;
        self.jitter = self.jitter + (d.unsigned_abs() - self.jitter) / 16;

        if self.window.len() > WINDOW_SIZE {
            self.window.pop_back();
        }

        let min = self.window.iter().fold(self.window[0], |min, val| {
            if val.wrapping_sub(min) & 0x80000000 != 0 {
                *val
            } else {
                min
            }
        });

        self.min_window = min;

        if self.initial_sequence_number.is_none() {
            self.initial_sequence_number = Some(header.sequence_number);
            self.max_sequence_number = header.sequence_number;
        }

        let delta = header.sequence_number - self.max_sequence_number;

        if delta < 3000 {
            // accounting for wraparound
            if header.sequence_number < self.max_sequence_number {
                self.wrap_around_count += 1;
            }
            self.max_sequence_number = header.sequence_number;
        } else if delta <= 65535 - 100 {
            // sequence number made a large jump
        } else {
            // misordered packet.
        }
    }
}

pub async fn packet_receiver(
    mut rx: Receiver<(RTPHeader, Bytes)>,
    packet_type: PacketType,
    cancellation_token: CancellationToken,
    peer_data: Arc<Mutex<Peer>>,
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
            Some((header, _)) = rx.recv() => {
                let arrival_time = instant.elapsed();
                let arrival_time = arrival_time.as_millis() as u32 * (media_clock_rate / 1000);
                let difference = arrival_time.wrapping_sub(header.timestamp);

                peer_data.lock().unwrap().update_reception_stats(difference, header);
            }
        }
    }
}
