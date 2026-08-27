use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::Bytes;
use dashmap::DashMap;
use iroh::{
    PublicKey,
    endpoint::{Connection, PathId},
    protocol::{AcceptError, ProtocolHandler},
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    rtcp::{
        rtcp_packet_header::{self, RTCPHeader},
        rtcp_sender::rtcp_sender,
        sender_report::SenderReport,
    },
    rtp::{
        rtp_packet_header::{RTPHeader, RTPSession},
        rtp_receiver::{Peer, packet_receiver},
        rtp_sender::PacketType,
    },
};

#[derive(Deserialize, Serialize, Debug)]
pub struct SessionInfo {
    pub peers: Vec<String>,
    pub video_ssrc: u32,
    pub audio_ssrc: u32,
}

#[derive(Debug)]
pub struct RtpConnectionManager {
    connections: DashMap<PublicKey, Connection>,
    video_rtp_session: Arc<RTPSession>,
    audio_rtp_session: Arc<RTPSession>,
    pub clock: Instant,
}

impl RtpConnectionManager {
    pub fn audio_ssrc(&self) -> u32 {
        self.audio_rtp_session.ssrc
    }

    pub fn video_ssrc(&self) -> u32 {
        self.video_rtp_session.ssrc
    }

    pub fn new() -> Self {
        let video_ssrc = {
            let mut rng = rand::rng();
            rng.next_u32()
        };

        let audio_ssrc = {
            let mut rng = rand::rng();
            rng.next_u32()
        };

        Self {
            connections: DashMap::new(),
            audio_rtp_session: Arc::new(RTPSession::new(audio_ssrc)),
            video_rtp_session: Arc::new(RTPSession::new(video_ssrc)),
            clock: Instant::now(),
        }
    }

    pub fn connections(&self) -> Vec<Connection> {
        let connections: Vec<Connection> =
            self.connections.iter().map(|conn| conn.clone()).collect();
        connections
    }

    pub fn session_info(&self) -> SessionInfo {
        let peers: Vec<String> = self
            .connections
            .iter()
            .map(|conn| conn.remote_id().to_string())
            .collect();

        SessionInfo {
            peers,
            video_ssrc: self.video_ssrc(),
            audio_ssrc: self.audio_ssrc(),
        }
    }

    pub fn get_packet_header(
        &self,
        marker: bool,
        timestamp: u32,
        packet_length: u32,
        packet_type: PacketType,
    ) -> RTPHeader {
        match packet_type {
            PacketType::Video => {
                self.video_rtp_session
                    .get_packet(marker, timestamp, packet_length)
            }
            PacketType::Audio => {
                self.audio_rtp_session
                    .get_packet(marker, timestamp, packet_length)
            }
        }
    }

    pub fn add_connection(&self, connection: Connection) {
        self.connections
            .insert(connection.remote_id(), connection.clone());
    }

    pub async fn spawn_receivers(&self, connection: Connection, request: &SessionInfo) {
        let recv_tasks = TaskTracker::new();
        let (audio_tx, audio_rx) = mpsc::channel::<(RTPHeader, Bytes)>(200);
        let (frame_tx, frame_rx) = mpsc::channel::<(RTPHeader, Bytes)>(200);
        let token = CancellationToken::new();

        let audio_peer = Arc::new(Mutex::new(Peer::new(request.audio_ssrc)));
        recv_tasks.spawn(packet_receiver(
            audio_rx,
            PacketType::Audio,
            token.child_token(),
            Arc::clone(&audio_peer),
        ));
        recv_tasks.spawn(rtcp_sender(
            connection.clone(),
            Arc::clone(&self.audio_rtp_session),
            self.clock,
            48_000,
            Arc::clone(&audio_peer),
            token.child_token(),
        ));

        let video_peer = Arc::new(Mutex::new(Peer::new(request.video_ssrc)));
        recv_tasks.spawn(packet_receiver(
            frame_rx,
            PacketType::Video,
            token.child_token(),
            Arc::clone(&video_peer),
        ));
        recv_tasks.spawn(rtcp_sender(
            connection.clone(),
            Arc::clone(&self.video_rtp_session),
            self.clock,
            90_000,
            Arc::clone(&video_peer),
            token.child_token(),
        ));

        'receive: loop {
            let mut packet = match connection.read_datagram().await {
                Ok(data) => data,
                Err(e) => {
                    eprintln!("Connection Error: {e}");
                    break;
                }
            };

            if packet[1] & 0x7F >= 72 {
                for path in &connection.paths() {
                    if let Some(rtt) = connection.rtt(path.id()) {
                        // println!("{} RTT: {}", connection.remote_id(), rtt.as_micros());
                        println!(
                            "path: {} \t is relay: {} \t is selected {} \t rtt: {}",
                            path.id(),
                            path.is_selected(),
                            path.is_relay(),
                            rtt.as_micros()
                        );
                    }
                }

                while !packet.is_empty() {
                    let header = RTCPHeader::deserialize(&mut packet);

                    if header.packet_type == rtcp_packet_header::PacketType::SenderReport {
                        let sender_report = SenderReport::deserialize(&mut packet, header.count);

                        let last_sr_timestamp = (sender_report.ntp_time >> 16 & 0xFFFFFFFF) as u32;

                        let peer = if sender_report.ssrc == self.video_ssrc() {
                            video_peer.lock()
                        } else {
                            audio_peer.lock()
                        };

                        match peer {
                            Ok(mut peer) => {
                                peer.update_last_sr_timestamp(last_sr_timestamp);
                            }
                            Err(e) => {
                                eprintln!("RTCP Lock failure: {}", e);
                                break 'receive;
                            }
                        }
                    }
                }
            } else {
                let header = RTPHeader::deserialize(&mut packet);

                let tx = if header.ssrc == request.audio_ssrc {
                    &audio_tx
                } else {
                    &frame_tx
                };

                if let Err(e) = tx.send((header, packet)).await {
                    eprintln!("RTP receiver failure: {}", e);
                    break;
                }
            }
        }

        token.cancel();

        self.connections.remove(&connection.remote_id());

        recv_tasks.wait().await;
    }
}

impl ProtocolHandler for RtpConnectionManager {
    async fn accept(&self, connection: Connection) -> Result<(), iroh::protocol::AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;

        let bytes = recv
            .read_to_end(1000)
            .await
            .map_err(|e| AcceptError::from_err(e))?;

        let request: SessionInfo =
            serde_json::from_slice(&bytes).map_err(|e| AcceptError::from_err(e))?;

        let response = self.session_info();
        let response = serde_json::to_vec(&response).map_err(|e| AcceptError::from_err(e))?;

        send.write_all(&response)
            .await
            .map_err(|e| AcceptError::from_err(e))?;
        send.finish()?;

        self.connections
            .insert(connection.remote_id(), connection.clone());

        self.spawn_receivers(connection, &request).await;

        Ok(())
    }
}
