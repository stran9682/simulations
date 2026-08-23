use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use iroh::{
    PublicKey,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

use crate::rtp_packet_header::RTPHeader;

#[derive(Deserialize, Serialize, Debug)]
pub struct SessionInfo {
    peers: Vec<String>,
    video_ssrc: u32,
    audio_ssrc: u32,
}

#[derive(Debug)]
pub struct RtpConnectionManager {
    connections: DashMap<PublicKey, Connection>,
    audio_ssrc: u32,
    video_ssrc: u32,
}

impl RtpConnectionManager {
    pub fn new() -> Self {
        let video_ssrc = {
            let mut rng = rand::rng();
            rng.next_u32()
        };

        let audio_ssrc = {
            let mut rng = rand::rng();
            rng.next_u32()
        };

        Self { connections: DashMap::new(), audio_ssrc, video_ssrc }
    }

    pub fn connections(&self) -> Vec<Connection> {
        let connections: Vec<Connection> = self.connections.iter().map(|conn| conn.clone()).collect();
        connections
    }

    fn session_info(&self) -> SessionInfo {
        let peers: Vec<String> = self
            .connections
            .iter()
            .map(|conn| conn.remote_id().to_string())
            .collect();

        SessionInfo {
            peers,
            video_ssrc: self.video_ssrc,
            audio_ssrc: self.video_ssrc,
        }
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

        self.connections.insert(
            connection.remote_id(),
            connection.clone()
        );

        let response = self.session_info();
        let response = serde_json::to_vec(&response).map_err(|e| AcceptError::from_err(e))?;

        send.write_all(&response)
            .await
            .map_err(|e| AcceptError::from_err(e))?;
        send.finish()?;

        let recv_tasks = TaskTracker::new();
        let (audio_tx, audio_rx) = mpsc::channel::<(RTPHeader, Bytes)>(200);
        let (frame_tx, frame_rx) = mpsc::channel::<(RTPHeader, Bytes)>(200);

        todo!("Implement receiver tasks");
        
        loop {
            let mut packet = match connection.read_datagram().await {
                Ok(data) => data,
                Err(e) => {
                    eprintln!("Connection Error: {e}");
                    break
                },
            };

            if packet[1] & 0x7F >= 72 {
                todo!()
            } else {
                let header = RTPHeader::deserialize(&mut packet);

                let tx = if header.ssrc == request.audio_ssrc { &audio_tx } else { &frame_tx };

                let _ = tx
                    .send((header, packet))
                    .await
                    .inspect_err(|e| eprintln!("RTP receiver was full: {e}"));
            }
        }
        
        self.connections.remove(&connection.remote_id());

        recv_tasks.wait().await;

        Ok(())
    }
}


