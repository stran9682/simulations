use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::mpsc::Receiver;

use crate::rtp_connection_manager::RtpConnectionManager;

#[derive(Clone, Copy)]
pub enum PacketType {
    Video,
    Audio,
}

pub async fn send_packets(
    connection_manager: Arc<RtpConnectionManager>,
    mut rx: Receiver<(Bytes, u32)>,
    packet_type: PacketType,
) {
    while let Some((bytes, timestamp)) = rx.recv().await {
        let connections = connection_manager.connections();

        if connections.is_empty() {
            continue;
        }

        let payloads = match packet_type {
            PacketType::Video => split_payload(&bytes, &connection_manager, timestamp),
            PacketType::Audio => {
                let mut buf = BytesMut::with_capacity(1500);

                let rtp_header = connection_manager.get_packet_header(
                    true,
                    timestamp,
                    bytes.len() as u32,
                    PacketType::Audio,
                );

                rtp_header.serialize(&mut buf);

                buf.extend_from_slice(&bytes);

                vec![buf.freeze()]
            }
        };

        for payload in payloads {
            for connection in connection_manager.connections() {
                match connection.send_datagram_wait(payload.clone()).await {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Failed to send to {}: {}", connection.remote_id(), e);
                    }
                }
            }
        }
    }
}

fn split_payload(
    bytes: &Bytes,
    connection_manager: &Arc<RtpConnectionManager>,
    timestamp: u32,
) -> Vec<Bytes> {
    let mut payloads: Vec<Bytes> = Vec::new();

    let max_fragment_size = 1100; // low key a magic number...
    let mut nalu_data_index = 1;
    let nalu_data_length = bytes.len() - nalu_data_index;
    let mut nalu_data_remaining = nalu_data_length;

    let nalu_nri = bytes[0] & 0x60;
    let nalu_type = bytes[0] & 0x1F;

    let mut buf = BytesMut::with_capacity(1500);

    if bytes.len() <= max_fragment_size {
        let rtp_header = connection_manager.get_packet_header(
            true,
            timestamp,
            bytes.len() as u32,
            PacketType::Video,
        );

        rtp_header.serialize(&mut buf);
        //println!("Header (small packet): {}, {}, {}, {}", rtp_header.sequence_number, rtp_header.timestamp, rtp_header.marker, rtp_header.payload_type);

        buf.extend_from_slice(bytes);

        payloads.push(buf.freeze());
    } else {
        while nalu_data_remaining > 0 {
            let current_fragment_size = std::cmp::min(max_fragment_size, nalu_data_remaining);

            let rtp_header = connection_manager.get_packet_header(
                max_fragment_size >= nalu_data_remaining, // VERY last one
                timestamp,
                current_fragment_size as u32 + 2,
                PacketType::Video,
            );

            rtp_header.serialize(&mut buf); // this will move the sequence number by 1

            //println!("Header (split packet): {}, {}, {}, {}", rtp_header.sequence_number, rtp_header.timestamp, rtp_header.marker, rtp_header.payload_type);

            /*
                +---------------+---------------+
                |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
                +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                |F|NRI|  Type   |S|E|R|  Type   |
                +---------------+---------------+

                F           : should always be 0
                NRI         : Essentialy level of importance, needs to be copied
                Type (1)    : Type of header. 28 To indicate this is a fragment
                S(tart)     : indicates this is the start
                E(nd)       : indicates this is the end
                R(eserved)  : always 0
                Type (2)    : Kind of payload, needs to be copied

                Original header needs to be reconstructed!
            */

            let b0 = 28 | nalu_nri; // 28 to indicate FU-A packet type
            buf.put_u8(b0);

            let mut b1 = nalu_type;
            if nalu_data_remaining == nalu_data_length {
                // Set start bit
                b1 |= 1 << 7;
            } else if nalu_data_remaining - current_fragment_size == 0 {
                // Set end bit
                b1 |= 1 << 6;
            }
            buf.put_u8(b1);

            buf.put_slice(&bytes[nalu_data_index..nalu_data_index + current_fragment_size]);

            nalu_data_remaining -= current_fragment_size;
            nalu_data_index += current_fragment_size;

            payloads.push(buf.split().freeze());
            buf.reserve(1500);
        }
    }

    payloads
}
