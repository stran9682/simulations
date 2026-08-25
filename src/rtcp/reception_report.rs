/*
   Graciously from https://github.com/webrtc-rs/rtcp/blob/main/src/receiver_report/mod.rs
*/

use bytes::{Buf, BufMut, BytesMut};

pub struct ReceptionReport {
    pub reportee_ssrc: u32,
    pub fraction_lost: u8,
    pub total_lost: u32,
    pub extended_sequence_number: u32,
    pub jitter: u32,
    pub last_sr_timestamp: u32,
    pub delay_since_last_sr: u32,
}

impl ReceptionReport {
    pub fn serialize(&self) -> BytesMut {
        /*
         *  0                   1                   2                   3
         *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
         * +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
         * |                              SSRC                             |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * | fraction lost |       cumulative number of packets lost       |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |           extended highest sequence number received           |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |                      interarrival jitter                      |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |                         last SR (LSR)                         |
         * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * |                   delay since last SR (DLSR)                  |
         * +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
         */

        let mut buf = BytesMut::with_capacity(24);

        buf.put_u32(self.reportee_ssrc);

        buf.put_u8(self.fraction_lost);

        buf.put_u8(((self.total_lost >> 16) & 0xFF) as u8);
        buf.put_u8(((self.total_lost >> 8) & 0xFF) as u8);
        buf.put_u8((self.total_lost & 0xFF) as u8);

        buf.put_u32(self.extended_sequence_number);
        buf.put_u32(self.jitter);
        buf.put_u32(self.last_sr_timestamp);
        buf.put_u32(self.delay_since_last_sr);

        buf
    }

    pub fn deserialize<B: Buf>(packet: &mut B) -> Self {
        let reportee_ssrc = packet.get_u32();
        let fraction_lost = packet.get_u8();

        let t0 = packet.get_u8();
        let t1 = packet.get_u8();
        let t2 = packet.get_u8();
        let total_lost = (t2 as u32) | (t1 as u32) << 8 | (t0 as u32) << 16;

        let extended_sequence_number = packet.get_u32();
        let jitter = packet.get_u32();
        let last_sr_timestamp = packet.get_u32();
        let delay_since_last_sr = packet.get_u32();

        ReceptionReport {
            reportee_ssrc,
            fraction_lost,
            total_lost,
            extended_sequence_number,
            jitter,
            last_sr_timestamp,
            delay_since_last_sr,
        }
    }
}
