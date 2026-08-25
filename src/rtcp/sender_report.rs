/*
   Graciously from https://github.com/webrtc-rs/rtcp/blob/main/src/sender_report/mod.rs
*/

use bytes::{Buf, BufMut, BytesMut};

use crate::rtcp::reception_report::ReceptionReport;

pub struct SenderReport {
    pub ssrc: u32,
    pub ntp_time: u64,
    pub rtp_time: u32,
    pub packet_count: u32,
    pub octet_count: u32,
    pub reports: Vec<ReceptionReport>,
}

impl SenderReport {
    pub fn serialize(&self) -> BytesMut {
        /*
         *         0                   1                   2                   3
         *         0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
         *        +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
         * sender |              NTP timestamp, most significant word             |
         * info   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *        |             NTP timestamp, least significant word             |
         *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *        |                         RTP timestamp                         |
         *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *        |                     sender's packet count                     |
         *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *        |                      sender's octet count                     |
         *        +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
         * report |                 SSRC_1 (SSRC of first source)                 |
         * block  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *   1    | fraction lost |       cumulative number of packets lost       |
         *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *        |           extended highest sequence number received           |
         *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *        |                      interarrival jitter                      |
         *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *        |                         last SR (LSR)                         |
         *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *        |                   delay since last SR (DLSR)                  |
         *        +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
         * report |                 SSRC_2 (SSRC of second source)                |
         * block  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *   2    :                               ...                             :
         *        +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
         *        |                  profile-specific extensions                  |
         *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         */
        let mut buf = BytesMut::new();

        buf.put_u32(self.ssrc);
        buf.put_u64(self.ntp_time);
        buf.put_u32(self.rtp_time);
        buf.put_u32(self.packet_count);
        buf.put_u32(self.octet_count);

        for report in &self.reports {
            buf.put(report.serialize());
        }
        buf
    }

    pub fn deserialize<B: Buf>(packet: &mut B, report_counts: u8) -> Self {
        let ssrc = packet.get_u32();
        let ntp_time = packet.get_u64();
        let rtp_time = packet.get_u32();
        let packet_count = packet.get_u32();
        let octet_count = packet.get_u32();

        let mut reports = Vec::with_capacity(report_counts as usize);
        for _ in 0..report_counts {
            let reception_report = ReceptionReport::deserialize(packet);
            reports.push(reception_report);
        }

        SenderReport {
            ssrc,
            ntp_time,
            rtp_time,
            packet_count,
            octet_count,
            reports,
        }
    }

    // TODO: actually calculate this the right way
    pub fn length(&self) -> u16 {
        24 + (self.reports.len() * 24) as u16
    }
}
