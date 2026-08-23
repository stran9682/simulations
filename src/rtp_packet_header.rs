/*
    honestly, i've just stolen this from :
    https://github.com/webrtc-rs/rtcp/blob/main/src/source_description/mod.rs
*/

use bytes::{self, Buf, BufMut, BytesMut};

/*
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |V=2|P|X|  CC   |M|     PT      |       sequence number         |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                           timestamp                           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |           synchronization source (SSRC) identifier            |
 * +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
 * |            contributing source (CSRC) identifiers             |
 * |                             ....                              |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/

#[derive(Debug)]
pub struct RTPHeader {
    pub version: u8,
    pub padding: bool,
    pub extension: bool,
    pub marker: bool,
    pub payload_type: u8,
    pub sequence_number: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    // pub csrc: Vec<u32>,
    // pub extension_profile: u16,
    // pub extensions: Vec<Extension>,
}

// pub struct Extension {
//     pub id: u8,
//     pub payload: Bytes,
// }

impl RTPHeader {
    pub fn serialize(&self, buf: &mut BytesMut) {
        //let mut buf = BytesMut::with_capacity(1500);

        // first byte
        let mut b0 = (self.version << 6) | 0_u8; // this should be set to the CSRC length, but i've removed it for now
        if self.padding {
            b0 |= 1 << 5;
        }

        if self.extension {
            b0 |= 1 << 4;
        }
        buf.put_u8(b0);

        // second byte
        let mut b1 = self.payload_type;
        if self.marker {
            b1 |= 1 << 7;
        }
        buf.put_u8(b1);

        // the rest
        buf.put_u16(self.sequence_number);
        buf.put_u32(self.timestamp);
        buf.put_u32(self.ssrc);

        //println!("{:08b}{:08b}", buf[0], buf[1]);

        // for csrc in &self.csrc {
        //     buf.put_u32(*csrc);
        // }
    }

    pub fn deserialize<B: Buf>(packet: &mut B) -> RTPHeader {
        let b0 = packet.get_u8();
        let version = b0 >> 6 & 0x3;
        let padding = (b0 >> 5 & 0x1) > 0;
        let extension = (b0 >> 4 & 0x1) > 0;
        // let cc = (b0 & 0xF) as usize;

        let b1 = packet.get_u8();
        let marker = (b1 >> 7 & 0x1) > 0;
        let payload_type = b1 & 0x7F;

        let sequence_number = packet.get_u16();
        let timestamp = packet.get_u32();
        let ssrc = packet.get_u32();

        // let mut csrc = Vec::with_capacity(cc);
        // for _ in 0..cc {
        //     csrc.push(packet.get_u32());
        // }

        RTPHeader {
            version,
            padding,
            extension,
            marker,
            payload_type,
            sequence_number,
            timestamp,
            ssrc,
            // csrc
        }
    }
}
