use bytes::Bytes;

use crate::wire::{FromRaw, Header, IntoRaw, MessageError, RawPacket, ServerMessage};

#[derive(Debug, Clone)]
pub struct Echo {
    pub payload: bytes::Bytes,
}

impl IntoRaw for Echo {
    fn into_raw(self) -> crate::wire::RawPacket {
        let body = self.payload;
        let header = Header {
            magic: crate::wire::Magic::Request,
            packet_type: crate::wire::PacketType::EchoReq,
            body_len: body.len() as i32,
        };

        RawPacket { header, body }
    }
}

#[derive(Debug, Clone)]
pub struct EchoRes {
    pub payload: bytes::Bytes,
}

impl FromRaw for EchoRes {
    fn from_raw(
        raw: crate::wire::RawPacket,
    ) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        Ok(ServerMessage::EchoRes(EchoRes {
            payload: Bytes::copy_from_slice(raw.arguments().next().ok_or(
                MessageError::WrongArgumentCount {
                    expected: 1,
                    found: 0,
                },
            )?),
        }))
    }
}
