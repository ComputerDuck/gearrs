use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::wire::{ClientMessage, FromRaw, IntoRaw, ServerMessage};
use crate::wire::{MessageError, PacketType};
use crate::wire::{RawPacket, WireError};

const HEADER_SIZE: usize = 12;

pub struct GearmanCodec;

impl Decoder for GearmanCodec {
    type Item = ServerMessage;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_SIZE {
            src.reserve(HEADER_SIZE - src.len());
            return Ok(None);
        }

        let body_len = u32::from_be_bytes([src[8], src[9], src[10], src[11]]) as usize;
        let total_len = HEADER_SIZE + body_len;

        if src.len() < total_len {
            src.reserve(total_len - src.len());
            return Ok(None);
        }

        let raw_bytes = src.split_to(total_len);
        let raw = RawPacket::parse(&raw_bytes)?;
        let message = ServerMessage::from_raw(raw)?;

        Ok(Some(message))
    }
}

impl Encoder<ClientMessage> for GearmanCodec {
    type Error = CodecError;

    fn encode(&mut self, message: ClientMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let raw = message.into_raw();
        let bytes = raw.serialize();
        dst.reserve(bytes.len());
        dst.put(bytes);
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Message(#[from] MessageError),
    #[error("expected {expected:?} as response, received {received:?}")]
    UnexpectedMessage {
        expected: PacketType,
        received: ServerMessage,
    },
    #[error("Request already in flight")]
    RequestAlreadyInFlight,
}
