use bytes::Bytes;

use crate::wire::{FromRaw, Header, IntoRaw, MessageError, RawPacket, ServerMessage};

#[derive(Debug, Clone)]
pub struct SetOption {
    pub option: GearmanOption,
}

impl IntoRaw for SetOption {
    fn into_raw(self) -> crate::wire::RawPacket {
        let body: bytes::Bytes = self.option.into();
        let header = Header {
            magic: crate::wire::Magic::Request,
            packet_type: crate::wire::PacketType::OptionReq,
            body_len: body.len() as i32,
        };

        RawPacket { header, body }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GearmanOption {
    Exceptions,
}
impl Into<Bytes> for GearmanOption {
    fn into(self) -> Bytes {
        match self {
            Self::Exceptions => Bytes::from_static(b"exceptions"),
        }
    }
}
impl TryFrom<&[u8]> for GearmanOption {
    type Error = MessageError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match value {
            b"exceptions" => Ok(Self::Exceptions),
            _ => Err(MessageError::InvalidField {
                field: "option",
                reason: "Unknown gearman option".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OptionRes {
    pub option: GearmanOption,
}

impl FromRaw for OptionRes {
    fn from_raw(
        raw: crate::wire::RawPacket,
    ) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let option: GearmanOption = raw
            .arguments()
            .next()
            .ok_or(MessageError::WrongArgumentCount {
                expected: 1,
                found: 0,
            })?
            .try_into()?;

        Ok(ServerMessage::OptionRes(OptionRes { option }))
    }
}
