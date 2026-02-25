use std::{num::ParseIntError, ops::Div};

use bytes::Bytes;

use crate::{
    MessageError,
    wire::{FromRaw, Header, IntoRaw, RawPacket},
};

#[derive(Debug, Clone)]
pub struct GetStatus {
    pub job_handle: Bytes,
}

impl IntoRaw for GetStatus {
    fn into_raw(self) -> crate::wire::RawPacket {
        let body = self.job_handle;
        let header = Header {
            magic: crate::wire::Magic::Request,
            packet_type: crate::wire::PacketType::GetStatus,
            body_len: body.len() as i32,
        };

        RawPacket { header, body }
    }
}

#[derive(Debug, Clone)]
pub struct GetStatusUnique {
    pub unique_id: String,
}

impl IntoRaw for GetStatusUnique {
    fn into_raw(self) -> RawPacket {
        let body: Bytes = self.unique_id.into();
        let header = Header {
            magic: crate::wire::Magic::Request,
            packet_type: crate::wire::PacketType::GetStatusUnique,
            body_len: body.len() as i32,
        };

        RawPacket { header, body }
    }
}

#[derive(Debug, Clone)]
pub struct StatusRes {
    pub job_handle: Bytes,
    pub known: bool,
    pub running: bool,
    pub numerator: u32,
    pub denominator: u32,
}

impl FromRaw for StatusRes {
    fn from_raw(raw: RawPacket) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let mut arguments = raw.arguments();

        let job_handle =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 5,
                found: 0,
            })?);

        let known: bool = arguments.next().ok_or(MessageError::WrongArgumentCount {
            expected: 5,
            found: 1,
        })? == b"1";

        let running: bool = arguments.next().ok_or(MessageError::WrongArgumentCount {
            expected: 5,
            found: 2,
        })? == b"1";

        let numerator: u32 =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 5,
                found: 3,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "numerator",
                reason: err.to_string(),
            })?
            .parse()
            .map_err(|err: ParseIntError| MessageError::InvalidField {
                field: "numerator",
                reason: err.to_string(),
            })?;

        let denominator: u32 =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 5,
                found: 4,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "denominator",
                reason: err.to_string(),
            })?
            .parse()
            .map_err(|err: ParseIntError| MessageError::InvalidField {
                field: "denominator",
                reason: err.to_string(),
            })?;

        Ok(crate::wire::ServerMessage::StatusRes(StatusRes {
            job_handle,
            known,
            running,
            numerator,
            denominator,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct StatusResUnique {
    pub unique_id: String,
    pub known: bool,
    pub running: bool,
    pub numerator: u32,
    pub denominator: u32,
    pub waiting: u32,
}

impl FromRaw for StatusResUnique {
    fn from_raw(raw: RawPacket) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let mut arguments = raw.arguments();

        let unique_id =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 6,
                found: 0,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "unique_id",
                reason: err.to_string(),
            })?
            .to_string();

        let known: bool = arguments.next().ok_or(MessageError::WrongArgumentCount {
            expected: 6,
            found: 1,
        })? == b"1";

        let running: bool = arguments.next().ok_or(MessageError::WrongArgumentCount {
            expected: 6,
            found: 2,
        })? == b"1";

        let numerator: u32 =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 6,
                found: 3,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "numerator",
                reason: err.to_string(),
            })?
            .parse()
            .map_err(|err: ParseIntError| MessageError::InvalidField {
                field: "numerator",
                reason: err.to_string(),
            })?;

        let denominator: u32 =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 6,
                found: 4,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "denominator",
                reason: err.to_string(),
            })?
            .parse()
            .map_err(|err: ParseIntError| MessageError::InvalidField {
                field: "denominator",
                reason: err.to_string(),
            })?;

        let waiting: u32 =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 6,
                found: 5,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "waiting",
                reason: err.to_string(),
            })?
            .parse()
            .map_err(|err: ParseIntError| MessageError::InvalidField {
                field: "waiting",
                reason: err.to_string(),
            })?;

        Ok(crate::wire::ServerMessage::StatusResUnique(
            StatusResUnique {
                unique_id,
                known,
                running,
                numerator,
                denominator,
                waiting,
            },
        ))
    }
}

#[derive(Debug, Clone)]
pub struct WorkStatus {
    pub job_handle: Bytes,
    pub numerator: u32,
    pub denominator: u32,
}
impl FromRaw for WorkStatus {
    fn from_raw(raw: RawPacket) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let mut arguments = raw.arguments();

        let job_handle =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 3,
                found: 0,
            })?);

        let numerator: u32 =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 3,
                found: 1,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "numerator",
                reason: err.to_string(),
            })?
            .parse()
            .map_err(|err: ParseIntError| MessageError::InvalidField {
                field: "numerator",
                reason: err.to_string(),
            })?;

        let denominator: u32 =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 3,
                found: 2,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "denominator",
                reason: err.to_string(),
            })?
            .parse()
            .map_err(|err: ParseIntError| MessageError::InvalidField {
                field: "denominator",
                reason: err.to_string(),
            })?;

        Ok(crate::wire::ServerMessage::WorkStatus(WorkStatus {
            job_handle,
            numerator,
            denominator,
        }))
    }
}

impl WorkStatus {
    /// Progress as a value in [0.0, 1.0]. Returns None if denominator is 0.
    pub fn fraction(&self) -> Option<f32> {
        match (self.numerator as f64).div(self.denominator as f64) {
            res if res.is_normal() && res <= 1. && res >= 0. => Some(res as f32),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkComplete {
    pub job_handle: Bytes,
    pub payload: Bytes,
}

impl FromRaw for WorkComplete {
    fn from_raw(raw: RawPacket) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let mut arguments = raw.arguments();

        let job_handle =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 0,
            })?);

        let payload =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 1,
            })?);

        Ok(crate::wire::ServerMessage::WorkComplete(WorkComplete {
            job_handle,
            payload,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct WorkFail {
    pub job_handle: Bytes,
}

impl FromRaw for WorkFail {
    fn from_raw(raw: RawPacket) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let mut arguments = raw.arguments();

        let job_handle =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 1,
                found: 0,
            })?);

        Ok(crate::wire::ServerMessage::WorkFail(WorkFail {
            job_handle,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct WorkException {
    pub job_handle: Bytes,
    pub exception: Bytes,
}
impl FromRaw for WorkException {
    fn from_raw(raw: RawPacket) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let mut arguments = raw.arguments();

        let job_handle =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 0,
            })?);

        let exception =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 1,
            })?);

        Ok(crate::wire::ServerMessage::WorkException(WorkException {
            job_handle,
            exception,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct WorkData {
    pub job_handle: Bytes,
    pub payload: Bytes,
}
impl FromRaw for WorkData {
    fn from_raw(raw: RawPacket) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let mut arguments = raw.arguments();

        let job_handle =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 0,
            })?);

        let payload =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 1,
            })?);

        Ok(crate::wire::ServerMessage::WorkData(WorkData {
            job_handle,
            payload,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct WorkWarning {
    pub job_handle: Bytes,
    pub warning: Bytes,
}
impl FromRaw for WorkWarning {
    fn from_raw(raw: RawPacket) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let mut arguments = raw.arguments();

        let job_handle =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 0,
            })?);

        let warning =
            Bytes::copy_from_slice(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 1,
            })?);

        Ok(crate::wire::ServerMessage::WorkWarning(WorkWarning {
            job_handle,
            warning,
        }))
    }
}
