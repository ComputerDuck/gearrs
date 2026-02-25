use bytes::Bytes;

use crate::packages::*;

/// 12-byte prefix on every Gearman packet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub magic: Magic,
    pub packet_type: PacketType,
    /// Size of the body in bytes, as declared by the sender.
    pub body_len: i32,
}
impl TryFrom<&[u8; 12]> for Header {
    type Error = WireError;
    fn try_from(value: &[u8; 12]) -> Result<Self, Self::Error> {
        let magic = match value[0..4] {
            [b'\0', b'R', b'E', b'Q'] => Magic::Request,
            [b'\0', b'R', b'E', b'S'] => Magic::Response,
            _ => return Err(WireError::UnknownMagic(value[0..4].try_into().unwrap())),
        };
        let packet_type = i32::from_be_bytes(value[4..8].try_into().unwrap())
            .try_into()
            .map_err(|v: i32| WireError::UnknownPacketType(v as u32))?;
        let body_len = i32::from_be_bytes(value[8..12].try_into().unwrap());

        Ok(Header {
            magic,
            packet_type,
            body_len,
        })
    }
}
impl Into<[u8; 12]> for Header {
    fn into(self) -> [u8; 12] {
        let magic = match self.magic {
            Magic::Request => [b'\0', b'R', b'E', b'Q'],
            Magic::Response => [b'\0', b'R', b'E', b'S'],
        };
        let packet_type: [u8; 4] = (self.packet_type as i32).to_be_bytes();
        let body_len: [u8; 4] = (self.body_len).to_be_bytes();

        let bytes: [u8; 12] = [
            magic[0],
            magic[1],
            magic[2],
            magic[3],
            packet_type[0],
            packet_type[1],
            packet_type[2],
            packet_type[3],
            body_len[0],
            body_len[1],
            body_len[2],
            body_len[3],
        ];
        bytes
    }
}

/// \0REQ or \0RES
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Magic {
    Request,
    Response,
}

/// Every packet type the Gearman protocol defines
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PacketType {
    // Client → Server
    SubmitJob = 7,
    SubmitJobBg = 18,
    SubmitJobHigh = 21,
    SubmitJobHighBg = 32,
    SubmitJobLow = 33,
    SubmitJobLowBg = 34,
    GetStatus = 15,
    GetStatusUnique = 41,
    EchoReq = 16,
    OptionReq = 26,

    // Server → Client
    JobCreated = 8,
    WorkStatus = 12,
    WorkComplete = 13,
    WorkFail = 14,
    WorkException = 25,
    WorkData = 28,
    WorkWarning = 29,
    StatusRes = 20,
    StatusResUnique = 42,
    EchoRes = 17,
    OptionRes = 27,
    Error = 19,
}
impl TryFrom<i32> for PacketType {
    type Error = i32;
    fn try_from(value: i32) -> Result<Self, <Self as TryFrom<i32>>::Error> {
        match value {
            7 => Ok(PacketType::SubmitJob),
            18 => Ok(PacketType::SubmitJobBg),
            21 => Ok(PacketType::SubmitJobHigh),
            32 => Ok(PacketType::SubmitJobHighBg),
            33 => Ok(PacketType::SubmitJobLow),
            34 => Ok(PacketType::SubmitJobLowBg),
            15 => Ok(PacketType::GetStatus),
            41 => Ok(PacketType::GetStatusUnique),
            16 => Ok(PacketType::EchoReq),
            26 => Ok(PacketType::OptionReq),
            8 => Ok(PacketType::JobCreated),
            12 => Ok(PacketType::WorkStatus),
            13 => Ok(PacketType::WorkComplete),
            14 => Ok(PacketType::WorkFail),
            25 => Ok(PacketType::WorkException),
            28 => Ok(PacketType::WorkData),
            29 => Ok(PacketType::WorkWarning),
            20 => Ok(PacketType::StatusRes),
            42 => Ok(PacketType::StatusResUnique),
            17 => Ok(PacketType::EchoRes),
            27 => Ok(PacketType::OptionRes),
            19 => Ok(PacketType::Error),
            _ => Err(value),
        }
    }
}

/// A fully-read, length-validated raw packet fresh off the wire.
#[derive(Debug, Clone)]
pub struct RawPacket {
    pub header: Header,
    pub body: bytes::Bytes,
}

impl RawPacket {
    /// Parse a complete packet from a byte slice.
    /// Validates magic, packet type, body length.
    pub fn parse(src: &[u8]) -> Result<Self, WireError> {
        if src.len() < 12 {
            return Err(WireError::Incomplete {
                needed: 12,
                have: src.len(),
            });
        }
        let header: &[u8; 12] = &src[0..12].try_into().unwrap();
        let header = Header::try_from(header)?;
        let body = Bytes::copy_from_slice(&src[12..]);

        Ok(RawPacket { header, body })
    }

    /// Serialize this packet for sending over network
    pub fn serialize(&self) -> bytes::Bytes {
        let mut buf = bytes::BytesMut::with_capacity(12 + self.header.body_len as usize);
        buf.extend_from_slice(&Into::<[u8; 12]>::into(self.header));
        buf.extend_from_slice(self.body.as_ref());
        buf.freeze()
    }

    /// The null-byte-delimited arguments in the body, in order.
    /// An empty body yields a single empty slice.
    pub fn arguments(&self) -> impl Iterator<Item = &[u8]> {
        self.body.split(|&b| b == b'\0').clone()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("need at least {needed} bytes, have {have}")]
    Incomplete { needed: usize, have: usize },
    #[error("invalid magic bytes: {0:?}")]
    UnknownMagic([u8; 4]),
    #[error("unknown packet type: {0}")]
    UnknownPacketType(u32),
    #[error("declared body length {declared} does not match actual {actual}")]
    LengthMismatch { declared: u32, actual: usize },
}

/// Every packet a client can send to the server.
#[derive(Debug, Clone)]
pub enum ClientMessage {
    SubmitJob(SubmitJob),
    GetStatus(GetStatus),
    GetStatusUnique(GetStatusUnique),
    Echo(Echo),
    SetOption(SetOption),
}

impl IntoRaw for ClientMessage {
    fn into_raw(self) -> RawPacket {
        match self {
            Self::SubmitJob(job) => job.into_raw(),
            Self::GetStatus(status) => status.into_raw(),
            Self::GetStatusUnique(status_unique) => status_unique.into_raw(),
            Self::Echo(echo) => echo.into_raw(),
            Self::SetOption(option) => option.into_raw(),
        }
    }
}

/// Every packet the server can send to the client.
#[derive(Debug, Clone)]
pub enum ServerMessage {
    // Direct replies to a request:
    JobCreated(JobCreated),
    StatusRes(StatusRes),
    StatusResUnique(StatusResUnique),
    EchoRes(EchoRes),
    OptionRes(OptionRes),
    Error(ServerError),

    // Asynchronous job progress (can arrive at any time):
    WorkStatus(WorkStatus),
    WorkComplete(WorkComplete),
    WorkFail(WorkFail),
    WorkException(WorkException),
    WorkData(WorkData),
    WorkWarning(WorkWarning),
}

impl ServerMessage {
    /// True for packets that arrive outside of any request/reply cycle.
    pub fn is_async(&self) -> bool {
        use ServerMessage::*;
        matches!(
            self,
            WorkStatus(_)
                | WorkComplete(_)
                | WorkFail(_)
                | WorkException(_)
                | WorkData(_)
                | WorkWarning(_)
        )
    }

    /// The job handle this message refers to, if any.
    /// Returns `None` for non-job messages (EchoRes, OptionRes, Error).
    pub fn job_handle(&self) -> Option<Bytes> {
        use ServerMessage::*;
        match self {
            JobCreated(job) => Some(job.job_handle.clone()),
            StatusRes(status) => Some(status.job_handle.clone()),
            WorkStatus(w) => Some(w.job_handle.clone()),
            WorkComplete(w) => Some(w.job_handle.clone()),
            WorkFail(w) => Some(w.job_handle.clone()),
            WorkException(w) => Some(w.job_handle.clone()),
            WorkData(w) => Some(w.job_handle.clone()),
            WorkWarning(w) => Some(w.job_handle.clone()),
            _ => None,
        }
    }
}
impl FromRaw for ServerMessage {
    fn from_raw(raw: RawPacket) -> Result<ServerMessage, MessageError> {
        match raw.header.packet_type {
            PacketType::JobCreated => JobCreated::from_raw(raw),
            PacketType::EchoRes => EchoRes::from_raw(raw),
            PacketType::StatusRes => StatusRes::from_raw(raw),
            PacketType::StatusResUnique => StatusResUnique::from_raw(raw),
            PacketType::OptionRes => OptionRes::from_raw(raw),
            PacketType::Error => ServerError::from_raw(raw),

            PacketType::WorkStatus => WorkStatus::from_raw(raw),
            PacketType::WorkComplete => WorkComplete::from_raw(raw),
            PacketType::WorkFail => WorkFail::from_raw(raw),
            PacketType::WorkException => WorkException::from_raw(raw),
            PacketType::WorkData => WorkData::from_raw(raw),
            PacketType::WorkWarning => WorkWarning::from_raw(raw),

            _ => Err(MessageError::InvalidServerPacket(raw)),
        }
    }
}

/// Parse a [`RawPacket`] into a typed [`ServerMessage`].
pub trait FromRaw: Sized {
    fn from_raw(raw: RawPacket) -> Result<ServerMessage, MessageError>;
}

/// Serialize a typed message into a [`RawPacket`] ready for the wire.
pub trait IntoRaw {
    fn into_raw(self) -> RawPacket;
}

#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("expected {expected} arguments, found {found}")]
    WrongArgumentCount { expected: usize, found: usize },
    #[error("Cant parse message from the server: {0:?}")]
    InvalidServerPacket(RawPacket),
    #[error("field '{field}' is not valid UTF-8")]
    InvalidUtf8 { field: &'static str },
    #[error("field '{field}' could not be parsed: {reason}")]
    InvalidField { field: &'static str, reason: String },
    #[error(transparent)]
    Wire(#[from] WireError),
}
