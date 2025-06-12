use bytes::{Buf, Bytes};

use crate::connection::WaitingType;

#[derive(Debug)]
pub enum HeaderError {
    InvalidSize,
    UnknownMagic([u8; 4]),
    UnknownType(i32),
    Malformatted(String),
}
impl std::error::Error for HeaderError {}
impl std::fmt::Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSize => write!(
                f,
                "The payload size does not match the size provided in the header"
            ),
            Self::UnknownType(packet_type) => write!(f, "Unknown type: {packet_type}"),
            Self::UnknownMagic(bytes) => write!(
                f,
                "Invalid magic bytes. Expected \\0REQ or \\0RES, found: {:?}",
                bytes
            ),
            Self::Malformatted(custom) => write!(f, "Malformatted Header: {custom}"),
        }
    }
}
#[derive(Debug)]
pub enum BodyError {
    InvalidSize,
}
impl std::error::Error for BodyError {}
impl std::fmt::Display for BodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSize => write!(
                f,
                "The payload size does not match the size provided in the header"
            ),
        }
    }
}

#[derive(Debug)]
pub enum PacketError {
    Header(HeaderError),
    Body(BodyError),
    Parse(Box<ParseError>),
}
impl std::error::Error for PacketError {}
impl std::fmt::Display for PacketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Header(source) => write!(f, "Error while reading packet header: {source}"),
            Self::Body(source) => write!(f, "Error while reading packet body: {source}"),
            Self::Parse(source) => write!(f, "Error while parsing packet: {source}"),
        }
    }
}

#[derive(Debug)]
pub struct ParseError(String);
impl std::error::Error for ParseError {}
impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Parse error: {}", self.0)
    }
}
impl ParseError {
    pub fn with_message<S>(message: S) -> Self
    where
        S: AsRef<str>,
    {
        Self(message.as_ref().to_string())
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum PacketType {
    // Request
    SubmitJob = 7,
    SubmitJobBG = 18,
    SubmitJobHigh = 21,
    GetStatus = 15,
    EchoReq = 16,
    OptionReq = 26,
    SubmitJobHighBG = 32,
    SubmitJobLow = 33,
    SubmitJobLowBG = 34,
    SubmitJobSched = 35,
    SubmitJobEpoch = 36,
    SubmitReduceJob = 37,
    SubmitReduceJobBG = 38,
    GetStatusUnique = 41,

    // Response
    JobCreated = 8,
    WorkStatus = 12,
    WorkComplete = 13,
    WorkFail = 14,
    EchoRes = 17,
    StatusRes = 20,
    Error = 19,
    WorkException = 25,
    OptionRes = 27,
    WorkData = 28,
    WorkWarning = 29,
    StatusResUnique = 42,
}

impl PacketType {
    pub fn is_continuous(&self) -> bool {
        use PacketType::*;
        match self {
            WorkStatus | WorkComplete | WorkFail | WorkException | WorkData | WorkWarning => true,
            _ => false,
        }
    }
}

impl PartialEq for PacketType {
    fn eq(&self, other: &Self) -> bool {
        self.clone() as i32 == other.clone() as i32
    }
}
impl TryFrom<&[u8]> for PacketType {
    type Error = PacketError;
    fn try_from(mut value: &[u8]) -> Result<Self, PacketError> {
        let value = value
            .try_get_i32()
            .map_err(|err| PacketError::Header(HeaderError::Malformatted(format!("{err}"))))?;
        PacketType::try_from(value)
    }
}
impl TryFrom<i32> for PacketType {
    type Error = PacketError;
    fn try_from(value: i32) -> Result<PacketType, PacketError> {
        match value {
            7 => Ok(PacketType::SubmitJob),
            18 => Ok(PacketType::SubmitJobBG),
            21 => Ok(PacketType::SubmitJobHigh),
            15 => Ok(PacketType::GetStatus),
            16 => Ok(PacketType::EchoReq),
            26 => Ok(PacketType::OptionReq),
            32 => Ok(PacketType::SubmitJobHighBG),
            33 => Ok(PacketType::SubmitJobLow),
            34 => Ok(PacketType::SubmitJobLowBG),
            35 => Ok(PacketType::SubmitJobSched),
            36 => Ok(PacketType::SubmitJobEpoch),
            37 => Ok(PacketType::SubmitReduceJob),
            38 => Ok(PacketType::SubmitReduceJobBG),
            41 => Ok(PacketType::GetStatusUnique),

            // Response
            8 => Ok(PacketType::JobCreated),
            12 => Ok(PacketType::WorkStatus),
            13 => Ok(PacketType::WorkComplete),
            14 => Ok(PacketType::WorkFail),
            17 => Ok(PacketType::EchoRes),
            20 => Ok(PacketType::StatusRes),
            19 => Ok(PacketType::Error),
            25 => Ok(PacketType::WorkException),
            27 => Ok(PacketType::OptionRes),
            28 => Ok(PacketType::WorkData),
            29 => Ok(PacketType::WorkWarning),
            42 => Ok(PacketType::StatusResUnique),
            unknown => Err(PacketError::Header(HeaderError::UnknownType(unknown))),
        }
    }
}
impl Into<[u8; 4]> for PacketType {
    fn into(self) -> [u8; 4] {
        let value = self as i32;
        let bytes = value.to_be_bytes();
        bytes
    }
}
impl Into<i32> for PacketType {
    fn into(self) -> i32 {
        self as i32
    }
}
impl Into<Option<WaitingType>> for PacketType {
    fn into(self) -> Option<WaitingType> {
        match self {
            Self::OptionRes => Some(WaitingType::OptionRes),
            Self::StatusResUnique => Some(WaitingType::StatusResUnique),
            Self::JobCreated => Some(WaitingType::JobCreated),
            Self::EchoRes => Some(WaitingType::EchoRes),
            Self::StatusRes => Some(WaitingType::StatusRes),
            _ => None,
        }
    }
}

const REQUEST: i32 = unsafe {
    union BytesToInt {
        bytes: [u8; 4],
        int: i32,
    }
    let bytes = BytesToInt {
        bytes: [b'\0', b'R', b'E', b'Q'],
    };
    bytes.int
};
const RESPONSE: i32 = unsafe {
    union BytesToInt {
        bytes: [u8; 4],
        int: i32,
    }
    let bytes = BytesToInt {
        bytes: [b'\0', b'R', b'E', b'S'],
    };
    bytes.int
};

#[derive(Clone, Debug)]
#[repr(i32)]
pub enum Magic {
    Request = REQUEST,   // 5391697
    Response = RESPONSE, // 5391699
}
impl PartialEq for Magic {
    fn eq(&self, other: &Self) -> bool {
        self.clone() as u32 == other.clone() as u32
    }
}

impl TryFrom<&[u8]> for Magic {
    type Error = PacketError;
    fn try_from(mut value: &[u8]) -> Result<Self, Self::Error> {
        let value = value
            .try_get_i32()
            .map_err(|err| PacketError::Header(HeaderError::Malformatted(format!("{err}"))))?
            .to_le();
        Magic::try_from(value)
    }
}
impl TryFrom<&[u8; 4]> for Magic {
    type Error = PacketError;
    fn try_from(value: &[u8; 4]) -> Result<Self, Self::Error> {
        let value_as_i32 = unsafe { *(&raw const value as *const i32) };
        Magic::try_from(value_as_i32)
    }
}
impl TryFrom<i32> for Magic {
    type Error = PacketError;
    fn try_from(value: i32) -> Result<Magic, PacketError> {
        let value = value.to_le();
        let value_as_bytes = unsafe { *(&raw const value as *const [u8; 4]) };

        match value {
            v if v == Self::Request as i32 => Ok(Magic::Request),
            v if v == Self::Response as i32 => Ok(Magic::Response),
            _ => Err(PacketError::Header(HeaderError::UnknownMagic(
                value_as_bytes.clone(),
            ))),
        }
    }
}

impl Into<[u8; 4]> for Magic {
    fn into(self) -> [u8; 4] {
        (self as i32).to_be_bytes()
    }
}
impl Into<i32> for Magic {
    fn into(self) -> i32 {
        (self as i32).to_be()
    }
}

#[derive(Clone, Debug)]
pub struct Header {
    magic: Magic,
    packet_type: PacketType,
    size: i32,
}
impl Header {
    pub fn is_res(&self) -> bool {
        self.magic == Magic::Response
    }
    pub fn is_req(&self) -> bool {
        self.magic == Magic::Request
    }
    pub fn is_type(&self, t: PacketType) -> bool {
        self.packet_type == t
    }
    pub fn get_type(&self) -> PacketType {
        self.packet_type
    }
    /// Returns the size of the body
    pub fn size(&self) -> usize {
        self.size as usize
    }
    /// Returns the size of the body and the header combined
    pub fn total_size(&self) -> usize {
        self.size as usize + 12
    }
}
impl TryFrom<&[u8]> for Header {
    type Error = PacketError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut chunks = value.chunks_exact(4);
        if chunks.len() != 3 {
            Err(PacketError::Header(HeaderError::InvalidSize))
        } else {
            let magic = chunks.next().expect("Size has been validated before");
            let packet_type = chunks.next().expect("Size has been validated before");
            let mut size = chunks.next().expect("Size has been validated before");
            Ok(Self {
                magic: Magic::try_from(magic)?,
                packet_type: PacketType::try_from(packet_type)?,
                size: size.try_get_i32().map_err(|err| {
                    PacketError::Header(HeaderError::Malformatted(format!("{err}")))
                })?,
            })
        }
    }
}
impl Into<Bytes> for Header {
    fn into(self) -> Bytes {
        let mut bytes: [u8; 12] = [0; 12];
        let magic: [u8; 4] = self.magic.into();
        let packet_type: [u8; 4] = self.packet_type.into();
        bytes[0..4].copy_from_slice(&magic);
        bytes[4..8].copy_from_slice(&packet_type);
        bytes[8..12].copy_from_slice(self.size.to_be_bytes().as_slice());
        Bytes::copy_from_slice(bytes.as_slice())
    }
}

#[derive(Clone, Debug)]
pub struct Body {
    arguments: Vec<Bytes>,
}
impl Body {
    pub fn size(&self) -> usize {
        self.arguments.iter().map(|arg| arg.len()).sum::<usize>() + self.arguments.len() - 1
    }
    pub fn arguments(&self) -> &[Bytes] {
        self.arguments.as_slice()
    }
}
impl TryFrom<&[u8]> for Body {
    type Error = PacketError;
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let arguments = bytes
            .split(|&byte| byte == '\0' as u8)
            .map(|bytes| Bytes::copy_from_slice(bytes))
            .collect();

        Ok(Body { arguments })
    }
}
impl From<&str> for Body {
    fn from(value: &str) -> Self {
        Body::from(value.to_string())
    }
}
impl From<String> for Body {
    fn from(value: String) -> Self {
        Body {
            arguments: vec![Bytes::from(value)],
        }
    }
}
impl From<&[String]> for Body {
    fn from(value: &[String]) -> Self {
        let arguments = value.iter().map(|v| Bytes::from(v.to_owned())).collect();

        Body { arguments }
    }
}
impl From<&[Bytes]> for Body {
    fn from(value: &[Bytes]) -> Self {
        Body {
            arguments: value.to_owned(),
        }
    }
}
impl From<Vec<Bytes>> for Body {
    fn from(value: Vec<Bytes>) -> Self {
        Body { arguments: value }
    }
}

impl From<&[&str]> for Body {
    fn from(value: &[&str]) -> Self {
        Body::from(
            value
                .iter()
                .map(|&s| s.to_owned())
                .collect::<Vec<String>>()
                .as_slice(),
        )
    }
}
impl Into<Bytes> for Body {
    fn into(self) -> Bytes {
        Bytes::from(self.arguments.join(&('\0' as u8)))
    }
}

#[derive(Clone, Debug)]
pub struct Packet {
    header: Header,
    body: Body,
}
impl Packet {
    pub(crate) fn new(header: Header, body: Body) -> Self {
        Packet { header, body }
    }
    #[cfg(test)]
    pub(crate) fn try_new(header: Header, body: Body) -> Result<Self, PacketError> {
        if header.size as usize != body.size() {
            return Err(PacketError::Body(BodyError::InvalidSize));
        }

        Ok(Packet { header, body })
    }
}
impl Packet {
    pub fn header(&self) -> &Header {
        &self.header
    }
    pub fn body(&self) -> &Body {
        &self.body
    }
}

impl<P> From<P> for Packet
where
    P: IntoPacket,
{
    fn from(value: P) -> Self {
        let body = Body {
            arguments: value.get_arguments(),
        };
        let header = Header {
            magic: P::get_magic(),
            packet_type: P::get_type(),
            size: body.size() as i32,
        };
        Self { header, body }
    }
}
impl TryFrom<&[u8]> for Packet {
    type Error = PacketError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let (header, body) = value.split_at(12);
        let header = Header::try_from(header)?;
        let body = Body::try_from(body)?;
        if header.size as usize != body.size() {
            return Err(PacketError::Body(BodyError::InvalidSize));
        }

        Ok(Packet { header, body })
    }
}
impl TryFrom<Bytes> for Packet {
    type Error = PacketError;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        <Self as TryFrom<&[u8]>>::try_from(&value)
    }
}

impl Into<Bytes> for Packet {
    fn into(self) -> Bytes {
        let header: Bytes = Header::into(self.header);
        let body: Bytes = Body::into(self.body);

        Bytes::from([header, body].concat())
    }
}
impl Into<Bytes> for &Packet {
    fn into(self) -> Bytes {
        let header: Bytes = Header::into(self.header.clone());
        let body: Bytes = Body::into(self.body.clone());

        Bytes::from([header, body].concat())
    }
}

#[allow(unused)] // TODO: do I really need get_magic and get_type`?
pub trait FromPacket
where
    Self: Sized,
{
    fn get_magic() -> Magic;
    fn get_type() -> PacketType;
    fn from_packet(packet: Packet) -> Result<Self, ParseError>;
}

pub trait IntoPacket {
    fn get_magic() -> Magic;
    fn get_type() -> PacketType;
    fn get_arguments(&self) -> Vec<Bytes>;
    fn to_packet(&self) -> Packet {
        let body = Body::from(self.get_arguments());
        let header = Header {
            magic: Self::get_magic(),
            packet_type: Self::get_type(),
            size: body.size() as i32,
        };

        let packet = Packet { header, body };
        packet
    }
}

#[cfg(test)]
mod tests {
    use super::{Body, Header, Magic, Packet, PacketType};

    #[test]
    fn test_packet_request() {
        let expected_body = b"Echo this text";
        let size = (expected_body.len() as i32).to_be_bytes();
        let packet_type: [u8; 4] = PacketType::SubmitJob.into();
        let magic: [u8; 4] = Magic::Request.into();
        let mut expected_bytes: Vec<u8> = [magic, packet_type, size].concat();
        expected_bytes.append(&mut Vec::from(expected_body));
        let expected_bytes = bytes::Bytes::from(expected_bytes);

        let body = Body::from("Echo this text");
        let header = Header {
            magic: Magic::Request,
            packet_type: PacketType::SubmitJob,
            size: body.size() as i32,
        };

        let packet = Packet::try_new(header, body).expect("Invalid body and header");

        let packet_bytes: bytes::Bytes = packet.clone().into();
        // println!("{expected_bytes:?}");
        // println!("{packet_bytes:?}");
        assert!(packet_bytes == expected_bytes, "Bytes are incorrect");

        let parsed_packet = Packet::try_from(packet_bytes).expect("Incorrect bytes");

        assert!(parsed_packet.header.magic == packet.header.magic);
        assert!(parsed_packet.header.packet_type == packet.header.packet_type);
        assert!(parsed_packet.header.size == packet.header.size);

        assert!(parsed_packet.body.arguments == parsed_packet.body.arguments);
    }
    // fn test_packet_request() {
    //
    // }
    // fn test_packet_response() {
    //
    // }
}
