use bytes::Bytes;

use crate::options::Options;
use crate::packet::FromPacket;
use crate::packet::IntoPacket;
use crate::packet::Magic;
use crate::packet::Packet;
use crate::packet::PacketType;
use crate::packet::ParseError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    /// The command received is not recognized.
    UnknownCommand,
    /// The packet received was not expected in this context.
    UnexpectedPacket,
    /// The magic number in the packet is invalid.
    InvalidMagic,
    /// The command in the packet is invalid.
    InvalidCommand,
    /// The packet structure is invalid.
    InvalidPacket,
    /// The function name is missing in the request.
    NoFunctionName,
    /// The unique ID is missing in the request.
    NoUniqueId,
    /// The job queue is full.
    JobQueueFull,
    /// The server is shutting down.
    ServerShutdown,
    /// An unknown error code was received.
    Unknown(String),
}
impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownCommand => write!(f, "Unknown Command"),
            Self::UnexpectedPacket => write!(f, "Unexpected Packet"),
            Self::InvalidMagic => write!(f, "Invalid Magic"),
            Self::InvalidCommand => write!(f, "Invalid Command"),
            Self::InvalidPacket => write!(f, "Invalid Packet"),
            Self::NoFunctionName => write!(f, "No Function Name"),
            Self::NoUniqueId => write!(f, "No Unique ID"),
            Self::JobQueueFull => write!(f, "Job Queue Full"),
            Self::ServerShutdown => write!(f, "Server Shutdown"),
            Self::Unknown(err) => write!(f, "Unknown: {err}"),
        }
    }
}
impl<T> From<T> for ErrorCode
where
    T: AsRef<str>,
{
    fn from(code: T) -> Self {
        match code.as_ref() {
            "ERR_UNKNOWN_COMMAND" => Self::UnknownCommand,
            "ERR_UNEXPECTED_PACKET" => Self::UnexpectedPacket,
            "ERR_INVALID_MAGIC" => Self::InvalidMagic,
            "ERR_INVALID_COMMAND" => Self::InvalidCommand,
            "ERR_INVALID_PACKET" => Self::InvalidPacket,
            "ERR_NO_FUNCTION_NAME" => Self::NoFunctionName,
            "ERR_NO_UNIQUE_ID" => Self::NoUniqueId,
            "ERR_JOB_QUEUE_FULL" => Self::JobQueueFull,
            "ERR_SERVER_SHUTDOWN" => Self::ServerShutdown,
            other => Self::Unknown(other.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct Error {
    error_code: ErrorCode,
    error_message: String,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error_code, self.error_message)
    }
}
impl FromPacket for Error {
    fn get_type() -> PacketType {
        PacketType::JobCreated
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let args = packet.body().arguments();
        if args.len() != 2 {
            return Err(ParseError::with_message(
                "Error packet has invalid arguments",
            ));
        }
        let error_code = String::from_utf8(args[0].to_owned().to_vec()).map_err(|err| {
            ParseError::with_message(format!("Error packet has non-utf8 error code: {err}"))
        })?;
        let error = String::from_utf8(args[0].to_owned().to_vec()).map_err(|err| {
            ParseError::with_message(format!("Error packet has non-utf8 error message: {err}"))
        })?;

        Ok(Self {
            error_code: ErrorCode::from(error_code),
            error_message: error,
        })
    }
}

#[derive(Debug)]
pub struct EchoRes {
    payload: Bytes,
}
impl EchoRes {
    pub(super) fn as_bytes(self) -> Bytes {
        self.payload
    }

    #[cfg(test)]
    pub fn new(payload: Bytes) -> Self {
        Self { payload }
    }
}
impl IntoPacket for EchoRes {
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn get_type() -> PacketType {
        PacketType::EchoRes
    }
    fn get_arguments(&self) -> Vec<Bytes> {
        vec![self.payload.clone()]
    }
}
impl FromPacket for EchoRes {
    fn get_type() -> PacketType {
        PacketType::EchoRes
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let payload = packet
            .body()
            .arguments()
            .first()
            .ok_or(ParseError::with_message("EchoRes Packet has no arguments"))?
            .to_owned();
        Ok(Self { payload })
    }
}

#[derive(Debug)]
pub struct JobCreated {
    job_handle: Bytes,
}
impl JobCreated {
    pub fn take_handle(self) -> Bytes {
        self.job_handle
    }
    #[cfg(test)]
    pub fn new(job_handle: Bytes) -> Self {
        Self { job_handle }
    }
}
#[cfg(test)]
impl IntoPacket for JobCreated {
    fn get_type() -> PacketType {
        PacketType::JobCreated
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn get_arguments(&self) -> Vec<Bytes> {
        vec![self.job_handle.clone()]
    }
}
impl FromPacket for JobCreated {
    fn get_type() -> PacketType {
        PacketType::JobCreated
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let job_handle = packet
            .body()
            .arguments()
            .first()
            .ok_or(ParseError::with_message(
                "Job Created packet has no arguments",
            ))?
            .to_owned();
        Ok(Self { job_handle })
    }
}

#[derive(Clone, Debug)]
pub struct WorkComplete {
    job_handle: Bytes,
    response: Bytes, // Opaque
}
impl WorkComplete {
    #[cfg(test)]
    pub fn new(job_handle: Bytes, response: Bytes) -> Self {
        Self {
            job_handle,
            response,
        }
    }
    pub fn get_job_handle(&self) -> &Bytes {
        &self.job_handle
    }
    /// Returns the opaque complete payload crated by the worker
    pub fn get_opaque_response(&self) -> &Bytes {
        &self.response
    }
    /// Takes the opaque complete payload crated by the worker
    pub fn take_opaque_response(self) -> Bytes {
        self.response
    }
}
#[cfg(test)]
impl IntoPacket for WorkComplete {
    fn get_arguments(&self) -> Vec<Bytes> {
        vec![self.job_handle.clone(), self.response.clone()]
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn get_type() -> PacketType {
        PacketType::WorkComplete
    }
}
impl FromPacket for WorkComplete {
    fn get_type() -> PacketType {
        PacketType::WorkComplete
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let args = packet.body().arguments();
        if args.len() != 2 {
            return Err(ParseError::with_message(
                "Work Complete packet has invalid arguments",
            ));
        }

        let job_handle = args[0].to_owned();
        let response = args[1].to_owned();
        Ok(Self {
            job_handle,
            response,
        })
    }
}

#[derive(Clone)]
pub struct WorkFail {
    job_handle: Bytes,
}
impl WorkFail {
    pub fn get_job_handle(&self) -> &Bytes {
        &self.job_handle
    }
}
impl FromPacket for WorkFail {
    fn get_type() -> PacketType {
        PacketType::WorkFail
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let job_handle = packet
            .body()
            .arguments()
            .first()
            .ok_or(ParseError::with_message(
                "Work Fail packet has no arguments",
            ))?
            .to_owned();
        Ok(Self { job_handle })
    }
}

#[derive(Clone, Debug)]
pub struct WorkException {
    _job_handle: Bytes,
    exception: Bytes, // Opaque
}
impl WorkException {
    pub fn get_job_handle(&self) -> &Bytes {
        &self.exception
    }
    /// Returns the exception payload that the worker generated
    pub fn get_opaque_response(&self) -> &Bytes {
        &self.exception
    }
}
impl FromPacket for WorkException {
    fn get_type() -> PacketType {
        PacketType::WorkException
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let args = packet.body().arguments();
        if args.len() != 2 {
            return Err(ParseError::with_message(
                "Work Exception packet has invalid arguments",
            ));
        }

        let job_handle = args[0].to_owned();
        let exception = args[1].to_owned();
        Ok(Self {
            _job_handle: job_handle,
            exception,
        })
    }
}

pub struct WorkWarning {
    job_handle: Bytes,
    warning: Bytes, // Opaque
}
impl WorkWarning {
    pub fn get_job_handle(&self) -> &Bytes {
        &self.job_handle
    }
}
impl WorkWarning {
    #[allow(unused)] // TODO: This will be needed for status updates
    fn get_opaque_response(&self) -> &Bytes {
        &self.warning
    }
}
impl FromPacket for WorkWarning {
    fn get_type() -> PacketType {
        PacketType::WorkWarning
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let args = packet.body().arguments();
        if args.len() != 2 {
            return Err(ParseError::with_message(
                "Work Warning packet has invalid arguments",
            ));
        }

        let job_handle = args[0].to_owned();
        let warning = args[1].to_owned();
        Ok(Self {
            job_handle,
            warning,
        })
    }
}

pub struct WorkData {
    job_handle: Bytes,
    data: Bytes, // Opaque
}
impl WorkData {
    pub fn get_job_handle(&self) -> &Bytes {
        &self.job_handle
    }
}
impl WorkData {
    #[allow(unused)] // TODO: This will be needed for status updates
    fn get_opaque_response(&self) -> &Bytes {
        &self.data
    }
}
impl FromPacket for WorkData {
    fn get_type() -> PacketType {
        PacketType::WorkData
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let args = packet.body().arguments();
        if args.len() != 2 {
            return Err(ParseError::with_message(
                "Work Data packet has invalid arguments",
            ));
        }

        let job_handle = args[0].to_owned();
        let data = args[1].to_owned();
        Ok(Self { job_handle, data })
    }
}

#[allow(unused)]
pub struct OptionRes {
    option: Options, // TODO: This will be needed for status updates
}
impl FromPacket for OptionRes {
    fn get_type() -> PacketType {
        PacketType::OptionRes
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, ParseError> {
        let args = packet.body().arguments();
        let option = args
            .first()
            .ok_or(ParseError::with_message("Option packet has no argument"))?;
        let option = Options::try_from(option).map_err(|err| {
            ParseError::with_message(format!("Unable to parse Option packet: {err}"))
        })?;

        Ok(Self { option })
    }
}

pub struct WorkStatus {
    job_handle: Bytes,
    numerator: u64,
    denominator: u64,
}
impl WorkStatus {
    pub fn get_job_handle(&self) -> &Bytes {
        &self.job_handle
    }
    pub fn numerator(&self) -> u64 {
        self.numerator
    }
    pub fn denominator(&self) -> u64 {
        self.denominator
    }
}
impl FromPacket for WorkStatus {
    fn get_type() -> PacketType {
        PacketType::WorkStatus
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let args = packet.body().arguments();
        if args.len() != 3 {
            return Err(ParseError::with_message(
                "Work Status packet has invalid arguments",
            ));
        }

        let job_handle = args[0].to_owned();
        let numerator: u64 = String::from_utf8(args[1].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "Work Status packet has non-utf8 numerator value: {err}"
                ))
            })?
            .parse()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "Work Status packet numerator is not valid 64 bit number: {err}"
                ))
            })?;
        let denominator: u64 = String::from_utf8(args[2].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "Work Status packet has non-utf8 denominator value: {err}"
                ))
            })?
            .parse()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "Work Status packet denominator is not valid 64 bit number: {err}"
                ))
            })?;
        Ok(Self {
            job_handle,
            numerator,
            denominator,
        })
    }
}

#[allow(unused)]
pub struct StatusRes {
    job_handle: Bytes,
    known_status: bool,   // 0 or 1
    running_status: bool, // 0 or 1
    numerator: u64,
    denominator: u64,
}

impl FromPacket for StatusRes {
    fn get_type() -> PacketType {
        PacketType::StatusRes
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let args = packet.body().arguments();
        if args.len() != 5 {
            return Err(ParseError::with_message(
                "StatusRes packet has invalid arguments",
            ));
        }

        let job_handle = args[0].to_owned();
        let known_status: u64 = String::from_utf8(args[1].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusRes packet has non-utf8 known status value: {err}"
                ))
            })?
            .parse::<u64>()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusRes packet known status is not valid 64 bit number: {err}"
                ))
            })?;
        let known_status = match known_status {
            0 => false,
            1 => true,
            _ => {
                return Err(ParseError::with_message(
                    "StatusRes known status is not a valid bool",
                ));
            }
        };
        let running_status: u64 = String::from_utf8(args[2].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusRes packet has non-utf8 running status value: {err}"
                ))
            })?
            .parse::<u64>()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusRes packet running status is not valid 64 bit number: {err}"
                ))
            })?;
        let running_status = match running_status {
            0 => false,
            1 => true,
            _ => {
                return Err(ParseError::with_message(
                    "StatusRes running status is not a valid bool",
                ));
            }
        };
        let numerator: u64 = String::from_utf8(args[3].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusRes packet has non-utf8 numerator value: {err}"
                ))
            })?
            .parse::<u64>()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusRes packet numerator is not valid 64 bit number: {err}"
                ))
            })?;
        let denominator: u64 = String::from_utf8(args[4].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusRes packet has non-utf8 denominator value: {err}"
                ))
            })?
            .parse()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusRes packet denominator is not valid 64 bit number: {err}"
                ))
            })?;
        Ok(Self {
            job_handle,
            known_status,
            running_status,
            numerator,
            denominator,
        })
    }
}

#[allow(unused)] // TODO
pub struct StatusResUnique {
    pub job_handle: Bytes,
    pub known_status: bool,   // 0 or 1
    pub running_status: bool, // 0 or 1
    numerator: u64,
    denominator: u64,
    waiting_clients_count: u64,
}

impl FromPacket for StatusResUnique {
    fn get_type() -> PacketType {
        PacketType::StatusResUnique
    }
    fn get_magic() -> Magic {
        Magic::Response
    }
    fn from_packet(packet: Packet) -> Result<Self, super::packet::ParseError> {
        let args = packet.body().arguments();
        if args.len() != 6 {
            return Err(ParseError::with_message(
                "StatusResUnique packet has invalid arguments",
            ));
        }

        let job_handle = args[0].to_owned();
        let known_status: u64 = String::from_utf8(args[1].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet has non-utf8 known status value: {err}"
                ))
            })?
            .parse::<u64>()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet known status is not valid 64 bit number: {err}"
                ))
            })?;
        let known_status = match known_status {
            0 => false,
            1 => true,
            _ => {
                return Err(ParseError::with_message(
                    "StatusResUnique known status is not a valid bool",
                ));
            }
        };
        let running_status: u64 = String::from_utf8(args[2].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet has non-utf8 running status value: {err}"
                ))
            })?
            .parse::<u64>()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet running status is not valid 64 bit number: {err}"
                ))
            })?;
        let running_status = match running_status {
            0 => false,
            1 => true,
            _ => {
                return Err(ParseError::with_message(
                    "StatusResUnique running status is not a valid bool",
                ));
            }
        };
        let numerator: u64 = String::from_utf8(args[3].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet has non-utf8 numerator value: {err}"
                ))
            })?
            .parse::<u64>()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet numerator is not valid 64 bit number: {err}"
                ))
            })?;
        let denominator: u64 = String::from_utf8(args[4].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet has non-utf8 denominator value: {err}"
                ))
            })?
            .parse()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet denominator is not valid 64 bit number: {err}"
                ))
            })?;
        let waiting_clients_count: u64 = String::from_utf8(args[4].to_owned().to_vec())
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet has non-utf8 wating_clients_count value: {err}"
                ))
            })?
            .parse()
            .map_err(|err| {
                ParseError::with_message(format!(
                    "StatusResUnique packet waiting_clients_count is not valid 64 bit number: {err}"
                ))
            })?;
        Ok(Self {
            job_handle,
            known_status,
            running_status,
            numerator,
            denominator,
            waiting_clients_count,
        })
    }
}
