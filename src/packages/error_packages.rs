use crate::{MessageError, wire::FromRaw};

#[derive(Debug, Clone)]
pub struct ServerError {
    pub code: ErrorCode,
    pub message: String,
}

impl FromRaw for ServerError {
    fn from_raw(
        raw: crate::wire::RawPacket,
    ) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        let mut arguments = raw.arguments();

        let code: ErrorCode =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 0,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "code",
                reason: err.to_string(),
            })?
            .into();
        let message =
            std::str::from_utf8(arguments.next().ok_or(MessageError::WrongArgumentCount {
                expected: 2,
                found: 1,
            })?)
            .map_err(|err| MessageError::InvalidField {
                field: "message",
                reason: err.to_string(),
            })?
            .into();

        Ok(crate::wire::ServerMessage::Error(ServerError {
            code,
            message,
        }))
    }
}

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
impl ErrorCode {
    pub fn string_representation(&self) -> String {
        match self {
            Self::UnknownCommand => "ERR_UNKNOWN_COMMAND",
            Self::UnexpectedPacket => "ERR_UNEXPECTED_PACKET",
            Self::InvalidMagic => "ERR_INVALID_MAGIC",
            Self::InvalidCommand => "ERR_INVALID_COMMAND",
            Self::InvalidPacket => "ERR_INVALID_PACKET",
            Self::NoFunctionName => "ERR_NO_FUNCTION_NAME",
            Self::NoUniqueId => "ERR_NO_UNIQUE_ID",
            Self::JobQueueFull => "ERR_JOB_QUEUE_FULL",
            Self::ServerShutdown => "ERR_SERVER_SHUTDOWN",
            Self::Unknown(other) => other,
        }
        .to_string()
    }
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
