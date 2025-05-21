use bytes::Bytes;
use uuid::Uuid;

use crate::options::Options;
use crate::packet::FromPacket;
use crate::packet::IntoPacket;
use crate::packet::Magic;
use crate::packet::PacketType;
use crate::packet::ParseError;

pub struct EchoReq {
    payload: Bytes,
}
impl EchoReq {
    pub(super) fn new(payload: Bytes) -> Self {
        Self { payload }
    }
    #[cfg(test)]
    pub fn as_payload(self) -> Bytes {
        self.payload
    }
}
impl FromPacket for EchoReq {
    fn get_type() -> PacketType {
        PacketType::EchoReq
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
    fn from_packet(packet: super::packet::Packet) -> Result<Self, super::packet::ParseError> {
        Ok(Self {
            payload: packet
                .body()
                .arguments()
                .first()
                .take()
                .ok_or(ParseError::with_message("Failed to parse packet"))?
                .clone(),
        })
    }
}
impl IntoPacket for EchoReq {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        vec![self.payload.clone()]
    }
    fn get_type() -> PacketType {
        PacketType::EchoReq
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

pub struct SubmitJob {
    function_name: Bytes,
    uid: Uuid,
    payload: Bytes,
}
impl SubmitJob {
    pub fn new(function_name: String, payload: Bytes) -> Self {
        Self {
            function_name: Bytes::from(function_name),
            uid: Uuid::new_v4(),
            payload,
        }
    }
    pub fn get_uid(&self) -> String {
        self.uid.to_string()
    }
    #[cfg(test)]
    pub fn get_payload(&self) -> &Bytes {
        &self.payload
    }
}
#[cfg(test)]
impl FromPacket for SubmitJob {
    fn get_magic() -> Magic {
        Magic::Request
    }
    fn get_type() -> PacketType {
        PacketType::SubmitJob
    }
    fn from_packet(packet: super::packet::Packet) -> Result<Self, ParseError> {
        let arguments = packet.body().arguments();
        let function_name = arguments
            .get(0)
            .ok_or(ParseError::with_message("Arguments payload is too short"))?
            .clone();
        let uid = Uuid::from_slice(
            &arguments
                .get(1)
                .ok_or(ParseError::with_message("Arguments payload is too short"))?,
        )
        .map_err(|err| ParseError::with_message(format!("Invalid Uid: {}", err)))?;
        let payload = arguments
            .get(2)
            .ok_or(ParseError::with_message("Arguments payload is too short"))?
            .clone();

        Ok(Self {
            function_name,
            uid,
            payload,
        })
    }
}
impl IntoPacket for SubmitJob {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        let uid = Bytes::from(self.uid.as_bytes().to_vec());
        vec![self.function_name.clone(), uid, self.payload.clone()]
    }
    fn get_type() -> PacketType {
        PacketType::SubmitJob
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

pub struct SubmitJobBG {
    function_name: Bytes,
    uid: Uuid,
    payload: Bytes,
}
impl SubmitJobBG {
    pub fn new(function_name: String, payload: Bytes) -> Self {
        Self {
            function_name: Bytes::from(function_name),
            uid: Uuid::new_v4(),
            payload,
        }
    }
    pub fn get_uid(&self) -> String {
        self.uid.to_string()
    }
}
impl IntoPacket for SubmitJobBG {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        let uid = Bytes::from(self.uid.as_bytes().to_vec());
        vec![self.function_name.clone(), uid, self.payload.clone()]
    }
    fn get_type() -> PacketType {
        PacketType::SubmitJobBG
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

pub struct SubmitJobHigh {
    function_name: Bytes,
    uid: Uuid,
    payload: Bytes,
}
impl SubmitJobHigh {
    pub fn new(function_name: String, payload: Bytes) -> Self {
        Self {
            function_name: Bytes::from(function_name),
            uid: Uuid::new_v4(),
            payload,
        }
    }
    pub fn get_uid(&self) -> String {
        self.uid.to_string()
    }
}
impl IntoPacket for SubmitJobHigh {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        let uid = Bytes::from(self.uid.as_bytes().to_vec());
        vec![self.function_name.clone(), uid, self.payload.clone()]
    }
    fn get_type() -> PacketType {
        PacketType::SubmitJobHigh
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

pub struct SubmitJobHighBG {
    function_name: Bytes,
    uid: Uuid,
    payload: Bytes,
}
impl SubmitJobHighBG {
    pub fn new(function_name: String, payload: Bytes) -> Self {
        Self {
            function_name: Bytes::from(function_name),
            uid: Uuid::new_v4(),
            payload,
        }
    }
    pub fn get_uid(&self) -> String {
        self.uid.to_string()
    }
}
impl IntoPacket for SubmitJobHighBG {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        let uid = Bytes::from(self.uid.as_bytes().to_vec());
        vec![self.function_name.clone(), uid, self.payload.clone()]
    }
    fn get_type() -> PacketType {
        PacketType::SubmitJobHighBG
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

pub struct SubmitJobLow {
    function_name: Bytes,
    uid: Uuid,
    payload: Bytes,
}
impl SubmitJobLow {
    pub fn new(function_name: String, payload: Bytes) -> Self {
        Self {
            function_name: Bytes::from(function_name),
            uid: Uuid::new_v4(),
            payload,
        }
    }
    pub fn get_uid(&self) -> String {
        self.uid.to_string()
    }
}
impl IntoPacket for SubmitJobLow {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        let uid = Bytes::from(self.uid.as_bytes().to_vec());
        vec![self.function_name.clone(), uid, self.payload.clone()]
    }
    fn get_type() -> PacketType {
        PacketType::SubmitJobLow
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

pub struct SubmitJobLowBG {
    function_name: Bytes,
    uid: Uuid,
    payload: Bytes,
}
impl SubmitJobLowBG {
    pub fn new(function_name: String, payload: Bytes) -> Self {
        Self {
            function_name: Bytes::from(function_name),
            uid: Uuid::new_v4(),
            payload,
        }
    }
    pub fn get_uid(&self) -> String {
        self.uid.to_string()
    }
}
impl IntoPacket for SubmitJobLowBG {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        let uid = Bytes::from(self.uid.as_bytes().to_vec());
        vec![self.function_name.clone(), uid, self.payload.clone()]
    }
    fn get_type() -> PacketType {
        PacketType::SubmitJobLowBG
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

// NOTE: This is not currently used in the protocol
// pub struct SubmitJobSched {
//     function_name: Bytes,
//     uid: Uuid,
//     minute: u8, // 0-59
//     hour: u8, // 0-23
//     day_of_month: u8, // 1-31
//     day_of_week: u8, // 0-6, 0 = Monday
//     payload: Bytes,
// }
//
// impl IntoPacket for SubmitJobSched {
//     fn get_arguments(&self) -> Vec<bytes::Bytes> {
//         let uid = Bytes::from(self.uid.as_bytes().to_vec());
//         let minute = Bytes::copy_from_slice(&self.minute.to_ne_bytes());
//         let hour = Bytes::copy_from_slice(&self.hour.to_ne_bytes());
//         let day_of_month = Bytes::copy_from_slice(&self.day_of_month.to_ne_bytes());
//         let day_of_week = Bytes::copy_from_slice(&self.day_of_week.to_ne_bytes());
//         vec![self.function_name.clone(), uid, minute, hour, day_of_month, day_of_week, self.payload.clone()]
//     }
//     fn get_type() -> PacketType {
//         PacketType::SubmitJobSched
//     }
//     fn get_magic() -> Magic {
//         Magic::Request
//     }
// }

// NOTE: This is not currently used in the protocol
// pub struct SubmitJobEpoch {
//     function_name: Bytes,
//     uid: Uuid,
//     epoch: u128,
//     payload: Bytes,
// }
// impl IntoPacket for SubmitJobEpoch {
//     fn get_arguments(&self) -> Vec<bytes::Bytes> {
//         let uid = Bytes::from(self.uid.as_bytes().to_vec());
//         let epoch = Bytes::copy_from_slice(&self.epoch.to_ne_bytes());
//         vec![self.function_name.clone(), uid, epoch, self.payload.clone()]
//     }
//     fn get_type() -> PacketType {
// PacketType::SubmitJobEpoch
//     }
//     fn get_magic() -> Magic {
//         Magic::Request
//     }
// }

pub struct SubmitReduceJob {
    function_name: Bytes,
    uid: Uuid,
    reducer_function: Bytes,
    payload: Bytes,
}
impl IntoPacket for SubmitReduceJob {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        let uid = Bytes::from(self.uid.as_bytes().to_vec());
        vec![
            self.function_name.clone(),
            uid,
            self.reducer_function.clone(),
            self.payload.clone(),
        ]
    }
    fn get_type() -> PacketType {
        PacketType::SubmitReduceJob
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

pub struct SubmitReduceJobBG {
    function_name: Bytes,
    uid: Uuid,
    reducer_function: Bytes,
    payload: Bytes,
}
impl IntoPacket for SubmitReduceJobBG {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        let uid = Bytes::from(self.uid.as_bytes().to_vec());
        vec![
            self.function_name.clone(),
            uid,
            self.reducer_function.clone(),
            self.payload.clone(),
        ]
    }
    fn get_type() -> PacketType {
        PacketType::SubmitReduceJobBG
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

pub struct GetStatus {
    job_handle: Bytes,
}
#[allow(unused)] // TODO: Add Status updates
impl GetStatus {
    pub fn new(handle: Bytes) -> Self {
        Self { job_handle: handle }
    }
}
impl IntoPacket for GetStatus {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        vec![Bytes::from(self.job_handle.clone())]
    }
    fn get_type() -> PacketType {
        PacketType::GetStatus
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}
pub struct GetStatusUnique {
    uid: String,
}
#[allow(unused)] // TODO: Add Status updates
impl GetStatusUnique {
    pub fn new(uid: String) -> Self {
        Self { uid }
    }
}
impl IntoPacket for GetStatusUnique {
    fn get_arguments(&self) -> Vec<bytes::Bytes> {
        vec![Bytes::from(self.uid.clone())]
    }
    fn get_type() -> PacketType {
        PacketType::GetStatusUnique
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}

pub struct OptionReq {
    option: Options,
}
impl IntoPacket for OptionReq {
    fn get_arguments(&self) -> Vec<Bytes> {
        vec![self.option.clone().into()]
    }
    fn get_type() -> PacketType {
        PacketType::OptionReq
    }
    fn get_magic() -> Magic {
        Magic::Request
    }
}
