use bytes::Bytes;

use crate::{
    messages::job::{Foreground, Priority},
    wire::{FromRaw, Header, IntoRaw, MessageError, PacketType, RawPacket, ServerMessage},
};

#[derive(Debug, Clone)]
pub struct SubmitJob {
    pub function_name: String,
    pub unique_id: String,
    pub payload: bytes::Bytes,
    pub priority: Priority,
    pub foreground: Foreground,
}

impl IntoRaw for SubmitJob {
    fn into_raw(self) -> RawPacket {
        let body = bytes::Bytes::from(
            [
                self.function_name.as_bytes(),
                self.unique_id.as_bytes(),
                &self.payload,
            ]
            .join(&b'\0'),
        );
        let packet_type = match (self.priority, self.foreground) {
            (Priority::Low, Foreground::Yes) => PacketType::SubmitJobLow,
            (Priority::Low, Foreground::No) => PacketType::SubmitJobLowBg,
            (Priority::Normal, Foreground::Yes) => PacketType::SubmitJob,
            (Priority::Normal, Foreground::No) => PacketType::SubmitJobBg,
            (Priority::High, Foreground::Yes) => PacketType::SubmitJobHigh,
            (Priority::High, Foreground::No) => PacketType::SubmitJobHighBg,
        };
        let header = Header {
            magic: crate::wire::Magic::Request,
            packet_type,
            body_len: body.len() as i32,
        };

        RawPacket { header, body }
    }
}

#[derive(Debug, Clone)]
pub struct JobCreated {
    pub job_handle: Bytes,
}

impl FromRaw for JobCreated {
    fn from_raw(
        raw: crate::wire::RawPacket,
    ) -> Result<crate::wire::ServerMessage, crate::wire::MessageError> {
        Ok(ServerMessage::JobCreated(Self {
            job_handle: Bytes::copy_from_slice(raw.arguments().next().ok_or(
                MessageError::WrongArgumentCount {
                    expected: 1,
                    found: 0,
                },
            )?),
        }))
    }
}

// NOTE: This is not currently used in the protocol
//
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

// TODO: implement this packet type
// pub struct SubmitReduceJob {
//     function_name: Bytes,
//     uid: Uuid,
//     reducer_function: Bytes,
//     payload: Bytes,
// }
//
// pub struct SubmitReduceJobBG {
//     function_name: Bytes,
//     uid: Uuid,
//     reducer_function: Bytes,
//     payload: Bytes,
// }
