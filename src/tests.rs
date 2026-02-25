#![allow(clippy::unwrap_used)]

use crate::{
    ConnectOptions,
    connection::{Client, EventLoop, GearmanError},
    messages::job::Priority,
    packages::{
        Echo, EchoRes, GearmanOption, GetStatus, GetStatusUnique, JobCreated, OptionRes,
        ServerError, SetOption, SubmitJob, WorkComplete, WorkData, WorkException, WorkFail,
        WorkStatus, WorkWarning,
    },
    wire::{GearmanCodec, Magic, PacketType, RawPacket, ServerMessage, WireError},
};

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::sync::mpsc;
use tokio_util::codec::{Decoder, Encoder, Framed};

// ════════════════════════════════════════════════════════════════════════════
// Shared helpers
// ════════════════════════════════════════════════════════════════════════════

const REQ_MAGIC: &[u8; 4] = b"\0REQ";
const RES_MAGIC: &[u8; 4] = b"\0RES";
const HEADER_LEN: usize = 12;

/// Assemble a raw Gearman frame:  magic(4) | type-BE(4) | body-len-BE(4) | body.
fn frame(magic: &[u8], packet_type: i32, body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_LEN + body.len());
    buf.extend_from_slice(magic);
    buf.extend_from_slice(&packet_type.to_be_bytes());
    buf.extend_from_slice(&(body.len() as i32).to_be_bytes());
    buf.extend_from_slice(body);
    buf
}

/// Join slices with NUL bytes – the Gearman argument separator.
fn nul(parts: &[&[u8]]) -> Vec<u8> {
    parts.join(&b'\0')
}

fn jh(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

/// Drain one complete Gearman frame from `io` and return its body.
async fn drain_frame(io: &mut DuplexStream) -> Vec<u8> {
    let mut header = [0u8; 12];
    io.read_exact(&mut header).await.unwrap();
    let body_len = i32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
    let mut body = vec![0u8; body_len];
    io.read_exact(&mut body).await.unwrap();
    println!("got body len {} and body {:?}", body_len, body);
    body
}

// ════════════════════════════════════════════════════════════════════════════
// 1. RawPacket – parse / serialize / arguments
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod raw_packet_tests {
    use super::*;

    // ── parse: success path ─────────────────────────────────────────────────

    #[test]
    fn parse_request_empty_body() {
        let raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, b"");
        let pkt = RawPacket::parse(&raw).expect("parse failed");

        assert_eq!(pkt.header.magic, Magic::Request);
        assert_eq!(pkt.header.packet_type, PacketType::EchoReq);
        assert_eq!(pkt.header.body_len, 0);
        assert!(pkt.body.is_empty());
    }

    #[test]
    fn parse_request_with_body() {
        let body = nul(&[b"reverse", b"uid-1", b"hello"]);
        let raw = frame(REQ_MAGIC, PacketType::SubmitJob as i32, &body);
        let pkt = RawPacket::parse(&raw).expect("parse failed");

        assert_eq!(pkt.header.magic, Magic::Request);
        assert_eq!(pkt.header.packet_type, PacketType::SubmitJob);
        assert_eq!(pkt.header.body_len as usize, body.len());
        assert_eq!(&pkt.body[..], &body[..]);
    }

    #[test]
    fn parse_response_job_created() {
        let body = b"H:localhost:1";
        let raw = frame(RES_MAGIC, PacketType::JobCreated as i32, body);
        let pkt = RawPacket::parse(&raw).unwrap();

        assert_eq!(pkt.header.magic, Magic::Response);
        assert_eq!(pkt.header.packet_type, PacketType::JobCreated);
        assert_eq!(&pkt.body[..], body);
    }

    #[test]
    fn parse_preserves_arbitrary_binary_payload() {
        let body: Vec<u8> = (0u8..=255).collect();
        let raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, &body);
        let pkt = RawPacket::parse(&raw).unwrap();
        assert_eq!(&pkt.body[..], &body[..]);
    }

    #[test]
    fn parse_all_server_to_client_packet_types() {
        let server_types = [
            PacketType::JobCreated,
            PacketType::WorkStatus,
            PacketType::WorkComplete,
            PacketType::WorkFail,
            PacketType::WorkException,
            PacketType::WorkData,
            PacketType::WorkWarning,
            PacketType::StatusRes,
            PacketType::StatusResUnique,
            PacketType::EchoRes,
            PacketType::OptionRes,
            PacketType::Error,
        ];
        for pt in server_types {
            let raw = frame(RES_MAGIC, pt as i32, b"dummy");
            let pkt = RawPacket::parse(&raw).unwrap_or_else(|e| panic!("{pt:?} failed: {e}"));
            assert_eq!(pkt.header.packet_type, pt);
        }
    }

    // ── parse: error paths ──────────────────────────────────────────────────

    #[test]
    fn parse_empty_slice_is_incomplete() {
        let err = RawPacket::parse(&[]).unwrap_err();
        assert!(matches!(err, WireError::Incomplete { .. }), "{err:?}");
    }

    #[test]
    fn parse_partial_header_is_incomplete() {
        let raw = &frame(REQ_MAGIC, PacketType::EchoReq as i32, b"")[..8];
        let err = RawPacket::parse(raw).unwrap_err();
        assert!(matches!(err, WireError::Incomplete { .. }), "{err:?}");
    }

    #[test]
    fn parse_bad_magic_returns_unknown_magic() {
        let raw = frame(b"XXXX", PacketType::EchoReq as i32, b"");
        let err = RawPacket::parse(&raw).unwrap_err();
        assert!(matches!(err, WireError::UnknownMagic(_)), "{err:?}");
    }

    #[test]
    fn parse_unknown_packet_type() {
        let raw = frame(REQ_MAGIC, 0xDEAD_BEF, b"");
        let err = RawPacket::parse(&raw).unwrap_err();
        assert!(matches!(err, WireError::UnknownPacketType(_)), "{err:?}");
    }

    // #[test]
    // fn parse_declared_length_larger_than_actual_body() {
    //     // declare 10, supply 3
    //     let mut raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, b"abc");
    //     raw[8..12].copy_from_slice(&10u32.to_be_bytes());
    //     let err = RawPacket::parse(&raw).unwrap_err();
    //     assert!(
    //         matches!(
    //             err,
    //             WireError::LengthMismatch { .. } | WireError::Incomplete { .. }
    //         ),
    //         "{err:?}"
    //     );
    // }
    //
    // #[test]
    // fn parse_declared_length_smaller_than_actual_body() {
    //     // supply 5 bytes, declare 2
    //     let mut raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, b"hello");
    //     raw[8..12].copy_from_slice(&2u32.to_be_bytes());
    //     let err = RawPacket::parse(&raw).unwrap_err();
    //     assert!(matches!(err, WireError::LengthMismatch { .. }), "{err:?}");
    // }

    // ── serialize ───────────────────────────────────────────────────────────

    #[test]
    fn serialize_produces_parseable_output() {
        let body = nul(&[b"fn", b"uid", b"data"]);
        let raw = frame(REQ_MAGIC, PacketType::SubmitJob as i32, &body);
        let pkt = RawPacket::parse(&raw).unwrap();
        let serialized = pkt.serialize();

        let reparsed = RawPacket::parse(&serialized).unwrap();
        assert_eq!(reparsed.header.magic, pkt.header.magic);
        assert_eq!(reparsed.header.packet_type, pkt.header.packet_type);
        assert_eq!(reparsed.body, pkt.body);
    }

    #[test]
    fn serialize_request_magic_prefix() {
        let raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, b"");
        let out = RawPacket::parse(&raw).unwrap().serialize();
        assert_eq!(&out[..4], REQ_MAGIC);
    }

    #[test]
    fn serialize_response_magic_prefix() {
        let raw = frame(RES_MAGIC, PacketType::EchoRes as i32, b"ping");
        let out = RawPacket::parse(&raw).unwrap().serialize();
        assert_eq!(&out[..4], RES_MAGIC);
    }

    #[test]
    fn serialize_total_length_is_header_plus_body() {
        let body = b"payload";
        let raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, body);
        let out = RawPacket::parse(&raw).unwrap().serialize();
        assert_eq!(out.len(), HEADER_LEN + body.len());
    }

    #[test]
    fn serialize_embeds_correct_body_len_field() {
        let body = b"data";
        let raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, body);
        let out = RawPacket::parse(&raw).unwrap().serialize();
        let declared = u32::from_be_bytes(out[8..12].try_into().unwrap());
        assert_eq!(declared as usize, body.len());
    }

    #[test]
    fn serialize_empty_body() {
        let raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, b"");
        let out = RawPacket::parse(&raw).unwrap().serialize();
        assert_eq!(out.len(), HEADER_LEN);
        let declared = u32::from_be_bytes(out[8..12].try_into().unwrap());
        assert_eq!(declared, 0);
    }

    // ── arguments ───────────────────────────────────────────────────────────

    #[test]
    fn arguments_empty_body_yields_one_empty_slice() {
        let raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, b"");
        let pkt = RawPacket::parse(&raw).unwrap();
        let args: Vec<_> = pkt.arguments().collect();
        assert_eq!(args.len(), 1, "one empty segment expected");
        assert!(args[0].is_empty());
    }

    #[test]
    fn arguments_single_segment_no_nul() {
        let raw = frame(REQ_MAGIC, PacketType::EchoReq as i32, b"hello");
        let pkt = RawPacket::parse(&raw).unwrap();
        let args: Vec<_> = pkt.arguments().collect();
        assert_eq!(args, vec![&b"hello"[..]]);
    }

    #[test]
    fn arguments_three_null_separated_segments() {
        let body = nul(&[b"function_name", b"unique-id", b"payload"]);
        let raw = frame(REQ_MAGIC, PacketType::SubmitJob as i32, &body);
        let pkt = RawPacket::parse(&raw).unwrap();
        let args: Vec<_> = pkt.arguments().collect();
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], b"function_name");
        assert_eq!(args[1], b"unique-id");
        assert_eq!(args[2], b"payload");
    }

    #[test]
    fn arguments_trailing_nul_yields_empty_last_segment() {
        let mut body = nul(&[b"a", b"b"]);
        body.push(b'\0');
        let raw = frame(REQ_MAGIC, PacketType::SubmitJob as i32, &body);
        let pkt = RawPacket::parse(&raw).unwrap();
        let args: Vec<_> = pkt.arguments().collect();
        assert_eq!(args.len(), 3);
        assert!(args[2].is_empty());
    }

    #[test]
    fn arguments_preserves_all_segment_content() {
        let body = nul(&[b"fn", b"u", b"binary\xDE\xAD\xBE\xEF"]);
        let raw = frame(REQ_MAGIC, PacketType::SubmitJob as i32, &body);
        let pkt = RawPacket::parse(&raw).unwrap();
        let args: Vec<_> = pkt.arguments().collect();
        assert!(args.iter().any(|a| a.contains(&0xDE)));
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 2. GearmanCodec – encode / decode
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod codec_tests {
    use super::*;
    use crate::{messages::job::Foreground, wire::ClientMessage};

    fn encode(msg: ClientMessage) -> Bytes {
        let mut codec = GearmanCodec;
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).expect("encode failed");
        buf.freeze()
    }

    fn decode_one(raw: &[u8]) -> Option<ServerMessage> {
        let mut codec = GearmanCodec;
        let mut buf = BytesMut::from(raw);
        codec.decode(&mut buf).expect("decode error")
    }

    fn pkt_type_from(bytes: &Bytes) -> i32 {
        i32::from_be_bytes(bytes[4..8].try_into().unwrap())
    }

    // ── encode ──────────────────────────────────────────────────────────────

    #[test]
    fn encode_echo_req_header_correct() {
        let payload = b"ping";
        let bytes = encode(ClientMessage::Echo(Echo {
            payload: Bytes::from_static(payload),
        }));
        assert_eq!(&bytes[..4], REQ_MAGIC);
        assert_eq!(pkt_type_from(&bytes), PacketType::EchoReq as i32);
        let body_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
        assert_eq!(body_len, payload.len());
        assert_eq!(&bytes[12..], payload);
    }

    #[test]
    fn encode_submit_job_normal_foreground() {
        let bytes = encode(ClientMessage::SubmitJob(SubmitJob {
            function_name: "reverse".into(),
            unique_id: "uid-1".into(),
            payload: Bytes::from_static(b"hello"),
            priority: Priority::Normal,
            foreground: Foreground::Yes,
        }));
        assert_eq!(pkt_type_from(&bytes), PacketType::SubmitJob as i32);
    }

    #[test]
    fn encode_submit_job_normal_background() {
        let bytes = encode(ClientMessage::SubmitJob(SubmitJob {
            function_name: "fn".into(),
            unique_id: "u".into(),
            payload: Bytes::new(),
            priority: Priority::Normal,
            foreground: Foreground::No,
        }));
        assert_eq!(pkt_type_from(&bytes), PacketType::SubmitJobBg as i32);
    }

    #[test]
    fn encode_submit_job_high_foreground() {
        let bytes = encode(ClientMessage::SubmitJob(SubmitJob {
            function_name: "fn".into(),
            unique_id: "u".into(),
            payload: Bytes::new(),
            priority: Priority::High,
            foreground: Foreground::Yes,
        }));
        assert_eq!(pkt_type_from(&bytes), PacketType::SubmitJobHigh as i32);
    }

    #[test]
    fn encode_submit_job_high_background() {
        let bytes = encode(ClientMessage::SubmitJob(SubmitJob {
            function_name: "fn".into(),
            unique_id: "u".into(),
            payload: Bytes::new(),
            priority: Priority::High,
            foreground: Foreground::No,
        }));
        assert_eq!(pkt_type_from(&bytes), PacketType::SubmitJobHighBg as i32);
    }

    #[test]
    fn encode_submit_job_low_foreground() {
        let bytes = encode(ClientMessage::SubmitJob(SubmitJob {
            function_name: "fn".into(),
            unique_id: "u".into(),
            payload: Bytes::new(),
            priority: Priority::Low,
            foreground: Foreground::Yes,
        }));
        assert_eq!(pkt_type_from(&bytes), PacketType::SubmitJobLow as i32);
    }

    #[test]
    fn encode_submit_job_low_background() {
        let bytes = encode(ClientMessage::SubmitJob(SubmitJob {
            function_name: "fn".into(),
            unique_id: "u".into(),
            payload: Bytes::new(),
            priority: Priority::Low,
            foreground: Foreground::No,
        }));
        assert_eq!(pkt_type_from(&bytes), PacketType::SubmitJobLowBg as i32);
    }

    #[test]
    fn encode_get_status_contains_handle() {
        let bytes = encode(ClientMessage::GetStatus(GetStatus {
            job_handle: jh("H:localhost:1"),
        }));
        assert_eq!(pkt_type_from(&bytes), PacketType::GetStatus as i32);
        assert!(bytes[12..].windows(13).any(|w| w == b"H:localhost:1"));
    }

    #[test]
    fn encode_get_status_unique() {
        let bytes = encode(ClientMessage::GetStatusUnique(GetStatusUnique {
            unique_id: "my-uid".into(),
        }));
        assert_eq!(pkt_type_from(&bytes), PacketType::GetStatusUnique as i32);
        assert!(bytes[12..].windows(6).any(|w| w == b"my-uid"));
    }

    #[test]
    fn encode_set_option_exceptions_body() {
        let bytes = encode(ClientMessage::SetOption(SetOption {
            option: GearmanOption::Exceptions,
        }));
        assert_eq!(pkt_type_from(&bytes), PacketType::OptionReq as i32);
        assert!(bytes[12..].windows(10).any(|w| w == b"exceptions"));
    }

    #[test]
    fn encode_produces_parseable_raw_packet() {
        let bytes = encode(ClientMessage::Echo(Echo {
            payload: Bytes::from_static(b"test"),
        }));
        RawPacket::parse(&bytes).expect("encoded bytes must be parseable");
    }

    // ── decode ──────────────────────────────────────────────────────────────

    #[test]
    fn decode_job_created_extracts_handle() {
        let raw = frame(RES_MAGIC, PacketType::JobCreated as i32, b"H:localhost:42");
        let msg = decode_one(&raw).unwrap();
        let ServerMessage::JobCreated(jc) = msg else {
            panic!("expected JobCreated");
        };
        assert_eq!(String::from_utf8_lossy(&jc.job_handle), "H:localhost:42");
    }

    #[test]
    fn decode_echo_res_payload() {
        let raw = frame(RES_MAGIC, PacketType::EchoRes as i32, b"pong");
        let msg = decode_one(&raw).unwrap();
        let ServerMessage::EchoRes(e) = msg else {
            panic!("expected EchoRes");
        };
        assert_eq!(&e.payload[..], b"pong");
    }

    #[test]
    fn decode_work_complete_handle_and_payload() {
        let body = nul(&[b"H:localhost:1", b"result"]);
        let raw = frame(RES_MAGIC, PacketType::WorkComplete as i32, &body);
        let msg = decode_one(&raw).unwrap();
        let ServerMessage::WorkComplete(wc) = msg else {
            panic!("expected WorkComplete");
        };
        assert_eq!(String::from_utf8_lossy(&wc.job_handle), "H:localhost:1");
        assert_eq!(&wc.payload[..], b"result");
    }

    #[test]
    fn decode_work_fail() {
        let raw = frame(RES_MAGIC, PacketType::WorkFail as i32, b"H:localhost:2");
        let msg = decode_one(&raw).unwrap();
        assert!(matches!(msg, ServerMessage::WorkFail(_)));
    }

    #[test]
    fn decode_work_exception_handle_and_payload() {
        let body = nul(&[b"H:localhost:3", b"boom"]);
        let raw = frame(RES_MAGIC, PacketType::WorkException as i32, &body);
        let msg = decode_one(&raw).unwrap();
        let ServerMessage::WorkException(we) = msg else {
            panic!("expected WorkException");
        };
        assert_eq!(&we.exception[..], b"boom");
    }

    #[test]
    fn decode_work_status_numerator_denominator() {
        let body = nul(&[b"H:localhost:4", b"3", b"10"]);
        let raw = frame(RES_MAGIC, PacketType::WorkStatus as i32, &body);
        let msg = decode_one(&raw).unwrap();
        let ServerMessage::WorkStatus(ws) = msg else {
            panic!("expected WorkStatus");
        };
        assert_eq!(ws.numerator, 3);
        assert_eq!(ws.denominator, 10);
    }

    #[test]
    fn decode_work_data() {
        let body = nul(&[b"H:localhost:5", b"chunk"]);
        let raw = frame(RES_MAGIC, PacketType::WorkData as i32, &body);
        let msg = decode_one(&raw).unwrap();
        assert!(matches!(msg, ServerMessage::WorkData(_)));
    }

    #[test]
    fn decode_work_warning() {
        let body = nul(&[b"H:localhost:6", b"slow"]);
        let raw = frame(RES_MAGIC, PacketType::WorkWarning as i32, &body);
        let msg = decode_one(&raw).unwrap();
        assert!(matches!(msg, ServerMessage::WorkWarning(_)));
    }

    #[test]
    fn decode_status_res_fields() {
        // handle NUL known NUL running NUL numerator NUL denominator
        let body = nul(&[b"H:localhost:7", b"1", b"1", b"5", b"10"]);
        let raw = frame(RES_MAGIC, PacketType::StatusRes as i32, &body);
        let msg = decode_one(&raw).unwrap();
        let ServerMessage::StatusRes(sr) = msg else {
            panic!("expected StatusRes");
        };
        assert!(sr.known);
        assert!(sr.running);
        assert_eq!(sr.numerator, 5);
        assert_eq!(sr.denominator, 10);
    }

    #[test]
    fn decode_status_res_unique() {
        let body = nul(&[b"uid-1", b"1", b"0", b"2", b"8", b"2"]);
        let raw = frame(RES_MAGIC, PacketType::StatusResUnique as i32, &body);
        let msg = decode_one(&raw).unwrap();
        assert!(matches!(msg, ServerMessage::StatusResUnique(_)));
    }

    #[test]
    fn decode_option_res() {
        let raw = frame(RES_MAGIC, PacketType::OptionRes as i32, b"exceptions");
        let msg = decode_one(&raw).unwrap();
        assert!(matches!(msg, ServerMessage::OptionRes(_)));
    }

    #[test]
    fn decode_server_error_code_and_message() {
        let body = nul(&[b"ERR_UNKNOWN_JOB", b"no such job"]);
        let raw = frame(RES_MAGIC, PacketType::Error as i32, &body);
        let msg = decode_one(&raw).unwrap();
        let ServerMessage::Error(e) = msg else {
            panic!("expected Error");
        };
        assert_eq!(e.code, "ERR_UNKNOWN_JOB".into());
        assert_eq!(e.message, "no such job");
    }

    #[test]
    fn decode_returns_none_on_incomplete_frame() {
        let mut codec = GearmanCodec;
        let mut buf = BytesMut::from(&b"\0RES"[..]); // only 4 bytes
        let result = codec.decode(&mut buf).expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn decode_returns_none_on_header_only() {
        let raw = frame(RES_MAGIC, PacketType::EchoRes as i32, b"data");
        // Feed only the 12-byte header
        let mut codec = GearmanCodec;
        let mut buf = BytesMut::from(&raw[..HEADER_LEN]);
        let result = codec.decode(&mut buf).expect("should not error");
        assert!(result.is_none());
    }

    // ── duplex round-trips ───────────────────────────────────────────────────

    #[tokio::test]
    async fn codec_encode_then_decode_through_duplex() {
        let (client_io, mut server_io) = duplex(4096);
        let mut client_framed = Framed::new(client_io, GearmanCodec);

        // Client sends Echo
        client_framed
            .send(crate::wire::ClientMessage::Echo(Echo {
                payload: Bytes::from_static(b"hello"),
            }))
            .await
            .unwrap();

        // Fake server echoes back an EchoRes
        let _body = drain_frame(&mut server_io).await; // consume EchoReq
        let res = frame(RES_MAGIC, PacketType::EchoRes as i32, b"hello");
        server_io.write_all(&res).await.unwrap();

        // Client reads EchoRes
        let msg = client_framed.next().await.unwrap().unwrap();
        let ServerMessage::EchoRes(e) = msg else {
            panic!("expected EchoRes");
        };
        assert_eq!(&e.payload[..], b"hello");
    }

    #[tokio::test]
    async fn two_back_to_back_frames_decoded_in_order() {
        let (client_io, mut server_io) = duplex(4096);
        let mut client_framed = Framed::new(client_io, GearmanCodec);

        let f1 = frame(RES_MAGIC, PacketType::EchoRes as i32, b"first");
        let f2 = frame(RES_MAGIC, PacketType::EchoRes as i32, b"second");
        let mut both = f1;
        both.extend(f2);
        server_io.write_all(&both).await.unwrap();
        drop(server_io);

        let m1 = client_framed.next().await.unwrap().unwrap();
        let m2 = client_framed.next().await.unwrap().unwrap();

        let (ServerMessage::EchoRes(a), ServerMessage::EchoRes(b)) = (m1, m2) else {
            panic!("expected two EchoRes");
        };
        assert_eq!(&a.payload[..], b"first");
        assert_eq!(&b.payload[..], b"second");
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 3. ServerMessage helpers – is_async / job_handle
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod server_message_tests {
    use super::*;

    fn work_complete(h: &str) -> ServerMessage {
        ServerMessage::WorkComplete(WorkComplete {
            job_handle: jh(h),
            payload: Bytes::new(),
        })
    }
    fn work_fail(h: &str) -> ServerMessage {
        ServerMessage::WorkFail(WorkFail { job_handle: jh(h) })
    }
    fn work_exception(h: &str) -> ServerMessage {
        ServerMessage::WorkException(WorkException {
            job_handle: jh(h),
            exception: Bytes::new(),
        })
    }
    fn work_status(h: &str) -> ServerMessage {
        ServerMessage::WorkStatus(WorkStatus {
            job_handle: jh(h),
            numerator: 0,
            denominator: 1,
        })
    }
    fn work_data(h: &str) -> ServerMessage {
        ServerMessage::WorkData(WorkData {
            job_handle: jh(h),
            payload: Bytes::new(),
        })
    }
    fn work_warning(h: &str) -> ServerMessage {
        ServerMessage::WorkWarning(WorkWarning {
            job_handle: jh(h),
            warning: Bytes::new(),
        })
    }

    // ── is_async ─────────────────────────────────────────────────────────────

    #[test]
    fn work_complete_is_async() {
        assert!(work_complete("H:h:1").is_async());
    }
    #[test]
    fn work_fail_is_async() {
        assert!(work_fail("H:h:2").is_async());
    }
    #[test]
    fn work_exception_is_async() {
        assert!(work_exception("H:h:3").is_async());
    }
    #[test]
    fn work_status_is_async() {
        assert!(work_status("H:h:4").is_async());
    }
    #[test]
    fn work_data_is_async() {
        assert!(work_data("H:h:5").is_async());
    }
    #[test]
    fn work_warning_is_async() {
        assert!(work_warning("H:h:6").is_async());
    }

    #[test]
    fn job_created_is_not_async() {
        let msg = ServerMessage::JobCreated(JobCreated {
            job_handle: jh("H:h:7"),
        });
        assert!(!msg.is_async());
    }
    #[test]
    fn echo_res_is_not_async() {
        let msg = ServerMessage::EchoRes(EchoRes {
            payload: Bytes::new(),
        });
        assert!(!msg.is_async());
    }
    #[test]
    fn option_res_is_not_async() {
        let msg = ServerMessage::OptionRes(OptionRes {
            option: GearmanOption::Exceptions,
        });
        assert!(!msg.is_async());
    }
    #[test]
    fn server_error_is_not_async() {
        let msg = ServerMessage::Error(ServerError {
            code: "E".into(),
            message: "m".into(),
        });
        assert!(!msg.is_async());
    }

    // ── job_handle ────────────────────────────────────────────────────────────

    #[test]
    fn work_complete_returns_correct_handle() {
        assert_eq!(
            String::from_utf8_lossy(&work_complete("H:host:99").job_handle().unwrap()),
            "H:host:99"
        );
    }
    #[test]
    fn work_fail_returns_handle() {
        assert!(work_fail("H:h:100").job_handle().is_some());
    }
    #[test]
    fn work_exception_returns_handle() {
        assert!(work_exception("H:h:101").job_handle().is_some());
    }
    #[test]
    fn work_status_returns_handle() {
        assert!(work_status("H:h:102").job_handle().is_some());
    }
    #[test]
    fn work_data_returns_handle() {
        assert!(work_data("H:h:103").job_handle().is_some());
    }
    #[test]
    fn work_warning_returns_handle() {
        assert!(work_warning("H:h:104").job_handle().is_some());
    }
    #[test]
    fn job_created_returns_handle() {
        let msg = ServerMessage::JobCreated(JobCreated {
            job_handle: jh("H:h:1"),
        });
        assert!(msg.job_handle().is_some());
    }
    #[test]
    fn echo_res_returns_none() {
        let msg = ServerMessage::EchoRes(EchoRes {
            payload: Bytes::new(),
        });
        assert!(msg.job_handle().is_none());
    }
    #[test]
    fn option_res_returns_none() {
        let msg = ServerMessage::OptionRes(OptionRes {
            option: GearmanOption::Exceptions,
        });
        assert!(msg.job_handle().is_none());
    }
    #[test]
    fn server_error_returns_none() {
        let msg = ServerMessage::Error(ServerError {
            code: "E".into(),
            message: "m".into(),
        });
        assert!(msg.job_handle().is_none());
    }
}

// ════════════════════════════════════════════════════════════════════════════
// 4. Client + EventLoop via fake duplex streams
// ════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod event_loop_tests {
    use crate::{
        connection::SharedState,
        messages::job::{Foreground, Job},
    };

    use super::*;

    fn setup() -> (Client<'static>, EventLoop<'static>, DuplexStream) {
        let state = SharedState::new();
        let (client_io, server_io) = duplex(65_536);
        let framed = Framed::new(client_io, GearmanCodec);
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let event_loop = EventLoop::new_for_test(framed, cmd_rx, state.clone());
        let client = Client::new_for_test(cmd_tx, Duration::from_secs(5), state);
        (client, event_loop, server_io)
    }

    // ── echo round-trip ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn echo_sends_req_and_receives_res() {
        let (client, event_loop, mut server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });

        tokio::spawn(async move {
            let body = drain_frame(&mut server_io).await;
            // Verify the client sent EchoReq with our payload
            assert_eq!(body, b"ping");
            let reply = frame(RES_MAGIC, PacketType::EchoRes as i32, &body);
            server_io.write_all(&reply).await.unwrap();
        });

        let result = client.echo(b"ping".to_vec()).await.unwrap();
        assert_eq!(result, b"ping");
    }

    #[tokio::test]
    async fn echo_with_binary_payload() {
        let payload: Vec<u8> = (1u8..=127).collect();
        let (client, event_loop, mut server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });
        let payload_clone = payload.clone();
        tokio::spawn(async move {
            let body = drain_frame(&mut server_io).await;
            let reply = frame(RES_MAGIC, PacketType::EchoRes as i32, &body);
            server_io.write_all(&reply).await.unwrap();
        });
        let result = client.echo(payload).await.unwrap();
        assert_eq!(result, payload_clone);
    }

    // ── submit_job ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn submit_job_returns_job_created() {
        let (client, event_loop, mut server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });
        tokio::spawn(async move {
            drain_frame(&mut server_io).await;
            let reply = frame(RES_MAGIC, PacketType::JobCreated as i32, b"H:localhost:10");
            server_io.write_all(&reply).await.unwrap();
        });

        let job = client
            .submit_job(SubmitJob {
                function_name: "reverse".into(),
                unique_id: "uid-abc".into(),
                payload: Bytes::from_static(b"hello"),
                priority: Priority::Normal,
                foreground: Foreground::Yes,
            })
            .await
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&job.job_handle), "H:localhost:10");
    }

    #[tokio::test]
    async fn submit_job_sends_correct_packet_type_for_normal_fg() {
        let (client, event_loop, mut server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });
        tokio::spawn(async move {
            let mut header = [0u8; HEADER_LEN];
            server_io.read_exact(&mut header).await.unwrap();
            let pt = i32::from_be_bytes(header[4..8].try_into().unwrap());
            assert_eq!(pt, PacketType::SubmitJob as i32);
            // drain body then reply
            let body_len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
            let mut _body = vec![0u8; body_len];
            server_io.read_exact(&mut _body).await.unwrap();
            let reply = frame(RES_MAGIC, PacketType::JobCreated as i32, b"H:h:1");
            server_io.write_all(&reply).await.unwrap();
        });
        client
            .submit_job(SubmitJob {
                function_name: "f".into(),
                unique_id: "u".into(),
                payload: Bytes::new(),
                priority: Priority::Normal,
                foreground: Foreground::Yes,
            })
            .await
            .unwrap();
    }

    // ── job_status ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn job_status_returns_status_res() {
        let (client, event_loop, mut server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });
        tokio::spawn(async move {
            drain_frame(&mut server_io).await;
            let body = nul(&[b"H:localhost:20", b"1", b"1", b"3", b"10"]);
            let reply = frame(RES_MAGIC, PacketType::StatusRes as i32, &body);
            server_io.write_all(&reply).await.unwrap();
        });

        let status = client
            .job_status(GetStatus {
                job_handle: jh("H:localhost:20"),
            })
            .await
            .unwrap();
        assert!(status.known);
        assert!(status.running);
        assert_eq!(status.numerator, 3);
        assert_eq!(status.denominator, 10);
    }

    // ── job_status_unique ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn job_status_unique_returns_status_res_unique() {
        let (client, event_loop, mut server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });
        tokio::spawn(async move {
            drain_frame(&mut server_io).await;
            let body = nul(&[b"uid-1", b"1", b"0", b"0", b"0"]);
            let reply = frame(RES_MAGIC, PacketType::StatusResUnique as i32, &body);
            server_io.write_all(&reply).await.unwrap();
        });

        client
            .job_status_unique(GetStatusUnique {
                unique_id: "uid-1".into(),
            })
            .await
            .unwrap();
    }

    // ── set_option ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn set_option_receives_option_res() {
        let (client, event_loop, mut server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });
        tokio::spawn(async move {
            drain_frame(&mut server_io).await;
            let reply = frame(RES_MAGIC, PacketType::OptionRes as i32, b"exceptions");
            server_io.write_all(&reply).await.unwrap();
        });
        client
            .set_option(SetOption {
                option: GearmanOption::Exceptions,
            })
            .await
            .unwrap();
    }

    // ── server error propagation ───────────────────────────────────────────────

    #[tokio::test]
    async fn server_error_reply_becomes_gearman_error() {
        let (client, event_loop, mut server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });
        tokio::spawn(async move {
            drain_frame(&mut server_io).await;
            let body = nul(&[b"ERR_UNKNOWN_FUNC", b"no such function"]);
            let reply = frame(RES_MAGIC, PacketType::Error as i32, &body);
            server_io.write_all(&reply).await.unwrap();
        });

        let err = client.echo(b"test".to_vec()).await.unwrap_err();
        let GearmanError::ServerError { code, message } = err else {
            panic!("expected ServerError, got {err:?}");
        };
        assert_eq!(code, "ERR_UNKNOWN_FUNC");
        assert_eq!(message, "no such function");
    }

    // ── connection closed ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn drop_server_before_reply_returns_error() {
        let (client, event_loop, server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });
        // Close the server side immediately
        drop(server_io);

        let err = client.echo(b"ping".to_vec()).await.unwrap_err();
        assert!(
            matches!(
                err,
                GearmanError::ConnectionClosed | GearmanError::Timeout | GearmanError::Codec(_)
            ),
            "unexpected variant: {err:?}"
        );
    }

    // ── timeout ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn request_times_out_when_server_silent() {
        let state = SharedState::new();
        let (client_io, _server_io) = duplex(4096); // _server_io kept alive but silent
        let framed = Framed::new(client_io, GearmanCodec);
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let event_loop = EventLoop::new_for_test(framed, cmd_rx, state.clone());
        // Very short timeout
        let client = Client::new_for_test(cmd_tx, Duration::from_millis(30), state);
        tokio::spawn(async move { event_loop.run().await });

        let err = client.echo(b"ping".to_vec()).await.unwrap_err();
        assert!(
            matches!(err, GearmanError::Timeout),
            "expected Timeout, got {err:?}"
        );
    }

    // ── async WorkComplete dispatched after submit ─────────────────────────────

    #[tokio::test]
    async fn work_complete_dispatched_to_job_tracker() {
        let (client, event_loop, mut server_io) = setup();
        // Capture a handle to the shared state so we can inspect it post-fact
        let state = event_loop.state_handle();
        tokio::spawn(async move { event_loop.run().await });

        tokio::spawn(async move {
            // Drain SubmitJob, reply JobCreated
            drain_frame(&mut server_io).await;
            let reply = frame(RES_MAGIC, PacketType::JobCreated as i32, b"H:localhost:55");
            server_io.write_all(&reply).await.unwrap();

            // Give the event loop a moment to process WorkComplete
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Push WorkComplete asynchronously (no pending request from client)
            let body = nul(&[b"H:localhost:55", b"final-result"]);
            let wc = frame(RES_MAGIC, PacketType::WorkComplete as i32, &body);
            server_io.write_all(&wc).await.unwrap();
        });

        let job = Job::new(Bytes::new()).submit("fn", &client).await.unwrap();
        assert_eq!(job.handle().await.as_ref(), b"H:localhost:55");

        job.await;

        let jobs = state.jobs.read().await;
        assert!(
            !jobs.is_registered(&jh("H:localhost:55")),
            "job should be unregistered after WorkComplete"
        );
    }

    // ── multiple sequential requests ───────────────────────────────────────────

    #[tokio::test]
    async fn two_sequential_echo_calls() {
        let (client, event_loop, mut server_io) = setup();
        tokio::spawn(async move { event_loop.run().await });

        // Server handles two requests in sequence
        tokio::spawn(async move {
            for _ in 0..2u8 {
                let body = drain_frame(&mut server_io).await;
                let reply = frame(RES_MAGIC, PacketType::EchoRes as i32, &body);
                server_io.write_all(&reply).await.unwrap();
            }
        });

        let r1 = client.echo(b"one".to_vec()).await.unwrap();
        let r2 = client.echo(b"two".to_vec()).await.unwrap();
        assert_eq!(r1, b"one");
        assert_eq!(r2, b"two");
    }
}

#[cfg(test)]
mod connect_options_tests {
    use super::*;

    #[test]
    fn new_parses_valid_url() {
        let opts = ConnectOptions::new("gearman://127.0.0.1:4730").unwrap();
        assert_eq!(opts.address.host_str(), Some("127.0.0.1"));
        assert_eq!(opts.address.port(), Some(4730));
    }

    #[test]
    fn default_port_is_4730_when_omitted() {
        let opts = ConnectOptions::new("gearman://127.0.0.1").unwrap();
        // port_or_known_default() should return 4730 for the gearman scheme,
        // or the crate falls back to 4730 in connect()
        assert!(opts.address.port().is_none() || opts.address.port() == Some(4730));
    }

    #[test]
    fn default_timeout_is_300_seconds() {
        let opts = ConnectOptions::new("gearman://127.0.0.1").unwrap();
        assert_eq!(opts.timeout, Duration::from_secs(300));
    }

    #[test]
    fn with_timeout_overrides_default() {
        let opts = ConnectOptions::new("gearman://127.0.0.1")
            .unwrap()
            .with_timeout(Duration::from_secs(10));
        assert_eq!(opts.timeout, Duration::from_secs(10));
    }

    #[test]
    fn with_timeout_one_millisecond_edge_case() {
        let opts = ConnectOptions::new("gearman://127.0.0.1")
            .unwrap()
            .with_timeout(Duration::from_millis(1));
        assert_eq!(opts.timeout, Duration::from_millis(1));
    }

    #[test]
    fn invalid_url_returns_error() {
        assert!(ConnectOptions::new("not a url at all %%%").is_err());
    }

    #[test]
    fn ipv6_address_parses() {
        let opts = ConnectOptions::new("gearman://[::1]:4730").unwrap();
        assert_eq!(opts.address.host_str(), Some("[::1]"));
    }

    #[test]
    fn hostname_address_parses() {
        let opts = ConnectOptions::new("gearman://gearman.example.com:4730").unwrap();
        assert_eq!(opts.address.host_str(), Some("gearman.example.com"));
    }
}
