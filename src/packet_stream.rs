use std::io::ErrorKind;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::{Duration, timeout};

use crate::GearmanError;
use crate::packet::{Body, Header, Magic, Packet, ParseError};

pub fn packet_stream(
    stream: TcpStream,
    timeout: Duration,
) -> (GearmanPacketReader, GearmanPacketSender) {
    let (reader, writer) = stream.into_split();
    let reader = GearmanPacketReader::new(reader, timeout);
    let writer = GearmanPacketSender::new(writer, timeout);
    (reader, writer)
}

const HEADER_SIZE: usize = 12; // 4 bytes magic + 4 bytes type + 4 bytes size

pub struct GearmanPacketReader {
    reader: OwnedReadHalf,
    buffer: Vec<u8>,
    read_timeout: Duration,
}

impl GearmanPacketReader {
    pub fn new(reader: OwnedReadHalf, timeout: Duration) -> Self {
        GearmanPacketReader {
            reader,
            buffer: Vec::with_capacity(1024),
            read_timeout: timeout,
        }
    }

    pub async fn read_packet(&mut self) -> Result<Packet, GearmanError> {
        let mut header_buf = [0u8; HEADER_SIZE];
        match timeout(self.read_timeout, self.reader.read_exact(&mut header_buf)).await {
            Ok(Ok(0)) => return Err(GearmanError::ConnectionClosed),
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == ErrorKind::UnexpectedEof => return Err(GearmanError::UnexpectedEof),
            Ok(Err(e)) => return Err(GearmanError::IoError(e)),
            Err(_) => return Err(GearmanError::Timeout),
        }
        
        self.validate_magic_number(&header_buf)?;
        let data_size = self.extract_data_size(&header_buf)?;

        let header =
            Header::try_from(&header_buf[..]).map_err(|err| GearmanError::InvalidPacket(err))?;

        // Read the body with timeout
        self.buffer.clear();
        self.buffer.resize(data_size, 0);
        if data_size > 0 {
            self.read_exact_with_timeout().await?;
        }

        let body =
            Body::try_from(&self.buffer[..]).map_err(|err| GearmanError::InvalidPacket(err))?;
        let packet = Packet::new(header, body);

        Ok(packet)
    }

    async fn read_exact_with_timeout(&mut self, ) -> Result<(), GearmanError> {
        let buf: &mut [u8] = &mut self.buffer;
        let mut pos = 0;
        while pos < buf.len() {
            match timeout(self.read_timeout, self.reader.read(&mut buf[pos..])).await {
                Ok(Ok(0)) => return Err(GearmanError::ConnectionClosed),
                Ok(Ok(n)) => pos += n,
                Ok(Err(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                    return Err(GearmanError::UnexpectedEof);
                }
                Ok(Err(e)) if e.kind() == ErrorKind::WouldBlock => {
                    // Continue reading on WouldBlock
                    continue;
                }
                Ok(Err(e)) => return Err(GearmanError::IoError(e)),
                Err(_) => return Err(GearmanError::Timeout),
            }
        }
        Ok(())
    }

    /// Validate the magic number in the header
    fn validate_magic_number(&self, header_bytes: &[u8]) -> Result<(), GearmanError> {
        if header_bytes.len() < 4 {
            return Err(GearmanError::ParseError(ParseError::with_message(
                "Header too short for magic number",
            )));
        }

        let magic_bytes = [
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
        ];
        let magic = u32::from_be_bytes(magic_bytes);

        if magic != Magic::Request as u32 && magic != Magic::Response as u32 {
            return Err(GearmanError::ParseError(ParseError::with_message(format!(
                "Invalid magic number: 0x{:08x}",
                magic
            ))));
        }

        Ok(())
    }

    fn extract_data_size(&self, header_bytes: &[u8]) -> Result<usize, GearmanError> {
        if header_bytes.len() < HEADER_SIZE {
            return Err(GearmanError::ParseError(ParseError::with_message(
                "Header too short for size field",
            )));
        }

        // The size is in the last 4 bytes of the header, in network byte order (big endian)
        let size_bytes = [
            header_bytes[8],
            header_bytes[9],
            header_bytes[10],
            header_bytes[11],
        ];

        let size = u32::from_be_bytes(size_bytes) as usize;

        Ok(size)
    }
}

pub struct GearmanPacketSender {
    writer: OwnedWriteHalf,
    write_timeout: Duration,
}

impl GearmanPacketSender {
    pub fn new(writer: OwnedWriteHalf, timeout: Duration) -> Self {
        GearmanPacketSender {
            writer,
            write_timeout: timeout,
        }
    }

    /// Send a packet with timeout
    pub async fn send_packet(&mut self, packet: &Packet) -> Result<(), GearmanError> {
        let bytes: Bytes = packet.into();

        timeout(self.write_timeout, self.writer.write_all(&bytes)).await??;
        timeout(self.write_timeout, self.writer.flush()).await??;

        Ok(())
    }
}
