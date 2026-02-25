use bytes::Bytes;

use crate::connection::{Client, GearmanError};

pub struct Echo {
    bytes: Vec<u8>,
}

impl Echo {
    pub fn new(payload: impl AsRef<str>) -> Self {
        Self {
            bytes: payload.as_ref().to_string().into_bytes(),
        }
    }

    pub fn as_string(&self) -> String {
        String::from_utf8_lossy(self.bytes.as_ref()).to_string()
    }

    pub async fn submit(self, connection: &Client<'_>) -> Result<EchoResponse, GearmanError> {
        let echo_res = connection.echo(self.bytes).await?;
        Ok(EchoResponse { bytes: echo_res })
    }
}

pub struct EchoResponse {
    bytes: Vec<u8>,
}
impl EchoResponse {
    pub fn validate(&self, request: impl AsRef<str>) -> Option<()> {
        match self.bytes == Bytes::from(request.as_ref().to_string()) {
            true => Some(()),
            false => None,
        }
    }
}
