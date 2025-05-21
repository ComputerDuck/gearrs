use bytes::Bytes;

use crate::connection::{Client, GearmanError};
use crate::request::EchoReq;

pub struct Echo {
    bytes: Bytes,
}

impl Echo {
    pub fn new(payload: impl AsRef<str>) -> Self {
        Self {
            bytes: Bytes::from(payload.as_ref().to_string()),
        }
    }

    pub fn as_string(&self) -> String {
        String::from_utf8_lossy(self.bytes.as_ref()).to_string()
    }

    pub async fn submit(self, connection: &Client<'_>) -> Result<EchoResponse, GearmanError> {
        let echo_req = EchoReq::new(self.bytes);
        let echo_res = connection.submit_echo(echo_req).await?;
        Ok(EchoResponse {
            bytes: echo_res.as_bytes(),
        })
    }
}

pub struct EchoResponse {
    bytes: Bytes,
}
impl EchoResponse {
    pub fn validate(&self, request: impl AsRef<str>) -> Option<()> {
        match self.bytes == Bytes::from(request.as_ref().to_string()) {
            true => Some(()),
            false => None,
        }
    }
}
