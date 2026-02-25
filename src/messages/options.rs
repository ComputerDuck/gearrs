use bytes::Bytes;

use crate::{
    Client, GearmanError,
    packages::{GearmanOption, OptionRes, SetOption},
};

#[derive(Debug, thiserror::Error)]
pub enum OptionsError {
    #[error("Invalid Option")]
    InvalidOption,
}

pub enum OptionsResult {
    Valid(Options),
    Error(OptionsError),
}

#[derive(Clone, Copy)]
pub enum Options {
    Exceptions,
}
impl From<OptionRes> for Options {
    fn from(value: OptionRes) -> Self {
        match value.option {
            GearmanOption::Exceptions => Options::Exceptions,
        }
    }
}
impl Into<GearmanOption> for &Options {
    fn into(self) -> GearmanOption {
        match self {
            Options::Exceptions => GearmanOption::Exceptions,
        }
    }
}

impl Options {
    pub async fn enable(self, client: &Client<'_>) -> Result<OptionsResult, GearmanError> {
        client
            .set_option(SetOption {
                option: (&self).into(),
            })
            .await
            .map(|res| OptionsResult::Valid(res.into()))
    }
}

impl Into<Bytes> for Options {
    fn into(self) -> Bytes {
        match self {
            Self::Exceptions => Bytes::from("exceptions"),
        }
    }
}

impl TryFrom<Bytes> for Options {
    type Error = OptionsError;
    fn try_from(value: Bytes) -> Result<Self, OptionsError> {
        Self::try_from(&value)
    }
}

impl TryFrom<&Bytes> for Options {
    type Error = OptionsError;
    fn try_from(value: &Bytes) -> Result<Self, OptionsError> {
        let value: &[u8] = &value.as_ref();
        match value {
            v if v == "exceptions".as_bytes() => Ok(Self::Exceptions),
            _ => Err(OptionsError::InvalidOption),
        }
    }
}
