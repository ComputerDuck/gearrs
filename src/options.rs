use bytes::Bytes;

#[derive(Debug)]
pub enum OptionsError {
    InvalidOption
}
impl std::error::Error for OptionsError {}
impl std::fmt::Display for OptionsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidOption => write!(f, "Invalid Option"),
        }
    }
}

#[derive(Clone, Copy)]
pub enum Options {
    Exceptions,
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
