// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

pub use url::ParseError;

use mysql_common::{
    named_params::MixedParamsError, params::MissingNamedParameterError,
    proto::codec::error::PacketCodecError, row::Row, value::Value,
};
use thiserror::Error;

use std::{borrow::Cow, io, result};

/// Result type alias for this library.
pub type Result<T> = result::Result<T, Error>;

/// This type enumerates library errors.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Driver error: `{}'", _0)]
    Driver(#[source] DriverError),

    #[error("Input/output error: {}", _0)]
    Io(#[source] IoError),

    #[error("Other error: {}", _0)]
    Other(Cow<'static, str>),

    #[error("Server error: `{}'", _0)]
    Server(#[source] ServerError),

    #[error("URL error: `{}'", _0)]
    Url(#[source] UrlError),
}

impl Error {
    /// Returns true if the error means that connection is broken.
    pub fn is_fatal(&self) -> bool {
        match self {
            Error::Driver(_) | Error::Io(_) | Error::Other(_) | Error::Url(_) => true,
            Error::Server(_) => false,
        }
    }
}

/// This type enumerates IO errors.
#[derive(Debug, Error)]
pub enum IoError {
    #[error("Input/output error: {}", _0)]
    Io(#[source] io::Error),

    #[error("TLS error: `{}'", _0)]
    Tls(#[source] native_tls::Error),
}

/// This type represents MySql server error.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("ERROR {} ({}): {}", state, code, message)]
pub struct ServerError {
    pub code: u16,
    pub message: String,
    pub state: String,
}

/// This type enumerates connection URL errors.
#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum UrlError {
    #[error("Connection URL parameter `{}' requires feature `{}'", param, feature)]
    FeatureRequired { feature: String, param: String },

    #[error("Invalid or incomplete connection URL")]
    Invalid,

    #[error("Invalid value `{}' for connection URL parameter `{}'", value, param)]
    InvalidParamValue { param: String, value: String },

    #[error("Invalid pool constraints: pool_min ({}) > pool_max ({}).", min, max)]
    InvalidPoolConstraints { min: usize, max: usize },

    #[error("URL parse error: {}", _0)]
    Parse(#[source] ParseError),

    #[error("Unknown connection URL parameter `{}'", param)]
    UnknownParameter { param: String },

    #[error("Unsupported connection URL scheme `{}'", scheme)]
    UnsupportedScheme { scheme: String },
}

/// This type enumerates driver errors.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum DriverError {
    #[error("Can't parse server version from string `{}'.", version_string)]
    CantParseServerVersion { version_string: String },

    #[error("Connection to the server is closed.")]
    ConnectionClosed,

    #[error("Error converting from mysql value.")]
    FromValue { value: Value },

    #[error("Error converting from mysql row.")]
    FromRow { row: Row },

    #[error("Missing named parameter `{}'.", name)]
    MissingNamedParam { name: String },

    #[error("Named and positional parameters mixed in one statement.")]
    MixedParams,

    #[error("Named parameters supplied for positional query.")]
    NamedParamsForPositionalQuery,

    #[error("Transactions couldn't be nested.")]
    NestedTransaction,

    #[error("Can't handle local infile request. Handler not specified.")]
    NoLocalInfileHandler,

    #[error("Packet out of order.")]
    PacketOutOfOrder,

    #[error("Pool was disconnected.")]
    PoolDisconnected,

    #[error("`SET TRANSACTION READ (ONLY|WRITE)' is not supported in your MySQL version.")]
    ReadOnlyTransNotSupported,

    #[error(
        "Statement takes {} parameters but {} was supplied.",
        required,
        supplied
    )]
    StmtParamsMismatch { required: u16, supplied: u16 },

    #[error("Unexpected packet.")]
    UnexpectedPacket { payload: Vec<u8> },

    #[error("Unknown authentication plugin `{}'.", name)]
    UnknownAuthPlugin { name: String },

    #[error("Packet too large.")]
    PacketTooLarge,

    #[error("Bad compressed packet header.")]
    BadCompressedPacketHeader,

    #[error("Named pipe connections temporary disabled (see tokio-rs/tokio#3118)")]
    NamedPipesDisabled,

    #[error("`mysql_old_password` plugin is insecure and disabled by default")]
    MysqlOldPasswordDisabled,
}

impl From<DriverError> for Error {
    fn from(err: DriverError) -> Self {
        Error::Driver(err)
    }
}

impl From<IoError> for Error {
    fn from(io: IoError) -> Self {
        Error::Io(io)
    }
}

impl From<io::Error> for IoError {
    fn from(err: io::Error) -> Self {
        IoError::Io(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err.into())
    }
}

impl From<ServerError> for Error {
    fn from(err: ServerError) -> Self {
        Error::Server(err)
    }
}

impl From<UrlError> for Error {
    fn from(err: UrlError) -> Self {
        Error::Url(err)
    }
}

impl From<native_tls::Error> for IoError {
    fn from(err: native_tls::Error) -> Self {
        IoError::Tls(err)
    }
}

impl From<mysql_common::packets::ServerError<'_>> for ServerError {
    fn from(packet: mysql_common::packets::ServerError<'_>) -> Self {
        ServerError {
            code: packet.error_code(),
            message: packet.message_str().into(),
            state: packet.sql_state_str().into(),
        }
    }
}

impl From<mysql_common::packets::ServerError<'_>> for Error {
    fn from(packet: mysql_common::packets::ServerError<'_>) -> Self {
        Error::Server(packet.into())
    }
}

// For convenience
impl From<(Error, crate::io::Stream)> for Error {
    fn from((err, _): (Error, crate::io::Stream)) -> Self {
        err
    }
}

impl From<MissingNamedParameterError> for DriverError {
    fn from(err: MissingNamedParameterError) -> Self {
        DriverError::MissingNamedParam { name: err.0 }
    }
}

impl From<MissingNamedParameterError> for Error {
    fn from(err: MissingNamedParameterError) -> Self {
        Error::Driver(err.into())
    }
}

impl From<MixedParamsError> for DriverError {
    fn from(_err: MixedParamsError) -> Self {
        DriverError::MixedParams
    }
}

impl From<MixedParamsError> for Error {
    fn from(err: MixedParamsError) -> Self {
        Error::Driver(err.into())
    }
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::Other(Cow::from(err))
    }
}

impl From<&'static str> for Error {
    fn from(err: &'static str) -> Self {
        Error::Other(Cow::from(err))
    }
}

impl From<ParseError> for UrlError {
    fn from(err: ParseError) -> Self {
        UrlError::Parse(err)
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Self {
        Error::Url(err.into())
    }
}

impl From<PacketCodecError> for IoError {
    fn from(err: PacketCodecError) -> Self {
        match err {
            PacketCodecError::Io(err) => err.into(),
            PacketCodecError::PacketTooLarge => {
                io::Error::new(io::ErrorKind::Other, "packet too large").into()
            }
            PacketCodecError::PacketsOutOfSync => {
                io::Error::new(io::ErrorKind::Other, "packet out of order").into()
            }
            PacketCodecError::BadCompressedPacketHeader => {
                io::Error::new(io::ErrorKind::Other, "bad compressed packet header").into()
            }
        }
    }
}

impl From<PacketCodecError> for Error {
    fn from(err: PacketCodecError) -> Self {
        Error::Io(err.into())
    }
}
