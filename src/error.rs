// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

pub use url::ParseError;

use failure::Fail;
use mysql_common::{
    named_params::MixedParamsError, packets::ErrPacket, params::MissingNamedParameterError,
    row::Row, value::Value,
};

use std::{io, result};

/// Result type alias for this library.
pub type Result<T> = result::Result<T, Error>;

/// This type enumerates library errors.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Driver error: `{}'", _0)]
    Driver(#[cause] DriverError),

    #[fail(display = "Input/output error: {}", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "Other error: {}", _0)]
    Other(#[cause] failure::Error),

    #[fail(display = "Server error: `{}'", _0)]
    Server(#[cause] ServerError),

    #[cfg(feature = "ssl")]
    #[fail(display = "TLS error: `{}'", _0)]
    Tls(#[cause] native_tls::Error),

    #[fail(display = "URL error: `{}'", _0)]
    Url(#[cause] UrlError),
}

/// This type represents MySql server error.
#[derive(Debug, Fail)]
#[fail(display = "ERROR {} ({}): {}", state, code, message)]
pub struct ServerError {
    pub code: u16,
    pub message: String,
    pub state: String,
}

/// This type enumerates connection URL errors.
#[derive(Debug, Fail)]
pub enum UrlError {
    #[fail(
        display = "Connection URL parameter `{}' requires feature `{}'",
        param, feature
    )]
    FeatureRequired { feature: String, param: String },

    #[fail(display = "Invalid or incomplete connection URL")]
    Invalid,

    #[fail(
        display = "Invalid value `{}' for connection URL parameter `{}'",
        value, param
    )]
    InvalidParamValue { param: String, value: String },

    #[fail(
        display = "Invalid pool constraints: pool_min ({}) > pool_max ({}).",
        min, max
    )]
    InvalidPoolConstraints { min: usize, max: usize },

    #[fail(display = "URL parse error: {}", _0)]
    Parse(#[cause] ParseError),

    #[fail(display = "Unknown connection URL parameter `{}'", param)]
    UnknownParameter { param: String },

    #[fail(display = "Unsupported connection URL scheme `{}'", scheme)]
    UnsupportedScheme { scheme: String },
}

/// This type enumerates driver errors.
#[derive(Debug, Fail)]
pub enum DriverError {
    #[fail(display = "AuthSwitchRequest handling is not implemented.")]
    AuthSwitchUnimplemented,

    #[fail(
        display = "Can't parse server version from string `{}'.",
        version_string
    )]
    CantParseServerVersion { version_string: String },

    #[fail(display = "Connection to the server is closed.")]
    ConnectionClosed,

    #[fail(display = "Error converting from mysql value.")]
    FromValue { value: Value },

    #[fail(display = "Error converting from mysql row.")]
    FromRow { row: Row },

    #[fail(display = "Missing named parameter `{}'.", name)]
    MissingNamedParam { name: String },

    #[fail(display = "Named and positional parameters mixed in one statement.")]
    MixedParams,

    #[fail(display = "Named parameters supplied for positional query.")]
    NamedParamsForPositionalQuery,

    #[fail(display = "Transactions couldn't be nested.")]
    NestedTransaction,

    #[fail(display = "Can't handle local infile request. Handler not specified.")]
    NoLocalInfileHandler,

    #[fail(display = "Packet out of order.")]
    PacketOutOfOrder,

    #[fail(display = "Pool was disconnected.")]
    PoolDisconnected,

    #[fail(
        display = "`SET TRANSACTION READ (ONLY|WRITE)' is not supported in your MySQL version."
    )]
    ReadOnlyTransNotSupported,

    #[fail(
        display = "Statement takes {} parameters but {} was supplied.",
        required, supplied
    )]
    StmtParamsMismatch { required: u16, supplied: u16 },

    #[fail(display = "Unexpected packet.")]
    UnexpectedPacket { payload: Vec<u8> },

    #[fail(display = "Unknown authentication plugin `{}'.", name)]
    UnknownAuthPlugin { name: String },
}

impl From<DriverError> for Error {
    fn from(err: DriverError) -> Self {
        Error::Driver(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
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

#[cfg(feature = "ssl")]
impl From<native_tls::Error> for Error {
    fn from(err: native_tls::Error) -> Self {
        Error::Tls(err)
    }
}

impl From<ErrPacket<'_>> for ServerError {
    fn from(packet: ErrPacket<'_>) -> Self {
        ServerError {
            code: packet.error_code(),
            message: packet.message_str().into(),
            state: packet.sql_state_str().into(),
        }
    }
}

impl From<ErrPacket<'_>> for Error {
    fn from(packet: ErrPacket<'_>) -> Self {
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
        Error::Other(failure::Context::new(err).into())
    }
}

impl From<&'static str> for Error {
    fn from(err: &'static str) -> Self {
        Error::Other(failure::Context::new(err).into())
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
