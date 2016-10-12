use proto::{
    ErrPacket,
    Row,
};

use std::io;

use url;

use value::Value;

error_chain! {
    foreign_links {
        io::Error, Io;
        url::ParseError, UrlParseError;
    }

    errors {
        UnexpectedPacket { // TODO: Add packet data here?
            description("Unexpected packet")
        }
        ConnectionClosed {
            description("Connection closed")
        }
        MismatchedStmtParams(required: u16, supplied: u16) {
            description("Number of statement parameters does not match")
            display("Statement takes {} parameters but {} was supplied", required, supplied)
        }
        NamedParamsForPositionalQuery {
            description("Named parameters for positional query")
        }
        MissingNamedParameter(name: String) {
            description("Missing named parameter")
            display("Missing named parameter: {}", name)
        }
        MixedParams {
            description("Named and positional parameters mixed in one statement")
        }
        FromValue(value: Value) {
            description("Error converting from mysql value")
        }
        FromRow(row: Row) {
            description("Error converting from mysql row")
        }
        Server(packet: ErrPacket) {
            description("Mysql server error")
            display("ERROR {} ({}): {}", packet.error_code(), packet.state(), packet.message())
        }
        CantParseVersion(ver_str: String) {
            description("Can't parse server version")
            display("Can't parse server version from string: `{}'", ver_str)
        }
        UrlUnsupportedScheme(scheme: String) {
            description("Unsupported connection URL scheme")
            display("Unsupported connection URL scheme {}", scheme)
        }
        UrlFeatureRequired(feature: String, param: String) {
            description("Used connection URL parameter requires feature")
            display("Connection URL parameter `{}' requires feature `{}'", param, feature)
        }
        UrlInvalidParamValue(param: String, value: String) {
            description("Invalid value for connection URL parameter")
            display("Invalid value `{}' for connection URL parameter `{}'", value, param)
        }
        UrlUnknownParameter(param: String) {
            description("Unknown connection URL parameter")
            display("Unknown connection URL parameter `{}'", param)
        }
        UrlInvalid {
            description("Invalid or incomplete connection URL")
        }
    }
}

impl From<(Error, ::io::Stream)> for Error {
    fn from((err, _): (Error, ::io::Stream)) -> Self {
        err
    }
}
