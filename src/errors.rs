// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use proto::ErrPacket;
use proto::Row;
use std::io;
use url;
use value::Value;

error_chain! {
    foreign_links {
        Io(io::Error);
        UrlParseError(url::ParseError);
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
        InvalidPoolConstraints(min: usize, max: usize) {
            description("Invalid pool constraints: pool_min > pool_max.")
            display("Invalid pool constraints: pool_min ({}) > pool_max ({}).", min, max)
        }
        PoolDisconnected {
            description("Pool was disconnected")
        }
        ReadOnlyTransNotSupported {
            description("`SET TRANSACTION READ (ONLY|WRITE)' is not supported in your MySQL version")
        }
        PacketOutOfOrder {
            description("Packet out of order")
        }
        NoLocalInfileHandler {
            description("Can't handle local infile request. Handler not specified.")
        }
    }
}

impl From<(Error, ::io::Stream)> for Error {
    fn from((err, _): (Error, ::io::Stream)) -> Self {
        err
    }
}
