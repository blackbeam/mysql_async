#[allow(unused_imports)]
use mysql_async::{
    consts, from_row, from_row_opt, from_value, from_value_opt,
    futures::{DisconnectPool, GetConn},
    params,
    prelude::{
        BatchQuery, FromRow, FromValue, GlobalHandler, Protocol, Query, Queryable, StatementLike,
        ToValue,
    },
    BinaryProtocol, Column, Conn, Deserialized, DriverError, Error, FromRowError, FromValueError,
    GnoInterval, Gtids, IoError, IsolationLevel, OkPacket, Opts, OptsBuilder, Params, ParseError,
    Pool, PoolConstraints, PoolOpts, QueryResult, Result, Row, Schema, Serialized, ServerError,
    SessionStateChange, SessionStateInfo, Sid, SslOpts, Statement, SystemVariable, TextProtocol,
    Transaction, TransactionCharacteristics, TransactionState, TxOpts, Unsupported, UrlError,
    Value, WhiteListFsHandler, DEFAULT_INACTIVE_CONNECTION_TTL, DEFAULT_TTL_CHECK_INTERVAL,
};

#[cfg(feature = "binlog")]
#[allow(unused_imports)]
use mysql_async::{binlog, BinlogStream, BinlogStreamRequest};
