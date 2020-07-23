// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::{
    constants::MAX_PAYLOAD_LEN,
    named_params::parse_named_params,
    packets::{
        column_from_payload, parse_stmt_packet, ComStmtClose, ComStmtExecuteRequestBuilder,
        ComStmtSendLongData, StmtPacket,
    },
};

use std::{borrow::Cow, sync::Arc};

use crate::{
    consts::{CapabilityFlags, Command},
    error::*,
    queryable::BinaryProtocol,
    Column, Params, Value,
};

/// Result of a `StatementLike::to_statement` call.
pub enum ToStatementResult<'a> {
    /// Statement is immediately available.
    Immediate(Statement),
    /// We need some time to get a statement and the operation itself may fail.
    Mediate(crate::BoxFuture<'a, Statement>),
}

pub trait StatementLike: Send + Sync {
    /// Returns a statement.
    fn to_statement<'a>(&'a self, conn: &'a mut crate::Conn) -> ToStatementResult<'a>;
}

impl StatementLike for str {
    fn to_statement<'a>(&'a self, conn: &'a mut crate::Conn) -> ToStatementResult<'a> {
        let fut = crate::BoxFuture(Box::pin(async move {
            let (named_params, raw_query) = parse_named_params(self)?;
            let inner_stmt = match conn.get_cached_stmt(&*raw_query) {
                Some(inner_stmt) => inner_stmt,
                None => conn.prepare_statement(raw_query).await?,
            };
            Ok(Statement::new(inner_stmt, named_params))
        }));
        ToStatementResult::Mediate(fut)
    }
}

impl StatementLike for Statement {
    fn to_statement<'a>(&'a self, _conn: &'a mut crate::Conn) -> ToStatementResult<'static> {
        ToStatementResult::Immediate(self.clone())
    }
}

/// Statement data.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StmtInner {
    pub(crate) raw_query: Arc<str>,
    columns: Option<Box<[Column]>>,
    params: Option<Box<[Column]>>,
    stmt_packet: StmtPacket,
    connection_id: u32,
}

impl StmtInner {
    pub(crate) fn from_payload(
        pld: &[u8],
        connection_id: u32,
        raw_query: Arc<str>,
    ) -> std::io::Result<Self> {
        let stmt_packet = parse_stmt_packet(pld)?;

        Ok(Self {
            raw_query,
            columns: None,
            params: None,
            stmt_packet,
            connection_id,
        })
    }

    pub(crate) fn with_params(mut self, params: Vec<Column>) -> Self {
        self.params = if params.is_empty() {
            None
        } else {
            Some(params.into_boxed_slice())
        };
        self
    }

    pub(crate) fn with_columns(mut self, columns: Vec<Column>) -> Self {
        self.columns = if columns.is_empty() {
            None
        } else {
            Some(columns.into_boxed_slice())
        };
        self
    }

    pub(crate) fn columns(&self) -> &[Column] {
        self.columns.as_ref().map(AsRef::as_ref).unwrap_or(&[])
    }

    pub(crate) fn params(&self) -> &[Column] {
        self.params.as_ref().map(AsRef::as_ref).unwrap_or(&[])
    }

    pub(crate) fn id(&self) -> u32 {
        self.stmt_packet.statement_id()
    }

    pub(crate) const fn connection_id(&self) -> u32 {
        self.connection_id
    }

    pub(crate) fn num_params(&self) -> u16 {
        self.stmt_packet.num_params()
    }

    pub(crate) fn num_columns(&self) -> u16 {
        self.stmt_packet.num_columns()
    }
}

/// Prepared statement.
///
/// Statement is only valid for connection with id `Statement::connection_id()`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Statement {
    pub(crate) inner: Arc<StmtInner>,
    pub(crate) named_params: Option<Vec<String>>,
}

impl Statement {
    pub(crate) fn new(inner: Arc<StmtInner>, named_params: Option<Vec<String>>) -> Self {
        Self {
            inner,
            named_params,
        }
    }

    /// Returned columns.
    pub fn columns(&self) -> &[Column] {
        self.inner.columns()
    }

    /// Requred parameters.
    pub fn params(&self) -> &[Column] {
        self.inner.params()
    }

    /// MySql statement identifier.
    pub fn id(&self) -> u32 {
        self.inner.id()
    }

    /// MySql connection identifier.
    pub fn connection_id(&self) -> u32 {
        self.inner.connection_id()
    }

    /// Number of parameters.
    pub fn num_params(&self) -> u16 {
        self.inner.num_params()
    }

    /// Number of columns.
    pub fn num_columns(&self) -> u16 {
        self.inner.num_columns()
    }
}

impl crate::Conn {
    /// Low-level helpers, that reads the given number of column packets terminated by EOF packet.
    ///
    /// Requires `num > 0`.
    pub(crate) async fn read_column_defs<U>(&mut self, num: U) -> Result<Vec<Column>>
    where
        U: Into<usize>,
    {
        let num = num.into();
        debug_assert!(num > 0);
        let packets = self.read_packets(num).await?;
        let defs = packets
            .into_iter()
            .map(column_from_payload)
            .collect::<std::result::Result<Vec<Column>, _>>()
            .map_err(Error::from)?;

        if !self
            .capabilities()
            .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
        {
            self.read_packet().await?;
        }

        Ok(defs)
    }

    /// Helper, that retrieves `Statement` from `StatementLike`.
    pub(crate) async fn get_statement<U>(&mut self, stmt_like: &U) -> Result<Statement>
    where
        U: StatementLike + ?Sized,
    {
        match stmt_like.to_statement(self) {
            ToStatementResult::Immediate(statement) => Ok(statement),
            ToStatementResult::Mediate(statement) => statement.await,
        }
    }

    /// Low-level helper, that prepares the given statement.
    ///
    /// `raw_query` is a query with `?` placeholders (if any).
    async fn prepare_statement(&mut self, raw_query: Cow<'_, str>) -> Result<Arc<StmtInner>> {
        let raw_query: Arc<str> = raw_query.into_owned().into_boxed_str().into();

        self.write_command_data(Command::COM_STMT_PREPARE, raw_query.as_bytes())
            .await?;

        let packet = self.read_packet().await?;
        let mut inner_stmt = StmtInner::from_payload(&*packet, self.id(), raw_query)?;

        if inner_stmt.num_params() > 0 {
            let params = self.read_column_defs(inner_stmt.num_params()).await?;
            inner_stmt = inner_stmt.with_params(params);
        }

        if inner_stmt.num_columns() > 0 {
            let columns = self.read_column_defs(inner_stmt.num_columns()).await?;
            inner_stmt = inner_stmt.with_columns(columns);
        }

        let inner_stmt = Arc::new(inner_stmt);

        if let Some(old_stmt) = self.cache_stmt(&inner_stmt) {
            self.close_statement(old_stmt.id()).await?;
        }

        Ok(inner_stmt)
    }

    /// Helper, that executes the given statement with the given params.
    pub(crate) async fn execute_statement<P>(
        &mut self,
        statement: &Statement,
        params: P,
    ) -> Result<()>
    where
        P: Into<Params>,
    {
        let mut params = params.into();
        loop {
            match params {
                Params::Positional(params) => {
                    if statement.num_params() as usize != params.len() {
                        Err(DriverError::StmtParamsMismatch {
                            required: statement.num_params(),
                            supplied: params.len() as u16,
                        })?
                    }

                    let params = params.into_iter().collect::<Vec<_>>();

                    let (body, as_long_data) =
                        ComStmtExecuteRequestBuilder::new(statement.id()).build(&*params);

                    if as_long_data {
                        self.send_long_data(statement.id(), params.iter()).await?;
                    }

                    self.write_command_raw(body).await?;
                    self.read_result_set::<BinaryProtocol>().await?;
                    break;
                }
                Params::Named(_) => {
                    if statement.named_params.is_none() {
                        let error = DriverError::NamedParamsForPositionalQuery.into();
                        return Err(error);
                    }

                    params = match params.into_positional(statement.named_params.as_ref().unwrap())
                    {
                        Ok(positional_params) => positional_params,
                        Err(error) => return Err(error.into()),
                    };

                    continue;
                }
                Params::Empty => {
                    if statement.num_params() > 0 {
                        let error = DriverError::StmtParamsMismatch {
                            required: statement.num_params(),
                            supplied: 0,
                        }
                        .into();
                        return Err(error);
                    }

                    let (body, _) = ComStmtExecuteRequestBuilder::new(statement.id()).build(&[]);
                    self.write_command_raw(body).await?;
                    self.read_result_set::<BinaryProtocol>().await?;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Helper, that sends all `Value::Bytes` in the given list of paramenters as long data.
    async fn send_long_data<'a, I>(&mut self, statement_id: u32, params: I) -> Result<()>
    where
        I: Iterator<Item = &'a Value>,
    {
        for (i, value) in params.enumerate() {
            if let Value::Bytes(bytes) = value {
                let chunks = bytes.chunks(MAX_PAYLOAD_LEN - 6);
                let chunks = chunks.chain(if bytes.is_empty() {
                    Some(&[][..])
                } else {
                    None
                });
                for chunk in chunks {
                    let com = ComStmtSendLongData::new(statement_id, i, chunk);
                    self.write_command_raw(com.into()).await?;
                }
            }
        }

        Ok(())
    }

    /// Helper, that closes statement with the given id.
    pub(crate) async fn close_statement(&mut self, id: u32) -> Result<()> {
        self.stmt_cache_mut().remove(id);
        self.write_command_raw(ComStmtClose::new(id).into()).await
    }
}
