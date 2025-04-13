use std::mem;

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::{packets::ComStmtExecuteRequestBuilder, params::Params};
#[cfg(feature = "tracing")]
use tracing::{field, info_span, Level, Span};

use crate::{conn::MAX_STATEMENT_PARAMS, BinaryProtocol, Conn, DriverError, Statement};

use super::Routine;

/// A routine that executes `COM_STMT_EXECUTE`.
#[derive(Debug, Clone)]
pub struct ExecRoutine<'a> {
    stmt: &'a Statement,
    params: Params,
}

impl<'a> ExecRoutine<'a> {
    pub fn new(stmt: &'a Statement, params: Params) -> Self {
        Self { stmt, params }
    }
}

impl Routine<()> for ExecRoutine<'_> {
    fn call<'a>(&'a mut self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>> {
        #[cfg(feature = "tracing")]
        let span = info_span!(
            "mysql_async::exec",
            mysql_async.connection.id = conn.id(),
            mysql_async.statement.id = self.stmt.id(),
            mysql_async.query.params = field::Empty,
        );

        let fut = async move {
            loop {
                match self.params {
                    Params::Positional(ref params) => {
                        #[cfg(feature = "tracing")]
                        if tracing::span_enabled!(Level::DEBUG) {
                            // The params may contain sensitive data. Restrict to DEBUG.
                            // TODO: make more efficient
                            // TODO: use intersperse() once stable
                            let sep = std::iter::repeat(", ");
                            let ps = params
                                .iter()
                                .map(|p| p.as_sql(true))
                                .zip(sep)
                                .map(|(val, sep)| val + sep)
                                .collect::<String>();
                            Span::current().record("mysql_async.query.params", ps);
                        }

                        if params.len() > MAX_STATEMENT_PARAMS {
                            Err(DriverError::StmtParamsNumberExceedsLimit {
                                supplied: params.len(),
                            })?
                        }

                        if self.stmt.num_params() as usize != params.len() {
                            Err(DriverError::StmtParamsMismatch {
                                required: self.stmt.num_params(),
                                supplied: params.len(),
                            })?
                        }

                        let (body, as_long_data) =
                            ComStmtExecuteRequestBuilder::new(self.stmt.id()).build(params);

                        if as_long_data {
                            conn.send_long_data(self.stmt.id(), params.iter()).await?;
                        }

                        conn.write_command(&body).await?;
                        conn.read_result_set::<BinaryProtocol>(true).await?;
                        break;
                    }
                    Params::Named(_) => {
                        if self.stmt.named_params.is_empty() {
                            let error = DriverError::NamedParamsForPositionalQuery.into();
                            return Err(error);
                        }

                        let named = mem::replace(&mut self.params, Params::Empty);
                        self.params = named.into_positional(&self.stmt.named_params)?;

                        continue;
                    }
                    Params::Empty => {
                        if self.stmt.num_params() > 0 {
                            let error = DriverError::StmtParamsMismatch {
                                required: self.stmt.num_params(),
                                supplied: 0,
                            }
                            .into();
                            return Err(error);
                        }

                        let (body, _) =
                            ComStmtExecuteRequestBuilder::new(self.stmt.id()).build(&[]);
                        conn.write_command(&body).await?;
                        conn.read_result_set::<BinaryProtocol>(true).await?;
                        break;
                    }
                }
            }
            Ok(())
        };

        #[cfg(feature = "tracing")]
        let fut = instrument_result!(fut, span);

        fut.boxed()
    }
}
