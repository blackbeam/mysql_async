// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::stmt::futures::*;
use byteorder::ReadBytesExt;
use byteorder::LittleEndian as LE;
use errors::*;
use proto::Column;
use value::FromRow;
use value::Params;


pub mod futures;


/// Inner statement representation.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct InnerStmt {
    /// Positions and names of named parameters
    pub named_params: Option<Vec<String>>,
    pub params: Option<Vec<Column>>,
    pub columns: Option<Vec<Column>>,
    pub statement_id: u32,
    pub num_columns: u16,
    pub num_params: u16,
    pub warning_count: u16,
}

impl InnerStmt {
    // TODO: Consume payload?
    pub fn new(pld: &[u8], named_params: Option<Vec<String>>) -> Result<InnerStmt> {
        let mut reader = &pld[1..];
        let statement_id = try!(reader.read_u32::<LE>());
        let num_columns = try!(reader.read_u16::<LE>());
        let num_params = try!(reader.read_u16::<LE>());
        let warning_count = try!(reader.read_u16::<LE>());
        Ok(InnerStmt {
            named_params: named_params,
            statement_id: statement_id,
            num_columns: num_columns,
            num_params: num_params,
            warning_count: warning_count,
            params: None,
            columns: None,
        })
    }
}

/// Prepered MySql statement.
#[derive(Debug)]
pub struct Stmt {
    stmt: InnerStmt,
    conn: Conn,
}

pub fn new_stmt(stmt: InnerStmt, conn: Conn) -> Stmt {
    Stmt {
        stmt: stmt,
        conn: conn,
    }
}

impl Stmt {
    /// Returns future that executes statement and resolves to `BinQueryResult`.
    pub fn execute<T: Into<Params>>(self, params: T) -> Execute {
        new_execute(self, params.into())
    }

    /// Returns future that performs batch execution of statement and resolves to `Stmt`.
    ///
    /// All results will be dropped.
    pub fn batch<T: Into<Params>>(self, params_vec: Vec<T>) -> Batch {
        new_batch(self, params_vec)
    }

    /// Returns future that executes statement and resolves to a first row of result if any.
    ///
    /// Returned future will call `R::from_row(row)` internally.
    pub fn first<T: Into<Params>, R: FromRow>(self, params: T) -> First<R> {
        new_first::<R>(self, params.into())
    }

    /// Unwraps `Conn`.
    pub fn unwrap(self) -> Conn {
        self.conn
    }

    #[doc(hidden)]
    pub fn num_params(&self) -> u16 {
        self.stmt.num_params
    }

    #[doc(hidden)]
    pub fn num_columns(&self) -> u16 {
        self.stmt.num_columns
    }

    #[doc(hidden)]
    pub fn id(&self) -> u32 {
        self.stmt.statement_id
    }

    #[doc(hidden)]
    pub fn params(&self) -> Option<&[Column]> {
        self.stmt.params.as_ref().map(|x| &**x)
    }

    #[doc(hidden)]
    pub fn named_params(&self) -> Option<&[String]> {
        self.stmt.named_params.as_ref().map(|x| &**x)
    }
}
