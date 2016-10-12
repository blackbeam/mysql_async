use errors::*;
use byteorder::{
    ReadBytesExt,
    LittleEndian as LE,
};

use Column;
use Conn;
use Params;

use self::futures::{
    Execute,
    new_execute,
};

pub mod futures;

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
    pub fn execute<T: Into<Params>>(self, params: T) -> Execute {
        new_execute(self, params.into())
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
    pub fn params(&self) -> Option<&Vec<Column>> {
        self.stmt.params.as_ref()
    }

    #[doc(hidden)]
    pub fn named_params(&self) -> Option<&Vec<String>> {
        self.stmt.named_params.as_ref()
    }
}
