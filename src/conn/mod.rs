// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::futures::*;
use conn::futures::query_result::BinQueryResult;
use conn::futures::query_result::ResultKind;
use conn::futures::query_result::UnconsumedQueryResult;
use conn::futures::query_result::futures::DropResult as DropQueryResult;
use conn::pool::Pool;
use conn::stmt::Stmt;
use conn::stmt::InnerStmt;
use conn::transaction::IsolationLevel;
use conn::transaction::futures::new_start_transaction;
use conn::transaction::futures::StartTransaction;
use consts;
use io::Stream;
use lib_futures::Future;
use lib_futures::stream::Stream as FuturesStream;
use opts::Opts;
use proto::Column;
use proto::OkPacket;
use value::FromRow;
use value::Params;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::fmt;
use std::mem;
use std::sync::Arc;
use time::SteadyTime;
use tokio::reactor::Handle;
use twox_hash::XxHash;


pub mod futures;
pub mod named_params;
pub mod pool;
pub mod stmt;
pub mod transaction;


/// Mysql connection
pub struct Conn {
    stream: Option<Stream>,
    id: u32,
    version: (u16, u16, u16),
    seq_id: u8,
    last_command: consts::Command,
    max_allowed_packet: u64,
    capabilities: consts::CapabilityFlags,
    status: consts::StatusFlags,
    last_insert_id: u64,
    affected_rows: u64,
    warnings: u16,
    pool: Option<Pool>,
    has_result: Option<(Arc<Vec<Column>>, Option<OkPacket>, Option<InnerStmt>)>,
    in_transaction: bool,
    opts: Opts,
    handle: Handle,
    last_io: SteadyTime,
    wait_timeout: u32,
    stmt_cache: HashMap<String, InnerStmt, BuildHasherDefault<XxHash>>,
}

impl fmt::Debug for Conn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Conn")
            .field("connection id", &self.id)
            .field("server version", &self.version)
            .field("pool", &self.pool)
            .field("has result", &self.has_result.is_some())
            .field("in transaction", &self.in_transaction)
            .field("options", &self.opts)
            .finish()
    }
}


impl Conn {
    /// Hacky way to move connection through &mut. `self` becomes unusable.
    fn take(&mut self) -> Conn {
        let handle = self.handle.clone();
        mem::replace(self,
                     Conn {
                         stream: Default::default(),
                         id: Default::default(),
                         version: Default::default(),
                         seq_id: Default::default(),
                         last_command: consts::Command::COM_QUIT,
                         max_allowed_packet: Default::default(),
                         capabilities: consts::CapabilityFlags::empty(),
                         status: consts::StatusFlags::empty(),
                         last_insert_id: Default::default(),
                         affected_rows: Default::default(),
                         warnings: Default::default(),
                         pool: Default::default(),
                         has_result: Default::default(),
                         in_transaction: false,
                         opts: Default::default(),
                         handle: handle,
                         last_io: SteadyTime::now(),
                         wait_timeout: 0,
                         stmt_cache: Default::default(),
                     })
    }

    /// Returns future that resolves to `Conn`.
    pub fn new<O>(opts: O, handle: &Handle) -> NewConn
        where O: Into<Opts>,
    {
        let opts = opts.into();

        let future = {
            let addr = opts.get_ip_or_hostname();
            let port = opts.get_tcp_port();
            Stream::connect((addr, port), &handle)
        };

        new_new_conn(future, opts, handle)
    }

    /// Returns future that resolves to `Conn` with `max_allowed_packet` stored in it.
    fn read_max_allowed_packet(self) -> ReadMaxAllowedPacket {
        fn map((maybe_max_allwed_packet, mut conn): (Option<(u64,)>, Conn)) -> Conn {
            conn.max_allowed_packet = maybe_max_allwed_packet.unwrap_or((1024 * 1024 * 2,)).0;
            conn
        }

        self.first::<(u64,), _>("SELECT @@max_allowed_packet").map(map)
    }

    /// Returns future that resolves to `Conn` with `wait_timeout` stored in it.
    fn read_wait_timeout(self) -> ReadWaitTimeout {
        fn map((maybe_wait_timeout, mut conn): (Option<(u32,)>, Conn)) -> Conn {
            conn.wait_timeout = maybe_wait_timeout.unwrap_or((28800,)).0;
            conn
        }

        self.first::<(u32,), _>("SELECT @@wait_timeout").map(map)
    }

    /// Returns future that resolves to `Conn` if `COM_PING` executed successfully.
    pub fn ping(self) -> Ping {
        new_ping(self.write_command_data(consts::Command::COM_PING, &[]))
    }

    /// Returns future that executes `query` and resolves to `TextQueryResult`.
    pub fn query<Q: AsRef<str>>(self, query: Q) -> Query {
        let query = query.as_ref().as_bytes();
        new_query(self.write_command_data(consts::Command::COM_QUERY, query))
    }

    /// Returns future that resolves to a first row of result of a `query` execution (if any).
    pub fn first<R, Q>(self, query: Q) -> First<R>
        where Q: AsRef<str>,
              R: FromRow,
    {
        new_first(self.query(query))
    }

    /// Returns future that performs query, drops result and resolves to `Conn`.
    pub fn drop_query<Q: AsRef<str>>(self, query: Q) -> DropQuery {
        self.query(query).and_then(UnconsumedQueryResult::drop_result)
    }

    /// Returns future that resolves to a `Stmt`.
    pub fn prepare<Q>(self, query: Q) -> Prepare
        where Q: AsRef<str>,
    {
        new_prepare(self, query.as_ref())
    }

    /// Returns future that prepares and executes statement in one pass and resolves to
    /// `BinQueryResult`.
    pub fn prep_exec<Q, P>(self, query: Q, params: P) -> PrepExec
        where Q: AsRef<str>,
              P: Into<Params>,
    {
        new_prep_exec(self, query, params)
    }

    /// Returns future that resolves to a first row of result of a statement execution (if any).
    ///
    /// Returned future will call `R::from_row(row)` internally.
    pub fn first_exec<R, Q, P>(self, query: Q, params: P) -> FirstExec<R>
        where Q: AsRef<str>,
              P: Into<Params>,
              R: FromRow,
    {
        new_first_exec(self, query, params)
    }

    /// Returns future that prepares and executes statement, drops result and resolves to `Conn`.
    pub fn drop_exec<Q, P>(self, query: Q, params: P) -> DropExec
        where Q: AsRef<str>,
              P: Into<Params>,
    {
        self.prep_exec(query, params)
            .and_then(UnconsumedQueryResult::drop_result as fn(BinQueryResult)
                                                               -> DropQueryResult<BinQueryResult>)
            .map(Stmt::unwrap as fn(Stmt) -> Conn)
    }

    /// Returns future that prepares statement, performs batch execution and resolves to `Conn`.
    ///
    /// All results will be dropped.
    pub fn batch_exec<Q, P>(self, query: Q, params_vec: Vec<P>) -> BatchExec
        where Q: AsRef<str>,
              P: Into<Params>,
    {
        new_batch_exec(self, query, params_vec)
    }

    /// Returns future that resolves to a `Conn` with `COM_RESET_CONNECTION` executed on it.
    pub fn reset(self) -> Reset {
        new_reset(self)
    }

    /// Returns future that consumes `Conn` and disconnects it from a server.
    pub fn disconnect(mut self) -> Disconnect {
        fn map(_: Conn) {
            ()
        }

        self.pool = None;
        self.write_command_data(consts::Command::COM_QUIT, &[]).map(map)
    }

    /// Returns future that starts transaction and resolves to `Transaction`.
    pub fn start_transaction(self,
                             consistent_snapshot: bool,
                             isolation_level: Option<IsolationLevel>,
                             readonly: Option<bool>)
                             -> StartTransaction {
        new_start_transaction(self, consistent_snapshot, isolation_level, readonly)
    }

    fn rollback_transaction(mut self) -> DropQuery {
        assert!(self.in_transaction);
        self.in_transaction = false;
        self.drop_query("ROLLBACK")
    }

    /// Returns future that reads result from a server and resolves to `Conn`.
    fn drop_result(mut self) -> DropResult {
        let has_result = self.has_result.take();
        new_drop_result(self, has_result)
    }

    /// Returns future that resolves to `RawQueryResult` of corresponding `K: ResultKind`.
    fn handle_result_set<K: ResultKind>(self,
                                        inner_stmt: Option<InnerStmt>)
                                        -> NewRawQueryResult<K> {
        new_new_raw_query_result::<K>(self.read_packet(), inner_stmt)
    }

    /// Returns future that resolves to a columns of result set.
    fn read_result_set_columns(self, column_count: u64) -> Columns {
        new_columns(self.read_packet(), column_count)
    }

    /// Returns future that writes comand and it's data to a server and resolves to `Conn`.
    fn write_command_data<D>(mut self, cmd: consts::Command, command_data: D) -> WritePacket
        where D: AsRef<[u8]>,
    {
        let mut data = Vec::with_capacity(1 + command_data.as_ref().len());
        data.push(cmd as u8);
        data.extend_from_slice(command_data.as_ref());
        self.seq_id = 0;
        self.write_packet(data)
    }

    /// Returns future that writes packet to a server and resolves to `Conn`.
    fn write_packet<D>(mut self, data: D) -> WritePacket
        where D: Into<Vec<u8>>,
    {
        let stream = self.stream.take().unwrap();
        let future = stream.write_packet(data.into(), self.seq_id);
        new_write_packet(self, future)
    }

    /// Returns future that read packet from a server end resolves to `(Conn, Packet)`.
    fn read_packet(mut self) -> ReadPacket {
        let stream = self.stream.take().unwrap();
        let future = stream.into_future();
        new_read_packet(self, future)
    }
}

#[cfg(test)]
mod test {
    use opts::OptsBuilder;
    use env_logger;
    use either::*;
    use prelude::*;
    use lib_futures::Future;
    use super::Conn;
    use test_misc::DATABASE_URL;
    use tokio::reactor::Core;
    use value::from_row;
    use WhiteListFsLocalInfileHandler;


    #[test]
    fn should_connect() {
        env_logger::init().unwrap();
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| conn.ping())
            .and_then(|conn| conn.disconnect());

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_execute_init_queryes_on_new_connection() {
        let mut lp = Core::new().unwrap();

        let mut opts_builder = OptsBuilder::from_opts(&**DATABASE_URL);
        opts_builder.init(vec!["SET @a = 42", "SET @b = 'foo'"]);
        let fut = Conn::new(opts_builder, &lp.handle())
            .and_then(|conn| conn.query("SELECT @a, @b"))
            .and_then(|query_result| query_result.collect::<(u8, String)>())
            .and_then(|(result_set, conn)| conn.right().unwrap().disconnect().map(|_| result_set));

        let result = lp.run(fut).unwrap();
        assert_eq!(result.0, vec![(42, "foo".into())]);
    }

    #[test]
    fn should_reset_the_connection() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| conn.reset())
            .and_then(|conn| conn.disconnect());

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_perform_queries() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.query(r"
                SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 231
            ")
            })
            .and_then(|query_result| {
                query_result.reduce(vec![], |mut accum, row| {
                    accum.push(from_row(row));
                    accum
                })
            })
            .and_then(|(result, conn)| conn.right().unwrap().disconnect().map(|_| result));

        let result = lp.run(fut).unwrap();
        assert_eq!((String::from("hello"), 123), result[0]);
        assert_eq!((String::from("world"), 231), result[1]);
    }

    #[test]
    fn should_drop_query() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| conn.drop_query("CREATE TEMPORARY TABLE tmp (id int, name text)"))
            .and_then(|conn| conn.drop_query("INSERT INTO tmp VALUES (1, 'foo')"))
            .and_then(|conn| conn.first::<(u8,), _>("SELECT COUNT(*) FROM tmp"))
            .and_then(|(result, conn)| conn.disconnect().map(move |_| result));

        let result = lp.run(fut).unwrap();
        assert_eq!(result, Some((1,)));
    }

    #[test]
    fn should_handle_mutliresult_set() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.query(r"
                SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 231;
                SELECT 'foo', 255;
            ")
            })
            .and_then(|query_result| query_result.collect_all())
            .and_then(|(sets, conn)| conn.disconnect().map(|_| sets));

        let sets = lp.run(fut).unwrap();
        assert_eq!(sets.len(), 2);
        for (i, rows) in sets.into_iter().enumerate() {
            if i == 0 {
                assert_eq!((String::from("hello"), 123),
                           from_row(rows.as_ref()[0].clone()));
                assert_eq!((String::from("world"), 231),
                           from_row(rows.as_ref()[1].clone()));
            }
            if i == 1 {
                assert_eq!((String::from("foo"), 255),
                           from_row(rows.as_ref()[0].clone()));
            }
        }
    }

    #[test]
    fn should_map_resultset() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.query(r"
                SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 231;
                SELECT 'foo', 255;
            ")
            })
            .and_then(|query_result| query_result.map(|row| from_row::<(String, u8)>(row)))
            .and_then(|(out, next)| {
                next.left()
                    .unwrap()
                    .collect_all()
                    .and_then(|(_, conn)| conn.disconnect())
                    .map(|_| out)
            });

        let sets = lp.run(fut).unwrap();
        assert_eq!(sets.len(), 2);
        assert_eq!((String::from("hello"), 123), sets[0].clone());
        assert_eq!((String::from("world"), 231), sets[1].clone());
    }

    #[test]
    fn should_reduce_resultset() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.query(r"
                SELECT 5
                UNION ALL
                SELECT 6;
                SELECT 'foo';
            ")
            })
            .and_then(|query_result| {
                query_result.reduce(0, |mut acc, row| {
                    let (val,) = from_row::<(u8,)>(row);
                    acc += val;
                    acc
                })
            })
            .and_then(|(out, next)| {
                next.left()
                    .unwrap()
                    .collect_all()
                    .and_then(|(_, conn)| conn.disconnect())
                    .map(move |_| out)
            });

        let result = lp.run(fut).unwrap();
        assert_eq!(result, 11);
    }

    #[test]
    fn should_iterate_over_resultset() {
        use std::cell::RefCell;
        let mut lp = Core::new().unwrap();
        let acc: RefCell<u8> = RefCell::new(1);

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.query(r"
                SELECT 2
                UNION ALL
                SELECT 3;
                SELECT 5;
            ")
            })
            .and_then(|query_result| {
                query_result.for_each(|row| {
                    let (x,) = from_row(row);
                    *acc.borrow_mut() *= x;
                })
            })
            .and_then(|maybe_next| {
                if let Left(query_result) = maybe_next {
                    query_result.for_each(|row| {
                        let (x,) = from_row(row);
                        *acc.borrow_mut() *= x;
                    })
                } else {
                    unreachable!();
                }
            })
            .and_then(|conn| conn.right().unwrap().disconnect());

        lp.run(fut).unwrap();
        assert_eq!(*acc.borrow(), 30);
    }

    #[test]
    fn should_prepare_statement() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| conn.prepare(r"SELECT ?"))
            .and_then(|stmt| stmt.unwrap().disconnect());

        lp.run(fut).unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| conn.prepare(r"SELECT :foo"))
            .and_then(|stmt| stmt.unwrap().disconnect());

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_execute_statement() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| conn.prepare(r"SELECT ?"))
            .and_then(|stmt| stmt.execute((42,)))
            .and_then(|query_result| query_result.collect::<(u8,)>())
            .and_then(|(result_set, stmt)| {
                assert_eq!(result_set.as_ref(), [(42u8,)]);
                stmt.execute(("foo",))
            })
            .and_then(|query_result| query_result.map(|row| from_row::<(String,)>(row)))
            .and_then(|(mut rows, stmt)| {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows.pop(), Some(("foo".into(),)));
                stmt.execute((8,))
            })
            .and_then(|query_result| {
                query_result.reduce(2, |acc, row| {
                    let (val,): (u8,) = from_row(row);
                    acc + val
                })
            })
            .and_then(|(output, stmt)| stmt.unwrap().disconnect().map(move |_| output));

        let output = lp.run(fut).unwrap();
        assert_eq!(output, 10);

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| conn.prepare(r"SELECT :foo, :bar, :foo"))
            .and_then(|stmt| {
                stmt.execute(params! {
                "foo" => 2,
                "bar" => 3,
            })
            })
            .and_then(|query_result| query_result.collect::<(u8, u8, u8)>())
            .and_then(|(result_set, stmt)| {
                assert_eq!(result_set.as_ref(), [(2, 3, 2)]);
                stmt.execute(params! {
                "foo" => "quux",
                "bar" => "baz",
            })
            })
            .and_then(|query_result| {
                query_result.map(|row| from_row::<(String, String, String)>(row))
            })
            .and_then(|(mut rows, stmt)| {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows.pop(),
                           Some(("quux".into(), "baz".into(), "quux".into())));
                stmt.execute(params! {
                "foo" => 2,
                "bar" => 3,
            })
            })
            .and_then(|query_result| {
                query_result.reduce(0, |acc, row| {
                    let (a, b, c): (u8, u8, u8) = from_row(row);
                    acc + a + b + c
                })
            })
            .and_then(|(output, stmt)| stmt.unwrap().disconnect().map(move |_| output));

        let output = lp.run(fut).unwrap();
        assert_eq!(output, 7);
    }

    #[test]
    fn should_prep_exec_statement() {

        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.prep_exec(r"SELECT :a, :b, :a",
                               params! {
                "a" => 2,
                "b" => 3,
            })
            })
            .and_then(|query_result| {
                query_result.map(|row| {
                    let (a, b, c): (u8, u8, u8) = from_row(row);
                    a * b * c
                })
            })
            .and_then(|(output, stmt)| stmt.unwrap().disconnect().map(move |_| output[0]));

        let output = lp.run(fut).unwrap();
        assert_eq!(output, 12u8);
    }

    #[test]
    fn should_first_exec_statement() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.first_exec::<(u8,), _, _>(r"SELECT :a UNION ALL SELECT :b",
                                               params! {
                "a" => 2,
                "b" => 3,
            })
            })
            .and_then(|(row, conn)| conn.disconnect().map(move |_| row.unwrap()));

        let output = lp.run(fut).unwrap();
        assert_eq!(output, (2,));
    }

    #[test]
    fn should_use_statement_cache() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| conn.drop_exec("SELECT ?", (42,)))
            .and_then(|conn| {
                conn.drop_exec("SELECT :foo, :bar", params! ("foo" => 42, "bar" => "baz"))
            })
            .and_then(|conn| conn.first_exec::<(u8,), _, _>("SELECT ?", (42,)))
            .and_then(|(row, conn)| {
                assert_eq!(row, Some((42,)));
                conn.first_exec::<(u8, u8), _, _>("SELECT :baz, :quux",
                                                  params!("baz" => 1, "quux" => 2))
            })
            .and_then(|(row, conn)| {
                assert_eq!(row, Some((1, 2)));
                assert_eq!(conn.stmt_cache.len(), 2);
                conn.disconnect()
            })
            .map(|_| ());

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_run_transactions() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.query("CREATE TEMPORARY TABLE tmp (id INT, name TEXT)")
                    .and_then(|result| result.drop_result())
            })
            .and_then(|conn| conn.start_transaction(false, None, None))
            .and_then(|transaction| {
                transaction.query("INSERT INTO tmp VALUES (1, 'foo'), (2, 'bar')")
                    .and_then(|result| result.drop_result())
            })
            .and_then(|transaction| transaction.commit())
            .and_then(|conn| {
                conn.first::<(usize,), _>("SELECT COUNT(*) FROM tmp").map(|(result, conn)| {
                    assert_eq!(result, Some((2,)));
                    conn
                })
            })
            .and_then(|conn| conn.start_transaction(false, None, None))
            .and_then(|transaction| {
                transaction.query("INSERT INTO tmp VALUES (3, 'baz'), (4, 'quux')")
                    .and_then(|result| result.drop_result())
            })
            .and_then(|transaction| {
                transaction.prep_exec("SELECT COUNT(*) FROM tmp", ())
                    .and_then(|result| result.collect::<(usize,)>())
                    .map(|(set, transaction)| {
                        assert_eq!(set.0[0], (4,));
                        transaction
                    })
            })
            .and_then(|transaction| transaction.rollback())
            .and_then(|conn| {
                conn.first::<(usize,), _>("SELECT COUNT(*) FROM tmp").map(|(result, conn)| {
                    assert_eq!(result, Some((2,)));
                    conn
                })
            })
            .and_then(|conn| conn.disconnect());

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_handle_local_infile() {
        use std::io::Write;

        let mut lp = Core::new().unwrap();
        let mut opts = OptsBuilder::from_opts(&**DATABASE_URL);
        opts.local_infile_handler(Some(WhiteListFsLocalInfileHandler::new(&[
            "local_infile.txt",
        ][..])));

        let fut = Conn::new(opts, &lp.handle())
            .and_then(|conn| conn.drop_query("CREATE TEMPORARY TABLE tmp (a TEXT);"))
            .and_then(|conn| {
                let mut file = ::std::fs::File::create("local_infile.txt").unwrap();
                let _ = file.write(b"AAAAAA\n");
                let _ = file.write(b"BBBBBB\n");
                let _ = file.write(b"CCCCCC\n");
                conn.drop_query("LOAD DATA LOCAL INFILE 'local_infile.txt' INTO TABLE tmp;")
            })
            .and_then(|conn| {
                conn.prep_exec("SELECT * FROM tmp;", ())
                    .and_then(|result| result.map(|row| from_row::<(String,)>(row).0))
            })
            .and_then(|(result, stmt)| {
                assert_eq!(result.len(), 3);
                assert_eq!(result[0], "AAAAAA");
                assert_eq!(result[1], "BBBBBB");
                assert_eq!(result[2], "CCCCCC");
                stmt.unwrap().drop_result()
            })
            .and_then(|conn| {
                let _ = ::std::fs::remove_file("local_infile.txt");
                conn.disconnect().map(|_| ())
            });

        lp.run(fut).unwrap();
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;
        use conn::Conn;
        use tokio::reactor::Core;
        use lib_futures::Future;
        use test_misc::DATABASE_URL;

        #[bench]
        fn connect(bencher: &mut test::Bencher) {
            let mut lp = Core::new().unwrap();

            bencher.iter(|| {
                let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| conn.ping());
                lp.run(fut).unwrap();
            })
        }
    }
}
