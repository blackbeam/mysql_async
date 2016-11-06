use conn::futures::*;
use conn::futures::query_result::ResultKind;
use conn::pool::Pool;
use conn::stmt::InnerStmt;
use consts;
use io::Stream;
use lib_futures::stream::Stream as FuturesStream;
use opts::Opts;
use proto::Column;
use proto::OkPacket;
use value::FromRow;
use value::Params;
use std::mem;
use std::sync::Arc;
use tokio::reactor::Handle;


pub mod futures;
pub mod named_params;
pub mod pool;
pub mod stmt;


/// Mysql connection
#[derive(Debug)]
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
}

impl Conn {
    /// Hacky way to move connection through &mut. `self` becomes unusable.
    fn take(&mut self) -> Conn {
        mem::replace(self, Conn {
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

        new_new_conn(future, opts)
    }

    /// Returns future that resolves to `Conn` with `max_allowed_packet` stored in it.
    fn read_max_allowed_packet(self) -> ReadMaxAllowedPacket {
        new_read_max_allowed_packet(self.first::<(u64,), _>("SELECT @@max_allowed_packet"))
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
          R: FromRow + Send + 'static,
    {
        new_first(self.query(query))
    }

    // TODO: Implement cache.
    /// Returns future that resolves to a `Stmt`.
    pub fn prepare<Q>(self, query: Q) -> Prepare
        where Q: AsRef<str>
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


    /// Returns future that resolves to a `Conn` with `COM_RESET_CONNECTION` executed on it.
    pub fn reset(self) -> Reset {
        new_reset(self.write_command_data(consts::Command::COM_RESET_CONNECTION, &[]))
    }

    /// Returns future that consumes `Conn` and disconnects it from a server.
    pub fn disconnect(mut self) -> Disconnect {
        self.pool = None;
        new_disconnect(self.write_command_data(consts::Command::COM_QUIT, &[]))
    }

    /// Return future that reads result from a server and resolves to `Conn`.
    fn drop_result(mut self) -> DropResult {
        let has_result = self.has_result.take();
        new_drop_result(self, has_result)
    }

    /// Returns future that resolves to `RawQueryResult` of corresponding `K: ResultKind`.
    fn handle_result_set<K: ResultKind>(self, inner_stmt: Option<InnerStmt>) -> NewRawQueryResult<K> {
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
    use either::Left;
    use prelude::*;
    use lib_futures::Future;

    use super::Conn;

    use test_misc::DATABASE_URL;

    use tokio::reactor::Core;

    use value::from_row;

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
        let fut = Conn::new(opts_builder, &lp.handle()).and_then(|conn| {
            conn.query("SELECT @a, @b")
        }).and_then(|query_result| {
            query_result.collect::<(u8, String)>()
        }).and_then(|(result_set, conn)| {
            conn.right().unwrap().disconnect().map(|_| result_set)
        });

        let result = lp.run(fut).unwrap();
        assert_eq!(result.0, vec![(42, "foo".into())]);
    }

    #[test]
    fn should_reset_the_connection() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| {
            conn.reset()
        }).and_then(|conn| conn.disconnect());

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_perform_queries() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| {
            conn.query(r"
                SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 231
            ")
        }).and_then(|query_result| {
            query_result.reduce(vec![], |mut accum, row| {
                accum.push(from_row(row));
                accum
            })
        }).and_then(|(result, conn)| {
            conn.right().unwrap().disconnect().map(|_| result)
        });

        let result = lp.run(fut).unwrap();
        assert_eq!((String::from("hello"), 123), result[0]);
        assert_eq!((String::from("world"), 231), result[1]);
    }

    #[test]
    fn should_handle_mutliresult_set() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| {
            conn.query(r"
                SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 231;
                SELECT 'foo', 255;
            ")
        }).and_then(|query_result| {
            query_result.collect_all()
        }).and_then(|(sets, conn)| {
            conn.disconnect().map(|_| sets)
        });

        let sets = lp.run(fut).unwrap();
        assert_eq!(sets.len(), 2);
        for (i, rows) in sets.into_iter().enumerate() {
            if i == 0 {
                assert_eq!((String::from("hello"), 123), from_row(rows.as_ref()[0].clone()));
                assert_eq!((String::from("world"), 231), from_row(rows.as_ref()[1].clone()));
            }
            if i == 1 {
                assert_eq!((String::from("foo"), 255), from_row(rows.as_ref()[0].clone()));
            }
        }
    }

    #[test]
    fn should_map_resultset() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| {
            conn.query(r"
                SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 231;
                SELECT 'foo', 255;
            ")
        }).and_then(|query_result| {
            query_result.map(|row| from_row::<(String, u8)>(row))
        }).and_then(|(out, next)| {
            next.left().unwrap().collect_all().and_then(|(_, conn)| conn.disconnect()).map(|_| out)
        });

        let sets = lp.run(fut).unwrap();
        assert_eq!(sets.len(), 2);
        assert_eq!((String::from("hello"), 123), sets[0].clone());
        assert_eq!((String::from("world"), 231), sets[1].clone());
    }

    #[test]
    fn should_reduce_resultset() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| {
            conn.query(r"
                SELECT 5
                UNION ALL
                SELECT 6;
                SELECT 'foo';
            ")
        }).and_then(|query_result| {
            query_result.reduce(0, |mut acc, row| {
                let (val,) = from_row::<(u8,)>(row);
                acc += val;
                acc
            })
        }).and_then(|(out, next)| {
            next.left().unwrap().collect_all().and_then(|(_, conn)| conn.disconnect()).map(move|_| out)
        });

        let result = lp.run(fut).unwrap();
        assert_eq!(result, 11);
    }

    #[test]
    fn should_iterate_over_resultset() {
        use std::cell::RefCell;
        let mut lp = Core::new().unwrap();
        let acc: RefCell<u8> = RefCell::new(1);

        let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| {
            conn.query(r"
                SELECT 2
                UNION ALL
                SELECT 3;
                SELECT 5;
            ")
        }).and_then(|query_result| {
            query_result.for_each(|row| {
                let (x,) = from_row(row);
                *acc.borrow_mut() *= x;
            })
        }).and_then(|maybe_next| {
            if let Left(query_result) = maybe_next {
                query_result.for_each(|row| {
                    let (x,) = from_row(row);
                    *acc.borrow_mut() *= x;
                })
            } else {
                unreachable!();
            }
        }).and_then(|conn| {
            conn.right().unwrap().disconnect()
        });

        lp.run(fut).unwrap();
        assert_eq!(*acc.borrow(), 30);
    }

    #[test]
    fn should_prepare_statement() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.prepare(r"SELECT ?")
            }).and_then(|stmt| stmt.unwrap().disconnect());

        lp.run(fut).unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.prepare(r"SELECT :foo")
            }).and_then(|stmt| stmt.unwrap().disconnect());

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_execute_statement() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.prepare(r"SELECT ?")
            }).and_then(|stmt| {
                stmt.execute((42,))
            }).and_then(|query_result| {
                query_result.collect::<(u8,)>()
            }).and_then(|(result_set, stmt)| {
                assert_eq!(result_set.as_ref(), [(42u8,)]);
                stmt.execute(("foo",))
            }).and_then(|query_result| {
                query_result.map(|row| {
                    from_row::<(String,)>(row)
                })
            }).and_then(|(mut rows, stmt)| {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows.pop(), Some(("foo".into(),)));
                stmt.execute((8,))
            }).and_then(|query_result| {
                query_result.reduce(2, |acc, row| {
                    let (val,): (u8,) = from_row(row);
                    acc + val
                })
            }).and_then(|(output, stmt)| {
                stmt.unwrap().disconnect().map(move|_| output)
            });

        let output = lp.run(fut).unwrap();
        assert_eq!(output, 10);

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.prepare(r"SELECT :foo, :bar, :foo")
            }).and_then(|stmt| {
            stmt.execute(params! {
                "foo" => 2,
                "bar" => 3,
            })
        }).and_then(|query_result| {
            query_result.collect::<(u8, u8, u8)>()
        }).and_then(|(result_set, stmt)| {
            assert_eq!(result_set.as_ref(), [(2, 3, 2)]);
            stmt.execute(params! {
                "foo" => "quux",
                "bar" => "baz",
            })
        }).and_then(|query_result| {
            query_result.map(|row| {
                from_row::<(String, String, String)>(row)
            })
        }).and_then(|(mut rows, stmt)| {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows.pop(), Some(("quux".into(), "baz".into(), "quux".into())));
            stmt.execute(params! {
                "foo" => 2,
                "bar" => 3,
            })
        }).and_then(|query_result| {
            query_result.reduce(0, |acc, row| {
                let (a, b, c): (u8,u8,u8) = from_row(row);
                acc + a + b + c
            })
        }).and_then(|(output, stmt)| {
            stmt.unwrap().disconnect().map(move|_| output)
        });

        let output = lp.run(fut).unwrap();
        assert_eq!(output, 7);
    }

    #[test]
    fn should_prep_exec_statement() {

        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| {
            conn.prep_exec(r"SELECT :a, :b, :a", params! {
                "a" => 2,
                "b" => 3,
            })
        }).and_then(|query_result| {
            query_result.map(|row| {
                let (a, b, c): (u8, u8, u8) = from_row(row);
                a * b * c
            })
        }).and_then(|(output, stmt)| {
            stmt.unwrap().disconnect().map(move|_| output[0])
        });

        let output = lp.run(fut).unwrap();
        assert_eq!(output, 12u8);
    }

    #[test]
    fn should_first_exec_statement() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| {
            conn.first_exec::<(u8,), _, _>(r"SELECT :a UNION ALL SELECT :b", params! {
                "a" => 2,
                "b" => 3,
            })
        }).and_then(|(row, conn)| {
            conn.disconnect().map(move|_| row.unwrap())
        });

        let output = lp.run(fut).unwrap();
        assert_eq!(output, (2,));
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
                let fut = Conn::new(&**DATABASE_URL, &lp.handle())
                    .and_then(|conn| {
                        conn.ping()
                    });
                lp.run(fut).unwrap();
            })
        }
    }
}
