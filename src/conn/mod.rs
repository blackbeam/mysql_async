use consts;

use lib_futures::stream::Stream as FuturesStream;

use io::Stream;

use FromRow;
use Opts;

pub use self::futures::{
    BinQueryResult,
    Columns,
    Disconnect,
    First,
    MaybeRow,
    NewConn,
    NewTextQueryResult,
    Ping,
    Prepare,
    Query,
    TextQueryResult,
    ReadMaxAllowedPacket,
    ReadPacket,
    Reset,
    ResultSet,
    SendLongData,
    WritePacket,
};

use self::futures::{
    new_columns,
    new_disconnect,
    new_first,
    new_new_conn,
    new_new_text_query_result,
    new_ping,
    new_prepare,
    new_query,
    new_read_max_allowed_packet,
    new_read_packet,
    new_reset,
    new_write_packet,
};

use tokio::reactor::Handle;

pub mod futures;
pub mod named_params;
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
    warnings: u16
}

impl Conn {
    /// Returns future that resolves to `Conn`.
    pub fn new<O>(opts: O, handle: &Handle) -> NewConn
        where O: Into<Opts>,
    {
        let opts = opts.into();

        let future = {
            let addr = opts.get_ip_or_hostname();
            let port = opts.get_tcp_port();
            Stream::connect((addr, port), handle)
        };

        new_new_conn(future, opts)
    }

    fn read_max_allowed_packet(self) -> ReadMaxAllowedPacket {
        new_read_max_allowed_packet(self.first::<(u64,), _>("SELECT @@max_allowed_packet"))
    }

    /// Returns `Conn` on success or `Error` on error.
    pub fn ping(self) -> Ping {
        new_ping(self.write_command_data(consts::Command::COM_PING, &[]))
    }

    /// Return future, that resolves to `TextQueryResult`.
    pub fn query<Q: AsRef<str>>(self, query: Q) -> Query {
        let query = query.as_ref().as_bytes();
        new_query(self.write_command_data(consts::Command::COM_QUERY, query))
    }

    pub fn first<R, Q>(self, query: Q) -> First<R>
    where Q: AsRef<str>,
          R: FromRow + Send + 'static,
    {
        new_first(self.query(query))
    }

    // TODO: Implement cache.
    pub fn prepare<Q>(self, query: Q) -> Prepare
        where Q: AsRef<str>
    {
        new_prepare(self, query.as_ref())
    }

    /// Resets the session state.
    pub fn reset(self) -> Reset {
        new_reset(self.write_command_data(consts::Command::COM_RESET_CONNECTION, &[]))
    }

    /// Disconnect
    pub fn disconnect(self) -> Disconnect {
        new_disconnect(self.write_command_data(consts::Command::COM_QUIT, &[]))
    }

    fn handle_text_resultset(self) -> NewTextQueryResult {
        new_new_text_query_result(self.read_packet(), false)
    }

    fn read_result_set_columns(self, column_count: u64) -> Columns {
        new_columns(self.read_packet(), column_count)
    }

    /// Returns future that resolves to `Conn`.
    fn write_command_data<D>(mut self, cmd: consts::Command, command_data: D) -> WritePacket
    where D: AsRef<[u8]>,
    {
        let mut data = Vec::with_capacity(1 + command_data.as_ref().len());
        data.push(cmd as u8);
        data.extend_from_slice(command_data.as_ref());
        self.seq_id = 0;
        self.write_packet(data)
    }

    /// Returns future that resolves to `Conn`.
    fn write_packet<D>(mut self, data: D) -> WritePacket
    where D: Into<Vec<u8>>,
    {
        let stream = self.stream.take().unwrap();
        let future = stream.write_packet(data.into(), self.seq_id);
        new_write_packet(self, future)
    }

    fn read_packet(mut self) -> ReadPacket {
        let stream = self.stream.take().unwrap();
        let future = stream.into_future();
        new_read_packet(self, future)
    }
}

#[cfg(test)]
mod test {
    use env_logger;
    use errors::*;
    use lib_futures::Future;
    use lib_futures::stream::Stream;

    use super::Conn;
    use super::futures::MaybeRow;

    use test_misc::DATABASE_URL;

    use tokio::reactor::Core;

    use value::from_row;

    #[test]
    fn should_connect() {
        env_logger::init().unwrap();
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
        .and_then(|conn| {
            conn.ping()
        });

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_reset_the_connect() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.reset()
            });

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_disconnect() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle()).and_then(|conn| {
            conn.ping()
        }).and_then(|conn| {
            conn.disconnect()
        });

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
            }).and_then(|query_result| {
                query_result.fold(vec![], |mut accum, row| {
                    if let MaybeRow::Row(row) = row {
                        accum.push(from_row(row))
                    }
                    Ok::<_, Error>(accum)
                })
            });

        let result = lp.run(fut).unwrap();
        assert_eq!((String::from("hello"), 123), result[0]);
        assert_eq!((String::from("world"), 231), result[1]);
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
            }).and_then(|query_result| {
            query_result.collect_all()
        });

        let (sets, _) = lp.run(fut).unwrap();
        assert_eq!(sets.len(), 2);
        for (i, rows) in sets.into_iter().enumerate() {
            if i == 0 {
                assert_eq!((String::from("hello"), 123), from_row((*rows)[0].clone()));
                assert_eq!((String::from("world"), 231), from_row((*rows)[1].clone()));
            }
            if i == 1 {
                assert_eq!((String::from("foo"), 255), from_row((*rows)[0].clone()));
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
            }).and_then(|query_result| {
            query_result.map(|row| from_row::<(String, u8)>(row))
        });

        let (sets, conn) = lp.run(fut).unwrap();
        assert!(conn.is_left());
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
            }).and_then(|query_result| {
            query_result.reduce(0, |mut acc, row| {
                let (val,) = from_row::<(u8,)>(row);
                acc += val;
                acc
            })
        });

        let (result, conn) = lp.run(fut).unwrap();
        assert!(conn.is_left());
        assert_eq!(result, 11);
    }

    #[test]
    fn should_prepare_statement() {
        let mut lp = Core::new().unwrap();

        let fut = Conn::new(&**DATABASE_URL, &lp.handle())
            .and_then(|conn| {
                conn.prepare(r"SELECT ?")
            });

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
                assert_eq!(*result_set, [(42u8,)]);
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
            }).map(|(output, _)| output);

        let output = lp.run(fut).unwrap();
        assert_eq!(output, 10);
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;
        use conn::Conn;
        use tokio::reactor::Core;
        use lib_futures::Future;

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
