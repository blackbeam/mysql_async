// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

pub use mysql_common::named_params;

use futures::future::{err, loop_fn, ok, Either::*, Future, IntoFuture, Loop};
use mysql_common::{
    crypto,
    packets::{
        parse_auth_switch_request, parse_handshake_packet, AuthPlugin, AuthSwitchRequest,
        HandshakeResponse, SslRequest,
    },
};

use std::{fmt, mem, str::FromStr, sync::Arc};

use crate::{
    conn::{pool::Pool, stmt_cache::StmtCache},
    connection_like::{streamless::Streamless, ConnectionLike, StmtCacheResult},
    consts::{self, CapabilityFlags},
    error::*,
    io::Stream,
    local_infile_handler::LocalInfileHandler,
    opts::Opts,
    queryable::{query_result, BinaryProtocol, Queryable, TextProtocol},
    time::SteadyTime,
    BoxFuture, Column, MyFuture, OptsBuilder,
};

pub mod pool;
pub mod stmt_cache;

/// Helper function that asynchronously disconnects connection on the default tokio executor.
fn disconnect<E: crate::MyExecutor>(mut conn: Conn<E>) {
    use tokio::executor::{DefaultExecutor, Executor};
    let mut executor = DefaultExecutor::current();

    let disconnected = conn.inner.disconnected;

    // Mark conn as disconnected.
    conn.inner.disconnected = true;

    if !disconnected {
        // Server will report broken connection if spawn fails.
        let _ = executor.spawn(Box::new(
            conn.cleanup().and_then(Conn::<E>::disconnect).map_err(drop),
        ));
    }
}

/// Mysql connection
struct ConnInner<T: crate::MyExecutor> {
    stream: Option<Stream>,
    id: u32,
    version: (u16, u16, u16),
    seq_id: u8,
    last_command: consts::Command,
    max_allowed_packet: u64,
    socket: Option<String>,
    capabilities: consts::CapabilityFlags,
    status: consts::StatusFlags,
    last_insert_id: u64,
    affected_rows: u64,
    warnings: u16,
    pool: Option<Pool<T>>,
    has_result: Option<(Arc<Vec<Column>>, Option<StmtCacheResult>)>,
    in_transaction: bool,
    opts: Opts,
    last_io: SteadyTime,
    wait_timeout: u32,
    stmt_cache: StmtCache,
    nonce: Vec<u8>,
    auth_plugin: AuthPlugin<'static>,
    auth_switched: bool,
    /// Connection is already disconnected.
    disconnected: bool,
}

impl<T: crate::MyExecutor> fmt::Debug for ConnInner<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl<T: crate::MyExecutor> ConnInner<T> {
    /// Constructs an empty connection.
    fn empty(opts: Opts) -> ConnInner<T> {
        ConnInner {
            last_command: consts::Command::COM_PING,
            capabilities: opts.get_capabilities(),
            status: consts::StatusFlags::empty(),
            last_insert_id: 0,
            affected_rows: 0,
            stream: None,
            seq_id: 0,
            max_allowed_packet: 1024 * 1024,
            warnings: 0,
            version: (0, 0, 0),
            id: 0,
            has_result: None,
            pool: None,
            in_transaction: false,
            last_io: SteadyTime::now(),
            wait_timeout: 0,
            stmt_cache: StmtCache::new(opts.get_stmt_cache_size()),
            socket: opts.get_socket().map(Into::into),
            opts,
            nonce: Vec::default(),
            auth_plugin: AuthPlugin::MysqlNativePassword,
            auth_switched: false,
            disconnected: false,
        }
    }
}

pub struct Conn<T: crate::MyExecutor = ::tokio::executor::DefaultExecutor> {
    executor: T,
    inner: Box<ConnInner<T>>,
}

impl<T: crate::MyExecutor> fmt::Debug for Conn<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl Conn {
    /// Returns future that resolves to `Conn`.
    pub fn new<T: Into<Opts>>(opts: T) -> impl MyFuture<Self> {
        let executor = ::tokio::executor::DefaultExecutor::current();
        Conn::with_executor(executor, opts)
    }

    /// Returns future that resolves to `Conn`.
    pub fn from_url<T: AsRef<str>>(url: T) -> impl MyFuture<Self> {
        Opts::from_str(url.as_ref())
            .map_err(Error::from)
            .into_future()
            .and_then(Conn::new)
    }
}

impl<E: crate::MyExecutor> Conn<E> {
    /// Returns future that resolves to `Conn`.
    pub fn from_url_with_executor<T: AsRef<str>>(executor: E, url: T) -> impl MyFuture<Self> {
        Opts::from_str(url.as_ref())
            .map_err(Error::from)
            .into_future()
            .and_then(|opts| Conn::with_executor(executor, opts))
    }

    /// Returns future that resolves to `Conn`.
    pub fn with_executor<T: Into<Opts>>(executor: E, opts: T) -> impl MyFuture<Self> {
        let opts = opts.into();
        let mut conn = Conn::empty(executor, opts.clone());

        let stream = if let Some(path) = opts.get_socket() {
            A(Stream::connect_socket(path.to_owned()))
        } else {
            B(Stream::connect_tcp((
                opts.get_ip_or_hostname(),
                opts.get_tcp_port(),
            )))
        };

        stream
            .map(move |stream| {
                conn.inner.stream = Some(stream);
                conn
            })
            .and_then(Conn::setup_stream)
            .and_then(Conn::handle_handshake)
            .and_then(Conn::switch_to_ssl_if_needed)
            .and_then(Conn::do_handshake_response)
            .and_then(Conn::continue_auth)
            .and_then(Conn::read_socket)
            .and_then(Conn::reconnect_via_socket_if_needed)
            .and_then(Conn::read_max_allowed_packet)
            .and_then(Conn::read_wait_timeout)
            .and_then(Conn::run_init_commands)
    }

    /// Hacky way to move connection through &mut. `self` becomes unusable.
    fn take(&mut self) -> Self {
        let executor = self.executor.clone();
        let inner = mem::replace(&mut *self.inner, ConnInner::<E>::empty(Default::default()));
        Conn {
            executor,
            inner: Box::new(inner),
        }
    }

    /// Returns true if time since last io exceeds wait_timeout (or conn_ttl if specified in opts).
    fn expired(&self) -> bool {
        let idle_duration = SteadyTime::now() - self.inner.last_io;
        let ttl = self
            .inner
            .opts
            .get_conn_ttl()
            .unwrap_or(self.inner.wait_timeout);
        idle_duration.num_milliseconds() > i64::from(ttl) * 1000
    }

    fn is_secure(&self) -> bool {
        if let Some(ref stream) = self.inner.stream {
            stream.is_secure()
        } else {
            false
        }
    }

    fn empty(executor: E, opts: Opts) -> Self {
        Self {
            executor,
            inner: Box::new(ConnInner::empty(opts)),
        }
    }

    fn setup_stream(mut self) -> Result<Self> {
        if let Some(stream) = self.inner.stream.take() {
            stream.set_keepalive_ms(self.inner.opts.get_tcp_keepalive())?;
            stream.set_tcp_nodelay(self.inner.opts.get_tcp_nodelay())?;
            self.inner.stream = Some(stream);
            Ok(self)
        } else {
            unreachable!();
        }
    }

    /// Returns the ID generated by a query (usually `INSERT`) on a table with a column having the
    /// `AUTO_INCREMENT` attribute. Returns `None` if there was no previous query on the connection
    /// or if the query did not update an AUTO_INCREMENT value.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.get_last_insert_id()
    }

    /// Returns the number of rows affected by the last `INSERT`, `UPDATE`, `REPLACE` or `DELETE`
    /// query.
    pub fn affected_rows(&self) -> u64 {
        self.get_affected_rows()
    }

    fn close(mut self) -> impl MyFuture<()> {
        self.inner.disconnected = true;
        self.cleanup().and_then(Conn::disconnect)
    }

    fn handle_handshake(self) -> impl MyFuture<Self> {
        self.read_packet().and_then(move |(mut conn, packet)| {
            parse_handshake_packet(&*packet.0)
                .map_err(Error::from)
                .and_then(|handshake| {
                    conn.inner.nonce = {
                        let mut nonce = Vec::from(handshake.scramble_1_ref());
                        nonce.extend_from_slice(handshake.scramble_2_ref().unwrap_or(&[][..]));
                        nonce
                    };

                    conn.inner.capabilities =
                        handshake.capabilities() & conn.inner.opts.get_capabilities();
                    conn.inner.version = handshake.server_version_parsed().unwrap_or((0, 0, 0));
                    conn.inner.id = handshake.connection_id();
                    conn.inner.status = handshake.status_flags();
                    conn.inner.auth_plugin = match handshake.auth_plugin() {
                        Some(AuthPlugin::MysqlNativePassword) => AuthPlugin::MysqlNativePassword,
                        Some(AuthPlugin::CachingSha2Password) => AuthPlugin::CachingSha2Password,
                        Some(AuthPlugin::Other(ref name)) => {
                            let name = String::from_utf8_lossy(name).into();
                            return Err(DriverError::UnknownAuthPlugin { name }.into());
                        }
                        None => AuthPlugin::MysqlNativePassword,
                    };
                    Ok(conn)
                })
        })
    }

    fn switch_to_ssl_if_needed(self) -> impl MyFuture<Self> {
        if self
            .inner
            .opts
            .get_capabilities()
            .contains(CapabilityFlags::CLIENT_SSL)
        {
            let ssl_request = SslRequest::new(self.inner.capabilities);
            let fut = self.write_packet(ssl_request.as_ref()).and_then(|conn| {
                let ssl_opts = conn
                    .get_opts()
                    .get_ssl_opts()
                    .cloned()
                    .expect("unreachable");
                let domain = conn.get_opts().get_ip_or_hostname().into();
                let (streamless, stream) = conn.take_stream();
                stream
                    .make_secure(domain, ssl_opts)
                    .map(move |stream| streamless.return_stream(stream))
            });
            A(fut)
        } else {
            B(ok(self))
        }
    }

    fn do_handshake_response(self) -> impl MyFuture<Self> {
        let auth_data = self
            .inner
            .auth_plugin
            .gen_data(self.inner.opts.get_pass(), &*self.inner.nonce);

        let handshake_response = HandshakeResponse::new(
            &auth_data,
            self.inner.version,
            self.inner.opts.get_user(),
            self.inner.opts.get_db_name(),
            self.inner.auth_plugin.clone(),
            self.get_capabilities(),
        );

        self.write_packet(handshake_response.as_ref())
    }

    fn perform_auth_switch(
        mut self,
        auth_switch_request: AuthSwitchRequest<'_>,
    ) -> BoxFuture<Self> {
        if !self.inner.auth_switched {
            self.inner.auth_switched = true;
            self.inner.nonce = auth_switch_request.plugin_data().into();
            self.inner.auth_plugin = auth_switch_request.auth_plugin().clone().into_owned();
            let plugin_data = self
                .inner
                .auth_plugin
                .gen_data(self.inner.opts.get_pass(), &*self.inner.nonce)
                .unwrap_or_else(Vec::new);
            let fut = self.write_packet(plugin_data).and_then(Conn::continue_auth);
            // We'll box it to avoid recursion.
            Box::new(fut)
        } else {
            unreachable!("auth_switched flag should be checked by caller")
        }
    }

    fn continue_auth(self) -> impl MyFuture<Self> {
        match self.inner.auth_plugin {
            AuthPlugin::MysqlNativePassword => A(self.continue_mysql_native_password_auth()),
            AuthPlugin::CachingSha2Password => B(self.continue_caching_sha2_password_auth()),
            _ => unreachable!(),
        }
    }

    fn continue_caching_sha2_password_auth(self) -> impl MyFuture<Self> {
        self.read_packet()
            .and_then(|(conn, packet)| match packet.as_ref().get(0) {
                Some(0x01) => match packet.as_ref().get(1) {
                    Some(0x03) => {
                        // auth ok
                        A(conn.drop_packet())
                    }
                    Some(0x04) => {
                        let mut pass = conn
                            .inner
                            .opts
                            .get_pass()
                            .map(Vec::from)
                            .unwrap_or_default();
                        pass.push(0);
                        let fut = if conn.is_secure() {
                            A(conn.write_packet(&*pass))
                        } else {
                            B(conn
                                .write_packet(&[0x02][..])
                                .and_then(Conn::read_packet)
                                .and_then(move |(conn, packet)| {
                                    let key = &packet.as_ref()[1..];
                                    for (i, byte) in pass.iter_mut().enumerate() {
                                        *byte ^= conn.inner.nonce[i % conn.inner.nonce.len()];
                                    }
                                    let encrypted_pass = crypto::encrypt(&*pass, key);
                                    conn.write_packet(&*encrypted_pass)
                                }))
                        };
                        B(A(fut.and_then(Conn::drop_packet)))
                    }
                    _ => B(B(A(err(DriverError::UnexpectedPacket {
                        payload: packet.as_ref().into(),
                    }
                    .into())))),
                },
                Some(0xfe) if !conn.inner.auth_switched => {
                    let fut = parse_auth_switch_request(packet.as_ref())
                        .map(AuthSwitchRequest::into_owned)
                        .map_err(Error::from)
                        .into_future()
                        .and_then(|auth_switch_request| {
                            conn.perform_auth_switch(auth_switch_request)
                        });
                    B(B(B(A(fut))))
                }
                _ => B(B(B(B(err(DriverError::UnexpectedPacket {
                    payload: packet.as_ref().into(),
                }
                .into()))))),
            })
    }

    fn continue_mysql_native_password_auth(self) -> impl MyFuture<Self> {
        self.read_packet()
            .and_then(|(this, packet)| match packet.0.get(0) {
                Some(0x00) => A(ok(this)),
                Some(0xfe) if !this.inner.auth_switched => {
                    let fut = parse_auth_switch_request(packet.as_ref())
                        .map(AuthSwitchRequest::into_owned)
                        .map_err(Error::from)
                        .into_future()
                        .and_then(|auth_switch_request| {
                            this.perform_auth_switch(auth_switch_request)
                        });
                    B(A(fut))
                }
                _ => B(B(err(
                    DriverError::UnexpectedPacket { payload: packet.0 }.into()
                ))),
            })
    }

    fn drop_packet(self) -> impl MyFuture<Self> {
        self.read_packet().map(|(conn, _)| conn)
    }

    fn run_init_commands(self) -> impl MyFuture<Self> {
        let init = self
            .inner
            .opts
            .get_init()
            .iter()
            .map(Clone::clone)
            .collect();

        loop_fn(
            (init, self),
            |(mut init, conn): (Vec<String>, Conn<E>)| match init.pop() {
                None => A(ok(Loop::Break(conn))),
                Some(query) => {
                    let fut = conn
                        .drop_query(query)
                        .map(|conn| Loop::Continue((init, conn)));
                    B(fut)
                }
            },
        )
    }

    /// Will try to connect via socket using socket address in `self.inner.socket`.
    ///
    /// Returns new connection on success or self on error.
    ///
    /// Won't try to reconnect if socket connection is already enforced in `Opts`.
    fn reconnect_via_socket_if_needed(self) -> Box<MyFuture<Self>> {
        if let Some(socket) = self.inner.socket.as_ref() {
            let opts = self.inner.opts.clone();
            if opts.get_socket().is_none() {
                let mut builder = OptsBuilder::from_opts(opts);
                builder.socket(Some(&**socket));
                let fut =
                    Conn::with_executor(self.executor.clone(), builder).then(
                        |result| match result {
                            Ok(conn) => Ok(conn),
                            Err(_) => Ok(self),
                        },
                    );
                return Box::new(fut);
            }
        }
        Box::new(ok(self))
    }

    /// Returns future that resolves to `Conn` with socket address stored in it.
    ///
    /// Do nothing if socket address is already in `Opts` or if `prefer_socket` is `false`.
    fn read_socket(self) -> impl MyFuture<Self> {
        if self.inner.opts.get_prefer_socket() && self.inner.socket.is_none() {
            A(self.first("SELECT @@socket").map(|(mut this, row_opt)| {
                this.inner.socket = row_opt.unwrap_or((None,)).0;
                this
            }))
        } else {
            B(ok(self))
        }
    }

    /// Returns future that resolves to `Conn` with `max_allowed_packet` stored in it.
    fn read_max_allowed_packet(self) -> impl MyFuture<Self> {
        self.first("SELECT @@max_allowed_packet")
            .map(|(mut this, row_opt)| {
                this.inner.max_allowed_packet = row_opt.unwrap_or((1024 * 1024 * 2,)).0;
                this
            })
    }

    /// Returns future that resolves to `Conn` with `wait_timeout` stored in it.
    fn read_wait_timeout(self) -> impl MyFuture<Self> {
        self.first("SELECT @@wait_timeout")
            .map(|(mut this, row_opt)| {
                this.inner.wait_timeout = row_opt.unwrap_or((28800,)).0;
                this
            })
    }

    /// Returns future that resolves to a `Conn` with `COM_RESET_CONNECTION` executed on it.
    pub fn reset(self) -> impl MyFuture<Self> {
        let pool = self.inner.pool.clone();
        let fut = if self.inner.version > (5, 7, 2) {
            let fut = self
                .write_command_data(consts::Command::COM_RESET_CONNECTION, &[])
                .and_then(|conn| conn.read_packet())
                .map(|(conn, _)| conn);
            (ok(pool), A(fut))
        } else {
            (
                ok(pool),
                B(Conn::with_executor(
                    self.executor.clone(),
                    self.inner.opts.clone(),
                )),
            )
        };
        fut.into_future().map(|(pool, mut conn)| {
            conn.inner.stmt_cache.clear();
            conn.inner.pool = pool;
            conn
        })
    }

    fn rollback_transaction(mut self) -> impl MyFuture<Self> {
        assert!(self.inner.in_transaction);
        self.inner.in_transaction = false;
        self.drop_query("ROLLBACK")
    }

    fn drop_result(mut self) -> impl MyFuture<Self> {
        match self.inner.has_result.take() {
            Some((columns, None)) => A(B(query_result::assemble::<_, TextProtocol>(
                self,
                Some(columns),
                None,
            )
            .drop_result())),
            Some((columns, cached)) => A(A(query_result::assemble::<_, BinaryProtocol>(
                self,
                Some(columns),
                cached,
            )
            .drop_result())),
            None => B(ok(self)),
        }
    }

    fn cleanup(self) -> BoxFuture<Self> {
        if self.inner.has_result.is_some() {
            Box::new(self.drop_result().and_then(Self::cleanup))
        } else if self.inner.in_transaction {
            Box::new(self.rollback_transaction().and_then(Self::cleanup))
        } else {
            Box::new(ok(self))
        }
    }
}

impl<E: crate::MyExecutor> ConnectionLike for Conn<E> {
    fn take_stream(mut self) -> (Streamless<Self>, Stream) {
        let stream = self.inner.stream.take().expect("Logic error: stream taken");
        (Streamless::new(self), stream)
    }

    fn return_stream(&mut self, stream: Stream) {
        self.inner.stream = Some(stream);
    }

    fn stmt_cache_ref(&self) -> &StmtCache {
        &self.inner.stmt_cache
    }

    fn stmt_cache_mut(&mut self) -> &mut StmtCache {
        &mut self.inner.stmt_cache
    }

    fn get_affected_rows(&self) -> u64 {
        self.inner.affected_rows
    }

    fn get_capabilities(&self) -> consts::CapabilityFlags {
        self.inner.capabilities
    }

    fn get_in_transaction(&self) -> bool {
        self.inner.in_transaction
    }

    fn get_last_insert_id(&self) -> Option<u64> {
        match self.inner.last_insert_id {
            0 => None,
            x => Some(x),
        }
    }

    fn get_last_command(&self) -> consts::Command {
        self.inner.last_command
    }

    fn get_local_infile_handler(&self) -> Option<Arc<dyn LocalInfileHandler>> {
        self.inner.opts.get_local_infile_handler()
    }

    fn get_max_allowed_packet(&self) -> u64 {
        self.inner.max_allowed_packet
    }

    fn get_opts(&self) -> &Opts {
        &self.inner.opts
    }

    fn get_pending_result(&self) -> Option<&(Arc<Vec<Column>>, Option<StmtCacheResult>)> {
        self.inner.has_result.as_ref()
    }

    fn get_seq_id(&self) -> u8 {
        self.inner.seq_id
    }

    fn get_server_version(&self) -> (u16, u16, u16) {
        self.inner.version
    }

    fn get_status(&self) -> consts::StatusFlags {
        self.inner.status
    }

    fn set_affected_rows(&mut self, affected_rows: u64) {
        self.inner.affected_rows = affected_rows;
    }

    fn set_in_transaction(&mut self, in_transaction: bool) {
        self.inner.in_transaction = in_transaction;
    }

    fn set_last_command(&mut self, last_command: consts::Command) {
        self.inner.last_command = last_command;
    }

    fn set_last_insert_id(&mut self, last_insert_id: u64) {
        self.inner.last_insert_id = last_insert_id;
    }

    fn set_pending_result(&mut self, meta: Option<(Arc<Vec<Column>>, Option<StmtCacheResult>)>) {
        self.inner.has_result = meta;
    }

    fn set_status(&mut self, status: consts::StatusFlags) {
        self.inner.status = status;
    }

    fn set_warnings(&mut self, warnings: u16) {
        self.inner.warnings = warnings;
    }

    fn set_seq_id(&mut self, seq_id: u8) {
        self.inner.seq_id = seq_id;
    }

    fn touch(&mut self) {
        self.inner.last_io = SteadyTime::now();
    }

    fn on_disconnect(&mut self) {
        self.inner.pool = None;
    }
}

#[cfg(test)]
mod test {
    use futures::Future;

    #[cfg(feature = "ssl")]
    use crate::SslOpts;
    use crate::{
        from_row, params, prelude::*, test_misc::DATABASE_URL, Conn, OptsBuilder,
        TransactionOptions, WhiteListFsLocalInfileHandler,
    };

    /// Same as `tokio::run`, but will panic if future panics and will return the result
    /// of future execution.
    fn run<F, T, U>(future: F) -> Result<T, U>
    where
        F: Future<Item = T, Error = U> + Send + 'static,
        T: Send + 'static,
        U: Send + 'static,
    {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(future);
        runtime.shutdown_on_idle().wait().unwrap();
        result
    }

    fn get_opts() -> OptsBuilder {
        let mut builder = OptsBuilder::from_opts(&**DATABASE_URL);
        // to suppress warning on unused mut
        builder.stmt_cache_size(None);
        #[cfg(feature = "ssl")]
        {
            let mut ssl_opts = SslOpts::default();
            ssl_opts.set_danger_skip_domain_validation(true);
            ssl_opts.set_danger_accept_invalid_certs(true);
            builder.ssl_opts(ssl_opts);
        }
        builder
    }

    #[test]
    fn opts_should_satisfy_send_and_sync() {
        struct A<T: Sync + Send>(T);
        A(get_opts());
    }

    #[test]
    fn should_connect() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| Queryable::ping(conn))
            .and_then(|conn| Queryable::disconnect(conn));

        run(fut).unwrap();
    }

    #[test]
    fn should_not_panic_if_dropped_without_tokio_runtime() {
        let fut = Conn::new(get_opts());
        run(fut).unwrap();
        // connection will drop here
    }

    #[test]
    fn should_execute_init_queries_on_new_connection() {
        let mut opts_builder = OptsBuilder::from_opts(get_opts());
        opts_builder.init(vec!["SET @a = 42", "SET @b = 'foo'"]);
        let fut = Conn::new(opts_builder)
            .and_then(|conn| Queryable::query(conn, "SELECT @a, @b"))
            .and_then(|result| result.collect_and_drop::<(u8, String)>())
            .and_then(|(conn, rows)| Queryable::disconnect(conn).map(|_| rows))
            .map(|result| assert_eq!(result, vec![(42, "foo".into())]));

        run(fut).unwrap();
    }

    #[test]
    fn should_reset_the_connection() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| conn.drop_exec("SELECT ?", (1,)))
            .and_then(|conn| conn.reset())
            .and_then(|conn| conn.drop_exec("SELECT ?", (1,)))
            .and_then(|conn| conn.disconnect());

        run(fut).unwrap();
    }

    #[test]
    fn should_not_cache_statements_if_stmt_cache_size_is_zero() {
        let mut opts = OptsBuilder::from_opts(get_opts());
        opts.stmt_cache_size(0);
        let fut = Conn::new(opts)
            .and_then(|conn| conn.drop_exec("DO ?", (1,)))
            .and_then(|conn| {
                conn.prepare("DO 2").and_then(|stmt| {
                    stmt.first::<_, (crate::Value,)>(())
                        .and_then(|(stmt, _)| stmt.first::<_, (crate::Value,)>(()))
                        .and_then(|(stmt, _)| stmt.close())
                })
            })
            .and_then(|conn| {
                conn.prep_exec("DO 3", ())
                    .and_then(|result| result.drop_result())
            })
            .and_then(|conn| conn.batch_exec("DO 4", vec![(), ()]))
            .and_then(|conn| conn.first_exec::<_, _, (u8,)>("DO 5", ()))
            .and_then(|(conn, _)| conn.first("SHOW SESSION STATUS LIKE 'Com_stmt_close';"))
            .and_then(|(conn, row)| {
                assert_eq!(from_row::<(String, usize)>(row.unwrap()).1, 5);
                conn.disconnect()
            });

        run(fut).unwrap();
    }

    #[test]
    fn should_hold_stmt_cache_size_bound() {
        use crate::connection_like::ConnectionLike;

        let mut opts = OptsBuilder::from_opts(get_opts());
        opts.stmt_cache_size(3);
        let fut = Conn::new(opts)
            .and_then(|conn| conn.drop_exec("DO 1", ()))
            .and_then(|conn| conn.drop_exec("DO 2", ()))
            .and_then(|conn| conn.drop_exec("DO 3", ()))
            .and_then(|conn| conn.drop_exec("DO 1", ()))
            .and_then(|conn| conn.drop_exec("DO 4", ()))
            .and_then(|conn| conn.drop_exec("DO 3", ()))
            .and_then(|conn| conn.drop_exec("DO 5", ()))
            .and_then(|conn| conn.drop_exec("DO 6", ()))
            .and_then(|conn| conn.first("SHOW SESSION STATUS LIKE 'Com_stmt_close';"))
            .and_then(|(conn, row_opt)| {
                let (_, count): (String, usize) = row_opt.unwrap();
                assert_eq!(count, 3);
                let order = conn
                    .stmt_cache_ref()
                    .iter()
                    .map(Clone::clone)
                    .collect::<Vec<String>>();
                assert_eq!(order, &["DO 3", "DO 5", "DO 6"]);
                conn.disconnect()
            });

        run(fut).unwrap();
    }

    #[test]
    fn should_perform_queries() {
        let long_string = ::std::iter::repeat('A')
            .take(18 * 1024 * 1024)
            .collect::<String>();
        let long_string_clone = long_string.clone();
        let fut = Conn::new(get_opts())
            .and_then(move |conn| {
                Queryable::query(conn, format!(r"SELECT '{}', 231", long_string_clone))
            })
            .and_then(move |result| {
                result.reduce_and_drop(vec![], move |mut acc, row| {
                    acc.push(from_row(row));
                    acc
                })
            })
            .and_then(move |(conn, out)| Queryable::disconnect(conn).map(|_| out))
            .map(move |result| {
                assert_eq!((long_string, 231), result[0]);
            });

        run(fut).unwrap();
    }

    #[test]
    fn should_drop_query() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                conn.drop_query("CREATE TEMPORARY TABLE tmp (id int DEFAULT 10, name text)")
            })
            .and_then(|conn| Queryable::drop_query(conn, "INSERT INTO tmp VALUES (1, 'foo')"))
            .and_then(|conn| Queryable::first::<_, (u8,)>(conn, "SELECT COUNT(*) FROM tmp"))
            .and_then(|(conn, row)| conn.disconnect().map(move |_| row))
            .map(|result| assert_eq!(result, Some((1,))));

        run(fut).unwrap();
    }

    #[test]
    fn should_try_collect() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                Queryable::query(
                    conn,
                    r"SELECT 'hello', 123
                    UNION ALL
                    SELECT 'world', 'bar'
                    UNION ALL
                    SELECT 'hello', 123
                ",
                )
            })
            .and_then(|result| result.try_collect::<(String, u8)>())
            .and_then(|(result, mut rows)| {
                assert!(rows.pop().unwrap().is_ok());
                assert!(rows.pop().unwrap().is_err());
                assert!(rows.pop().unwrap().is_ok());
                result.drop_result()
            })
            .and_then(Conn::disconnect);

        run(fut).unwrap()
    }

    #[test]
    fn should_try_collect_and_drop() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                Queryable::query(
                    conn,
                    r"SELECT 'hello', 123
                    UNION ALL
                    SELECT 'world', 'bar'
                    UNION ALL
                    SELECT 'hello', 123;
                    SELECT 'foo', 255;
                ",
                )
            })
            .and_then(|result| result.try_collect_and_drop::<(String, u8)>())
            .and_then(|(conn, mut rows)| {
                assert!(rows.pop().unwrap().is_ok());
                assert!(rows.pop().unwrap().is_err());
                assert!(rows.pop().unwrap().is_ok());
                conn.disconnect()
            });

        run(fut).unwrap()
    }

    #[test]
    fn should_handle_mutliresult_set() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                Queryable::query(
                    conn,
                    r"SELECT 'hello', 123
                    UNION ALL
                    SELECT 'world', 231;
                    SELECT 'foo', 255;
                ",
                )
            })
            .and_then(|result| result.collect::<(String, u8)>())
            .and_then(|(result, rows_1)| (result.collect_and_drop(), Ok(rows_1)))
            .and_then(|((conn, rows_2), rows_1)| {
                Queryable::disconnect(conn).map(|_| vec![rows_1, rows_2])
            })
            .map(|rows_vec| {
                assert_eq!(rows_vec.len(), 2);
                for (i, rows) in rows_vec.into_iter().enumerate() {
                    if i == 0 {
                        assert_eq!((String::from("hello"), 123), rows[0]);
                        assert_eq!((String::from("world"), 231), rows[1]);
                    }
                    if i == 1 {
                        assert_eq!((String::from("foo"), 255), rows[0]);
                    }
                }
            });

        run(fut).unwrap();
    }

    #[test]
    fn should_map_resultset() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                Queryable::query(
                    conn,
                    r"
                    SELECT 'hello', 123
                    UNION ALL
                    SELECT 'world', 231;
                    SELECT 'foo', 255;
                ",
                )
            })
            .and_then(|result| result.map(|row| from_row::<(String, u8)>(row)))
            .and_then(|(result, rows_1)| (result.map_and_drop(from_row), Ok(rows_1)))
            .and_then(|((conn, rows_2), rows_1)| {
                Queryable::disconnect(conn).map(|_| vec![rows_1, rows_2])
            })
            .map(|rows_vec| {
                assert_eq!(rows_vec.len(), 2);
                for (i, rows) in rows_vec.into_iter().enumerate() {
                    if i == 0 {
                        assert_eq!((String::from("hello"), 123), rows[0]);
                        assert_eq!((String::from("world"), 231), rows[1]);
                    }
                    if i == 1 {
                        assert_eq!((String::from("foo"), 255), rows[0]);
                    }
                }
            });

        run(fut).unwrap();
    }

    #[test]
    fn should_reduce_resultset() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                Queryable::query(
                    conn,
                    r"SELECT 5
                    UNION ALL
                    SELECT 6;
                    SELECT 7;",
                )
            })
            .and_then(|result| {
                result.reduce(0, |mut acc, row| {
                    acc += from_row::<i32>(row);
                    acc
                })
            })
            .and_then(|(result, reduced)| (result.collect_and_drop(), Ok(reduced)))
            .and_then(|((conn, rows_2), reduced)| {
                Queryable::disconnect(conn).map(move |_| vec![vec![reduced], rows_2])
            })
            .map(|rows_vec| {
                assert_eq!(rows_vec.len(), 2);
                for (i, rows) in rows_vec.into_iter().enumerate() {
                    if i == 0 {
                        assert_eq!(11, rows[0]);
                    }
                    if i == 1 {
                        assert_eq!(7, rows[0]);
                    }
                }
            });

        run(fut).unwrap();
    }

    #[test]
    fn should_handle_multi_result_sets_where_some_results_have_no_output() {
        const QUERY: &str = r"SELECT 1;
            UPDATE time_zone SET Time_zone_id = 1 WHERE Time_zone_id = 1;
            SELECT 2;
            SELECT 3;
            UPDATE time_zone SET Time_zone_id = 1 WHERE Time_zone_id = 1;
            UPDATE time_zone SET Time_zone_id = 1 WHERE Time_zone_id = 1;
            SELECT 4;";

        let fut = Conn::new(get_opts())
            .and_then(|c| {
                c.start_transaction(TransactionOptions::new())
                    .and_then(|t| t.drop_query(QUERY))
                    .and_then(|t| t.query(QUERY).and_then(|r| r.collect_and_drop::<u8>()))
                    .and_then(|(t, out)| {
                        assert_eq!(vec![1], out);
                        t.query(QUERY)
                            .and_then(|r| r.for_each_and_drop(|x| assert_eq!(from_row::<u8>(x), 1)))
                    })
                    .and_then(|t| {
                        t.query(QUERY)
                            .and_then(|r| r.map_and_drop(|row| from_row::<u8>(row)))
                    })
                    .and_then(|(t, out)| {
                        assert_eq!(vec![1], out);
                        t.query(QUERY)
                            .and_then(|r| r.reduce_and_drop(0u8, |acc, x| acc + from_row::<u8>(x)))
                    })
                    .and_then(|(t, out)| {
                        assert_eq!(1, out);
                        t.query(QUERY).and_then(|r| r.drop_result())
                    })
                    .and_then(|t| t.commit())
            })
            .and_then(|c| c.first_exec::<_, _, u8>("SELECT 1", ()))
            .and_then(|(c, output)| c.disconnect().map(move |_| output))
            .map(|result| assert_eq!(result, Some(1)));

        run(fut).unwrap();
    }

    #[test]
    fn should_iterate_over_resultset() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let acc = Arc::new(AtomicUsize::new(0));

        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                Queryable::query(
                    conn,
                    r"SELECT 2
                    UNION ALL
                    SELECT 3;
                    SELECT 5;",
                )
            })
            .and_then({
                let acc = acc.clone();
                move |result| {
                    result.for_each({
                        let acc = acc.clone();
                        move |row| {
                            acc.fetch_add(from_row::<usize>(row), Ordering::SeqCst);
                        }
                    })
                }
            })
            .and_then({
                let acc = acc.clone();
                move |result| {
                    result.for_each_and_drop({
                        let acc = acc.clone();
                        move |row| {
                            acc.fetch_add(from_row::<usize>(row), Ordering::SeqCst);
                        }
                    })
                }
            })
            .and_then(Queryable::disconnect)
            .map(move |_| assert_eq!(acc.load(Ordering::SeqCst), 10));

        run(fut).unwrap();
    }

    #[test]
    fn should_prepare_statement() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| Queryable::prepare(conn, r"SELECT ?"))
            .and_then(|stmt| stmt.close())
            .and_then(|conn| conn.disconnect());

        run(fut).unwrap();

        let fut = Conn::new(get_opts())
            .and_then(|conn| Queryable::prepare(conn, r"SELECT :foo"))
            .and_then(|stmt| stmt.close())
            .and_then(|conn| conn.disconnect());

        run(fut).unwrap();
    }

    #[test]
    fn should_execute_statement() {
        let long_string = ::std::iter::repeat('A')
            .take(18 * 1024 * 1024)
            .collect::<String>();
        let fut = Conn::new(get_opts())
            .and_then(|conn| Queryable::prepare(conn, r"SELECT ?"))
            .and_then({
                let long_string = long_string.clone();
                move |stmt| stmt.execute((long_string,))
            })
            .and_then(|result| result.map_and_drop(|row| from_row::<(String,)>(row)))
            .and_then(|(stmt, mut mapped)| {
                assert_eq!(mapped.len(), 1);
                assert_eq!(mapped.pop(), Some((long_string,)));
                stmt.execute((42,))
            })
            .and_then(|result| result.collect_and_drop::<(u8,)>())
            .and_then(|(stmt, collected)| {
                assert_eq!(collected, vec![(42u8,)]);
                stmt.execute((8,))
            })
            .and_then(|result| {
                result.reduce_and_drop(2, |mut acc, row| {
                    acc += from_row::<i32>(row);
                    acc
                })
            })
            .and_then(|(stmt, reduced)| {
                stmt.close()
                    .and_then(|conn| conn.disconnect())
                    .map(move |_| reduced)
            })
            .map(|output| assert_eq!(output, 10));

        run(fut).unwrap();

        let fut = Conn::new(get_opts())
            .and_then(|conn| Queryable::prepare(conn, r"SELECT :foo, :bar, :foo, 3"))
            .and_then(|stmt| stmt.execute(params! { "foo" => "quux", "bar" => "baz" }))
            .and_then(|result| {
                result.map_and_drop(|row| from_row::<(String, String, String, u8)>(row))
            })
            .and_then(|(stmt, mut mapped)| {
                assert_eq!(mapped.len(), 1);
                assert_eq!(
                    mapped.pop(),
                    Some(("quux".into(), "baz".into(), "quux".into(), 3))
                );
                stmt.execute(params! { "foo" => 2, "bar" => 3 })
            })
            .and_then(|result| result.collect_and_drop::<(u8, u8, u8, u8)>())
            .and_then(|(stmt, collected)| {
                assert_eq!(collected, vec![(2, 3, 2, 3)]);
                stmt.execute(params! { "foo" => 2, "bar" => 3 })
            })
            .and_then(|result| {
                result.reduce_and_drop(0, |acc, row| {
                    let (a, b, c, d): (u8, u8, u8, u8) = from_row(row);
                    acc + a + b + c + d
                })
            })
            .and_then(|(stmt, reduced)| {
                stmt.close()
                    .and_then(|conn| conn.disconnect())
                    .map(move |_| reduced)
            })
            .map(|output| assert_eq!(output, 10));

        run(fut).unwrap();
    }

    #[test]
    fn should_prep_exec_statement() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                Queryable::prep_exec(conn, r"SELECT :a, :b, :a", params! { "a" => 2, "b" => 3 })
            })
            .and_then(|result| {
                result.map_and_drop(|row| {
                    let (a, b, c): (u8, u8, u8) = from_row(row);
                    a * b * c
                })
            })
            .and_then(|(conn, output)| Queryable::disconnect(conn).map(move |_| output[0]))
            .map(|output| assert_eq!(output, 12u8));

        run(fut).unwrap();
    }

    #[test]
    fn should_first_exec_statement() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                Queryable::first_exec(
                    conn,
                    r"SELECT :a UNION ALL SELECT :b",
                    params! { "a" => 2, "b" => 3 },
                )
            })
            .and_then(|(conn, row_opt): (_, Option<(u8,)>)| {
                Queryable::disconnect(conn).map(move |_| row_opt.unwrap())
            })
            .map(|output| assert_eq!(output, (2u8,)));

        run(fut).unwrap();
    }

    #[test]
    fn should_run_transactions() {
        let fut = Conn::new(get_opts())
            .and_then(|conn| {
                Queryable::drop_query(conn, "CREATE TEMPORARY TABLE tmp (id INT, name TEXT)")
            })
            .and_then(|conn| Queryable::start_transaction(conn, Default::default()))
            .and_then(|transaction| {
                Queryable::drop_query(transaction, "INSERT INTO tmp VALUES (1, 'foo'), (2, 'bar')")
            })
            .and_then(|transaction| transaction.commit())
            .and_then(|conn| Queryable::first(conn, "SELECT COUNT(*) FROM tmp"))
            .map(|(conn, output_opt)| {
                assert_eq!(output_opt, Some((2u8,)));
                conn
            })
            .and_then(|conn| Queryable::start_transaction(conn, Default::default()))
            .and_then(|transaction| {
                Queryable::drop_query(
                    transaction,
                    "INSERT INTO tmp VALUES (3, 'baz'), (4, 'quux')",
                )
            })
            .and_then(|transaction| {
                Queryable::first_exec(transaction, "SELECT COUNT(*) FROM tmp", ())
            })
            .map(|(transaction, output_opt)| {
                assert_eq!(output_opt, Some((4u8,)));
                transaction
            })
            .and_then(|transaction| transaction.rollback())
            .and_then(|conn| Queryable::first(conn, "SELECT COUNT(*) FROM tmp"))
            .map(|(conn, output_opt)| {
                assert_eq!(output_opt, Some((2u8,)));
                conn
            })
            .and_then(Queryable::disconnect);

        run(fut).unwrap();
    }

    #[test]
    fn should_handle_local_infile() {
        use std::io::Write;

        let mut opts = OptsBuilder::from_opts(get_opts());
        opts.local_infile_handler(Some(WhiteListFsLocalInfileHandler::new(
            &["local_infile.txt"][..],
        )));

        let fut = Conn::new(opts)
            .and_then(|conn| Queryable::drop_query(conn, "CREATE TEMPORARY TABLE tmp (a TEXT);"))
            .and_then(|conn| {
                let mut file = ::std::fs::File::create("local_infile.txt").unwrap();
                let _ = file.write(b"AAAAAA\n");
                let _ = file.write(b"BBBBBB\n");
                let _ = file.write(b"CCCCCC\n");
                Queryable::drop_query(
                    conn,
                    "LOAD DATA LOCAL INFILE 'local_infile.txt' INTO TABLE tmp;",
                )
            })
            .and_then(|conn| Queryable::prep_exec(conn, "SELECT * FROM tmp;", ()))
            .and_then(|result| result.map_and_drop(|row| from_row::<(String,)>(row).0))
            .and_then(|(conn, result)| {
                assert_eq!(result.len(), 3);
                assert_eq!(result[0], "AAAAAA");
                assert_eq!(result[1], "BBBBBB");
                assert_eq!(result[2], "CCCCCC");
                Queryable::disconnect(conn)
            })
            .then(|x| {
                let _ = ::std::fs::remove_file("local_infile.txt");
                x
            })
            .then(|result| match result {
                Err(err) => match err {
                    crate::error::Error::Server(ref err) if err.code == 1148 => {
                        // The used command is not allowed with this MySQL version
                        Ok(())
                    }
                    _ => Err(err),
                },
                _ => Ok(()),
            });

        run(fut).unwrap();
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use futures::Future;

        use super::get_opts;
        use crate::{conn::Conn, queryable::Queryable};

        #[bench]
        fn simple_exec(bencher: &mut test::Bencher) {
            let mut runtime = tokio::runtime::Runtime::new().unwrap();
            let mut conn_opt = Some(runtime.block_on(Conn::new(get_opts())).unwrap());

            bencher.iter(|| {
                let conn = conn_opt.take().unwrap();
                conn_opt = Some(runtime.block_on(conn.drop_query("DO 1")).unwrap());
            });

            runtime
                .block_on(conn_opt.take().unwrap().disconnect())
                .unwrap();
            runtime.shutdown_on_idle().wait().unwrap();
        }

        #[bench]
        fn select_large_string(bencher: &mut test::Bencher) {
            let mut runtime = tokio::runtime::Runtime::new().unwrap();
            let mut conn_opt = Some(runtime.block_on(Conn::new(get_opts())).unwrap());

            bencher.iter(|| {
                let conn = conn_opt.take().unwrap();
                conn_opt = Some(
                    runtime
                        .block_on(conn.drop_query("SELECT REPEAT('A', 10000)"))
                        .unwrap(),
                );
            });

            runtime
                .block_on(conn_opt.take().unwrap().disconnect())
                .unwrap();
            runtime.shutdown_on_idle().wait().unwrap();
        }

        #[bench]
        fn prepared_exec(bencher: &mut test::Bencher) {
            let mut runtime = tokio::runtime::Runtime::new().unwrap();
            let mut stmt_opt = Some(
                runtime
                    .block_on(Conn::new(get_opts()).and_then(|conn| conn.prepare("DO 1")))
                    .unwrap(),
            );

            bencher.iter(|| {
                let stmt = stmt_opt.take().unwrap();
                stmt_opt = Some(
                    runtime
                        .block_on(stmt.execute(()).and_then(|result| result.drop_result()))
                        .unwrap(),
                );
            });

            runtime
                .block_on(
                    stmt_opt
                        .take()
                        .unwrap()
                        .close()
                        .and_then(|conn| conn.disconnect()),
                )
                .unwrap();
            runtime.shutdown_on_idle().wait().unwrap();
        }

        #[bench]
        fn prepare_and_exec(bencher: &mut test::Bencher) {
            let mut runtime = tokio::runtime::Runtime::new().unwrap();
            let mut conn_opt = Some(runtime.block_on(Conn::new(get_opts())).unwrap());

            bencher.iter(|| {
                let conn = conn_opt.take().unwrap();
                conn_opt = Some(
                    runtime
                        .block_on(
                            conn.prepare("SELECT ?")
                                .and_then(|stmt| stmt.execute((0,)))
                                .and_then(|result| result.drop_result())
                                .and_then(|stmt| stmt.close()),
                        )
                        .unwrap(),
                );
            });

            runtime
                .block_on(conn_opt.take().unwrap().disconnect())
                .unwrap();
            runtime.shutdown_on_idle().wait().unwrap();
        }
    }
}
