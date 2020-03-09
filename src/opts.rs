// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use percent_encoding::percent_decode;
use url::{Host, Url};

use std::{
    borrow::Cow,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    path::Path,
    str::FromStr,
    sync::Arc,
    time::Duration,
    vec,
};

use crate::{
    consts::CapabilityFlags,
    error::*,
    local_infile_handler::{LocalInfileHandler, LocalInfileHandlerObject},
};

const DEFAULT_POOL_CONSTRAINTS: PoolConstraints = PoolConstraints { min: 10, max: 100 };
const_assert!(
    _DEFAULT_POOL_CONSTRAINTS_ARE_CORRECT,
    DEFAULT_POOL_CONSTRAINTS.min <= DEFAULT_POOL_CONSTRAINTS.max,
);
const DEFAULT_STMT_CACHE_SIZE: usize = 10;
const DEFAULT_PORT: u16 = 3306;

/// Represents information about a host and port combination that can be converted
/// into socket addresses using to_socket_addrs.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum HostPortOrUrl {
    HostPort(String, u16),
    Url(Url),
}

impl Default for HostPortOrUrl {
    fn default() -> Self {
        HostPortOrUrl::HostPort("127.0.0.1".to_string(), DEFAULT_PORT)
    }
}

impl ToSocketAddrs for HostPortOrUrl {
    type Iter = vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> io::Result<vec::IntoIter<SocketAddr>> {
        let res = match self {
            Self::Url(url) => url.socket_addrs(|| Some(DEFAULT_PORT))?.into_iter(),
            Self::HostPort(host, port) => (host.as_ref(), *port).to_socket_addrs()?,
        };

        Ok(res)
    }
}

impl HostPortOrUrl {
    pub fn get_ip_or_hostname(&self) -> &str {
        match self {
            Self::HostPort(host, _) => host,
            Self::Url(url) => url.host_str().unwrap_or("127.0.0.1"),
        }
    }

    pub fn get_tcp_port(&self) -> u16 {
        match self {
            Self::HostPort(_, port) => *port,
            Self::Url(url) => url.port().unwrap_or(DEFAULT_PORT),
        }
    }

    #[doc(hidden)]
    pub fn is_loopback(&self) -> bool {
        match self {
            Self::HostPort(host, _) => {
                let v4addr: Option<Ipv4Addr> = FromStr::from_str(host).ok();
                let v6addr: Option<Ipv6Addr> = FromStr::from_str(host).ok();
                if let Some(addr) = v4addr {
                    addr.is_loopback()
                } else if let Some(addr) = v6addr {
                    addr.is_loopback()
                } else {
                    host == "localhost"
                }
            }
            Self::Url(url) => match url.host() {
                Some(Host::Ipv4(ip)) => ip.is_loopback(),
                Some(Host::Ipv6(ip)) => ip.is_loopback(),
                Some(Host::Domain(s)) => s == "localhost",
                _ => false,
            },
        }
    }
}

/// Default `inactive_connection_ttl` of a pool.
///
/// `0` value means, that connection will be dropped immediately
/// if it is outside of the pool's lower bound.
pub const DEFAULT_INACTIVE_CONNECTION_TTL: Duration = Duration::from_secs(0);

/// Default `ttl_check_interval` of a pool.
///
/// It isn't used if `inactive_connection_ttl` is `0`.
pub const DEFAULT_TTL_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Ssl Options.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct SslOpts {
    pkcs12_path: Option<Cow<'static, Path>>,
    password: Option<Cow<'static, str>>,
    root_cert_path: Option<Cow<'static, Path>>,
    skip_domain_validation: bool,
    accept_invalid_certs: bool,
}

impl SslOpts {
    /// Sets path to the pkcs12 archive.
    pub fn set_pkcs12_path<T: Into<Cow<'static, Path>>>(
        &mut self,
        pkcs12_path: Option<T>,
    ) -> &mut Self {
        self.pkcs12_path = pkcs12_path.map(Into::into);
        self
    }

    /// Sets the password for a pkcs12 archive (defaults to `None`).
    pub fn set_password<T: Into<Cow<'static, str>>>(&mut self, password: Option<T>) -> &mut Self {
        self.password = password.map(Into::into);
        self
    }

    /// Sets path to a der certificate of the root that connector will trust.
    pub fn set_root_cert_path<T: Into<Cow<'static, Path>>>(
        &mut self,
        root_cert_path: Option<T>,
    ) -> &mut Self {
        self.root_cert_path = root_cert_path.map(Into::into);
        self
    }

    /// The way to not validate the server's domain
    /// name against its certificate (defaults to `false`).
    pub fn set_danger_skip_domain_validation(&mut self, value: bool) -> &mut Self {
        self.skip_domain_validation = value;
        self
    }

    /// If `true` then client will accept invalid certificate (expired, not trusted, ..)
    /// (defaults to `false`).
    pub fn set_danger_accept_invalid_certs(&mut self, value: bool) -> &mut Self {
        self.accept_invalid_certs = value;
        self
    }

    pub fn pkcs12_path(&self) -> Option<&Path> {
        self.pkcs12_path.as_ref().map(|x| x.as_ref())
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_ref().map(AsRef::as_ref)
    }

    pub fn root_cert_path(&self) -> Option<&Path> {
        self.root_cert_path.as_ref().map(AsRef::as_ref)
    }

    pub fn skip_domain_validation(&self) -> bool {
        self.skip_domain_validation
    }

    pub fn accept_invalid_certs(&self) -> bool {
        self.accept_invalid_certs
    }
}

/// Connection pool options.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PoolOptions {
    constraints: PoolConstraints,
    inactive_connection_ttl: Duration,
    ttl_check_interval: Duration,
}

impl PoolOptions {
    /// Creates [`PoolOptions`].
    pub const fn new(
        constraints: PoolConstraints,
        inactive_connection_ttl: Duration,
        ttl_check_interval: Duration,
    ) -> Self {
        Self {
            constraints,
            inactive_connection_ttl,
            ttl_check_interval,
        }
    }

    /// Creates default [`PoolOptions`] with given constraints.
    pub const fn with_constraints(constraints: PoolConstraints) -> Self {
        Self {
            constraints,
            inactive_connection_ttl: DEFAULT_INACTIVE_CONNECTION_TTL,
            ttl_check_interval: DEFAULT_TTL_CHECK_INTERVAL,
        }
    }

    /// Sets pool constraints.
    pub fn set_constraints(&mut self, constraints: PoolConstraints) {
        self.constraints = constraints;
    }

    /// Returns `constrains` value.
    pub fn constraints(&self) -> PoolConstraints {
        self.constraints
    }

    /// Pool will recycle inactive connection if it outside of the lower bound of a pool
    /// and if it is idling longer than this value (defaults to [`DEFAULT_INACTIVE_CONNECTION_TTL`]).
    ///
    /// Note that it may, actually, idle longer because of [`PoolOptions::ttl_check_interval`].
    pub fn set_inactive_connection_ttl(&mut self, ttl: Duration) {
        self.inactive_connection_ttl = ttl;
    }

    /// Returns `inactive_connection_ttl` value.
    pub fn inactive_connection_ttl(&self) -> Duration {
        self.inactive_connection_ttl
    }

    /// Pool will check idling connection for expiration with this interval
    /// (defaults to [`DEFAULT_TTL_CHECK_INTERVAL`]).
    ///
    /// If `interval` is less than one second, then [`DEFAULT_TTL_CHECK_INTERVAL`] will be used.
    pub fn set_ttl_check_interval(&mut self, interval: Duration) {
        if interval < Duration::from_secs(1) {
            self.ttl_check_interval = DEFAULT_TTL_CHECK_INTERVAL
        } else {
            self.ttl_check_interval = interval;
        }
    }

    /// Returns `ttl_check_interval` value.
    pub fn ttl_check_interval(&self) -> Duration {
        self.ttl_check_interval
    }

    /// Returns active bound for this `PoolOptions`.
    ///
    /// This value controls how many connections will be returned to an idle queue of a pool.
    ///
    /// Active bound is either:
    /// * `min` bound of the pool constraints, if this [`PoolOptions`] defines
    ///   `inactive_connection_ttl` to be `0`. This means, that pool will hold no more than `min`
    ///   number of idling connection and other connection will be immediately disconnected.
    /// * `max` bound of the pool constraints, if this [`PoolOptions`] defines
    ///   `inactive_connection_ttl` to be non-zero. This means, that pool will hold up to `max`
    ///   number of idling connections and this number will be eventually reduced to `min`
    ///   by a handler of `ttl_check_interval`.
    pub(crate) fn active_bound(&self) -> usize {
        if self.inactive_connection_ttl > Duration::from_secs(0) {
            self.constraints.max
        } else {
            self.constraints.min
        }
    }
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            constraints: DEFAULT_POOL_CONSTRAINTS,
            inactive_connection_ttl: DEFAULT_INACTIVE_CONNECTION_TTL,
            ttl_check_interval: DEFAULT_TTL_CHECK_INTERVAL,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Default, Debug)]
pub struct InnerOpts {
    mysql_opts: MysqlOpts,
    address: HostPortOrUrl,
}

/// Mysql connection options.
///
/// Build one with [`OptsBuilder`].
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MysqlOpts {
    /// User (defaults to `None`).
    user: Option<String>,

    /// Password (defaults to `None`).
    pass: Option<String>,

    /// Database name (defaults to `None`).
    db_name: Option<String>,

    /// TCP keep alive timeout in milliseconds (defaults to `None`).
    tcp_keepalive: Option<u32>,

    /// Whether to enable `TCP_NODELAY` (defaults to `true`).
    ///
    /// This option disables Nagle's algorithm, which can cause unusually high latency (~40ms) at
    /// some cost to maximum throughput. See blackbeam/rust-mysql-simple#132.
    tcp_nodelay: bool,

    /// Local infile handler
    local_infile_handler: Option<LocalInfileHandlerObject>,

    /// Connection pool options (defaults to [`PoolOptions::default`]).
    pool_options: PoolOptions,

    /// Pool will close connection if time since last IO exceeds this number of seconds
    /// (defaults to `wait_timeout`).
    conn_ttl: Option<Duration>,

    /// Commands to execute on each new database connection.
    init: Vec<String>,

    /// Number of prepared statements cached on the client side (per connection). Defaults to `10`.
    stmt_cache_size: usize,

    /// Driver will require SSL connection if this option isn't `None` (default to `None`).
    ssl_opts: Option<SslOpts>,

    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket (or named pipe on Windows) after TCP connection to `127.0.0.1`
    /// if `true`.
    ///
    /// Will fall back to TCP on error. Use `socket` option to enforce socket connection.
    ///
    /// # Note
    ///
    /// Library will query the `@@socket` server variable to get socket address,
    /// and this address may be incorrect in some cases (i.e. docker).
    prefer_socket: bool,

    /// Path to unix socket (or named pipe on Windows) (defaults to `None`).
    socket: Option<String>,

    /// If not `None`, then client will ask for compression if server supports it
    /// (defaults to `None`).
    ///
    /// Can be defined using `compress` connection url parameter with values:
    /// * `fast` - for compression level 1;
    /// * `best` - for compression level 9;
    /// * `on`, `true` - for default compression level;
    /// * `0`, ..., `9`.
    ///
    /// Note that compression level defined here will affect only outgoing packets.
    compression: Option<crate::Compression>,
}

/// Mysql connection options.
///
/// Build one with [`OptsBuilder`].
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct Opts {
    inner: Arc<InnerOpts>,
}

impl Opts {
    #[doc(hidden)]
    pub fn addr_is_loopback(&self) -> bool {
        self.inner.address.is_loopback()
    }

    pub fn from_url(url: &str) -> std::result::Result<Opts, UrlError> {
        let mut url = Url::parse(url)?;

        // We use the URL for socket address resolution later, so make
        // sure it has a port set.
        if !url.port().is_some() {
            url.set_port(Some(DEFAULT_PORT))
                .map_err(|_| UrlError::Invalid)?;
        }

        let mysql_opts = mysqlopts_from_url(&url)?;
        let address = HostPortOrUrl::Url(url);

        let inner_opts = InnerOpts {
            mysql_opts,
            address,
        };

        Ok(Opts {
            inner: Arc::new(inner_opts),
        })
    }

    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    pub fn get_ip_or_hostname(&self) -> &str {
        self.inner.address.get_ip_or_hostname()
    }

    pub fn get_hostport_or_url(&self) -> &HostPortOrUrl {
        &self.inner.address
    }

    /// TCP port of mysql server (defaults to `3306`).
    pub fn get_tcp_port(&self) -> u16 {
        self.inner.address.get_tcp_port()
    }

    /// User (defaults to `None`).
    pub fn get_user(&self) -> Option<&str> {
        self.inner.mysql_opts.user.as_ref().map(AsRef::as_ref)
    }

    /// Password (defaults to `None`).
    pub fn get_pass(&self) -> Option<&str> {
        self.inner.mysql_opts.pass.as_ref().map(AsRef::as_ref)
    }

    /// Database name (defaults to `None`).
    pub fn get_db_name(&self) -> Option<&str> {
        self.inner.mysql_opts.db_name.as_ref().map(AsRef::as_ref)
    }

    /// Commands to execute on each new database connection.
    pub fn get_init(&self) -> &[String] {
        self.inner.mysql_opts.init.as_ref()
    }

    /// TCP keep alive timeout in milliseconds (defaults to `None).
    pub fn get_tcp_keepalive(&self) -> Option<u32> {
        self.inner.mysql_opts.tcp_keepalive
    }

    /// Whether `TCP_NODELAY` will be set for mysql connection.
    pub fn get_tcp_nodelay(&self) -> bool {
        self.inner.mysql_opts.tcp_nodelay
    }

    /// Local infile handler
    pub fn get_local_infile_handler(&self) -> Option<Arc<dyn LocalInfileHandler>> {
        self.inner
            .mysql_opts
            .local_infile_handler
            .as_ref()
            .map(|x| x.clone_inner())
    }

    /// Connection pool options (defaults to [`Default::default`]).
    pub fn get_pool_options(&self) -> &PoolOptions {
        &self.inner.mysql_opts.pool_options
    }

    /// Pool will close connection if time since last IO exceeds this number of seconds
    /// (defaults to `wait_timeout`).
    pub fn get_conn_ttl(&self) -> Option<Duration> {
        self.inner.mysql_opts.conn_ttl
    }

    /// Number of prepared statements cached on the client side (per connection). Defaults to `10`.
    pub fn get_stmt_cache_size(&self) -> usize {
        self.inner.mysql_opts.stmt_cache_size
    }

    /// Driver will require SSL connection if this option isn't `None` (default to `None`).
    pub fn get_ssl_opts(&self) -> Option<&SslOpts> {
        self.inner.mysql_opts.ssl_opts.as_ref()
    }

    /// Will prefer socket connection if `true` (defaults to `true`).
    pub fn get_perfer_socket(&self) -> bool {
        self.inner.mysql_opts.prefer_socket
    }

    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket (or named pipe on Windows) after TCP connection to `127.0.0.1`
    /// if `true`.
    ///
    /// Will fall back to TCP on error. Use `socket` option to enforce socket connection.
    ///
    /// # Note
    ///
    /// Library will query the `@@socket` server variable to get socket address,
    /// and this address may be incorrect in some cases (i.e. docker).
    pub fn get_prefer_socket(&self) -> bool {
        self.inner.mysql_opts.prefer_socket
    }

    /// Socket path (defaults to `None`).
    pub fn get_socket(&self) -> Option<&str> {
        self.inner.mysql_opts.socket.as_ref().map(|x| &**x)
    }

    /// If not `None`, then client will ask for compression if server supports it
    /// (defaults to `None`).
    ///
    /// Can be defined using `compress` connection url parameter with values:
    /// * `fast` - for compression level 1;
    /// * `best` - for compression level 9;
    /// * `on`, `true` - for default compression level;
    /// * `0`, ..., `9`.
    ///
    /// Note that compression level defined here will affect only outgoing packets.
    pub fn get_compression(&self) -> Option<crate::Compression> {
        self.inner.mysql_opts.compression
    }

    pub(crate) fn get_capabilities(&self) -> CapabilityFlags {
        let mut out = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS
            | CapabilityFlags::CLIENT_LOCAL_FILES
            | CapabilityFlags::CLIENT_MULTI_STATEMENTS
            | CapabilityFlags::CLIENT_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PS_MULTI_RESULTS
            | CapabilityFlags::CLIENT_DEPRECATE_EOF
            | CapabilityFlags::CLIENT_PLUGIN_AUTH;

        if self.inner.mysql_opts.db_name.is_some() {
            out |= CapabilityFlags::CLIENT_CONNECT_WITH_DB;
        }
        if self.inner.mysql_opts.ssl_opts.is_some() {
            out |= CapabilityFlags::CLIENT_SSL;
        }
        if self.inner.mysql_opts.compression.is_some() {
            out |= CapabilityFlags::CLIENT_COMPRESS;
        }

        out
    }
}

impl Default for MysqlOpts {
    fn default() -> MysqlOpts {
        MysqlOpts {
            user: None,
            pass: None,
            db_name: None,
            init: vec![],
            tcp_keepalive: None,
            tcp_nodelay: true,
            local_infile_handler: None,
            pool_options: Default::default(),
            conn_ttl: None,
            stmt_cache_size: DEFAULT_STMT_CACHE_SIZE,
            ssl_opts: None,
            prefer_socket: true,
            socket: None,
            compression: None,
        }
    }
}

/// Connection pool constraints.
///
/// This type stores `min` and `max` constraints for [`crate::Pool`] and ensures that `min <= max`.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct PoolConstraints {
    min: usize,
    max: usize,
}

impl PoolConstraints {
    /// Creates new [`PoolConstraints`] if constraints are valid (`min <= max`).
    ///
    /// `inactive_connection_ttl` will have the default value.
    pub fn new(min: usize, max: usize) -> Option<PoolConstraints> {
        if min <= max {
            Some(PoolConstraints { min, max })
        } else {
            None
        }
    }

    /// Lower bound of this pool constraints.
    pub fn min(&self) -> usize {
        self.min
    }

    /// Upper bound of this pool constraints.
    pub fn max(&self) -> usize {
        self.max
    }
}

impl Default for PoolConstraints {
    fn default() -> Self {
        DEFAULT_POOL_CONSTRAINTS
    }
}

impl From<PoolConstraints> for (usize, usize) {
    /// Transforms constraints to a pair of `(min, max)`.
    fn from(PoolConstraints { min, max }: PoolConstraints) -> Self {
        (min, max)
    }
}

/// Provides a way to build [`Opts`].
///
/// ```ignore
/// // You can create new default builder
/// let mut builder = OptsBuilder::new();
/// builder.ip_or_hostname(Some("foo"))
///        .db_name(Some("bar"))
///        .ssl_opts(Some(("/foo/cert.pem", None::<(String, String)>)));
///
/// // Or use existing T: Into<Opts>
/// let mut builder = OptsBuilder::from_opts(existing_opts);
/// builder.ip_or_hostname(Some("foo"))
///        .db_name(Some("bar"));
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OptsBuilder {
    opts: MysqlOpts,
    ip_or_hostname: String,
    tcp_port: u16,
}

impl Default for OptsBuilder {
    fn default() -> Self {
        let address = HostPortOrUrl::default();
        Self {
            opts: MysqlOpts::default(),
            ip_or_hostname: address.get_ip_or_hostname().into(),
            tcp_port: address.get_tcp_port(),
        }
    }
}

impl OptsBuilder {
    pub fn new() -> Self {
        OptsBuilder::default()
    }

    pub fn from_opts<T: Into<Opts>>(opts: T) -> Self {
        let opts = opts.into();

        OptsBuilder {
            tcp_port: opts.inner.address.get_tcp_port(),
            ip_or_hostname: opts.inner.address.get_ip_or_hostname().to_string(),
            opts: (*opts.inner).mysql_opts.clone(),
        }
    }

    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    pub fn ip_or_hostname<T: Into<String>>(&mut self, ip_or_hostname: T) -> &mut Self {
        self.ip_or_hostname = ip_or_hostname.into();
        self
    }

    /// TCP port of mysql server (defaults to `3306`).
    pub fn tcp_port(&mut self, tcp_port: u16) -> &mut Self {
        self.tcp_port = tcp_port;
        self
    }

    /// User (defaults to `None`).
    pub fn user<T: Into<String>>(&mut self, user: Option<T>) -> &mut Self {
        self.opts.user = user.map(Into::into);
        self
    }

    /// Password (defaults to `None`).
    pub fn pass<T: Into<String>>(&mut self, pass: Option<T>) -> &mut Self {
        self.opts.pass = pass.map(Into::into);
        self
    }

    /// Database name (defaults to `None`).
    pub fn db_name<T: Into<String>>(&mut self, db_name: Option<T>) -> &mut Self {
        self.opts.db_name = db_name.map(Into::into);
        self
    }

    /// Commands to execute on each new database connection.
    pub fn init<T: Into<String>>(&mut self, init: Vec<T>) -> &mut Self {
        self.opts.init = init.into_iter().map(Into::into).collect();
        self
    }

    /// TCP keep alive timeout in milliseconds (defaults to `None`).
    pub fn tcp_keepalive<T: Into<u32>>(&mut self, tcp_keepalive: Option<T>) -> &mut Self {
        self.opts.tcp_keepalive = tcp_keepalive.map(Into::into);
        self
    }

    /// Set the `TCP_NODELAY` option for the mysql connection (defaults to `true`).
    ///
    /// Setting this option to false re-enables Nagle's algorithm, which can cause unusually high
    /// latency (~40ms) but may increase maximum throughput. See #132.
    pub fn tcp_nodelay(&mut self, nodelay: bool) -> &mut Self {
        self.opts.tcp_nodelay = nodelay;
        self
    }

    /// Handler for local infile requests (defaults to `None`).
    pub fn local_infile_handler<T>(&mut self, handler: Option<T>) -> &mut Self
    where
        T: LocalInfileHandler + 'static,
    {
        self.opts.local_infile_handler = handler.map(LocalInfileHandlerObject::new);
        self
    }

    /// Connection pool options (defaults to `PoolOptions::default()`).
    pub fn pool_options<T: Into<Option<PoolOptions>>>(&mut self, pool_options: T) -> &mut Self {
        self.opts.pool_options = pool_options.into().unwrap_or_default();
        self
    }

    /// Pool will close connection if time since last IO exceeds this number of seconds
    /// (defaults to `wait_timeout`. `None` to reset to default).
    pub fn conn_ttl<T: Into<Option<Duration>>>(&mut self, conn_ttl: T) -> &mut Self {
        self.opts.conn_ttl = conn_ttl.into();
        self
    }

    /// Number of prepared statements cached on the client side (per connection). Defaults to `10`.
    ///
    /// Call with `None` to reset to default.
    pub fn stmt_cache_size<T>(&mut self, cache_size: T) -> &mut Self
    where
        T: Into<Option<usize>>,
    {
        self.opts.stmt_cache_size = cache_size.into().unwrap_or(DEFAULT_STMT_CACHE_SIZE);
        self
    }

    /// Driver will require SSL connection if this option isn't `None` (default to `None`).
    pub fn ssl_opts<T: Into<Option<SslOpts>>>(&mut self, ssl_opts: T) -> &mut Self {
        self.opts.ssl_opts = ssl_opts.into();
        self
    }

    /// Prefer socket connection (defaults to `true`).
    ///
    /// Will reconnect via socket (or named pipe on Windows) after TCP connection to `127.0.0.1`
    /// if `true`.
    ///
    /// Will fall back to TCP on error. Use `socket` option to enforce socket connection.
    ///
    /// # Note
    ///
    /// Library will query the `@@socket` server variable to get socket address,
    /// and this address may be incorrect in some cases (i.e. docker).
    pub fn prefer_socket<T: Into<Option<bool>>>(&mut self, prefer_socket: T) -> &mut Self {
        self.opts.prefer_socket = prefer_socket.into().unwrap_or(true);
        self
    }

    /// Path to unix socket (or named pipe on Windows) (defaults to `None`).
    pub fn socket<T: Into<String>>(&mut self, socket: Option<T>) -> &mut Self {
        self.opts.socket = socket.map(Into::into);
        self
    }

    /// If not `None`, then client will ask for compression if server supports it
    /// (defaults to `None`).
    ///
    /// Can be defined using `compress` connection url parameter with values:
    /// * `fast` - for compression level 1;
    /// * `best` - for compression level 9;
    /// * `on`, `true` - for default compression level;
    /// * `0`, ..., `9`.
    ///
    /// Note that compression level defined here will affect only outgoing packets.
    pub fn compression<T: Into<Option<crate::Compression>>>(
        &mut self,
        compression: T,
    ) -> &mut Self {
        self.opts.compression = compression.into();
        self
    }
}

impl From<OptsBuilder> for Opts {
    fn from(builder: OptsBuilder) -> Opts {
        let address = HostPortOrUrl::HostPort(builder.ip_or_hostname, builder.tcp_port);
        let inner_opts = InnerOpts {
            mysql_opts: builder.opts,
            address,
        };

        Opts {
            inner: Arc::new(inner_opts),
        }
    }
}

fn get_opts_user_from_url(url: &Url) -> Option<String> {
    let user = url.username();
    if user != "" {
        Some(
            percent_decode(user.as_ref())
                .decode_utf8_lossy()
                .into_owned(),
        )
    } else {
        None
    }
}

fn get_opts_pass_from_url(url: &Url) -> Option<String> {
    if let Some(pass) = url.password() {
        Some(
            percent_decode(pass.as_ref())
                .decode_utf8_lossy()
                .into_owned(),
        )
    } else {
        None
    }
}

fn get_opts_db_name_from_url(url: &Url) -> Option<String> {
    if let Some(mut segments) = url.path_segments() {
        segments.next().map(|db_name| {
            percent_decode(db_name.as_ref())
                .decode_utf8_lossy()
                .into_owned()
        })
    } else {
        None
    }
}

fn from_url_basic(url: &Url) -> std::result::Result<(MysqlOpts, Vec<(String, String)>), UrlError> {
    if url.scheme() != "mysql" {
        return Err(UrlError::UnsupportedScheme {
            scheme: url.scheme().to_string(),
        });
    }
    if url.cannot_be_a_base() || !url.has_host() {
        return Err(UrlError::Invalid);
    }
    let user = get_opts_user_from_url(&url);
    let pass = get_opts_pass_from_url(&url);
    let db_name = get_opts_db_name_from_url(&url);

    let query_pairs = url.query_pairs().into_owned().collect();
    let opts = MysqlOpts {
        user,
        pass,
        db_name,
        ..MysqlOpts::default()
    };

    Ok((opts, query_pairs))
}

fn mysqlopts_from_url(url: &Url) -> std::result::Result<MysqlOpts, UrlError> {
    let (mut opts, query_pairs): (MysqlOpts, _) = from_url_basic(url)?;
    let mut pool_min = DEFAULT_POOL_CONSTRAINTS.min;
    let mut pool_max = DEFAULT_POOL_CONSTRAINTS.max;
    for (key, value) in query_pairs {
        if key == "pool_min" {
            match usize::from_str(&*value) {
                Ok(value) => pool_min = value,
                _ => {
                    return Err(UrlError::InvalidParamValue {
                        param: "pool_min".into(),
                        value,
                    });
                }
            }
        } else if key == "pool_max" {
            match usize::from_str(&*value) {
                Ok(value) => pool_max = value,
                _ => {
                    return Err(UrlError::InvalidParamValue {
                        param: "pool_max".into(),
                        value,
                    });
                }
            }
        } else if key == "inactive_connection_ttl" {
            match u64::from_str(&*value) {
                Ok(value) => opts
                    .pool_options
                    .set_inactive_connection_ttl(Duration::from_secs(value)),
                _ => {
                    return Err(UrlError::InvalidParamValue {
                        param: "inactive_connection_ttl".into(),
                        value,
                    });
                }
            }
        } else if key == "ttl_check_interval" {
            match u64::from_str(&*value) {
                Ok(value) => opts
                    .pool_options
                    .set_ttl_check_interval(Duration::from_secs(value)),
                _ => {
                    return Err(UrlError::InvalidParamValue {
                        param: "ttl_check_interval".into(),
                        value,
                    });
                }
            }
        } else if key == "conn_ttl" {
            match u32::from_str(&*value) {
                Ok(value) => opts.conn_ttl = Some(Duration::from_secs(value as u64)),
                _ => {
                    return Err(UrlError::InvalidParamValue {
                        param: "conn_ttl".into(),
                        value,
                    });
                }
            }
        } else if key == "tcp_keepalive" {
            match u32::from_str(&*value) {
                Ok(value) => opts.tcp_keepalive = Some(value),
                _ => {
                    return Err(UrlError::InvalidParamValue {
                        param: "tcp_keepalive_ms".into(),
                        value,
                    });
                }
            }
        } else if key == "tcp_nodelay" {
            match bool::from_str(&*value) {
                Ok(value) => opts.tcp_nodelay = value,
                _ => {
                    return Err(UrlError::InvalidParamValue {
                        param: "tcp_nodelay".into(),
                        value,
                    });
                }
            }
        } else if key == "stmt_cache_size" {
            match usize::from_str(&*value) {
                Ok(stmt_cache_size) => {
                    opts.stmt_cache_size = stmt_cache_size;
                }
                _ => {
                    return Err(UrlError::InvalidParamValue {
                        param: "stmt_cache_size".into(),
                        value,
                    });
                }
            }
        } else if key == "prefer_socket" {
            match bool::from_str(&*value) {
                Ok(prefer_socket) => {
                    opts.prefer_socket = prefer_socket;
                }
                _ => {
                    return Err(UrlError::InvalidParamValue {
                        param: "prefer_socket".into(),
                        value,
                    });
                }
            }
        } else if key == "socket" {
            opts.socket = Some(value)
        } else if key == "compression" {
            if value == "fast" {
                opts.compression = Some(crate::Compression::fast());
            } else if value == "on" || value == "true" {
                opts.compression = Some(crate::Compression::default());
            } else if value == "best" {
                opts.compression = Some(crate::Compression::best());
            } else if value.len() == 1 && 0x30 <= value.as_bytes()[0] && value.as_bytes()[0] <= 0x39
            {
                opts.compression =
                    Some(crate::Compression::new((value.as_bytes()[0] - 0x30) as u32));
            } else {
                return Err(UrlError::InvalidParamValue {
                    param: "compression".into(),
                    value: value.to_string(),
                });
            }
        } else {
            return Err(UrlError::UnknownParameter { param: key });
        }
    }

    if let Some(pool_constraints) = PoolConstraints::new(pool_min, pool_max) {
        opts.pool_options.set_constraints(pool_constraints);
    } else {
        return Err(UrlError::InvalidPoolConstraints {
            min: pool_min,
            max: pool_max,
        });
    }

    Ok(opts)
}

impl FromStr for Opts {
    type Err = UrlError;

    fn from_str(s: &str) -> std::result::Result<Self, <Self as FromStr>::Err> {
        Opts::from_url(s)
    }
}

impl<T: AsRef<str> + Sized> From<T> for Opts {
    fn from(url: T) -> Opts {
        Opts::from_url(url.as_ref()).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::{HostPortOrUrl, MysqlOpts, Opts, Url};
    use crate::error::UrlError::InvalidParamValue;

    use std::str::FromStr;

    #[test]
    fn test_builer_eq_url() {
        const URL: &str = "mysql://iq-controller@localhost/iq_controller";

        let url_opts = super::Opts::from_str(URL).unwrap();
        let mut builder = super::OptsBuilder::new();
        builder
            .user(Some("iq-controller"))
            .ip_or_hostname("localhost")
            .db_name(Some("iq_controller"));
        let builder_opts = Opts::from(builder);

        assert_eq!(url_opts.addr_is_loopback(), builder_opts.addr_is_loopback());
        assert_eq!(
            url_opts.get_ip_or_hostname(),
            builder_opts.get_ip_or_hostname()
        );
        assert_eq!(url_opts.get_tcp_port(), builder_opts.get_tcp_port());
        assert_eq!(url_opts.get_user(), builder_opts.get_user());
        assert_eq!(url_opts.get_pass(), builder_opts.get_pass());
        assert_eq!(url_opts.get_db_name(), builder_opts.get_db_name());
        assert_eq!(url_opts.get_init(), builder_opts.get_init());
        assert_eq!(
            url_opts.get_tcp_keepalive(),
            builder_opts.get_tcp_keepalive()
        );
        assert_eq!(url_opts.get_tcp_nodelay(), builder_opts.get_tcp_nodelay());
        assert_eq!(url_opts.get_pool_options(), builder_opts.get_pool_options());
        assert_eq!(url_opts.get_conn_ttl(), builder_opts.get_conn_ttl());
        assert_eq!(
            url_opts.get_stmt_cache_size(),
            builder_opts.get_stmt_cache_size()
        );
        assert_eq!(url_opts.get_ssl_opts(), builder_opts.get_ssl_opts());
        assert_eq!(
            url_opts.get_perfer_socket(),
            builder_opts.get_perfer_socket()
        );
        assert_eq!(
            url_opts.get_prefer_socket(),
            builder_opts.get_prefer_socket()
        );
        assert_eq!(url_opts.get_socket(), builder_opts.get_socket());
        assert_eq!(url_opts.get_compression(), builder_opts.get_compression());
        assert_eq!(
            url_opts.get_hostport_or_url().get_ip_or_hostname(),
            builder_opts.get_hostport_or_url().get_ip_or_hostname()
        );
        assert_eq!(
            url_opts.get_hostport_or_url().get_tcp_port(),
            builder_opts.get_hostport_or_url().get_tcp_port()
        );
    }

    #[test]
    fn should_convert_url_into_opts() {
        let url = "mysql://usr:pw@192.168.1.1:3309/dbname";
        let parsed_url = Url::parse("mysql://usr:pw@192.168.1.1:3309/dbname").unwrap();

        let mysql_opts = MysqlOpts {
            user: Some("usr".to_string()),
            pass: Some("pw".to_string()),
            db_name: Some("dbname".to_string()),
            ..MysqlOpts::default()
        };
        let host = HostPortOrUrl::Url(parsed_url);

        let opts = Opts::from_url(url).unwrap();

        assert_eq!(opts.inner.mysql_opts, mysql_opts);
        assert_eq!(opts.get_hostport_or_url(), &host);
    }

    #[test]
    fn should_convert_ipv6_url_into_opts() {
        let url = "mysql://usr:pw@[::1]:3309/dbname";

        let opts = Opts::from_url(url).unwrap();

        assert_eq!(opts.get_ip_or_hostname(), "[::1]");
    }

    #[test]
    #[should_panic]
    fn should_panic_on_invalid_url() {
        let opts = "42";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    fn should_panic_on_invalid_scheme() {
        let opts = "postgres://localhost";
        let _: Opts = opts.into();
    }

    #[test]
    #[should_panic]
    fn should_panic_on_unknown_query_param() {
        let opts = "mysql://localhost/foo?bar=baz";
        let _: Opts = opts.into();
    }

    #[test]
    fn should_parse_compression() {
        let err = Opts::from_url("mysql://localhost/foo?compression=").unwrap_err();
        assert_eq!(
            err,
            InvalidParamValue {
                param: "compression".into(),
                value: "".into()
            }
        );

        let err = Opts::from_url("mysql://localhost/foo?compression=a").unwrap_err();
        assert_eq!(
            err,
            InvalidParamValue {
                param: "compression".into(),
                value: "a".into()
            }
        );

        let opts = Opts::from_url("mysql://localhost/foo?compression=fast").unwrap();
        assert_eq!(opts.get_compression(), Some(crate::Compression::fast()));

        let opts = Opts::from_url("mysql://localhost/foo?compression=on").unwrap();
        assert_eq!(opts.get_compression(), Some(crate::Compression::default()));

        let opts = Opts::from_url("mysql://localhost/foo?compression=true").unwrap();
        assert_eq!(opts.get_compression(), Some(crate::Compression::default()));

        let opts = Opts::from_url("mysql://localhost/foo?compression=best").unwrap();
        assert_eq!(opts.get_compression(), Some(crate::Compression::best()));

        let opts = Opts::from_url("mysql://localhost/foo?compression=0").unwrap();
        assert_eq!(opts.get_compression(), Some(crate::Compression::new(0)));

        let opts = Opts::from_url("mysql://localhost/foo?compression=9").unwrap();
        assert_eq!(opts.get_compression(), Some(crate::Compression::new(9)));
    }
}
