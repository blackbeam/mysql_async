// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use consts::{self, CapabilityFlags};
use errors::*;

use local_infile_handler::{LocalInfileHandler, LocalInfileHandlerObject};

use std::borrow::Cow;
use std::path::Path;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;

use url::Url;
use url::percent_encoding::percent_decode;

const DEFAULT_MIN_CONNS: usize = 10;
const DEFAULT_MAX_CONNS: usize = 100;
const DEFAULT_STMT_CACHE_SIZE: usize = 10;

/// Ssl Options.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SslOpts {
    pkcs12_path: Cow<'static, Path>,
    password: Option<Cow<'static, str>>,
    skip_domain_validation: bool,
}

impl SslOpts {
    pub fn new<T: Into<Cow<'static, Path>>>(pkcs12_path: T) -> SslOpts {
        SslOpts {
            pkcs12_path: pkcs12_path.into(),
            password: None,
            skip_domain_validation: false,
        }
    }

    /// Sets path to the pkcs12 archive.
    pub fn set_pkcs12_path<T: Into<Cow<'static, Path>>>(&mut self, pkcs12_path: T) -> &mut Self {
        self.pkcs12_path = pkcs12_path.into();
        self
    }

    /// Sets the password for a pkcs12 archive (defaults to `None`).
    pub fn set_password<T: Into<Cow<'static, str>>>(&mut self, password: Option<T>) -> &mut Self {
        self.password = password.map(Into::into);
        self
    }

    /// The way to not validate the server's domain
    /// name against its certificate (defaults to `false`).
    pub fn set_danger_skip_domain_validation(&mut self, value: bool) -> &mut Self {
        self.skip_domain_validation = value;
        self
    }

    pub fn pkcs12_path(&self) -> &Path {
        self.pkcs12_path.as_ref()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_ref().map(AsRef::as_ref)
    }

    pub fn skip_domain_validation(&self) -> bool {
        self.skip_domain_validation
    }
}

/// Mysql connection options.
///
/// Build one with [`OptsBuilder`](struct.OptsBuilder.html).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Opts {
    /// Address of mysql server (defaults to `127.0.0.1`). Host names should also work.
    ip_or_hostname: String,

    /// TCP port of mysql server (defaults to `3306`).
    tcp_port: u16,

    /// User (defaults to `None`).
    user: Option<String>,

    /// Password (defaults to `None`).
    pass: Option<String>,

    /// Database name (defaults to `None`).
    db_name: Option<String>,

    /// TCP keep alive timeout in milliseconds (defaults to `None`).
    tcp_keepalive: Option<u32>,

    /// Local infile handler
    local_infile_handler: Option<LocalInfileHandlerObject>,

    /// Lower bound of opened connections for `Pool` (defaults to 10).
    pool_min: usize,

    /// Upper bound of opened connections for `Pool` (defaults to 100).
    pool_max: usize,

    /// Pool will close connection if time since last IO exceeds this value
    /// (defaults to `wait_timeout`).
    conn_ttl: Option<u32>,

    /// Commands to execute on each new database connection.
    init: Vec<String>,

    /// Number of prepared statements cached on the client side (per connection). Defaults to `10`.
    stmt_cache_size: usize,

    /// Driver will require SSL connection if this option isn't `None` (default to `None`).
    ///
    /// This option requires `ssl` feature to work.
    ssl_opts: Option<SslOpts>,
}

impl Opts {
    #[doc(hidden)]
    pub fn addr_is_loopback(&self) -> bool {
        let v4addr: Option<Ipv4Addr> = FromStr::from_str(self.ip_or_hostname.as_ref()).ok();
        let v6addr: Option<Ipv6Addr> = FromStr::from_str(self.ip_or_hostname.as_ref()).ok();
        if let Some(addr) = v4addr {
            addr.is_loopback()
        } else if let Some(addr) = v6addr {
            addr.is_loopback()
        } else if self.ip_or_hostname == "localhost" {
            true
        } else {
            false
        }
    }

    pub fn from_url(url: &str) -> Result<Opts> {
        from_url(url)
    }

    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    pub fn get_ip_or_hostname(&self) -> &str {
        &*self.ip_or_hostname
    }

    /// TCP port of mysql server (defaults to `3306`).
    pub fn get_tcp_port(&self) -> u16 {
        self.tcp_port
    }

    /// User (defaults to `None`).
    pub fn get_user(&self) -> Option<&String> {
        self.user.as_ref()
    }

    /// Password (defaults to `None`).
    pub fn get_pass(&self) -> Option<&String> {
        self.pass.as_ref()
    }

    /// Database name (defaults to `None`).
    pub fn get_db_name(&self) -> Option<&String> {
        self.db_name.as_ref()
    }

    /// Commands to execute on each new database connection.
    pub fn get_init(&self) -> &[String] {
        self.init.as_ref()
    }

    /// TCP keep alive timeout in milliseconds (defaults to `None).
    pub fn get_tcp_keepalive(&self) -> Option<u32> {
        self.tcp_keepalive.clone()
    }

    /// Local infile handler
    pub fn get_local_infile_handler(&self) -> Option<Arc<LocalInfileHandler>> {
        self.local_infile_handler.as_ref().map(|x| x.clone_inner())
    }

    /// Lower bound of opened connections for `Pool` (defaults to 10).
    pub fn get_pool_min(&self) -> usize {
        self.pool_min
    }

    /// Upper bound of opened connections for `Pool` (defaults to 100).
    pub fn get_pool_max(&self) -> usize {
        self.pool_max
    }

    /// Pool will close connection if time since last IO exceeds this value
    /// (defaults to `wait_timeout`).
    pub fn get_conn_ttl(&self) -> Option<u32> {
        self.conn_ttl
    }

    /// Number of prepared statements cached on the client side (per connection). Defaults to `10`.
    pub fn get_stmt_cache_size(&self) -> usize {
        self.stmt_cache_size
    }

    /// Driver will require SSL connection if this option isn't `None` (default to `None`).
    ///
    /// This option requires `ssl` feature to work.
    pub fn get_ssl_opts(&self) -> Option<&SslOpts> {
        self.ssl_opts.as_ref()
    }

    pub(crate) fn get_capabilities(&self) -> CapabilityFlags {
        let mut out = consts::CLIENT_PROTOCOL_41 | consts::CLIENT_SECURE_CONNECTION |
            consts::CLIENT_LONG_PASSWORD | consts::CLIENT_TRANSACTIONS |
            consts::CLIENT_LOCAL_FILES |
            consts::CLIENT_MULTI_STATEMENTS | consts::CLIENT_MULTI_RESULTS |
            consts::CLIENT_PS_MULTI_RESULTS;

        if self.db_name.is_some() {
            out |= consts::CLIENT_CONNECT_WITH_DB;
        }
        if self.ssl_opts.is_some() {
            out |= consts::CLIENT_SSL;
        }

        out
    }
}

impl Default for Opts {
    fn default() -> Opts {
        Opts {
            ip_or_hostname: "127.0.0.1".to_string(),
            tcp_port: 3306,
            user: None,
            pass: None,
            db_name: None,
            init: vec![],
            tcp_keepalive: None,
            local_infile_handler: None,
            pool_min: 10,
            pool_max: 100,
            conn_ttl: None,
            stmt_cache_size: DEFAULT_STMT_CACHE_SIZE,
            ssl_opts: None,
        }
    }
}

/// Provides a way to build [`Opts`](struct.Opts.html).
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
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct OptsBuilder {
    opts: Opts,
}

impl OptsBuilder {
    pub fn new() -> Self {
        OptsBuilder::default()
    }

    pub fn from_opts<T: Into<Opts>>(opts: T) -> Self {
        OptsBuilder { opts: opts.into() }
    }

    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    pub fn ip_or_hostname<T: Into<String>>(&mut self, ip_or_hostname: T) -> &mut Self {
        self.opts.ip_or_hostname = ip_or_hostname.into();
        self
    }

    /// TCP port of mysql server (defaults to `3306`).
    pub fn tcp_port(&mut self, tcp_port: u16) -> &mut Self {
        self.opts.tcp_port = tcp_port;
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

    /// Handler for local infile requests (defaults to `None`).
    pub fn local_infile_handler<T>(&mut self, handler: Option<T>) -> &mut Self
    where
        T: LocalInfileHandler + 'static,
    {
        self.opts.local_infile_handler = handler.map(LocalInfileHandlerObject::new);
        self
    }

    /// Lower bound of opened connections for `Pool`
    /// (defaults to `10`. `None` to reset to default).
    pub fn pool_min<T: Into<usize>>(&mut self, pool_min: Option<T>) -> &mut Self {
        self.opts.pool_min = pool_min.map(Into::into).unwrap_or(DEFAULT_MIN_CONNS);
        self
    }

    /// Lower bound of opened connections for `Pool`
    /// (defaults to `100`. `None` to reset to default).
    pub fn pool_max<T: Into<usize>>(&mut self, pool_max: Option<T>) -> &mut Self {
        self.opts.pool_max = pool_max.map(Into::into).unwrap_or(DEFAULT_MAX_CONNS);
        self
    }

    /// Pool will close connection if time since last IO exceeds this value
    /// (defaults to `wait_timeout`. `None` to reset to default).
    pub fn conn_ttl<T: Into<u32>>(&mut self, conn_ttl: Option<T>) -> &mut Self {
        self.opts.conn_ttl = conn_ttl.map(Into::into);
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
    ///
    /// This option requires `ssl` feature to work.
    pub fn ssl_opts<T: Into<Option<SslOpts>>>(&mut self, ssl_opts: T) -> &mut Self {
        self.opts.ssl_opts = ssl_opts.into();
        self
    }
}

impl From<OptsBuilder> for Opts {
    fn from(builder: OptsBuilder) -> Opts {
        builder.opts
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

fn from_url_basic(url_str: &str) -> Result<(Opts, Vec<(String, String)>)> {
    let url = Url::parse(url_str)?;
    if url.scheme() != "mysql" {
        return Err(
            ErrorKind::UrlUnsupportedScheme(url.scheme().to_string()).into(),
        );
    }
    if url.cannot_be_a_base() || !url.has_host() {
        return Err(ErrorKind::UrlInvalid.into());
    }
    let user = get_opts_user_from_url(&url);
    let pass = get_opts_pass_from_url(&url);
    let ip_or_hostname = url.host_str().map(String::from).unwrap_or(
        "127.0.0.1".into(),
    );
    let tcp_port = url.port().unwrap_or(3306);
    let db_name = get_opts_db_name_from_url(&url);

    let query_pairs = url.query_pairs().into_owned().collect();
    let opts = Opts {
        user: user,
        pass: pass,
        ip_or_hostname: ip_or_hostname,
        tcp_port: tcp_port,
        db_name: db_name,
        ..Opts::default()
    };

    Ok((opts, query_pairs))
}

fn from_url(url: &str) -> Result<Opts> {
    let (mut opts, query_pairs) = from_url_basic(url)?;
    for (key, value) in query_pairs {
        if key == "pool_min" {
            match usize::from_str(&*value) {
                Ok(value) => opts.pool_min = value,
                _ => {
                    return Err(
                        ErrorKind::UrlInvalidParamValue("pool_min".into(), value).into(),
                    )
                }
            }
        } else if key == "pool_max" {
            match usize::from_str(&*value) {
                Ok(value) => opts.pool_max = value,
                _ => {
                    return Err(
                        ErrorKind::UrlInvalidParamValue("pool_max".into(), value).into(),
                    )
                }
            }
        } else if key == "conn_ttl" {
            match u32::from_str(&*value) {
                Ok(value) => opts.conn_ttl = Some(value),
                _ => {
                    return Err(
                        ErrorKind::UrlInvalidParamValue("conn_ttl".into(), value).into(),
                    )
                }
            }
        } else if key == "tcp_keepalive" {
            match u32::from_str(&*value) {
                Ok(value) => opts.tcp_keepalive = Some(value),
                _ => {
                    return Err(
                        ErrorKind::UrlInvalidParamValue("tcp_keepalive_ms".into(), value).into(),
                    )
                }
            }
        } else if key == "stmt_cache_size" {
            match usize::from_str(&*value) {
                Ok(stmt_cache_size) => {
                    opts.stmt_cache_size = stmt_cache_size;
                }
                _ => {
                    return Err(
                        ErrorKind::UrlInvalidParamValue("stmt_cache_size".into(), value).into(),
                    );
                }
            }
        } else {
            return Err(ErrorKind::UrlUnknownParameter(key).into());
        }
    }
    if opts.pool_min > opts.pool_max {
        return Err(
            ErrorKind::InvalidPoolConstraints(opts.pool_min, opts.pool_max).into(),
        );
    }
    Ok(opts)
}

impl<T: AsRef<str> + Sized> From<T> for Opts {
    fn from(url: T) -> Opts {
        match from_url(url.as_ref()) {
            Ok(opts) => opts,
            Err(err) => panic!("{}", err),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Opts;

    #[test]
    fn should_convert_url_into_opts() {
        let opts = "mysql://usr:pw@192.168.1.1:3309/dbname";
        assert_eq!(
            Opts {
                user: Some("usr".to_string()),
                pass: Some("pw".to_string()),
                ip_or_hostname: "192.168.1.1".to_string(),
                tcp_port: 3309,
                db_name: Some("dbname".to_string()),
                ..Opts::default()
            },
            opts.into()
        );
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
}
