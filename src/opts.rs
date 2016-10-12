use errors::*;

use std::net::{
    Ipv4Addr,
    Ipv6Addr,
};
use std::str::FromStr;

use url::Url;
use url::percent_encoding::percent_decode;

/// Mysql connection options.
///
/// Build one with [`OptsBuilder`](struct.OptsBuilder.html).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Opts {
    /// Address of mysql server (defaults to `127.0.0.1`). Hostnames should also work.
    ip_or_hostname: String,

    /// TCP port of mysql server (defaults to `3306`).
    tcp_port: u16,

    /// User (defaults to `None`).
    user: Option<String>,

    /// Password (defaults to `None`).
    pass: Option<String>,

    /// Database name (defaults to `None`).
    db_name: Option<String>,

    // TODO: keepalive_timeout
    // TODO: local infile handler

    /// Commands to execute on each new database connection.
    init: Vec<String>,
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
        OptsBuilder {
            opts: opts.into(),
        }
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
}

impl From<OptsBuilder> for Opts {
    fn from(builder: OptsBuilder) -> Opts {
        builder.opts
    }
}

fn get_opts_user_from_url(url: &Url) -> Option<String> {
    let user = url.username();
    if user != "" {
        Some(percent_decode(user.as_ref()).decode_utf8_lossy().into_owned())
    } else {
        None
    }
}

fn get_opts_pass_from_url(url: &Url) -> Option<String> {
    if let Some(pass) = url.password() {
        Some(percent_decode(pass.as_ref()).decode_utf8_lossy().into_owned())
    } else {
        None
    }
}

fn get_opts_db_name_from_url(url: &Url) -> Option<String> {
    if let Some(mut segments) = url.path_segments() {
        segments.next().map(|db_name| {
            percent_decode(db_name.as_ref()).decode_utf8_lossy().into_owned()
        })
    } else {
        None
    }
}

fn from_url_basic(url_str: &str) -> Result<(Opts, Vec<(String, String)>)> {
    let url = try!(Url::parse(url_str));
    if url.scheme() != "mysql" {
        return Err(ErrorKind::UrlUnsupportedScheme(url.scheme().to_string()).into());
    }
    if url.cannot_be_a_base() || !url.has_host() {
        return Err(ErrorKind::UrlInvalid.into());
    }
    let user = get_opts_user_from_url(&url);
    let pass = get_opts_pass_from_url(&url);
    let ip_or_hostname = url.host_str().map(String::from).unwrap_or("127.0.0.1".into());
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
    let (opts, query_pairs) = try!(from_url_basic(url));
    for (key, _ /* value */) in query_pairs {
        if key == "prefer_socket" {
//            if cfg!(all(not(feature = "socket"), not(feature = "pipe"))) {
//                return Err(
//                    ErrorKind::UrlFeatureRequired("`socket' or `pipe'".into(),
//                                                  "prefer_socket".into()).into()
//                );
//            } else {
//                if value == "true" {
//                    opts.set_prefer_socket(true);
//                } else if value == "false" {
//                    opts.set_prefer_socket(false);
//                } else {
//                    return Err(ErrorKind::UrlInvalidValue("prefer_socket".into(), value).into());
//                }
//            }
        } else {
            return Err(ErrorKind::UrlUnknownParameter(key).into());
        }
    }
    Ok(opts)
}

impl<'a> From<&'a str> for Opts {
    fn from(url: &'a str) -> Opts {
        match from_url(url) {
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
        assert_eq!(Opts {
            user: Some("usr".to_string()),
            pass: Some("pw".to_string()),
            ip_or_hostname: "192.168.1.1".to_string(),
            tcp_port: 3309,
            db_name: Some("dbname".to_string()),
            ..Opts::default()
        }, opts.into());
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
