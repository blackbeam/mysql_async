use crate::io::Endpoint;
use crate::{Result, SslOpts};
use mysql_common::crypto::MariaDbZeroConfigCheck;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub(crate) struct TlsConnector;

impl SslOpts {
    pub(crate) async fn build_tls_connector(
        &self,
        _zero_config_check: Option<Arc<Mutex<Option<MariaDbZeroConfigCheck>>>>,
    ) -> Result<TlsConnector> {
        panic!(
            "Client had asked for TLS connection but TLS support is disabled. \
            Please enable one of the following features: [\"native-tls-tls\", \"rustls-tls\"]"
        )
    }
}

impl Endpoint {
    pub async fn make_secure(
        &mut self,
        _domain: String,
        _tls_connector: &TlsConnector,
    ) -> Result<()> {
        unreachable!();
    }
}
