#![cfg(feature = "native-tls-tls")]

use native_tls::{Certificate, TlsConnector};

use crate::io::Endpoint;
use crate::{Result, SslOpts};

impl SslOpts {
    async fn load_root_certs(&self) -> crate::Result<Vec<Certificate>> {
        let mut output = Vec::new();

        for root_cert in self.root_certs() {
            let root_cert_data = root_cert.read().await?;
            output.extend(parse_certs(root_cert_data.as_ref())?);
        }

        Ok(output)
    }
}

impl Endpoint {
    pub async fn make_secure(&mut self, domain: String, ssl_opts: SslOpts) -> Result<()> {
        #[cfg(unix)]
        if self.is_socket() {
            // won't secure socket connection
            return Ok(());
        }

        let mut builder = TlsConnector::builder();
        for root_cert in ssl_opts.load_root_certs().await? {
            builder.add_root_certificate(root_cert);
        }

        if let Some(client_identity) = ssl_opts.client_identity() {
            builder.identity(client_identity.load().await?);
        }
        builder.danger_accept_invalid_hostnames(ssl_opts.skip_domain_validation());
        builder.danger_accept_invalid_certs(ssl_opts.accept_invalid_certs());
        let tls_connector: tokio_native_tls::TlsConnector = builder.build()?.into();

        *self = match self {
            Endpoint::Plain(ref mut stream) => {
                let stream = stream.take().unwrap();
                let tls_stream = tls_connector.connect(&domain, stream).await?;
                Endpoint::Secure(tls_stream)
            }
            Endpoint::Secure(_) => unreachable!(),
            #[cfg(unix)]
            Endpoint::Socket(_) => unreachable!(),
        };

        Ok(())
    }
}

fn parse_certs(buf: &[u8]) -> Result<Vec<Certificate>> {
    Ok(Certificate::from_der(buf).map(|x| vec![x]).or_else(|_| {
        pem::parse_many(buf)
            .unwrap_or_default()
            .iter()
            .map(pem::encode)
            .map(|s| Certificate::from_pem(s.as_bytes()))
            .collect()
    })?)
}
