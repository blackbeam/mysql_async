#![cfg(feature = "native-tls")]

use std::{fs::File, io::Read};

use native_tls::{Certificate, Identity, TlsConnector};

use crate::io::Endpoint;
use crate::{Result, SslOpts};

impl Endpoint {
    pub async fn make_secure(&mut self, domain: String, ssl_opts: SslOpts) -> Result<()> {
        #[cfg(unix)]
        if self.is_socket() {
            // won't secure socket connection
            return Ok(());
        }

        let mut builder = TlsConnector::builder();
        if let Some(root_cert_path) = ssl_opts.root_cert_path() {
            let mut root_cert_data = vec![];
            let mut root_cert_file = File::open(root_cert_path)?;
            root_cert_file.read_to_end(&mut root_cert_data)?;

            let root_certs = Certificate::from_der(&*root_cert_data)
                .map(|x| vec![x])
                .or_else(|_| {
                    pem::parse_many(&*root_cert_data)
                        .unwrap_or_default()
                        .iter()
                        .map(pem::encode)
                        .map(|s| Certificate::from_pem(s.as_bytes()))
                        .collect()
                })?;

            for root_cert in root_certs {
                builder.add_root_certificate(root_cert);
            }
        }

        if let Some(client_identity) = ssl_opts.client_identity() {
            let pkcs12_path = client_identity.pkcs12_path();
            let password = client_identity.password().unwrap_or("");

            let der = std::fs::read(pkcs12_path)?;
            let identity = Identity::from_pkcs12(&*der, password)?;
            builder.identity(identity);
        }
        builder.danger_accept_invalid_hostnames(ssl_opts.skip_domain_validation());
        builder.danger_accept_invalid_certs(ssl_opts.accept_invalid_certs());
        let tls_connector: tokio_native_tls::TlsConnector = builder.build()?.into();

        *self = match self {
            Endpoint::Plain(ref mut stream) => {
                let stream = stream.take().unwrap();
                let tls_stream = tls_connector.connect(&*domain, stream).await?;
                Endpoint::Secure(tls_stream)
            }
            Endpoint::Secure(_) => unreachable!(),
            #[cfg(unix)]
            Endpoint::Socket(_) => unreachable!(),
        };

        Ok(())
    }
}
