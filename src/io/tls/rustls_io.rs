use std::sync::Arc;

use rustls::{
    client::{
        danger::{ServerCertVerified, ServerCertVerifier},
        WebPkiServerVerifier,
    },
    pki_types::{CertificateDer, ServerName},
    ClientConfig, RootCertStore,
};

use rustls_pemfile::certs;
pub(crate) use tokio_rustls::TlsConnector;

use crate::{io::Endpoint, Result, SslOpts, TlsError};

impl SslOpts {
    async fn load_root_certs(&self) -> crate::Result<Vec<CertificateDer<'static>>> {
        let mut output = Vec::new();

        for root_cert in self.root_certs() {
            let root_cert_data = root_cert.read().await?;
            let mut seen = false;
            for cert in certs(&mut &*root_cert_data) {
                seen = true;
                output.push(cert?);
            }

            if !seen && !root_cert_data.is_empty() {
                output.push(CertificateDer::from(root_cert_data.into_owned()));
            }
        }

        Ok(output)
    }

    pub(crate) async fn build_tls_connector(&self) -> Result<TlsConnector> {
        let mut root_store = RootCertStore::empty();
        if !self.disable_built_in_roots() {
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|x| x.to_owned()));
        }

        for cert in self.load_root_certs().await? {
            root_store.add(cert)?;
        }

        let config_builder = ClientConfig::builder().with_root_certificates(root_store.clone());

        let mut config = if let Some(identity) = self.client_identity() {
            let (cert_chain, priv_key) = identity.load().await?;
            config_builder.with_client_auth_cert(cert_chain, priv_key)?
        } else {
            config_builder.with_no_client_auth()
        };

        let mut dangerous = config.dangerous();
        let web_pki_verifier = WebPkiServerVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(TlsError::from)?;
        let dangerous_verifier = DangerousVerifier::new(
            self.accept_invalid_certs(),
            self.skip_domain_validation(),
            web_pki_verifier,
        );
        dangerous.set_certificate_verifier(Arc::new(dangerous_verifier));
        let client_config = Arc::new(config);
        Ok(TlsConnector::from(client_config))
    }
}

impl Endpoint {
    pub async fn make_secure(
        &mut self,
        domain: String,
        tls_connector: &TlsConnector,
    ) -> Result<()> {
        #[cfg(unix)]
        if self.is_socket() {
            // won't secure socket connection
            return Ok(());
        }

        *self = match self {
            Endpoint::Plain(ref mut stream) => {
                let stream = stream.take().unwrap();

                let server_name = ServerName::try_from(domain.as_str())
                    .map_err(|_| webpki::InvalidDnsNameError)?
                    .to_owned();
                let connection = tls_connector.connect(server_name, stream).await?;

                Endpoint::Secure(connection)
            }
            Endpoint::Secure(_) => unreachable!(),
            #[cfg(unix)]
            Endpoint::Socket(_) => unreachable!(),
        };

        Ok(())
    }
}

#[derive(Debug)]
struct DangerousVerifier {
    accept_invalid_certs: bool,
    skip_domain_validation: bool,
    verifier: Arc<WebPkiServerVerifier>,
}

impl DangerousVerifier {
    fn new(
        accept_invalid_certs: bool,
        skip_domain_validation: bool,
        verifier: Arc<WebPkiServerVerifier>,
    ) -> Self {
        Self {
            accept_invalid_certs,
            skip_domain_validation,
            verifier,
        }
    }
}

impl ServerCertVerifier for DangerousVerifier {
    // fn verify_server_cert(
    //     &self,
    //     end_entity: &Certificate,
    //     intermediates: &[Certificate],
    //     server_name: &rustls::ServerName,
    //     scts: &mut dyn Iterator<Item = &[u8]>,
    //     ocsp_response: &[u8],
    //     now: std::time::SystemTime,
    // ) -> std::result::Result<rustls::client::ServerCertVerified, rustls::Error> {
    //     if self.accept_invalid_certs {
    //         Ok(rustls::client::ServerCertVerified::assertion())
    //     } else {
    //         match self.verifier.verify_server_cert(
    //             end_entity,
    //             intermediates,
    //             server_name,
    //             scts,
    //             ocsp_response,
    //             now,
    //         ) {
    //             Ok(assertion) => Ok(assertion),
    //             Err(ref e)
    //                 if e.to_string().contains("NotValidForName") && self.skip_domain_validation =>
    //             {
    //                 Ok(rustls::client::ServerCertVerified::assertion())
    //             }
    //             Err(e) => Err(e),
    //         }
    //     }
    // }
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> std::prelude::v1::Result<ServerCertVerified, rustls::Error> {
        if self.accept_invalid_certs {
            Ok(ServerCertVerified::assertion())
        } else {
            match self.verifier.verify_server_cert(
                end_entity,
                intermediates,
                server_name,
                ocsp_response,
                now,
            ) {
                Ok(assertion) => Ok(assertion),
                Err(ref e)
                    if e.to_string().contains("NotValidForName") && self.skip_domain_validation =>
                {
                    Ok(ServerCertVerified::assertion())
                }
                Err(e) => Err(e),
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::prelude::v1::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
    {
        self.verifier.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::prelude::v1::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
    {
        self.verifier.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.verifier.supported_verify_schemes()
    }
}
