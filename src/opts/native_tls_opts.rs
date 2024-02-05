#![cfg(feature = "native-tls")]

use std::{borrow::Cow, path::Path};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Pkcs12Archive {
    Path(Cow<'static, Path>),
    Data(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientIdentity {
    pkcs12_archive: Pkcs12Archive,
    password: Option<Cow<'static, str>>,
}

impl ClientIdentity {
    /// Creates new identity with the given path to the pkcs12 archive.
    pub fn new<T>(pkcs12_path: T) -> Self
    where
        T: Into<Cow<'static, Path>>,
    {
        Self {
            pkcs12_archive: Pkcs12Archive::Path(pkcs12_path.into()),
            password: None,
        }
    }

    /// Creates new identity with the given bytes for a pkcs12 archive.
    pub fn new_from_bytes(pkcs12_data: Vec<u8>) -> Self {
        Self {
            pkcs12_archive: Pkcs12Archive::Data(pkcs12_data),
            password: None,
        }
    }

    /// Sets the archive password.
    pub fn with_password<T>(mut self, pass: T) -> Self
    where
        T: Into<Cow<'static, str>>,
    {
        self.password = Some(pass.into());
        self
    }

    /// Returns the pkcs12 archive path.
    pub fn pkcs12_path(&self) -> &Path {
        match &self.pkcs12_archive {
            Pkcs12Archive::Path(path) => path.as_ref(),
            Pkcs12Archive::Data(_) => panic!("pkcs12 archive path is not set"),
        }
    }

    /// Returns the pkcs12 archive data, if set.
    pub fn pkcs12_data(&self) -> Option<&[u8]> {
        match &self.pkcs12_archive {
            Pkcs12Archive::Data(data) => Some(data.as_ref()),
            Pkcs12Archive::Path(_) => None,
        }
    }

    /// Returns the archive password.
    pub fn password(&self) -> Option<&str> {
        self.password.as_ref().map(AsRef::as_ref)
    }
}
