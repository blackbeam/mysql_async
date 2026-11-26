use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{opts::Opts, OptsBuilder};

/// TCP proxy to the test server whose established links can be blackholed.
pub(crate) struct Proxy {
    addr: SocketAddr,
    links: Arc<Mutex<Vec<Arc<AtomicBool>>>>,
}

impl Proxy {
    /// Starts forwarding every accepted connection to the server `opts` points at.
    pub(crate) async fn start(opts: &OptsBuilder) -> Self {
        let opts = Opts::from(opts.clone());
        let upstream = format!("{}:{}", opts.ip_or_hostname(), opts.tcp_port());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let links = Arc::new(Mutex::new(Vec::new()));
        let accepted = links.clone();
        tokio::spawn(async move {
            loop {
                let (client, _) = listener.accept().await.unwrap();
                let server = TcpStream::connect(&upstream).await.unwrap();
                let blackhole = Arc::new(AtomicBool::new(false));
                accepted.lock().unwrap().push(blackhole.clone());
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                tokio::spawn(pump(client_read, server_write, blackhole.clone()));
                tokio::spawn(pump(server_read, client_write, blackhole));
            }
        });
        Self { addr, links }
    }

    /// Returns `opts` redirected through this proxy over TCP.
    pub(crate) fn opts(&self, opts: OptsBuilder) -> OptsBuilder {
        opts.prefer_socket(false)
            .ip_or_hostname(self.addr.ip().to_string())
            .tcp_port(self.addr.port())
    }

    /// Stops forwarding on every established link in both directions while keeping its sockets
    /// open, like a half-open peer. Later connections are forwarded normally.
    pub(crate) fn blackhole_links(&self) {
        for blackhole in self.links.lock().unwrap().iter() {
            blackhole.store(true, Ordering::SeqCst);
        }
    }
}

async fn pump(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    blackhole: Arc<AtomicBool>,
) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let read = from.read(&mut buf).await;
        if blackhole.load(Ordering::SeqCst) {
            let _sockets = (from, to);
            return std::future::pending().await;
        }
        let n = match read {
            Ok(0) | Err(_) => {
                let _ = to.shutdown().await;
                return;
            }
            Ok(n) => n,
        };
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}
