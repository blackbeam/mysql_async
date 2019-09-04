// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use byteorder::{ByteOrder, LittleEndian};
use futures_util::future::ok;
use mysql_common::{
    io::ReadMysqlExt,
    packets::{column_from_payload, parse_local_infile_packet, Column, RawPacket},
};
use tokio::prelude::*;

use std::sync::Arc;

use crate::{
    conn::{named_params::parse_named_params, stmt_cache::StmtCache},
    connection_like::{read_packet::ReadPacket, streamless::Streamless, write_packet::WritePacket},
    consts::{CapabilityFlags, Command, StatusFlags},
    error::*,
    io,
    local_infile_handler::LocalInfileHandler,
    queryable::{
        query_result::{self, QueryResult},
        stmt::InnerStmt,
        Protocol,
    },
    BoxFuture, Opts,
};

pub mod read_packet;
pub mod streamless {
    use super::ConnectionLike;
    use crate::io::Stream;

    pub struct Streamless<T>(T);

    impl<T: ConnectionLike> Streamless<T> {
        pub fn new(x: T) -> Streamless<T> {
            Streamless(x)
        }

        pub fn return_stream(mut self, stream: Stream) -> T {
            self.0.return_stream(stream);
            self.0
        }
    }
}
pub mod write_packet;

#[derive(Debug)]
pub enum StmtCacheResult {
    Cached,
    NotCached(u32),
}

pub trait ConnectionLikeWrapper {
    type ConnLike: ConnectionLike;

    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized;
    fn return_stream(&mut self, stream: io::Stream) -> ();
    fn conn_like_ref(&self) -> &Self::ConnLike;

    fn conn_like_mut(&mut self) -> &mut Self::ConnLike;
}

impl<T, U> ConnectionLike for T
where
    T: ConnectionLikeWrapper<ConnLike = U>,
    T: Send,
    U: ConnectionLike + 'static,
{
    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized,
    {
        <Self as ConnectionLikeWrapper>::take_stream(self)
    }

    fn return_stream(&mut self, stream: io::Stream) {
        <Self as ConnectionLikeWrapper>::return_stream(self, stream)
    }

    fn stmt_cache_ref(&self) -> &StmtCache {
        self.conn_like_ref().stmt_cache_ref()
    }

    fn stmt_cache_mut(&mut self) -> &mut StmtCache {
        self.conn_like_mut().stmt_cache_mut()
    }

    fn get_affected_rows(&self) -> u64 {
        self.conn_like_ref().get_affected_rows()
    }

    fn get_capabilities(&self) -> CapabilityFlags {
        self.conn_like_ref().get_capabilities()
    }

    fn get_in_transaction(&self) -> bool {
        self.conn_like_ref().get_in_transaction()
    }

    fn get_last_command(&self) -> Command {
        self.conn_like_ref().get_last_command()
    }

    fn get_last_insert_id(&self) -> Option<u64> {
        self.conn_like_ref().get_last_insert_id()
    }

    fn get_local_infile_handler(&self) -> Option<Arc<dyn LocalInfileHandler>> {
        self.conn_like_ref().get_local_infile_handler()
    }

    fn get_max_allowed_packet(&self) -> u64 {
        self.conn_like_ref().get_max_allowed_packet()
    }

    fn get_opts(&self) -> &Opts {
        self.conn_like_ref().get_opts()
    }

    fn get_pending_result(&self) -> Option<&(Arc<Vec<Column>>, Option<StmtCacheResult>)> {
        self.conn_like_ref().get_pending_result()
    }

    fn get_server_version(&self) -> (u16, u16, u16) {
        self.conn_like_ref().get_server_version()
    }

    fn get_status(&self) -> StatusFlags {
        self.conn_like_ref().get_status()
    }

    fn get_seq_id(&self) -> u8 {
        self.conn_like_ref().get_seq_id()
    }

    fn set_affected_rows(&mut self, affected_rows: u64) {
        self.conn_like_mut().set_affected_rows(affected_rows);
    }

    fn set_in_transaction(&mut self, in_transaction: bool) {
        self.conn_like_mut().set_in_transaction(in_transaction);
    }

    fn set_last_command(&mut self, last_command: Command) {
        self.conn_like_mut().set_last_command(last_command);
    }

    fn set_last_insert_id(&mut self, last_insert_id: u64) {
        self.conn_like_mut().set_last_insert_id(last_insert_id);
    }

    fn set_pending_result(&mut self, meta: Option<(Arc<Vec<Column>>, Option<StmtCacheResult>)>) {
        self.conn_like_mut().set_pending_result(meta);
    }

    fn set_status(&mut self, status: StatusFlags) {
        self.conn_like_mut().set_status(status);
    }

    fn set_warnings(&mut self, warnings: u16) {
        self.conn_like_mut().set_warnings(warnings);
    }

    fn set_seq_id(&mut self, seq_id: u8) {
        self.conn_like_mut().set_seq_id(seq_id);
    }

    fn touch(&mut self) {
        self.conn_like_mut().touch();
    }

    fn on_disconnect(&mut self) {
        self.conn_like_mut().on_disconnect();
    }
}

pub trait ConnectionLike: Send {
    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized;
    fn return_stream(&mut self, stream: io::Stream) -> ();
    fn stmt_cache_ref(&self) -> &StmtCache;
    fn stmt_cache_mut(&mut self) -> &mut StmtCache;
    fn get_affected_rows(&self) -> u64;
    fn get_capabilities(&self) -> CapabilityFlags;
    fn get_in_transaction(&self) -> bool;
    fn get_last_command(&self) -> Command;
    fn get_last_insert_id(&self) -> Option<u64>;
    fn get_local_infile_handler(&self) -> Option<Arc<dyn LocalInfileHandler>>;
    fn get_max_allowed_packet(&self) -> u64;
    fn get_opts(&self) -> &Opts;
    fn get_pending_result(&self) -> Option<&(Arc<Vec<Column>>, Option<StmtCacheResult>)>;
    fn get_server_version(&self) -> (u16, u16, u16);
    fn get_status(&self) -> StatusFlags;
    fn get_seq_id(&self) -> u8;
    fn set_affected_rows(&mut self, affected_rows: u64);
    fn set_in_transaction(&mut self, in_transaction: bool);
    fn set_last_command(&mut self, last_command: Command);
    fn set_last_insert_id(&mut self, last_insert_id: u64);
    fn set_pending_result(&mut self, meta: Option<(Arc<Vec<Column>>, Option<StmtCacheResult>)>);
    fn set_status(&mut self, status: StatusFlags);
    fn set_warnings(&mut self, warnings: u16);
    fn set_seq_id(&mut self, seq_id: u8);
    fn touch(&mut self) -> ();
    fn on_disconnect(&mut self);

    fn cache_stmt(mut self, query: String, stmt: &InnerStmt) -> BoxFuture<(Self, StmtCacheResult)>
    where
        Self: Sized + 'static,
    {
        if self.get_opts().get_stmt_cache_size() > 0 {
            if let Some(old_stmt) = self.stmt_cache_mut().put(query, stmt.clone()) {
                let f = self.close_stmt(old_stmt.statement_id);
                Box::pin(async move { Ok((f.await?, StmtCacheResult::Cached)) })
            } else {
                Box::pin(futures_util::future::ok((self, StmtCacheResult::Cached)))
            }
        } else {
            Box::pin(futures_util::future::ok((
                self,
                StmtCacheResult::NotCached(stmt.statement_id),
            )))
        }
    }

    fn get_cached_stmt(&mut self, query: &str) -> Option<&InnerStmt> {
        self.stmt_cache_mut().get(query)
    }

    /// Returns future that reads packet from a server end resolves to `(Self, Packet)`.
    fn read_packet(self) -> ReadPacket<Self>
    where
        Self: Sized + 'static,
    {
        ReadPacket::new(self)
    }

    /// Returns future that reads packets from a server and resolves to `(Self, Vec<Packet>)`.
    fn read_packets(self, n: usize) -> BoxFuture<(Self, Vec<RawPacket>)>
    where
        Self: Sized + 'static,
    {
        if n == 0 {
            return Box::pin(ok((self, Vec::new())));
        }
        Box::pin(async move {
            let mut acc = Vec::new();
            let mut conn_like = self;
            for _ in 0..n {
                let (cl, packet) = conn_like.read_packet().await?;
                conn_like = cl;
                acc.push(packet);
            }
            Ok((conn_like, acc))
        })
    }

    fn prepare_stmt<Q>(mut self, query: Q) -> BoxFuture<(Self, InnerStmt, StmtCacheResult)>
    where
        Q: AsRef<str>,
        Self: Sized + 'static,
    {
        match parse_named_params(query.as_ref()) {
            Ok((named_params, query)) => {
                let query = query.into_owned();
                if let Some(mut inner_stmt) = self.get_cached_stmt(&query).map(Clone::clone) {
                    inner_stmt.named_params = named_params.clone();
                    Box::pin(ok((self, inner_stmt, StmtCacheResult::Cached)))
                } else {
                    Box::pin(async move {
                        let (this, packet) = self
                            .write_command_data(Command::COM_STMT_PREPARE, &*query)
                            .await?
                            .read_packet()
                            .await?;
                        let mut inner_stmt = InnerStmt::new(&*packet.0, named_params)?;
                        let (mut this, packets) =
                            this.read_packets(inner_stmt.num_params as usize).await?;
                        if !packets.is_empty() {
                            let params = packets
                                .into_iter()
                                .map(|packet| column_from_payload(packet.0).map_err(Error::from))
                                .collect::<Result<Vec<Column>>>()?;
                            inner_stmt.params = Some(params);
                        }

                        if inner_stmt.num_params > 0 {
                            if !this
                                .get_capabilities()
                                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
                            {
                                this = this.read_packet().await?.0;
                            }
                        }

                        let (mut this, packets) =
                            this.read_packets(inner_stmt.num_columns as usize).await?;
                        if !packets.is_empty() {
                            let columns = packets
                                .into_iter()
                                .map(|packet| column_from_payload(packet.0).map_err(Error::from))
                                .collect::<Result<Vec<Column>>>()?;
                            inner_stmt.columns = Some(columns);
                        }

                        if inner_stmt.num_columns > 0 {
                            if !this
                                .get_capabilities()
                                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
                            {
                                this = this.read_packet().await?.0;
                            }
                        }

                        let (this, stmt_cache_result) = this.cache_stmt(query, &inner_stmt).await?;
                        Ok((this, inner_stmt, stmt_cache_result))
                    })
                }
            }
            Err(err) => Box::pin(async move { Err(Error::from(err)) }),
        }
    }

    fn close_stmt(self, statement_id: u32) -> WritePacket<Self>
    where
        Self: Sized + 'static,
    {
        let mut stmt_id = [0; 4];
        LittleEndian::write_u32(&mut stmt_id[..], statement_id);
        self.write_command_data(Command::COM_STMT_CLOSE, &stmt_id[..])
    }

    /// Returns future that reads result set from a server and resolves to `QueryResult`.
    fn read_result_set<P>(self, cached: Option<StmtCacheResult>) -> BoxFuture<QueryResult<Self, P>>
    where
        Self: Sized + 'static,
        P: Protocol,
        P: Send + 'static,
    {
        Box::pin(async move {
            let (this, packet) = self.read_packet().await?;
            match packet.0[0] {
                0x00 => Ok(query_result::new(this, None, cached)),
                0xFB => handle_local_infile(this, packet, cached).await,
                _ => handle_result_set(this, packet, cached).await,
            }
        })
    }

    /// Returns future that writes packet to a server end resolves to `Self`.
    fn write_packet<T>(self, data: T) -> WritePacket<Self>
    where
        T: Into<Vec<u8>>, // TODO: Switch to `AsRef<u8> + 'static`?
        Self: Sized + 'static,
    {
        WritePacket::new(self, data)
    }

    /// Returns future that writes command to a server end resolves to `Self`.
    fn write_command_data<T>(mut self, cmd: Command, cmd_data: T) -> WritePacket<Self>
    where
        Self: Sized + 'static,
        T: AsRef<[u8]>,
    {
        let mut data = Vec::with_capacity(1 + cmd_data.as_ref().len());
        data.push(cmd as u8);
        data.extend_from_slice(cmd_data.as_ref());
        self.set_seq_id(0);
        self.write_packet(data)
    }
}

/// Will handle local infile packet.
async fn handle_local_infile<T, P>(
    mut this: T,
    packet: RawPacket,
    cached: Option<StmtCacheResult>,
) -> Result<QueryResult<T, P>>
where
    P: Protocol + 'static,
    T: ConnectionLike,
    T: Send + Sized + 'static,
{
    let local_infile = parse_local_infile_packet(&*packet.0)?;
    let (local_infile, handler) = match this.get_local_infile_handler() {
        Some(handler) => ((local_infile.into_owned(), handler)),
        None => return Err(DriverError::NoLocalInfileHandler.into()),
    };
    let mut reader = handler.handle(local_infile.file_name_ref()).await?;

    let mut buf = [0; 4096];
    loop {
        let read = reader.read(&mut buf[..]).await?;
        this = this.write_packet(&buf[..read]).await?;

        if read == 0 {
            break;
        }
    }

    let (this, _) = this.read_packet().await?;
    Ok(query_result::new(this, None, cached))
}

/// Will handle result set packet.
async fn handle_result_set<T, P>(
    this: T,
    packet: RawPacket,
    cached: Option<StmtCacheResult>,
) -> Result<QueryResult<T, P>>
where
    P: Protocol,
    P: Send + 'static,
    T: ConnectionLike,
    T: Send + Sized + 'static,
{
    let column_count = (&*packet.0).read_lenenc_int()?;
    let (mut this, packets) = this.read_packets(column_count as usize).await?;
    let columns = packets
        .into_iter()
        .map(|packet| column_from_payload(packet.0).map_err(Error::from))
        .collect::<Result<Vec<Column>>>()?;

    if !this
        .get_capabilities()
        .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
    {
        this = this.read_packet().await?.0;
    }

    let columns = Arc::new(columns);
    this.set_pending_result(Some((Clone::clone(&columns), None)));
    Ok(query_result::new(this, Some(columns), cached))
}
