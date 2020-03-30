// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::conn::PendingResult;
use crate::connection_like::write_packet2::WritePacket2;
use futures_util::future::ok;
use mysql_common::{
    io::ReadMysqlExt,
    packets::{column_from_payload, parse_local_infile_packet, Column, ComStmtClose, OkPacket},
};
use tokio::prelude::*;

use std::{borrow::Cow, sync::Arc};

use crate::{
    conn::{named_params::parse_named_params, stmt_cache::StmtCache},
    connection_like::read_packet::{ReadPacket, ReadPackets},
    consts::{CapabilityFlags, Command, StatusFlags},
    error::*,
    io,
    local_infile_handler::LocalInfileHandler,
    queryable::{query_result::QueryResult, stmt::InnerStmt, Protocol},
    BoxFuture, Opts,
};

pub mod read_packet;
pub mod write_packet2;

#[derive(Debug, Clone, Copy)]
pub enum StmtCacheResult {
    Cached,
    NotCached(u32),
}

pub trait ConnectionLike: Send + Sized {
    fn stream_mut(&mut self) -> &mut io::Stream;
    fn stmt_cache_ref(&self) -> &StmtCache;
    fn stmt_cache_mut(&mut self) -> &mut StmtCache;
    fn get_affected_rows(&self) -> u64;
    fn get_capabilities(&self) -> CapabilityFlags;
    fn get_in_transaction(&self) -> bool;
    fn get_last_insert_id(&self) -> Option<u64>;
    fn get_info(&self) -> Cow<'_, str>;
    fn get_warnings(&self) -> u16;
    fn get_local_infile_handler(&self) -> Option<Arc<dyn LocalInfileHandler>>;
    fn get_max_allowed_packet(&self) -> usize;
    fn get_opts(&self) -> &Opts;
    fn get_pending_result(&self) -> Option<&PendingResult>;
    fn get_server_version(&self) -> (u16, u16, u16);
    fn get_status(&self) -> StatusFlags;
    fn set_last_ok_packet(&mut self, ok_packet: Option<OkPacket<'static>>);
    fn set_in_transaction(&mut self, in_transaction: bool);
    fn set_pending_result(&mut self, meta: Option<PendingResult>);
    fn set_status(&mut self, status: StatusFlags);
    fn reset_seq_id(&mut self);
    fn sync_seq_id(&mut self);
    fn touch(&mut self) -> ();
    fn on_disconnect(&mut self);

    fn cache_stmt<'a>(
        &'a mut self,
        query: String,
        stmt: &InnerStmt,
    ) -> BoxFuture<'a, StmtCacheResult>
    where
        Self: Sized,
    {
        if self.get_opts().get_stmt_cache_size() > 0 {
            if let Some(old_stmt) = self.stmt_cache_mut().put(query, stmt.clone()) {
                Box::pin(async move {
                    self.close_stmt(old_stmt.statement_id).await?;
                    Ok(StmtCacheResult::Cached)
                })
            } else {
                Box::pin(futures_util::future::ok(StmtCacheResult::Cached))
            }
        } else {
            Box::pin(futures_util::future::ok(StmtCacheResult::NotCached(
                stmt.statement_id,
            )))
        }
    }

    fn get_cached_stmt(&mut self, query: &str) -> Option<&InnerStmt> {
        self.stmt_cache_mut().get(query)
    }

    fn read_packet<'a>(&'a mut self) -> ReadPacket<'a, Self> {
        ReadPacket::new(self)
    }

    /// Returns future that reads packets from a server and resolves to `(Self, Vec<Packet>)`.
    fn read_packets<'a>(&'a mut self, n: usize) -> ReadPackets<'a, Self> {
        ReadPackets::new(self, n)
    }

    fn prepare_stmt<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, (InnerStmt, StmtCacheResult)>
    where
        Q: AsRef<str>,
        Self: Sized,
    {
        match parse_named_params(query.as_ref()) {
            Ok((named_params, query)) => {
                let query = query.into_owned();
                if let Some(mut inner_stmt) = self.get_cached_stmt(&query).map(Clone::clone) {
                    inner_stmt.named_params = named_params.clone();
                    Box::pin(ok((inner_stmt, StmtCacheResult::Cached)))
                } else {
                    Box::pin(async move {
                        self.write_command_data(Command::COM_STMT_PREPARE, &*query)
                            .await?;
                        let packet = self.read_packet().await?;
                        let mut inner_stmt = InnerStmt::new(&*packet, named_params)?;
                        let packets = self.read_packets(inner_stmt.num_params as usize).await?;
                        if !packets.is_empty() {
                            let params = packets
                                .into_iter()
                                .map(|packet| column_from_payload(packet).map_err(Error::from))
                                .collect::<Result<Vec<Column>>>()?;
                            inner_stmt.params = Some(params);
                        }

                        if inner_stmt.num_params > 0 {
                            if !self
                                .get_capabilities()
                                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
                            {
                                self.read_packet().await?;
                            }
                        }

                        let packets = self.read_packets(inner_stmt.num_columns as usize).await?;
                        if !packets.is_empty() {
                            let columns = packets
                                .into_iter()
                                .map(|packet| column_from_payload(packet).map_err(Error::from))
                                .collect::<Result<Vec<Column>>>()?;
                            inner_stmt.columns = Some(columns);
                        }

                        if inner_stmt.num_columns > 0 {
                            if !self
                                .get_capabilities()
                                .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
                            {
                                self.read_packet().await?;
                            }
                        }

                        let stmt_cache_result = self.cache_stmt(query, &inner_stmt).await?;
                        Ok((inner_stmt, stmt_cache_result))
                    })
                }
            }
            Err(err) => Box::pin(async move { Err(Error::from(err)) }),
        }
    }

    fn close_stmt<'a>(&'a mut self, statement_id: u32) -> WritePacket2<'a, Self> {
        self.write_command_raw(ComStmtClose::new(statement_id).into())
    }

    /// Returns future that reads result set from a server and resolves to `QueryResult`.
    fn read_result_set<'a, P>(
        &'a mut self,
        cached: Option<StmtCacheResult>,
    ) -> BoxFuture<'a, QueryResult<'a, Self, P>>
    where
        Self: Sized,
        P: Protocol,
    {
        Box::pin(async move {
            let packet = self.read_packet().await?;
            match packet.get(0) {
                Some(0x00) => Ok(QueryResult::new(self, None, cached)),
                Some(0xFB) => handle_local_infile(self, &*packet, cached).await,
                _ => handle_result_set(self, &*packet, cached).await,
            }
        })
    }

    fn write_packet<T>(&mut self, data: T) -> WritePacket2<'_, Self>
    where
        T: Into<Vec<u8>>,
    {
        WritePacket2::new(self, data.into())
    }

    /// Returns future that sends full command body to a server and resolves to `Self`.
    fn write_command_raw<'a>(&'a mut self, body: Vec<u8>) -> WritePacket2<'a, Self> {
        assert!(body.len() > 0);
        self.reset_seq_id();
        self.write_packet(body)
    }

    /// Returns future that writes command to a server and resolves to `Self`.
    fn write_command_data<T>(&mut self, cmd: Command, cmd_data: T) -> WritePacket2<'_, Self>
    where
        T: AsRef<[u8]>,
    {
        let cmd_data = cmd_data.as_ref();
        let mut body = Vec::with_capacity(1 + cmd_data.len());
        body.push(cmd as u8);
        body.extend_from_slice(cmd_data);
        self.write_command_raw(body)
    }
}

/// Will handle local infile packet.
async fn handle_local_infile<'a, T: ?Sized, P>(
    this: &'a mut T,
    packet: &[u8],
    cached: Option<StmtCacheResult>,
) -> Result<QueryResult<'a, T, P>>
where
    P: Protocol,
    T: ConnectionLike + Sized,
{
    let local_infile = parse_local_infile_packet(&*packet)?;
    let (local_infile, handler) = match this.get_local_infile_handler() {
        Some(handler) => ((local_infile.into_owned(), handler)),
        None => return Err(DriverError::NoLocalInfileHandler.into()),
    };
    let mut reader = handler.handle(local_infile.file_name_ref()).await?;

    let mut buf = [0; 4096];
    loop {
        let read = reader.read(&mut buf[..]).await?;
        this.write_packet(&buf[..read]).await?;

        if read == 0 {
            break;
        }
    }

    this.read_packet().await?;
    Ok(QueryResult::new(this, None, cached))
}

/// Will handle result set packet.
async fn handle_result_set<'a, T: Sized, P>(
    this: &'a mut T,
    mut packet: &[u8],
    cached: Option<StmtCacheResult>,
) -> Result<QueryResult<'a, T, P>>
where
    P: Protocol,
    T: ConnectionLike,
{
    let column_count = packet.read_lenenc_int()?;
    let packets = this.read_packets(column_count as usize).await?;
    let columns = packets
        .into_iter()
        .map(|packet| column_from_payload(packet).map_err(Error::from))
        .collect::<Result<Vec<Column>>>()?;

    if !this
        .get_capabilities()
        .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF)
    {
        this.read_packet().await?;
    }

    if column_count > 0 {
        let columns = Arc::new(columns);
        match cached {
            Some(cached) => {
                this.set_pending_result(Some(PendingResult::Binary(columns.clone(), cached)))
            }
            None => this.set_pending_result(Some(PendingResult::Text(columns.clone()))),
        }
        Ok(QueryResult::new(this, Some(columns), cached))
    } else {
        this.set_pending_result(Some(PendingResult::Empty));
        Ok(QueryResult::new(this, None, cached))
    }
}
