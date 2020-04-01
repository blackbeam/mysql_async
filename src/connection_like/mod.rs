// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::{
    io::ReadMysqlExt,
    packets::{column_from_payload, parse_local_infile_packet, Column, OkPacket},
};
use tokio::prelude::*;

use std::{borrow::Cow, sync::Arc};

use crate::{
    conn::{stmt_cache::StmtCache, PendingResult},
    connection_like::{
        read_packet::{ReadPacket, ReadPackets},
        write_packet::WritePacket,
    },
    consts::{CapabilityFlags, Command, StatusFlags},
    error::*,
    io,
    local_infile_handler::LocalInfileHandler,
    queryable::{query_result::QueryResult, stmt::StmtInner, transaction::TxStatus, Protocol},
    BoxFuture, Opts,
};

pub mod read_packet;
pub mod write_packet;

pub trait ConnectionLike: Send + Sized {
    fn conn_mut(&mut self) -> &mut crate::Conn;
    fn connection_id(&self) -> u32;
    fn stream_mut(&mut self) -> &mut io::Stream;
    fn stmt_cache_ref(&self) -> &StmtCache;
    fn stmt_cache_mut(&mut self) -> &mut StmtCache;
    fn get_affected_rows(&self) -> u64;
    fn get_capabilities(&self) -> CapabilityFlags;
    fn get_tx_status(&self) -> TxStatus;
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
    fn set_tx_status(&mut self, tx_statux: TxStatus);
    fn set_pending_result(&mut self, meta: Option<PendingResult>);
    fn set_status(&mut self, status: StatusFlags);
    fn reset_seq_id(&mut self);
    fn sync_seq_id(&mut self);
    fn touch(&mut self) -> ();
    fn on_disconnect(&mut self);

    fn cache_stmt<'a>(&'a mut self, stmt: &Arc<StmtInner>) -> Option<Arc<StmtInner>> {
        let query = stmt.raw_query.clone();
        if self.get_opts().get_stmt_cache_size() > 0 {
            self.stmt_cache_mut().put(query, stmt.clone())
        } else {
            None
        }
    }

    fn get_cached_stmt(&mut self, raw_query: &str) -> Option<Arc<StmtInner>> {
        self.stmt_cache_mut()
            .by_query(raw_query)
            .map(|entry| entry.stmt.clone())
    }

    fn read_packet<'a>(&'a mut self) -> ReadPacket<'a, Self> {
        ReadPacket::new(self)
    }

    /// Returns future that reads packets from a server.
    fn read_packets<'a>(&'a mut self, n: usize) -> ReadPackets<'a, Self> {
        ReadPackets::new(self, n)
    }

    /// Returns future that reads result set from a server.
    fn read_result_set<'a, P>(&'a mut self) -> BoxFuture<'a, QueryResult<'a, Self, P>>
    where
        Self: Sized,
        P: Protocol,
    {
        BoxFuture(Box::pin(async move {
            let packet = self.read_packet().await?;
            match packet.get(0) {
                Some(0x00) => Ok(QueryResult::new(self, None)),
                Some(0xFB) => handle_local_infile(self, &*packet).await,
                _ => handle_result_set(self, &*packet).await,
            }
        }))
    }

    fn write_packet<T>(&mut self, data: T) -> WritePacket<'_, Self>
    where
        T: Into<Vec<u8>>,
    {
        WritePacket::new(self, data.into())
    }

    /// Returns future that sends full command body to a server.
    fn write_command_raw<'a>(&'a mut self, body: Vec<u8>) -> WritePacket<'a, Self> {
        assert!(body.len() > 0);
        self.reset_seq_id();
        self.write_packet(body)
    }

    /// Returns future that writes command to a server.
    fn write_command_data<T>(&mut self, cmd: Command, cmd_data: T) -> WritePacket<'_, Self>
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
    Ok(QueryResult::new(this, None))
}

/// Will handle result set packet.
async fn handle_result_set<'a, T: Sized, P>(
    this: &'a mut T,
    mut packet: &[u8],
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
        this.set_pending_result(Some(P::pending_result(columns.clone())));
        Ok(QueryResult::new(this, Some(columns)))
    } else {
        this.set_pending_result(Some(PendingResult::Empty));
        Ok(QueryResult::new(this, None))
    }
}
