//! Private routine helpers.

use std::sync::Arc;

use mysql_common::{
    constants::MAX_PAYLOAD_LEN,
    io::{ParseBuf, ReadMysqlExt},
    packets::{ComStmtSendLongData, LocalInfilePacket},
    value::Value,
};
use tokio::io::AsyncReadExt;

use crate::{queryable::Protocol, Conn, DriverError, Error};

impl Conn {
    /// Helper, that sends all `Value::Bytes` in the given list of paramenters as long data.
    pub(super) async fn send_long_data<'a, I>(
        &mut self,
        statement_id: u32,
        params: I,
    ) -> crate::Result<()>
    where
        I: Iterator<Item = &'a Value>,
    {
        for (i, value) in params.enumerate() {
            if let Value::Bytes(bytes) = value {
                let chunks = bytes.chunks(MAX_PAYLOAD_LEN - 6);
                let chunks = chunks.chain(if bytes.is_empty() {
                    Some(&[][..])
                } else {
                    None
                });
                for chunk in chunks {
                    let com = ComStmtSendLongData::new(statement_id, i as u16, chunk);
                    self.write_command(&com).await?;
                }
            }
        }

        Ok(())
    }

    /// Will read result set and write pending result into `self` (if any).
    pub(super) async fn read_result_set<P>(
        &mut self,
        is_first_result_set: bool,
    ) -> crate::Result<()>
    where
        P: Protocol,
    {
        let packet = match self.read_packet().await {
            Ok(packet) => packet,
            Err(err @ Error::Server(_)) if is_first_result_set => {
                // shortcut to emit an error right to the caller of a query/execute
                return Err(err);
            }
            Err(Error::Server(error)) => {
                // error will be consumed as a part of a multi-result set
                self.set_pending_result_error(error)?;
                return Ok(());
            }
            Err(err) => {
                // non-server errors are fatal
                return Err(err);
            }
        };

        match packet.get(0) {
            Some(0x00) => {
                self.set_pending_result(Some(P::result_set_meta(Arc::from(
                    Vec::new().into_boxed_slice(),
                ))))?;
            }
            Some(0xFB) => self.handle_local_infile::<P>(&*packet).await?,
            _ => self.handle_result_set::<P>(&*packet).await?,
        }

        Ok(())
    }

    /// Will handle local infile packet.
    pub(super) async fn handle_local_infile<P>(&mut self, packet: &[u8]) -> crate::Result<()>
    where
        P: Protocol,
    {
        let local_infile = ParseBuf(packet).parse::<LocalInfilePacket>(())?;
        let (local_infile, handler) = match self.opts().local_infile_handler() {
            Some(handler) => ((local_infile.into_owned(), handler)),
            None => return Err(DriverError::NoLocalInfileHandler.into()),
        };
        let mut reader = handler.handle(local_infile.file_name_ref()).await?;

        let mut buf = [0; 4096];
        loop {
            let read = reader.read(&mut buf[..]).await?;
            self.write_bytes(&buf[..read]).await?;

            if read == 0 {
                break;
            }
        }

        self.read_packet().await?;
        self.set_pending_result(Some(P::result_set_meta(Arc::from(
            Vec::new().into_boxed_slice(),
        ))))?;
        Ok(())
    }

    /// Helper that handles result set packet.
    ///
    /// Requires that `packet` contains non-zero length-encoded integer.
    pub(super) async fn handle_result_set<P>(&mut self, mut packet: &[u8]) -> crate::Result<()>
    where
        P: Protocol,
    {
        let column_count = packet.read_lenenc_int()?;
        let columns = self.read_column_defs(column_count as usize).await?;
        let meta = P::result_set_meta(Arc::from(columns.into_boxed_slice()));
        self.set_pending_result(Some(meta))?;
        Ok(())
    }
}
