use Conn;
use Column;

use errors::*;

use lib_futures::{
    Async,
    Future,
    Poll,
};
use lib_futures::Async::Ready;

use proto::{
    OkPacket,
    Packet,
    PacketType,
    read_lenenc_int,
};

use super::{
    Columns,
    TextQueryResult,
    ReadPacket,

    new_text_query_result,
};

enum Step {
    ReadPacket(ReadPacket),
    ReadColumns(Columns),
}

enum Out {
    ReadPacket((Conn, Packet)),
    ReadColumns((Conn, Vec<Column>)),
}

pub struct NewTextQueryResult {
    step: Step,
    is_bin: bool,
}

pub fn new(read_packet: ReadPacket, is_bin: bool) -> NewTextQueryResult {
    NewTextQueryResult {
        step: Step::ReadPacket(read_packet),
        is_bin: is_bin,
    }
}

impl NewTextQueryResult {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::ReadPacket(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadPacket(val)))
            },
            Step::ReadColumns(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadColumns(val)))
            },
        }
    }
}

impl Future for NewTextQueryResult {
    type Item = TextQueryResult;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::ReadPacket((conn, packet)) => if packet.is(PacketType::Ok) {
                let ok_packet = OkPacket::new(packet, conn.capabilities);
                let query_result = new_text_query_result(conn, vec![], ok_packet, false);
                Ok(Async::Ready(query_result))
            } else {
                let column_count = try!(read_lenenc_int(&mut packet.as_ref()));
                self.step = Step::ReadColumns(conn.read_result_set_columns(column_count));
                self.poll()
            },
            Out::ReadColumns((conn, columns)) => {
                let query_result = new_text_query_result(conn, columns, None, self.is_bin);
                Ok(Async::Ready(query_result))
            },
        }
    }
}
