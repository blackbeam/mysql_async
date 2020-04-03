// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

pub(super) use self::get_conn::GetConnInner;
pub use self::{disconnect_pool::DisconnectPool, get_conn::GetConn};

mod disconnect_pool;
mod get_conn;
