// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use tokio::fs::File;

use std::{collections::HashSet, path::PathBuf, str::from_utf8};

use crate::local_infile_handler::LocalInfileHandler;

/// Handles local infile requests from filesystem using explicit path white list.
///
/// Example usage:
///
/// ```rust
/// use mysql_async::{OptsBuilder, WhiteListFsLocalInfileHandler};
///
/// # let database_url = "mysql://root:password@127.0.0.1:3307/mysql";
/// let mut opts = OptsBuilder::from_opts(database_url);
/// opts.local_infile_handler(Some(WhiteListFsLocalInfileHandler::new(
///     &["path/to/local_infile.txt"][..],
/// )));
/// ```
#[derive(Clone, Debug)]
pub struct WhiteListFsLocalInfileHandler {
    white_list: HashSet<PathBuf>,
}

impl WhiteListFsLocalInfileHandler {
    pub fn new<A, B>(white_list: B) -> WhiteListFsLocalInfileHandler
    where
        A: Into<PathBuf>,
        B: IntoIterator<Item = A>,
    {
        let mut white_list_set = HashSet::new();
        for path in white_list.into_iter() {
            white_list_set.insert(Into::<PathBuf>::into(path));
        }
        WhiteListFsLocalInfileHandler {
            white_list: white_list_set,
        }
    }
}

impl LocalInfileHandler for WhiteListFsLocalInfileHandler {
    fn handle(&self, file_name: &[u8]) -> super::InfileHandlerFuture {
        let path: PathBuf = match from_utf8(file_name) {
            Ok(path_str) => path_str.into(),
            Err(_) => return Box::pin(futures_util::future::err("Invalid file name".into())),
        };

        if !self.white_list.contains(&path) {
            let err_msg = format!("Path `{}' is not in white list", path.display());
            return Box::pin(futures_util::future::err(err_msg.into()));
        }

        Box::pin(async move { Ok(Box::new(File::open(path.to_owned()).await?) as Box<_>) })
    }
}
