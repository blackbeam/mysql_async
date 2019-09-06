// Copyright (c) 2019 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_async::error::*;
use mysql_async::prelude::*;
use mysql_async::{Opts, Pool, QueryResult};

use std::env;
use std::io;

fn get_url() -> String {
    if let Ok(url) = env::var("DATABASE_URL") {
        let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
        if opts
            .get_db_name()
            .expect("a database name is required")
            .is_empty()
        {
            panic!("database name is empty");
        }
        url
    } else {
        "mysql://root:password@127.0.0.1:3307/mysql".into()
    }
}

pub async fn get_all_results<TupleType, T, P>(result: QueryResult<T, P>) -> Result<Vec<TupleType>>
where
    TupleType: FromRow + Send + 'static,
    P: Protocol + Send + 'static,
    T: ConnectionLike + Sized + Send + 'static,
{
    Ok(result.collect().await?.1)
}

pub async fn get_single_result<TupleType, T, P>(result: QueryResult<T, P>) -> Result<TupleType>
where
    TupleType: FromRow + Send + 'static,
    P: Protocol + Send + 'static,
    T: ConnectionLike + Sized + Send + 'static,
{
    let mut data = get_all_results(result).await?;
    if data.len() != 1 {
        Err(Error::from(io::Error::from(io::ErrorKind::InvalidData)))
    } else {
        Ok(data.remove(0))
    }
}

#[tokio::test]
async fn use_generic_code() {
    let pool = Pool::new(Opts::from_url(&*get_url()).unwrap());
    let conn = pool.get_conn().await.unwrap();
    let result = conn.query("SELECT 1, 2, 3").await.unwrap();
    let result = get_single_result::<(u8, u8, u8), _, _>(result)
        .await
        .unwrap();
    pool.disconnect().await.unwrap();
    assert_eq!(result, (1, 2, 3));
}
