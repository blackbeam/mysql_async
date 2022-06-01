[![Gitter](https://badges.gitter.im/rust-mysql/community.svg)](https://gitter.im/rust-mysql/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

[![Build Status](https://dev.azure.com/aikorsky/mysql%20Rust/_apis/build/status/blackbeam.mysql_async?branchName=master)](https://dev.azure.com/aikorsky/mysql%20Rust/_build/latest?definitionId=2&branchName=master)
[![](https://meritbadge.herokuapp.com/mysql_async)](https://crates.io/crates/mysql_async)
[![](https://img.shields.io/crates/d/mysql_async.svg)](https://crates.io/crates/mysql_async)
[![API Documentation on docs.rs](https://docs.rs/mysql_async/badge.svg)](https://docs.rs/mysql_async)

# mysql_async

Tokio based asynchronous MySql client library for The Rust Programming Language.

## Installation

The library is hosted on [crates.io](https://crates.io/crates/mysql_async/).

```toml
[dependencies]
mysql_async = "<desired version>"
```

## Example

```rust
use mysql_async::prelude::*;

#[derive(Debug, PartialEq, Eq, Clone)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let payments = vec![
        Payment { customer_id: 1, amount: 2, account_name: None },
        Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
        Payment { customer_id: 5, amount: 6, account_name: None },
        Payment { customer_id: 7, amount: 8, account_name: None },
        Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
    ];

    let database_url = /* ... */
    # get_opts();

    let pool = mysql_async::Pool::new(database_url);
    let mut conn = pool.get_conn().await?;

    // Create a temporary table
    r"CREATE TEMPORARY TABLE payment (
        customer_id int not null,
        amount int not null,
        account_name text
    )".ignore(&mut conn).await?;

    // Save payments
    r"INSERT INTO payment (customer_id, amount, account_name)
      VALUES (:customer_id, :amount, :account_name)"
        .with(payments.iter().map(|payment| params! {
            "customer_id" => payment.customer_id,
            "amount" => payment.amount,
            "account_name" => payment.account_name.as_ref(),
        }))
        .batch(&mut conn)
        .await?;

    // Load payments from the database. Type inference will work here.
    let loaded_payments = "SELECT customer_id, amount, account_name FROM payment"
        .with(())
        .map(&mut conn, |(customer_id, amount, account_name)| Payment { customer_id, amount, account_name })
        .await?;

    // Dropped connection will go to the pool
    drop(conn);

    // The Pool must be disconnected explicitly because
    // it's an asynchronous operation.
    pool.disconnect().await?;

    assert_eq!(loaded_payments, payments);

    // the async fn returns Result, so
    Ok(())
}
```

## Pool

The [`Pool`] structure is an asynchronous connection pool.

Please note:

* [`Pool`] is a smart pointer – each clone will point to the same pool instance.
* [`Pool`] is `Send + Sync + 'static` – feel free to pass it around.
* use [`Pool::disconnect`] to gracefuly close the pool.
* [`Pool::new`] is lazy and won't assert server availability.

## LOCAL INFILE Handlers

**Warning:** You should be aware of [Security Considerations for LOAD DATA LOCAL][1].

There are two flavors of LOCAL INFILE handlers – _global_ and _local_.

I case of a LOCAL INFILE request from the server the driver will try to find a handler for it:

1.  It'll try to use _local_ handler installed on the connection, if any;
2.  It'll try to use _global_ handler, specified via [`OptsBuilder::local_infile_handler`],
    if any;
3.  It will emit [`LocalInfileError::NoHandler`] if no handlers found.

The purpose of a handler (_local_ or _global_) is to return [`InfileData`].

### _Global_ LOCAL INFILE handler

See [`prelude::GlobalHandler`].

Simply speaking the _global_ handler is an async function that takes a file name (as `&[u8]`)
and returns `Result<InfileData>`.

You can set it up using [`OptsBuilder::local_infile_handler`]. Server will use it if there is no
_local_ handler installed for the connection. This handler might be called multiple times.

Examles:

1.  [`WhiteListFsHandler`] is a _global_ handler.
2.  Every `T: Fn(&[u8]) -> BoxFuture<'static, Result<InfileData, LocalInfileError>>`
    is a _global_ handler.

### _Local_ LOCAL INFILE handler.

Simply speaking the _local_ handler is a future, that returns `Result<InfileData>`.

This is a one-time handler – it's consumed after use. You can set it up using
[`Conn::set_infile_handler`]. This handler have priority over _global_ handler.

Worth noting:

1.  `impl Drop for Conn` will clear _local_ handler, i.e. handler will be removed when
    connection is returned to a `Pool`.
2.  [`Conn::reset`] will clear _local_ handler.

Example:

```rust
#
let pool = mysql_async::Pool::new(database_url);

let mut conn = pool.get_conn().await?;
"CREATE TEMPORARY TABLE tmp (id INT, val TEXT)".ignore(&mut conn).await?;

// We are going to call `LOAD DATA LOCAL` so let's setup a one-time handler.
conn.set_infile_handler(async move {
    // We need to return a stream of `io::Result<Bytes>`
    Ok(stream::iter([Bytes::from("1,a\r\n"), Bytes::from("2,b\r\n3,c")]).map(Ok).boxed())
});

let result = r#"LOAD DATA LOCAL INFILE 'whatever'
    INTO TABLE `tmp`
    FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
    LINES TERMINATED BY '\r\n'"#.ignore(&mut conn).await;

match result {
    Ok(()) => (),
    Err(Error::Server(ref err)) if err.code == 1148 => {
        // The used command is not allowed with this MySQL version
        return Ok(());
    },
    Err(Error::Server(ref err)) if err.code == 3948 => {
        // Loading local data is disabled;
        // this must be enabled on both the client and the server
        return Ok(());
    }
    e @ Err(_) => e.unwrap(),
}

// Now let's verify the result
let result: Vec<(u32, String)> = conn.query("SELECT * FROM tmp ORDER BY id ASC").await?;
assert_eq!(
    result,
    vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]
);

drop(conn);
pool.disconnect().await?;
```

[1]: https://dev.mysql.com/doc/refman/8.0/en/load-data-local-security.html

## Testing

Tests uses followin environment variables:
* `DATABASE_URL` – defaults to `mysql://root:password@127.0.0.1:3307/mysql`
* `COMPRESS` – set to `1` or `true` to enable compression for tests
* `SSL` – set to `1` or `true` to enable TLS for tests

You can run a test server using doker. Please note that params related
to max allowed packet, local-infile and binary logging are required
to properly run tests:

```sh
docker run -d --name container \
    -v `pwd`:/root \
    -p 3307:3306 \
    -e MYSQL_ROOT_PASSWORD=password \
    mysql:8.0 \
    --max-allowed-packet=36700160 \
    --local-infile \
    --log-bin=mysql-bin \
    --log-slave-updates \
    --gtid_mode=ON \
    --enforce_gtid_consistency=ON \
    --server-id=1
```


## Change log

Available [here](https://github.com/blackbeam/mysql_async/releases)

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
