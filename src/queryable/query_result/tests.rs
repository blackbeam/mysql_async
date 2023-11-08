#![cfg(test)]

use futures_util::TryStreamExt;

use crate::{from_row, prelude::*, test_misc::get_opts, Conn, Row, TxOpts};

#[tokio::test]
async fn should_stream_text_result_sets() -> crate::Result<()> {
    const QUERY: &str = r"
        SELECT 1;
        SELECT 'foo' UNION ALL SELECT 'bar';
        SELECT 3, 4 UNION ALL SELECT 5, 6;
        SELECT FOO();
        SELECT 3.14;
    ";
    let mut conn = Conn::new(get_opts()).await?;
    let mut result = QUERY.run(&mut conn).await?;

    let stream = result.stream::<u8>().await?.unwrap();
    assert_eq!(vec![1], stream.try_collect::<Vec<_>>().await?);

    let _ = result.stream::<Row>().await?.unwrap();

    let stream = result.stream::<(u8, u8)>().await?.unwrap();
    assert_eq!(vec![(3, 4), (5, 6)], stream.try_collect::<Vec<_>>().await?);

    let _ = result.stream::<Row>().await.unwrap_err();
    assert!(result.stream::<Row>().await?.is_none());

    let mut conn = Conn::new(get_opts()).await?;
    let mut result = QUERY.run(&mut conn).await?;

    assert_eq!(vec![1], result.collect::<u8>().await?);

    assert_eq!("foo", from_row::<String>(result.next().await?.unwrap()));
    let _ = result.stream::<Row>().await?.unwrap();

    assert_eq!((3_u8, 4_u8), from_row(result.next().await?.unwrap()));
    assert_eq!(vec![(5, 6)], result.collect::<(u8, u8)>().await?);

    result.collect::<Row>().await.unwrap_err();
    assert!(result.stream::<Row>().await?.is_none());

    conn.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn should_stream_binary_result_sets() -> crate::Result<()> {
    let mut conn = Conn::new(get_opts()).await?;
    let mut result = "SELECT ?".with((1_u8,)).run(&mut conn).await?;
    assert_eq!(vec![1], result.collect::<u8>().await?);
    assert!(result.stream::<Row>().await?.is_none());

    let mut result = "SELECT ?".with((1_u8,)).run(&mut conn).await?;
    assert_eq!(
        vec![1],
        result
            .stream::<u8>()
            .await?
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?,
    );
    assert_eq!(result.collect::<u8>().await?, Vec::<u8>::new());

    conn.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn dropped_query_result_should_emit_errors_on_cleanup() -> super::Result<()> {
    use crate::{Error::Server, ServerError};
    let mut conn = Conn::new(get_opts()).await?;
    conn.query_iter("SELECT '1'; BLABLA;").await?;
    assert!(matches!(
        conn.query_drop("DO 42;").await.unwrap_err(),
        Server(ServerError { code: 1064, .. })
    ));
    conn.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn should_try_collect() -> super::Result<()> {
    let mut conn = Conn::new(get_opts()).await?;
    let mut result = conn
        .query_iter(
            r"SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 'bar'
                UNION ALL
                SELECT 'hello', 123
            ",
        )
        .await?;
    let mut rows = result.try_collect::<(String, u8)>().await?;
    assert!(rows.pop().unwrap().is_ok());
    assert!(rows.pop().unwrap().is_err());
    assert!(rows.pop().unwrap().is_ok());
    result.drop_result().await?;
    conn.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn should_try_collect_and_drop() -> super::Result<()> {
    let mut conn = Conn::new(get_opts()).await?;
    let mut rows = conn
        .query_iter(
            r"SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 'bar'
                UNION ALL
                SELECT 'hello', 123;
                SELECT 'foo', 255;
            ",
        )
        .await?
        .try_collect_and_drop::<(String, u8)>()
        .await?;
    assert!(rows.pop().unwrap().is_ok());
    assert!(rows.pop().unwrap().is_err());
    assert!(rows.pop().unwrap().is_ok());
    conn.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn should_handle_mutliresult_set() -> super::Result<()> {
    let mut conn = Conn::new(get_opts()).await?;
    let mut result = conn
        .query_iter(
            r"SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 231;
                SELECT 'foo', 255;
            ",
        )
        .await?;
    let rows_1 = result.collect::<(String, u8)>().await?;
    let rows_2 = result.collect_and_drop().await?;
    conn.disconnect().await?;

    assert_eq!((String::from("hello"), 123), rows_1[0]);
    assert_eq!((String::from("world"), 231), rows_1[1]);
    assert_eq!((String::from("foo"), 255), rows_2[0]);
    Ok(())
}

#[tokio::test]
async fn should_map_resultset() -> super::Result<()> {
    let mut conn = Conn::new(get_opts()).await?;
    let mut result = conn
        .query_iter(
            r"
                SELECT 'hello', 123
                UNION ALL
                SELECT 'world', 231;
                SELECT 'foo', 255;
            ",
        )
        .await?;

    let rows_1 = result.map(from_row::<(String, u8)>).await?;
    let rows_2 = result.map_and_drop(from_row).await?;
    conn.disconnect().await?;

    assert_eq!((String::from("hello"), 123), rows_1[0]);
    assert_eq!((String::from("world"), 231), rows_1[1]);
    assert_eq!((String::from("foo"), 255), rows_2[0]);
    Ok(())
}

#[tokio::test]
async fn should_reduce_resultset() -> super::Result<()> {
    let mut conn = Conn::new(get_opts()).await?;
    let mut result = conn
        .query_iter(
            r"SELECT 5
                UNION ALL
                SELECT 6;
                SELECT 7;",
        )
        .await?;
    let reduced = result
        .reduce(0, |mut acc, row| {
            acc += from_row::<i32>(row);
            acc
        })
        .await?;
    let rows_2 = result.collect_and_drop::<i32>().await?;
    conn.disconnect().await?;
    assert_eq!(11, reduced);
    assert_eq!(7, rows_2[0]);
    Ok(())
}

#[tokio::test]
async fn should_handle_multi_result_sets_where_some_results_have_no_output() -> super::Result<()> {
    const QUERY: &str = r"SELECT 1;
        UPDATE time_zone SET Time_zone_id = 1 WHERE Time_zone_id = 1;
        SELECT 2;
        SELECT 3;
        UPDATE time_zone SET Time_zone_id = 1 WHERE Time_zone_id = 1;
        UPDATE time_zone SET Time_zone_id = 1 WHERE Time_zone_id = 1;
        SELECT 4;";

    let mut c = Conn::new(get_opts()).await?;
    c.query_drop("CREATE TEMPORARY TABLE time_zone (Time_zone_id INT)")
        .await
        .unwrap();
    let mut t = c.start_transaction(TxOpts::new()).await?;
    t.query_drop(QUERY).await?;
    let r = t.query_iter(QUERY).await?;
    let out = r.collect_and_drop::<u8>().await?;
    assert_eq!(vec![1], out);
    let r = t.query_iter(QUERY).await?;
    r.for_each_and_drop(|x| assert_eq!(from_row::<u8>(x), 1))
        .await?;
    let r = t.query_iter(QUERY).await?;
    let out = r.map_and_drop(from_row::<u8>).await?;
    assert_eq!(vec![1], out);
    let r = t.query_iter(QUERY).await?;
    let out = r
        .reduce_and_drop(0u8, |acc, x| acc + from_row::<u8>(x))
        .await?;
    assert_eq!(1, out);
    t.query_drop(QUERY).await?;
    t.commit().await?;
    let result = c.exec_first("SELECT 1", ()).await?;
    c.disconnect().await?;
    assert_eq!(result, Some(1_u8));
    Ok(())
}

#[tokio::test]
async fn should_iterate_over_resultset() -> super::Result<()> {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    let acc = Arc::new(AtomicUsize::new(0));

    let mut conn = Conn::new(get_opts()).await?;
    let mut result = conn
        .query_iter(
            r"SELECT 2
                UNION ALL
                SELECT 3;
                SELECT 5;",
        )
        .await?;
    result
        .for_each({
            let acc = acc.clone();
            move |row| {
                acc.fetch_add(from_row::<usize>(row), Ordering::SeqCst);
            }
        })
        .await?;
    result
        .for_each_and_drop({
            let acc = acc.clone();
            move |row| {
                acc.fetch_add(from_row::<usize>(row), Ordering::SeqCst);
            }
        })
        .await?;
    conn.disconnect().await?;
    assert_eq!(acc.load(Ordering::SeqCst), 10);
    Ok(())
}
