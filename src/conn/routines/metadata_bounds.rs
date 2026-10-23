// Copyright (c) 2026 mysql_async contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! Deterministic peers retain their listener/socket ownership throughout each scenario.
//! Every exchange has a deadline; definition rejection cases send at most a one-byte type prefix.

use std::{future::Future, sync::Arc, time::Duration};

use futures_util::FutureExt;
use mysql_common::{
    constants::{CapabilityFlags, ColumnType, Command, MariadbCapabilities},
    packets::{ErrPacket, ProgressReport},
    proto::{codec::error::PacketCodecError, MySerialize},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    error::IoError,
    io::{ReadPacket, Stream},
    prelude::Queryable,
    queryable::stmt::StmtInner,
    BinaryProtocol, Column, Conn, DriverError, Error, OptsBuilder, Statement,
};

const PACKET_LIMIT: usize = 1024;
const EOF: &[u8] = &[0xfe, 0, 0, 2, 0];
const OK_EOF: &[u8] = &[0xfe, 0, 0, 2, 0, 0, 0];

async fn deadline<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("metadata peer deadline")
}

fn capabilities() -> CapabilityFlags {
    CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_DEPRECATE_EOF
}

async fn pair(opts: OptsBuilder) -> (Conn, TcpStream) {
    deadline(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let opts = opts
            .ip_or_hostname("127.0.0.1")
            .tcp_port(listener.local_addr().unwrap().port())
            .max_allowed_packet(Some(PACKET_LIMIT));
        let mut conn = Conn::empty(opts.into());
        let (stream, peer) = tokio::join!(
            Stream::connect_tcp(conn.opts().hostport_or_url(), None),
            listener.accept()
        );
        conn.inner.stream = Some(stream.unwrap());
        conn.setup_stream().unwrap();
        conn.inner.capabilities = capabilities();
        conn.inner.handshake_complete = true;
        // Synthetic connections must not spawn cleanup traffic when the test drops them.
        conn.inner.disconnected = true;
        (conn, peer.unwrap().0)
    })
    .await
}

async fn header(peer: &mut TcpStream, len: usize, seq: u8) {
    assert!(len <= 0xff_ffff);
    let len = u32::try_from(len).unwrap().to_le_bytes();
    peer.write_all(&[len[0], len[1], len[2], seq])
        .await
        .unwrap();
}

async fn packet(peer: &mut TcpStream, body: &[u8], seq: u8) {
    header(peer, body.len(), seq).await;
    peer.write_all(body).await.unwrap();
}

async fn request(peer: &mut TcpStream) -> Vec<u8> {
    let mut header = [0; 4];
    peer.read_exact(&mut header).await.unwrap();
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    assert!(len <= PACKET_LIMIT);
    let mut body = vec![0; len];
    peer.read_exact(&mut body).await.unwrap();
    body
}

async fn closed(peer: &mut TcpStream) {
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
}

fn definition(name: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    Column::new(ColumnType::MYSQL_TYPE_LONG)
        .with_name(name)
        .serialize(&mut payload);
    payload
}

fn prepared(params: u16, columns: u16) -> Vec<u8> {
    let mut payload = vec![0];
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(&columns.to_le_bytes());
    payload.extend_from_slice(&params.to_le_bytes());
    payload.extend_from_slice(&[0, 0, 0]);
    payload
}

fn count(value: u64) -> Vec<u8> {
    let mut payload = vec![0xfe];
    payload.extend_from_slice(&value.to_le_bytes());
    payload
}

fn is_packet_too_large(error: &Error) -> bool {
    matches!(
        error,
        Error::Io(IoError::Io(error))
            if matches!(
                error.get_ref().and_then(|error| error.downcast_ref::<PacketCodecError>()),
                Some(PacketCodecError::PacketTooLarge)
            )
    )
}

/// Scenario: a result set advertises the largest length-encoded u64, with/without an opt-in cap.
/// Guarantees: a typed count error occurs without a panic, a count-sized reserve, or definition I/O.
#[tokio::test]
async fn enormous_result_counts_are_rejected() {
    for limit in [None, Some(2)] {
        let (mut conn, mut peer) = pair(OptsBuilder::default().max_metadata_count(limit)).await;
        let (result, ()) = deadline(async {
            tokio::join!(conn.query_iter("SELECT enormous"), async {
                assert_eq!(request(&mut peer).await[0], Command::COM_QUERY as u8);
                packet(&mut peer, &count(u64::MAX), 1).await;
                closed(&mut peer).await;
            })
        })
        .await;
        assert!(matches!(
            result.err().unwrap(),
            Error::Driver(DriverError::MetadataCountExceeded {
                count: u64::MAX,
                ..
            })
        ));
        assert!(conn.is_disconnected());
    }
}

/// Scenario: either prepare count is excessive, including a valid parameter block before it.
/// Guarantees: both counts are checked before retaining any parameter or column definitions.
#[tokio::test]
async fn prepared_counts_are_checked_before_definitions() {
    for (params, columns) in [(u16::MAX, 0), (0, u16::MAX), (1, u16::MAX)] {
        let (mut conn, mut peer) = pair(OptsBuilder::default().max_metadata_count(Some(1))).await;
        let (result, ()) = deadline(async {
            tokio::join!(conn.prep("SELECT ?"), async {
                assert_eq!(request(&mut peer).await[0], Command::COM_STMT_PREPARE as u8);
                packet(&mut peer, &prepared(params, columns), 1).await;
                closed(&mut peer).await;
            })
        })
        .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::Driver(DriverError::MetadataCountExceeded {
                count: 65535,
                limit: 1
            })
        ));
    }
}

/// Scenario: parameter/column metadata packets exceed a finite byte budget.
/// Guarantees: rejection needs only framing and a type byte, never the remaining definition body.
#[tokio::test]
async fn prepared_payloads_are_bounded_before_reading() {
    for (params, columns) in [(1, 0), (0, 1)] {
        let (mut conn, mut peer) = pair(OptsBuilder::default().max_metadata_bytes(Some(8))).await;
        let (result, ()) = deadline(async {
            tokio::join!(conn.prep("SELECT ?"), async {
                request(&mut peer).await;
                packet(&mut peer, &prepared(params, columns), 1).await;
                header(&mut peer, PACKET_LIMIT, 2).await;
                peer.write_all(&[3]).await.unwrap();
                closed(&mut peer).await;
            })
        })
        .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::Driver(DriverError::MetadataBytesExceeded { limit: 8 })
        ));
    }
}

/// Scenario: individually small packets exceed the aggregate budget on the second definition.
/// Guarantees: the remaining budget is enforced at the next type prefix, including zero remaining.
#[tokio::test]
async fn aggregate_payloads_are_bounded() {
    let def = definition(b"field");
    for limit in [def.len(), def.len() * 2 - 1] {
        let (mut conn, mut peer) =
            pair(OptsBuilder::default().max_metadata_bytes(Some(limit))).await;
        let (result, ()) = deadline(async {
            tokio::join!(conn.prep("SELECT 1, 2"), async {
                request(&mut peer).await;
                packet(&mut peer, &prepared(0, 2), 1).await;
                packet(&mut peer, &def, 2).await;
                header(&mut peer, def.len(), 3).await;
                peer.write_all(&def[..1]).await.unwrap();
                closed(&mut peer).await;
            })
        })
        .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::Driver(DriverError::MetadataBytesExceeded { limit: actual }) if actual == limit
        ));
    }
}

/// Scenario: a server error exceeds the remaining metadata budget but fits the normal packet cap.
/// Guarantees: excluded control packets retain their server error and leave the connection reusable.
#[tokio::test]
async fn control_errors_preserve_connection() {
    let def = definition(b"field");
    for limit in [def.len(), def.len() * 2] {
        let (mut conn, mut peer) =
            pair(OptsBuilder::default().max_metadata_bytes(Some(limit))).await;
        deadline(async {
            tokio::join!(
                async {
                    let error = conn.prep("SELECT 1, 2").await.unwrap_err();
                    let Error::Server(error) = error else {
                        panic!("expected a server error, got {error:?}");
                    };
                    assert_eq!(error.code, 1105);
                    assert_eq!(error.state, "HY000");
                    assert_eq!(error.message, "x".repeat(64));
                    assert!(!conn.is_disconnected());
                    conn.ping().await.unwrap();
                    conn.inner.disconnected = true;
                },
                async {
                    request(&mut peer).await;
                    packet(&mut peer, &prepared(0, 2), 1).await;
                    packet(&mut peer, &def, 2).await;
                    let mut error = b"\xff\x51\x04#HY000".to_vec();
                    error.extend_from_slice(&[b'x'; 64]);
                    packet(&mut peer, &error, 3).await;
                    assert_eq!(request(&mut peer).await, [Command::COM_PING as u8]);
                    packet(&mut peer, &[0, 0, 0, 2, 0, 0, 0], 1).await;
                }
            );
        })
        .await;
    }
}

/// Scenario: a progress report is larger than the entire definition budget.
/// Guarantees: it uses the normal packet cap and does not reduce the next definition's budget.
#[tokio::test]
async fn progress_packets_do_not_consume_metadata_budget() {
    let def = definition(b"field");
    let (mut conn, mut peer) =
        pair(OptsBuilder::default().max_metadata_bytes(Some(def.len()))).await;
    conn.inner
        .capabilities
        .insert(CapabilityFlags::CLIENT_PROGRESS_OBSOLETE);
    let mut progress = Vec::new();
    ErrPacket::Progress(ProgressReport::new(1, 1, 50_000, vec![b'x'; 64])).serialize(&mut progress);
    let (result, ()) = deadline(async {
        tokio::join!(conn.read_column_defs(1), async {
            packet(&mut peer, &progress, 0).await;
            packet(&mut peer, &def, 1).await;
        })
    })
    .await;
    assert_eq!(result.unwrap()[0].name_ref(), b"field");
}

/// Scenario: the first of two definitions is malformed and the second is withheld.
/// Guarantees: parsing is incremental, not deferred until all raw packets have been retained.
#[tokio::test]
async fn definitions_are_parsed_incrementally() {
    let (mut conn, mut peer) = pair(OptsBuilder::default()).await;
    let (result, ()) = deadline(async {
        tokio::join!(conn.prep("SELECT 1, 2"), async {
            request(&mut peer).await;
            packet(&mut peer, &prepared(0, 2), 1).await;
            packet(&mut peer, &[1], 2).await;
            closed(&mut peer).await;
        })
    })
    .await;
    assert!(matches!(result.unwrap_err(), Error::Io(_)));
}

/// Scenario: both prepare blocks exactly fill their own count/byte budgets, with either EOF mode.
/// Guarantees: parameters and columns are independently bounded; terminators consume no budget.
#[tokio::test]
async fn exact_prepared_block_boundaries_pass() {
    let def = definition(b"field");
    for deprecate_eof in [false, true] {
        let (mut conn, mut peer) = pair(
            OptsBuilder::default()
                .max_metadata_count(Some(2))
                .max_metadata_bytes(Some(def.len() * 2)),
        )
        .await;
        conn.inner
            .capabilities
            .set(CapabilityFlags::CLIENT_DEPRECATE_EOF, deprecate_eof);
        let (result, ()) = deadline(async {
            tokio::join!(conn.prep("SELECT ?, ?"), async {
                request(&mut peer).await;
                packet(&mut peer, &prepared(2, 2), 1).await;
                let mut seq = 2;
                for _ in 0..2 {
                    for _ in 0..2 {
                        packet(&mut peer, &def, seq).await;
                        seq += 1;
                    }
                    if !deprecate_eof {
                        packet(&mut peer, EOF, seq).await;
                        seq += 1;
                    }
                }
            })
        })
        .await;
        let stmt = result.unwrap();
        assert_eq!(stmt.params().len(), 2);
        assert_eq!(stmt.columns().len(), 2);
        assert_eq!(
            conn.stream_ref().unwrap().get_max_allowed_packet(),
            PACKET_LIMIT
        );
        conn.inner.disconnected = true;
    }
}

/// Scenario: an empty prepare response is followed by an ordinary nonempty one with zero limits.
/// Guarantees: zero is not clamped or treated as unlimited; empty blocks still work.
#[tokio::test]
async fn zero_limits_are_explicit() {
    let (mut conn, mut peer) = pair(
        OptsBuilder::default()
            .max_metadata_count(Some(0))
            .max_metadata_bytes(Some(0)),
    )
    .await;
    deadline(async {
        tokio::join!(
            async {
                let stmt = conn.prep("DO 1").await.unwrap();
                assert!(stmt.params().is_empty());
                assert!(stmt.columns().is_empty());
                assert!(matches!(
                    conn.prep("SELECT 1").await.unwrap_err(),
                    Error::Driver(DriverError::MetadataCountExceeded { count: 1, limit: 0 })
                ));
            },
            async {
                request(&mut peer).await;
                packet(&mut peer, &prepared(0, 0), 1).await;
                request(&mut peer).await;
                packet(&mut peer, &prepared(0, 1), 1).await;
                closed(&mut peer).await;
            }
        );
    })
    .await;
}

/// Scenario: a block exhausts its budget, then EOF and a larger data packet follow.
/// Guarantees: the temporary metadata limit does not leak into terminators or data reads.
#[tokio::test]
async fn data_packet_limit_is_restored() {
    let def = definition(b"field");
    let (mut conn, mut peer) = pair(
        OptsBuilder::default()
            .max_metadata_count(Some(1))
            .max_metadata_bytes(Some(def.len())),
    )
    .await;
    conn.inner
        .capabilities
        .remove(CapabilityFlags::CLIENT_DEPRECATE_EOF);
    deadline(async {
        tokio::join!(
            async {
                assert_eq!(conn.read_column_defs(1).await.unwrap().len(), 1);
                assert_eq!(conn.stream_mut().unwrap().set_metadata_limit(None), None);
                assert_eq!(conn.read_packet().await.unwrap().len(), 512);
            },
            async {
                packet(&mut peer, &def, 0).await;
                packet(&mut peer, EOF, 1).await;
                packet(&mut peer, &[42; 512], 2).await;
            }
        );
    })
    .await;
    assert_eq!(
        conn.stream_ref().unwrap().get_max_allowed_packet(),
        PACKET_LIMIT
    );
}

/// Scenario: a limited read yields Pending and is cancelled, or returns a codec error.
/// Guarantees: the normal codec limit is restored on both paths, not just success.
#[tokio::test]
async fn pending_and_error_restore_packet_limit() {
    let (mut conn, mut peer) = pair(OptsBuilder::default()).await;
    assert!(ReadPacket::new(&mut conn)
        .with_metadata_limit(Some(8))
        .now_or_never()
        .is_none());
    assert_eq!(conn.stream_mut().unwrap().set_metadata_limit(None), None);
    assert_eq!(
        conn.stream_ref().unwrap().get_max_allowed_packet(),
        PACKET_LIMIT
    );
    deadline(async {
        tokio::join!(
            async {
                assert_eq!(conn.read_packet().await.unwrap().len(), 64);
                assert!(ReadPacket::new(&mut conn)
                    .with_metadata_limit(Some(8))
                    .await
                    .is_err());
            },
            async {
                packet(&mut peer, &[42; 64], 0).await;
                header(&mut peer, 64, 1).await;
                peer.write_all(&[42]).await.unwrap();
            }
        );
    })
    .await;
    assert_eq!(conn.stream_mut().unwrap().set_metadata_limit(None), None);
    assert_eq!(
        conn.stream_ref().unwrap().get_max_allowed_packet(),
        PACKET_LIMIT
    );
}

/// Scenario: normal/metadata caps overlap, including control packets with zero remaining budget.
/// Guarantees: definitions retain metadata errors and controls never bypass the normal packet cap.
#[tokio::test]
async fn regular_packet_limit_still_applies() {
    for (budget, first, metadata_error) in [
        (PACKET_LIMIT * 2, None, false),
        (PACKET_LIMIT, Some(3), true),
        (0, Some(0xff), false),
        (PACKET_LIMIT, Some(0xff), false),
    ] {
        let (mut conn, mut peer) =
            pair(OptsBuilder::default().max_metadata_bytes(Some(budget))).await;
        let (result, ()) = deadline(async {
            tokio::join!(conn.read_column_defs(1), async {
                header(&mut peer, PACKET_LIMIT + 1, 0).await;
                if let Some(first) = first {
                    peer.write_all(&[first]).await.unwrap();
                }
                closed(&mut peer).await;
            })
        })
        .await;
        let error = result.unwrap_err();
        if metadata_error {
            assert!(matches!(
                error,
                Error::Driver(DriverError::MetadataBytesExceeded {
                    limit: PACKET_LIMIT
                })
            ));
        } else {
            assert!(is_packet_too_large(&error));
        }
    }
}

/// Scenario: ordinary binary results refresh then skip MariaDB column metadata.
/// Guarantees: bounded metadata remains compatible and the cache/result share one column array.
#[tokio::test]
async fn prepared_results_share_validated_cached_metadata() {
    let def = definition(b"updated");
    let (mut conn, mut peer) = pair(
        OptsBuilder::default()
            .max_metadata_count(Some(1))
            .max_metadata_bytes(Some(def.len())),
    )
    .await;
    conn.inner
        .mariadb_capabilities
        .insert(MariadbCapabilities::MARIADB_CLIENT_CACHE_METADATA);
    deadline(async {
        tokio::join!(
            async {
                let stmt = conn.prep("SELECT 42").await.unwrap();
                for _ in 0..2 {
                    let result = conn.exec_iter(&stmt, ()).await.unwrap();
                    assert!(Arc::ptr_eq(&stmt.columns(), &result.columns().unwrap()));
                    assert_eq!(result.columns_ref()[0].name_ref(), b"updated");
                    assert_eq!(result.collect_and_drop::<(i32,)>().await.unwrap(), [(42,)]);
                }
                conn.inner.disconnected = true;
            },
            async {
                request(&mut peer).await;
                packet(&mut peer, &prepared(0, 1), 1).await;
                packet(&mut peer, &definition(b"old"), 2).await;
                for present in [true, false] {
                    assert_eq!(request(&mut peer).await[0], Command::COM_STMT_EXECUTE as u8);
                    packet(&mut peer, &[1, u8::from(present)], 1).await;
                    let seq = if present {
                        packet(&mut peer, &def, 2).await;
                        3
                    } else {
                        2
                    };
                    packet(&mut peer, &[0, 0, 42, 0, 0, 0], seq).await;
                    packet(&mut peer, OK_EOF, seq + 1).await;
                }
            }
        );
    })
    .await;
}

/// Scenario: skipped metadata advertises an excessive or inconsistent column count.
/// Guarantees: caching cannot bypass the count limit or pass mismatched columns to row parsing.
#[tokio::test]
async fn skipped_metadata_counts_are_checked() {
    let mut conn = Conn::empty(OptsBuilder::default().max_metadata_count(Some(2)).into());
    conn.inner.disconnected = true;
    conn.inner
        .mariadb_capabilities
        .insert(MariadbCapabilities::MARIADB_CLIENT_CACHE_METADATA);
    let inner = StmtInner::from_payload(&prepared(0, 1), conn.id(), Arc::from(&b""[..]))
        .unwrap()
        .with_columns(vec![Column::new(ColumnType::MYSQL_TYPE_LONG)]);
    let stmt = Statement::new(Arc::new(inner), Vec::new());
    let mut huge = count(u64::MAX);
    huge.push(0);
    assert!(matches!(
        conn.handle_result_set::<BinaryProtocol>(&huge, Some(&stmt))
            .await
            .unwrap_err(),
        Error::Driver(DriverError::MetadataCountExceeded { .. })
    ));
    assert!(matches!(
        conn.handle_result_set::<BinaryProtocol>(&[2, 0], Some(&stmt))
            .await
            .unwrap_err(),
        Error::Driver(DriverError::MetadataCountMismatch {
            expected: 2,
            actual: 1
        })
    ));
}

/// Scenario: a text result's column definition is exactly at or one byte over its budget.
/// Guarantees: the public query path enforces the same limits as prepare, without limiting rows.
#[tokio::test]
async fn text_result_byte_boundaries() {
    let def = definition(b"field");
    for fits in [false, true] {
        let limit = def.len() - usize::from(!fits);
        let (mut conn, mut peer) = pair(
            OptsBuilder::default()
                .max_metadata_count(Some(1))
                .max_metadata_bytes(Some(limit)),
        )
        .await;
        deadline(async {
            tokio::join!(
                async {
                    let result = conn.query_iter("SELECT 42").await;
                    if fits {
                        assert_eq!(
                            result.unwrap().collect_and_drop::<(i32,)>().await.unwrap(),
                            [(42,)]
                        );
                        conn.inner.disconnected = true;
                    } else {
                        assert!(matches!(
                            result.err().unwrap(),
                            Error::Driver(DriverError::MetadataBytesExceeded { .. })
                        ));
                    }
                },
                async {
                    request(&mut peer).await;
                    packet(&mut peer, &[1], 1).await;
                    header(&mut peer, def.len(), 2).await;
                    if fits {
                        peer.write_all(&def).await.unwrap();
                        packet(&mut peer, b"\x0242", 3).await;
                        packet(&mut peer, OK_EOF, 4).await;
                    } else {
                        peer.write_all(&def[..1]).await.unwrap();
                        closed(&mut peer).await;
                    }
                }
            );
        })
        .await;
    }
}

/// Scenario: compression is enabled, with either default metadata limits or a tight byte budget.
/// Guarantees: default compressed reads still work; oversized envelopes reject at their header.
#[tokio::test]
async fn compressed_metadata_limits() {
    for limit in [None, Some(8)] {
        let (mut conn, mut peer) = pair(OptsBuilder::default().max_metadata_bytes(limit)).await;
        conn.stream_mut()
            .unwrap()
            .compress(crate::Compression::fast());
        let (result, ()) = deadline(async {
            tokio::join!(conn.read_column_defs(1), async {
                if limit.is_some() {
                    peer.write_all(&[1, 0, 0, 0, 9, 0, 0]).await.unwrap();
                    closed(&mut peer).await;
                } else {
                    let mut codec = mysql_common::proto::codec::PacketCodec::default();
                    codec.compress(crate::Compression::fast());
                    let mut wire = bytes::BytesMut::new();
                    codec
                        .encode(&mut &definition(b"field")[..], &mut wire)
                        .unwrap();
                    peer.write_all(&wire).await.unwrap();
                }
            })
        })
        .await;
        if limit.is_some() {
            assert!(matches!(
                result.unwrap_err(),
                Error::Driver(DriverError::MetadataBytesExceeded { limit: 8 })
            ));
        } else {
            assert_eq!(result.unwrap()[0].name_ref(), b"field");
            assert_eq!(
                conn.stream_ref().unwrap().get_max_allowed_packet(),
                PACKET_LIMIT
            );
        }
    }
}

fn greeting(flags: CapabilityFlags) -> Vec<u8> {
    let mut body = b"\x0a8.4.13\0".to_vec();
    body.extend_from_slice(&1_u32.to_le_bytes());
    body.extend_from_slice(b"12345678\0");
    let caps = flags.bits().to_le_bytes();
    body.extend_from_slice(&caps[..2]);
    body.extend_from_slice(&[45, 2, 0]);
    body.extend_from_slice(&caps[2..]);
    body.push(21);
    body.extend_from_slice(&[0; 10]);
    body.extend_from_slice(b"123456789012\0mysql_native_password\0");
    body
}

/// Scenario: an oversized greeting or authentication packet provides only a length header.
/// Guarantees: explicit max_allowed_packet applies through Conn::new before authentication ends.
#[tokio::test]
async fn pre_authentication_packet_limit_applies() {
    for during_auth in [false, true] {
        deadline(async {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let opts = OptsBuilder::default()
                .ip_or_hostname("127.0.0.1")
                .tcp_port(listener.local_addr().unwrap().port())
                .prefer_socket(false)
                .max_allowed_packet(Some(PACKET_LIMIT));
            let (result, ()) = tokio::join!(Conn::new(opts), async {
                let (mut peer, _) = listener.accept().await.unwrap();
                let seq = if during_auth {
                    packet(&mut peer, &greeting(capabilities()), 0).await;
                    let response = request(&mut peer).await;
                    assert_eq!(
                        u32::from_le_bytes(response[4..8].try_into().unwrap()),
                        PACKET_LIMIT as u32
                    );
                    2
                } else {
                    0
                };
                header(&mut peer, PACKET_LIMIT + 1, seq).await;
                closed(&mut peer).await;
            });
            assert!(is_packet_too_large(&result.unwrap_err()));
        })
        .await;
    }
}

/// Scenario: TLS is requested but loading a deliberately missing CA stops before provider setup.
/// Guarantees: the actual SSL request advertises the installed packet cap without selecting crypto.
#[tokio::test]
#[cfg(any(feature = "native-tls-tls", feature = "rustls-tls"))]
async fn ssl_request_advertises_packet_limit() {
    let dir = tempfile::tempdir().unwrap();
    let ssl =
        crate::SslOpts::default().with_root_certs(vec![dir.path().join("missing-ca.pem").into()]);
    let (mut conn, mut peer) = pair(OptsBuilder::default().ssl_opts(ssl)).await;
    conn.inner.capabilities.insert(CapabilityFlags::CLIENT_SSL);
    let (result, response) =
        deadline(async { tokio::join!(conn.switch_to_ssl_if_needed(), request(&mut peer)) }).await;
    assert!(matches!(result.unwrap_err(), Error::Io(_)));
    assert_eq!(response.len(), 32);
    assert_eq!(
        u32::from_le_bytes(response[4..8].try_into().unwrap()),
        PACKET_LIMIT as u32
    );
}
