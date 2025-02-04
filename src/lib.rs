use std::ffi::OsString;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::time::Duration;

use bytes::Buf;
use lexopt::ValueExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::select;

pub fn port_or_addr(arg: OsString, default_addr: Ipv4Addr) -> eyre::Result<SocketAddr> {
    match arg.parse::<SocketAddr>() {
        Ok(addr) => Ok(addr),
        Err(_e) => match arg.parse::<u16>() {
            Ok(port) => Ok(SocketAddr::new(IpAddr::V4(default_addr), port)),
            Err(_e) => {
                eyre::bail!("provided value is not an address or a port number");
            }
        },
    }
}

pub async fn run(
    listen: bool,
    tcp_addr: SocketAddr,
    udp_bind: SocketAddr,
    initial_udp_sendto: SocketAddr,
) -> eyre::Result<()> {
    let mut udp_sendto: SocketAddr = initial_udp_sendto;
    tracing::debug!("bind to udp {udp_bind:?}");
    let udp = tokio::net::UdpSocket::bind(udp_bind)
        .await
        .expect("udp-bind");
    let mut listener = if listen {
        tracing::info!("bind to tcp {tcp_addr:?}");
        Some(
            tokio::net::TcpListener::bind(tcp_addr)
                .await
                .expect("tcp-listen"),
        )
    } else {
        None
    };
    let mut tcp = None::<tokio::net::TcpStream>;
    let mut connect_again = None::<Pin<Box<tokio::time::Sleep>>>;

    let mut udp_buf = Vec::with_capacity(65536);
    let mut tcp_buf = Vec::with_capacity(65536);

    loop {
        let has_tcp = tcp.is_some();
        let connect_fut = async {
            if !has_tcp && !listen {
                if let Some(timeout) = &mut connect_again {
                    timeout.await;
                    connect_again = None;
                }

                tracing::debug!("connect to tcp {tcp_addr:?}");
                tokio::net::TcpStream::connect(tcp_addr).await
            } else {
                std::future::pending().await
            }
        };
        let listener_fut = async {
            if let Some(listener) = &mut listener {
                listener.accept().await
            } else {
                std::future::pending().await
            }
        };
        let tcp_fut = async {
            if let Some(tcp) = &mut tcp {
                tcp.read_buf(&mut tcp_buf).await
            } else {
                std::future::pending().await
            }
        };

        select! {
            conn = connect_fut, if !has_tcp && !listen => {
                match conn {
                    Ok(stream) => {
                        tracing::info!("established tcp connection");
                        tcp = Some(stream);
                        tcp_buf.clear();
                    }
                    Err(e) => {
                        tracing::error!("tcp connect failed: {e}");
                        connect_again = Some(Box::pin(tokio::time::sleep(Duration::from_secs(1))));
                    }
                }
            }
            conn = listener_fut, if listen => {
                let (conn, addr) = conn.expect("TcpListener::accept only fails if out of FDs or on protocol errors");
                if let Some(old) = tcp.replace(conn) {
                    tracing::warn!(
                        "new tcp connection from {addr:?} replaces old {:?}",
                        old.peer_addr().expect("TcpStream::peer_addr never fails")
                    );
                } else {
                    tracing::info!("accepted incoming tcp connection from {addr:?}");
                }
                tcp_buf.clear();
            }
            res = udp.recv_from(&mut udp_buf) => {
                if let Some(tcp_stream) = &mut tcp {
                    let res = res.expect("UdpSocket::recv_from has no relevant error conditions");
                    let source_address: SocketAddr = res.1;
                    if source_address != udp_sendto {
                        tracing::debug!("received udp packet from different source address: {}. setting as new UDP peer.", source_address);
                    }
                    udp_sendto = source_address;
                    let len = udp_buf.len() as u32;
                    tracing::trace!(n = len, "forward udp packet to tcp");
                    if let Err(e) = tcp_stream.write_all_buf(&mut Buf::chain(&len.to_le_bytes()[..], &udp_buf[..])).await {
                        tracing::error!("dropping tcp connection after failed write: {e}");
                        tcp = None;
                    } else if let Err(e) = tcp_stream.flush().await {
                        tracing::error!("dropping tcp connection after failed flush: {e}");
                        tcp = None;
                    }
                    udp_buf.clear();
                } else {
                    tracing::debug!("dropping udp packet without a tcp peer");
                }
            }
            msg = tcp_fut => {
                let n = msg.expect("tcp-read");
                if n == 0 {
                    tracing::warn!("dropping disconnected tcp connection");
                    tcp = None;
                    continue;
                }

                let mut rest = &tcp_buf[..];
                loop {
                    if rest.len() < std::mem::size_of::<u32>() {
                        break;
                    }
                    let len = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
                    let tail = &rest[4..];
                    if tail.len() < len {
                        break;
                    }
                    let msg = &tail[..len];
                    rest = &tail[len..];
                    tracing::trace!(n = len, "forward tcp packet to udp");
                    if let Err(e) = udp.send_to(msg, udp_sendto).await {
                        tracing::error!("udp forward failed: {e}");
                    }
                }

                if rest.is_empty() {
                    tcp_buf.clear();
                } else {
                    tracing::trace!(n = rest.len(), "bytes left over in tcp receive buffer");
                    let keep = tcp_buf.len() - rest.len();
                    tcp_buf.drain(..keep);
                }
            }
        }
    }
}
