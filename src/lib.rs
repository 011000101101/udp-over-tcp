use std::ffi::OsString;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bimap::BiMap;
use bincode::{config, Decode, Encode};
use bytes::Buf;
use elsa::FrozenMap;
use lexopt::ValueExt;
use serde::__private::from_utf8_lossy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UdpSocket;
use tokio::select;
use tokio::task::JoinSet;

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

#[derive(Encode, Decode, Debug)]
struct UdpPacketWrapper {
    data: Vec<u8>,
    /// source port before NAT mapping -> perform mapping at UDP sender to avoid port conflicts
    source_addr: SocketAddr,
    /// original target port
    target_port: u16,
}

#[derive(Clone)]
struct UdpSockAccessor {
    local_port: u16,
    sock: Arc<UdpSocket>,
}

type Buffer = [u8; 65536];

impl UdpSockAccessor {
    pub fn new(sock: UdpSocket) -> (Self, Buffer) {
        (
            UdpSockAccessor {
                local_port: sock.local_addr().unwrap().port(),
                sock: Arc::new(sock),
            },
            [0_u8; 65536],
        )
    }

    pub async fn recv_from(self, mut buf: Buffer) -> (Self, Buffer, usize, SocketAddr) {
        let (len, source_addr) = self
            .sock
            .recv_from(&mut buf[..65535])
            .await
            .expect("UdpSocket::recv_from has no relevant error conditions");
        tracing::debug!("received UDP: {}B from {}", len, source_addr);
        (self, buf, len, source_addr)
    }

    pub async fn send_to(&self, payload: &[u8], addr: SocketAddr) -> usize {
        self.sock.send_to(payload, addr).await.unwrap_or_else(|e| {
            tracing::error!("udp forward failed: {e}");
            0
        })
    }
}

pub struct UdpToTcp {
    listen: bool,
    tcp_addr: SocketAddr,
    udp_ip_bind: IpAddr,
    udp_ip_peer: IpAddr,
    nat_table: BiMap<u16, SocketAddr>,
    udp_source_sockets: FrozenMap<u16, Box<UdpSockAccessor>>,
    udp_recv_socks: Option<JoinSet<(UdpSockAccessor, Buffer, usize, SocketAddr)>>,
}

impl UdpToTcp {
    pub fn new(
        listen: bool,
        tcp_addr: SocketAddr,
        udp_bind: SocketAddr,
        initial_udp_sendto: SocketAddr,
    ) -> Self {
        UdpToTcp {
            listen,
            tcp_addr,
            udp_ip_bind: udp_bind.ip(),
            udp_ip_peer: initial_udp_sendto.ip(),
            nat_table: BiMap::<u16, SocketAddr>::new(),
            udp_source_sockets: FrozenMap::new(),
            udp_recv_socks: Some(JoinSet::new()),
        }
    }

    fn add_udp_sock(
        &self,
        sock: UdpSocket,
        udp_receivers: &mut JoinSet<(UdpSockAccessor, Buffer, usize, SocketAddr)>,
    ) -> u16 {
        let (udp_sock, udp_buf) = UdpSockAccessor::new(sock);
        let port = udp_sock.local_port;
        let udp_sock_recv = udp_sock.clone();
        self.udp_source_sockets.insert(port, Box::new(udp_sock));
        // generate new receive future to monitor created port for incoming packets
        udp_receivers.spawn(udp_sock_recv.recv_from(udp_buf));
        port
    }

    fn take_udp_receivers(&mut self) -> JoinSet<(UdpSockAccessor, Buffer, usize, SocketAddr)> {
        self.udp_recv_socks.take().unwrap()
    }

    pub async fn run(&mut self, udp_bind: SocketAddr) -> eyre::Result<()> {
        let config = config::standard();
        let mut listener = if self.listen {
            tracing::info!("bind to tcp {:?}", self.tcp_addr);
            Some(
                tokio::net::TcpListener::bind(self.tcp_addr)
                    .await
                    .expect("tcp-listen"),
            )
        } else {
            None
        };
        let mut tcp = None::<tokio::net::TcpStream>;
        let mut connect_again = None::<Pin<Box<tokio::time::Sleep>>>;
        let mut tcp_buf = Vec::with_capacity(65536);

        tracing::debug!("bind to initial udp {udp_bind:?}");
        let udp_initial = UdpSocket::bind(udp_bind).await.expect("udp-bind");

        let mut udp_receivers = self.take_udp_receivers();
        self.add_udp_sock(udp_initial, &mut udp_receivers);

        loop {
            let has_tcp = tcp.is_some();
            let connect_fut = async {
                if !has_tcp && !self.listen {
                    if let Some(timeout) = &mut connect_again {
                        timeout.await;
                        connect_again = None;
                    }

                    tracing::debug!("connect to tcp {:?}", self.tcp_addr);
                    tokio::net::TcpStream::connect(self.tcp_addr).await
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
                conn = connect_fut, if !has_tcp && !self.listen => {
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
                conn = listener_fut, if self.listen => {
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
                Some(res) = udp_receivers.join_next() => {
                    let (recv_sock, mut buf, len, source_addr): (UdpSockAccessor, Buffer, usize, SocketAddr) = res.unwrap();
                    if let Some(tcp_stream) = &mut tcp {
                        let udp_packet = UdpPacketWrapper{
                            data: buf[..len].to_vec(),
                            source_addr,
                            target_port: recv_sock.local_port,
                        };
                        let bytes = bincode::encode_to_vec(&udp_packet, config).unwrap();
                        let len: u32 = bytes.len() as u32;
                        tracing::debug!("forward udp packet to tcp: {}B from {} to port {}", udp_packet.data.len(), udp_packet.source_addr, udp_packet.target_port);
                        if let Err(e) = tcp_stream.write_all_buf(&mut Buf::chain(&len.to_le_bytes()[..], &bytes[..])).await {
                        // if let Err(e) = tcp_stream.write_all_buf(&mut Buf::chain(&len.to_le_bytes()[..], &len.to_le_bytes()[..])).await {
                            tracing::error!("dropping tcp connection after failed write: {e}");
                            tcp = None;
                        } else if let Err(e) = tcp_stream.flush().await {
                            tracing::error!("dropping tcp connection after failed flush: {e}");
                            tcp = None;
                        }
                    } else {
                        tracing::debug!("dropping udp packet without a tcp peer");
                    }
                    udp_receivers.spawn(recv_sock.recv_from(buf));
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
                        tracing::debug!(num_bytes = len, "forward tcp packet to udp");
                        let tail = &rest[4..];
                        if tail.len() < len {
                            break;
                        }
                        let msg = &tail[..len];
                        let (udp_packet, _len): (UdpPacketWrapper, usize)  = bincode::decode_from_slice(msg, config).unwrap();
                        rest = &tail[len..];
                        let send_sock = if self.nat_table.contains_right(&udp_packet.source_addr) {
                             self.udp_source_sockets.get(self.nat_table.get_by_right(&udp_packet.source_addr).unwrap()).unwrap()
                        } else {
                                let udp_sock_tmp = UdpSocket::bind(SocketAddr::new(self.udp_ip_bind, udp_packet.source_addr.port())).await.unwrap_or(UdpSocket::bind(SocketAddr::new(self.udp_ip_bind, 0)).await.unwrap());
                                let local_port = self.add_udp_sock(udp_sock_tmp, &mut udp_receivers);
                                self.nat_table.insert(local_port, udp_packet.source_addr);
                                self.udp_source_sockets.get(&local_port).unwrap()
                        };
                        tracing::debug!("received udp packet over tcp: {}B from {} to port {}", udp_packet.data.len(), udp_packet.source_addr, udp_packet.target_port);
                        send_sock.send_to(&udp_packet.data, SocketAddr::new(self.udp_ip_peer, udp_packet.target_port)).await;
                    }

                    if rest.is_empty() {
                        tcp_buf.clear();
                    } else {
                        tracing::debug!(n = rest.len(), "bytes left over in tcp receive buffer");
                        let keep = tcp_buf.len() - rest.len();
                        tcp_buf.drain(..keep);
                    }
                }
            }
        }
    }
}
