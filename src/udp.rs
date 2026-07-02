use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::context::RPCContext;
use crate::rpc::{make_success_reply, proc_unavail_reply_message, prog_mismatch_reply_message, rpc_body, rpc_msg};
use crate::rpcwire::handle_rpc;
use crate::transaction_tracker::TransactionTracker;
use crate::vfs::NFSFileSystem;
use crate::xdr::XDR;

/// A NFS UDP Listener
///
/// Per RFC 1057 / RFC 5531, RPC over UDP uses datagrams directly —
/// each datagram contains exactly one complete RPC message with zero
/// record-marking overhead.
pub struct NFSUdpListener<T: NFSFileSystem + Send + Sync + 'static> {
    socket: UdpSocket,
    port: u16,
    arcfs: Arc<T>,
    mount_signal: Option<mpsc::Sender<bool>>,
    export_name: Arc<String>,
    transaction_tracker: Arc<TransactionTracker>,
}

#[async_trait]
pub trait NFSUdp: Send + Sync {
    /// Gets the true listening port. Useful if the bound port number is 0
    fn get_listen_port(&self) -> u16;

    /// Gets the true listening IP.
    fn get_listen_ip(&self) -> IpAddr;

    /// Sets a mount listener. A "true" signal will be sent on a mount
    /// and a "false" will be sent on an unmount
    fn set_mount_listener(&mut self, signal: mpsc::Sender<bool>);

    /// Loops forever handling all incoming UDP datagrams.
    async fn handle_forever(&self) -> io::Result<()>;
}

impl<T: NFSFileSystem + Send + Sync + 'static> NFSUdpListener<T> {
    /// Binds to an ipstr of the form [ip address]:port. For instance
    /// "127.0.0.1:12000". fs is an instance of an implementation
    /// of NFSFileSystem.
    pub async fn bind(ipstr: &str, fs: T) -> io::Result<NFSUdpListener<T>> {
        let (ip, port) = ipstr
            .split_once(':')
            .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "IP Address must be of form ip:port"))?;
        let port = port
            .parse::<u16>()
            .map_err(|_| io::Error::new(io::ErrorKind::AddrNotAvailable, "Port not in range 0..=65535"))?;

        let arcfs: Arc<T> = Arc::new(fs);
        let ipstr = format!("{ip}:{port}");
        let socket = UdpSocket::bind(&ipstr).await?;
        info!("Listening on UDP {:?}", &ipstr);

        let port = match socket.local_addr().unwrap() {
            SocketAddr::V4(s) => s.port(),
            SocketAddr::V6(s) => s.port(),
        };

        Ok(NFSUdpListener {
            socket,
            port,
            arcfs,
            mount_signal: None,
            export_name: Arc::from("/".to_string()),
            transaction_tracker: Arc::new(TransactionTracker::new(Duration::from_secs(60))),
        })
    }

    /// Sets an optional NFS export name.
    ///
    /// - `export_name`: The desired export name without slashes.
    ///
    /// Example: Name `foo` results in the export path `/foo`.
    /// Default path is `/` if not set.
    pub fn with_export_name<S: AsRef<str>>(&mut self, export_name: S) {
        self.export_name = Arc::new(format!("/{}", export_name.as_ref().trim_end_matches('/').trim_start_matches('/')))
    }

    /// Spawns a separate rpcbind/portmap UDP listener on `bind_addr` (e.g. "0.0.0.0:111").
    ///
    /// Only portmap requests (program 100000) are handled. GETPORT always returns
    /// the main NFS port. Other RPC programs receive PROG_UNAVAIL.
    pub fn spawn_rpcbind(&self, bind_addr: &str) -> io::Result<()> {
        let bind_addr = bind_addr.to_string();
        let nfs_port = self.port;
        let tracker = self.transaction_tracker.clone();

        std::thread::Builder::new().name("rpcbind".into()).spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

            rt.block_on(async move {
                let socket = match UdpSocket::bind(&bind_addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("rpcbind: failed to bind {}: {:?}", bind_addr, e);
                        return;
                    },
                };
                info!("RPCBIND listening on UDP {}", bind_addr);

                const PORTMAP_PROGRAM: u32 = 100000;
                const PORTMAP_VERSION: u32 = 2;

                let mut buf = vec![0u8; 65536];
                loop {
                    let (n, client_addr) = match socket.recv_from(&mut buf).await {
                        Ok(v) => v,
                        Err(e) => {
                            error!("rpcbind recv error: {:?}", e);
                            continue;
                        },
                    };

                    let mut msg = rpc_msg::default();
                    if msg.deserialize(&mut std::io::Cursor::new(&buf[..n])).is_err() {
                        continue;
                    }
                    let xid = msg.xid;
                    let client_str = client_addr.to_string();

                    let call = match msg.body {
                        rpc_body::CALL(c) => c,
                        _ => continue,
                    };

                    // Only serve portmap
                    if call.prog != PORTMAP_PROGRAM {
                        debug!("rpcbind: ignoring program {}", call.prog);
                        let mut reply = Vec::new();
                        let _ = proc_unavail_reply_message(xid).serialize(&mut reply);
                        let _ = socket.send_to(&reply, client_addr).await;
                        continue;
                    }
                    if call.vers != PORTMAP_VERSION {
                        debug!("rpcbind: wrong version {}", call.vers);
                        let mut reply = Vec::new();
                        let _ = prog_mismatch_reply_message(xid, PORTMAP_VERSION).serialize(&mut reply);
                        let _ = socket.send_to(&reply, client_addr).await;
                        continue;
                    }
                    if tracker.is_retransmission(xid, &client_str) {
                        debug!("rpcbind: retransmission from {} — no reply", client_str);
                        continue;
                    }

                    let mut reply = Vec::new();
                    match call.proc {
                        0 => {
                            // PMAPPROC_NULL
                            let _ = make_success_reply(xid).serialize(&mut reply);
                        },
                        3 => {
                            // PMAPPROC_GETPORT — always return NFS port
                            debug!("rpcbind: GETPORT xid={} -> port={}", xid, nfs_port);
                            let _ = make_success_reply(xid).serialize(&mut reply);
                            let _ = (nfs_port as u32).serialize(&mut reply);
                        },
                        _ => {
                            let _ = proc_unavail_reply_message(xid).serialize(&mut reply);
                        },
                    }
                    let _ = socket.send_to(&reply, client_addr).await;
                    tracker.mark_processed(xid, &client_str);
                }
            });
        })?;

        Ok(())
    }
}

#[async_trait]
impl<T: NFSFileSystem + Send + Sync + 'static> NFSUdp for NFSUdpListener<T> {
    fn get_listen_port(&self) -> u16 {
        self.socket.local_addr().unwrap().port()
    }

    fn get_listen_ip(&self) -> IpAddr {
        self.socket.local_addr().unwrap().ip()
    }

    fn set_mount_listener(&mut self, signal: mpsc::Sender<bool>) {
        self.mount_signal = Some(signal);
    }

    /// Main UDP receive loop.
    ///
    /// Each UDP datagram contains one complete RPC message (no RM framing).
    /// We pass the datagram bytes directly to `handle_rpc` via a Cursor,
    /// and send the serialized reply back as a single datagram.
    async fn handle_forever(&self) -> io::Result<()> {
        // 65536 covers the maximum theoretical UDP datagram size
        let mut buf = vec![0u8; 65536];
        loop {
            let (n, client_addr) = self.socket.recv_from(&mut buf).await?;

            let context = RPCContext {
                local_port: self.port,
                client_addr: client_addr.to_string(),
                auth: crate::rpc::auth_unix::default(),
                vfs: self.arcfs.clone(),
                mount_signal: self.mount_signal.clone(),
                export_name: self.export_name.clone(),
                transaction_tracker: self.transaction_tracker.clone(),
            };

            let mut write_buf: Vec<u8> = Vec::new();
            let result = handle_rpc(&mut std::io::Cursor::new(&buf[..n]), &mut write_buf, context).await;

            match result {
                Ok(true) => {
                    if let Err(e) = self.socket.send_to(&write_buf, client_addr).await {
                        error!("UDP send error to {}: {:?}", client_addr, e);
                    }
                },
                Ok(false) => {
                    // Retransmission detected, no reply sent
                    debug!("Retransmission from {} — no reply", client_addr);
                },
                Err(e) => {
                    error!("RPC error from {}: {:?}", client_addr, e);
                },
            }
        }
    }
}
