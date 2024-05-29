use connection_manager::connection_manager::ConnectionManager;
use discovery::discovery::DiscoveryService;
use message::message::MessageBody;
use std::net::SocketAddr;
use tokio::signal;
use tokio_util::sync::CancellationToken;

pub struct GossipApp {
    port: u16,
    broadcast_period: u32,
    initial_peers: Vec<String>,
}

impl GossipApp {
    pub fn new(port: u16, broadcast_period: u32, initial_peers: Vec<String>) -> Self {
        GossipApp {
            port,
            broadcast_period,
            initial_peers,
        }
    }

    pub async fn run(&self) {
        tracing::debug!(
            "Gossip Server starting, port={}, period={}, initial peers=[{}]",
            self.port,
            self.broadcast_period,
            self.initial_peers.join(", ")
        );

        let tcp_listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", self.port))
            .await
            .expect("failed to bind to port");
        let my_address = tcp_listener.local_addr().unwrap();

        let cancellation_token = CancellationToken::new();

        let connection_manager = ConnectionManager::new(my_address);

        self.run_connection_manager_task(
            connection_manager.clone(),
            tcp_listener,
            self.initial_peers.clone(),
            cancellation_token.clone(),
        )
        .await;

        self.run_discovery_task(
            connection_manager.clone(),
            my_address,
            cancellation_token.clone(),
        )
        .await;

        self.run_broadcast_task(
            connection_manager.clone(),
            my_address,
            self.broadcast_period,
            cancellation_token.clone(),
        )
        .await;

        signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c event");

        tracing::info!("Received ctrl-c signal, shutting down");

        cancellation_token.cancel();

        tracing::info!("Graceful shutdown completed, bye");
    }

    async fn run_connection_manager_task(
        &self,
        connection_manager: ConnectionManager,
        tcp_listener: tokio::net::TcpListener,
        initial_peers: Vec<String>,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn({
            let connection_manager = connection_manager.clone();
            let cancellation_token = cancellation_token.clone();
            let initial_peers: Vec<_> = initial_peers
                .iter()
                .map(|s| {
                    s.parse::<SocketAddr>()
                        .expect("Failed to parse peer address")
                })
                .collect();

            async move {
                connection_manager
                    .run(tcp_listener, cancellation_token.clone())
                    .await;

                for peer in initial_peers {
                    let res = connection_manager
                        .connect(peer, cancellation_token.clone())
                        .await;
                    if let Err(e) = res {
                        tracing::error!("Failed to connect to peer: {}", e);
                    }
                }
            }
        });
    }

    async fn run_discovery_task(
        &self,
        connection_manager: ConnectionManager,
        my_address: SocketAddr,
        cancellation_token: CancellationToken,
    ) {
        let discovery = DiscoveryService::new(connection_manager.clone(), my_address);
        discovery.run(cancellation_token).await
    }

    async fn run_broadcast_task(
        &self,
        connection_manager: ConnectionManager,
        my_address: SocketAddr,
        period: u32,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn({
            async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(period as u64)) => {
                            tracing::debug!("Broadcasting message to all peers");
                        }
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }

                    let peers = connection_manager.peers().await;
                    let peer_num = peers.len();

                    let futures: Vec<_> = peers
                        .into_iter()
                        .map(|(peer_addr, peer)| {
                            let peer = peer.clone();
                            tokio::spawn(async move {
                                peer.send_message(
                                    my_address,
                                    MessageBody::GossipBroadcast {
                                        message: "Hello, world".to_string(),
                                    },
                                )
                                .await
                                .expect("Failed to send message");

                                tracing::debug!("Message sent to peer: {}", peer_addr);
                            })
                        })
                        .collect();

                    futures::future::join_all(futures).await;

                    tracing::info!("Broadcasted message to {} peers", peer_num);
                }
            }
        });
    }
}
