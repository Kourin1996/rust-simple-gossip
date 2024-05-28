use connection_manager::connection_manager::ConnectionManager;
use discovery::discovery::DiscoveryService;
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
            .expect("Failed to bind to port");
        let my_address = tcp_listener.local_addr().unwrap();

        let cancellation_token = CancellationToken::new();

        let connection_manager = ConnectionManager::new(self.port);

        tokio::spawn({
            let connection_manager = connection_manager.clone();
            let cancellation_token = cancellation_token.clone();
            let initial_peers = self
                .initial_peers
                .iter()
                .map(|s| s.parse().expect("Failed to parse peer address"))
                .collect();

            async move {
                connection_manager.run(tcp_listener, initial_peers, cancellation_token);
            }
        });

        let discovery = DiscoveryService::new(connection_manager.clone(), my_address);
        tokio::spawn({
            let cancellation_token = cancellation_token.clone();

            async move {
                discovery.run(cancellation_token).await;
            }
        });

        tokio::spawn({
            let connection_manager = connection_manager.clone();
            let cancellation_token = cancellation_token.clone();
            let period = self.broadcast_period;

            async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(period as u64)) => {
                            tracing::debug!("Broadcasting message to all peers");

                            for (addr, peer) in connection_manager.peers().await {
                                let peer = peer.clone();
                                tokio::spawn(async move {
                                    peer.send_message(b"Hello, world!").await.expect("Failed to send message");

                                    tracing::debug!("Message sent to peer: {}", addr);
                                });
                            }
                        }
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }
                }
            }
        });

        // tokio::spawn({
        //     let connection_manager = connection_manager.clone();
        //     let (peer_event_tx, mut peer_event_rx) = mpsc::channel(1);
        //     let cancellation_token = cancellation_token.clone();
        //
        //     async move {
        //         tracing::debug!("Subscribing to peer events");
        //         connection_manager.subscribe(peer_event_tx).await;
        //
        //         loop {
        //             tokio::select! {
        //                 _ = cancellation_token.cancelled() => {
        //                     break;
        //                 }
        //                 event = peer_event_rx.recv() => {
        //                     if let Some(event) = event {
        //                         tracing::debug!("Received peer event: {:?}", event);
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // });

        signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c event");

        tracing::info!("Received ctrl-c signal, shutting down");

        cancellation_token.cancel();

        tracing::info!("Graceful shutdown completed, bye");
    }
}
