use connection_manager::connection_manager::{ConnectionManager, PeerEvent};
use connection_manager::peer::Peer;
use discovery::discovery::DiscoveryService;
use message::message::MessageBody;
use rand::Rng;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use utils::channel::convert_mpsc_channel_to_tokio_channel;

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

        let cancellation_token = CancellationToken::new();

        tokio::spawn({
            let cancellation_token = cancellation_token.clone();

            async move {
                signal::ctrl_c()
                    .await
                    .expect("Failed to listen for ctrl-c event");

                tracing::info!("Received ctrl-c signal, shutting down");

                cancellation_token.cancel();
            }
        });

        self.setup_and_run(cancellation_token).await;
    }

    async fn setup_and_run(&self, cancellation_token: CancellationToken) {
        let tcp_listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", self.port))
            .await
            .expect("failed to bind to port");
        let my_address = tcp_listener.local_addr().unwrap();

        tracing::info!("My address is \"{}\"", my_address.to_string());

        let connection_manager = ConnectionManager::new(my_address);

        Self::run_connection_manager_task(
            connection_manager.clone(),
            tcp_listener,
            cancellation_token.clone(),
        )
        .await;

        Self::run_discovery_task(
            connection_manager.clone(),
            my_address,
            cancellation_token.clone(),
        )
        .await;

        Self::run_broadcast_task(
            connection_manager.clone(),
            self.broadcast_period,
            cancellation_token.clone(),
        )
        .await;

        Self::run_initial_peers_connection_task(
            connection_manager.clone(),
            self.initial_peers.clone(),
            cancellation_token.clone(),
        )
        .await;

        cancellation_token.cancelled().await;

        connection_manager.shutdown().await;

        tracing::info!("Graceful shutdown completed, bye");
    }

    // spawn a task for connection manager
    async fn run_connection_manager_task(
        connection_manager: ConnectionManager,
        tcp_listener: tokio::net::TcpListener,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn({
            async move {
                connection_manager
                    .run(tcp_listener, cancellation_token.clone())
                    .await;

                let (tx, rx) = mpsc::channel();
                let mut rx = convert_mpsc_channel_to_tokio_channel(rx);
                connection_manager.subscribe(tx).await;

                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                        event = rx.recv() => {
                            match event {
                                Some(PeerEvent::Connected(peer_address, peer)) => {
                                    tracing::info!("Connected to peer at \"{}\"", peer_address);

                                    Self::run_peer_message_reception_task(peer, cancellation_token.clone()).await;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        });
    }

    async fn run_peer_message_reception_task(
        mut peer: Peer,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn({
            async move {
                let (tx, rx) = mpsc::channel();
                let mut rx = convert_mpsc_channel_to_tokio_channel(rx);
                peer.subscribe(tx).await;

                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                        message = rx.recv() => {
                            match message {
                                Some(message) => {
                                    match message.body {
                                        MessageBody::GossipBroadcast { message: message_body } => {
                                            tracing::info!("Received message [{}] from \"{}\"", message_body, message.sender);
                                        }
                                        _ => {}
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        });
    }

    // spawn a task for discovery service
    async fn run_discovery_task(
        connection_manager: ConnectionManager,
        my_address: SocketAddr,
        cancellation_token: CancellationToken,
    ) {
        let discovery = DiscoveryService::new(connection_manager.clone(), my_address);
        discovery.run(cancellation_token).await
    }

    // spawn a task that broadcasts a message to all peers every `period` seconds
    async fn run_broadcast_task(
        connection_manager: ConnectionManager,
        period: u32,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn({
            async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(period as u64)) => {}
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }

                    let peers = connection_manager.peers().await;

                    Self::broadcast_message(peers).await;
                }
            }
        });
    }

    async fn run_initial_peers_connection_task(
        connection_manager: ConnectionManager,
        initial_peers: Vec<String>,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn({
            async move {
                let initial_peers: Vec<_> = initial_peers
                    .iter()
                    .map(|s| {
                        s.parse::<SocketAddr>()
                            .expect("Failed to parse peer address")
                    })
                    .collect();

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

    async fn broadcast_message(peers: HashMap<SocketAddr, Peer>) {
        let peer_num = peers.len();
        if peer_num == 0 {
            return;
        }

        let peer_addrs = peers
            .keys()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>();

        tracing::debug!("Broadcasting message to {} peers", peer_num);

        let msg = Self::generate_random_string(10);

        let futures: Vec<_> = peers
            .into_iter()
            .map(|(peer_addr, peer)| {
                let peer = peer.clone();
                tokio::spawn({
                    let msg = msg.clone();

                    async move {
                        peer.send_message(MessageBody::GossipBroadcast { message: msg })
                            .await
                            .unwrap();

                        tracing::debug!("Message sent to peer: {}", peer_addr);
                    }
                })
            })
            .collect();

        futures::future::join_all(futures).await;

        let formatted_addrs = peer_addrs
            .iter()
            .map(|s| format!("\"{}\"", s))
            .collect::<Vec<_>>()
            .join(", ");

        tracing::info!("Sending message [{}] to [{}]", msg, formatted_addrs);
    }

    fn generate_random_string(length: usize) -> String {
        let charset: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                           abcdefghijklmnopqrstuvwxyz\
                           0123456789";
        let mut rng = rand::thread_rng();

        let random_string: String = (0..length)
            .map(|_| {
                let idx = rng.gen_range(0..charset.len());
                charset[idx] as char
            })
            .collect();

        random_string
    }
}

#[cfg(test)]
mod tests {
    use crate::app::GossipApp;
    use connection_manager::connection_manager::{ConnectionManager, PeerEvent};
    use connection_manager::peer::Peer;
    use message::message::{Message, MessageBody};
    use std::net::SocketAddr;
    use std::sync::mpsc;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc::Receiver;
    use tokio_util::sync::CancellationToken;
    use utils::channel::convert_mpsc_channel_to_tokio_channel;

    async fn new_connection_manager_helper(
        cancellation_token: CancellationToken,
    ) -> (ConnectionManager, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connection_manager = ConnectionManager::new(address);

        connection_manager.run(listener, cancellation_token).await;

        return (connection_manager, address);
    }

    async fn subscribe_connection_manager_event(
        connection_manager: &ConnectionManager,
    ) -> (Receiver<PeerEvent>, usize) {
        let (tx, rx) = mpsc::channel();
        let rx = convert_mpsc_channel_to_tokio_channel(rx);
        let subscription = connection_manager.subscribe(tx).await;

        return (rx, subscription);
    }

    async fn subscribe_peer_message(mut peer: Peer) -> (Receiver<Message>, usize) {
        let (tx, rx) = mpsc::channel();
        let rx = convert_mpsc_channel_to_tokio_channel(rx);
        let subscription = peer.subscribe(tx).await;

        return (rx, subscription);
    }

    async fn connect(
        connection_manager_from: &ConnectionManager,
        connection_manager_to: &ConnectionManager,
        from_address: SocketAddr,
        to_address: SocketAddr,
    ) {
        let (mut event_rx, event_subscription) =
            subscribe_connection_manager_event(connection_manager_to).await;

        connection_manager_from
            .connect(to_address, CancellationToken::new())
            .await
            .unwrap();

        tokio::select! {
            event = event_rx.recv() => {
                match event {
                    Some(PeerEvent::Connected(address, _)) => {
                        assert_eq!(address, from_address);
                    }
                    _ => panic!("Unexpected event"),
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                panic!("Timeout waiting for peer to connect");
            }
        }

        connection_manager_to.unsubscribe(event_subscription).await;
    }

    #[tokio::test]
    async fn test_run_connection_manager_task() {
        let cancellation_token = CancellationToken::new();

        let (my_cm, _) = new_connection_manager_helper(cancellation_token.clone()).await;
        let (_peer1_cm, peer1_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;

        let (mut my_rx, my_subscription) = subscribe_connection_manager_event(&my_cm.clone()).await;

        // setup discovery tasks
        GossipApp::run_initial_peers_connection_task(
            my_cm.clone(),
            vec![peer1_address.to_string()],
            cancellation_token.clone(),
        )
        .await;

        tokio::select! {
            event = my_rx.recv() => {
                match event {
                    Some(PeerEvent::Connected(address, _)) => {
                        assert_eq!(address, peer1_address);
                    }
                    _ => panic!("Unexpected event"),
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                panic!("Timeout waiting for peer to connect");
            }
        }

        my_cm.unsubscribe(my_subscription).await;
    }

    #[tokio::test]
    async fn test_run_discovery_task() {
        let cancellation_token = CancellationToken::new();
        let (my_cm, my_address) = new_connection_manager_helper(cancellation_token.clone()).await;
        let (peer1_cm, peer1_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;
        let (peer2_cm, peer2_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;

        // setup discovery tasks
        GossipApp::run_discovery_task(my_cm.clone(), my_address, cancellation_token.clone()).await;
        GossipApp::run_discovery_task(peer1_cm.clone(), peer1_address, cancellation_token.clone())
            .await;
        GossipApp::run_discovery_task(peer2_cm.clone(), peer2_address, cancellation_token.clone())
            .await;

        // setup subscription for connection manager events
        let (mut peer1_rx, peer1_subscription) =
            subscribe_connection_manager_event(&peer1_cm.clone()).await;
        let (mut peer2_rx, peer2_subscription) =
            subscribe_connection_manager_event(&peer2_cm.clone()).await;

        // connect server and peer1, server and peer2
        connect(&my_cm, &peer1_cm, my_address, peer1_address).await;

        // wait for peer1 to connect to server
        tokio::select! {
            event = peer1_rx.recv() => {
                match event {
                    Some(PeerEvent::Connected(address, _)) => {
                        assert_eq!(address, my_address);
                    }
                    _ => panic!("Unexpected event"),
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                panic!("Timeout waiting for peer1 to connect");
            }
        }
        peer1_cm.unsubscribe(peer1_subscription).await;

        // connect server and peer2
        connect(&my_cm, &peer2_cm, my_address, peer2_address).await;

        // wait for peer2 to connect to peer1
        loop {
            tokio::select! {
                event = peer2_rx.recv() => {
                    match event {
                        Some(PeerEvent::Connected(address, _)) => {
                            if address == my_address {
                                continue;
                            }

                            assert_eq!(address, peer1_address);
                            break;
                        }
                        _ => panic!("Unexpected event"),
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    panic!("Timeout waiting for peer1 to connect");
                }
            }
        }
        peer2_cm.unsubscribe(peer2_subscription).await;
    }

    #[tokio::test]
    async fn test_run_broadcast_task() {
        let cancellation_token = CancellationToken::new();
        let (my_cm, my_address) = new_connection_manager_helper(cancellation_token.clone()).await;
        let (peer1_cm, peer1_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;
        let (peer2_cm, peer2_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;

        connect(&my_cm, &peer1_cm, my_address, peer1_address).await;
        connect(&my_cm, &peer2_cm, my_address, peer2_address).await;

        GossipApp::run_broadcast_task(my_cm.clone(), 1, cancellation_token.clone()).await;

        // Wait for broadcast message to reach peer1
        let peer1_peers = peer1_cm.peers().await;
        let peer1_server_peer = peer1_peers.get(&my_address).unwrap();
        let (mut peer1_rx, peer1_subscription) =
            subscribe_peer_message(peer1_server_peer.clone()).await;
        tokio::select! {
            message = peer1_rx.recv() => {
                match message {
                    Some(Message { sender, body: MessageBody::GossipBroadcast {..}}) => {
                        assert_eq!(sender, my_address.to_string());
                    }
                    _ => panic!("Unexpected message"),
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                panic!("Timeout waiting for message");
            }
        }
        peer1_server_peer
            .clone()
            .unsubscribe(peer1_subscription)
            .await;

        // Wait for broadcast message to reach peer2
        let peer2_peers = peer2_cm.peers().await;
        let peer2_server_peer = peer2_peers.get(&my_address).unwrap();
        let (mut peer2_rx, peer2_subscription) =
            subscribe_peer_message(peer2_server_peer.clone()).await;
        tokio::select! {
            message = peer2_rx.recv() => {
                match message {
                    Some(Message { sender, body: MessageBody::GossipBroadcast {..}}) => {
                        assert_eq!(sender, my_address.to_string());
                    }
                    _ => panic!("Unexpected message"),
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                panic!("Timeout waiting for message");
            }
        }
        peer2_server_peer
            .clone()
            .unsubscribe(peer2_subscription)
            .await;
    }
}
