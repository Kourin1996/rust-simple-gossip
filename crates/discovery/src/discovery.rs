use connection_manager::connection_manager::{ConnectionManager, PeerEvent};
use connection_manager::peer::Peer;
use message::message::{Message, MessageBody};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::mpsc;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use utils::channel::convert_mpsc_channel_to_tokio_channel;

// DiscoveryService is a service that is responsible for communicating with connected peers and finding new peers
#[derive(Debug)]
pub struct DiscoveryService {
    // my server address
    my_address: SocketAddr,
    state: Arc<SharedState>,
}

#[derive(Debug)]
pub(self) struct SharedState {
    // connection manager
    connection_manager: ConnectionManager,
    // maps that holds peer event subscription
    peer_subscription_map: RwLock<HashMap<SocketAddr, PeerSubscriptionEntity>>,
    // known peers
    known_peers: RwLock<HashSet<SocketAddr>>,
}

#[derive(Debug)]
struct PeerSubscriptionEntity {
    index: usize,
    peer: Peer,
}

impl DiscoveryService {
    pub fn new(connection_manager: ConnectionManager, my_address: SocketAddr) -> Self {
        Self {
            my_address,
            state: Arc::new(SharedState {
                connection_manager,
                peer_subscription_map: RwLock::new(HashMap::new()),
                known_peers: RwLock::new(HashSet::new()),
            }),
        }
    }

    // spawn the discovery service
    pub async fn run(&self, cancellation_token: CancellationToken) {
        self.run_peer_event_handler(cancellation_token).await;
    }

    // spawn the peer event handler
    async fn run_peer_event_handler(&self, cancellation_token: CancellationToken) {
        tracing::debug!("spawning run_peer_event_handler");

        tokio::spawn({
            let state = self.state.clone();
            let my_address = self.my_address;

            async move {
                let (peer_event_tx, peer_event_rx) = mpsc::channel();

                state.connection_manager.subscribe(peer_event_tx).await;

                let mut peer_event_rx = convert_mpsc_channel_to_tokio_channel(peer_event_rx);

                loop {
                    tracing::debug!("Waiting for peer event");

                    let ct = cancellation_token.clone();
                    let (peer_address, peer) = tokio::select! {
                        _ = ct.cancelled() => {
                            break;
                        }
                        Some(event) = peer_event_rx.recv() => {
                            match event {
                                PeerEvent::Connected(peer_address, peer) => {
                                    (peer_address, peer)
                                }
                                _ => {
                                    continue;
                                }
                            }
                        }
                    };

                    Self::handle_connected(
                        state.clone(),
                        peer,
                        peer_address,
                        my_address,
                        cancellation_token.clone(),
                    )
                    .await;
                }

                tracing::debug!("run_peer_event_handler finished");
            }
        });
    }

    // handler for peer connected event
    // register the peer to the known peers, send discovery request, and run handler for discovery request
    async fn handle_connected(
        state: Arc<SharedState>,
        peer: Peer,
        peer_address: SocketAddr,
        my_address: SocketAddr,
        cancellation_token: CancellationToken,
    ) {
        state.known_peers.write().await.insert(peer_address);

        DiscoveryService::run_discovery_request_handler(
            state.clone(),
            my_address,
            peer.clone(),
            cancellation_token.clone(),
        )
        .await;

        DiscoveryService::request_peers_info(
            state.clone(),
            peer.clone(),
            cancellation_token.clone(),
        )
        .await;
    }

    // handler for discovery request
    async fn run_discovery_request_handler(
        state: Arc<SharedState>,
        my_addr: SocketAddr,
        mut peer: Peer,
        cancellation_token: CancellationToken,
    ) {
        tracing::debug!("spawning run_discovery_request_handler");

        tokio::spawn({
            let state = state.clone();

            let (tx, rx) = mpsc::channel::<Message>();
            let mut rx = convert_mpsc_channel_to_tokio_channel(rx);

            let id = peer.subscribe(tx).await;

            async move {
                loop {
                    let sender = tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            peer.unsubscribe(id).await;
                            break;
                        }
                        msg = rx.recv() => {
                            match msg {
                                Some(Message{sender, body: MessageBody::DiscoveryRequest {},..}) => {
                                    sender
                                }
                                None => {
                                    break;
                                }
                                _ => {
                                    continue;
                                }
                            }
                        }
                    };

                    tracing::debug!("Received DiscoveryRequest from {}", sender);

                    DiscoveryService::reply_discovery_response(
                        state.clone(),
                        peer.clone(),
                        sender,
                        my_addr,
                    )
                    .await;
                }

                peer.unsubscribe(id).await;
            }
        });
    }

    // send known peers to the peer
    async fn reply_discovery_response(
        state: Arc<SharedState>,
        peer: Peer,
        sender: String,
        my_address: SocketAddr,
    ) {
        let sender = sender.parse().unwrap();
        let peers = state
            .known_peers
            .read()
            .await
            .iter()
            .filter_map(|addr| {
                if *addr != my_address && *addr != sender {
                    Some(addr.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let peer_num = peers.len();

        let _ = peer
            .send_message(MessageBody::DiscoveryResponse { peers })
            .await;

        tracing::debug!("Sent DiscoveryResponse to {}, num={}", sender, peer_num);
    }

    // send discovery request to the peer
    async fn request_peers_info(
        state: Arc<SharedState>,
        peer: Peer,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn({
            let peer_address = peer.address().await.expect("Failed to get peer address");

            async move {
                let mut rx =
                    DiscoveryService::subscribe_message(state.clone(), peer.clone(), peer_address)
                        .await;

                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        return;
                    }
                    _ = peer.send_message(MessageBody::DiscoveryRequest {}) => {}
                }

                tracing::debug!("Sent DiscoveryRequest to {}", peer_address);

                let res = DiscoveryService::await_for_discovery_response(
                    peer_address,
                    &mut rx,
                    cancellation_token.clone(),
                )
                .await;

                DiscoveryService::unsubscribe_message(state.clone(), peer_address).await;

                if let Some((sender, peers)) = res {
                    tracing::debug!(
                        "Received DiscoveryResponse from {} peers={:?}",
                        sender,
                        peers
                    );

                    DiscoveryService::connect_to_multiple_peers(
                        state.clone(),
                        peers,
                        cancellation_token.clone(),
                    )
                    .await;
                }
            }
        });
    }

    // connect to multiple peers
    async fn connect_to_multiple_peers(
        state: Arc<SharedState>,
        peers: Vec<String>,
        cancellation_token: CancellationToken,
    ) {
        let futures: Vec<_> = peers
            .iter()
            .filter_map(|peer| SocketAddr::from_str(peer).ok())
            .map(|peer_addr| {
                state
                    .connection_manager
                    .connect(peer_addr, cancellation_token.clone())
            })
            .collect();

        futures::future::join_all(futures).await;
    }

    // subscribe message from Peer
    async fn subscribe_message(
        state: Arc<SharedState>,
        mut peer: Peer,
        peer_address: SocketAddr,
    ) -> tokio::sync::mpsc::Receiver<Message> {
        let (tx, rx) = mpsc::channel::<Message>();
        let rx = convert_mpsc_channel_to_tokio_channel(rx);

        let index = peer.subscribe(tx).await;
        let _ = state.peer_subscription_map.write().await.insert(
            peer_address.clone(),
            PeerSubscriptionEntity {
                index,
                peer: peer.clone(),
            },
        );

        rx
    }

    // unsubscribe message from Peer
    async fn unsubscribe_message(state: Arc<SharedState>, addr: SocketAddr) {
        let res = state.peer_subscription_map.write().await.remove(&addr);

        if let Some(PeerSubscriptionEntity {
            index, mut peer, ..
        }) = res
        {
            let ok = peer.unsubscribe(index).await;
            if !ok {
                tracing::warn!("Failed to unsubscribe peer message because the subscription has been already removed");
            }
        }
    }

    // await for the discovery response
    async fn await_for_discovery_response(
        peer_address: SocketAddr,
        rx: &mut tokio::sync::mpsc::Receiver<Message>,
        cancellation_token: CancellationToken,
    ) -> Option<(String, Vec<String>)> {
        loop {
            let (sender, peers) = tokio::select! {
                _ = cancellation_token.cancelled() => {
                    return None;
                }
                msg = rx.recv() => {
                    match msg {
                        Some(Message {
                            sender,
                            body: MessageBody::DiscoveryResponse { peers, .. },
                        }) => {
                            (sender, peers)
                        },
                        // channel is closed
                        None => {
                            return None;
                        }
                        // other message, ignore
                        _ => {
                            continue;
                        }
                    }
                }
            };

            if sender != peer_address.to_string() {
                continue;
            }

            return Some((sender, peers));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use connection_manager::connection_manager::ConnectionManager;
    use connection_manager::connection_manager::PeerEvent::Connected;
    use message::message::MessageBody;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc::Receiver;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

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

    async fn subscribe_peer_message(peer: &mut Peer) -> (Receiver<Message>, usize) {
        let (tx, rx) = mpsc::channel();
        let rx = convert_mpsc_channel_to_tokio_channel(rx);
        let subscription = peer.subscribe(tx).await;

        return (rx, subscription);
    }

    #[tokio::test]
    async fn test_run_discovery_request_handler() {
        tokio::spawn(async {
            sleep(tokio::time::Duration::from_secs(10)).await;
            assert!(false);
        });

        let cancellation_token = CancellationToken::new();

        let (my_cm, my_address) = new_connection_manager_helper(cancellation_token.clone()).await;
        let (peer1_cm, peer1_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;
        let (_peer2_cm, peer2_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;

        let discovery_service = DiscoveryService::new(my_cm.clone(), my_address);
        discovery_service.run(cancellation_token.clone()).await;

        let (mut my_rx, my_subscription) = subscribe_connection_manager_event(&my_cm).await;
        let (mut peer1_rx, peer1_subscription) =
            subscribe_connection_manager_event(&peer1_cm).await;

        // connect to peer1 & peer2 from server
        DiscoveryService::connect_to_multiple_peers(
            discovery_service.state.clone(),
            vec![peer2_address.to_string(), peer1_address.to_string()],
            cancellation_token.clone(),
        )
        .await;

        // make sure the server is connected to peer1 & peer2
        let (mut peer1_received, mut peer2_received) = (false, false);
        loop {
            let event = tokio::select! {
                event = my_rx.recv() => {
                    event
                },
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                    panic!("Timeout");
                }
            };

            match event {
                Some(Connected(addr, _)) => {
                    assert!(addr == peer1_address || addr == peer2_address);
                    peer1_received |= addr == peer1_address;
                    peer2_received |= addr == peer2_address;
                }
                _ => {
                    panic!("Expected Connected event but got: {:?}", event);
                }
            }

            if peer1_received && peer2_received {
                break;
            }
        }

        tokio::select! {
            event = peer1_rx.recv() => {
                match event {
                    Some(Connected(addr, _)) => {
                        assert_eq!(addr, my_address);
                    }
                    _ => {
                        panic!("Expected Connected event but got: {:?}", event);
                    }
                }
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                panic!("Timeout");
            }
        }

        my_cm.unsubscribe(my_subscription).await;
        peer1_cm.unsubscribe(peer1_subscription).await;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // listen messages from peer1
        let mut peer1_server_peer = peer1_cm.peers().await.get(&my_address).unwrap().clone();
        let (mut rx, subscription) = subscribe_peer_message(&mut peer1_server_peer).await;

        // send discovery response from my server to peer1
        peer1_server_peer
            .send_message(MessageBody::DiscoveryRequest {})
            .await
            .expect("Failed to send message");

        // awaits for the discovery response
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(Message{sender, body: MessageBody::DiscoveryResponse { peers },..}) => {
                            assert_eq!(sender, my_address.to_string());
                            assert_eq!(peers, vec![peer2_address.to_string()]);
                            break
                        }
                        // ignore other messages
                        _ => {}
                    }
                },
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                    panic!("Timeout");
                }
            };
        }

        peer1_server_peer.unsubscribe(subscription).await;
    }

    #[tokio::test]
    async fn test_request_peers_info() {
        let cancellation_token = CancellationToken::new();

        let (my_cm, my_address) = new_connection_manager_helper(cancellation_token.clone()).await;
        let (peer1_cm, peer1_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;
        let (peer2_cm, peer2_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;

        let (mut peer1_rx, peer1_subscription) =
            subscribe_connection_manager_event(&peer1_cm).await;

        // connect to peer1 from server
        my_cm
            .connect(peer1_address, cancellation_token.clone())
            .await
            .expect("Failed to connect to peer1");

        // make sure the peer1 is connected to my server
        let event = peer1_rx.recv().await;
        assert!(event.is_some());
        match event.clone().unwrap() {
            PeerEvent::Connected(addr, _) => {
                assert_eq!(addr, my_address);
            }
            _ => {
                panic!("Expected Connected event but got: {:?}", event);
            }
        }
        peer1_cm.unsubscribe(peer1_subscription).await;

        // listen messages from peer1
        let mut peer1_server_peer = peer1_cm.peers().await.get(&my_address).unwrap().clone();
        let (mut peer1_rx, peer1_subscription) =
            subscribe_peer_message(&mut peer1_server_peer).await;

        // call request_peers_info
        tokio::spawn({
            let my_cm = my_cm.clone();

            async move {
                let peer1 = my_cm.peers().await.get(&peer1_address).unwrap().clone();

                let discovery_service = DiscoveryService::new(my_cm.clone(), my_address);
                DiscoveryService::request_peers_info(
                    discovery_service.state.clone(),
                    peer1,
                    cancellation_token.clone(),
                )
                .await;
            }
        });

        let msg = tokio::select! {
            msg = peer1_rx.recv() => {
                msg
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                panic!("Timeout");
            }
        };

        assert_eq!(
            msg,
            Some(Message {
                sender: my_address.to_string(),
                body: MessageBody::DiscoveryRequest {},
            })
        );

        let (mut peer2_rx, peer2_subscription) =
            subscribe_connection_manager_event(&peer2_cm).await;

        // send discovery response from peer1 to my server
        peer1_server_peer
            .send_message(MessageBody::DiscoveryResponse {
                peers: vec![peer2_address.to_string()],
            })
            .await
            .expect("Failed to send message");

        // make sure the peer2 is connected to my server
        tokio::select! {
            event = peer2_rx.recv() => {
                match event {
                    Some(Connected (sender, _)) => {
                        assert_eq!(sender, my_address);
                    }
                    _ => {
                        panic!("Expected DiscoveryResponse but got: {:?}", event);
                    }
                }
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                panic!("Timeout");
            }
        };

        peer1_server_peer.unsubscribe(peer1_subscription).await;
        peer2_cm.unsubscribe(peer2_subscription).await;
    }

    #[tokio::test]
    async fn test_connect_to_multiple_peers() {
        let cancellation_token = CancellationToken::new();

        let (my_cm, my_address) = new_connection_manager_helper(cancellation_token.clone()).await;
        let (_peer1_cm, peer1_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;
        let (_peer2_cm, peer2_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;
        let discovery_service = DiscoveryService::new(my_cm.clone(), my_address);

        DiscoveryService::connect_to_multiple_peers(
            discovery_service.state.clone(),
            vec![peer1_address.to_string(), peer2_address.to_string()],
            cancellation_token.clone(),
        )
        .await;

        let peers = my_cm.peers().await;

        assert!(peers.contains_key(&peer1_address));
        assert!(peers.contains_key(&peer2_address));
    }

    #[tokio::test]
    async fn test_await_for_discovery_response() {
        let cancellation_token = CancellationToken::new();

        let (my_cm, my_address) = new_connection_manager_helper(cancellation_token.clone()).await;
        let (peer1_cm, peer1_address) =
            new_connection_manager_helper(cancellation_token.clone()).await;

        // subscribe to peer1 connected events
        let (mut peer1_rx, peer1_subscription) =
            subscribe_connection_manager_event(&peer1_cm).await;

        // connect to peer1 from my server
        my_cm
            .connect(peer1_address, cancellation_token.clone())
            .await
            .expect("Failed to connect to peer1");

        let peer1 = my_cm.peers().await.get(&peer1_address).unwrap().clone();

        let discovery_service = DiscoveryService::new(my_cm.clone(), my_address);
        let mut my_rx = DiscoveryService::subscribe_message(
            discovery_service.state.clone(),
            peer1.clone(),
            peer1_address,
        )
        .await;

        // send discovery response from peer1 to my server
        let target_addresses = vec![
            String::from("192.168.0.1:8000"),
            String::from("192.168.0.1:8001"),
            String::from("192.168.0.1:8002"),
        ];
        tokio::spawn({
            let target_addresses = target_addresses.clone();

            async move {
                // make sure the peer1 is connected to my server
                peer1_rx.recv().await;

                let my_server = peer1_cm.peers().await.get(&my_address).unwrap().clone();

                my_server
                    .send_message(MessageBody::DiscoveryResponse {
                        peers: target_addresses,
                    })
                    .await
                    .expect("Failed to send message");

                peer1_cm.unsubscribe(peer1_subscription).await;
            }
        });

        // wait for the discovery response
        let res = DiscoveryService::await_for_discovery_response(
            peer1_address,
            &mut my_rx,
            cancellation_token.clone(),
        )
        .await;

        assert_eq!(res, Some((peer1_address.to_string(), target_addresses)));
    }
}
