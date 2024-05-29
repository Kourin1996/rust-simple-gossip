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

#[derive(Debug)]
pub struct DiscoveryService {
    // my server address
    my_address: SocketAddr,
    state: Arc<SharedState>,
}

#[derive(Debug)]
pub(self) struct SharedState {
    connection_manager: ConnectionManager,
    // maps that holds peer event subscription
    peer_subscription_map: RwLock<HashMap<SocketAddr, PeerSubscriptionEntity>>,
    // known peers
    known_peers: Arc<RwLock<HashSet<SocketAddr>>>,
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
                known_peers: Arc::new(RwLock::new(HashSet::new())),
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
                    let event = tokio::select! {
                        _ = ct.cancelled() => {
                            break;
                        }
                        Some(event) = peer_event_rx.recv() => {
                            event
                        }
                    };

                    tracing::debug!("Received peer event: {:?}", event);

                    match event {
                        PeerEvent::Connected(peer_address, peer) => {
                            DiscoveryService::handle_connected(
                                state.clone(),
                                peer,
                                peer_address,
                                my_address,
                                cancellation_token.clone(),
                            )
                            .await;
                        }
                        _ => {}
                    }
                }

                tracing::debug!("run_peer_event_handler finished");
            }
        });
    }

    async fn handle_connected(
        state: Arc<SharedState>,
        peer: Peer,
        peer_address: SocketAddr,
        my_address: SocketAddr,
        cancellation_token: CancellationToken,
    ) {
        state.known_peers.write().await.insert(peer_address);

        DiscoveryService::run_discovery_request_handler(
            my_address,
            state.known_peers.clone(),
            peer.clone(),
            cancellation_token.clone(),
        )
        .await;

        DiscoveryService::request_peers_info(
            state.clone(),
            my_address,
            peer.clone(),
            cancellation_token.clone(),
        )
        .await;
    }

    async fn run_discovery_request_handler(
        my_addr: SocketAddr,
        known_peers: Arc<RwLock<HashSet<SocketAddr>>>,
        mut peer: Peer,
        cancellation_token: CancellationToken,
    ) {
        tracing::debug!("spawning run_discovery_request_handler");

        let (tx, rx) = mpsc::channel::<Message>();
        let mut rx = convert_mpsc_channel_to_tokio_channel(rx);

        tokio::spawn({
            let id = peer.subscribe(tx).await;
            let address = peer.address().await.unwrap();

            tracing::debug!("Subscribed to peer message: {}", address);

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
                                    tracing::debug!("Received DiscoveryRequest from {}", sender);

                                    sender
                                }
                                _ => {
                                    continue;
                                }
                            }
                        }
                    };

                    DiscoveryService::reply_discovery_response(
                        peer.clone(),
                        sender,
                        my_addr,
                        known_peers.clone(),
                    )
                    .await;
                }

                peer.unsubscribe(id).await;
            }
        });
    }

    async fn reply_discovery_response(
        peer: Peer,
        sender: String,
        my_address: SocketAddr,
        known_peers: Arc<RwLock<HashSet<SocketAddr>>>,
    ) {
        let sender = sender.parse().unwrap();
        let peers = known_peers
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
            .send_message(my_address, MessageBody::DiscoveryResponse { peers })
            .await;

        tracing::debug!("Sent DiscoveryResponse to {}, num={}", sender, peer_num);
    }

    // TODO: periodically send DiscoveryRequest to the peers

    async fn request_peers_info(
        state: Arc<SharedState>,
        my_addr: SocketAddr,
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
                    _ = peer.send_message(my_addr, MessageBody::DiscoveryRequest {}) => {}
                }

                tracing::debug!("Sent DiscoveryRequest to {}", peer_address);

                // await for the response
                loop {
                    let (sender, peers) = tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                        msg = rx.recv() => {
                            match msg {
                                Some(Message {
                                    sender,
                                    body: MessageBody::DiscoveryResponse { peers, .. },
                                }) => {
                                    (sender, peers)
                                },
                                None => {
                                    break
                                }
                                _ => {
                                    continue;
                                }
                            }
                        }
                    };

                    if sender != peer_address.to_string() {
                        continue;
                    }

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

                DiscoveryService::unsubscribe_message(state, peer_address).await;
            }
        });
    }

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

    async fn unsubscribe_message(state: Arc<SharedState>, addr: SocketAddr) {
        let res = state.peer_subscription_map.write().await.remove(&addr);
        match res {
            Some(PeerSubscriptionEntity {
                index, mut peer, ..
            }) => {
                let ok = peer.unsubscribe(index).await;
                if !ok {
                    tracing::warn!("Failed to unsubscribe peer message because the subscription has been already removed");
                }
            }
            None => {}
        }
    }
}
