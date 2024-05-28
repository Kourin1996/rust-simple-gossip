use connection_manager::connection_manager::{ConnectionManager, PeerEvent};
use connection_manager::peer::Peer;
use message::message::MessageBody;
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
    my_address: SocketAddr,
    state: Arc<SharedState>,
}

#[derive(Debug)]
pub(self) struct SharedState {
    connection_manager: ConnectionManager,
    peer_subscription_map: RwLock<HashMap<SocketAddr, PeerSubscriptionEntity>>,
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

    pub async fn run(&self, cancellation_token: CancellationToken) {
        self.run_peer_event_handler(cancellation_token).await;
    }

    async fn run_peer_event_handler(&self, cancellation_token: CancellationToken) {
        tracing::debug!("spawning run_peer_event_handler");

        tokio::spawn({
            let state = self.state.clone();
            let my_addr = self.my_address;

            async move {
                let (peer_event_tx, peer_event_rx) = mpsc::channel();

                state.connection_manager.subscribe(peer_event_tx).await;

                let mut peer_event_rx = convert_mpsc_channel_to_tokio_channel(peer_event_rx);

                loop {
                    tracing::debug!("Waiting for peer event");

                    let event = tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            tracing::debug!("Cancellation token is cancelled");
                            break;
                        }
                        Some(event) = peer_event_rx.recv() => {
                            event
                        }
                    };

                    tracing::debug!("Received peer event: {:?}", event);

                    match event {
                        PeerEvent::Connected(peer) => {
                            // TODO: create method
                            state.known_peers.write().await.insert(peer.address);

                            DiscoveryService::run_discovery_request_handler(
                                my_addr,
                                state.known_peers.clone(),
                                peer.clone(),
                                cancellation_token.clone(),
                            )
                            .await;

                            DiscoveryService::request_peers_info(
                                state.clone(),
                                my_addr,
                                peer,
                                cancellation_token.clone(),
                            )
                            .await;
                        }
                        PeerEvent::Disconnected(peer) => {
                            DiscoveryService::unsubscribe_message(state.clone(), peer.address)
                                .await;
                        }
                    }
                }

                tracing::debug!("run_peer_event_handler finished");
            }
        });
    }

    async fn run_discovery_request_handler(
        my_addr: SocketAddr,
        known_peers: Arc<RwLock<HashSet<SocketAddr>>>,
        mut peer: Peer,
        cancellation_token: CancellationToken,
    ) {
        tracing::debug!("spawning run_discovery_request_handler");
        let (tx, rx) = mpsc::channel::<MessageBody>();
        let mut rx = convert_mpsc_channel_to_tokio_channel(rx);

        tokio::spawn({
            let id = peer.subscribe(tx).await;

            tracing::debug!("Subscribed to peer message: {}", peer.address);

            async move {
                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            peer.unsubscribe(id).await;
                            break;
                        }
                        msg = rx.recv() => {
                            match msg {
                                Some(MessageBody::DiscoveryRequest { sender }) => {
                                    tracing::debug!("Received DiscoveryRequest from {}", sender);

                                    let peers = known_peers
                                        .read()
                                        .await
                                        .iter()
                                        .filter_map(|addr| {
                                            if *addr != my_addr && *addr != peer.address {
                                                Some(addr.to_string())
                                            } else {
                                                None
                                            }})
                                        .collect::<Vec<_>>();

                                    let response_message = MessageBody::DiscoveryResponse {
                                        sender: my_addr.to_string(),
                                        peers,
                                    }
                                    .encode();

                                    let _ = peer.send_message(response_message.as_slice()).await;

                                    tracing::debug!("Sent DiscoveryResponse to {}", sender);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        });
    }

    // TODO: periodically send DiscoveryRequest to the peers

    async fn request_peers_info(
        state: Arc<SharedState>,
        my_addr: SocketAddr,
        mut peer: Peer,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn({
            let peer_addr = peer.address;

            async move {
                let (tx, rx) = mpsc::channel::<MessageBody>();

                let subscription_index = peer.subscribe(tx).await;
                let _ = state.peer_subscription_map.write().await.insert(
                    peer_addr.clone(),
                    PeerSubscriptionEntity {
                        index: subscription_index,
                        peer: peer.clone(),
                    },
                );

                let request_message = MessageBody::DiscoveryRequest {
                    sender: my_addr.to_string(),
                }
                .encode();

                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        return;
                    }
                    _ = peer.send_message(request_message.as_slice()) => {}
                }

                tracing::debug!("Sent DiscoveryRequest to {}", peer_addr);

                let mut rx = convert_mpsc_channel_to_tokio_channel(rx);

                // await for the response
                loop {
                    let msg = tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                        msg = rx.recv() => {
                            match msg {
                                Some(msg) => msg,
                                None => {
                                    break;
                                }
                            }
                        }
                    };

                    if msg.sender() != peer_addr.to_string() {
                        continue;
                    }

                    match msg {
                        MessageBody::DiscoveryResponse { sender, peers } => {
                            tracing::debug!(
                                "Received DiscoveryResponse from {} peers={:?}",
                                sender,
                                peers
                            );

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
                        _ => {}
                    }
                }

                DiscoveryService::unsubscribe_message(state, peer_addr).await;
            }
        });
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
