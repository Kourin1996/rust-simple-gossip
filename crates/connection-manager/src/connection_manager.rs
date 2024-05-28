use crate::peer::Peer;
use message::message::MessageBody;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub enum PeerEvent {
    Connected(SocketAddr, Peer),
    Disconnected(SocketAddr, Peer),
}

impl fmt::Debug for PeerEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PeerEvent::Connected(address, _) => write!(f, "Connected({})", address),
            PeerEvent::Disconnected(address, _) => write!(f, "Disconnected({})", address),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionManager {
    state: Arc<SharedState>,
}

#[derive(Debug)]
pub struct SharedState {
    my_address: SocketAddr,
    peer_map: RwLock<HashMap<SocketAddr, Peer>>,
    disconnection_tx: tokio_mpsc::Sender<SocketAddr>,
    disconnection_rx: Mutex<tokio_mpsc::Receiver<SocketAddr>>,
    peer_event_subscribers: RwLock<Vec<mpsc::Sender<PeerEvent>>>,
}

impl ConnectionManager {
    pub fn new(my_address: SocketAddr) -> Self {
        let (disconnection_tx, disconnection_rx) = tokio_mpsc::channel(1);

        Self {
            state: Arc::new(SharedState {
                my_address,
                peer_map: RwLock::new(HashMap::new()),
                disconnection_tx,
                disconnection_rx: Mutex::new(disconnection_rx),
                peer_event_subscribers: RwLock::new(vec![]),
            }),
        }
    }

    pub async fn run(
        &self,
        tcp_listener: TcpListener,
        initial_peers: Vec<SocketAddr>,
        cancellation_token: CancellationToken,
    ) {
        self.run_peer_reception(tcp_listener, cancellation_token.clone());

        self.run_peer_disconnection_handler(cancellation_token.clone());

        self.run_initial_connection(initial_peers, cancellation_token);
    }

    pub async fn peers(&self) -> HashMap<SocketAddr, Peer> {
        self.state.peer_map.read().await.clone()
    }

    pub async fn connect(&self, addr: SocketAddr, cancellation_token: CancellationToken) {
        if self.state.peer_map.read().await.get(&addr).is_some() {
            return;
        }

        ConnectionManager::connect_impl(
            addr,
            self.state.my_address,
            &self.state.peer_map,
            &self.state.peer_event_subscribers,
            self.state.disconnection_tx.clone(),
            cancellation_token,
        )
        .await;
    }

    pub async fn subscribe(&self, sender: mpsc::Sender<PeerEvent>) {
        self.state.peer_event_subscribers.write().await.push(sender);
        tracing::debug!("Subscribed to peer events");
    }

    fn run_peer_reception(&self, tcp_listener: TcpListener, cancellation_token: CancellationToken) {
        tokio::spawn({
            let state = self.state.clone();

            async move {
                let my_address = state.my_address;

                loop {
                    let ct = cancellation_token.clone();
                    let peer_result = tokio::select! {
                        _ = ct.cancelled() => {
                            return;
                        }
                        peer = tcp_listener.accept() => {
                            peer
                        }
                    };

                    let (stream, peer_ephemeral_address) = match peer_result {
                        Ok(res) => res,
                        Err(_) => {
                            tracing::warn!("Failed to accept connection, skip");
                            continue;
                        }
                    };

                    tracing::debug!("Accepted connection from: {}", peer_ephemeral_address);

                    let mut peer = Peer::new(stream, state.disconnection_tx.clone());

                    peer.send_message(my_address, MessageBody::Handshake {})
                        .await
                        .unwrap();

                    tracing::debug!("Handshake sent to peer: {:?}", peer_ephemeral_address);

                    peer.run(cancellation_token.clone()).await;

                    peer.await_handshake().await;

                    let address = peer.address().await;
                    let address = match address {
                        Some(address) => address,
                        None => {
                            tracing::warn!("Failed to get address from peer, closing connection");
                            continue;
                        }
                    };

                    tracing::debug!("Handshake received from peer: {:?}", address);

                    state.peer_map.write().await.insert(address, peer.clone());

                    let subscribers = state.peer_event_subscribers.read().await;
                    for subscriber in subscribers.iter() {
                        let _ = subscriber.send(PeerEvent::Connected(address, peer.clone()));
                    }
                    tracing::debug!("Published ConnectedEvent, num={}", subscribers.len());

                    tracing::debug!("Incoming connection established: {}", address);
                }
            }
        });
    }

    fn run_peer_disconnection_handler(&self, cancellation_token: CancellationToken) {
        tokio::spawn({
            let state = self.state.clone();

            async move {
                let mut disconnection_rx = state.disconnection_rx.lock().await;

                loop {
                    let address = tokio::select! {
                        address = disconnection_rx.recv() => {
                            match address {
                                Some(address) => {
                                    address
                                }
                                None => {
                                    break;
                                }
                            }
                        }
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    };

                    let peer = state.peer_map.write().await.remove(&address);
                    match peer {
                        Some(peer) => {
                            let peer_address = peer.address().await.unwrap();

                            peer.shutdown().await;

                            let subscribers = state.peer_event_subscribers.read().await;
                            for subscriber in subscribers.iter() {
                                let _ = subscriber
                                    .send(PeerEvent::Disconnected(peer_address, peer.clone()));
                            }
                            tracing::debug!(
                                "Published DisconnectedEvent, num={}",
                                subscribers.len()
                            );

                            tracing::debug!("Disconnected from peer: {}", address);
                        }
                        None => {}
                    }
                }
            }
        });
    }

    fn run_initial_connection(
        &self,
        initial_peers: Vec<SocketAddr>,
        cancellation_token: CancellationToken,
    ) {
        tokio::spawn({
            let state = self.state.clone();

            async move {
                let my_address = state.my_address;

                for address in initial_peers.into_iter() {
                    let ct = cancellation_token.clone();
                    tokio::select! {
                        _ = ct.cancelled() => {
                            return;
                        }
                        _ = ConnectionManager::connect_impl(
                            address,
                            my_address,
                            &state.peer_map,
                            &state.peer_event_subscribers,
                            state.disconnection_tx.clone(),
                            cancellation_token.clone(),
                        ) => {}
                    }
                }
            }
        });
    }

    async fn connect_impl(
        peer_address: SocketAddr,
        my_address: SocketAddr,
        peer_map: &RwLock<HashMap<SocketAddr, Peer>>,
        peer_event_subscribers: &RwLock<Vec<mpsc::Sender<PeerEvent>>>,
        disconnect_ch: tokio_mpsc::Sender<SocketAddr>,
        cancellation_token: CancellationToken,
    ) {
        tracing::debug!("Connecting to peer: {}", peer_address);

        // panic here
        let stream = TcpStream::connect(peer_address).await.unwrap();

        let mut peer = Peer::new(stream, disconnect_ch);

        peer.run(cancellation_token.clone()).await;

        // notify my port to the peer
        peer.send_message(my_address, MessageBody::Handshake {})
            .await
            .unwrap();

        tracing::debug!("Handshake sent to peer: {:?}", peer_address);

        peer.await_handshake().await;

        tracing::debug!("Handshake received from peer: {:?}", peer_address);

        peer_map.write().await.insert(peer_address, peer.clone());

        tracing::debug!("Outgoing connection established: {}", peer_address);

        // TODO: create function
        for subscriber in peer_event_subscribers.read().await.iter() {
            let _ = subscriber.send(PeerEvent::Connected(peer_address, peer.clone()));
        }
    }
}
