use crate::peer::Peer;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum PeerEvent {
    Connected(SocketAddr, Arc<Peer>),
    Disconnected(SocketAddr),
}

#[derive(Debug)]
pub struct ConnectionManager {
    peer_map: Arc<RwLock<HashMap<SocketAddr, Arc<Peer>>>>,
    disconnection_tx: mpsc::Sender<SocketAddr>,
    disconnection_rx: Arc<Mutex<Receiver<SocketAddr>>>,
    peer_event_subscribers: Arc<RwLock<Vec<mpsc::Sender<PeerEvent>>>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        let (disconnection_tx, disconnection_rx) = mpsc::channel(1);

        Self {
            peer_map: Arc::new(RwLock::new(HashMap::new())),
            disconnection_tx,
            disconnection_rx: Arc::new(Mutex::new(disconnection_rx)),
            peer_event_subscribers: Arc::new(RwLock::new(vec![])),
        }
    }

    pub fn run(
        &self,
        tcp_listener: TcpListener,
        initial_peers: Vec<SocketAddr>,
        cancellation_token: CancellationToken,
    ) {
        self.run_peer_reception(tcp_listener, cancellation_token.clone());

        self.run_peer_disconnection_handler(cancellation_token.clone());

        self.run_initial_connection(initial_peers, cancellation_token);
    }

    pub async fn peers(&self) -> HashMap<SocketAddr, Arc<Peer>> {
        self.peer_map.read().await.clone()
    }

    pub async fn connect(&self, addr: SocketAddr) {
        ConnectionManager::connect_impl(
            addr,
            self.peer_map.clone(),
            self.peer_event_subscribers.clone(),
            self.disconnection_tx.clone(),
        )
        .await;
    }

    pub async fn subscribe(&self, sender: mpsc::Sender<PeerEvent>) {
        self.peer_event_subscribers.write().await.push(sender);
        tracing::debug!("Subscribed to peer events");
    }

    fn run_peer_reception(&self, tcp_listener: TcpListener, cancellation_token: CancellationToken) {
        tokio::spawn({
            let peer_map = self.peer_map.clone();
            let disconnection_tx = self.disconnection_tx.clone();
            let cancellation_token = cancellation_token.clone();
            let peer_event_subscribers = self.peer_event_subscribers.clone();

            async move {
                loop {
                    let peer_result = tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            return;
                        }
                        peer = tcp_listener.accept() => {
                            peer
                        }
                    };

                    match peer_result {
                        Ok((stream, addr)) => {
                            let peer = Arc::new(Peer::new(addr, stream, disconnection_tx.clone()));

                            peer_map.write().await.insert(addr, peer.clone());
                            for subscriber in peer_event_subscribers.read().await.iter() {
                                let _ = subscriber.send(PeerEvent::Connected(addr, peer.clone()));
                                tracing::debug!("Published ConnectedEvent");
                            }

                            tokio::spawn({
                                let peer = peer.clone();

                                async move {
                                    peer.run_message_reception().await;
                                }
                            });

                            tracing::debug!("Incoming connection established: {}", addr);
                        }
                        Err(err) => {
                            tracing::warn!("Failed to accept connection: {}", err);
                        }
                    }
                }
            }
        });
    }

    fn run_peer_disconnection_handler(&self, cancellation_token: CancellationToken) {
        tokio::spawn({
            let disconnection_rx = Arc::clone(&self.disconnection_rx);
            let peer_map = self.peer_map.clone();
            let cancellation_token = cancellation_token.clone();
            let peer_event_subscribers = self.peer_event_subscribers.clone();

            async move {
                let mut disconnection_rx = disconnection_rx.lock().await;

                loop {
                    tokio::select! {
                        address = disconnection_rx.recv() => {
                            match address {
                                Some(address) => {
                                    let peer = peer_map.write().await.remove(&address);
                                    match peer {
                                        Some(peer) => {
                                            peer.shutdown().await;

                                            for subscriber in peer_event_subscribers.read().await.iter() {
                                                let _ = subscriber.send(PeerEvent::Disconnected(address));
                                                tracing::debug!("Published DisconnectedEvent");
                                            }

                                            tracing::debug!("Disconnected from peer: {}", address);
                                        }
                                        None => {}
                                    }
                                }
                                None => {
                                    break;
                                }
                            }
                        }
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
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
            let peer_map = self.peer_map.clone();
            let peer_event_subscribers = self.peer_event_subscribers.clone();
            let disconnection_tx = self.disconnection_tx.clone();

            async move {
                for address in initial_peers.into_iter() {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            return;
                        }
                        _ = ConnectionManager::connect_impl(
                            address,
                            peer_map.clone(),
                            peer_event_subscribers.clone(),
                            disconnection_tx.clone(),
                        ) => {}
                    }
                }
            }
        });
    }

    async fn connect_impl(
        address: SocketAddr,
        peer_map: Arc<RwLock<HashMap<SocketAddr, Arc<Peer>>>>,
        peer_event_subscribers: Arc<RwLock<Vec<mpsc::Sender<PeerEvent>>>>,
        disconnect_ch: mpsc::Sender<SocketAddr>,
    ) {
        let stream = TcpStream::connect(address).await.unwrap();

        let peer = Arc::new(Peer::new(address, stream, disconnect_ch));

        peer_map.write().await.insert(address, peer.clone());

        for subscriber in peer_event_subscribers.read().await.iter() {
            let _ = subscriber.send(PeerEvent::Connected(address, peer.clone()));
            tracing::debug!("Published ConnectedEvent");
        }

        tokio::spawn({
            let peer = peer.clone();

            async move {
                peer.run_message_reception().await;
            }
        });

        tracing::debug!("Outgoing connection established: {}", address);
    }
}
