use crate::peer::Peer;
use message::message::MessageBody;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::mpsc;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use utils::channel::convert_mpsc_channel_to_tokio_channel;

#[derive(Clone)]
pub enum PeerEvent {
    Connected(Peer),
    Disconnected(Peer),
}

impl fmt::Debug for PeerEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PeerEvent::Connected(Peer { address, .. }) => write!(f, "Connected({})", address),
            PeerEvent::Disconnected(Peer { address, .. }) => write!(f, "Disconnected({})", address),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionManager {
    state: Arc<SharedState>,
}

#[derive(Debug)]
pub struct SharedState {
    my_port: u16,
    peer_map: RwLock<HashMap<SocketAddr, Peer>>,
    disconnection_tx: tokio_mpsc::Sender<SocketAddr>,
    disconnection_rx: Mutex<tokio_mpsc::Receiver<SocketAddr>>,
    peer_event_subscribers: RwLock<Vec<mpsc::Sender<PeerEvent>>>,
}

impl ConnectionManager {
    pub fn new(my_port: u16) -> Self {
        let (disconnection_tx, disconnection_rx) = tokio_mpsc::channel(1);

        Self {
            state: Arc::new(SharedState {
                my_port,
                peer_map: RwLock::new(HashMap::new()),
                disconnection_tx,
                disconnection_rx: Mutex::new(disconnection_rx),
                peer_event_subscribers: RwLock::new(vec![]),
            }),
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

    pub async fn peers(&self) -> HashMap<SocketAddr, Peer> {
        self.state.peer_map.read().await.clone()
    }

    pub async fn connect(&self, addr: SocketAddr, cancellation_token: CancellationToken) {
        if self.state.peer_map.read().await.get(&addr).is_some() {
            return;
        }

        ConnectionManager::connect_impl(
            addr,
            self.state.my_port,
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

                    // TODO: create sub method
                    match peer_result {
                        Ok((stream, addr)) => {
                            tracing::debug!("Accepted connection from: {}", addr);

                            let mut peer = Peer::new(addr, stream, state.disconnection_tx.clone());
                            // TODO: create method
                            tokio::spawn({
                                let peer = peer.clone();
                                let ct = cancellation_token.clone();

                                async move {
                                    peer.run(ct).await;
                                }
                            })
                                .await
                                .unwrap();

                            let (tx, rx) = mpsc::channel();
                            let id = peer.subscribe(tx).await;
                            let mut rx = convert_mpsc_channel_to_tokio_channel(rx);

                            let peer_addr = tokio::select! {
                                _ = cancellation_token.cancelled() => {
                                    break;
                                },
                                msg = rx.recv() => {
                                    tracing::debug!("Received message in handshaking: {:?}", msg);
                                    match msg {
                                        Some(MessageBody::Handshake { sender }) => {
                                            // TODO: parse error handling
                                            SocketAddr::from_str(&sender).unwrap()
                                        }
                                        _ => {
                                            continue;
                                        }
                                    }
                                }
                            };

                            peer.unsubscribe(id).await;

                            tracing::debug!("Handshake received from peer: {}", peer_addr);

                            peer.change_address(peer_addr);

                            state.peer_map.write().await.insert(addr, peer.clone());

                            let subscribers = state.peer_event_subscribers.read().await;
                            for subscriber in subscribers.iter() {
                                let _ = subscriber.send(PeerEvent::Connected(peer.clone()));
                            }
                            tracing::debug!("Published ConnectedEvent, num={}", subscribers.len());

                            tokio::spawn({
                                let peer = peer.clone();
                                let ct = cancellation_token.clone();

                                async move {
                                    peer.run(ct).await;
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
            let state = self.state.clone();

            async move {
                let mut disconnection_rx = state.disconnection_rx.lock().await;

                loop {
                    tokio::select! {
                        address = disconnection_rx.recv() => {
                            match address {
                                Some(address) => {
                                    let peer = state.peer_map.write().await.remove(&address);
                                    match peer {
                                        Some(peer) => {
                                            peer.shutdown().await;

                                            let subscribers = state.peer_event_subscribers.read().await;
                                            for subscriber in subscribers.iter() {
                                                let _ = subscriber.send(PeerEvent::Disconnected(peer.clone()));
                                            }
                                            tracing::debug!("Published DisconnectedEvent, num={}", subscribers.len());

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
            let state = self.state.clone();

            async move {
                for address in initial_peers.into_iter() {
                    let ct = cancellation_token.clone();
                    tokio::select! {
                        _ = ct.cancelled() => {
                            return;
                        }
                        _ = ConnectionManager::connect_impl(
                            address,
                            state.my_port,
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
        address: SocketAddr,
        my_port: u16,
        peer_map: &RwLock<HashMap<SocketAddr, Peer>>,
        peer_event_subscribers: &RwLock<Vec<mpsc::Sender<PeerEvent>>>,
        disconnect_ch: tokio_mpsc::Sender<SocketAddr>,
        cancellation_token: CancellationToken,
    ) {
        tracing::debug!("Connecting to peer: {}", address);
        // panic here
        let stream = TcpStream::connect(address).await.unwrap();

        let peer = Peer::new(address, stream, disconnect_ch);

        // notify my port to the peer
        let message = MessageBody::Handshake {
            sender: format!("127.0.0.1:{}", my_port),
        }
        .encode();
        peer.send_message(message.as_slice()).await.unwrap();

        peer_map.write().await.insert(address, peer.clone());

        tracing::debug!("Outgoing connection established: {}", address);

        tokio::spawn({
            let peer = peer.clone();
            let ct = cancellation_token.clone();

            async move {
                peer.run(ct).await;
            }
        })
        .await
        .unwrap();

        // TODO: create function
        for subscriber in peer_event_subscribers.read().await.iter() {
            let _ = subscriber.send(PeerEvent::Connected(peer.clone()));
        }
    }
}
