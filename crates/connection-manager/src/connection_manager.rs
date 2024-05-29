use crate::peer::Peer;
use message::message::MessageBody;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::Arc;
use std::{fmt, io};
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
    // my server address
    my_address: SocketAddr,
    // map of connected peers
    peer_map: RwLock<HashMap<SocketAddr, Peer>>,
    // channel to notify peer disconnection
    disconnection_tx: tokio_mpsc::Sender<SocketAddr>,
    // channel to receive peer disconnection
    disconnection_rx: Mutex<tokio_mpsc::Receiver<SocketAddr>>,
    // index of the next subscriber
    next_peer_event_subscriber_index: Mutex<usize>,
    // subscribers to the peer events
    peer_event_subscribers: RwLock<HashMap<usize, mpsc::Sender<PeerEvent>>>,
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
                next_peer_event_subscriber_index: Mutex::new(0),
                peer_event_subscribers: RwLock::new(HashMap::new()),
            }),
        }
    }

    // spawn the connection manager tasks
    pub async fn run(&self, tcp_listener: TcpListener, cancellation_token: CancellationToken) {
        self.run_peer_reception(tcp_listener, cancellation_token.clone());

        self.run_peer_disconnection_handler(cancellation_token.clone());
    }

    // return the connected peers
    pub async fn peers(&self) -> HashMap<SocketAddr, Peer> {
        self.state.peer_map.read().await.clone()
    }

    // connect to a peer by address
    pub async fn connect(
        &self,
        addr: SocketAddr,
        cancellation_token: CancellationToken,
    ) -> Result<(), io::Error> {
        if self.state.peer_map.read().await.get(&addr).is_some() {
            return Ok(());
        }

        ConnectionManager::connect_impl(
            addr,
            self.state.my_address,
            &self.state.peer_map,
            &self.state.peer_event_subscribers,
            self.state.disconnection_tx.clone(),
            cancellation_token,
        )
        .await?;

        Ok(())
    }

    // subscribe to peer events
    pub async fn subscribe(&self, sender: mpsc::Sender<PeerEvent>) -> usize {
        let index = {
            let mut next_index = self.state.next_peer_event_subscriber_index.lock().await;
            let index = *next_index;
            *next_index += 1;
            index
        };

        self.state
            .peer_event_subscribers
            .write()
            .await
            .insert(index, sender);
        tracing::debug!("Subscribed to peer events");

        index
    }

    // unsubscribe peer events
    pub async fn unsubscribe(&self, index: usize) -> bool {
        let res = self
            .state
            .peer_event_subscribers
            .write()
            .await
            .remove(&index);
        tracing::debug!("Unsubscribed from peer events");

        res.is_some()
    }

    // shutdown all peers
    pub async fn shutdown(&self) {
        let mut peer_map = self.state.peer_map.write().await;
        for (_, peer) in peer_map.iter() {
            let _ = peer.shutdown().await;
        }

        peer_map.clear();
    }

    // spawn the peer reception task
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

                    let peer_address = ConnectionManager::initialize_peer_task(
                        &mut peer,
                        my_address,
                        cancellation_token.clone(),
                    )
                    .await;

                    let peer_address = match peer_address {
                        Ok(peer_address) => peer_address,
                        Err(_) => {
                            tracing::warn!("Failed to initialize peer task, closing connection");
                            continue;
                        }
                    };

                    state
                        .peer_map
                        .write()
                        .await
                        .insert(peer_address, peer.clone());

                    ConnectionManager::notify_peer_event(
                        PeerEvent::Connected(peer_address, peer.clone()),
                        &state.peer_event_subscribers,
                    )
                    .await;

                    tracing::debug!("Incoming connection established: {}", peer_address);
                }
            }
        });
    }

    // spawn the peer disconnection handler task
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

                    // if the peer is found, shutdown the peer and notify the subscribers
                    if let Some(peer) = peer {
                        let _ = peer.shutdown().await;

                        let peer_address = peer.address().await;

                        if let Some(peer_address) = peer_address {
                            ConnectionManager::notify_peer_event(
                                PeerEvent::Disconnected(peer_address, peer.clone()),
                                &state.peer_event_subscribers,
                            )
                            .await;

                            tracing::debug!("Disconnected from peer: {}", address)
                        }
                    }
                }
            }
        });
    }

    // implementation to connect to a peer by address
    async fn connect_impl(
        peer_address: SocketAddr,
        my_address: SocketAddr,
        peer_map: &RwLock<HashMap<SocketAddr, Peer>>,
        peer_event_subscribers: &RwLock<HashMap<usize, mpsc::Sender<PeerEvent>>>,
        disconnect_ch: tokio_mpsc::Sender<SocketAddr>,
        cancellation_token: CancellationToken,
    ) -> Result<(), std::io::Error> {
        tracing::debug!("Connecting to peer: {}", peer_address);

        // panic here
        let stream = TcpStream::connect(peer_address).await?;

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

        ConnectionManager::notify_peer_event(
            PeerEvent::Connected(peer_address, peer.clone()),
            peer_event_subscribers,
        )
        .await;

        Ok(())
    }

    // initialize the peer task
    async fn initialize_peer_task(
        peer: &mut Peer,
        my_address: SocketAddr,
        cancellation_token: CancellationToken,
    ) -> Result<SocketAddr, io::Error> {
        // spawn awaiting handshake task before starting to read data
        let handler = tokio::spawn({
            let mut peer = peer.clone();
            async move { peer.await_handshake().await }
        });

        peer.run(cancellation_token.clone()).await;

        match peer
            .send_message(my_address, MessageBody::Handshake {})
            .await
        {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("Failed to send handshake to peer");
                return Err(e);
            }
        }

        tracing::debug!("Handshake sent to peer");

        handler.await?;

        match peer.address().await {
            Some(address) => Ok(address),
            None => {
                tracing::warn!("Failed to get address from peer");

                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to get address from peer",
                ))
            }
        }
    }

    // notify peer event to subscribers
    async fn notify_peer_event(
        event: PeerEvent,
        subscribers: &RwLock<HashMap<usize, mpsc::Sender<PeerEvent>>>,
    ) {
        let subscribers = subscribers.read().await;
        for (_, subscriber) in subscribers.iter() {
            let _ = subscriber.send(event.clone());
        }

        tracing::debug!(
            "Notified peer event, event={:?}, num={}",
            event,
            subscribers.len()
        );
    }
}
