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

// ConnectionManager provides the functionality to manage the connections to the peers
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
        peer_address: SocketAddr,
        cancellation_token: CancellationToken,
    ) -> Result<(), io::Error> {
        if let Some(_) = self.state.peer_map.read().await.get(&peer_address) {
            return Ok(());
        }

        let state = self.state.clone();

        tracing::debug!("Connecting to peer: {}", peer_address);

        let stream = TcpStream::connect(peer_address).await?;

        let mut peer = Peer::new(stream, state.my_address, state.disconnection_tx.clone());

        let _ = Self::initialize_peer(
            &mut peer,
            &state.peer_map,
            &state.peer_event_subscribers,
            cancellation_token,
        )
        .await?;

        tracing::debug!("Outgoing connection established: {}", peer_address);

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

        let futures: Vec<_> = peer_map
            .iter()
            .map(|(_peer_addr, peer)| peer.shutdown())
            .collect();

        futures::future::join_all(futures).await;

        peer_map.clear();
    }

    // spawn the peer reception task
    fn run_peer_reception(&self, tcp_listener: TcpListener, cancellation_token: CancellationToken) {
        tokio::spawn({
            let state = self.state.clone();
            let my_address = state.my_address;

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

                    let (stream, peer_ephemeral_address) = match peer_result {
                        Ok(res) => res,
                        Err(_) => {
                            tracing::warn!("Failed to accept connection, skip");
                            continue;
                        }
                    };

                    tracing::debug!("Accepted connection from: {}", peer_ephemeral_address);

                    let mut peer = Peer::new(stream, my_address, state.disconnection_tx.clone());

                    let peer_address = Self::initialize_peer(
                        &mut peer,
                        &state.peer_map,
                        &state.peer_event_subscribers,
                        cancellation_token.clone(),
                    )
                    .await;

                    match peer_address {
                        Ok(peer_address) => {
                            tracing::debug!("Incoming connection established: {}", peer_address);
                        }
                        Err(e) => {
                            tracing::warn!("Failed to initialize peer task: {:?}", e);
                            continue;
                        }
                    }
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
                            Self::notify_peer_event(
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

    // initialize peer
    async fn initialize_peer(
        peer: &mut Peer,
        peer_map: &RwLock<HashMap<SocketAddr, Peer>>,
        peer_event_subscribers: &RwLock<HashMap<usize, mpsc::Sender<PeerEvent>>>,
        cancellation_token: CancellationToken,
    ) -> Result<SocketAddr, io::Error> {
        // run the peer task
        peer.run(cancellation_token.clone()).await;

        // spawn awaiting handshake task before starting to read data
        let handler = tokio::spawn({
            let mut peer = peer.clone();
            async move { peer.await_handshake().await }
        });

        // send handshake to peer
        match peer.send_message(MessageBody::Handshake {}).await {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("Failed to send handshake to peer");
                return Err(e);
            }
        }

        tracing::debug!("Handshake sent to peer");

        handler.await?;

        // get the peer address
        let peer_address = match peer.address().await {
            Some(address) => address,
            None => {
                tracing::warn!("Failed to get address from peer");

                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to get address from peer",
                ));
            }
        };

        // insert the peer to the map and publish an event
        peer_map.write().await.insert(peer_address, peer.clone());

        Self::notify_peer_event(
            PeerEvent::Connected(peer_address, peer.clone()),
            peer_event_subscribers,
        )
        .await;

        Ok(peer_address)
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

#[cfg(test)]
mod tests {
    use crate::connection_manager::ConnectionManager;
    use crate::peer::Peer;
    use std::net::SocketAddr;
    use std::sync::mpsc;
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;
    use utils::channel::convert_mpsc_channel_to_tokio_channel;

    async fn new_connection_manager(
        cancellation_token: CancellationToken,
    ) -> (ConnectionManager, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connection_manager = ConnectionManager::new(address);

        connection_manager.run(listener, cancellation_token).await;

        return (connection_manager, address);
    }

    async fn new_server() -> (TcpListener, SocketAddr) {
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = server.local_addr().unwrap();

        (server, address)
    }

    #[tokio::test]
    async fn test_outgoing_connect() {
        let cancellation_token = CancellationToken::new();
        let (connection_manager, _) = new_connection_manager(cancellation_token.clone()).await;
        let (peer_server, peer_address) = new_server().await;

        // send handshake from peer
        tokio::spawn({
            async move {
                let (stream, _) = peer_server.accept().await.unwrap();
                let peer = Peer::new(stream, peer_address, tokio::sync::mpsc::channel(1).0);

                peer.send_message(message::message::MessageBody::Handshake {})
                    .await
                    .unwrap();
            }
        });

        let (tx, rx) = mpsc::channel();
        let mut rx = convert_mpsc_channel_to_tokio_channel(rx);
        let _ = connection_manager.subscribe(tx).await;

        tokio::select! {
            res = connection_manager.connect(peer_address, cancellation_token.clone()) => {
                assert!(res.is_ok());
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                panic!("Timeout");
            }
        }

        // make sure the connected event is delivered
        tokio::select! {
            x = rx.recv() => {
                match x {
                    Some(super::PeerEvent::Connected(address, _)) => {
                        assert_eq!(address, peer_address);
                    },
                    _ => {
                        panic!("Invalid event");
                    }
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                panic!("Timeout");
            }
        }

        // check peers map
        let peers = connection_manager.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers.contains_key(&peer_address), true);
    }

    #[tokio::test]
    async fn test_incoming_connect() {
        let cancellation_token = CancellationToken::new();
        let (my_connection_manager, my_address) =
            new_connection_manager(cancellation_token.clone()).await;
        let (peer_connection_manager, peer_address) =
            new_connection_manager(cancellation_token.clone()).await;

        let (tx, rx) = mpsc::channel();
        let mut rx = convert_mpsc_channel_to_tokio_channel(rx);
        let _ = my_connection_manager.subscribe(tx).await;

        tokio::select! {
            x = peer_connection_manager.connect(my_address, cancellation_token.clone()) => {
                assert!(x.is_ok());
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                panic!("Timeout 1");
            }
        }

        // make sure the connected event is delivered
        tokio::select! {
            x = rx.recv() => {
                match x {
                    Some(super::PeerEvent::Connected(address, _)) => {
                        assert_eq!(address, peer_address);
                    },
                    _ => {
                        panic!("Invalid event");
                    }
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                panic!("Timeout 2");
            }
        }

        // check peers map
        let peers = my_connection_manager.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers.contains_key(&peer_address), true);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let cancellation_token = CancellationToken::new();
        let (my_connection_manager, my_address) =
            new_connection_manager(cancellation_token.clone()).await;
        let (peer_connection_manager, peer_address) =
            new_connection_manager(cancellation_token.clone()).await;

        let (tx, rx) = mpsc::channel();
        let mut rx = convert_mpsc_channel_to_tokio_channel(rx);
        let _ = peer_connection_manager.subscribe(tx).await;

        // connecting
        tokio::select! {
            x = my_connection_manager.connect(peer_address, cancellation_token.clone()) => {
                assert!(x.is_ok());
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                panic!("Timeout 1");
            }
        }

        // wait for peer to connect
        tokio::select! {
            x = rx.recv() => {
                match x {
                    Some(super::PeerEvent::Connected(address, _)) => {
                        assert_eq!(address, my_address);
                    },
                    _ => {
                        panic!("Invalid event");
                    }
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                panic!("Timeout 2");
            }
        }

        // make sure both have each other in the peers map
        let my_peers = my_connection_manager.peers().await;
        assert_eq!(my_peers.len(), 1);
        assert!(my_peers.contains_key(&peer_address));

        let peers_peers = peer_connection_manager.peers().await;
        assert_eq!(peers_peers.len(), 1);
        assert!(peers_peers.contains_key(&my_address));

        // shutdown
        my_connection_manager.shutdown().await;

        // waiting for peer to disconnect from my node
        tokio::select! {
            x = rx.recv() => {
                match x {
                    Some(super::PeerEvent::Disconnected(address, _)) => {
                        assert_eq!(address, my_address);
                    },
                    _ => {
                        panic!("Invalid event");
                    }
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                panic!("Timeout 2");
            }
        }

        // make sure both peers are empty
        let my_peers = my_connection_manager.peers().await;
        assert_eq!(my_peers.len(), 0);

        let peers_peers = peer_connection_manager.peers().await;
        assert_eq!(peers_peers.len(), 0);
    }

    #[tokio::test]
    async fn test_subscribe_disconnected() {
        let cancellation_token = CancellationToken::new();
        let (my_connection_manager, my_address) =
            new_connection_manager(cancellation_token.clone()).await;
        let (peer_connection_manager, peer_address) =
            new_connection_manager(cancellation_token.clone()).await;

        let cancellation_token = CancellationToken::new();

        let (tx, rx) = mpsc::channel();
        let mut rx = convert_mpsc_channel_to_tokio_channel(rx);
        let _ = my_connection_manager.subscribe(tx).await;

        tokio::select! {
            x = peer_connection_manager.connect(my_address, cancellation_token.clone()) => {
                assert!(x.is_ok());
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                panic!("Timeout 1");
            }
        }

        // ignore the connected event
        let _ = rx.recv().await;

        peer_connection_manager.shutdown().await;

        tokio::select! {
            x = rx.recv() => {
                match x {
                    Some(super::PeerEvent::Disconnected(address, _)) => {
                        assert_eq!(address, peer_address);
                    },
                    _ => {
                        println!("debug::received {:?}", x);
                        panic!("Invalid event");
                    }
                }
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                panic!("Timeout 3");
            }
        }
    }
}
