use message::message::{Message, MessageBody};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use utils::channel::convert_mpsc_channel_to_tokio_channel;

// Peer represents a connected peer and provides methods to interact with it
#[derive(Debug, Clone)]
pub struct Peer {
    my_address: SocketAddr,
    state: Arc<SharedState>,
}

#[derive(Debug)]
struct SharedState {
    // peer address
    address: RwLock<Option<SocketAddr>>,
    // read stream of TCP connection
    read_stream: Mutex<ReadHalf<TcpStream>>,
    // write stream of TCP connection
    write_stream: Mutex<WriteHalf<TcpStream>>,
    // channel to notify peer disconnection
    disconnection_tx: tokio_mpsc::Sender<SocketAddr>,
    // index of the next subscriber
    subscriber_index: Mutex<usize>,
    // subscribers to the peer messages
    message_subscribers: RwLock<HashMap<usize, mpsc::Sender<Message>>>,
}

impl Peer {
    pub fn new(
        stream: TcpStream,
        my_address: SocketAddr,
        disconnection_tx: tokio_mpsc::Sender<SocketAddr>,
    ) -> Self {
        let (read_stream, write_stream) = tokio::io::split(stream);

        Self {
            my_address,
            state: Arc::new(SharedState {
                address: RwLock::new(None),
                read_stream: Mutex::new(read_stream),
                write_stream: Mutex::new(write_stream),
                disconnection_tx,
                subscriber_index: Mutex::new(0),
                message_subscribers: RwLock::new(HashMap::new()),
            }),
        }
    }

    // returns the peer's address
    pub async fn address(&self) -> Option<SocketAddr> {
        self.state.address.read().await.clone()
    }

    // runs the peer message reception loop
    pub async fn run(&self, cancellation_token: CancellationToken) {
        self.run_message_reception(cancellation_token).await;
    }

    // closes the connection
    pub async fn shutdown(&self) -> std::io::Result<()> {
        self.state.write_stream.lock().await.shutdown().await
    }

    // sends a message to the peer
    pub async fn send_message(&self, message: MessageBody) -> std::io::Result<usize> {
        let mut message = Message {
            sender: self.my_address.to_string(),
            body: message,
        }
        .encode();

        let mut write_stream = self.state.write_stream.lock().await;

        message.push('\n');

        let size = write_stream.write(message.as_bytes()).await?;
        write_stream.flush().await?;

        Ok(size)
    }

    // subscribes to the peer messages
    pub async fn subscribe(&mut self, tx: mpsc::Sender<Message>) -> usize {
        let mut message_subscribers = self.state.message_subscribers.write().await;

        let index = {
            let mut index_ref = self.state.subscriber_index.lock().await;
            let index = *index_ref;
            *index_ref += 1;
            index
        };

        tracing::debug!("Subscribing to peer messages, index={}", index);

        message_subscribers.insert(index, tx);

        index
    }

    // unsubscribes from the peer messages
    pub async fn unsubscribe(&mut self, index: usize) -> bool {
        let mut message_subscribers = self.state.message_subscribers.write().await;
        let res = message_subscribers.remove(&index);

        res.is_some()
    }

    // awaits for a handshake message from the peer and sets the peer's address
    pub(crate) async fn await_handshake(&mut self) {
        let state = self.state.clone();
        let (message_tx, message_rx) = mpsc::channel();

        let id = self.subscribe(message_tx).await;

        let mut rx = convert_mpsc_channel_to_tokio_channel(message_rx);

        loop {
            let message = rx.recv().await.unwrap();

            if let MessageBody::Handshake = message.body {
                let address = match message.sender.parse() {
                    Ok(address) => address,
                    Err(_) => {
                        tracing::warn!("Invalid address: {:?}", message.sender);
                        continue;
                    }
                };

                state.address.write().await.replace(address);

                tracing::debug!("Received handshake message: address={:?}", address);

                break;
            }
        }

        self.unsubscribe(id).await;
    }

    // runs the message reception loop in a separate task
    async fn run_message_reception(&self, cancellation_token: CancellationToken) {
        tokio::spawn({
            let state = self.state.clone();

            async move {
                let mut read_stream = state.read_stream.lock().await;
                let mut reader = BufReader::new(&mut *read_stream);

                loop {
                    let mut line = String::new();
                    let read_size = tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                        read_size = reader.read_line(&mut line) => {
                            read_size
                        }
                    };

                    let peer_address = state.address.read().await.clone();

                    let n = match read_size {
                        Ok(n) => n,
                        Err(e) => match e.kind() {
                            std::io::ErrorKind::ConnectionReset => {
                                // connection reset by peer
                                tracing::warn!(
                                    "connection reset by peer: {:?}, err={}",
                                    peer_address,
                                    e
                                );

                                Self::notify_peer_disconnection(
                                    peer_address,
                                    &state.disconnection_tx,
                                )
                                .await;

                                break;
                            }
                            _ => {
                                continue;
                            }
                        },
                    };

                    // the connection is closed if the read size is zero
                    if n == 0 {
                        tracing::debug!("connection closed: {:?}", peer_address);

                        Self::notify_peer_disconnection(peer_address, &state.disconnection_tx)
                            .await;

                        break;
                    }

                    Self::notify_message(&state.message_subscribers, line, peer_address).await;
                }
            }
        });
    }

    // notifies the message to all the subscribers
    async fn notify_message(
        subscribers: &RwLock<HashMap<usize, mpsc::Sender<Message>>>,
        message: String,
        peer_address: Option<SocketAddr>,
    ) {
        let message = Message::decode(message);
        let subscribers = subscribers.read().await;

        for (_, subscriber) in subscribers.iter() {
            let _ = subscriber.send(message.clone());
        }

        tracing::debug!(
            "Received message, from={:?}, message={:?}, subscribers={}",
            peer_address,
            message,
            subscribers.len(),
        );
    }

    // notifies the peer disconnection to the disconnection channel
    async fn notify_peer_disconnection(
        address: Option<SocketAddr>,
        channel: &tokio_mpsc::Sender<SocketAddr>,
    ) {
        if let Some(address) = address {
            let _ = channel.send(address).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::peer::Peer;
    use message::message::{Message, MessageBody};
    use std::io::ErrorKind::BrokenPipe;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::string::FromUtf8Error;
    use std::sync::mpsc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc::Receiver;
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    async fn open_random_server_helper() -> (TcpListener, SocketAddr) {
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = server.local_addr().unwrap();

        (server, address)
    }

    async fn spawn_raw_message_reception_helper(
        listener: TcpListener,
    ) -> JoinHandle<Result<String, FromUtf8Error>> {
        tokio::spawn({
            async move {
                let (mut connection, _) = listener.accept().await.unwrap();
                let mut buffer = [0; 1024];

                let n = connection.read(&mut buffer).await.unwrap();

                String::from_utf8(Vec::from(&buffer[..n]))
            }
        })
    }

    async fn setup_peer_helper(
        peer_address: SocketAddr,
        my_address: SocketAddr,
        should_run_task: bool,
        cancellation_token: CancellationToken,
    ) -> (Peer, Receiver<SocketAddr>) {
        let peer_connection = TcpStream::connect(peer_address).await.unwrap();

        let (disconnection_tx, disconnect_rx) = tokio::sync::mpsc::channel(1);
        let peer = Peer::new(peer_connection, my_address, disconnection_tx);

        if should_run_task {
            peer.run(cancellation_token.clone()).await;
        }

        (peer, disconnect_rx)
    }

    async fn subscribe_message_helper(peer: &mut Peer) -> (Receiver<Message>, usize) {
        let (tx, rx) = mpsc::channel();
        let index = peer.subscribe(tx).await;
        let rx = utils::channel::convert_mpsc_channel_to_tokio_channel(rx);

        (rx, index)
    }

    async fn await_connection_and_send_message_helper(peer_server: &TcpListener, msg: Message) {
        let (mut peer_stream, _) = peer_server.accept().await.unwrap();

        peer_stream
            .write(format!("{}\n", msg.encode()).as_bytes())
            .await
            .unwrap();
        peer_stream.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_message() {
        let my_address = SocketAddr::from_str("127.0.0.1:80").unwrap();
        let msg_body = MessageBody::GossipBroadcast {
            message: "Hello, world!".to_string(),
        };
        let msg = Message {
            sender: my_address.to_string(),
            body: msg_body.clone(),
        };

        let (peer_server, peer_address) = open_random_server_helper().await;

        let peer_message_reception_handler = spawn_raw_message_reception_helper(peer_server);

        let (peer, _) =
            setup_peer_helper(peer_address, my_address, false, CancellationToken::new()).await;

        peer.send_message(msg_body.clone()).await.unwrap();

        tokio::select! {
            received = peer_message_reception_handler.await => {
                let received = received.unwrap();

                assert_eq!(received, Ok(msg.encode() + "\n"))
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                panic!("Timeout");
            }
        }
    }

    #[tokio::test]
    async fn test_send_message_to_closed_stream() {
        let my_address = SocketAddr::from_str("127.0.0.1:80").unwrap();

        let msg_body = MessageBody::GossipBroadcast {
            message: "Hello, world!".to_string(),
        };

        let (_peer_server, peer_address) = open_random_server_helper().await;

        let (peer, _) =
            setup_peer_helper(peer_address, my_address, false, CancellationToken::new()).await;

        peer.shutdown().await.unwrap();

        let res = peer.send_message(msg_body).await;

        assert!(!res.is_ok());

        if let Err(err) = res {
            assert_eq!(err.kind(), BrokenPipe);
        }
    }

    #[tokio::test]
    async fn test_subscribe() {
        let my_address = SocketAddr::from_str("127.0.0.1:80").unwrap();
        let msg_body = MessageBody::GossipBroadcast {
            message: "Hello, world!".to_string(),
        };

        let (peer_server, peer_address) = open_random_server_helper().await;

        let cancellation_token = CancellationToken::new();
        let (mut peer, _) =
            setup_peer_helper(peer_address, my_address, true, cancellation_token.clone()).await;

        let (mut rx, index) = subscribe_message_helper(&mut peer).await;

        let msg = Message {
            sender: peer_address.to_string(),
            body: msg_body.clone(),
        };

        await_connection_and_send_message_helper(&peer_server, msg.clone()).await;

        assert_eq!(index, 0);

        tokio::select! {
            received = rx.recv() => {
                assert_eq!(received, Some(msg));
                peer.unsubscribe(index).await;
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                peer.unsubscribe(index).await;
                panic!("Timeout");
            }
        }
    }

    #[tokio::test]
    async fn test_handshake() {
        let my_address = SocketAddr::from_str("127.0.0.1:80").unwrap();
        let (peer_server, peer_address) = open_random_server_helper().await;

        let cancellation_token = CancellationToken::new();
        let (mut peer, _) =
            setup_peer_helper(peer_address, my_address, true, cancellation_token.clone()).await;

        // send handshake from peer to notify its address
        let (mut stream, _) = peer_server.accept().await.unwrap();
        let handshake = Message {
            sender: peer_address.to_string(),
            body: MessageBody::Handshake,
        }
        .encode();

        stream
            .write(format!("{}\n", handshake).as_bytes())
            .await
            .unwrap();
        stream.flush().await.expect("failed to flush");

        peer.await_handshake().await;

        assert_eq!(peer.address().await, Some(peer_address));
    }

    #[tokio::test]
    async fn test_notify_peer_disconnection() {
        let my_address = SocketAddr::from_str("127.0.0.1:80").unwrap();
        let (peer_server, peer_address) = open_random_server_helper().await;

        let cancellation_token = CancellationToken::new();
        let (mut peer, mut disconnection_rx) =
            setup_peer_helper(peer_address, my_address, true, cancellation_token.clone()).await;

        // send handshake from peer to notify its address
        let (mut stream, _) = peer_server.accept().await.unwrap();
        let handshake = Message {
            sender: peer_address.to_string(),
            body: MessageBody::Handshake,
        }
        .encode();
        stream
            .write(format!("{}\n", handshake).as_bytes())
            .await
            .unwrap();
        stream.flush().await.expect("failed to flush");

        peer.await_handshake().await;

        // close connection by peer
        stream.shutdown().await.expect("shutdown failed");

        tokio::select! {
            res = disconnection_rx.recv() => {
                assert_eq!(res, Some(peer_address));
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                panic!("Timeout");
            }
        }
    }
}
