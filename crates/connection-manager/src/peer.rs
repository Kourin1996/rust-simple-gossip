use message::message::{Message, MessageBody};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use utils::channel::convert_mpsc_channel_to_tokio_channel;

#[derive(Debug, Clone)]
pub struct Peer {
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
    pub fn new(stream: TcpStream, disconnection_tx: tokio_mpsc::Sender<SocketAddr>) -> Self {
        let (read_stream, write_stream) = tokio::io::split(stream);

        Self {
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

    pub async fn address(&self) -> Option<SocketAddr> {
        self.state.address.read().await.clone()
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        self.run_message_reception(cancellation_token).await;
    }

    pub async fn shutdown(&self) -> std::io::Result<()> {
        self.state.write_stream.lock().await.shutdown().await
    }

    pub async fn send_message(
        &self,
        my_address: SocketAddr,
        message: MessageBody,
    ) -> std::io::Result<usize> {
        let message = Message {
            sender: my_address.to_string(),
            body: message,
        }
        .encode();

        let mut write_stream = self.state.write_stream.lock().await;

        let size = write_stream.write(&message).await?;
        write_stream.flush().await?;

        Ok(size)
    }

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

            match message.body {
                MessageBody::Handshake => {
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
                _ => continue,
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

                let mut buf = vec![0; 1024];

                loop {
                    let res = tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                        res = read_stream.read(&mut buf) => {
                            res
                        }
                    };

                    let peer_address = state.address.read().await.clone();

                    let n = match res {
                        Ok(n) => n,
                        Err(e) => match e.kind() {
                            std::io::ErrorKind::ConnectionReset => {
                                // connection reset by peer
                                tracing::warn!(
                                    "connection reset by peer: {:?}, err={}",
                                    peer_address,
                                    e
                                );

                                Peer::notify_peer_disconnection(
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

                        Peer::notify_peer_disconnection(peer_address, &state.disconnection_tx)
                            .await;

                        break;
                    }

                    Peer::notify_message(&state.message_subscribers, &buf[..n], peer_address).await;
                }
            }
        });
    }

    // notifies the message to all the subscribers
    async fn notify_message(
        subscribers: &RwLock<HashMap<usize, mpsc::Sender<Message>>>,
        message: &[u8],
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
        match address {
            Some(address) => {
                let _ = channel.send(address).await;
            }
            None => {}
        }
    }
}
