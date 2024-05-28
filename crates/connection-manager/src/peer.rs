use message::message::{Message, MessageBody};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use utils::channel::convert_mpsc_channel_to_tokio_channel;

#[derive(Debug, Clone)]
pub struct Peer {
    state: Arc<SharedState>,
}

#[derive(Debug)]
struct SharedState {
    address: RwLock<Option<SocketAddr>>,
    read_stream: Mutex<ReadHalf<TcpStream>>,
    write_stream: Mutex<WriteHalf<TcpStream>>,
    disconnection_tx: tokio_mpsc::Sender<SocketAddr>,
    subscriber_index: RwLock<usize>,
    message_subscribers: RwLock<HashMap<usize, mpsc::Sender<Message>>>,
}

impl Peer {
    pub fn new(stream: TcpStream, disconnection_tx: tokio_mpsc::Sender<SocketAddr>) -> Self {
        let (read_stream, write_stream) = tokio::io::split(stream);
        let message_subscribers = RwLock::new(HashMap::new());

        Self {
            state: Arc::new(SharedState {
                address: RwLock::new(None),
                read_stream: Mutex::new(read_stream),
                write_stream: Mutex::new(write_stream),
                disconnection_tx,
                subscriber_index: RwLock::new(0),
                message_subscribers,
            }),
        }
    }

    pub async fn address(&self) -> Option<SocketAddr> {
        self.state.address.read().await.clone()
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        self.run_message_reception(cancellation_token).await;
    }

    pub async fn shutdown(&self) {
        self.state
            .write_stream
            .lock()
            .await
            .shutdown()
            .await
            .unwrap();
    }

    pub async fn send_message(
        &self,
        my_address: SocketAddr,
        message: MessageBody,
    ) -> std::io::Result<usize> {
        // TODO: fix this
        sleep(std::time::Duration::from_secs(1)).await;

        let message = Message {
            sender: my_address.to_string(),
            body: message,
        }
        .encode();

        self.state
            .write_stream
            .lock()
            .await
            .write(message.as_slice())
            .await
    }

    pub async fn subscribe(&mut self, tx: mpsc::Sender<Message>) -> usize {
        let mut message_subscribers = self.state.message_subscribers.write().await;

        let index = {
            let mut index_ref = self.state.subscriber_index.write().await;
            let index = *index_ref;
            *index_ref += 1;
            index
        };

        message_subscribers.insert(index, tx);

        index
    }

    pub async fn unsubscribe(&mut self, index: usize) -> bool {
        let mut message_subscribers = self.state.message_subscribers.write().await;
        let res = message_subscribers.remove(&index);

        res.is_some()
    }

    pub(crate) async fn await_handshake(&mut self) {
        let state = self.state.clone();
        let (message_tx, message_rx) = mpsc::channel();

        let id = self.subscribe(message_tx).await;

        let mut rx = convert_mpsc_channel_to_tokio_channel(message_rx);

        loop {
            let message = rx.recv().await.unwrap();

            match message.body {
                MessageBody::Handshake => {
                    tracing::debug!("Received handshake message: {:?}", message);

                    state
                        .address
                        .write()
                        .await
                        .replace(message.sender.parse().unwrap());

                    break;
                }
                _ => continue,
            }
        }

        self.unsubscribe(id).await;
    }

    async fn run_message_reception(&self, cancellation_token: CancellationToken) {
        tokio::spawn({
            let state = self.state.clone();
            let peer_address = state.address.read().await.clone();

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

                    let n = match res {
                        Ok(n) => n,
                        Err(e) => {
                            tracing::warn!(
                                "stream is not ready for reading: {:?}, err={}",
                                peer_address,
                                e
                            );
                            continue;
                        }
                    };

                    if n == 0 {
                        tracing::debug!("connection closed: {:?}", peer_address);

                        match peer_address {
                            Some(address) => {
                                let _ = state.disconnection_tx.send(address).await;
                            }
                            None => {}
                        }

                        break;
                    }

                    let message = Message::decode(&buf[..n]);

                    let subscribers = state.message_subscribers.read().await;

                    Peer::notify_message(&subscribers, message.clone());

                    tracing::debug!(
                        "Received message, from={:?}, message={:?}",
                        peer_address,
                        message
                    );
                }
            }
        });
    }

    fn notify_message(subscribers: &HashMap<usize, mpsc::Sender<Message>>, message: Message) {
        for (_, subscriber) in subscribers.iter() {
            let _ = subscriber.send(message.clone());
        }
        tracing::debug!(
            "Notified message to subscribers, len={}, msg={:?}",
            subscribers.len(),
            message
        );
    }
}
