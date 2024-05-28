use message::message::MessageBody;
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

#[derive(Debug, Clone)]
pub struct Peer {
    pub address: SocketAddr,
    state: Arc<SharedState>,
}

#[derive(Debug)]
struct SharedState {
    read_stream: Mutex<ReadHalf<TcpStream>>,
    write_stream: Mutex<WriteHalf<TcpStream>>,
    disconnection_tx: tokio_mpsc::Sender<SocketAddr>,
    subscriber_index: RwLock<usize>,
    message_subscribers: RwLock<HashMap<usize, mpsc::Sender<MessageBody>>>,
}

impl Peer {
    pub fn new(
        address: SocketAddr,
        stream: TcpStream,
        disconnection_tx: tokio_mpsc::Sender<SocketAddr>,
    ) -> Self {
        let (read_stream, write_stream) = tokio::io::split(stream);
        let message_subscribers = RwLock::new(HashMap::new());

        Self {
            address,
            state: Arc::new(SharedState {
                read_stream: Mutex::new(read_stream),
                write_stream: Mutex::new(write_stream),
                disconnection_tx,
                subscriber_index: RwLock::new(0),
                message_subscribers,
            }),
        }
    }

    pub fn change_address(&mut self, address: SocketAddr) {
        self.address = address;
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

    pub async fn send_message(&self, message: &[u8]) -> std::io::Result<usize> {
        // TODO: fix this
        sleep(std::time::Duration::from_secs(1)).await;
        self.state.write_stream.lock().await.write(message).await
    }

    pub async fn subscribe(&mut self, tx: mpsc::Sender<MessageBody>) -> usize {
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

    async fn run_message_reception(&self, cancellation_token: CancellationToken) {
        tokio::spawn({
            let address = self.address.clone();
            let state = self.state.clone();

            async move {
                let mut read_stream = state.read_stream.lock().await;

                tracing::debug!("Waiting for message: {}", address);

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
                                "stream is not ready for reading: {}, err={}",
                                address,
                                e
                            );
                            continue;
                        }
                    };

                    if n == 0 {
                        tracing::debug!("connection closed: {}", address);

                        let _ = state.disconnection_tx.send(address).await;

                        break;
                    }

                    let message = MessageBody::decode(&buf[..n]);

                    let subscribers = state.message_subscribers.read().await;

                    Peer::notify_message(&subscribers, message.clone());

                    tracing::debug!("Received message, from={}, message={:?}", address, message);
                }
            }
        });
    }

    fn notify_message(
        subscribers: &HashMap<usize, mpsc::Sender<MessageBody>>,
        message: MessageBody,
    ) {
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
