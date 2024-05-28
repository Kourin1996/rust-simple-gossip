use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, RwLock};

#[derive(Debug)]
pub struct Peer {
    pub address: SocketAddr,
    read_stream: Arc<Mutex<ReadHalf<TcpStream>>>,
    write_stream: Arc<Mutex<WriteHalf<TcpStream>>>,

    disconnection_tx: mpsc::Sender<SocketAddr>,
    message_subscribers: Arc<RwLock<Vec<std::sync::mpsc::Sender<(SocketAddr, Vec<u8>)>>>>,
}

impl Peer {
    pub fn new(
        address: SocketAddr,
        stream: TcpStream,
        disconnection_tx: mpsc::Sender<SocketAddr>,
    ) -> Self {
        let (read_stream, write_stream) = tokio::io::split(stream);
        let message_subscribers = Arc::new(RwLock::new(vec![]));

        Self {
            address,
            read_stream: Arc::new(Mutex::new(read_stream)),
            write_stream: Arc::new(Mutex::new(write_stream)),
            disconnection_tx,
            message_subscribers,
        }
    }

    pub async fn send_message(&self, message: &[u8]) -> std::io::Result<usize> {
        self.write_stream.lock().await.write(message).await
    }

    pub async fn run_message_reception(&self) {
        tokio::spawn({
            let read_stream = self.read_stream.clone();
            let address = self.address.clone();
            let subscribers = self.message_subscribers.clone();
            let disconnection_tx = self.disconnection_tx.clone();

            async move {
                let mut read_stream = read_stream.lock().await;

                tracing::debug!("Waiting for message: {}", address);

                let mut buf = vec![0; 1024];

                loop {
                    let n = match read_stream.read(&mut buf).await {
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

                        let _ = disconnection_tx.send(address).await;

                        break;
                    }

                    let message = &buf[..n];

                    Peer::notify_message(subscribers.clone(), address, message.to_vec()).await;

                    tracing::debug!("Received message: {:?}", message);
                }
            }
        });
    }

    pub async fn shutdown(&self) {
        self.write_stream.lock().await.shutdown().await.unwrap();
    }

    pub async fn subscribe(&mut self, tx: std::sync::mpsc::Sender<(SocketAddr, Vec<u8>)>) {
        self.message_subscribers.write().await.push(tx);
    }

    async fn notify_message(
        subscribers: Arc<RwLock<Vec<std::sync::mpsc::Sender<(SocketAddr, Vec<u8>)>>>>,
        address: SocketAddr,
        message: Vec<u8>,
    ) {
        let subscribers = subscribers.read().await;

        for subscriber in subscribers.iter() {
            let _ = subscriber.send((address, message.clone()));
        }
    }
}
