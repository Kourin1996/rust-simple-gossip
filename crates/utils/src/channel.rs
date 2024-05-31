use std::sync::mpsc;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task;

// Convert a `std::sync::mpsc::Receiver` to a `tokio::sync::mpsc::Receiver`
// tokio's mpsc channel blocks the sender if the channel is full,
// but tokio's mpsc channel is good for receiver side because it can be used in async functions
// for that reason, we convert the std mpsc channel to tokio mpsc channel
pub fn convert_mpsc_channel_to_tokio_channel<T: Send + 'static>(
    receiver: mpsc::Receiver<T>,
) -> tokio_mpsc::Receiver<T> {
    let (tx, rx) = tokio_mpsc::channel::<T>(10);

    task::spawn_blocking(move || {
        while let Ok(msg) = receiver.recv() {
            let tx = tx.clone();
            task::block_in_place(move || {
                tx.try_send(msg).unwrap();
            });
        }
    });

    rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time;

    #[tokio::test]
    async fn test_convert_mpsc_channel_to_tokio_channel_send_signals() {
        let values = vec![1, 2, 3, 4, 5];

        let (tx, rx) = mpsc::channel::<i32>();
        let mut tokio_rx = convert_mpsc_channel_to_tokio_channel(rx);

        tokio::spawn({
            let values = values.clone();
            async move {
                for v in values {
                    tx.send(v).unwrap();
                    time::sleep(Duration::from_millis(10)).await;
                }

                drop(tx);
            }
        });

        let mut received = vec![];
        loop {
            tokio::select! {
                _ = time::sleep(Duration::from_secs(1)) => {
                        panic!("Timeout");
                    }
                v = tokio_rx.recv() => {
                    match v {
                        Some(v) => {
                            received.push(v);
                        }
                        None => {
                            break;
                        }
                    }
                }
            }
        }

        assert_eq!(received, values);
    }
}
