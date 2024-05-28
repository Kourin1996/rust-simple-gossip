use std::sync::mpsc;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task;

pub fn convert_mpsc_channel_to_tokio_channel<T: Send + 'static>(
    receiver: mpsc::Receiver<T>,
) -> tokio_mpsc::Receiver<T> {
    let (tx, rx) = tokio_mpsc::channel::<T>(1024);

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
