mod subscription_manager;
mod topic_registry;

use futures::SinkExt;
use tokio;
use tokio::stream::StreamExt;
use tokio_util;
#[macro_use]
extern crate log;

mod protocol;

#[tokio::main]
async fn main() {
    env_logger::init();

    let mut listener = tokio::net::TcpListener::bind("127.0.0.1:8889")
        .await
        .unwrap();
    debug!("Started broker server at {}", "127.0.0.1:8889".to_string());

    loop {
        // В peer хранится ip адрес и порт входящего подключения.
        let (socket, peer) = listener.accept().await.unwrap();

        // Для каждого входящего подключения мы будем создавать отдельную задачу.
        tokio::spawn(async move {
            process(socket, peer).await;
        });
    }
}

async fn process(socket: tokio::net::TcpStream, peer: std::net::SocketAddr) {
    debug!("New connection from {}:{}", peer.ip(), peer.port());

    let codec = protocol::ZaichikCodec::new();
    let (read_half, write_half) = socket.into_split();

    let mut reader = tokio_util::codec::FramedRead::new(read_half, codec.clone());
    let mut writer = tokio_util::codec::FramedWrite::new(write_half, codec);

    while let Some(result) = reader.next().await {
        match result {
            Ok(frame) => {
                writer.send(frame).await.unwrap();
            }
            Err(e) => {
                error!("error on decoding from socket; error = {:?}", e);
            }
        }
    }
}
