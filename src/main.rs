mod protocol;
mod subscription_manager;
mod topic_controller;
mod topic_registry;

use crate::topic_registry::TopicRegistry;
use std::sync::{Arc, RwLock};
use tokio::stream::StreamExt;
use tokio::sync::mpsc;

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() {
    env_logger::init();

    let port = std::env::vars()
        .find(|(key, _value)| key == "PORT")
        .map(|(_key, value)| value)
        .unwrap_or_else(|| "8889".to_string());

    // База данных топиков, в которой хранятся ссылки на контроллеры топиков.
    let topic_registry = Arc::new(RwLock::new(TopicRegistry::new()));

    let mut listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    debug!("Started broker server at 127.0.0.1:{}", port);

    loop {
        // В peer хранится ip адрес и порт входящего подключения.
        let (socket, peer) = listener.accept().await.unwrap();
        let topic_registry = Arc::clone(&topic_registry);

        // Для каждого входящего подключения мы будем создавать отдельную задачу.
        tokio::spawn(async move {
            process(socket, peer, topic_registry).await;
        });
    }
}

async fn process(
    socket: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    topic_registry: Arc<RwLock<TopicRegistry>>,
) {
    debug!("New connection from {}:{}", peer.ip(), peer.port());

    let codec = protocol::ZaichikCodec::new();
    let (read_half, write_half) = socket.into_split();

    let mut reader = tokio_util::codec::FramedRead::new(read_half, codec.clone());
    let writer = tokio_util::codec::FramedWrite::new(write_half, codec);

    // Канал, для того, чтобы отправлять сообщения от клиента в управляющий компонент.
    let (mut subscription_manager_channel, commands_receiver) = mpsc::channel(1000);

    // Запись в сокет и управление подписками мы отдадим в отдельную задачу.
    tokio::spawn(async move {
        subscription_manager::SubscriptionManager::start_loop(
            peer,
            topic_registry,
            commands_receiver,
            writer,
        )
        .await
    });

    // Читаем фреймы, приходящие от клиента из сокета и передаем их в управляющий компонент.
    while let Some(result) = reader.next().await {
        match result {
            Ok(frame) => {
                let wrapped_frame = subscription_manager::MessageWrapper::from_frame(frame);
                subscription_manager_channel
                    .send(wrapped_frame)
                    .await
                    .unwrap();
            }
            Err(e) => {
                error!("error on decoding from socket; error = {:?}", e);
            }
        }
    }

    // Говорим управляющему модулю, что мы больше не работаем с клиентом.
    let _ = subscription_manager_channel
        .send(subscription_manager::MessageWrapper::from_frame(
            protocol::ZaichikFrame::CloseConnection {},
        ))
        .await;

    debug!("[{}:{}] Stopped client", peer.ip(), peer.port());
}
