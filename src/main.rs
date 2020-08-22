mod protocol;
mod subscription_manager;
mod topic_controller;
mod topic_registry;

// use crate::topic_registry::TopicRegistry;
use crate::topic_controller::TopicRegistry;
use std::sync::{Arc, RwLock};
use tokio::stream::StreamExt;

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() {
    env_logger::init();

    // Todo уберем глобальный броадкаст отсюда
    // Основной бродкаст системы. Через него все подписчики будут получать уведомления
    // о новых сообщениях, изменениях подписок и коммитах.
    let (broadcast, _) = tokio::sync::broadcast::channel(1000);

    // База данных топиков, в которой хранятся настройки для каждого из них.
    let topic_registry = Arc::new(RwLock::new(TopicRegistry::new()));

    let mut listener = tokio::net::TcpListener::bind("127.0.0.1:8889")
        .await
        .unwrap();

    debug!("Started broker server at {}", "127.0.0.1:8889".to_string());

    loop {
        // В peer хранится ip адрес и порт входящего подключения.
        let (socket, peer) = listener.accept().await.unwrap();
        let broadcast = broadcast.clone();
        let topic_registry = Arc::clone(&topic_registry);

        // Для каждого входящего подключения мы будем создавать отдельную задачу.
        tokio::spawn(async move {
            process(socket, peer, broadcast, topic_registry).await;
        });
    }
}

async fn process(
    socket: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    broadcast: tokio::sync::broadcast::Sender<subscription_manager::MessageWrapper>,
    topic_registry: Arc<RwLock<TopicRegistry>>,
) {
    debug!("New connection from {}:{}", peer.ip(), peer.port());

    let codec = protocol::ZaichikCodec::new();
    let (read_half, write_half) = socket.into_split();

    let mut reader = tokio_util::codec::FramedRead::new(read_half, codec.clone());
    let writer = tokio_util::codec::FramedWrite::new(write_half, codec);

    let broadcast_receiver = broadcast.subscribe();

    // Запись в сокет и работу с броадкастом мы отдадим в отдельную задачу
    tokio::spawn(async move {
        subscription_manager::SubscriptionManager::start_loop(
            peer,
            topic_registry,
            broadcast_receiver,
            writer,
        )
        .await
    });

    // Читать фреймы приходящие от клиента из сокета мы будем в этом цикле.
    while let Some(result) = reader.next().await {
        match result {
            Ok(frame) => {
                let wrapped_frame = subscription_manager::MessageWrapper::from_frame(frame, peer);
                broadcast.send(wrapped_frame).unwrap();
            }
            Err(e) => {
                error!("error on decoding from socket; error = {:?}", e);
            }
        }
    }

    debug!(
        "[{}:{}] Stopping SubscriptionManager",
        peer.ip(),
        peer.port()
    );
    let close = protocol::ZaichikFrame::CloseConnection {};
    // Не интересуемся результатом.
    let _ = broadcast.send(subscription_manager::MessageWrapper::from_frame(close, peer));

    debug!("[{}:{}] Stopped client", peer.ip(), peer.port());
}
