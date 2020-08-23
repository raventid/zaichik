use crate::protocol;
use crate::topic_controller::Message;
use crate::topic_registry::TopicRegistry;
use futures::SinkExt;
use std::sync::{Arc, RwLock};
use std::time;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::stream::{StreamExt, StreamMap};

// MessageWrapper оборачивает Frame или сообщение от топика Topic, добавляя к нему
// дополнительную информацию, например, когда он был получен брокером. Создан
// он для того, чтобы быть общим форматом сообщения для обработки в tokio::select!,
// который мы используем для выбора между обработкой командного сообщения от клиента (Subscribe, CreateTopic)
// или сообщения от топика, которое нужно оптравить клиенту.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageWrapper {
    Frame {
        frame: protocol::ZaichikFrame,
        received_at: time::Instant,
    },
    TopicMessage {
        topic_name: String,
        message: Message,
    },
}

impl MessageWrapper {
    pub fn from_frame(frame: protocol::ZaichikFrame) -> MessageWrapper {
        MessageWrapper::Frame {
            frame,
            received_at: time::Instant::now(),
        }
    }

    pub fn from_topic_message(topic_name: String, message: Message) -> MessageWrapper {
        MessageWrapper::TopicMessage {
            topic_name,
            message,
        }
    }
}

// Наш сабскрипшн менеджер будет асинхронным компонентом, который будет читать из броадкаста
// и писать в клиентский стрим нужные сообщения.
// Его задача в основном хранить настройки и координировать действия.
pub struct SubscriptionManager {
    topic_registry: Arc<RwLock<TopicRegistry>>,
    commands_receiver: tokio::sync::mpsc::Receiver<MessageWrapper>,
    client_connection: tokio_util::codec::FramedWrite<OwnedWriteHalf, protocol::ZaichikCodec>,
    waiting_for_next_message: bool,
}

impl SubscriptionManager {
    pub async fn start_loop(
        peer: std::net::SocketAddr,
        topic_registry: Arc<RwLock<TopicRegistry>>,
        commands_receiver: tokio::sync::mpsc::Receiver<MessageWrapper>,
        client_connection: tokio_util::codec::FramedWrite<OwnedWriteHalf, protocol::ZaichikCodec>,
    ) {
        debug!(
            "[{}:{}] Starting SubscriptionManager",
            peer.ip(),
            peer.port()
        );

        let mut manager = SubscriptionManager {
            topic_registry,
            commands_receiver,
            client_connection,
            waiting_for_next_message: false,
        };

        let mut subscriptions = StreamMap::new();

        // Обрабатываем, как команды от управляющего потока, так и то, что нам прилетает из
        // мультиплексированного стрима всех подписок на топики.
        loop {
            let message = tokio::select! {
                Some(message) = manager.commands_receiver.recv() => message,

                Some((topic_name, Ok(message))) = subscriptions.next(),
                   if manager.waiting_for_next_message =>
                     MessageWrapper::from_topic_message(topic_name, message),

                else => break,
            };

            match message {
                // Эта ветка обрабатывает команды от клиента.
                MessageWrapper::Frame { frame, received_at } => {
                    debug!(
                        "[{}:{}] Received broadcast with frame {:?} || Waiting for message: {} || Subscribed on: {:?}",
                        peer.ip(),
                        peer.port(),
                        frame,
                        manager.waiting_for_next_message,
                        subscriptions.keys().collect::<Vec<_>>()
                    );

                    match frame {
                        protocol::ZaichikFrame::CreateTopic {
                            topic,
                            retention_ttl,
                            compaction_window,
                        } => {
                            if !Self::topic_exists(&manager.topic_registry, &topic) {
                                Self::create_topic(
                                    &manager.topic_registry,
                                    &topic,
                                    retention_ttl,
                                    compaction_window,
                                );
                            }
                        }
                        protocol::ZaichikFrame::Subscribe { topic } => {
                            // Если это наша первая подписка, то отметим, что
                            // наш клиент готов получать сообщения.
                            if subscriptions.is_empty() {
                                manager.waiting_for_next_message = true;
                            };

                            // Если у нас нет такого топика, то заведем его с настройками
                            // по умолчанию.
                            if !Self::topic_exists(&manager.topic_registry, &topic) {
                                Self::create_topic_with_defaults(&manager.topic_registry, &topic);
                            }

                            let topic_registry = manager.topic_registry.read().unwrap();
                            let topic_controller = topic_registry.topics.get(&topic).unwrap();

                            // Добавляем новую подписку на новый топик.
                            let topic_controller = topic_controller.read().unwrap();
                            let topic_stream = topic_controller.subscribe();
                            subscriptions.insert(topic, Box::pin(topic_stream));
                        }
                        protocol::ZaichikFrame::Unsubscribe { topic } => {
                            // Удаляем подписку на топик и ее стрим.
                            subscriptions.remove(&topic);

                            // Если мы удалили последнюю подписку, то отметим, что
                            // клиент больше не готов получать сообщения.
                            if subscriptions.is_empty() {
                                manager.waiting_for_next_message = false;
                            }
                        }
                        protocol::ZaichikFrame::Publish {
                            topic,
                            key,
                            payload,
                        } => {
                            // Если у нас не было такого топика, то добавим его в реестр,
                            // с настройками по умолчанию.
                            if !Self::topic_exists(&manager.topic_registry, &topic) {
                                Self::create_topic_with_defaults(&manager.topic_registry, &topic)
                            }

                            let topic_registry = manager.topic_registry.read().unwrap();
                            let topic_controller = topic_registry.get_topic(&topic).unwrap();

                            // Так как топик контроллер должен поддерживать консистентность
                            // записи мы берем уникальный лок на запись.
                            let mut topic_controller = topic_controller.write().unwrap();
                            topic_controller.publish(key, payload, received_at);
                        }
                        protocol::ZaichikFrame::Commit => {
                            // Просто помечаем, что наш клиент справился с предыдущим
                            // сообщением и готов к приему нового.
                            manager.waiting_for_next_message = true;
                        }
                        protocol::ZaichikFrame::CloseConnection => {
                            // Завершаем SubscriptionManager. Клиент закрыл соединение.
                            break;
                        }
                    };
                }
                MessageWrapper::TopicMessage {
                    topic_name,
                    message,
                } => {
                    debug!(
                        "[{}:{}] Client is ready to receive message",
                        peer.ip(),
                        peer.port(),
                    );

                    if !Self::message_is_out_of_date(&message) {
                        // Для отправки сообщения обратно на клиент мы
                        // используем фрейм Publish, можно было бы сделать
                        // разные кодеки для Sink, Stream.
                        let frame = protocol::ZaichikFrame::Publish {
                            topic: topic_name,
                            key: message.key,
                            payload: message.payload,
                        };

                        debug!(
                            "[{}:{}] Sending Frame to client || {:?}",
                            peer.ip(),
                            peer.port(),
                            frame.clone(),
                        );

                        match manager.client_connection.send(frame).await {
                            // Отметим, что отправили сообщение, ждем следующего
                            // коммита от пользователя.
                            Ok(_) => manager.waiting_for_next_message = false,
                            Err(e) => info!(
                                "[{}:{}] TCP connection error:  {}",
                                peer.ip(),
                                peer.port(),
                                e,
                            ),
                        }

                        debug!("[{}:{}] Frame sending handled", peer.ip(), peer.port(),);
                    } else {
                        debug!(
                            "[{}:{}] Frame is out of date, skipping",
                            peer.ip(),
                            peer.port()
                        )
                    }
                }
            }
        }

        debug!(
            "[{}:{}] Stopped SubscriptionManager",
            peer.ip(),
            peer.port()
        );
    }

    fn topic_exists(registry: &Arc<RwLock<TopicRegistry>>, topic: &str) -> bool {
        let reader = registry.read().unwrap();
        reader.topics.contains_key(topic)
    }

    fn create_topic(
        registry: &Arc<RwLock<TopicRegistry>>,
        topic: &str,
        retention_ttl: u64,
        compaction_window: u64,
    ) {
        let mut writer = registry.write().unwrap();
        writer.create_topic(topic.to_string(), retention_ttl, compaction_window);
    }

    fn create_topic_with_defaults(registry: &Arc<RwLock<TopicRegistry>>, topic: &str) {
        let mut writer = registry.write().unwrap();
        // По умолчанию не будем включать ни ретеншн, ни компакшн.
        writer.create_topic(topic.to_string(), 0, 0);
    }

    fn message_is_out_of_date(message: &Message) -> bool {
        if message.expires_at.is_none() {
            false
        } else {
            time::Instant::now() > message.expires_at.unwrap()
        }
    }
}
