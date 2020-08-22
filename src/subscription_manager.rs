use crate::protocol;
use crate::topic_controller;
use crate::topic_controller::Message;
use crate::topic_controller::{TopicController, TopicRegistry};
use crate::topic_registry::TopicName;
use futures::SinkExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::read;
use std::sync::{Arc, RwLock};
use std::time;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::stream::{Stream, StreamExt, StreamMap};


// FrameWrapper оборачивает Frame, добавляя к нему
// дополнительную информацию, например кто отправил фрейм,
// и когда он был получен брокером.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageWrapper {
    Frame {
        inner: protocol::ZaichikFrame,
        received_at: time::Instant,
        send_by: std::net::SocketAddr,
    },
    TopicMessage {
        topic_name: String,
        inner: Message,
    },
}

impl MessageWrapper {
    pub fn from_frame(frame: protocol::ZaichikFrame, peer: std::net::SocketAddr) -> MessageWrapper {
        MessageWrapper::Frame {
            inner: frame,
            received_at: time::Instant::now(),
            send_by: peer,
        }
    }

    pub fn from_topic_message(topic_name: String, inner: Message) -> MessageWrapper {
        MessageWrapper::TopicMessage { topic_name, inner }
    }
}

// Наш сабскрипшн менеджер будет асинхронным компонентом, который будет читать из броадкаста
// и писать в клиентский стрим нужные сообщения.
// Его задача в основном хранить настройки и координировать действия. Логика по фильтрации
// сообщений находится в MessagesBuffer.
pub struct SubscriptionManager {
    subscribed_on: HashSet<String>,
    topic_registry: Arc<RwLock<TopicRegistry>>,
    broadcast_receiver: tokio::sync::broadcast::Receiver<MessageWrapper>,
    client_connection: tokio_util::codec::FramedWrite<OwnedWriteHalf, protocol::ZaichikCodec>,
    waiting_for_next_message: bool,
}

impl SubscriptionManager {
    pub async fn start_loop(
        peer: std::net::SocketAddr,
        topic_registry: Arc<RwLock<TopicRegistry>>,
        broadcast_receiver: tokio::sync::broadcast::Receiver<MessageWrapper>,
        client_connection: tokio_util::codec::FramedWrite<OwnedWriteHalf, protocol::ZaichikCodec>,
    ) {
        debug!(
            "[{}:{}] Starting SubscriptionManager",
            peer.ip(),
            peer.port()
        );

        let mut manager = SubscriptionManager {
            subscribed_on: HashSet::new(),
            topic_registry,
            broadcast_receiver,
            client_connection,
            waiting_for_next_message: false,
        };

        // StreamMap<String, Pin<Box<impl Stream<Item=Result<Message, RecvError>>>>>
        let mut topic_streams = StreamMap::new();

        // Обрабатываем, как команды от управляющего потока, так и то, что нам прилетает из
        // мультиплексированного стрима всех подписок на топики.
        loop {
            let message = tokio::select! {
                // message
                Ok(message) = manager.broadcast_receiver.recv() => message,
                // Option<(topic_name, result<Message, receive_error>)>
                Some((topic_name, Ok(message))) = topic_streams.next(), if manager.waiting_for_next_message => MessageWrapper::from_topic_message(topic_name, message),
                else => break,
            };

            match message {
                MessageWrapper::Frame {
                    inner,
                    received_at,
                    send_by,
                } => {
                    let frame = inner;

                    debug!(
                        "[{}:{}] Received broadcast with frame {:?}",
                        peer.ip(),
                        peer.port(),
                        frame
                    );

                    debug!(
                        "[{}:{}] Client is waiting for message: {} || Subscribed on: {:?}",
                        peer.ip(),
                        peer.port(),
                        manager.waiting_for_next_message,
                        manager.subscribed_on
                    );

                    // На старте проекта мне показалось, что возможность видеть коммиты,
                    // подписки и любые события адресованные другим клиентам было бы интересно
                    // и на этой основе можно было бы реализовать логику внутри обработчика
                    // (например: если клиент 123 подписался на топик "Зайцы", то мы можем от него отписаться)
                    // На деле оказалось, что я этой возможностью не пользуюсь, а в канале создается
                    // лишний шум из-за большого количества чужих сообщений.
                    if peer == send_by {
                        match frame {
                            protocol::ZaichikFrame::CreateTopic {
                                topic,
                                retention_ttl,
                                dedup_ttl,
                            } => {
                                // Сейчас CreateTopic затирает предыдущие настройки, если кто-то пытается
                                // пересоздать топик. Другим вариантом было бы выдавать на такое ошибку или
                                // игнорировать изменения.
                                // todo c новым подходом, где мы храним контроллеры, нам уже не стоит так делать
                                // как мы делали раньше, нам стоит игнорировать команду все-таки.
                                // let mut registry = manager.topic_registry.write().unwrap();
                                // registry.create_topic(topic, retention_ttl, dedup_ttl);

                                if !Self::topic_exists(&manager.topic_registry, &topic) {
                                    Self::create_topic(
                                        &manager.topic_registry,
                                        &topic,
                                        retention_ttl,
                                        dedup_ttl,
                                    );
                                }
                            }
                            protocol::ZaichikFrame::Subscribe { topic } => {
                                // Дополняем наш HashSet подписок.
                                // Если это наша первая подписка, то отметим, что
                                // наш клиент готов получать сообщения.
                                if manager.subscribed_on.is_empty() {
                                    manager.waiting_for_next_message = true;
                                };

                                // Если у нас нет такого топика в реестре, то заведем его.
                                let topic_exists = {
                                    let registry = manager.topic_registry.read().unwrap();
                                    registry.topics.contains_key(&topic)
                                };

                                if !topic_exists {
                                    let mut topic_registry =
                                        manager.topic_registry.write().unwrap();
                                    topic_registry.create_topic(topic.clone(), 0, 0);
                                }

                                let registry = manager.topic_registry.read().unwrap();
                                let topic_controller = registry.topics.get(&topic).unwrap();
                                manager.subscribed_on.insert(topic.clone());

                                // Добавляем в нашу мапу стримов откуда читаем новый новую подписку.
                                let topic_controller = topic_controller.read().unwrap();
                                let stream = topic_controller.subscribe();
                                topic_streams.insert(topic, Box::pin(stream));
                            }
                            protocol::ZaichikFrame::Unsubscribe { topic } => {
                                manager.subscribed_on.remove(&topic);

                                // Удаляем подписку и ее стрим, в этот момент у нас разрушается один из
                                // ридеров.
                                topic_streams.remove(&topic);
                            }
                            protocol::ZaichikFrame::Publish {
                                topic,
                                key,
                                payload,
                            } => {
                                if !Self::topic_exists(&manager.topic_registry, &topic) {
                                    Self::create_topic(&manager.topic_registry, &topic, 0, 0)
                                }

                                let reader = manager.topic_registry.read().unwrap();
                                let topic_controller = reader.get_topic(topic.to_string()).unwrap();

                                let mut topic_controller = topic_controller.write().unwrap();
                                topic_controller.publish(key, payload, received_at);
                            }
                            protocol::ZaichikFrame::Commit => {
                                // Просто помечаем, что наш клиент справился с предыдущим
                                // сообщением и готов к приему нового.
                                manager.waiting_for_next_message = true;
                            }
                            protocol::ZaichikFrame::CloseConnection => {
                                break;
                            }
                        };
                    }
                }
                MessageWrapper::TopicMessage { topic_name, inner } => {
                    debug!(
                        "[{}:{}] Client is ready to receive message",
                        peer.ip(),
                        peer.port(),
                    );

                    let message = inner;

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
        dedup_ttl: u64,
    ) {
        let mut writer = registry.write().unwrap();
        writer.create_topic(topic.clone().parse().unwrap(), retention_ttl, dedup_ttl);
    }

    fn message_is_out_of_date(message: &Message) -> bool {
        if message.expires_at.is_none() {
            false
        } else {
            time::Instant::now() > message.expires_at.unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use tokio::time::Duration;

    #[test]
    fn test_popping_value_with_retention_ttl() {
        let mut buffer = MessagesBuffer::new();
        let mut subscribed_on = HashSet::new();
        subscribed_on.insert("topic_name".to_string());
        let topic_registry = Arc::new(RwLock::new({
            let mut r = topic_registry::TopicRegistry::new();
            r.add_topic("topic_name".to_string(), 5000, 0);
            r
        }));

        let in_past = time::Instant::now()
            .checked_sub(time::Duration::from_millis(5000))
            .unwrap();
        let message = Message {
            topic: "topic_name".to_string(),
            key: None,
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
        };

        buffer.queue_message(message);
        let message = buffer.next(&subscribed_on, &topic_registry);
        assert!(message.is_none())
    }
}
