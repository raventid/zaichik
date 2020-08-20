use crate::protocol;
use crate::topic_controller;
use crate::topic_controller::TopicRegistry;
use futures::SinkExt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::stream::StreamMap;
use crate::topic_registry::TopicName;

// FrameWrapper оборачивает Frame, добавляя к нему
// дополнительную информацию, например кто отправил фрейм,
// и когда он был получен брокером.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameWrapper {
    inner: protocol::ZaichikFrame,
    received_at: time::Instant,
    send_by: std::net::SocketAddr,
}

impl FrameWrapper {
    pub fn new(frame: protocol::ZaichikFrame, peer: std::net::SocketAddr) -> FrameWrapper {
        FrameWrapper {
            inner: frame,
            received_at: time::Instant::now(),
            send_by: peer,
        }
    }
}

// Для сообщений в компоненте, который управляет подпиской мы будем использовать
// отдельный внутренний тип Message. Он нам нужен для того, чтобы добавить чуть
// больше гибкости и иметь возможность добавлять к сообщениям дополнительные поля или методы.
// Это юнит хранения сообщения в MessageBuffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    topic: String,
    key: Option<String>,
    payload: Vec<u8>,
    received_at: time::Instant,
    pub expires_at: time::Instant,
}

// Наш сабскрипшн менеджер будет асинхронным компонентом, который будет читать из броадкаста
// и писать в клиентский стрим нужные сообщения.
// Его задача в основном хранить настройки и координировать действия. Логика по фильтрации
// сообщений находится в MessagesBuffer.
pub struct SubscriptionManager {
    subscribed_on: StreamMap<TopicName, tokio::stream::Stream<Item = Result<Message, String>>>,
    message_buffer: MessagesBuffer,
    topic_registry: Arc<RwLock<TopicRegistry>>,
    broadcast_receiver: tokio::sync::broadcast::Receiver<FrameWrapper>,
    client_connection: tokio_util::codec::FramedWrite<OwnedWriteHalf, protocol::ZaichikCodec>,
    waiting_for_next_message: bool,
}

impl SubscriptionManager {
    pub async fn start_loop(
        peer: std::net::SocketAddr,
        topic_registry: Arc<RwLock<TopicRegistry>>,
        broadcast_receiver: tokio::sync::broadcast::Receiver<FrameWrapper>,
        client_connection: tokio_util::codec::FramedWrite<OwnedWriteHalf, protocol::ZaichikCodec>,
    ) {
        debug!(
            "[{}:{}] Starting SubscriptionManager",
            peer.ip(),
            peer.port()
        );

        let mut manager = SubscriptionManager {
            subscribed_on: HashSet::new(),
            message_buffer: MessagesBuffer::new(),
            topic_registry,
            broadcast_receiver,
            client_connection,
            waiting_for_next_message: false,
        };

        while let Ok(frame) = manager.broadcast_receiver.recv().await {
            debug!(
                "[{}:{}] Received broadcast with frame {:?}",
                peer.ip(),
                peer.port(),
                frame
            );

            debug!("[{}:{}] Current lag: {} || Client is waiting for message: {} || Subscribed on: {:?}",
                                   peer.ip(),
                                   peer.port(), manager.message_buffer.lag(), manager.waiting_for_next_message, manager.subscribed_on);

            // На старте проекта мне показалось, что возможность видеть коммиты,
            // подписки и любые события адресованные другим клиентам было бы интересно
            // и на этой основе можно было бы реализовать логику внутри обработчика
            // (например: если клиент 123 подписался на топик "Зайцы", то мы можем от него отписаться)
            // На деле оказалось, что я этой возможностью не пользуюсь, а в канале создается
            // лишний шум из-за большого количества чужих сообщений.
            if Self::peer_is_me(&frame, peer) {
                match frame.inner {
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
                        let mut registry = manager.topic_registry.write().unwrap();
                        registry.add_topic(topic, retention_ttl, dedup_ttl);
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
                            let mut topic_registry = manager.topic_registry.write().unwrap();
                            topic_registry.add_topic(topic.clone(), 0, 0);
                        }

                        let registry = manager.topic_registry.read().unwrap();
                        let topic_controller = registry.topics.get(&topic).unwrap();
                        manager.subscribed_on.insert(topic, topic_controller.subscribe());
                    }
                    protocol::ZaichikFrame::Unsubscribe { topic } => {
                        manager.subscribed_on.remove(&topic);
                        // Если мы отписались от всех подписок в системе,
                        // то полностью очистим буфер сообщений.
                        // Они нам не понадобятся, пока клиент не захочется
                        // подписаться на что-нибудь новое.

                        // UPD: Решил пойти по пути хранения всех сообщений и дальше.
                        // Не будем удалять.
                        // if manager.subscribed_on.is_empty() {
                        //     manager.message_buffer = MessagesBuffer::new();
                        // }
                    }
                    protocol::ZaichikFrame::Publish {
                        topic,
                        key,
                        payload,
                    } => {
                        // Тут нам приходит броадкаст с новым сообщением.
                        // Мы будем копировать в буфер все сообщения, так как клиент может захотеть
                        // подписаться на новый топик в любой момент и мы хотели бы отдавать ему
                        // все данные, которые у нас есть.
                        let message = Message {
                            topic,
                            key,
                            payload,
                            received_at: frame.received_at,
                            expires_at: time::Instant::now(),
                        };

                        manager.message_buffer.queue_message(message);
                    }
                    protocol::ZaichikFrame::Commit => {
                        // Просто помечаем, что наш клиент справился с предыдущим
                        // сообщением и готов к приему нового.
                        manager.waiting_for_next_message = true;
                    }
                    protocol::ZaichikFrame::CloseConnection => {
                        break;
                    }
                }
            };

            // Если наш клиент готов к приму нового сообщения
            // и если какое-то из сообщений доступно, то отправим его.
            if manager.waiting_for_next_message {
                // let message = manager
                //     .message_buffer
                //     .next(&manager.subscribed_on, &manager.topic_registry);

                // Асинк не всегда удобно писать во вложенном блоке
                // поэтому иногда приходится писать is_some(), а потом
                // делать unwrap().
                if message.is_some() {
                    let unwrapped = message.unwrap();
                    // Для отправки сообщения обратно на клиент мы
                    // используем фрейм Publish, можно было бы сделать
                    // разные кодеки для Sink, Stream.
                    let frame = protocol::ZaichikFrame::Publish {
                        topic: unwrapped.topic,
                        key: unwrapped.key,
                        payload: unwrapped.payload,
                    };

                    match manager.client_connection.send(frame).await {
                        // Отметим, что отправили сообщение, ждем следующего
                        // коммита от пользователя.
                        Ok(_) => {
                            debug!(
                                "[{}:{}] Sent Frame to client || Current lag is {}",
                                peer.ip(),
                                peer.port(),
                                manager.message_buffer.lag()
                            );
                            manager.waiting_for_next_message = false
                        }
                        Err(e) => info!("{}", e),
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

    fn peer_is_me(frame: &FrameWrapper, peer: std::net::SocketAddr) -> bool {
        match frame.inner {
            protocol::ZaichikFrame::Publish {
                topic: _,
                key: _,
                payload: _,
            } => true,
            _ => frame.send_by == peer,
        }
    }
}

// Внутренний буфер для хранения всех сообщений в которых заинтересован наш клиент.
// Данный буфер умеет следить за retention, делать dedup в рамках ttl.
struct MessagesBuffer {
    buffer: VecDeque<Message>,
    dedup_map: HashMap<String, time::Instant>,
}

impl MessagesBuffer {
    pub fn new() -> MessagesBuffer {
        MessagesBuffer {
            buffer: VecDeque::new(),
            dedup_map: HashMap::new(),
        }
    }

    fn queue_message(&mut self, message: Message) {
        self.buffer.push_back(message)
    }

    fn next(
        &mut self,
        subscribed_on: &HashSet<topic_controller::TopicName>,
        // тут пока оставим регистри
        topic_registry: &Arc<RwLock<crate::topic_registry::TopicRegistry>>,
    ) -> Option<Message> {
        while !self.buffer.is_empty() {
            if let Some(message) = self.buffer.pop_front() {
                // Если мы не нашли настройки для топика, то проигнорируем сообщение и пойдем дальше.
                // Нам имеют право присылать сообщения, которые не привязаны ни к какому существующему
                // топику. Такие сообщения начнут потреблять тогда, когда кто-то подпишется на топик
                // или создаст его с помощью CreateTopic. Но можно и запретить такое.
                let registry = topic_registry.read().unwrap();
                let maybe_topic_settings = registry.topics.get(&message.topic);
                if maybe_topic_settings.is_none() {
                    continue;
                }
                let topic_settings = maybe_topic_settings.unwrap();

                let not_interested_in = !subscribed_on.contains(&message.topic);
                let out_of_date = topic_settings.retention_ttl.is_some()
                    && Self::message_is_out_of_date(
                        &message,
                        topic_settings.retention_ttl.unwrap(),
                    );

                let is_duplicate = !not_interested_in
                    && !out_of_date
                    && topic_settings.dedup_ttl.is_some()
                    && self.check_duplicate_and_update_dedup_map(
                        &message,
                        topic_settings.dedup_ttl.unwrap(),
                    );

                if not_interested_in || out_of_date || is_duplicate {
                    continue;
                } else {
                    return Some(message);
                }
            }
        }

        None
    }

    fn check_duplicate_and_update_dedup_map(
        &mut self,
        message: &Message,
        dedup_ttl: time::Duration,
    ) -> bool {
        // Если у сообщения нет ключа для дедупликации, то мы ничего не будет предпринимать
        if message.key.is_none() {
            return false;
        }

        let now = time::Instant::now();
        let key = message.key.as_ref().unwrap();

        match self.dedup_map.get(key) {
            Some(last_sent_at) => {
                let since_last_seen = now.duration_since(*last_sent_at);
                if since_last_seen < dedup_ttl {
                    // Если мы отравляли сообщение не так давно,
                    // то скажем, что текущее сообщение дубликат.
                    true
                } else {
                    // Здесь мы видим, что можем повторить отравку,
                    // сообщение ушло приличное время назад.
                    self.dedup_map.insert(key.to_string(), now);
                    false
                }
            }
            None => {
                // Мы еще не встречали такого сообщения,
                // отправим его и пометим, что оно ушло сейчас.
                self.dedup_map.insert(key.to_string(), now);
                false
            }
        }
    }

    fn message_is_out_of_date(message: &Message, retention_ttl: time::Duration) -> bool {
        let time_passed = time::Instant::now().duration_since(message.received_at);
        time_passed > retention_ttl
    }

    fn lag(&self) -> usize {
        self.buffer.len()
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

    #[test]
    fn test_dedup_works() {
        let mut buffer = MessagesBuffer::new();
        let mut subscribed_on = HashSet::new();
        subscribed_on.insert("topic_name".to_string());
        let topic_registry = Arc::new(RwLock::new({
            let mut r = topic_registry::TopicRegistry::new();
            r.add_topic("topic_name".to_string(), 0, 5000);
            r
        }));

        let in_past = time::Instant::now()
            .checked_sub(time::Duration::from_millis(5000))
            .unwrap();
        let message1 = Message {
            topic: "topic_name".to_string(),
            key: Some("same".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
        };
        let message2 = Message {
            topic: "topic_name".to_string(),
            key: Some("same".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
        };

        buffer.queue_message(message1.clone());
        buffer.queue_message(message2.clone());

        let out1 = buffer.next(&subscribed_on, &topic_registry);
        let out2 = buffer.next(&subscribed_on, &topic_registry);

        assert_eq!(out1, Some(message1));
        assert_eq!(out2, None); // Второе сообщение является дубликатом
    }

    #[test]
    fn test_do_not_dedup_if_too_much_time_passed() {
        let mut buffer = MessagesBuffer::new();
        let mut subscribed_on = HashSet::new();
        subscribed_on.insert("topic_name".to_string());
        let topic_registry = Arc::new(RwLock::new({
            let mut r = topic_registry::TopicRegistry::new();
            r.add_topic("topic_name".to_string(), 0, 1);
            r
        }));

        let in_past = time::Instant::now()
            .checked_sub(time::Duration::from_millis(5000))
            .unwrap();
        let message1 = Message {
            topic: "topic_name".to_string(),
            key: Some("same".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
        };
        let message2 = Message {
            topic: "topic_name".to_string(),
            key: Some("same".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
        };

        buffer.queue_message(message1.clone());
        buffer.queue_message(message2.clone());

        let out1 = buffer.next(&subscribed_on, &topic_registry);
        // Подождем перед отправкой следующего сообщения, чтобы обновить таблицу дедупликации
        sleep(Duration::from_millis(500));
        let out2 = buffer.next(&subscribed_on, &topic_registry);

        assert_eq!(out1, Some(message1));
        assert_eq!(out2, Some(message2)); // Второе сообщение не дулбикат, прошло много времени
    }

    #[test]
    fn test_dedup_works_with_different_keys() {
        let mut buffer = MessagesBuffer::new();
        let mut subscribed_on = HashSet::new();
        subscribed_on.insert("topic_name".to_string());
        let topic_registry = Arc::new(RwLock::new({
            let mut r = topic_registry::TopicRegistry::new();
            r.add_topic("topic_name".to_string(), 0, 5000);
            r
        }));

        let in_past = time::Instant::now()
            .checked_sub(time::Duration::from_millis(5000))
            .unwrap();
        let message1 = Message {
            topic: "topic_name".to_string(),
            key: Some("same".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
        };
        let message2 = Message {
            topic: "topic_name".to_string(),
            key: Some("different".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
        };

        buffer.queue_message(message1.clone());
        buffer.queue_message(message2.clone());

        let out1 = buffer.next(&subscribed_on, &topic_registry);
        let out2 = buffer.next(&subscribed_on, &topic_registry);

        assert_eq!(out1, Some(message1));
        assert_eq!(out2, Some(message2)); // Второе сообщение прошло, потому что другой ключ
    }
}
