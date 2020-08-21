use crate::protocol::ZaichikFrame;
use im;
use std::collections::{HashMap, VecDeque};
use std::ops::Add;
use std::sync::{Arc, Mutex, RwLock};
use std::time;
use tokio::stream::{self, StreamExt};
use tokio::sync::broadcast;

pub type TopicName = String;

// Для сообщений в компоненте, который управляет подпиской мы будем использовать
// отдельный внутренний тип Message. Он нам нужен для того, чтобы добавить чуть
// больше гибкости и иметь возможность добавлять к сообщениям дополнительные поля или методы.
// Это юнит хранения сообщения в MessageBuffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub key: Option<String>,
    pub payload: Vec<u8>,
    received_at: time::Instant,
    pub expires_at: Option<time::Instant>,
}

#[derive(Debug)]
pub struct TopicRegistry {
    pub topics: HashMap<TopicName, RwLock<TopicController>>,
}

impl TopicRegistry {
    pub fn new() -> TopicRegistry {
        TopicRegistry {
            topics: HashMap::new(),
        }
    }

    pub fn create_topic(
        &mut self,
        topic: TopicName,
        retention_ttl: u64,
        compaction_window: u64,
    ) -> Option<&RwLock<TopicController>> {
        let topic_controller = RwLock::new(TopicController::new(
            topic.clone(),
            retention_ttl,
            compaction_window,
            10_000,
        ));

        self.topics.insert(topic.clone(), topic_controller);
        self.topics.get(&topic)
    }

    pub fn get_topic(&self, topic: TopicName) -> Option<&RwLock<TopicController>> {
        self.topics.get(&topic)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TopicSettings {
    pub retention_ttl: Option<time::Duration>,
    pub compaction_window: Option<time::Duration>,
    pub buffer_size: usize,
}

impl TopicSettings {
    pub fn new(retention_ttl: u64, compaction_window: u64, buffer_size: usize) -> TopicSettings {
        let retention_ttl = if retention_ttl == 0 {
            None
        } else {
            Some(time::Duration::from_millis(retention_ttl))
        };
        let compaction_window = if compaction_window == 0 {
            None
        } else {
            Some(time::Duration::from_millis(compaction_window))
        };
        let buffer_size = if buffer_size == 0 { 1000 } else { 0 } as usize;

        TopicSettings {
            retention_ttl,
            compaction_window,
            buffer_size,
        }
    }
}

#[derive(Debug)]
pub struct TopicController {
    // Ключ сообщения, время, когда получили сообщение.
    name: TopicName,                                    // Clone / Sync
    broadcast_sender: broadcast::Sender<Message>,       // Clone? / Sync?
    settings: TopicSettings,                            // Copy + Clone / Sync?
    compaction_map: im::HashMap<String, time::Instant>, // Sync
    retained_buffer: im::Vector<Message>,               // Sync
}

impl TopicController {
    pub fn new(
        name: TopicName,
        retention_ttl: u64,
        compaction_window: u64,
        buffer_size: u32,
    ) -> TopicController {
        let settings = TopicSettings::new(retention_ttl, compaction_window, buffer_size as usize);
        let (broadcast_sender, _) = broadcast::channel(buffer_size as usize);
        let compaction_map = im::HashMap::new();
        let retained_buffer = im::Vector::new();

        // Делаем канал
        TopicController {
            name,
            broadcast_sender,
            settings,
            compaction_map,
            retained_buffer,
        }
    }

    pub fn publish(&mut self, key: Option<String>, payload: Vec<u8>, received_at: time::Instant) {
        // Устанавливаем опциональный expires_at, если наш topic поддерживает retention.
        let message = Message {
            key,
            payload,
            received_at,
            expires_at: self
                .settings
                .retention_ttl
                .map(|millis| received_at.add(millis)),
        };

        // Проверяем не дубль ли это сообщения, если у нас включен compaction
        let is_duplicate = if let Some(compaction_window) = self.settings.compaction_window {
            self.check_duplicate_and_update_dedup_map(&message, compaction_window)
        } else {
            false
        };

        if !is_duplicate {
            match self.broadcast_sender.send(message.clone()) {
                Ok(count_subscribers) => debug!(
                    "[TopicController:{}] Sent to {} subscribers",
                    self.name, count_subscribers,
                ),
                Err(_) => debug!(
                    "[TopicController:{}] No subscribers to receive message",
                    self.name,
                ),
            };
        }

        // Пройдемся по буфферу и оставим только те элементы, которые
        // все еще не истекли по времени. Такую работу не очень хорошо
        // делать на каждом publish сообщения, но мы позволим себе этот
        // ход для упрощения.
        if self.settings.retention_ttl.is_some() {
            self.retained_buffer
                .retain(|message| message.expires_at.unwrap() > time::Instant::now());
        }

        // Также, если мы используем compaction для топика, то мы хотели
        // бы не бесконечно увеличивать размер хэшмапы. Мы наивно будет удалять
        // удалять ключи, которые уже точно устарели, что не является очень
        // эффективной и удачной операцией в publish, но для упрощения мы оставим
        // этот код здесь.

        self.retained_buffer.push_back(message);
    }

    // Объединяем retained сообщения и канал Receiver, куда будут поступать сообщения.
    // Наш брокер гарантирует порядок доставки сообщений в рамках одного топика, поэтому
    // мы используем chain комбинатор, чтобы вначале отдать старые сообщения, а уже потом
    // начать слушать текущий stream из топика.
    pub fn subscribe(
        &self,
    ) -> impl tokio::stream::Stream<Item = Result<Message, tokio::sync::broadcast::RecvError>> {
        let retained_messages = self
            .retained_buffer
            .iter()
            .map(|message| Ok(message.clone()))
            .collect::<Vec<_>>();

        let subscription = self.broadcast_sender.subscribe().into_stream();

        stream::iter(retained_messages).chain(subscription)
    }

    fn check_duplicate_and_update_dedup_map(
        &mut self,
        message: &Message,
        compaction_window: time::Duration,
    ) -> bool {
        // Если у сообщения нет ключа для compaction, то мы ничего не будет предпринимать.
        if message.key.is_none() {
            return false;
        }

        let now = time::Instant::now();
        let key = message.key.as_ref().unwrap();

        match self.compaction_map.get(key) {
            Some(last_sent_at) => {
                let since_last_seen = now.duration_since(*last_sent_at);
                if since_last_seen < compaction_window {
                    // Если мы отравляли сообщение не так давно,
                    // то скажем, что текущее сообщение дубликат.
                    true
                } else {
                    // Здесь мы видим, что можем повторить отправку,
                    // сообщение ушло давно.
                    self.compaction_map.insert(key.to_string(), now);
                    false
                }
            }
            None => {
                // Мы еще не встречали такого сообщения,
                // отправим его и пометим, что оно ушло сейчас.
                self.compaction_map.insert(key.to_string(), now);
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initializing_topic_settings_with_default() {}

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
            let mut r = TopicRegistry::new();
            r.add_topic("topic_name".to_string(), 0, 5000);
            r
        }));

        let in_past = time::Instant::now()
            .checked_sub(time::Duration::from_millis(5000))
            .unwrap();
        let message1 = Message {
            key: Some("same".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
            expires_at: None,
        };
        let message2 = Message {
            key: Some("different".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
            expires_at: None,
        };

        buffer.queue_message(message1.clone());
        buffer.queue_message(message2.clone());

        let out1 = buffer.next(&subscribed_on, &topic_registry);
        let out2 = buffer.next(&subscribed_on, &topic_registry);

        assert_eq!(out1, Some(message1));
        assert_eq!(out2, Some(message2)); // Второе сообщение прошло, потому что другой ключ
    }
}
