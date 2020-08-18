use crate::topic_registry;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::time;

// Для сообщений в компоненте, который управляет подпиской мы будем использовать
// отдельный внутренний тип Message. Он нам нужен для того, чтобы добавить чуть
// больше гибкости и иметь возможность добавлять к сообщениям дополнительные поля или методы.

// Пожалуй здесь мы лучше воспользуемся другой штучкой, мы можем сделать Message структурой,
// а другие типы сообщений уже сделать энумом, если так хочется. Потому что хранить мы будем
// только паблиши, а применять настройки подписки или создавать новые топики мы уже будем моментально.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Message {
    topic: String,
    key: Option<String>,
    payload: Vec<u32>,
    received_at: time::Instant,
}

// Наш сабскрипшн менеджер будет асинхронным компонентом, который будет читать из броадкаста
// и писать в клиентский стрим нужные сообщения, а также он будет вносить правки в разные компоненты
struct SubscriptionManager {
    message_buffer: MessagesBuffer,
    // topic_registry: Arc<RWLock<TopicRegistry>>
    // broadcast_receiver: broadcast::Receiver<Frame>
    // client_connection: TcpStream
}

impl SubscriptionManager {
    pub fn start_loop() {
        ()
    }

    fn send_to_client() {
        ()
    }

    // Метод для того, чтобы обратиться напрямую к SubscriptionManager
    // для обработки определенных команд, которые не требуют броадкаста.
    pub fn direct_dispatch() {
        ()
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

    fn next(&mut self, topic_settings: topic_registry::TopicSettings) -> Option<Message> {
        while self.buffer.len() != 0 {
            match self.buffer.pop_front() {
                Some(message) => {
                    let mut out_of_date = false;
                    let mut is_duplicate = false;
                    if topic_settings.retention_ttl.is_some() {
                        out_of_date = Self::message_is_out_of_date(
                            &message,
                            topic_settings.retention_ttl.unwrap(),
                        );
                    }

                    if !out_of_date && topic_settings.dedup_ttl.is_some() {
                        is_duplicate =
                            self.update_dedup_map(&message, topic_settings.dedup_ttl.unwrap());
                    }

                    if !out_of_date && !is_duplicate {
                        return Some(message);
                    }
                }
                None => (),
            }
        }

        None
    }

    fn update_dedup_map(&mut self, message: &Message, dedup_ttl: time::Duration) -> bool {
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
        let topic_settings = topic_registry::TopicSettings {
            retention_ttl: Some(time::Duration::from_millis(5000)),
            dedup_ttl: None,
        };

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
        let message = buffer.next(topic_settings);
        assert!(message.is_none())
    }

    #[test]
    fn test_dedup_works() {
        let mut buffer = MessagesBuffer::new();
        let topic_settings = topic_registry::TopicSettings {
            retention_ttl: None,
            dedup_ttl: Some(time::Duration::from_millis(5000)),
        };

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

        let out1 = buffer.next(topic_settings);
        let out2 = buffer.next(topic_settings);

        assert_eq!(out1, Some(message1));
        assert_eq!(out2, None); // Второе сообщение является дубликатом
    }

    #[test]
    fn test_do_not_dedup_if_too_much_time_passed() {
        let mut buffer = MessagesBuffer::new();
        let topic_settings = topic_registry::TopicSettings {
            retention_ttl: None,
            dedup_ttl: Some(time::Duration::from_millis(1)),
        };

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

        let out1 = buffer.next(topic_settings);
        // Подождем перед отправкой следующего сообщения, чтобы обновить таблицу дедупликации
        sleep(Duration::from_millis(500));
        let out2 = buffer.next(topic_settings);

        assert_eq!(out1, Some(message1));
        assert_eq!(out2, Some(message2)); // Второе сообщение не дулбикат, прошло много времени
    }

    #[test]
    fn test_dedup_works_with_different_keys() {
        let mut buffer = MessagesBuffer::new();
        let topic_settings = topic_registry::TopicSettings {
            retention_ttl: None,
            dedup_ttl: Some(time::Duration::from_millis(5000)),
        };

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

        let out1 = buffer.next(topic_settings);
        let out2 = buffer.next(topic_settings);

        assert_eq!(out1, Some(message1));
        assert_eq!(out2, Some(message2)); // Второе сообщение прошло, потому что другой ключ
    }
}
