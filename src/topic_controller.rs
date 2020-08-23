use std::collections::HashMap;
use std::ops::Add;
use std::time;
use tokio::stream::{self, StreamExt};
use tokio::sync::broadcast;

use crate::topic_registry::TopicName;

// Сообщение в том в виде, в котором оно хранится в топике.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub key: Option<String>,
    pub payload: Vec<u8>,
    received_at: time::Instant,
    pub expires_at: Option<time::Instant>,
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
        let buffer_size = if buffer_size == 0 { 1000 } else { buffer_size } as usize;

        TopicSettings {
            retention_ttl,
            compaction_window,
            buffer_size,
        }
    }
}

// Компонент управляющий топиком.
// Осуществляет запись в топик, контроль retention и compaction, выдает подписки.
#[derive(Debug)]
pub struct TopicController {
    name: TopicName,
    broadcast_sender: broadcast::Sender<Message>,
    settings: TopicSettings,
    compaction_map: HashMap<String, time::Instant>,
    retained_buffer: Vec<Message>,
}

impl TopicController {
    pub fn new(
        name: TopicName,
        retention_ttl: u64,
        compaction_window: u64,
        buffer_size: u32,
    ) -> TopicController {
        let settings = TopicSettings::new(retention_ttl, compaction_window, buffer_size as usize);
        let (broadcast_sender, _) = broadcast::channel(settings.buffer_size);
        let compaction_map = HashMap::new();
        let retained_buffer = Vec::new();

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
            Self::check_duplicate_and_update_compaction_map(
                &message,
                &mut self.compaction_map,
                compaction_window,
            )
        } else {
            false
        };

        if !is_duplicate {
            // Отправляем сообщение в броадкаст, его прочитают, если у нас есть
            // подписчики.
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

            // Если мы поддерживаем retention, то сохраним сообщение
            // в локальный буффер для таких сообщений.
            if self.settings.retention_ttl.is_some() {
                self.retained_buffer.push(message);
            }
        }

        // Пройдемся по буфферу и оставим только те элементы, которые
        // все еще не истекли по времени. Такую работу не очень хорошо
        // делать на каждом publish сообщения, но мы позволим себе этот
        // ход для упрощения. Также здесь мы добавим текущее сообщение в
        // retention_buffer.
        self.clean_outdated_retained_messages();

        // Также, если мы используем compaction для топика, то мы не хотели
        // бы бесконечно увеличивать размер хэшмапы. Мы наивно будет удалять
        // ключи, которые уже точно устарели, что не является очень
        // эффективной и удачной операцией в publish, но в целях упрощения мы оставим
        // этот код здесь.
        // Можно было выполнять эту операцию раз в 1000 паблишей, но мы также не будем
        // усложнять приведенный код.
        self.clean_outdated_compaction_keys();
    }

    fn clean_outdated_compaction_keys(&mut self) {
        if self.settings.compaction_window.is_some() {
            let outdated_keys = self
                .compaction_map
                .iter()
                .filter(|(_key, time)| {
                    time.add(self.settings.compaction_window.unwrap()) < time::Instant::now()
                })
                .map(|(key, _val)| key.to_string())
                .collect::<Vec<_>>();

            for key in outdated_keys {
                self.compaction_map.remove(&key);
            }
        }
    }

    fn clean_outdated_retained_messages(&mut self) {
        if self.settings.retention_ttl.is_some() {
            self.retained_buffer
                .retain(|message| message.expires_at.unwrap() > time::Instant::now());
        }
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

    fn check_duplicate_and_update_compaction_map(
        message: &Message,
        compaction_map: &mut HashMap<String, time::Instant>,
        compaction_window: time::Duration,
    ) -> bool {
        // Если у сообщения нет ключа для compaction, то мы ничего не будет предпринимать.
        if message.key.is_none() {
            return false;
        }

        let now = time::Instant::now();
        let key = message.key.as_ref().unwrap();

        match compaction_map.get(key) {
            Some(last_sent_at) => {
                let since_last_seen = now.duration_since(*last_sent_at);
                if since_last_seen < compaction_window {
                    // Если мы отравляли сообщение не так давно,
                    // то скажем, что текущее сообщение дубликат.
                    true
                } else {
                    // Здесь мы видим, что можем повторить отправку,
                    // сообщение ушло давно.
                    compaction_map.insert(key.to_string(), now);
                    false
                }
            }
            None => {
                // Мы еще не встречали такого сообщения,
                // отправим его и пометим, что оно ушло сейчас.
                compaction_map.insert(key.to_string(), now);
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cleaning_compaction_map() {
        let mut topic_controller_with_small_compaction_window =
            TopicController::new("test".to_string(), 0, 1, 0);

        let mut topic_controller_with_large_compaction_window =
            TopicController::new("test1".to_string(), 0, 10_000, 0);

        let in_past = time::Instant::now()
            .checked_sub(time::Duration::from_millis(5000))
            .unwrap();

        let message1 = Message {
            key: Some("same".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
            expires_at: None,
        };

        TopicController::check_duplicate_and_update_compaction_map(
            &message1,
            &mut topic_controller_with_small_compaction_window.compaction_map,
            topic_controller_with_small_compaction_window
                .settings
                .compaction_window
                .unwrap(),
        );

        TopicController::check_duplicate_and_update_compaction_map(
            &message1,
            &mut topic_controller_with_large_compaction_window.compaction_map,
            topic_controller_with_large_compaction_window
                .settings
                .compaction_window
                .unwrap(),
        );

        std::thread::sleep(time::Duration::from_millis(100));

        topic_controller_with_small_compaction_window.clean_outdated_compaction_keys();
        topic_controller_with_large_compaction_window.clean_outdated_compaction_keys();

        assert!(topic_controller_with_small_compaction_window
            .compaction_map
            .is_empty());

        assert!(!topic_controller_with_large_compaction_window
            .compaction_map
            .is_empty())
    }

    #[test]
    fn test_dedup_works() {
        let mut compaction_map = HashMap::new();
        let compaction_window = time::Duration::from_millis(5000);

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
            key: Some("same".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
            expires_at: None,
        };

        assert!(!TopicController::check_duplicate_and_update_compaction_map(
            &message1,
            &mut compaction_map,
            compaction_window
        ));
        assert!(TopicController::check_duplicate_and_update_compaction_map(
            &message2,
            &mut compaction_map,
            compaction_window
        )); // Второе сообщение является дубликатом
    }

    #[test]
    fn test_do_not_dedup_if_too_much_time_passed() {
        let mut compaction_map = HashMap::new();
        let compaction_window = time::Duration::from_millis(1);

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
            key: Some("same".to_string()),
            payload: vec![1, 2, 3, 4],
            received_at: in_past,
            expires_at: None,
        };

        assert!(!TopicController::check_duplicate_and_update_compaction_map(
            &message1,
            &mut compaction_map,
            compaction_window
        ));

        std::thread::sleep(time::Duration::from_millis(100));

        assert!(!TopicController::check_duplicate_and_update_compaction_map(
            &message2,
            &mut compaction_map,
            compaction_window
        )); // Второе сообщение не дубликат, прошло много времени
    }

    #[test]
    fn test_dedup_works_with_different_keys() {
        let mut compaction_map = HashMap::new();
        let compaction_window = time::Duration::from_millis(5000);

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

        assert!(!TopicController::check_duplicate_and_update_compaction_map(
            &message1,
            &mut compaction_map,
            compaction_window
        ));
        assert!(!TopicController::check_duplicate_and_update_compaction_map(
            &message2,
            &mut compaction_map,
            compaction_window
        )); // Второе сообщение прошло, потому что другой ключ
    }
}
