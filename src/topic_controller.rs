use crate::protocol::ZaichikFrame;
use crate::subscription_manager::Message;
use im;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time;
use tokio::stream::{self, StreamExt};
use tokio::sync::broadcast;

pub type TopicName = String;

#[derive(Debug)]
pub struct TopicRegistry {
    pub topics: HashMap<TopicName, TopicController>,
}

impl TopicRegistry {
    pub fn new() -> TopicRegistry {
        TopicRegistry {
            topics: HashMap::new(),
        }
    }

    pub fn add_topic(&mut self, topic: TopicName, retention_ttl: u64, dedup_ttl: u64) {
        let topic_controller =
            TopicController::new(topic.clone(), retention_ttl, dedup_ttl, 10_000);
        self.topics.insert(topic, topic_controller);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TopicSettings {
    pub retention_ttl: Option<time::Duration>,
    pub dedup_ttl: Option<time::Duration>,
}

impl TopicSettings {
    pub fn new(retention_ttl: u64, dedup_ttl: u64) -> TopicSettings {
        let retention_ttl = if retention_ttl == 0 {
            None
        } else {
            Some(time::Duration::from_millis(retention_ttl))
        };
        let dedup_ttl = if dedup_ttl == 0 {
            None
        } else {
            Some(time::Duration::from_millis(dedup_ttl))
        };

        TopicSettings {
            retention_ttl,
            dedup_ttl,
        }
    }
}

impl TopicRegistry {
    // fn dispatch(&self, f: ZaichikFrame) {
    //     // Получаем фрейм, нам нужно понять в какой топик его отправить
    //     // потом понять в какой
    //     let topic = "name".to_string(); // f.topic
    //     // У нас может быть зарегистрирован такой топик, а может и не быть.
    //
    //     // todo создадим топик, если его нет, да?
    //     let top = self.t.get(&topic);
    //
    //     // todo нужно проверить компкашн по табличке компакшна здесь
    //
    //     // todo
    //
    //     // Записываем сообщение в броадкаст канал.
    //     top.unwrap().add_message()
    // }
}

#[derive(Debug)]
pub struct TopicController {
    // Ключ сообщения, время, когда получили сообщение.
    name: TopicName,                                    // Clone
    broadcast_sender: broadcast::Sender<Message>,       // Clone?
    settings: TopicSettings,                            // Copy + Clone
    compaction_map: im::HashMap<String, time::Instant>, // Придется обезопасить, можно персистентную мапу
    receivers: broadcast::Receiver<Message>,            // Нельзя клонить
    retained_buffer: im::Vector<Message>, // Персистентный вектор, хорош для push и pop
}

impl TopicController {
    pub fn new(
        name: TopicName,
        retention_ttl: u64,
        compaction_window: u64,
        buffer_size: u32,
    ) -> TopicController {
        let settings = TopicSettings::new(retention_ttl, compaction_window);
        let (broadcast_sender, receivers) = broadcast::channel(buffer_size as usize);
        let compaction_map = im::HashMap::new();
        let retained_buffer = im::Vector::new();

        // Делаем канал
        TopicController {
            name,
            broadcast_sender,
            settings,
            compaction_map,
            receivers,
            retained_buffer,
        }
    }

    pub async fn publish(&mut self, message: Message) {
        // dedup
        // set expiration
        // send_to_topic
        match self.broadcast_sender.send(message.clone()) {
            Ok(count_subscribers) => count_subscribers - 1,
            Err(_) => 0,
        };

        // Пройдемся по буфферу и оставим только те элементы, которые
        // все еще не истекли по времени.
        self.retained_buffer
            .retain(|message| message.expires_at > time::Instant::now());

        // clean compaction_map

        self.retained_buffer.push_back(message);

        // Смещаем курсор на локальном слушателе. Игнорируем ошибку.
        let _ = self.receivers.recv().await;
    }
    // broadcast::Receiver<Message>
    pub fn subscribe(&self) -> impl tokio::stream::Stream {
        // todo спереди нужно приклеить retained buffer
        let retained_messages = self
            .retained_buffer
            .iter()
            .map(|message| Ok(message.clone()))
            .collect::<Vec<_>>();
        let retained_stream = stream::iter(retained_messages);
        let subscription_stream = self.broadcast_sender.subscribe().into_stream();
        retained_stream.chain(subscription_stream)
    }
}
