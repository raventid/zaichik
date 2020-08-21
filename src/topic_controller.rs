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
    pub topics: HashMap<TopicName, RwLock<TopicController>>,
}

impl TopicRegistry {
    pub fn new() -> TopicRegistry {
        TopicRegistry {
            topics: HashMap::new(),
        }
    }

    pub fn create_topic(&mut self, topic: TopicName, retention_ttl: u64, dedup_ttl: u64) -> Option<&RwLock<TopicController>> {
        let topic_controller =
            RwLock::new(TopicController::new(topic.clone(), retention_ttl, dedup_ttl, 10_000));

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
    name: TopicName,                                    // Clone / Sync
    broadcast_sender: broadcast::Sender<Message>,       // Clone? / Sync?
    settings: TopicSettings,                            // Copy + Clone / Sync?
    compaction_map: im::HashMap<String, time::Instant>, // Sync
    receivers: broadcast::Receiver<Message>,            // Нельзя клонить Sync
    retained_buffer: im::Vector<Message>, // Sync
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

    pub fn publish(&mut self, message: Message) {
        // dedup
        // set expiration
        // send_to_topic
        match self.broadcast_sender.send(message.clone()) {
            Ok(count_subscribers) => debug!("[TopicController:{}] Sent to {} subscribers", self.name, count_subscribers - 1),
            Err(_) => debug!("[TopicController:{}] Failed to send to {} subscribers", self.name, 0),
        };

        // Пройдемся по буфферу и оставим только те элементы, которые
        // все еще не истекли по времени.
        self.retained_buffer
            .retain(|message| message.expires_at > time::Instant::now());

        // clean compaction_map

        self.retained_buffer.push_back(message);

        // Смещаем курсор на локальном слушателе. Игнорируем ошибку.
        // todo убрать в отдельный tokio::spawn, чтобы снять ограничение на многопоточность
        // let _ = self.receivers.recv().await;
    }
    // broadcast::Receiver<Message>
    // Объединяем retained сообщнения и канал куда будут поступать сообщения
    // наш брокер гарантирует порядок доставки сообщений в рамках одного топика.
    pub fn subscribe(&self) -> impl tokio::stream::Stream<Item = Result<Message, tokio::sync::broadcast::RecvError>> {
        // todo спереди нужно приклеить retained buffer
        let retained_messages = self
            .retained_buffer
            .iter().map(|message| Ok(message.clone())).collect::<Vec<_>>();

        dbg!(self
            .retained_buffer.clone());
        // let retained_stream = stream::iter(retained_messages);
        let subscription = self.broadcast_sender.subscribe().into_stream();

        stream::iter(retained_messages).chain(subscription)

        // (retained_messages, subscription.into_stream())
        // retained_stream.chain(subscription_stream)
    }
}

// struct TopicStreamMap {
//     retained: Vec<("", VecDeque<Message>)>,
//
// }