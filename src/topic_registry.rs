use std::collections::HashMap;
use std::time;

pub type TopicName = String;

#[derive(Clone, Debug)]
pub struct TopicRegistry {
    pub topics: HashMap<TopicName, TopicSettings>,
}

impl TopicRegistry {
    pub fn new() -> TopicRegistry {
        TopicRegistry {
            topics: HashMap::new(),
        }
    }

    pub fn add_topic(&mut self, topic: String, retention_ttl: u64, dedup_ttl: u64) {
        let settings = TopicSettings::new(retention_ttl, dedup_ttl);
        self.topics.insert(topic, settings);
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
