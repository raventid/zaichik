use std::collections::HashMap;
use std::sync::RwLock;

use crate::topic_controller::TopicController;

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
