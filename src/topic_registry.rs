use std::collections::HashMap;
use std::time;

type TopicName = String;

pub struct TopicRegistry {
    topics: HashMap<TopicName, TopicSettings>,
}

#[derive(Clone, Copy, Debug)]
pub struct TopicSettings {
    pub retention_ttl: Option<time::Duration>,
    pub dedup_ttl: Option<time::Duration>,
}
