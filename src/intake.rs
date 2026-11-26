//! What the MQTT broker and the HTTP endpoint hand to the Matrix forwarder.

use std::{
    fmt,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::SystemTime,
};

/// Where a message came from.
pub enum Source {
    /// Published by an MQTT client on this topic.
    Mqtt(String),
    /// Posted to the HTTP endpoint.
    Http,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Mqtt(topic) => write!(f, "mqtt:{topic}"),
            Source::Http => f.write_str("http"),
        }
    }
}

/// One received message, not yet rendered.
pub struct Incoming {
    pub source: Source,
    pub payload: Vec<u8>,
}

/// Counters shown by `!heating status`.
#[derive(Default)]
pub struct Status {
    pub mqtt_listen: Option<SocketAddr>,
    pub http_listen: Option<SocketAddr>,
    pub received: u64,
    pub posted: u64,
    pub failed: u64,
    pub last_message_at: Option<SystemTime>,
    /// Places with a message in the chat right now (`None`: replacing off).
    pub places_shown: Option<usize>,
}

impl Status {
    pub fn record_received(&mut self) {
        self.received += 1;
        self.last_message_at = Some(SystemTime::now());
    }
}

pub type SharedStatus = Arc<Mutex<Status>>;
