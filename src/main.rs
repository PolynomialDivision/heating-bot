//! heating-bot: Home Assistant → MQTT (built-in broker) or HTTP POST →
//! (encrypted) Matrix group chat.

mod broker;
mod config;
mod http;
mod intake;
mod message;
mod slots;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use anyhow::{bail, Result};
use mxbot_common::{
    admin::Dispatch,
    format::mentionify,
    matrix_sdk::{
        deserialized_responses::EncryptionInfo,
        ruma::{
            events::room::message::{
                MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent,
            },
            OwnedEventId, OwnedRoomId,
        },
        Room, RoomState,
    },
    send::send_with_retry,
    supervise::{report, supervise, Alive},
    Bot,
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use config::{Config, HeatingConfig};
use intake::{Incoming, SharedStatus, Source, Status};
use slots::{Rules, Shown, Slots};

/// How often messages whose time ran out are looked for.
const EXPIRY_CHECK: Duration = Duration::from_secs(60);

const ADMIN_HELP: &str = "🔥 !heating status: broker, endpoint and counters";

#[tokio::main]
async fn main() -> Result<()> {
    mxbot_common::logging::init("heating_bot");

    let mut config: Config =
        mxbot_common::config::load_toml(&mxbot_common::config::config_path_from_args())?;
    if config.mqtt.enabled {
        config.mqtt.apply_env_overrides();
        config.mqtt.validate()?;
    }
    if config.http.enabled {
        config.http.apply_env_overrides();
        config.http.validate()?;
    }
    if !config.mqtt.enabled && !config.http.enabled {
        bail!("Both [mqtt] and [http] are disabled. Enable at least one");
    }
    let room_id = mxbot_common::rooms::parse_room_id("[heating] room_id", &config.heating.room_id)?;
    let rules = Rules::from_config(&config.heating.replace)?;
    let slots_path = mxbot_common::config::store_path_from_env().join("heating-messages.json");
    let slots: Slots = mxbot_common::persist::load_json_or_default(&slots_path).await?;

    let bot = Bot::builder("heating-bot", env!("CARGO_PKG_VERSION"))
        .admin_help(ADMIN_HELP)
        .start(&config.matrix, &config.security)
        .await?;

    let status: SharedStatus = Arc::new(Mutex::new(Status {
        places_shown: rules.as_ref().map(|_| slots.len()),
        ..Status::default()
    }));
    install_message_handler(&bot, room_id.clone(), Arc::clone(&status));

    info!("Starting Matrix sync...");
    bot.initial_sync().await;
    if bot.client.get_room(&room_id).map(|room| room.state()) != Some(RoomState::Joined) {
        warn!(%room_id, "The bot is not in the configured room yet: invite it there");
    }

    // Intake only starts after the first sync, so the room is known.
    let (tx, rx) = mpsc::channel(100);
    if config.mqtt.enabled {
        broker::start(&config.mqtt, tx.clone(), Arc::clone(&status))?;
    }
    if config.http.enabled {
        http::start(&config.http, tx, Arc::clone(&status)).await?;
    }
    let forwarder = Forwarder {
        bot: bot.clone(),
        room_id,
        heating: config.heating,
        status,
        rules,
        slots,
        slots_path,
        // For the status file: each message posted or failed.
        posts: report("message posts"),
    };
    supervise("message forwarder", forwarder.run(rx));

    bot.run().await
}

/// Posts every MQTT and HTTP message into the configured room, keeping one
/// message per place (see [`slots`]).
struct Forwarder {
    bot: Bot,
    room_id: OwnedRoomId,
    heating: HeatingConfig,
    status: SharedStatus,
    rules: Option<Rules>,
    slots: Slots,
    slots_path: PathBuf,
    posts: Alive,
}

impl Forwarder {
    async fn run(mut self, mut rx: mpsc::Receiver<Incoming>) {
        let mut expiry = tokio::time::interval(EXPIRY_CHECK);
        loop {
            tokio::select! {
                incoming = rx.recv() => match incoming {
                    Some(incoming) => self.forward(incoming).await,
                    None => return,
                },
                _ = expiry.tick() => self.expire().await,
            }
        }
    }

    fn room(&self) -> Option<Room> {
        self.bot
            .client
            .get_room(&self.room_id)
            .filter(|room| room.state() == RoomState::Joined)
    }

    async fn forward(&mut self, incoming: Incoming) {
        let source = &incoming.source;
        let prefix = match source {
            Source::Mqtt(topic) if self.heating.show_topic => Some(topic.as_str()),
            _ => None,
        };
        let Some(text) = message::render(&incoming.payload, prefix) else {
            warn!(%source, "Skipping empty or non-UTF-8 message");
            return;
        };
        let posted = match self.room() {
            Some(room) => match send_with_retry(&room, mentionify(&text)).await {
                Ok(event_id) => {
                    info!(%source, %event_id, "Posted message");
                    self.posts.ok();
                    self.replace(&room, &text, &event_id).await;
                    true
                }
                Err(e) => {
                    error!(%source, "Failed to post message: {e}");
                    self.posts.failed(format!("posting a message failed: {e}"));
                    false
                }
            },
            None => {
                error!(%source, room_id = %self.room_id, "Not joined to the configured room: message dropped");
                self.posts.failed("not joined to the configured room");
                false
            }
        };
        let mut status = self.status.lock().expect("status lock poisoned");
        if posted {
            status.posted += 1;
        } else {
            status.failed += 1;
        }
    }

    /// `event_id` (just posted, `text`) now shows its place: the message
    /// before about it goes. Posted first, deleted second: if posting
    /// fails, the old message stays.
    async fn replace(&mut self, room: &Room, text: &str, event_id: &OwnedEventId) {
        let Some(rules) = &self.rules else {
            return;
        };
        let Some(place) = rules.key(text) else {
            return;
        };
        let shown = Shown {
            event_id: event_id.to_string(),
            expires_at: rules.expires_at(text, SystemTime::now()),
        };
        if let Some(old) = self.slots.show(place, shown) {
            delete(room, &old).await;
        }
        self.save().await;
    }

    /// Delete the all-clear messages whose time ran out.
    async fn expire(&mut self) {
        let expired = self.slots.take_expired(SystemTime::now());
        if expired.is_empty() {
            return;
        }
        if let Some(room) = self.room() {
            for event_id in &expired {
                delete(&room, event_id).await;
            }
        }
        self.save().await;
    }

    async fn save(&self) {
        if let Err(e) = mxbot_common::persist::save_json_atomic(&self.slots_path, &self.slots).await
        {
            error!("Could not save which message shows which place: {e:#}");
        }
        self.status
            .lock()
            .expect("status lock poisoned")
            .places_shown = Some(self.slots.len());
    }
}

/// Delete one of the bot's messages; one that is gone already is fine.
async fn delete(room: &Room, event_id: &str) {
    let Ok(event_id) = OwnedEventId::try_from(event_id) else {
        return;
    };
    match room.redact(&event_id, None, None).await {
        Ok(_) => info!(%event_id, "Deleted the message it replaces"),
        Err(e) => warn!(%event_id, "Could not delete an old message: {e}"),
    }
}

fn install_message_handler(bot: &Bot, room_id: OwnedRoomId, status: SharedStatus) {
    let bot = bot.clone();
    bot.client.clone().add_event_handler(
        move |ev: OriginalSyncRoomMessageEvent, room: Room, encryption: Option<EncryptionInfo>| {
            let bot = bot.clone();
            let room_id = room_id.clone();
            let status = Arc::clone(&status);
            async move {
                if ev.sender == bot.user_id || room.state() != RoomState::Joined {
                    return;
                }
                let dispatch = bot.admin.handle(&room, &ev, encryption.as_ref()).await;
                if dispatch == Dispatch::Handled {
                    return;
                }
                // `!heating status` works in the notification room and in
                // admin direct chats.
                if dispatch != Dispatch::AdminDm && room.room_id() != room_id {
                    return;
                }
                let MessageType::Text(text) = &ev.content.msgtype else {
                    return;
                };
                if text.body.trim() == "!heating status" {
                    let reply = status_text(&status.lock().expect("status lock poisoned"));
                    room.send(RoomMessageEventContent::notice_plain(reply))
                        .await
                        .ok();
                }
            }
        },
    );
}

fn status_text(status: &Status) -> String {
    let last = status
        .last_message_at
        .and_then(|at| SystemTime::now().duration_since(at).ok())
        .map(|ago| format!("{} min ago", ago.as_secs() / 60))
        .unwrap_or_else(|| "never".to_owned());
    let listening = |addr: Option<std::net::SocketAddr>, off: &str| {
        addr.map(|addr| format!("listening on {addr}"))
            .unwrap_or_else(|| off.to_owned())
    };
    let broker = listening(status.mqtt_listen, "disabled");
    let http = listening(status.http_listen, "disabled");
    format!(
        "🔥 heating-bot\n📡 MQTT broker: {broker}\n🌐 HTTP endpoint: {http}\n📨 Messages: {} received, {} posted, {} failed\n🕒 Last message: {last}\n🧹 {}",
        status.received,
        status.posted,
        status.failed,
        match status.places_shown {
            Some(n) => format!("One message per place: {n} in the chat"),
            None => "One message per place: off".to_owned(),
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_shows_broker_and_counters() {
        let text = status_text(&Status::default());
        assert!(text.contains("MQTT broker: disabled"));
        assert!(text.contains("HTTP endpoint: disabled"));
        assert!(text.contains("Last message: never"));

        let status = Status {
            mqtt_listen: Some("0.0.0.0:1883".parse().unwrap()),
            http_listen: Some("0.0.0.0:8765".parse().unwrap()),
            received: 3,
            posted: 2,
            failed: 1,
            last_message_at: Some(SystemTime::now()),
            places_shown: Some(2),
        };
        let text = status_text(&status);
        assert!(text.contains("listening on 0.0.0.0:1883"));
        assert!(text.contains("HTTP endpoint: listening on 0.0.0.0:8765"));
        assert!(text.contains("3 received, 2 posted, 1 failed"));
        assert!(text.contains("0 min ago"));
        assert!(text.contains("One message per place: 2 in the chat"));
    }
}
