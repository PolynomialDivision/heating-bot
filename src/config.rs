use std::net::SocketAddr;

use anyhow::{bail, Result};
pub use mxbot_common::config::{MatrixConfig, SecurityConfig};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub matrix: MatrixConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub http: HttpConfig,
    pub heating: HeatingConfig,
}

/// The built-in MQTT broker Home Assistant can connect to: `[mqtt]`.
/// Off unless `enabled = true`.
#[derive(Deserialize, Clone)]
pub struct MqttConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Address the broker listens on (MQTT 3.1.1, plain TCP).
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    /// Credentials Home Assistant must use; required when enabled.
    #[serde(default)]
    pub username: String,
    /// May instead be set via `MQTT_PASSWORD`.
    #[serde(default)]
    pub password: String,
    /// Topic filters whose messages are posted (`+` / `#` wildcards allowed).
    #[serde(default = "default_topics")]
    pub topics: Vec<String>,
    /// Largest accepted message, in bytes.
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: usize,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: default_listen(),
            username: String::new(),
            password: String::new(),
            topics: default_topics(),
            max_payload_bytes: default_max_payload_bytes(),
        }
    }
}

impl MqttConfig {
    /// Apply the `MQTT_PASSWORD` environment override.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(password) = std::env::var("MQTT_PASSWORD") {
            if !password.is_empty() {
                self.password = password;
            }
        }
    }

    /// The broker must never run open: an MQTT broker without credentials
    /// accepts every client.
    pub fn validate(&self) -> Result<()> {
        if self.username.trim().is_empty() {
            bail!("[mqtt] username must not be empty");
        }
        if self.password.len() < 12 {
            bail!("[mqtt] password (or MQTT_PASSWORD) must be at least 12 characters");
        }
        if self.password.starts_with("CHANGE-ME") {
            bail!("[mqtt] password is still the placeholder from config.toml.example");
        }
        if self.topics.is_empty() {
            bail!("[mqtt] topics must list at least one topic filter");
        }
        Ok(())
    }
}

/// The HTTP endpoint (`POST /message`) Home Assistant posts to: `[http]`.
/// On unless `enabled = false`.
#[derive(Deserialize, Clone)]
pub struct HttpConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Address the endpoint listens on (plain HTTP).
    #[serde(default = "default_http_listen")]
    pub listen: SocketAddr,
    /// Optional bearer token (`Authorization: Bearer <token>`); empty = no
    /// authentication. May instead be set via `HTTP_TOKEN`.
    #[serde(default)]
    pub token: String,
    /// Largest accepted request body, in bytes.
    #[serde(default = "default_max_payload_bytes")]
    pub max_body_bytes: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen: default_http_listen(),
            token: String::new(),
            max_body_bytes: default_max_payload_bytes(),
        }
    }
}

impl HttpConfig {
    /// Apply the `HTTP_TOKEN` environment override.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(token) = std::env::var("HTTP_TOKEN") {
            if !token.is_empty() {
                self.token = token;
            }
        }
    }

    /// No token is fine (internal network); a token, if set, must be real.
    pub fn validate(&self) -> Result<()> {
        if self.token.is_empty() {
            return Ok(());
        }
        if self.token.len() < 24 {
            bail!("[http] token (or HTTP_TOKEN) must be at least 24 characters");
        }
        if self.token.starts_with("CHANGE-ME") {
            bail!("[http] token is still the placeholder from config.toml.example");
        }
        if self
            .token
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
        {
            bail!("[http] token must not contain whitespace");
        }
        Ok(())
    }
}

/// Where and how notifications are posted: `[heating]`.
#[derive(Deserialize, Clone)]
pub struct HeatingConfig {
    /// Matrix room (group chat) the notifications are posted to.
    pub room_id: String,
    /// Prefix every message with the MQTT topic it came from.
    #[serde(default)]
    pub show_topic: bool,
    #[serde(default)]
    pub replace: ReplaceConfig,
}

/// One message per place: `[heating.replace]`, see [`crate::slots`].
#[derive(Deserialize, Clone, Debug)]
pub struct ReplaceConfig {
    /// A new message about a place deletes the one before about it.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Finds the place in the text: its first capture group.
    #[serde(default = "default_key_pattern")]
    pub key_pattern: String,
    /// A message saying the place is fine again.
    #[serde(default = "default_resolved_pattern")]
    pub resolved_pattern: String,
    /// That one is deleted after this many minutes (0: kept until the next
    /// message about the place).
    #[serde(default = "default_resolved_minutes")]
    pub resolved_minutes: u64,
}

impl Default for ReplaceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            key_pattern: default_key_pattern(),
            resolved_pattern: default_resolved_pattern(),
            resolved_minutes: default_resolved_minutes(),
        }
    }
}

/// "In the 2nd floor bathroom a window …" / "… the windows and door …".
fn default_key_pattern() -> String {
    r"(?i)\bin the (.+?) (?:a|the) (?:window|door)".to_owned()
}

fn default_resolved_pattern() -> String {
    r"(?i)\bnow closed\b".to_owned()
}

fn default_resolved_minutes() -> u64 {
    30
}

fn default_listen() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 1883))
}

fn default_true() -> bool {
    true
}

fn default_http_listen() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 8765))
}

fn default_topics() -> Vec<String> {
    vec!["heating/#".to_owned()]
}

fn default_max_payload_bytes() -> usize {
    64 * 1024
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
[matrix]
homeserver = "https://matrix.org"
user_id = "@heating-bot:matrix.org"
access_token = "token"
device_id = "DEVICE"

[heating]
room_id = "!room:matrix.org"
"#;

    #[test]
    fn minimal_config_means_http_only_without_auth() {
        let config: Config = toml::from_str(MINIMAL).unwrap();
        assert!(config.http.enabled);
        assert_eq!(config.http.listen, "0.0.0.0:8765".parse().unwrap());
        assert!(config.http.token.is_empty());
        assert_eq!(config.http.max_body_bytes, 65536);
        assert!(config.http.validate().is_ok());
        assert!(!config.mqtt.enabled);
        assert_eq!(config.mqtt.listen, "0.0.0.0:1883".parse().unwrap());
        assert_eq!(config.mqtt.topics, ["heating/#"]);
        assert!(!config.heating.show_topic);
        assert!(config.heating.replace.enabled);
        assert_eq!(config.heating.replace.resolved_minutes, 30);
    }

    #[test]
    fn example_config_is_http_only_and_its_mqtt_placeholder_is_rejected() {
        let config: Config = toml::from_str(include_str!("../config.toml.example")).unwrap();
        assert!(config.security.admins().is_ok());
        assert!(config.http.enabled);
        assert!(config.http.validate().is_ok());
        assert!(!config.mqtt.enabled);
        let error = config.mqtt.validate().unwrap_err();
        assert!(error.to_string().contains("placeholder"));
        // What the example documents is what applies without it.
        let replace = config.heating.replace;
        assert_eq!(replace.key_pattern, default_key_pattern());
        assert_eq!(replace.resolved_pattern, default_resolved_pattern());
        assert_eq!(replace.resolved_minutes, default_resolved_minutes());
    }

    #[test]
    fn an_http_token_if_set_must_be_real() {
        let config: Config =
            toml::from_str(&format!("{MINIMAL}\n[http]\ntoken = \"short\"\n")).unwrap();
        let mut http = config.http;
        assert!(http.validate().is_err());
        http.token = "CHANGE-ME-long-random-token-0000".into();
        assert!(http.validate().is_err());
        http.token = "a long token with spaces in it".into();
        assert!(http.validate().is_err());
        http.token = "0123456789abcdef0123456789abcdef".into();
        assert!(http.validate().is_ok());
    }

    #[test]
    fn broker_refuses_to_run_without_real_credentials() {
        let config: Config = toml::from_str(&format!(
            "{MINIMAL}\n[mqtt]\nenabled = true\nusername = \"homeassistant\"\npassword = \"a-long-enough-secret\"\n"
        ))
        .unwrap();
        let mut mqtt = config.mqtt;
        assert!(mqtt.enabled);
        assert!(mqtt.validate().is_ok());
        mqtt.password = "short".into();
        assert!(mqtt.validate().is_err());
        mqtt.password = String::new();
        assert!(mqtt.validate().is_err());
        mqtt.password = "a-long-enough-secret".into();
        mqtt.username = " ".into();
        assert!(mqtt.validate().is_err());
    }
}
