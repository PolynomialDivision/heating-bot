//! Turning MQTT payloads and HTTP bodies from Home Assistant into chat
//! messages.
//!
//! Accepted payloads:
//!   * JSON `{"message": "...", "title": "..."}` (`title` optional)
//!   * any other UTF-8 text, posted as-is (e.g. a plain sentence or `21.5`)
//!
//! The text goes through `mxbot_common::format::mentionify`, so `**bold**`,
//! `[label](url)`, line breaks and `@user:server` mentions work.

use serde::Deserialize;

#[derive(Deserialize)]
struct Notification {
    message: String,
    #[serde(default)]
    title: Option<String>,
}

/// The chat text for one message, or `None` if there is nothing to post
/// (empty or not UTF-8). `prefix` (the MQTT topic with `show_topic`) is put
/// in front of the text.
pub fn render(payload: &[u8], prefix: Option<&str>) -> Option<String> {
    let raw = std::str::from_utf8(payload).ok()?.trim();
    if raw.is_empty() {
        return None;
    }
    let text = match serde_json::from_str::<Notification>(raw) {
        Ok(notification) => {
            let message = notification.message.trim();
            match notification.title.as_deref().map(str::trim) {
                Some(title) if !title.is_empty() && !message.is_empty() => {
                    format!("**{title}**\n{message}")
                }
                Some(title) if !title.is_empty() => format!("**{title}**"),
                _ if message.is_empty() => return None,
                _ => message.to_owned(),
            }
        }
        Err(_) => raw.to_owned(),
    };
    Some(match prefix {
        Some(prefix) => format!("{prefix}: {text}"),
        None => text,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_notifications_use_title_and_message() {
        assert_eq!(
            render(
                r#"{"title": "Heizung", "message": "Wohnzimmer unter 18 °C"}"#.as_bytes(),
                None
            )
            .as_deref(),
            Some("**Heizung**\nWohnzimmer unter 18 °C")
        );
        assert_eq!(
            render(br#"{"message": " Nur Text "}"#, None).as_deref(),
            Some("Nur Text")
        );
    }

    #[test]
    fn plain_payloads_are_posted_verbatim() {
        assert_eq!(render(b"Boiler an\n", None).as_deref(), Some("Boiler an"));
        assert_eq!(render(b"21.5", None).as_deref(), Some("21.5"));
        // JSON that isn't a notification is still just text.
        assert_eq!(
            render(br#"{"temp": 21.5}"#, None).as_deref(),
            Some(r#"{"temp": 21.5}"#)
        );
    }

    #[test]
    fn empty_and_binary_payloads_are_skipped() {
        assert_eq!(render(b"   ", None), None);
        assert_eq!(render(br#"{"message": ""}"#, None), None);
        assert_eq!(render(&[0xff, 0xfe], None), None);
    }

    #[test]
    fn topic_prefix_is_optional() {
        assert_eq!(
            render(b"an", Some("heating/boiler")).as_deref(),
            Some("heating/boiler: an")
        );
    }
}
