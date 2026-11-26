//! One message per place: a notification about a place (the 2nd floor
//! bathroom) replaces the one before about the same place, so the chat shows
//! how each place is now instead of its history. The replacement is a new
//! message (everyone gets notified) and the old one is deleted. One that says
//! it is resolved ("now closed") is deleted by itself after a while.
//!
//! The place is read from the text: the first capture group of
//! `[heating.replace] key_pattern`. Text that doesn't match is posted as
//! before and left alone. Which message shows which place is kept in
//! `store/heating-messages.json`, so a restart doesn't leave old ones behind.

use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::config::ReplaceConfig;

/// How texts are told apart: compiled from `[heating.replace]`.
pub struct Rules {
    key: Regex,
    resolved: Regex,
    resolved_after: Option<Duration>,
}

impl Rules {
    /// `None` when replacing is switched off.
    pub fn from_config(config: &ReplaceConfig) -> Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }
        let key = Regex::new(&config.key_pattern)
            .with_context(|| format!("[heating.replace] key_pattern {:?}", config.key_pattern))?;
        if key.captures_len() < 2 {
            bail!("[heating.replace] key_pattern needs a capture group: (…) around the place");
        }
        let resolved = Regex::new(&config.resolved_pattern).with_context(|| {
            format!(
                "[heating.replace] resolved_pattern {:?}",
                config.resolved_pattern
            )
        })?;
        Ok(Some(Self {
            key,
            resolved,
            resolved_after: (config.resolved_minutes > 0)
                .then(|| Duration::from_secs(config.resolved_minutes * 60)),
        }))
    }

    /// The place `text` is about ("2nd floor bathroom"), or `None` if it
    /// isn't about one.
    pub fn key(&self, text: &str) -> Option<String> {
        let place = self.key.captures(text)?.get(1)?.as_str();
        let place = place.split_whitespace().collect::<Vec<_>>().join(" ");
        (!place.is_empty()).then(|| place.to_lowercase())
    }

    /// When a message with `text`, posted at `now`, is to be deleted by
    /// itself: only one saying the place is fine again.
    pub fn expires_at(&self, text: &str, now: SystemTime) -> Option<u64> {
        let after = self.resolved_after?;
        self.resolved
            .is_match(text)
            .then(|| unix(now) + after.as_secs())
    }
}

/// The message showing a place right now.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Shown {
    pub event_id: String,
    /// Unix seconds when it is deleted by itself; `None`: until replaced.
    #[serde(default)]
    pub expires_at: Option<u64>,
}

/// Which message shows which place.
#[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
pub struct Slots {
    #[serde(default)]
    places: BTreeMap<String, Shown>,
}

impl Slots {
    /// `shown` now shows `place`; the message it replaces, if any.
    pub fn show(&mut self, place: String, shown: Shown) -> Option<String> {
        self.places.insert(place, shown).map(|old| old.event_id)
    }

    /// Messages whose time ran out by `now`: forgotten and returned.
    pub fn take_expired(&mut self, now: SystemTime) -> Vec<String> {
        let now = unix(now);
        let expired: Vec<String> = self
            .places
            .iter()
            .filter(|(_, shown)| shown.expires_at.is_some_and(|at| at <= now))
            .map(|(place, _)| place.clone())
            .collect();
        expired
            .into_iter()
            .filter_map(|place| self.places.remove(&place))
            .map(|shown| shown.event_id)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.places.len()
    }
}

fn unix(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPEN: &str =
        "🥶 In the 2nd floor bathroom a window or door is open. Can someone please close it? 🥶";
    const CLOSED: &str =
        "👍 In the 2nd floor bathroom the windows and door are now closed. Thank you! 👍";

    fn rules() -> Rules {
        Rules::from_config(&ReplaceConfig::default())
            .unwrap()
            .unwrap()
    }

    #[test]
    fn open_and_closed_are_about_the_same_place_and_floors_apart() {
        let rules = rules();
        assert_eq!(rules.key(OPEN).as_deref(), Some("2nd floor bathroom"));
        assert_eq!(rules.key(CLOSED).as_deref(), Some("2nd floor bathroom"));
        assert_eq!(
            rules.key(&OPEN.replace("2nd", "3rd")).as_deref(),
            Some("3rd floor bathroom")
        );
        // Anything else is posted as before.
        assert_eq!(rules.key("Heizung wurde eingeschaltet."), None);
    }

    #[test]
    fn only_the_all_clear_goes_by_itself() {
        let rules = rules();
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        assert_eq!(rules.expires_at(OPEN, now), None);
        assert_eq!(rules.expires_at(CLOSED, now), Some(1_000 + 30 * 60));
        let keep = Rules::from_config(&ReplaceConfig {
            resolved_minutes: 0,
            ..ReplaceConfig::default()
        })
        .unwrap()
        .unwrap();
        assert_eq!(keep.expires_at(CLOSED, now), None);
    }

    #[test]
    fn each_place_keeps_one_message_and_the_all_clear_expires() {
        let mut slots = Slots::default();
        let shown = |id: &str, expires_at| Shown {
            event_id: id.into(),
            expires_at,
        };
        assert_eq!(slots.show("2nd".into(), shown("$a", None)), None);
        assert_eq!(slots.show("3rd".into(), shown("$b", None)), None);
        // Open again: the reminder replaces the first one.
        assert_eq!(
            slots.show("2nd".into(), shown("$c", None)).as_deref(),
            Some("$a")
        );
        // Closed: replaces the open one, gone after its time.
        assert_eq!(
            slots.show("2nd".into(), shown("$d", Some(100))).as_deref(),
            Some("$c")
        );
        let at = |secs| UNIX_EPOCH + Duration::from_secs(secs);
        assert!(slots.take_expired(at(99)).is_empty());
        assert_eq!(slots.take_expired(at(100)), ["$d"]);
        assert_eq!(slots.len(), 1);
        // What is kept reads back after a restart.
        let stored = serde_json::to_string(&slots).unwrap();
        assert_eq!(serde_json::from_str::<Slots>(&stored).unwrap(), slots);
    }

    #[test]
    fn a_pattern_without_a_group_is_refused() {
        let config = ReplaceConfig {
            key_pattern: "bathroom".into(),
            ..ReplaceConfig::default()
        };
        assert!(Rules::from_config(&config).is_err());
        let off = ReplaceConfig {
            enabled: false,
            ..ReplaceConfig::default()
        };
        assert!(Rules::from_config(&off).unwrap().is_none());
    }
}
