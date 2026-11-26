//! Built-in MQTT broker (rumqttd) that Home Assistant connects to.
//!
//! The bot subscribes through an in-process link, so no network client and no
//! separate broker are needed. Clients must authenticate with the configured
//! username/password (compared in constant time by rumqttd).

use std::{collections::HashMap, net::TcpListener};

use anyhow::{Context, Result};
use mxbot_common::supervise::supervise;
use rumqttd::{Broker, Config, ConnectionSettings, Notification, RouterConfig, ServerSettings};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::{
    config::MqttConfig,
    intake::{Incoming, SharedStatus, Source},
};

fn broker_config(mqtt: &MqttConfig) -> Config {
    let connections = ConnectionSettings {
        connection_timeout_ms: 10_000,
        max_payload_size: mqtt.max_payload_bytes,
        max_inflight_count: 100,
        auth: Some(HashMap::from([(
            mqtt.username.clone(),
            mqtt.password.clone(),
        )])),
        external_auth: None,
        dynamic_filters: true,
    };
    let server = ServerSettings {
        name: "v4".to_owned(),
        listen: mqtt.listen,
        tls: None,
        next_connection_delay_ms: 1,
        connections,
    };
    Config {
        router: RouterConfig {
            // Home Assistant plus a few spare connections; this is not a
            // general-purpose broker.
            max_connections: 16,
            max_outgoing_packet_count: 200,
            max_segment_size: 1024 * 1024,
            max_segment_count: 10,
            ..Default::default()
        },
        v4: Some(HashMap::from([("1".to_owned(), server)])),
        ..Default::default()
    }
}

/// Start the broker and forward every message on the configured topics.
///
/// Fails early if the listen address is unavailable (rumqttd itself would
/// only log that from its server thread).
pub fn start(mqtt: &MqttConfig, tx: mpsc::Sender<Incoming>, status: SharedStatus) -> Result<()> {
    drop(
        TcpListener::bind(mqtt.listen)
            .with_context(|| format!("Cannot listen for MQTT on {}", mqtt.listen))?,
    );

    let mut broker = Broker::new(broker_config(mqtt));
    let (mut link_tx, mut link_rx) = broker
        .link("heating-bot")
        .context("Creating the broker's internal link")?;
    for topic in &mqtt.topics {
        link_tx
            .subscribe(topic.clone())
            .with_context(|| format!("Subscribing to {topic}"))?;
    }
    std::thread::Builder::new()
        .name("mqtt-broker".to_owned())
        // The broker runs for the bot's whole life. Its router has a thread of its own and
        // keeps the link above alive, so the intake would not notice a dead server: end the
        // bot instead (Docker restarts it), as mxbot_common::supervise does for a task.
        .spawn(move || {
            match broker.start() {
                Ok(()) => error!("MQTT broker stopped; exiting so that the container is restarted"),
                Err(e) => {
                    error!("MQTT broker stopped: {e}; exiting so that the container is restarted")
                }
            }
            std::process::exit(1);
        })
        .context("Starting the MQTT broker thread")?;
    status.lock().expect("status lock poisoned").mqtt_listen = Some(mqtt.listen);
    info!(listen = %mqtt.listen, topics = ?mqtt.topics, "MQTT broker started");

    supervise("mqtt intake", async move {
        // Keep the link's sending half alive: dropping it ends the subscription.
        let _link_tx = link_tx;
        loop {
            let notification = match link_rx.next().await {
                Ok(Some(notification)) => notification,
                Ok(None) => continue,
                Err(e) => {
                    error!("MQTT broker link closed: {e}");
                    return;
                }
            };
            let Notification::Forward(forward) = notification else {
                continue;
            };
            let topic = String::from_utf8_lossy(&forward.publish.topic).into_owned();
            status
                .lock()
                .expect("status lock poisoned")
                .record_received();
            let incoming = Incoming {
                source: Source::Mqtt(topic),
                payload: forward.publish.payload.to_vec(),
            };
            if tx.send(incoming).await.is_err() {
                warn!("Message forwarder is gone: stopping MQTT intake");
                return;
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc::{AsyncClient, ConnectReturnCode, Event, MqttOptions, Packet, QoS};
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    fn free_port() -> u16 {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn client(port: u16, id: &str, password: &str) -> (AsyncClient, rumqttc::EventLoop) {
        let mut options = MqttOptions::new(id, "127.0.0.1", port);
        options.set_credentials("homeassistant", password);
        AsyncClient::new(options, 10)
    }

    /// Poll until the broker answers the CONNECT; returns its verdict.
    async fn connect_result(eventloop: &mut rumqttc::EventLoop) -> Option<ConnectReturnCode> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(ack))) => return Some(ack.code),
                    Ok(_) => {}
                    Err(_) => return None,
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    #[tokio::test]
    async fn authenticated_clients_are_forwarded_and_others_rejected() {
        let port = free_port();
        let mqtt = MqttConfig {
            enabled: true,
            listen: SocketAddr::from(([127, 0, 0, 1], port)),
            username: "homeassistant".to_owned(),
            password: "a-long-enough-secret".to_owned(),
            topics: vec!["heating/#".to_owned()],
            max_payload_bytes: 65536,
        };
        let (tx, mut rx) = mpsc::channel(10);
        let status: SharedStatus = Arc::default();
        start(&mqtt, tx, Arc::clone(&status)).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Wrong password: no successful CONNACK.
        let (_bad, mut bad_loop) = client(port, "intruder", "wrong-password!!");
        assert_ne!(
            connect_result(&mut bad_loop).await,
            Some(ConnectReturnCode::Success)
        );

        // Correct credentials: messages on the topics reach the bot.
        let (good, mut good_loop) = client(port, "homeassistant", "a-long-enough-secret");
        assert_eq!(
            connect_result(&mut good_loop).await,
            Some(ConnectReturnCode::Success)
        );
        tokio::spawn(async move { while good_loop.poll().await.is_ok() {} });
        good.publish("other/topic", QoS::AtLeastOnce, false, "ignored")
            .await
            .unwrap();
        good.publish("heating/notify", QoS::AtLeastOnce, false, "Heizung an")
            .await
            .unwrap();

        let incoming = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("no message within 5 s")
            .unwrap();
        assert_eq!(incoming.source.to_string(), "mqtt:heating/notify");
        assert_eq!(incoming.payload, b"Heizung an");
        assert_eq!(status.lock().unwrap().received, 1);
    }

    #[test]
    fn a_busy_port_is_reported_at_startup() {
        let taken = TcpListener::bind("127.0.0.1:0").unwrap();
        let mqtt = MqttConfig {
            enabled: true,
            listen: taken.local_addr().unwrap(),
            username: "homeassistant".to_owned(),
            password: "a-long-enough-secret".to_owned(),
            topics: vec!["heating/#".to_owned()],
            max_payload_bytes: 65536,
        };
        let (tx, _rx) = mpsc::channel(1);
        let error = start(&mqtt, tx, Arc::default()).unwrap_err();
        assert!(error.to_string().contains("Cannot listen for MQTT"));
    }
}
