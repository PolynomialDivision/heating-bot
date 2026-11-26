//! HTTP endpoint Home Assistant can post notifications to (`rest_command`),
//! as an alternative to MQTT.
//!
//! Route: `POST /message` with the same body as an MQTT payload: JSON
//! `{"message": "...", "title": "..."}` or plain text. If a token is
//! configured, `Authorization: Bearer <token>` is required; without one the
//! endpoint is open (internal network only).
//!
//! Replies: `202` queued for posting, `400` nothing to post, `401` missing or
//! wrong token, `413` body too large, `503` queue full.

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use mxbot_common::supervise::supervise;
use subtle::ConstantTimeEq;
use tokio::{net::TcpListener, sync::mpsc};
use tracing::{error, info, warn};

use crate::{
    config::HttpConfig,
    intake::{Incoming, SharedStatus, Source},
    message,
};

struct AppState {
    token: String,
    tx: mpsc::Sender<Incoming>,
    status: SharedStatus,
}

fn router(http: &HttpConfig, tx: mpsc::Sender<Incoming>, status: SharedStatus) -> Router {
    let state = Arc::new(AppState {
        token: http.token.clone(),
        tx,
        status,
    });
    Router::new()
        .route("/message", post(post_message))
        .layer(DefaultBodyLimit::max(http.max_body_bytes))
        .with_state(state)
}

/// Bind the endpoint and serve it in the background.
///
/// Binding happens here so a busy port fails startup instead of only being
/// logged later.
pub async fn start(
    http: &HttpConfig,
    tx: mpsc::Sender<Incoming>,
    status: SharedStatus,
) -> Result<()> {
    let listener = TcpListener::bind(http.listen)
        .await
        .with_context(|| format!("Cannot listen for HTTP on {}", http.listen))?;
    let app = router(http, tx, Arc::clone(&status));
    status.lock().expect("status lock poisoned").http_listen = Some(http.listen);
    info!(
        listen = %http.listen,
        auth = if http.token.is_empty() { "none" } else { "bearer token" },
        "HTTP endpoint started (POST /message)"
    );
    supervise(
        "http endpoint",
        async move { axum::serve(listener, app).await },
    );
    Ok(())
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    if token.is_empty() {
        return true;
    }
    let Some(presented) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return false;
    };
    presented.trim().as_bytes().ct_eq(token.as_bytes()).into()
}

async fn post_message(
    State(app): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !authorized(&headers, &app.token) {
        warn!("Rejected HTTP message: missing or wrong bearer token");
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            "unauthorized\n",
        )
            .into_response();
    }
    if message::render(&body, None).is_none() {
        return (StatusCode::BAD_REQUEST, "empty or non-UTF-8 message\n").into_response();
    }
    let incoming = Incoming {
        source: Source::Http,
        payload: body.to_vec(),
    };
    if app.tx.try_send(incoming).is_err() {
        error!("Message queue full: HTTP message dropped");
        return (StatusCode::SERVICE_UNAVAILABLE, "queue full, retry later\n").into_response();
    }
    app.status
        .lock()
        .expect("status lock poisoned")
        .record_received();
    (StatusCode::ACCEPTED, "queued\n").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    fn config(listen: std::net::SocketAddr) -> HttpConfig {
        HttpConfig {
            enabled: true,
            listen,
            token: TOKEN.to_owned(),
            max_body_bytes: 64,
        }
    }

    fn request(auth: Option<&str>, body: impl Into<Body>) -> Request<Body> {
        let mut builder =
            Request::post("/message").header(header::CONTENT_TYPE, "application/json");
        if let Some(auth) = auth {
            builder = builder.header(header::AUTHORIZATION, auth);
        }
        builder.body(body.into()).unwrap()
    }

    async fn send(app: &Router, request: Request<Body>) -> (StatusCode, String) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn authorized_messages_are_queued() {
        let (tx, mut rx) = mpsc::channel(10);
        let status = SharedStatus::default();
        let app = router(&config(([127, 0, 0, 1], 0).into()), tx, Arc::clone(&status));

        let body = r#"{"message": "Heizung wurde eingeschaltet."}"#;
        let (code, _) = send(&app, request(Some(&format!("Bearer {TOKEN}")), body)).await;
        assert_eq!(code, StatusCode::ACCEPTED);

        let incoming = rx.try_recv().unwrap();
        assert_eq!(incoming.source.to_string(), "http");
        assert_eq!(
            message::render(&incoming.payload, None).as_deref(),
            Some("Heizung wurde eingeschaltet.")
        );
        assert_eq!(status.lock().unwrap().received, 1);
    }

    #[tokio::test]
    async fn without_a_token_any_request_is_accepted() {
        let (tx, mut rx) = mpsc::channel(10);
        let http = HttpConfig {
            token: String::new(),
            ..config(([127, 0, 0, 1], 0).into())
        };
        let app = router(&http, tx, SharedStatus::default());

        let body = r#"{"message": "Heizung wurde eingeschaltet."}"#;
        assert_eq!(
            send(&app, request(None, body)).await.0,
            StatusCode::ACCEPTED
        );
        assert_eq!(rx.try_recv().unwrap().payload, body.as_bytes());
        // Content still has to make sense.
        assert_eq!(
            send(&app, request(None, " ")).await.0,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn bad_requests_are_rejected_and_not_queued() {
        let (tx, mut rx) = mpsc::channel(10);
        let app = router(
            &config(([127, 0, 0, 1], 0).into()),
            tx,
            SharedStatus::default(),
        );
        let good = format!("Bearer {TOKEN}");

        let (code, _) = send(&app, request(None, "hi")).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        let (code, _) = send(&app, request(Some("Bearer wrong"), "hi")).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        let (code, _) = send(&app, request(Some(TOKEN), "hi")).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        let (code, _) = send(&app, request(Some(&good), r#"{"message": " "}"#)).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        let (code, _) = send(&app, request(Some(&good), "x".repeat(100))).await;
        assert_eq!(code, StatusCode::PAYLOAD_TOO_LARGE);
        let (code, _) = send(&app, Request::get("/message").body(Body::empty()).unwrap()).await;
        assert_eq!(code, StatusCode::METHOD_NOT_ALLOWED);

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_full_queue_answers_503() {
        let (tx, _rx) = mpsc::channel(1);
        let app = router(
            &config(([127, 0, 0, 1], 0).into()),
            tx,
            SharedStatus::default(),
        );
        let good = format!("Bearer {TOKEN}");
        assert_eq!(
            send(&app, request(Some(&good), "eins")).await.0,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            send(&app, request(Some(&good), "zwei")).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn the_server_answers_over_tcp_and_reports_a_busy_port() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let (tx, mut rx) = mpsc::channel(10);
        let status = SharedStatus::default();
        let http = config(([127, 0, 0, 1], port).into());
        start(&http, tx.clone(), Arc::clone(&status)).await.unwrap();
        assert_eq!(status.lock().unwrap().http_listen, Some(http.listen));

        let mut stream = tokio::net::TcpStream::connect(http.listen).await.unwrap();
        let body = "Heizung an";
        stream
            .write_all(
                format!(
                    "POST /message HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {TOKEN}\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 202"), "{response}");
        assert_eq!(rx.recv().await.unwrap().payload, b"Heizung an");

        let error = start(&http, tx, SharedStatus::default()).await.unwrap_err();
        assert!(error.to_string().contains("Cannot listen for HTTP"));
    }
}
