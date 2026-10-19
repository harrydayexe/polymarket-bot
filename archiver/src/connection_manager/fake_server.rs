//! A fake market channel for tests: accepts websocket connections on a local port and lets the
//! test script each one.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{SinkExt, StreamExt};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// How long a test waits for anything before failing.
pub const TIMEOUT: Duration = Duration::from_secs(5);

pub struct FakeServer {
    url: url::Url,
    accepted: mpsc::UnboundedReceiver<FakeConn>,
    refuse: Arc<AtomicBool>,
}

impl FakeServer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws/market", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let (accepted_tx, accepted) = mpsc::unbounded_channel();
        let refuse = Arc::new(AtomicBool::new(false));
        let refusing = refuse.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                if refusing.load(Ordering::SeqCst) {
                    // Dropping the stream fails the handshake.
                    continue;
                }
                let Ok(socket) = accept_async(stream).await else {
                    continue;
                };
                if accepted_tx.send(FakeConn::serve(socket)).is_err() {
                    return;
                }
            }
        });
        Self {
            url,
            accepted,
            refuse,
        }
    }

    pub fn url(&self) -> url::Url {
        self.url.clone()
    }

    /// Fail every handshake while `refuse` is set.
    pub fn refuse(&self, refuse: bool) {
        self.refuse.store(refuse, Ordering::SeqCst);
    }

    /// The next connection the client opened.
    pub async fn accept(&mut self) -> FakeConn {
        tokio::time::timeout(TIMEOUT, self.accepted.recv())
            .await
            .expect("timed out waiting for a connection")
            .expect("server stopped")
    }

    /// Asserts that no connection is opened within `d`.
    pub async fn assert_no_connection_within(&mut self, d: Duration) {
        if let Ok(conn) = tokio::time::timeout(d, self.accepted.recv()).await {
            panic!("unexpected connection: {:?}", conn.is_some());
        }
    }
}

enum Command {
    Send(String),
    Close,
}

/// The server side of one connection. Answers `PING` with `PONG` unless told to stop.
pub struct FakeConn {
    from_client: mpsc::UnboundedReceiver<String>,
    to_client: mpsc::UnboundedSender<Command>,
    answer_pings: Arc<AtomicBool>,
}

impl FakeConn {
    fn serve(mut socket: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Self {
        let (from_client_tx, from_client) = mpsc::unbounded_channel();
        let (to_client, mut commands) = mpsc::unbounded_channel();
        let answer_pings = Arc::new(AtomicBool::new(true));
        let answering = answer_pings.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = socket.next() => match msg {
                        Some(Ok(Message::Text(text))) if text.as_str() == "PING" => {
                            if answering.load(Ordering::SeqCst) {
                                let _ = socket.send(Message::text("PONG")).await;
                            }
                        }
                        Some(Ok(Message::Text(text))) => {
                            let _ = from_client_tx.send(text.as_str().to_owned());
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                        Some(Ok(_)) => {}
                    },
                    Some(command) = commands.recv() => match command {
                        Command::Send(text) => {
                            let _ = socket.send(Message::text(text)).await;
                        }
                        Command::Close => {
                            let _ = socket.close(None).await;
                        }
                    },
                }
            }
        });
        Self {
            from_client,
            to_client,
            answer_pings,
        }
    }

    /// The next text frame from the client, other than `PING`; `None` once it has closed.
    pub async fn recv(&mut self) -> Option<String> {
        tokio::time::timeout(TIMEOUT, self.from_client.recv())
            .await
            .expect("timed out waiting for a client frame")
    }

    pub async fn recv_json(&mut self) -> serde_json::Value {
        let text = self.recv().await.expect("client closed");
        serde_json::from_str(&text).unwrap()
    }

    pub fn send(&self, text: &str) {
        let _ = self.to_client.send(Command::Send(text.into()));
    }

    pub fn close(&self) {
        let _ = self.to_client.send(Command::Close);
    }

    pub fn stop_answering_pings(&self) {
        self.answer_pings.store(false, Ordering::SeqCst);
    }

    /// Waits for the client to close the connection.
    pub async fn closed(&mut self) {
        while let Some(text) = self.recv().await {
            tracing::debug!(text, "frame while waiting for close");
        }
    }
}
