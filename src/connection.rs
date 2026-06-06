use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use async_trait::async_trait;

/// A single sensor tick from a room.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tick {
    pub room_id: String,
    pub timestamp: DateTime<Utc>,
    pub sensors: HashMap<String, f64>,
    pub alarms: Vec<String>,
}

impl Tick {
    pub fn new(room_id: impl Into<String>, sensors: HashMap<String, f64>) -> Self {
        Self {
            room_id: room_id.into(),
            timestamp: Utc::now(),
            sensors,
            alarms: Vec::new(),
        }
    }

    pub fn with_alarms(mut self, alarms: Vec<String>) -> Self {
        self.alarms = alarms;
        self
    }

    pub fn at(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = ts;
        self
    }
}

/// Response from a room command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Response {
    pub room_id: String,
    pub command: String,
    pub status: String,
    pub data: serde_json::Value,
}

impl Response {
    pub fn ok(room_id: impl Into<String>, command: impl Into<String>, data: serde_json::Value) -> Self {
        Self {
            room_id: room_id.into(),
            command: command.into(),
            status: "ok".into(),
            data,
        }
    }

    pub fn error(room_id: impl Into<String>, command: impl Into<String>, msg: impl Into<String>) -> Self {
        Self {
            room_id: room_id.into(),
            command: command.into(),
            status: "error".into(),
            data: serde_json::json!({"error": msg.into()}),
        }
    }
}

/// Trait for room connections (enables mocking).
#[async_trait]
pub trait RoomConnectionTrait: Send + Sync {
    async fn connect(&mut self) -> Result<(), String>;
    async fn send(&self, cmd: &str) -> Result<Response, String>;
    async fn subscribe(&mut self) -> Result<(), String>;
    async fn unsubscribe(&mut self) -> Result<(), String>;
    async fn next_tick(&mut self) -> Option<Tick>;
    fn is_healthy(&self) -> bool;
    fn last_tick(&self) -> Option<Tick>;
    fn room_id(&self) -> &str;
    fn set_healthy(&mut self, healthy: bool);
}

/// Real TCP connection to a Plato room.
pub struct RoomConnection {
    room_id: String,
    host: String,
    port: u16,
    healthy: bool,
    connected: bool,
    subscribed: bool,
    last_tick: Option<Tick>,
}

impl RoomConnection {
    pub fn new(room_id: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            room_id: room_id.into(),
            host: host.into(),
            port,
            healthy: false,
            connected: false,
            subscribed: false,
            last_tick: None,
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

#[async_trait]
impl RoomConnectionTrait for RoomConnection {
    async fn connect(&mut self) -> Result<(), String> {
        // In production, this would establish a real TCP connection
        self.connected = true;
        self.healthy = true;
        Ok(())
    }

    async fn send(&self, cmd: &str) -> Result<Response, String> {
        if !self.connected {
            return Err("not connected".into());
        }
        Ok(Response::ok(&self.room_id, cmd, serde_json::json!({"echo": cmd})))
    }

    async fn subscribe(&mut self) -> Result<(), String> {
        if !self.connected {
            return Err("not connected".into());
        }
        self.subscribed = true;
        Ok(())
    }

    async fn unsubscribe(&mut self) -> Result<(), String> {
        self.subscribed = false;
        Ok(())
    }

    async fn next_tick(&mut self) -> Option<Tick> {
        // Real implementation would read from TCP stream
        None
    }

    fn is_healthy(&self) -> bool {
        self.healthy
    }

    fn last_tick(&self) -> Option<Tick> {
        self.last_tick.clone()
    }

    fn room_id(&self) -> &str {
        &self.room_id
    }

    fn set_healthy(&mut self, healthy: bool) {
        self.healthy = healthy;
    }
}

/// Mock connection for testing.
pub struct MockRoomConnection {
    room_id: String,
    healthy: bool,
    connected: bool,
    subscribed: bool,
    last_tick: Option<Tick>,
    tick_queue: Arc<Mutex<Vec<Tick>>>,
    responses: Arc<Mutex<HashMap<String, Response>>>,
    connect_fail: Arc<Mutex<bool>>,
}

impl MockRoomConnection {
    pub fn new(room_id: impl Into<String>) -> Self {
        Self {
            room_id: room_id.into(),
            healthy: true,
            connected: false,
            subscribed: false,
            last_tick: None,
            tick_queue: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(HashMap::new())),
            connect_fail: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn enqueue_tick(&self, tick: Tick) {
        self.tick_queue.lock().await.push(tick);
    }

    pub async fn enqueue_response(&self, command: String, response: Response) {
        self.responses.lock().await.insert(command, response);
    }

    pub async fn set_connect_fail(&self, fail: bool) {
        *self.connect_fail.lock().await = fail;
    }
}

#[async_trait]
impl RoomConnectionTrait for MockRoomConnection {
    async fn connect(&mut self) -> Result<(), String> {
        if *self.connect_fail.lock().await {
            return Err("connection refused".into());
        }
        self.connected = true;
        self.healthy = true;
        Ok(())
    }

    async fn send(&self, cmd: &str) -> Result<Response, String> {
        if !self.connected {
            return Err("not connected".into());
        }
        let responses = self.responses.lock().await;
        if let Some(resp) = responses.get(cmd) {
            return Ok(resp.clone());
        }
        Ok(Response::ok(&self.room_id, cmd, serde_json::json!({"echo": cmd})))
    }

    async fn subscribe(&mut self) -> Result<(), String> {
        if !self.connected {
            return Err("not connected".into());
        }
        self.subscribed = true;
        Ok(())
    }

    async fn unsubscribe(&mut self) -> Result<(), String> {
        self.subscribed = false;
        Ok(())
    }

    async fn next_tick(&mut self) -> Option<Tick> {
        let tick = self.tick_queue.lock().await.pop();
        if let Some(ref t) = tick {
            self.last_tick = Some(t.clone());
        }
        tick
    }

    fn is_healthy(&self) -> bool {
        self.healthy
    }

    fn last_tick(&self) -> Option<Tick> {
        self.last_tick.clone()
    }

    fn room_id(&self) -> &str {
        &self.room_id
    }

    fn set_healthy(&mut self, healthy: bool) {
        self.healthy = healthy;
    }
}
