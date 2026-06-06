use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::aggregator::TickAggregator;
use crate::config::{ConnectionConfig, FleetConfig};
use crate::connection::{MockRoomConnection, Response, RoomConnection, RoomConnectionTrait, Tick};
use crate::monitor::{FleetHealth, FleetMonitor, RoomStateSnapshot};

/// Current state of a room in the fleet.
#[derive(Debug, Clone)]
pub struct RoomState {
    pub id: String,
    pub connected: bool,
    pub last_tick: Option<Tick>,
    pub alarm_count: usize,
}

/// The top-level fleet orchestrator.
pub struct FleetManager {
    rooms: Arc<Mutex<HashMap<String, Box<dyn RoomConnectionTrait>>>>,
    aggregator: Arc<Mutex<TickAggregator>>,
    monitor: FleetMonitor,
    config: FleetConfig,
}

impl FleetManager {
    pub fn new(config: FleetConfig) -> Self {
        Self {
            rooms: Arc::new(Mutex::new(HashMap::new())),
            aggregator: Arc::new(Mutex::new(TickAggregator::new())),
            monitor: FleetMonitor::new(),
            config,
        }
    }

    /// Add a room with a mock connection (for testing).
    pub async fn add_mock_room(&self, id: impl Into<String>, conn: MockRoomConnection) {
        let id_str = id.into();
        self.rooms.lock().await.insert(id_str.clone(), Box::new(conn));
    }

    /// Register a room with a real TCP connection config.
    pub async fn add_room(&self, id: impl Into<String>, config: ConnectionConfig) {
        let id_str = id.into();
        let conn = RoomConnection::new(&id_str, config.host, config.port);
        self.rooms.lock().await.insert(id_str, Box::new(conn));
    }

    /// Remove and disconnect a room.
    pub async fn remove_room(&self, id: &str) -> bool {
        let mut rooms = self.rooms.lock().await;
        rooms.remove(id).is_some()
    }

    /// Connect all registered rooms.
    pub async fn connect_all(&self) -> Result<Vec<String>, Vec<String>> {
        let mut rooms = self.rooms.lock().await;
        let mut connected = Vec::new();
        let mut failed = Vec::new();

        for (id, conn) in rooms.iter_mut() {
            match conn.connect().await {
                Ok(()) => connected.push(id.clone()),
                Err(e) => failed.push(format!("{}: {}", id, e)),
            }
        }

        if failed.is_empty() {
            Ok(connected)
        } else {
            Err(failed)
        }
    }

    /// Send a command to all rooms.
    pub async fn broadcast_command(&self, cmd: &str) -> Vec<Result<Response, String>> {
        let rooms = self.rooms.lock().await;
        let mut results = Vec::new();
        for (_, conn) in rooms.iter() {
            results.push(conn.send(cmd).await);
        }
        results
    }

    /// Send a command to a specific room.
    pub async fn send_command(&self, room_id: &str, cmd: &str) -> Result<Response, String> {
        let rooms = self.rooms.lock().await;
        match rooms.get(room_id) {
            Some(conn) => conn.send(cmd).await,
            None => Err(format!("room '{}' not found", room_id)),
        }
    }

    /// Subscribe to all rooms' tick streams.
    pub async fn subscribe_all(&self) -> Vec<Result<(), String>> {
        let mut rooms = self.rooms.lock().await;
        let mut results = Vec::new();
        for (_, conn) in rooms.iter_mut() {
            results.push(conn.subscribe().await);
        }
        results
    }

    /// Get a snapshot of all room states.
    pub async fn get_room_states(&self) -> HashMap<String, RoomState> {
        let rooms = self.rooms.lock().await;
        let mut states = HashMap::new();
        for (id, conn) in rooms.iter() {
            states.insert(id.clone(), RoomState {
                id: id.clone(),
                connected: conn.is_healthy(),
                last_tick: conn.last_tick(),
                alarm_count: conn.last_tick().map(|t| t.alarms.len()).unwrap_or(0),
            });
        }
        states
    }

    /// Get aggregate fleet health.
    pub async fn get_fleet_health(&self) -> FleetHealth {
        let rooms = self.rooms.lock().await;
        let mut snapshots = HashMap::new();
        for (id, conn) in rooms.iter() {
            let lt = conn.last_tick();
            snapshots.insert(id.clone(), RoomStateSnapshot {
                connected: conn.is_healthy(),
                last_tick_time: lt.as_ref().map(|t| t.timestamp),
                alarm_count: lt.as_ref().map(|t| t.alarms.len()).unwrap_or(0),
                uptime_percentage: if conn.is_healthy() { 100.0 } else { 0.0 },
            });
        }
        drop(rooms);
        self.monitor.compute_health(&snapshots)
    }

    /// Ingest a tick into the aggregator (called when ticks are received).
    pub async fn ingest_tick(&self, tick: Tick) {
        self.aggregator.lock().await.ingest(tick).await;
    }

    /// Get fleet statistics.
    pub async fn get_statistics(&self) -> crate::aggregator::FleetStatistics {
        self.aggregator.lock().await.statistics().await
    }

    /// Main run loop (simplified for testing with mocks).
    pub async fn run(&self) -> Result<(), String> {
        self.connect_all().await.map_err(|e| e.join(", "))?;
        self.subscribe_all().await.iter().for_each(|r| {
            if let Err(e) = r {
                tracing::warn!("subscribe failed: {}", e);
            }
        });
        Ok(())
    }

    /// Get the fleet config.
    pub fn config(&self) -> &FleetConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EscalationPolicy;
    use std::collections::HashMap;

    fn test_config() -> FleetConfig {
        FleetConfig {
            fleet_name: "test-fleet".into(),
            vessel: None,
            rooms: vec![],
            escalation: EscalationPolicy::default(),
        }
    }

    #[tokio::test]
    async fn create_fleet_manager() {
        let mgr = FleetManager::new(test_config());
        let states = mgr.get_room_states().await;
        assert!(states.is_empty());
    }

    #[tokio::test]
    async fn add_and_remove_room() {
        let mgr = FleetManager::new(test_config());
        let mock = MockRoomConnection::new("room-a");
        mgr.add_mock_room("room-a", mock).await;

        assert!(mgr.get_room_states().await.contains_key("room-a"));
        assert!(mgr.remove_room("room-a").await);
        assert!(!mgr.get_room_states().await.contains_key("room-a"));
    }

    #[tokio::test]
    async fn broadcast_command() {
        let mgr = FleetManager::new(test_config());
        let mut mock_a = MockRoomConnection::new("room-a");
        let mut mock_b = MockRoomConnection::new("room-b");

        mock_a.connect().await.unwrap();
        mock_b.connect().await.unwrap();

        mgr.add_mock_room("room-a", mock_a).await;
        mgr.add_mock_room("room-b", mock_b).await;

        let results = mgr.broadcast_command("tick").await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn send_command_specific_room() {
        let mgr = FleetManager::new(test_config());
        let mut mock = MockRoomConnection::new("room-a");
        mock.connect().await.unwrap();
        mgr.add_mock_room("room-a", mock).await;

        let result = mgr.send_command("room-a", "history").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().command, "history");

        let missing = mgr.send_command("nonexistent", "tick").await;
        assert!(missing.is_err());
    }

    #[tokio::test]
    async fn get_room_states_snapshot() {
        let mgr = FleetManager::new(test_config());
        let mut mock = MockRoomConnection::new("room-a");
        mock.connect().await.unwrap();
        mgr.add_mock_room("room-a", mock).await;

        let states = mgr.get_room_states().await;
        assert_eq!(states.len(), 1);
        assert!(states.get("room-a").unwrap().connected);
    }

    #[tokio::test]
    async fn health_check_across_rooms() {
        let mgr = FleetManager::new(test_config());

        let mut mock_a = MockRoomConnection::new("room-a");
        mock_a.connect().await.unwrap();
        // Give room-a a tick so it's considered ticking
        let tick = Tick::new("room-a", HashMap::new());
        mock_a.enqueue_tick(tick).await;
        mock_a.next_tick().await; // consume to set last_tick

        let mut mock_b = MockRoomConnection::new("room-b");
        mock_b.connect().await.unwrap();
        mock_b.set_healthy(false);

        mgr.add_mock_room("room-a", mock_a).await;
        mgr.add_mock_room("room-b", mock_b).await;

        let health = mgr.get_fleet_health().await;
        assert_eq!(health.total_rooms, 2);
        assert_eq!(health.rooms_down, 1);
    }

    #[tokio::test]
    async fn reconnection_handling() {
        let mgr = FleetManager::new(test_config());
        let mut mock = MockRoomConnection::new("room-a");
        mock.set_connect_fail(true).await;
        mgr.add_mock_room("room-a", mock).await;

        let result = mgr.connect_all().await;
        assert!(result.is_err());

        // Now remove the failing room, add a working one
        mgr.remove_room("room-a").await;
        let mut mock2 = MockRoomConnection::new("room-a");
        mock2.connect().await.unwrap();
        mgr.add_mock_room("room-a", mock2).await;

        let result = mgr.connect_all().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn fleet_statistics_avg_sensors_total_alarms() {
        let mgr = FleetManager::new(test_config());

        let mut sensors = HashMap::new();
        sensors.insert("temp".into(), 20.0);
        let tick = Tick::new("room-a", sensors).with_alarms(vec!["high_temp".into()]);
        mgr.ingest_tick(tick).await;

        let mut s2 = HashMap::new();
        s2.insert("temp".into(), 22.0);
        s2.insert("pressure".into(), 101.0);
        mgr.ingest_tick(Tick::new("room-b", s2)).await;

        let stats = mgr.get_statistics().await;
        assert_eq!(stats.total_rooms, 2);
        assert_eq!(stats.total_sensors, 3);
        assert_eq!(stats.total_alarms, 1);
    }

    #[tokio::test]
    async fn full_fleet_lifecycle() {
        let config = FleetConfig::fishing_boat_manifest();
        let mgr = FleetManager::new(config);

        // Add mock rooms
        for room in &mgr.config().rooms {
            let mut mock = MockRoomConnection::new(&room.id);
            mock.connect().await.unwrap();
            mock.subscribe().await.unwrap();

            // Enqueue some ticks
            let mut sensors = HashMap::new();
            sensors.insert("temperature".into(), 75.0);
            let tick = Tick::new(&room.id, sensors).with_alarms(vec![]);
            mock.enqueue_tick(tick).await;

            mgr.add_mock_room(&room.id, mock).await;
        }

        // Run fleet
        mgr.run().await.unwrap();

        // Broadcast command
        let results = mgr.broadcast_command("tick").await;
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|r| r.is_ok()));

        // Specific command
        let result = mgr.send_command("engine-room", "history").await;
        assert!(result.is_ok());

        // Ingest ticks
        let mut sensors = HashMap::new();
        sensors.insert("temperature".into(), 95.0);
        let alarm_tick = Tick::new("engine-room", sensors)
            .with_alarms(vec!["overheat".into()]);
        mgr.ingest_tick(alarm_tick).await;

        // Check stats
        let stats = mgr.get_statistics().await;
        assert_eq!(stats.total_rooms, 1);
        assert_eq!(stats.total_alarms, 1);

        // Check health
        let health = mgr.get_fleet_health().await;
        assert_eq!(health.total_rooms, 4);

        // Get room states
        let states = mgr.get_room_states().await;
        assert_eq!(states.len(), 4);

        // Remove a room
        assert!(mgr.remove_room("bilge").await);
        assert_eq!(mgr.get_room_states().await.len(), 3);
    }
}
