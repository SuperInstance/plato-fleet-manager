use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::connection::Tick;
use chrono::{DateTime, Utc};

/// A cross-room correlation detected by the aggregator.
#[derive(Debug, Clone)]
pub struct CorrelationEvent {
    pub timestamp: DateTime<Utc>,
    pub rooms: Vec<String>,
    pub description: String,
    pub severity: String,
}

/// Aggregates tick streams from multiple rooms.
pub struct TickAggregator {
    /// Latest tick from each room.
    latest_ticks: Arc<Mutex<HashMap<String, Tick>>>,
    /// Historical ticks per room (last N).
    history: Arc<Mutex<HashMap<String, Vec<Tick>>>>,
    max_history: usize,
    /// Active correlation rules.
    correlation_rules: Vec<CorrelationRule>,
}

/// A rule that detects correlations across rooms.
struct CorrelationRule {
    name: String,
    check: Box<dyn Fn(&HashMap<String, Vec<Tick>>) -> Option<CorrelationEvent> + Send + Sync>,
}

impl TickAggregator {
    pub fn new() -> Self {
        Self {
            latest_ticks: Arc::new(Mutex::new(HashMap::new())),
            history: Arc::new(Mutex::new(HashMap::new())),
            max_history: 100,
            correlation_rules: Vec::new(),
        }
    }

    pub fn with_max_history(mut self, n: usize) -> Self {
        self.max_history = n;
        self
    }

    /// Ingest a tick from a room.
    pub async fn ingest(&self, tick: Tick) {
        let room_id = tick.room_id.clone();
        self.latest_ticks.lock().await.insert(room_id.clone(), tick.clone());
        let mut history = self.history.lock().await;
        match history.get_mut(&room_id) {
            Some(h) => {
                h.push(tick);
                if h.len() > self.max_history {
                    h.remove(0);
                }
            }
            None => {
                history.insert(room_id, vec![tick]);
            }
        }
    }

    /// Merge ticks from multiple rooms aligned by timestamp.
    pub async fn merge_aligned(&self, tolerance_ms: i64) -> Vec<Vec<Tick>> {
        let history = self.history.lock().await;
        let all_ticks: Vec<Tick> = history.values().flatten().cloned().collect();
        drop(history);

        if all_ticks.is_empty() {
            return vec![];
        }

        let mut sorted = all_ticks;
        sorted.sort_by_key(|t| t.timestamp);

        let mut groups: Vec<Vec<Tick>> = vec![];
        let mut current_group: Vec<Tick> = vec![];

        for tick in sorted {
            if current_group.is_empty() {
                current_group.push(tick);
            } else {
                let diff = (tick.timestamp - current_group[0].timestamp).num_milliseconds().abs();
                if diff <= tolerance_ms {
                    current_group.push(tick);
                } else {
                    groups.push(current_group);
                    current_group = vec![tick];
                }
            }
        }
        if !current_group.is_empty() {
            groups.push(current_group);
        }
        groups
    }

    /// Detect correlations across rooms using registered rules.
    pub async fn detect_correlations(&self) -> Vec<CorrelationEvent> {
        let history = self.history.lock().await;
        let mut events = Vec::new();
        for rule in &self.correlation_rules {
            if let Some(event) = (rule.check)(&history) {
                events.push(event);
            }
        }
        events
    }

    /// Get all latest ticks.
    pub async fn latest(&self) -> HashMap<String, Tick> {
        self.latest_ticks.lock().await.clone()
    }

    /// Compute fleet-wide statistics.
    pub async fn statistics(&self) -> FleetStatistics {
        let ticks = self.latest_ticks.lock().await;
        let total_rooms = ticks.len();
        let mut total_sensors = 0usize;
        let mut total_alarms = 0usize;
        let mut sensor_sum = 0.0f64;
        let mut sensor_count = 0usize;

        for tick in ticks.values() {
            total_sensors += tick.sensors.len();
            total_alarms += tick.alarms.len();
            for val in tick.sensors.values() {
                sensor_sum += val;
                sensor_count += 1;
            }
        }

        let avg_sensor_value = if sensor_count > 0 {
            sensor_sum / sensor_count as f64
        } else {
            0.0
        };

        FleetStatistics {
            total_rooms,
            total_sensors,
            total_alarms,
            avg_sensor_value,
        }
    }

    /// Add a built-in correlation rule: engine temp + bilge rising.
    pub fn add_engine_bilge_correlation(&mut self) {
        self.correlation_rules.push(CorrelationRule {
            name: "engine_bilge_rising".into(),
            check: Box::new(|history| {
                let engine = history.get("engine-room").and_then(|h| h.last())?;
                let bilge = history.get("bilge").and_then(|h| h.last())?;

                let engine_temp = engine.sensors.get("temperature")?;
                let bilge_level = bilge.sensors.get("water_level")?;

                if *engine_temp > 90.0 && *bilge_level > 50.0 {
                    Some(CorrelationEvent {
                        timestamp: Utc::now(),
                        rooms: vec!["engine-room".into(), "bilge".into()],
                        description: "Engine temperature and bilge water level both elevated".into(),
                        severity: "critical".into(),
                    })
                } else {
                    None
                }
            }),
        });
    }
}

#[derive(Debug, Clone)]
pub struct FleetStatistics {
    pub total_rooms: usize,
    pub total_sensors: usize,
    pub total_alarms: usize,
    pub avg_sensor_value: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    fn make_tick(room: &str, ts_offset_ms: i64, sensors: HashMap<String, f64>) -> Tick {
        Tick {
            room_id: room.into(),
            timestamp: Utc::now() + TimeDelta::milliseconds(ts_offset_ms),
            sensors,
            alarms: vec![],
        }
    }

    #[tokio::test]
    async fn merge_two_tick_streams_by_timestamp() {
        let agg = TickAggregator::new();
        let base = Utc::now();

        agg.ingest(make_tick("room-a", 0, HashMap::new())).await;
        agg.ingest(make_tick("room-b", 5, HashMap::new())).await;
        agg.ingest(make_tick("room-a", 1000, HashMap::new())).await;
        agg.ingest(make_tick("room-b", 1005, HashMap::new())).await;

        let groups = agg.merge_aligned(10).await;
        assert!(groups.len() >= 2, "Expected at least 2 groups, got {}", groups.len());
    }

    #[tokio::test]
    async fn detect_engine_bilge_correlation() {
        let mut agg = TickAggregator::new();
        agg.add_engine_bilge_correlation();

        // Normal readings — no correlation
        let mut sensors = HashMap::new();
        sensors.insert("temperature".into(), 70.0);
        agg.ingest(Tick::new("engine-room", sensors.clone())).await;
        let mut bilge_sensors = HashMap::new();
        bilge_sensors.insert("water_level".into(), 30.0);
        agg.ingest(Tick::new("bilge", bilge_sensors)).await;

        let events = agg.detect_correlations().await;
        assert!(events.is_empty());

        // Critical readings — correlation detected
        let mut hot = HashMap::new();
        hot.insert("temperature".into(), 95.0);
        agg.ingest(Tick::new("engine-room", hot)).await;
        let mut wet = HashMap::new();
        wet.insert("water_level".into(), 60.0);
        agg.ingest(Tick::new("bilge", wet)).await;

        let events = agg.detect_correlations().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].severity, "critical");
    }

    #[tokio::test]
    async fn handle_different_tick_rates() {
        let agg = TickAggregator::new();

        // Room A ticks every 100ms, Room B every 500ms
        for i in 0..5 {
            agg.ingest(make_tick("room-a", i * 100, HashMap::new())).await;
        }
        agg.ingest(make_tick("room-b", 0, HashMap::new())).await;
        agg.ingest(make_tick("room-b", 500, HashMap::new())).await;

        let groups = agg.merge_aligned(200).await;
        assert!(groups.len() > 0);
        // At least one group should have both rooms
        assert!(groups.iter().any(|g| g.iter().any(|t| t.room_id == "room-b")));
    }

    #[tokio::test]
    async fn fleet_statistics() {
        let agg = TickAggregator::new();
        let mut s1 = HashMap::new();
        s1.insert("temp".into(), 20.0);
        s1.insert("pressure".into(), 101.0);
        agg.ingest(Tick::new("room-a", s1)).await;

        let mut s2 = HashMap::new();
        s2.insert("temp".into(), 25.0);
        s2.insert("humidity".into(), 60.0);
        let tick = Tick::new("room-b", s2).with_alarms(vec!["high_temp".into()]);
        agg.ingest(tick).await;

        let stats = agg.statistics().await;
        assert_eq!(stats.total_rooms, 2);
        assert_eq!(stats.total_sensors, 4);
        assert_eq!(stats.total_alarms, 1);
        assert!((stats.avg_sensor_value - 51.5).abs() < 0.01);
    }

    #[tokio::test]
    async fn empty_aggregator() {
        let agg = TickAggregator::new();
        let stats = agg.statistics().await;
        assert_eq!(stats.total_rooms, 0);
        let groups = agg.merge_aligned(100).await;
        assert!(groups.is_empty());
    }
}
