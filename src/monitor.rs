use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Overall health status for the fleet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Green,
    Yellow,
    Red,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Green => write!(f, "🟢 GREEN"),
            HealthStatus::Yellow => write!(f, "🟡 YELLOW"),
            HealthStatus::Red => write!(f, "🔴 RED"),
        }
    }
}

/// Health snapshot for the entire fleet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetHealth {
    pub status: HealthStatus,
    pub total_rooms: usize,
    pub healthy_rooms: usize,
    pub rooms_with_alarms: usize,
    pub rooms_down: usize,
    pub last_updated: DateTime<Utc>,
    pub details: HashMap<String, RoomHealth>,
}

/// Health info for a single room.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomHealth {
    pub connected: bool,
    pub ticking: bool,
    pub alarm_count: usize,
    pub last_tick: Option<DateTime<Utc>>,
    pub uptime_percentage: f64,
}

/// Monitors fleet health and generates alerts.
pub struct FleetMonitor {
    /// Max seconds since last tick before a room is considered "not ticking".
    tick_timeout_secs: u64,
    /// Threshold of rooms down to trigger red status.
    red_threshold: f64,
}

impl FleetMonitor {
    pub fn new() -> Self {
        Self {
            tick_timeout_secs: 30,
            red_threshold: 0.5,
        }
    }

    pub fn with_tick_timeout(mut self, secs: u64) -> Self {
        self.tick_timeout_secs = secs;
        self
    }

    pub fn with_red_threshold(mut self, threshold: f64) -> Self {
        self.red_threshold = threshold;
        self
    }

    /// Compute fleet health from a set of room states.
    pub fn compute_health(
        &self,
        rooms: &HashMap<String, RoomStateSnapshot>,
    ) -> FleetHealth {
        let total_rooms = rooms.len();
        if total_rooms == 0 {
            return FleetHealth {
                status: HealthStatus::Green,
                total_rooms: 0,
                healthy_rooms: 0,
                rooms_with_alarms: 0,
                rooms_down: 0,
                last_updated: Utc::now(),
                details: HashMap::new(),
            };
        }

        let mut healthy_rooms = 0usize;
        let mut rooms_with_alarms = 0usize;
        let mut rooms_down = 0usize;
        let mut details = HashMap::new();
        let now = Utc::now();

        for (id, state) in rooms {
            let connected = state.connected;
            let ticking = if let Some(last) = state.last_tick_time {
                (now - last).num_seconds().abs() < self.tick_timeout_secs as i64
            } else {
                false
            };
            let alarm_count = state.alarm_count;

            if connected && ticking && alarm_count == 0 {
                healthy_rooms += 1;
            }
            if alarm_count > 0 {
                rooms_with_alarms += 1;
            }
            if !connected || !ticking {
                rooms_down += 1;
            }

            details.insert(id.clone(), RoomHealth {
                connected,
                ticking,
                alarm_count,
                last_tick: state.last_tick_time,
                uptime_percentage: state.uptime_percentage,
            });
        }

        let down_ratio = rooms_down as f64 / total_rooms as f64;
        let status = if total_rooms == rooms_down || down_ratio > self.red_threshold {
            HealthStatus::Red
        } else if rooms_down > 0 || rooms_with_alarms > 0 {
            HealthStatus::Yellow
        } else {
            HealthStatus::Green
        };

        FleetHealth {
            status,
            total_rooms,
            healthy_rooms,
            rooms_with_alarms,
            rooms_down,
            last_updated: now,
            details,
        }
    }

    /// Detect rooms that have stopped ticking.
    pub fn detect_stale_rooms(
        &self,
        rooms: &HashMap<String, RoomStateSnapshot>,
    ) -> Vec<String> {
        let now = Utc::now();
        let mut stale = Vec::new();
        for (id, state) in rooms {
            if let Some(last) = state.last_tick_time {
                if (now - last).num_seconds().abs() >= self.tick_timeout_secs as i64 {
                    stale.push(id.clone());
                }
            } else if state.connected {
                // Connected but never ticked
                stale.push(id.clone());
            }
        }
        stale
    }
}

/// Snapshot of a room's state for health monitoring.
#[derive(Debug, Clone)]
pub struct RoomStateSnapshot {
    pub connected: bool,
    pub last_tick_time: Option<DateTime<Utc>>,
    pub alarm_count: usize,
    pub uptime_percentage: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state(connected: bool, alarm_count: usize, uptime: f64) -> RoomStateSnapshot {
        RoomStateSnapshot {
            connected,
            last_tick_time: if connected { Some(Utc::now()) } else { None },
            alarm_count,
            uptime_percentage: uptime,
        }
    }

    fn make_stale_state() -> RoomStateSnapshot {
        RoomStateSnapshot {
            connected: true,
            last_tick_time: Some(Utc::now() - chrono::Duration::seconds(120)),
            alarm_count: 0,
            uptime_percentage: 95.0,
        }
    }

    #[test]
    fn healthy_fleet_is_green() {
        let monitor = FleetMonitor::new();
        let mut rooms = HashMap::new();
        rooms.insert("room-a".into(), make_state(true, 0, 99.0));
        rooms.insert("room-b".into(), make_state(true, 0, 98.0));

        let health = monitor.compute_health(&rooms);
        assert_eq!(health.status, HealthStatus::Green);
        assert_eq!(health.healthy_rooms, 2);
        assert_eq!(health.rooms_down, 0);
    }

    #[test]
    fn one_room_down_is_yellow() {
        let monitor = FleetMonitor::new();
        let mut rooms = HashMap::new();
        rooms.insert("room-a".into(), make_state(true, 0, 99.0));
        rooms.insert("room-b".into(), make_state(false, 0, 50.0));

        let health = monitor.compute_health(&rooms);
        assert_eq!(health.status, HealthStatus::Yellow);
        assert_eq!(health.rooms_down, 1);
    }

    #[test]
    fn critical_alarm_is_red() {
        let monitor = FleetMonitor::new().with_red_threshold(0.5);
        let mut rooms = HashMap::new();
        rooms.insert("room-a".into(), make_state(false, 2, 0.0));
        rooms.insert("room-b".into(), make_state(false, 1, 0.0));

        let health = monitor.compute_health(&rooms);
        assert_eq!(health.status, HealthStatus::Red);
        assert_eq!(health.rooms_down, 2);
    }

    #[test]
    fn detect_room_stopped_ticking() {
        let monitor = FleetMonitor::new().with_tick_timeout(30);
        let mut rooms = HashMap::new();
        rooms.insert("room-a".into(), make_state(true, 0, 99.0));
        rooms.insert("room-b".into(), make_stale_state());

        let stale = monitor.detect_stale_rooms(&rooms);
        assert_eq!(stale, vec!["room-b"]);
    }

    #[test]
    fn all_rooms_down_is_red() {
        let monitor = FleetMonitor::new();
        let mut rooms = HashMap::new();
        rooms.insert("room-a".into(), make_state(false, 0, 0.0));

        let health = monitor.compute_health(&rooms);
        assert_eq!(health.status, HealthStatus::Red);
    }

    #[test]
    fn empty_fleet_is_green() {
        let monitor = FleetMonitor::new();
        let rooms = HashMap::new();
        let health = monitor.compute_health(&rooms);
        assert_eq!(health.status, HealthStatus::Green);
    }

    #[test]
    fn multiple_alarms_across_rooms() {
        let monitor = FleetMonitor::new();
        let mut rooms = HashMap::new();
        rooms.insert("room-a".into(), make_state(true, 2, 90.0));
        rooms.insert("room-b".into(), make_state(true, 1, 85.0));
        rooms.insert("room-c".into(), make_state(true, 0, 99.0));

        let health = monitor.compute_health(&rooms);
        assert_eq!(health.status, HealthStatus::Yellow);
        assert_eq!(health.rooms_with_alarms, 2);
    }
}
