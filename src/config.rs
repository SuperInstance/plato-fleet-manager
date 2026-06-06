use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Connection parameters for a room.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_reconnect")]
    pub reconnect_interval_secs: u64,
}

fn default_timeout() -> u64 { 30 }
fn default_reconnect() -> u64 { 5 }

impl ConnectionConfig {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            timeout_secs: default_timeout(),
            reconnect_interval_secs: default_reconnect(),
        }
    }
}

/// Escalation policy for alarms.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EscalationPolicy {
    #[serde(default)]
    pub notify_channels: Vec<String>,
    #[serde(default)]
    pub auto_acknowledge_after_secs: Option<u64>,
    #[serde(default)]
    pub critical_threshold: u32,
}

impl Default for EscalationPolicy {
    fn default() -> Self {
        Self {
            notify_channels: vec!["bridge".into()],
            auto_acknowledge_after_secs: None,
            critical_threshold: 3,
        }
    }
}

/// Manifest for a single room in the fleet.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoomManifest {
    pub id: String,
    pub name: String,
    pub connection: ConnectionConfig,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub escalation: EscalationPolicy,
}

/// Fleet configuration loaded from manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FleetConfig {
    pub fleet_name: String,
    pub vessel: Option<String>,
    pub rooms: Vec<RoomManifest>,
    #[serde(default)]
    pub escalation: EscalationPolicy,
}

impl FleetConfig {
    /// Parse config from JSON string.
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|e| format!("invalid fleet manifest: {}", e))
    }

    /// Load config from a file path.
    pub fn from_file(path: &str) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {}", path, e))?;
        Self::from_json(&content)
    }

    /// Validate that all room IDs are unique.
    pub fn validate(&self) -> Result<(), String> {
        let mut seen = HashMap::new();
        for room in &self.rooms {
            if let Some(prev) = seen.insert(&room.id, &room.name) {
                return Err(format!(
                    "duplicate room id '{}': '{}' and '{}'",
                    room.id, prev, room.name
                ));
            }
        }
        if self.fleet_name.is_empty() {
            return Err("fleet_name cannot be empty".into());
        }
        Ok(())
    }

    /// Get a room manifest by ID.
    pub fn get_room(&self, id: &str) -> Option<&RoomManifest> {
        self.rooms.iter().find(|r| r.id == id)
    }

    /// Create a fishing boat manifest for testing.
    pub fn fishing_boat_manifest() -> Self {
        let json = r#"{
            "fleet_name": "Fishing Vessel Northern Star",
            "vessel": "MMSI-123456789",
            "rooms": [
                {
                    "id": "engine-room",
                    "name": "Main Engine Room",
                    "connection": { "host": "192.168.1.10", "port": 9090 },
                    "tags": ["critical", "engine"],
                    "escalation": { "notify_channels": ["bridge", "engineer"], "critical_threshold": 2 }
                },
                {
                    "id": "bridge",
                    "name": "Bridge Systems",
                    "connection": { "host": "192.168.1.20", "port": 9090 },
                    "tags": ["critical", "navigation"],
                    "escalation": { "notify_channels": ["captain"], "critical_threshold": 1 }
                },
                {
                    "id": "bilge",
                    "name": "Bilge Monitoring",
                    "connection": { "host": "192.168.1.30", "port": 9090 },
                    "tags": ["safety"],
                    "escalation": { "notify_channels": ["bridge", "engineer"], "critical_threshold": 2 }
                },
                {
                    "id": "cargo-hold",
                    "name": "Cargo Hold",
                    "connection": { "host": "192.168.1.40", "port": 9090 },
                    "tags": ["cargo"]
                }
            ],
            "escalation": { "notify_channels": ["bridge"], "critical_threshold": 3 }
        }"#;
        Self::from_json(json).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_manifest_json() {
        let config = FleetConfig::fishing_boat_manifest();
        assert_eq!(config.fleet_name, "Fishing Vessel Northern Star");
        assert_eq!(config.rooms.len(), 4);
        assert_eq!(config.rooms[0].id, "engine-room");
        assert_eq!(config.rooms[1].connection.host, "192.168.1.20");
    }

    #[test]
    fn validate_room_references() {
        let mut config = FleetConfig::fishing_boat_manifest();
        assert!(config.validate().is_ok());

        // Duplicate IDs should fail
        config.rooms.push(config.rooms[0].clone());
        assert!(config.validate().is_err());
    }

    #[test]
    fn load_fishing_boat_manifest() {
        let config = FleetConfig::fishing_boat_manifest();
        assert!(config.validate().is_ok());
        assert_eq!(config.vessel, Some("MMSI-123456789".into()));
        assert_eq!(config.rooms[0].tags, vec!["critical", "engine"]);
    }

    #[test]
    fn empty_fleet_name_fails_validation() {
        let config = FleetConfig {
            fleet_name: "".into(),
            vessel: None,
            rooms: vec![],
            escalation: EscalationPolicy::default(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn get_room_by_id() {
        let config = FleetConfig::fishing_boat_manifest();
        assert!(config.get_room("bridge").is_some());
        assert!(config.get_room("nonexistent").is_none());
        assert_eq!(config.get_room("bilge").unwrap().name, "Bilge Monitoring");
    }

    #[test]
    fn escalation_defaults() {
        let policy = EscalationPolicy::default();
        assert_eq!(policy.notify_channels, vec!["bridge"]);
        assert_eq!(policy.critical_threshold, 3);
    }

    #[test]
    fn connection_config_defaults() {
        let cc = ConnectionConfig::new("localhost", 8080);
        assert_eq!(cc.timeout_secs, 30);
        assert_eq!(cc.reconnect_interval_secs, 5);
    }
}
