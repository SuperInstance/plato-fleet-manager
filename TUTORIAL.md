# Tutorial — plato-fleet-manager

> **By the end of this tutorial, you will have built a complete fleet orchestration system** — loading a vessel manifest, connecting to rooms, routing commands, detecting cross-room anomalies, and monitoring fleet health.

---

## Prerequisites

- Rust 1.70+
- Basic familiarity with `plato-engine-block` (rooms, ticks, alarms)
- 20 minutes

## Step 1: Create the Project

```bash
cargo new fleet-monitor
cd fleet-monitor
```

```toml
[dependencies]
plato-fleet-manager = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Step 2: Load a Fleet Manifest

A fleet manifest describes all rooms and their connection parameters:

```rust
use plato_fleet_manager::FleetConfig;

fn main() {
    let config = FleetConfig::from_json(r#"{
        "fleet_name": "F/V Northern Star",
        "vessel": "MMSI-123456789",
        "rooms": [
            {
                "id": "engine-room",
                "name": "Main Engine Room",
                "connection": {
                    "host": "192.168.1.10",
                    "port": 9090
                }
            },
            {
                "id": "bilge",
                "name": "Bilge Monitoring",
                "connection": {
                    "host": "192.168.1.11",
                    "port": 9090
                }
            }
        ]
    }"#).unwrap();

    println!("Fleet: {}", config.fleet_name);
    for room in &config.rooms {
        println!("  {} ({}) → {}:{}",
            room.name, room.id,
            room.connection.host, room.connection.port);
    }

    config.validate().unwrap();
    println!("✓ Manifest valid");
}
```

**What happened:** `FleetConfig::from_json()` parses the manifest. `validate()` checks for duplicate IDs and empty names. Each room has an ID, display name, and TCP connection config.

## Step 3: Create Mock Rooms for Testing

Real rooms need TCP connections. For development, use mock rooms:

```rust
use plato_fleet_manager::{FleetManager, FleetConfig, MockRoomConnection};
use plato_fleet_manager::connection::Tick;

fn main() {
    let mut manager = FleetManager::new(FleetConfig::empty());

    // Create mock rooms
    let mut engine = MockRoomConnection::new("engine-room");
    engine.set_response("tick", "tick 0 @ 0.0s\n  temp = 85.0\n  pressure = 55.0");
    engine.set_response("history", "tick 0: temp=85.0");

    let mut bilge = MockRoomConnection::new("bilge");
    bilge.set_response("tick", "tick 0 @ 0.0s\n  water_level = 20.0");

    manager.add_mock_room("engine-room", engine);
    manager.add_mock_room("bilge", bilge);

    println!("Rooms: {:?}", manager.get_room_states().keys().collect::<Vec<_>>());
}
```

**What happened:** `MockRoomConnection` simulates room responses without TCP. You pre-configure responses for specific commands. This lets you test fleet logic without running actual engine blocks.

## Step 4: Broadcast Commands to All Rooms

Send the same command to every room:

```rust
use plato_fleet_manager::{FleetManager, FleetConfig, MockRoomConnection};
use plato_fleet_manager::connection::RoomConnectionTrait;

#[tokio::main]
async fn main() {
    let mut manager = FleetManager::new(FleetConfig::empty());

    let mut engine = MockRoomConnection::new("engine-room");
    engine.set_response("tick", "temp=85.0");
    let mut bilge = MockRoomConnection::new("bilge");
    bilge.set_response("tick", "water=20.0");

    manager.add_mock_room("engine-room", engine);
    manager.add_mock_room("bilge", bilge);

    // Broadcast a tick command
    let results = manager.broadcast_command("tick").await;
    println!("Broadcast results: {:?}", results);

    // Send to a specific room
    let response = manager.send_command("engine-room", "history 5").await;
    println!("Engine room: {:?}", response);
}
```

**What happened:** `broadcast_command()` sends the command to every registered room and collects results. `send_command()` targets a specific room by ID. Both return `Result<String>` per room.

## Step 5: Aggregate Tick Streams

The `TickAggregator` merges ticks from all rooms into a unified view:

```rust
use plato_fleet_manager::TickAggregator;
use plato_fleet_manager::connection::Tick;

fn main() {
    let mut aggregator = TickAggregator::new();

    // Simulate ticks arriving from different rooms
    aggregator.ingest(Tick {
        room_id: "engine-room".to_string(),
        timestamp: 1000,
        data: vec![
            ("coolant_temp".to_string(), 92.0),
            ("oil_pressure".to_string(), 55.0),
        ],
    });

    aggregator.ingest(Tick {
        room_id: "bilge".to_string(),
        timestamp: 1000,
        data: vec![
            ("water_level".to_string(), 45.0),
        ],
    });

    // Fleet-wide statistics
    let stats = aggregator.statistics();
    println!("Fleet stats: {:?}", stats);

    // Latest from each room
    let latest = aggregator.latest();
    for (room_id, tick) in latest {
        println!("{}: {:?}", room_id, tick.data);
    }
}
```

**What happened:** Ticks from different rooms are fed into the aggregator. `statistics()` computes fleet-wide min/max/avg for all sensors. `latest()` returns the most recent tick from each room.

## Step 6: Detect Cross-Room Correlations

The aggregator can detect patterns across rooms:

```rust
use plato_fleet_manager::TickAggregator;
use plato_fleet_manager::connection::Tick;

fn main() {
    let mut aggregator = TickAggregator::new();
    aggregator.add_engine_bilge_correlation();

    // Simulate a crisis: engine overheating AND bilge rising
    aggregator.ingest(Tick {
        room_id: "engine-room".to_string(),
        timestamp: 1000,
        data: vec![("coolant_temp".to_string(), 95.0)],
    });

    aggregator.ingest(Tick {
        room_id: "bilge".to_string(),
        timestamp: 1000,
        data: vec![("water_level".to_string(), 55.0)],
    });

    let correlations = aggregator.detect_correlations();
    for event in correlations {
        println!("⚠️  Correlation: {} — {}",
            event.rule, event.description);
    }
}
```

**What happened:** The built-in engine+bilge correlation detects when coolant temp > 90°C AND bilge water > 50%. This catches scenarios where a cooling system failure leads to flooding — a pattern invisible to single-room monitoring.

## Step 7: Monitor Fleet Health

```rust
use plato_fleet_manager::FleetMonitor;
use plato_fleet_manager::connection::Tick;
use std::collections::HashMap;

fn main() {
    let monitor = FleetMonitor::new()
        .with_tick_timeout(30)     // 30s stale threshold
        .with_red_threshold(0.5);  // RED if >50% rooms down

    // Simulate room states (healthy)
    let mut rooms = HashMap::new();
    // ... populate with room states ...

    let health = monitor.compute_health(&rooms);
    println!("Fleet status: {} ({}/{})",
        health.status,
        health.healthy_rooms,
        health.total_rooms);
}
```

The health status follows a traffic light model:
- 🟢 **Green** — All rooms connected, ticking, no alarms
- 🟡 **Yellow** — Some rooms down or alarms present
- 🔴 **Red** — Majority of rooms down

**Congratulations!** You've built a complete fleet monitoring system — from manifest loading to cross-room anomaly detection to health tracking. This is the same architecture used on fishing vessels monitoring engine rooms, bilges, and cargo holds.

## What's Next?

- Connect to real `plato-engine-block` instances via TCP
- Add custom correlation rules for your specific vessel layout
- Feed ternary room state from `plato-ternary-bridge` for compressed fleet monitoring
- Use `plato-music-sync` to coordinate tick rates across the fleet
- Build a dashboard with `oxide-fleet` for real-time visualization
