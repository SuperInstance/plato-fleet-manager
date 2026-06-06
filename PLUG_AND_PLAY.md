# Plug & Play — plato-fleet-manager

> Copy these templates. Change the room addresses and commands. You're orchestrating.

---

## Pattern 1: Load Manifest + Connect All Rooms

Load a fleet manifest and connect to every room.

```rust
use plato_fleet_manager::{FleetManager, FleetConfig};

#[tokio::main]
async fn main() {
    // ↓ Change this to your manifest path ↓
    let config = FleetConfig::from_file("fleet-manifest.json")
        .expect("Failed to load manifest");

    let mut manager = FleetManager::new(config);

    // Connect to all rooms
    manager.run().await.expect("Failed to connect");

    // Check health
    let health = manager.get_fleet_health().await;
    println!("Fleet: {} — {}/{} rooms healthy",
        health.status, health.healthy_rooms, health.total_rooms);

    // Broadcast a command
    let results = manager.broadcast_command("tick").await;
    for (room_id, result) in results {
        println!("{}: {:?}", room_id, result);
    }
}
```

**Change:** manifest file path. Manifest format:

```json
{
    "fleet_name": "My Vessel",
    "rooms": [
        {
            "id": "engine-room",
            "name": "Engine Room",
            "connection": { "host": "192.168.1.10", "port": 9090 }
        }
    ]
}
```

---

## Pattern 2: Fleet Command Routing

Send commands to specific rooms or broadcast to all.

```rust
use plato_fleet_manager::{FleetManager, FleetConfig, MockRoomConnection};

#[tokio::main]
async fn main() {
    let mut manager = FleetManager::new(FleetConfig::empty());

    // ↓ Add your rooms (mock shown, use real TCP in production) ↓
    let mut engine = MockRoomConnection::new("engine");
    engine.set_response("tick", "temp=92.0 oil=55.0");
    engine.set_response("history", "last 5 ticks...");

    manager.add_mock_room("engine", engine);

    // Broadcast to all rooms
    let results = manager.broadcast_command("tick").await;

    // Target specific room
    let resp = manager.send_command("engine", "history 10").await;
    println!("Engine response: {:?}", resp);

    // Get all room states
    for (id, state) in manager.get_room_states() {
        println!("{}: {:?}", id, state);
    }
}
```

**Change:** room IDs, commands, connection type (swap `MockRoomConnection` for `RoomConnection`).

---

## Pattern 3: Fleet Health + Cross-Room Detection

Monitor fleet health and detect correlations between rooms.

```rust
use plato_fleet_manager::{FleetManager, FleetConfig, TickAggregator, FleetMonitor};
use plato_fleet_manager::connection::Tick;

fn main() {
    // Aggregator with built-in correlation rules
    let mut aggregator = TickAggregator::new();
    aggregator.add_engine_bilge_correlation();

    // ↓ Feed ticks from your rooms ↓
    aggregator.ingest(Tick {
        room_id: "engine".into(),
        timestamp: 1000,
        data: vec![("coolant_temp".into(), 95.0)],
    });
    aggregator.ingest(Tick {
        room_id: "bilge".into(),
        timestamp: 1000,
        data: vec![("water_level".into(), 55.0)],
    });

    // Detect cross-room patterns
    for event in aggregator.detect_correlations() {
        println!("⚠️  {}", event.description);
    }

    // Fleet-wide stats
    let stats = aggregator.statistics();
    println!("Sensors tracked: {}", stats.len());

    // Health monitor
    let monitor = FleetMonitor::new()
        .with_tick_timeout(30)    // ↓ Change stale threshold ↓
        .with_red_threshold(0.5); // ↓ Change RED trigger ratio ↓
}
```

**Change:** tick data sources, correlation rules, health thresholds.

---

## Quick Reference

| What | API | Example |
|------|-----|---------|
| Load manifest | `FleetConfig::from_file(path)` | JSON fleet manifest |
| Parse manifest | `FleetConfig::from_json(str)` | From string |
| Create manager | `FleetManager::new(config)` | From fleet config |
| Add mock room | `manager.add_mock_room(id, conn)` | For testing |
| Connect all | `manager.run().await` | TCP to all rooms |
| Broadcast | `manager.broadcast_command("tick").await` | All rooms |
| Send to room | `manager.send_command("engine", "tick").await` | One room |
| Fleet health | `manager.get_fleet_health().await` | 🟢🟡🔴 status |
| Room states | `manager.get_room_states()` | All room snapshots |
| Ingest tick | `aggregator.ingest(tick)` | Feed aggregator |
| Correlations | `aggregator.detect_correlations()` | Cross-room patterns |
| Statistics | `aggregator.statistics()` | Fleet-wide sensor stats |
| Health config | `monitor.with_tick_timeout(secs)` | Stale threshold |
