# plato-fleet-manager

Fleet orchestration for **Plato room engine blocks** across devices. Discovers rooms, manages connections, routes commands, aggregates tick streams, and provides fleet-wide monitoring.

Built for maritime deployments — fishing vessels, research ships, offshore platforms — where multiple Plato engine blocks monitor different compartments and need a unified management layer.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     FleetManager                         │
│  ┌──────────┐ ┌───────────┐ ┌──────────┐ ┌───────────┐ │
│  │Room Conn │ │Room Conn  │ │Room Conn │ │Room Conn  │ │
│  │Engine Rm │ │ Bridge    │ │ Bilge    │ │ Cargo     │ │
│  └────┬─────┘ └─────┬─────┘ └────┬─────┘ └─────┬─────┘ │
│       │             │            │              │        │
│  ┌────▼─────────────▼────────────▼──────────────▼─────┐ │
│  │                  TickAggregator                      │ │
│  │  • Merge tick streams by timestamp                  │ │
│  │  • Detect cross-room correlations                   │ │
│  │  • Fleet-wide statistics                            │ │
│  └──────────────────────────┬──────────────────────────┘ │
│                             │                            │
│  ┌──────────────────────────▼──────────────────────────┐ │
│  │                  FleetMonitor                         │ │
│  │  • Room uptime tracking                              │ │
│  │  • Stale tick detection                              │ │
│  │  • Aggregate alarm state (Green/Yellow/Red)          │ │
│  └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
         │                              │
    ┌────▼────┐                    ┌─────▼─────┐
    │  TCP    │                    │  oxide-   │
    │ Plato   │                    │  fleet    │
    │ Engine  │                    │  dashboard│
    └─────────┘                    └───────────┘
```

## Modules

| Module | Purpose |
|--------|---------|
| `fleet` | `FleetManager` — top-level orchestrator, room lifecycle, command routing |
| `connection` | `RoomConnection` / `MockRoomConnection` — TCP or mock connection to a single room |
| `aggregator` | `TickAggregator` — merges tick streams, detects cross-room correlations |
| `monitor` | `FleetMonitor` — health status (🟢/🟡/🔴), stale room detection |
| `config` | `FleetConfig` — fleet manifest parsing, validation, escalation policies |

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
plato-fleet-manager = "0.1"
```

### Load a Fleet Manifest

```rust
use plato_fleet_manager::FleetConfig;

// Load from file
let config = FleetConfig::from_file("fleet-manifest.json").unwrap();

// Or parse directly
let config = FleetConfig::from_json(r#"{
    "fleet_name": "My Vessel",
    "rooms": [
        {
            "id": "engine",
            "name": "Engine Room",
            "connection": { "host": "192.168.1.10", "port": 9090 }
        }
    ]
}"#).unwrap();

config.validate().unwrap();
```

### Run a Fleet

```rust
use plato_fleet_manager::{FleetManager, FleetConfig};

#[tokio::main]
async fn main() {
    let config = FleetConfig::fishing_boat_manifest();
    let manager = FleetManager::new(config);

    // Connect to all rooms
    manager.run().await.unwrap();

    // Broadcast a command to every room
    let results = manager.broadcast_command("tick").await;
    for result in &results {
        println!("{:?}", result);
    }

    // Send to a specific room
    let response = manager.send_command("engine-room", "history").await;
    println!("Engine room: {:?}", response);

    // Get fleet health
    let health = manager.get_fleet_health().await;
    println!("Fleet status: {}", health.status);
    println!("Healthy rooms: {}/{}", health.healthy_rooms, health.total_rooms);
}
```

## API Reference

### FleetManager

The central orchestrator. Manages room connections, routes commands, and provides fleet-wide visibility.

| Method | Description |
|--------|-------------|
| `new(config)` | Create a manager from a fleet config |
| `add_room(id, config)` | Register a room with TCP connection parameters |
| `add_mock_room(id, conn)` | Register a mock room (for testing) |
| `remove_room(id)` | Disconnect and remove a room |
| `run()` | Connect and subscribe to all rooms |
| `broadcast_command(cmd)` | Send a command to every connected room |
| `send_command(room_id, cmd)` | Send a command to a specific room |
| `get_room_states()` | Snapshot of all room states |
| `get_fleet_health()` | Aggregate health metrics |
| `get_statistics()` | Fleet-wide sensor statistics |
| `ingest_tick(tick)` | Feed a tick into the aggregator |

### RoomConnection

Manages a TCP connection to a single Plato room engine block.

| Method | Description |
|--------|-------------|
| `new(id, host, port)` | Create a new connection |
| `connect()` | Establish TCP connection |
| `send(cmd)` | Send a command, await response |
| `subscribe()` | Begin streaming ticks |
| `unsubscribe()` | Stop streaming ticks |
| `is_healthy()` | Connection status check |
| `last_tick()` | Most recent tick received |

### TickAggregator

Merges tick streams from multiple rooms into a unified view.

| Method | Description |
|--------|-------------|
| `new()` | Create an empty aggregator |
| `ingest(tick)` | Feed a tick into the aggregator |
| `merge_aligned(tolerance_ms)` | Group ticks by timestamp across rooms |
| `detect_correlations()` | Check for cross-room correlations |
| `statistics()` | Compute fleet-wide sensor stats |
| `latest()` | Get the latest tick from each room |
| `add_engine_bilge_correlation()` | Register built-in engine+bilge correlation rule |

### FleetMonitor

Tracks room health and generates alerts.

| Method | Description |
|--------|-------------|
| `new()` | Create a monitor with defaults |
| `compute_health(rooms)` | Compute aggregate fleet health |
| `detect_stale_rooms(rooms)` | Find rooms that stopped ticking |
| `with_tick_timeout(secs)` | Set the stale tick threshold |
| `with_red_threshold(ratio)` | Set the down-room ratio for RED status |

### FleetConfig

Parses and validates fleet manifests.

| Method | Description |
|--------|-------------|
| `from_json(json)` | Parse from JSON string |
| `from_file(path)` | Load from a file |
| `validate()` | Check for duplicate IDs, empty names |
| `get_room(id)` | Look up a room by ID |
| `fishing_boat_manifest()` | Built-in test manifest |

## Fleet Manifest Format

```json
{
    "fleet_name": "Fishing Vessel Northern Star",
    "vessel": "MMSI-123456789",
    "rooms": [
        {
            "id": "engine-room",
            "name": "Main Engine Room",
            "connection": {
                "host": "192.168.1.10",
                "port": 9090,
                "timeout_secs": 30,
                "reconnect_interval_secs": 5
            },
            "tags": ["critical", "engine"],
            "escalation": {
                "notify_channels": ["bridge", "engineer"],
                "critical_threshold": 2
            }
        }
    ],
    "escalation": {
        "notify_channels": ["bridge"],
        "critical_threshold": 3
    }
}
```

## Health Status

The fleet monitor computes an overall status based on room connectivity and ticking:

| Status | Condition |
|--------|-----------|
| 🟢 **Green** | All rooms connected, ticking, no alarms |
| 🟡 **Yellow** | Some rooms down or alarms present |
| 🔴 **Red** | Majority of rooms down or all rooms offline |

## Cross-Room Correlations

The aggregator can detect patterns across rooms. Built-in rules:

- **Engine Temperature + Bilge Water Rising**: When engine room temperature exceeds 90°C and bilge water level exceeds 50%, a critical correlation event fires. This catches scenarios where a cooling system failure leads to flooding.

Custom correlation rules can be added by extending the `TickAggregator`.

## Fishing Boat Deployment

A typical fishing vessel has 4–6 rooms monitored by Plato engine blocks:

```
┌──────────────────────────────────────────────┐
│           F/V Northern Star                   │
│                                               │
│  ┌─────────┐  ┌─────────┐  ┌──────────────┐ │
│  │ Bridge  │  │ Engine  │  │   Bilge      │ │
│  │ Nav+Com │  │  Room   │  │  Monitoring  │ │
│  │ :9090   │  │ :9090   │  │   :9090      │ │
│  └─────────┘  └─────────┘  └──────────────┘ │
│                                               │
│  ┌──────────┐  ┌───────────┐                  │
│  │  Cargo   │  │  Galley   │                  │
│  │  Hold    │  │  + Refrig │                  │
│  │  :9090   │  │   :9090   │                  │
│  └──────────┘  └───────────┘                  │
│                                               │
│  ┌─────────────────────────────────────────┐ │
│  │     plato-fleet-manager (this crate)    │ │
│  │     Runs on bridge workstation          │ │
│  └─────────────────────────────────────────┘ │
└──────────────────────────────────────────────┘
```

Each room runs a `plato-engine-block` instance connected via the ship's LAN. The fleet manager runs on the bridge workstation, aggregating all rooms into a single view.

### Typical Sensor Layout

| Room | Sensors | Alarms |
|------|---------|--------|
| Engine Room | Temperature, RPM, oil pressure, coolant level | Overheat, low oil, high vibration |
| Bridge | GPS, heading, depth, wind speed | Navigation deviation, shallow water |
| Bilge | Water level, pump status, flow rate | High water, pump failure |
| Cargo Hold | Temperature, humidity, door status | Temp excursion, door open |
| Galley | Fridge temp, freezer temp, fire suppression | Temp excursion, fire alarm |

## Connection to Other Plato Crates

- **plato-engine-block**: The room-level engine that this crate manages. Each room runs one instance.
- **oxide-fleet**: The dashboard/visualization layer that consumes fleet health data from this crate.
- **plato-protocol**: The shared text protocol used for room communication (tick, history, actuator, subscribe, help).

```
plato-protocol  ← shared text protocol
       │
       ├── plato-engine-block  ← per-room, reads sensors
       │        │
       │        └── plato-fleet-manager  ← this crate, orchestrates
       │                 │
       │                 └── oxide-fleet  ← dashboard/visualization
```

## Testing

All tests use mock connections — no real TCP required:

```bash
cargo test
```

28 tests covering:
- FleetManager creation, room lifecycle, command routing
- RoomConnection connect/send/subscribe/unsubscribe
- TickAggregator merging, correlations, different tick rates
- FleetMonitor health states, stale detection, alarm aggregation
- FleetConfig parsing, validation, manifest loading
- Full integration: fleet lifecycle from add to anomaly detection

## License

MIT
