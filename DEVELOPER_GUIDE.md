# Developer Guide — plato-fleet-manager

> Architecture deep-dive, module walkthrough, extension points, and contributing guide for multi-room orchestration.

---

## Architecture Overview

`plato-fleet-manager` orchestrates multiple Plato room engine blocks across devices. It discovers rooms, manages TCP connections, routes commands, aggregates tick streams, and provides fleet-wide health monitoring. Built for maritime deployments — fishing vessels, research ships, offshore platforms — where multiple Plato engine blocks monitor different compartments.

### Topology

```
┌───────────────────────────────────────────────────────────┐
│                     FleetManager                           │
│                                                           │
│  RoomConnection × N ──▶ TickAggregator ──▶ FleetMonitor   │
│       (TCP/mock)         (merge/correlate)   (health/🟢🟡🔴) │
│                                                           │
│  FleetConfig (manifest) ──▶ validation ──▶ room registry  │
└───────────────────────────────────────────────────────────┘
```

The `FleetManager` is the top-level orchestrator. It owns:
- A collection of `RoomConnection`s (TCP or mock), each wrapping a single `plato-engine-block` instance
- A `TickAggregator` that merges tick streams from all rooms
- A `FleetMonitor` that computes aggregate health status

---

## Module-by-Module Walkthrough

### `fleet` — FleetManager

The central orchestrator. Responsibilities:

| Method | Purpose |
|--------|---------|
| `new(config)` | Create from a `FleetConfig` manifest |
| `add_room(id, config)` | Register a room with connection parameters |
| `add_mock_room(id, conn)` | Register a mock room (testing) |
| `remove_room(id)` | Disconnect and remove a room |
| `run()` | Connect and subscribe to all rooms |
| `broadcast_command(cmd)` | Send a command to every room |
| `send_command(room_id, cmd)` | Send to a specific room |
| `get_room_states()` | Snapshot all room states |
| `get_fleet_health()` | Aggregate health metrics |
| `get_statistics()` | Fleet-wide sensor stats |
| `ingest_tick(tick)` | Feed a tick into the aggregator |

**Extension point:** Add fleet-level behaviors (e.g., auto-scaling rooms, periodic health checks) by adding methods to `FleetManager` that use the existing connection/aggregator/monitor infrastructure.

### `connection` — RoomConnection & MockRoomConnection

Manages a TCP connection to a single Plato room engine block:

```rust
pub trait RoomConnectionTrait {
    fn connect(&mut self) -> Result<()>;
    fn send(&mut self, cmd: &str) -> Result<String>;
    fn subscribe(&mut self) -> Result<()>;
    fn unsubscribe(&mut self) -> Result<()>;
    fn is_healthy(&self) -> bool;
    fn last_tick(&self) -> Option<&Tick>;
    fn id(&self) -> &str;
}
```

Two implementations:
- **`RoomConnection`** — Real TCP connection to a `plato-engine-block` server
- **`MockRoomConnection`** — In-memory mock for testing (no network required)

**Tick struct:**

```rust
pub struct Tick {
    pub room_id: String,
    pub timestamp: u64,
    pub data: Vec<(String, f64)>,
}
```

**Extension point:** Add new connection types (WebSocket, MQTT, serial) by implementing `RoomConnectionTrait`. The fleet manager is agnostic to transport.

### `aggregator` — TickAggregator

Merges tick streams from multiple rooms into a unified view:

| Method | Purpose |
|--------|---------|
| `new()` | Create empty aggregator |
| `ingest(tick)` | Feed a tick into the aggregator |
| `merge_aligned(tolerance_ms)` | Group ticks by timestamp across rooms |
| `detect_correlations()` | Check for cross-room correlation events |
| `statistics()` | Fleet-wide sensor statistics (min, max, avg, count) |
| `latest()` | Latest tick from each room |
| `add_engine_bilge_correlation()` | Register built-in engine+bilge rule |

**Cross-room correlations** are pattern-based rules. The built-in rule detects when engine temperature exceeds 90°C AND bilge water level exceeds 50% — a cooling system failure leading to flooding. Custom correlation rules follow the same pattern: define a condition that checks values from multiple rooms simultaneously.

**Extension point:** Add custom correlation rules by creating new functions that check multi-room conditions. The correlation system is event-driven — rules fire when relevant ticks arrive.

### `monitor` — FleetMonitor

Tracks room health and generates alerts:

| Status | Condition |
|--------|-----------|
| 🟢 Green | All rooms connected, ticking, no alarms |
| 🟡 Yellow | Some rooms down or alarms present |
| 🔴 Red | Majority of rooms down or all offline |

Configuration:
- `with_tick_timeout(secs)` — Threshold for considering a room "stale" (default: 30s)
- `with_red_threshold(ratio)` — Fraction of down rooms that triggers RED (default: 0.5)

The monitor is stateless — it computes health on demand from current room states rather than maintaining internal state. This makes it safe to call from multiple contexts.

**Extension point:** Add custom health rules (e.g., "RED if any critical room is down regardless of total count") by extending `compute_health()`.

### `config` — FleetConfig

Parses and validates fleet manifests (JSON):

```rust
pub struct FleetConfig {
    pub fleet_name: String,
    pub vessel: Option<String>,
    pub rooms: Vec<RoomManifest>,
    pub escalation: Option<EscalationPolicy>,
}

pub struct RoomManifest {
    pub id: String,
    pub name: String,
    pub connection: ConnectionConfig,
    pub tags: Vec<String>,
    pub escalation: Option<EscalationPolicy>,
}
```

Validation checks:
- No duplicate room IDs
- Non-empty room names
- Valid connection parameters

**Extension point:** Add new config fields (e.g., sensor mappings, alarm templates) by extending the structs and adding validation rules.

---

## Testing Strategy

28 tests using mock connections — no real TCP required:

- **FleetManager:** Creation, room lifecycle, command routing, broadcast
- **RoomConnection:** Connect/send/subscribe/unsubscribe, health checks
- **TickAggregator:** Merging, correlations, different tick rates, statistics
- **FleetMonitor:** Green/Yellow/Red states, stale detection, alarm aggregation
- **FleetConfig:** Parsing, validation, manifest loading, error cases
- **Integration:** Full fleet lifecycle from add to anomaly detection

Run with:

```bash
cargo test
```

All tests use `MockRoomConnection` to simulate room behavior without network dependencies.

---

## Contributing Guide

### Adding a New Connection Type

1. Implement `RoomConnectionTrait` for your transport (WebSocket, MQTT, serial, etc.)
2. Add a constructor that takes connection parameters
3. Implement the health check appropriate for your transport
4. Add tests using a mock server or test harness

### Adding a New Correlation Rule

1. Define the correlation condition (which rooms, which sensors, which thresholds)
2. Add a function in `aggregator.rs` that evaluates the condition
3. Register the rule in `detect_correlations()` or via an `add_*_correlation()` method
4. Test with synthetic tick data that triggers and doesn't trigger the rule

### Adding a Health Rule

1. Define the condition (e.g., "RED if engine room is down for >5 minutes")
2. Add the rule to `FleetMonitor::compute_health()`
3. Configure thresholds via builder methods on `FleetMonitor`
4. Test all status transitions

### Code Style

- All async functions return `Result` with descriptive error types
- Use `MockRoomConnection` for all tests — never require real TCP
- Fleet manifests are JSON — maintain backward compatibility when adding fields
- Health status computation should be stateless (compute from current data, not accumulated state)

---

## Project Structure

```
plato-fleet-manager/
├── Cargo.toml
├── README.md
├── DEVELOPER_GUIDE.md
├── src/
│   ├── lib.rs         # Re-exports, crate docs
│   ├── fleet.rs       # FleetManager, RoomState
│   ├── connection.rs  # RoomConnection, MockRoomConnection, Tick
│   ├── aggregator.rs  # TickAggregator, CorrelationEvent
│   ├── monitor.rs     # FleetMonitor, FleetHealth, HealthStatus
│   └── config.rs      # FleetConfig, RoomManifest, ConnectionConfig
└── tests/
    └── (if separated)
```

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Trait-based connections | Pluggable transports (TCP, mock, future: WebSocket/MQTT) |
| Stateless health monitor | Safe to call from multiple contexts; no synchronization issues |
| JSON manifests | Human-readable, version-controllable, editable on deployment |
| Mock connections for tests | Fast, deterministic, no network dependencies |
| Async throughout | TCP I/O is inherently async; tokio is the standard Rust async runtime |
| Cross-room correlations | Multi-room patterns catch issues invisible to single-room monitoring |
