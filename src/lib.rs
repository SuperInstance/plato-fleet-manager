//! # plato-fleet-manager
//!
//! Fleet orchestration for Plato room engine blocks across devices.
//!
//! This crate provides a `FleetManager` that connects to multiple Plato room
//! engine blocks (local or remote via TCP), discovers rooms, manages connections,
//! routes commands, aggregates tick streams, and provides fleet-wide monitoring.
//!
//! ## Architecture
//!
//! - `FleetManager` — top-level orchestrator managing multiple rooms
//! - `RoomConnection` — TCP connection to a single Plato room
//! - `TickAggregator` — merges tick streams from multiple rooms
//! - `FleetMonitor` — health monitoring and alerting
//! - `FleetConfig` — fleet manifest parsing and validation
//!
//! ## Quick Start
//!
//! ```no_run
//! use plato_fleet_manager::{FleetManager, FleetConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = FleetConfig::from_file("fleet-manifest.json").unwrap();
//!     let mut manager = FleetManager::new(config);
//!     manager.run().await.unwrap();
//! }
//! ```

mod aggregator;
mod config;
mod connection;
mod fleet;
mod monitor;

pub use aggregator::{CorrelationEvent, TickAggregator};
pub use config::{ConnectionConfig, EscalationPolicy, FleetConfig, RoomManifest};
pub use connection::{MockRoomConnection, RoomConnection, RoomConnectionTrait, Tick};
pub use fleet::{FleetManager, RoomState};
pub use monitor::{FleetHealth, FleetMonitor, HealthStatus};
