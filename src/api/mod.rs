//! HTTP API for Atlas Queue.
//!
//! Routes, handlers, and DTOs live in separate modules so tests and `main` can
//! build the same Axum router without duplicating endpoint wiring.

pub mod dto;
pub mod handlers;
pub mod routes;

pub use routes::{AppState, router};
