//! Core task engine.
//!
//! Typestate enforces legal task transitions at compile time: a pending task
//! can start, but only a running task can complete, fail, or be requeued. The
//! handler layer uses two traits to balance typed payloads for implementors
//! with dyn-compatible storage in the registry.

pub mod core;
pub mod handler;
pub mod registry;
pub mod task;
