//! Atlas Queue core library.
//!
//! The engine uses typestate to make illegal task lifecycle transitions a
//! compile-time error. Handlers use a two-trait pattern: implementors get
//! type-safe payloads through `TaskHandler`, while the registry stores
//! dyn-compatible `ErasedHandler` trait objects.

pub mod engine;
pub mod pool;
