// This directory's extension (hand-written; ADR-0031). The generated mod.rs (lib.rs for
// src/) pulls it in with include!, so its lines resolve against this directory.

// Guarded composition (read-only CRUD + validated write verbs) — the recommended mount.
pub mod guarded_routes;
pub use guarded_routes::create_guarded_lifecycle_routes;
