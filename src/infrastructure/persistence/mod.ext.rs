// This directory's extension (hand-written; ADR-0031). The generated mod.rs (lib.rs for
// src/) pulls it in with include!, so its lines resolve against this directory.

// The statutory severance set (effective-dated) and the settlement settings.
pub mod severance_params_repository;
