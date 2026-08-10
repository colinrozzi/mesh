// The counter network's data schema — the shared contract between the SM (which folds it)
// and the system (which constructs commands and reads state). Generated on each side via
// `wit!`; there is no shared Rust crate. Drift is caught at compose (structural hashing).

variant cmd {
    inc(s64),
    reset,
}

record counter-state {
    count: s64,
    ops: u64,
}

world counter {}
