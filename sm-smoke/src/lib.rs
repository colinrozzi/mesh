//! Throwaway compose-smoke consumer (step-1 de-risk, not part of v0).
//!
//! It IMPORTS the `state-machine` interface and calls through it — the direction the
//! mesh node will eventually take (node imports a composed SM and calls it on the
//! fold). `packr compose`-ing this with `sm-trivial` proves the importer<->exporter
//! link works and the interface hashes match. Delete once the real node imports the SM.

#![no_std]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use packr_guest::{export, import_from};

packr_guest::setup_guest!();

packr_guest::pack_types! {
    imports {
        // The COMPLETE state-machine interface (the hash covers all four).
        state-machine {
            initial-state: func() -> list<u8>,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: list<u8>, state: list<u8>) -> list<u8>,
            members: func(state: list<u8>) -> list<list<u8>>,
        }
    }
    exports {
        smoke {
            // Fold one event through the composed SM and return the projected members.
            probe: func() -> list<list<u8>>,
        }
    }
}

#[import_from("state-machine", name = "initial-state")]
fn sm_initial_state() -> Vec<u8>;

#[import_from("state-machine", name = "validate")]
fn sm_validate(id: Vec<u8>, author: Vec<u8>, timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Result<bool, String>;

#[import_from("state-machine", name = "apply")]
fn sm_apply(id: Vec<u8>, author: Vec<u8>, timestamp: u64, payload: Vec<u8>, state: Vec<u8>) -> Vec<u8>;

#[import_from("state-machine", name = "members")]
fn sm_members(state: Vec<u8>) -> Vec<Vec<u8>>;

/// initial-state -> validate -> apply -> members, exactly the node's fold shape.
#[export]
fn probe() -> Vec<Vec<u8>> {
    let s0 = sm_initial_state();
    let author = alloc::vec![7u8; 32];
    if sm_validate(alloc::vec![1u8; 32], author.clone(), 0, Vec::new(), s0.clone()).is_ok() {
        let s1 = sm_apply(alloc::vec![1u8; 32], author, 0, Vec::new(), s0);
        return sm_members(s1);
    }
    Vec::new()
}
