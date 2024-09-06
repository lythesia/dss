#![recursion_limit = "256"]
#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[allow(unused_imports)]
#[macro_use]
extern crate prost_derive;

pub mod kvraft;
mod proto;
pub mod raft;

/// A place holder for suppressing unused_variables warning.
#[allow(unused)]
fn your_code_here<T>(_: T) -> ! {
    unimplemented!()
}
