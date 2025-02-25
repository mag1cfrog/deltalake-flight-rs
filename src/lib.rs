// Include generated code
pub mod deltaflight {
    // Contains messages + client/server code
    include!(concat!(env!("OUT_DIR"), "/deltaflight.rs"));
}

pub mod arrow {
    pub mod flight {
        pub mod protocol {
            include!(concat!(env!("OUT_DIR"), "/arrow.flight.protocol.rs"));
        }
    }
}

pub mod server;
pub mod utils;