pub mod fhs {
    include!(concat!(env!("OUT_DIR"), "/fhs.v1.rs"));
}

pub const FHS_PROTOCOL: &str = "/fhs/v1/0.1.0";
pub const FHS_VERSION: &str = "1";
