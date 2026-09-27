//! Capa P2P FHS sobre rust-libp2p: WSS+TLS, Noise, yamux, GossipSub,
//! Kademlia (cliente), identify, ping y streams `/fhs/v1/0.1.0`.

pub mod framing;
pub mod identity;
pub mod peer_cache;
pub mod tls;
pub mod wire;
