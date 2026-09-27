//! Nodo rust-libp2p compatible con la red FHS de js-libp2p.
//!
//! Un actor es dueño del `Swarm` y atiende comandos por canal; el resto del
//! agente usa un [`NodeHandle`] clonable. Equivale a `createNavNode` +
//! `dialBootstraps` + `PeerCache` + `BidCollector` del Navigator TS.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use libp2p::{
    core::{upgrade::Version, ConnectedPoint},
    gossipsub, identify,
    kad::{self, store::MemoryStore},
    multiaddr::Protocol,
    noise, ping,
    swarm::{dial_opts::DialOpts, ConnectionId, NetworkBehaviour, SwarmEvent},
    websocket, yamux, Multiaddr, PeerId, StreamProtocol, Swarm, Transport,
};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch, Notify};

use crate::p2p::identity::NodeIdentity;
use crate::p2p::peer_cache::{now_ms, PeerCache};
use crate::p2p::wire::{self, TOPIC_MISSIONS_BID, TOPIC_NODES_ADVERTISE};
use crate::protocol::fhs::{Beacon, MissionBidMessage};

pub const ADVERTISE_INTERVAL: Duration = Duration::from_secs(30);
pub const ADVERTISE_TTL_SECONDS: i32 = 60;
const BOOTSTRAP_INITIAL_BACKOFF: Duration = Duration::from_secs(2);
const BOOTSTRAP_MAX_BACKOFF: Duration = Duration::from_secs(30);
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);
/// Espera tras una conexión nueva antes de anunciarse, para que GossipSub
/// intercambie suscripciones y el anuncio le llegue al peer recién llegado.
const ADVERTISE_ON_CONNECT_DELAY: Duration = Duration::from_secs(1);
/// Cada cuánto se vuelve a publicar el beacon en el DHT.
const DHT_REPUBLISH_INTERVAL: Duration = Duration::from_secs(30 * 60);

#[derive(NetworkBehaviour)]
struct Behaviour {
    gossipsub: gossipsub::Behaviour,
    kad: kad::Behaviour<MemoryStore>,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    stream: libp2p_stream::Behaviour,
}

pub struct NodeConfig {
    pub identity: NodeIdentity,
    pub listen: Vec<Multiaddr>,
    /// Direcciones a anunciar en lugar de las de escucha (p. ej. la del túnel).
    pub announce: Vec<Multiaddr>,
    pub bootstrap: Vec<Multiaddr>,
    pub tls: websocket::tls::Config,
    /// Si se da, el nodo publica su `NodeAdvertise` cada 30 s con este beacon.
    /// Con el beacon de Navigator el Portal lo usará: solo en el cambio real.
    pub advertise: Option<Beacon>,
    /// Si se da, el nodo publica su `DhtBeaconRecord` firmado en
    /// `/fhs/beacon/<did>`. El Portal lo lee solo para el DID que ya vio
    /// anunciado, así que publicarlo en sombra no interfiere.
    pub dht_beacon: Option<Beacon>,
}

enum Command {
    Publish {
        topic: &'static str,
        data: Vec<u8>,
    },
    Dial {
        addr: Multiaddr,
        reply: oneshot::Sender<Result<PeerId, String>>,
    },
    Status {
        reply: oneshot::Sender<NodeStatus>,
    },
    PutRecord {
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfo {
    pub peer: String,
    pub remote_addr: String,
    pub direction: &'static str,
    pub status: &'static str,
    pub opened_at: String,
    pub streams: u32,
}

#[derive(Clone, Serialize)]
pub struct TopicStatus {
    pub subscribers: usize,
    pub mesh: usize,
}

/// Mismo formato que `nodeStatus` de `@rafex/galaxia-fhs-node`.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    pub peer_id: String,
    pub multiaddrs: Vec<String>,
    pub peer_count: usize,
    pub connections: Vec<ConnectionInfo>,
    pub pubsub: std::collections::BTreeMap<String, TopicStatus>,
}

/// Pujas recibidas por misión mientras su ventana está abierta.
#[derive(Clone, Default)]
pub struct BidCollector {
    open: Arc<Mutex<HashMap<String, OpenBidWindow>>>,
}

#[derive(Default)]
struct OpenBidWindow {
    bids: Vec<MissionBidMessage>,
    preferred_provider: Option<String>,
    changed: Arc<Notify>,
}

impl BidCollector {
    /// Abre la ventana hasta `deadline`; puede cerrar antes si llega el
    /// provider preferido, que por regla de selección ya es el ganador.
    pub async fn collect(
        &self,
        mission_id: &str,
        deadline: Duration,
        preferred_provider: Option<String>,
    ) -> Vec<MissionBidMessage> {
        self.open.lock().expect("bids").insert(
            mission_id.to_string(),
            OpenBidWindow {
                bids: Vec::new(),
                preferred_provider,
                changed: Arc::new(Notify::new()),
            },
        );
        let cutoff = tokio::time::Instant::now() + deadline;
        loop {
            let changed = {
                let open = self.open.lock().expect("bids");
                let Some(window) = open.get(mission_id) else {
                    break;
                };
                let preferred_arrived =
                    window.preferred_provider.as_ref().is_some_and(|preferred| {
                        window.bids.iter().any(|bid| &bid.provider_did == preferred)
                    });
                if preferred_arrived {
                    break;
                }
                window.changed.clone()
            };
            if tokio::time::timeout_at(cutoff, changed.notified())
                .await
                .is_err()
            {
                break;
            }
        }
        self.open
            .lock()
            .expect("bids")
            .remove(mission_id)
            .map(|window| window.bids)
            .unwrap_or_default()
    }

    /// Entrega una puja; las de misiones sin ventana abierta se descartan.
    pub fn deliver(&self, bid: MissionBidMessage) -> bool {
        match self.open.lock().expect("bids").get_mut(&bid.mission_id) {
            Some(window) => {
                window.bids.push(bid);
                window.changed.notify_one();
                true
            }
            None => false,
        }
    }
}

#[derive(Clone)]
pub struct NodeHandle {
    pub identity: NodeIdentity,
    pub peers: PeerCache,
    pub bids: BidCollector,
    commands: mpsc::Sender<Command>,
    control: libp2p_stream::Control,
    connected: watch::Receiver<HashSet<PeerId>>,
    /// Se avisa en cada conexión nueva (el TS se anuncia en `peer:connect`).
    peer_connected: Arc<Notify>,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("no se pudo construir el nodo libp2p: {0}")]
    Build(String),
    #[error("dirección de escucha inválida {0}: {1}")]
    Listen(String, String),
    #[error("el nodo libp2p se detuvo")]
    Stopped,
}

impl NodeHandle {
    pub async fn publish(&self, topic: &'static str, data: Vec<u8>) {
        let _ = self.commands.send(Command::Publish { topic, data }).await;
    }

    /// Marca una dirección (con `/p2p/<id>`) y espera la conexión.
    pub async fn dial(&self, addr: Multiaddr) -> Result<PeerId, String> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(Command::Dial { addr, reply })
            .await
            .map_err(|_| "el nodo libp2p se detuvo".to_string())?;
        match tokio::time::timeout(DIAL_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("el nodo libp2p se detuvo".into()),
            Err(_) => Err(format!("sin respuesta en {} s", DIAL_TIMEOUT.as_secs())),
        }
    }

    pub async fn status(&self) -> Option<NodeStatus> {
        let (reply, rx) = oneshot::channel();
        self.commands.send(Command::Status { reply }).await.ok()?;
        rx.await.ok()
    }

    /// Guarda un registro en el DHT (quórum 1); el resultado va al log.
    pub async fn put_record(&self, key: Vec<u8>, value: Vec<u8>) {
        let _ = self.commands.send(Command::PutRecord { key, value }).await;
    }

    pub fn is_connected(&self, peer: &PeerId) -> bool {
        self.connected.borrow().contains(peer)
    }

    pub fn stream_control(&self) -> libp2p_stream::Control {
        self.control.clone()
    }

    pub fn fhs_protocol() -> StreamProtocol {
        StreamProtocol::new(wire::FHS_STREAM_PROTOCOL)
    }
}

fn build_swarm(config: &NodeConfig) -> Result<Swarm<Behaviour>, NodeError> {
    let tls = config.tls.clone();
    let swarm = libp2p::SwarmBuilder::with_existing_identity(config.identity.keypair.clone())
        .with_tokio()
        .with_other_transport(move |key| {
            let tcp =
                libp2p::tcp::tokio::Transport::new(libp2p::tcp::Config::default().nodelay(true));
            let dns = libp2p::dns::tokio::Transport::system(tcp)?;
            let mut ws = websocket::Config::new(dns);
            ws.set_tls_config(tls);
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                ws.upgrade(Version::V1)
                    .authenticate(noise::Config::new(key)?)
                    .multiplex(yamux::Config::default())
                    .timeout(Duration::from_secs(20)),
            )
        })
        .map_err(|e| NodeError::Build(e.to_string()))?
        .with_behaviour(|key| {
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub::ConfigBuilder::default()
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .max_transmit_size(1024 * 1024)
                    .build()
                    .map_err(|e| e.to_string())?,
            )?;
            let peer_id = key.public().to_peer_id();
            let mut kad = kad::Behaviour::new(peer_id, MemoryStore::new(peer_id));
            kad.set_mode(Some(kad::Mode::Client));
            Ok(Behaviour {
                gossipsub,
                kad,
                identify: identify::Behaviour::new(
                    identify::Config::new("/ipfs/id/1.0.0".into(), key.public())
                        .with_agent_version(format!("galaxia-agent/{}", env!("CARGO_PKG_VERSION"))),
                ),
                ping: ping::Behaviour::default(),
                stream: libp2p_stream::Behaviour::new(),
            })
        })
        .map_err(|e| NodeError::Build(e.to_string()))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(300)))
        .build();
    Ok(swarm)
}

/// Arranca el nodo: escucha, se suscribe, marca a los bootstraps y, si se
/// pidió, empieza a anunciarse.
pub fn start(config: NodeConfig) -> Result<NodeHandle, NodeError> {
    let mut swarm = build_swarm(&config)?;
    for addr in &config.listen {
        swarm
            .listen_on(addr.clone())
            .map_err(|e| NodeError::Listen(addr.to_string(), e.to_string()))?;
    }
    for topic in [TOPIC_NODES_ADVERTISE, TOPIC_MISSIONS_BID] {
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&gossipsub::IdentTopic::new(topic))
            .map_err(|e| NodeError::Build(e.to_string()))?;
    }

    let control = swarm.behaviour().stream.new_control();
    let (commands, rx) = mpsc::channel(256);
    let (connected_tx, connected) = watch::channel(HashSet::new());
    let handle = NodeHandle {
        identity: config.identity.clone(),
        peers: PeerCache::default(),
        bids: BidCollector::default(),
        commands,
        control,
        connected,
        peer_connected: Arc::new(Notify::new()),
    };
    let (listen_tx, listen_rx) = watch::channel(Vec::<Multiaddr>::new());

    let actor = Actor {
        swarm,
        identity: config.identity.clone(),
        peers: handle.peers.clone(),
        bids: handle.bids.clone(),
        pending_dials: HashMap::new(),
        connections: HashMap::new(),
        connected: connected_tx,
        listen_addrs: listen_tx,
        peer_connected: handle.peer_connected.clone(),
    };
    tokio::spawn(actor.run(rx));

    for addr in config.bootstrap.iter().cloned() {
        tokio::spawn(bootstrap_loop(handle.clone(), addr));
    }
    if let Some(beacon) = config.dht_beacon {
        tokio::spawn(dht_beacon_loop(
            handle.clone(),
            beacon,
            config.announce.clone(),
            listen_rx.clone(),
        ));
    }
    if let Some(beacon) = config.advertise {
        tokio::spawn(advertise_loop(
            handle.clone(),
            beacon,
            config.announce.clone(),
            listen_rx,
        ));
    }
    Ok(handle)
}

struct OpenConnection {
    peer: PeerId,
    remote_addr: String,
    direction: &'static str,
    opened_at_ms: i64,
}

struct Actor {
    swarm: Swarm<Behaviour>,
    identity: NodeIdentity,
    peers: PeerCache,
    bids: BidCollector,
    pending_dials: HashMap<ConnectionId, oneshot::Sender<Result<PeerId, String>>>,
    connections: HashMap<ConnectionId, OpenConnection>,
    connected: watch::Sender<HashSet<PeerId>>,
    listen_addrs: watch::Sender<Vec<Multiaddr>>,
    peer_connected: Arc<Notify>,
}

impl Actor {
    async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    Some(command) => self.handle_command(command),
                    None => break,
                },
                event = self.swarm.select_next_some() => self.handle_event(event),
            }
        }
        tracing::info!("nodo libp2p detenido");
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Publish { topic, data } => {
                if let Err(error) = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(gossipsub::IdentTopic::new(topic), data)
                {
                    // Sin pares en el tema (p. ej. al arrancar): el TS lo
                    // registra y sigue; aquí igual.
                    tracing::debug!("publicación en {topic} no enviada: {error}");
                }
            }
            Command::Dial { addr, reply } => {
                let opts = DialOpts::unknown_peer_id().address(addr.clone()).build();
                let id = opts.connection_id();
                match self.swarm.dial(opts) {
                    Ok(()) => {
                        self.pending_dials.insert(id, reply);
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error.to_string()));
                    }
                }
            }
            Command::Status { reply } => {
                let _ = reply.send(self.status());
            }
            Command::PutRecord { key, value } => {
                let record = kad::Record::new(key, value);
                if let Err(error) = self
                    .swarm
                    .behaviour_mut()
                    .kad
                    .put_record(record, kad::Quorum::One)
                {
                    tracing::warn!("no se pudo guardar el registro DHT: {error:?}");
                }
            }
        }
    }

    fn handle_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!("escuchando en {address}");
                self.listen_addrs.send_modify(|addrs| addrs.push(address));
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                self.listen_addrs
                    .send_modify(|addrs| addrs.retain(|a| a != &address));
            }
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                ..
            } => {
                let (remote_addr, direction) = match &endpoint {
                    ConnectedPoint::Dialer { address, .. } => (address.to_string(), "outbound"),
                    ConnectedPoint::Listener { send_back_addr, .. } => {
                        (send_back_addr.to_string(), "inbound")
                    }
                };
                let arrow = if direction == "outbound" {
                    "→"
                } else {
                    "←"
                };
                tracing::info!("conexión abierta {arrow} {peer_id} {remote_addr}");
                self.connections.insert(
                    connection_id,
                    OpenConnection {
                        peer: peer_id,
                        remote_addr,
                        direction,
                        opened_at_ms: now_ms(),
                    },
                );
                self.connected.send_modify(|set| {
                    set.insert(peer_id);
                });
                if let Some(reply) = self.pending_dials.remove(&connection_id) {
                    let _ = reply.send(Ok(peer_id));
                }
                self.peer_connected.notify_one();
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                num_established,
                cause,
                ..
            } => {
                if let Some(conn) = self.connections.remove(&connection_id) {
                    let seconds = (now_ms() - conn.opened_at_ms) / 1000;
                    tracing::info!(
                        "conexión cerrada {peer_id} {} (duró {seconds} s){}",
                        conn.remote_addr,
                        cause.map(|c| format!(": {c}")).unwrap_or_default()
                    );
                }
                if num_established == 0 {
                    self.connected.send_modify(|set| {
                        set.remove(&peer_id);
                    });
                }
            }
            SwarmEvent::OutgoingConnectionError {
                connection_id,
                error,
                ..
            } => {
                if let Some(reply) = self.pending_dials.remove(&connection_id) {
                    let _ = reply.send(Err(error.to_string()));
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
                message,
                ..
            })) => {
                self.handle_gossip(message);
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                ..
            })) => {
                // Solo pares que sirven Kademlia (Atlas); los clientes (Portal,
                // providers) no responden consultas y solo las retrasarían.
                if info.protocols.contains(&kad::PROTOCOL_NAME) {
                    for addr in info.listen_addrs {
                        self.swarm.behaviour_mut().kad.add_address(&peer_id, addr);
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
                result: kad::QueryResult::PutRecord(result),
                step,
                ..
            })) if step.last => match result {
                Ok(ok) => tracing::info!(
                    "registro DHT guardado: {}",
                    String::from_utf8_lossy(ok.key.as_ref())
                ),
                Err(error) => tracing::warn!("no se pudo guardar el registro DHT: {error}"),
            },
            _ => {}
        }
    }

    fn handle_gossip(&mut self, message: gossipsub::Message) {
        let topic = message.topic.as_str();
        if topic == TOPIC_NODES_ADVERTISE {
            match wire::verified_node_advertise(&message.data) {
                Ok(advertise) if advertise.did != self.identity.did => {
                    self.peers.upsert(&advertise);
                }
                Ok(_) => {}
                Err(reason) => tracing::warn!("anuncio descartado en {topic}: {reason:?}"),
            }
        } else if topic == TOPIC_MISSIONS_BID {
            match wire::verified_mission_bid(&message.data) {
                Ok(bid) => {
                    self.bids.deliver(bid);
                }
                Err(reason) => tracing::warn!("puja descartada: {reason:?}"),
            }
        }
    }

    fn status(&mut self) -> NodeStatus {
        let peer_id = *self.swarm.local_peer_id();
        let multiaddrs = self
            .swarm
            .listeners()
            .cloned()
            .chain(self.swarm.external_addresses().cloned())
            .map(|a| with_peer_id(a, peer_id).to_string())
            .collect();
        let connections: Vec<ConnectionInfo> = self
            .connections
            .values()
            .map(|c| ConnectionInfo {
                peer: c.peer.to_string(),
                remote_addr: c.remote_addr.clone(),
                direction: c.direction,
                status: "open",
                opened_at: crate::p2p::peer_cache::iso8601(c.opened_at_ms),
                streams: 0,
            })
            .collect();
        let gossip = &self.swarm.behaviour().gossipsub;
        let mut pubsub = std::collections::BTreeMap::new();
        for topic in gossip.topics() {
            let subscribers = gossip
                .all_peers()
                .filter(|(_, topics)| topics.contains(&topic))
                .count();
            let mesh = gossip.mesh_peers(topic).count();
            pubsub.insert(topic.to_string(), TopicStatus { subscribers, mesh });
        }
        NodeStatus {
            peer_id: peer_id.to_string(),
            multiaddrs,
            peer_count: self.connected.borrow().len(),
            connections,
            pubsub,
        }
    }
}

/// Agrega `/p2p/<peer>` si la dirección no lo trae.
pub fn with_peer_id(addr: Multiaddr, peer: PeerId) -> Multiaddr {
    if addr.iter().any(|p| matches!(p, Protocol::P2p(_))) {
        addr
    } else {
        addr.with(Protocol::P2p(peer))
    }
}

pub fn peer_id_of(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| match p {
        Protocol::P2p(peer) => Some(peer),
        _ => None,
    })
}

/// Reintenta el bootstrap con backoff y vuelve a marcar si se pierde la
/// conexión (misma política que `dialBootstraps` en `fhs-node`).
async fn bootstrap_loop(handle: NodeHandle, addr: Multiaddr) {
    let Some(peer) = peer_id_of(&addr) else {
        tracing::warn!("bootstrap sin /p2p/<id>: {addr}; se marca una sola vez");
        if let Err(error) = handle.dial(addr.clone()).await {
            tracing::warn!("bootstrap no disponible ({addr}): {error}");
        }
        return;
    };
    let mut backoff = BOOTSTRAP_INITIAL_BACKOFF;
    let mut attempt: u32 = 0;
    let mut connected_once = false;
    loop {
        attempt += 1;
        if !handle.is_connected(&peer) {
            match handle.dial(addr.clone()).await {
                Ok(_) => {
                    let what = if connected_once {
                        "reconectado"
                    } else {
                        "conectado"
                    };
                    tracing::info!("bootstrap {what}: {addr} (intento {attempt})");
                    connected_once = true;
                    backoff = BOOTSTRAP_INITIAL_BACKOFF;
                    attempt = 0;
                }
                Err(error) => {
                    tracing::warn!(
                        "bootstrap no disponible ({addr}): {error} — reintento {attempt} en {} s",
                        backoff.as_secs()
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BOOTSTRAP_MAX_BACKOFF);
                    continue;
                }
            }
        }
        let mut connected = handle.connected.clone();
        let lost = connected.wait_for(|set| !set.contains(&peer)).await;
        if lost.is_err() {
            return;
        }
        tracing::warn!("se perdió la conexión con el bootstrap ({addr}); reintentando");
    }
}

/// Direcciones propias a anunciar, con `/p2p/<id>`.
fn own_addrs(
    announce: &[Multiaddr],
    listen: &watch::Receiver<Vec<Multiaddr>>,
    peer: PeerId,
) -> Vec<String> {
    let addrs = if announce.is_empty() {
        listen.borrow().clone()
    } else {
        announce.to_vec()
    };
    addrs
        .into_iter()
        .map(|a| with_peer_id(a, peer).to_string())
        .collect()
}

/// Publica el `DhtBeaconRecord` firmado al conectar con un bootstrap y cada
/// 30 min. Si falla, el Portal usa las direcciones del anuncio GossipSub.
async fn dht_beacon_loop(
    handle: NodeHandle,
    beacon: Beacon,
    announce: Vec<Multiaddr>,
    listen: watch::Receiver<Vec<Multiaddr>>,
) {
    let peer = handle.identity.peer_id;
    let mut connected = handle.connected.clone();
    loop {
        if connected.wait_for(|set| !set.is_empty()).await.is_err() {
            return;
        }
        // Dar tiempo a identify para llenar la tabla de Kademlia.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let record = wire::signed_dht_beacon(
            &handle.identity,
            beacon.clone(),
            own_addrs(&announce, &listen, peer),
        );
        handle
            .put_record(wire::dht_beacon_key(&handle.identity.did), record)
            .await;
        tokio::time::sleep(DHT_REPUBLISH_INTERVAL).await;
    }
}

/// Publica el `NodeAdvertise` firmado cada 30 s y tras cada conexión nueva.
async fn advertise_loop(
    handle: NodeHandle,
    beacon: Beacon,
    announce: Vec<Multiaddr>,
    mut listen: watch::Receiver<Vec<Multiaddr>>,
) {
    let peer = handle.identity.peer_id;
    let started = Instant::now();
    // Esperar a tener al menos una dirección de escucha (máx. 10 s).
    while listen.borrow().is_empty() && started.elapsed() < Duration::from_secs(10) {
        if tokio::time::timeout(Duration::from_secs(1), listen.changed())
            .await
            .is_ok_and(|r| r.is_err())
        {
            return;
        }
    }
    let mut interval = tokio::time::interval(ADVERTISE_INTERVAL);
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            () = handle.peer_connected.notified() => {
                tokio::time::sleep(ADVERTISE_ON_CONNECT_DELAY).await;
            }
        }
        let addrs = own_addrs(&announce, &listen, peer);
        let bytes = wire::signed_node_advertise(
            &handle.identity,
            beacon.clone(),
            addrs,
            ADVERTISE_TTL_SECONDS,
        );
        handle.publish(TOPIC_NODES_ADVERTISE, bytes).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bid_collector_only_keeps_bids_for_open_missions() {
        let bids = BidCollector::default();
        assert!(!bids.deliver(MissionBidMessage {
            mission_id: "cerrada".into(),
            ..Default::default()
        }));
        let collecting = {
            let bids = bids.clone();
            tokio::spawn(async move { bids.collect("m1", Duration::from_millis(50), None).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(bids.deliver(MissionBidMessage {
            mission_id: "m1".into(),
            provider_did: "did:key:zA".into(),
            ..Default::default()
        }));
        let got = collecting.await.unwrap();
        assert_eq!(got.len(), 1);
        assert!(!bids.deliver(MissionBidMessage {
            mission_id: "m1".into(),
            ..Default::default()
        }));
    }

    #[tokio::test]
    async fn preferred_provider_bid_closes_the_window_without_waiting_for_deadline() {
        let bids = BidCollector::default();
        let started = Instant::now();
        let collecting = {
            let bids = bids.clone();
            tokio::spawn(async move {
                bids.collect(
                    "preferred-mission",
                    Duration::from_secs(2),
                    Some("did:key:zPreferred".into()),
                )
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(bids.deliver(MissionBidMessage {
            mission_id: "preferred-mission".into(),
            provider_did: "did:key:zOther".into(),
            ..Default::default()
        }));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(bids.deliver(MissionBidMessage {
            mission_id: "preferred-mission".into(),
            provider_did: "did:key:zPreferred".into(),
            ..Default::default()
        }));

        let got = collecting.await.unwrap();
        assert_eq!(got.len(), 2);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn appends_peer_id_only_when_missing() {
        let peer = PeerId::random();
        let bare: Multiaddr = "/ip4/192.168.1.139/tcp/4010/tls/ws".parse().unwrap();
        let with = with_peer_id(bare.clone(), peer);
        assert_eq!(peer_id_of(&with), Some(peer));
        assert_eq!(with_peer_id(with.clone(), PeerId::random()), with);
        assert_eq!(peer_id_of(&bare), None);
    }
}
