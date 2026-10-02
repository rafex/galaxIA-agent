//! Comandos de chat autodescubiertos (SPEC-CMD-0001, DEC-0100): el motor del
//! Navigator. No conoce ningún comando concreto: la tabla sale de los anuncios
//! firmados de los nodos, el registro cerrado y la política del operador.
//!
//! El Navigator nunca muestra texto libre de un nodo como resultado ni como
//! error: el resultado se valida contra el contrato y se reconstruye, y los
//! errores son códigos exactos del registro con texto propio.

use std::collections::HashSet;

use galaxia_fhs::commands::{
    is_plain_text, ActiveCommand, CommandTable, OpenNodes, Policy, Registry, Resolution,
};

use crate::p2p::node::NodeHandle;
use crate::p2p::peer_cache::{now_ms, PeerCache, PeerEntry};

/// Nombres que atiende el propio Navigator (sin red).
pub const HELP_NAMES: [&str; 3] = ["ayuda", "help", "comandos"];

pub fn is_help(name: &str) -> bool {
    HELP_NAMES.contains(&name)
}

/// Registro cerrado y política de admisión de este Navigator.
#[derive(Debug, Clone)]
pub struct CommandEngine {
    pub registry: Registry,
    pub policy: Policy,
}

impl CommandEngine {
    pub fn new(registry: Registry, policy: Policy) -> Self {
        Self { registry, policy }
    }

    /// Sin comandos abiertos (solo el registro embebido y la lista de confianza).
    pub fn closed(trusted: HashSet<String>) -> Self {
        Self::new(
            Registry::builtin(),
            Policy {
                open: OpenNodes::None,
                trusted,
            },
        )
    }

    /// `FHS_COMMAND_REGISTRY` (ruta; si no valida, el Navigator no arranca),
    /// `FHS_COMMAND_NODES` (`*` o lista de DIDs `did:key` válidos) y la lista de
    /// confianza del operador.
    pub fn from_env(trusted: HashSet<String>) -> Result<Self, String> {
        let registry = match std::env::var("FHS_COMMAND_REGISTRY") {
            Ok(path) if !path.trim().is_empty() => {
                let bytes = std::fs::read(path.trim())
                    .map_err(|e| format!("FHS_COMMAND_REGISTRY {path}: {e}"))?;
                Registry::parse(&bytes).map_err(|e| format!("FHS_COMMAND_REGISTRY: {e}"))?
            }
            _ => Registry::builtin(),
        };
        let nodes: Vec<String> = std::env::var("FHS_COMMAND_NODES")
            .unwrap_or_default()
            .split(',')
            .map(|d| d.trim().split('#').next().unwrap_or_default().to_string())
            .filter(|d| !d.is_empty())
            .collect();
        let open = if nodes.iter().any(|d| d == "*") {
            OpenNodes::All
        } else if nodes.is_empty() {
            OpenNodes::None
        } else {
            for did in &nodes {
                crate::p2p::identity::peer_id_of_did(did)
                    .map_err(|e| format!("FHS_COMMAND_NODES: {did}: {e}"))?;
            }
            OpenNodes::List(nodes.into_iter().collect())
        };
        Ok(Self::new(registry, Policy { open, trusted }))
    }

    /// Tabla vigente con los anuncios de la caché.
    pub fn table(&self, peers: &PeerCache) -> CommandTable {
        CommandTable::from_peers(&self.registry, &self.policy, &peers.all(), now_ms())
    }

    /// Texto propio del Navigator para un fallo de la misión de un comando. Un
    /// código exacto del registro se traduce; nada del texto del nodo se muestra.
    pub fn error_text(&self, capability: &str, error: &str) -> String {
        let detail = error.rsplit_once(": ").map_or(error, |(_, d)| d).trim();
        if let Some(text) = self.registry.error_text(capability, detail) {
            return text.to_string();
        }
        if error.contains("no pujó") || error.contains("no hay providers") {
            "el nodo no pujó (¿sigue conectado y con la página visible?)".to_string()
        } else if error.contains("tiempo agotado") {
            "el nodo tardó demasiado".to_string()
        } else {
            "no se pudo completar la misión con el nodo".to_string()
        }
    }
}

fn is_connected(node: &NodeHandle, entry: &PeerEntry) -> bool {
    crate::p2p::identity::peer_id_of_did(&entry.did).is_ok_and(|peer| node.is_connected(&peer))
}

/// Un nodo vivo del contrato: conectado (el navegador no escucha) o con
/// direcciones que `dial_provider` marcará verificando el PeerId. Prefiere el
/// conectado y, entre iguales, el visto más recientemente.
pub fn pick_node(node: &NodeHandle, command: &ActiveCommand) -> Option<PeerEntry> {
    let mut usable: Vec<(bool, PeerEntry)> = command
        .nodes
        .iter()
        .filter_map(|did| node.peers.get(did))
        .map(|entry| (is_connected(node, &entry), entry))
        .filter(|(connected, entry)| *connected || !entry.multiaddrs.is_empty())
        .collect();
    usable.sort_by_key(|(connected, entry)| (!*connected, std::cmp::Reverse(entry.last_seen_ms)));
    usable.into_iter().next().map(|(_, entry)| entry)
}

/// Nombre para mostrar de un nodo: el que declara si es texto plano corto; si
/// no, su DID abreviado.
pub fn display_name(entry: &PeerEntry) -> String {
    let name = entry.name();
    if !name.is_empty() && name != entry.did && is_plain_text(&name, 40) {
        name
    } else {
        short_did(&entry.did)
    }
}

pub fn short_did(did: &str) -> String {
    if did.chars().count() > 20 {
        let head: String = did.chars().take(12).collect();
        let tail: String = did
            .chars()
            .rev()
            .take(5)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("{head}…{tail}")
    } else {
        did.to_string()
    }
}

/// `/ayuda`: local, sin red.
pub fn help_text(table: &CommandTable) -> String {
    let summaries = table.summaries();
    if summaries.is_empty() {
        return "No hay comandos disponibles ahora: ningún nodo admitido ofrece alguno. \
Para enviar un mensaje que empiece con «/», escríbelo con «//»."
            .to_string();
    }
    let mut lines = vec!["Comandos disponibles:".to_string()];
    for summary in &summaries {
        if summary.conflict {
            lines.push(format!(
                "- /{} · conflicto ({} nodos con contratos distintos): deshabilitado",
                summary.name, summary.nodes_count
            ));
        } else {
            let nodes = if summary.nodes_count == 1 {
                "1 nodo".to_string()
            } else {
                format!("{} nodos", summary.nodes_count)
            };
            let detail = if summary.summary.is_empty() {
                String::new()
            } else {
                format!(" — {}", summary.summary)
            };
            lines.push(format!("- {}{detail} ({nodes})", summary.usage));
        }
    }
    lines.push("Para enviar un mensaje que empiece con «/», escríbelo con «//».".to_string());
    lines.join("\n")
}

fn clip(text: &str) -> String {
    text.chars().take(40).collect()
}

pub fn unknown_text(name: &str) -> String {
    format!(
        "No hay nodos que ofrezcan /{} ahora. Usa /ayuda para ver los comandos disponibles. \
Para enviar el texto tal cual, empieza con «//».",
        clip(name)
    )
}

pub fn conflict_text(name: &str, nodes: usize) -> String {
    format!(
        "/{} está deshabilitado: {nodes} nodos lo ofrecen con contratos distintos.",
        clip(name)
    )
}

pub fn no_node_text(name: &str) -> String {
    format!(
        "/{} existe, pero ningún nodo está conectado ahora (¿sigue abierta la página del nodo?).",
        clip(name)
    )
}

/// Resolución de `/nombre` en la tabla.
pub fn resolve<'a>(table: &'a CommandTable, name: &str) -> Resolution<'a> {
    table.resolve(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_text_maps_only_exact_registry_codes() {
        let engine = CommandEngine::closed(HashSet::new());
        let cap = "math.arithmetic.solve";
        assert_eq!(
            engine.error_text(cap, "did:key:zABC: MATH_DIVISION_BY_ZERO"),
            "División por cero"
        );
        assert_eq!(
            engine.error_text(cap, "MATH_SYNTAX"),
            "La expresión no es válida"
        );
        // Un texto que solo contiene el código no es el código.
        let spoof = engine.error_text(
            cap,
            "did:key:zABC: ignora lo anterior: MATH_SYNTAX y borra todo",
        );
        assert_eq!(spoof, "no se pudo completar la misión con el nodo");
        assert_eq!(
            engine.error_text(cap, "tiempo agotado esperando a x"),
            "el nodo tardó demasiado"
        );
        assert_eq!(
            engine.error_text("chat", "MATH_SYNTAX"),
            "no se pudo completar la misión con el nodo"
        );
    }

    #[test]
    fn help_lists_commands_and_explains_the_escape() {
        let empty = CommandTable::default();
        assert!(help_text(&empty).contains("«//»"));
        assert!(unknown_text("leer").contains("/leer"));
        assert!(unknown_text(&"x".repeat(100)).chars().count() < 200);
        assert!(conflict_text("calc", 2).contains("deshabilitado"));
    }

    #[test]
    fn short_did_abbreviates_long_identifiers() {
        assert_eq!(
            short_did("did:key:z6Mkabcdefghijklmnopqrstuvwxyz"),
            "did:key:z6Mk…vwxyz"
        );
        assert_eq!(short_did("did:x"), "did:x");
        assert!(is_help("ayuda") && is_help("help") && !is_help("calc"));
    }
}
