# galaxIA-agent

Navigator de galaxIA en Rust: se une a la red FHS por libp2p, atiende la
sesión del Portal, subasta Missions entre Star y Satellites y arma la
respuesta con KB, RAG y OCR. Usa [Rig 0.42.0](https://docs.rs/rig/0.42.0/rig/)
como interfaz de `CompletionModel` hacia Star.

Es el reemplazo de `galaxIA-Core/apps/navigator` (TypeScript). Evaluación,
fases y resultados en [`docs/migracion-desde-ts.md`](docs/migracion-desde-ts.md).

## Estado

Navigator Rust/Rig está activo en Bastion y se ha probado contra Atlas, Star,
KB, RAG y OCR. El runtime TypeScript de Core queda como referencia histórica;
no se debe levantar junto al agente Rust porque comparten identidad y puertos.

| Pieza | Módulo |
|---|---|
| Identidad Ed25519 → PeerId + `did:key` (mismo archivo que el TS) | `p2p/identity.rs` |
| Transporte WSS+TLS, Noise, yamux; TLS con pin del certificado del lab | `p2p/tls.rs`, `vendor/libp2p-websocket` |
| Nodo: GossipSub, Kademlia (cliente), identify, ping, streams `/fhs/v1/0.1.0`, reconexión al bootstrap | `p2p/node.rs` |
| `NodeAdvertise` firmados → caché con TTL; anuncio propio cada 30 s y tras cada conexión nueva | `p2p/peer_cache.rs`, `p2p/wire.rs` |
| Beacon firmado en el DHT (`/fhs/beacon/<did>`), republicado cada 30 min | `p2p/node.rs`, `p2p/wire.rs` |
| offer / bid / assign firmados y elección del ganador | `p2p/mission.rs` |
| Misiones de chat (con streaming de deltas) y de tools | `p2p/client.rs` |
| Firmas y framing de Envelopes (verificación sobre bytes crudos) | `signing.rs`, `p2p/framing.rs` |
| Turno del agente: OCR determinista con failover, recomendación y consulta de KB, RAG por red, una ronda de tools (como el TS), procedencia | `runtime/agent.rs`, `runtime/kb.rs`, `runtime/providers.rs` |
| Adaptador Rig → Star (roles y tools reales, streaming) | `llm.rs` |
| Sesión del Portal: handshake, `agentStart`, `chatRequest`, `kbDecision`, `chatCancel` con aborto real | `session.rs` |
| `/health` y `/status` con el formato del TS; apagado limpio con SIGTERM | `main.rs` |

Observación de latencia pendiente de caracterización estadística:

- El plazo predeterminado de pujas es 2 s. La ventana ya termina antes si
  llega el provider preferido (caso seguro porque la regla de selección lo
  hace ganador); para elegir entre otros providers se conserva el plazo
  completo. El tiempo real se registra como `bid_wait_ms` por mission.
- Una pregunta con KB ha tardado 14–30 s en muestras del laboratorio; hay que
  separar TTFT, espera de bids, provider, prompt e inferencia antes de atribuir
  esa duración a una implementación concreta.
- Artefactos solo en línea (sin IPFS).

## Configuración

Configuración del agente Rust:

| Variable | Default |
|---|---|
| `IDENTITY_KEY_PATH` | `./.fhs-identity-navigator.json` |
| `FHS_LISTEN_ADDRS` | `/ip4/0.0.0.0/tcp/4010/tls/ws` |
| `FHS_ANNOUNCE_ADDRS`, `FHS_BOOTSTRAP_ADDRS` | vacío |
| `TLS_CERT_PATH`, `TLS_KEY_PATH` | sin TLS en la API HTTP |
| `NODE_EXTRA_CA_CERTS` | certificados de confianza adicionales |
| `HOST`, `PORT` | `127.0.0.1`, `8090` |
| `FHS_VETOED_PROVIDERS` | DIDs separados por coma |

`DEFAULT_BID_DEADLINE` conserva un máximo por oferta de 2 s; requests pueden
usar otro plazo. La llegada del provider preferido cierra antes esa ventana sin
cambiar al ganador definido por la política.

Y una propia: `FHS_ADVERTISE_AS_NAVIGATOR=true` hace que se anuncie como
`navigator`. Por defecto es `false`, para poder correrlo junto al TS sin que
el Portal lo tome.

## Desarrollo

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Pruebas contra una red real (el binario se une a la red y ejecuta una
acción):

```sh
galaxia-agent probe chat "<pregunta>"
galaxia-agent probe rig "<pregunta>"
galaxia-agent probe tool <capability> <tool> '<json>'
galaxia-agent probe turn "<pregunta>"
galaxia-agent probe portal <multiaddr-del-agente> "<pregunta>"
```

## Contenedor

```sh
podman build -t galaxia-agent:dev .
podman run -d --name galaxia-agent --network host --restart always \
  -v navigator-data:/data -e IDENTITY_KEY_PATH=/data/identity.json \
  -e FHS_BOOTSTRAP_ADDRS=... -e FHS_ANNOUNCE_ADDRS=... \
  galaxia-agent:dev
```

El contenedor no incluye `llama.cpp`: solo Star lo invoca.
