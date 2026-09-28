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

La base FHS (IDL, firmas, identidad, TLS, nodo libp2p, misiones) vive en el
crate `galaxia-fhs` de [galaxIA-SDK](https://github.com/rafex/galaxIA-SDK/tree/main/rust)
y se reexporta como `p2p`, `protocol` y `signing`; aquí queda lo propio del
Navigator (runtime, sesión del Portal, adaptador Rig).

| Pieza | Módulo |
|---|---|
| Identidad Ed25519 → PeerId + `did:key` (mismo archivo que el TS) | `p2p/identity.rs` (galaxia-fhs) |
| Transporte WSS+TLS, Noise, yamux; TLS con pin del certificado del lab | `p2p/tls.rs` (galaxia-fhs), `vendor/libp2p-websocket` (galaxia-fhs) |
| Nodo: GossipSub, Kademlia (cliente), identify, ping, streams `/fhs/v1/0.1.0`, reconexión al bootstrap | `p2p/node.rs` (galaxia-fhs) |
| `NodeAdvertise` firmados → caché con TTL; anuncio propio cada 30 s y tras cada conexión nueva | `p2p/peer_cache.rs` (galaxia-fhs), `p2p/wire.rs` (galaxia-fhs) |
| Beacon firmado en el DHT (`/fhs/beacon/<did>`), republicado cada 30 min | `p2p/node.rs` (galaxia-fhs), `p2p/wire.rs` (galaxia-fhs) |
| offer / bid / assign firmados y elección del ganador | `p2p/mission.rs` (galaxia-fhs) |
| Misiones de chat (con streaming de deltas) y de tools | `p2p/client.rs` (galaxia-fhs) |
| Firmas y framing de Envelopes (verificación sobre bytes crudos) | `signing.rs` (galaxia-fhs), `p2p/framing.rs` (galaxia-fhs) |
| Turno del agente: OCR determinista con failover, recomendación y consulta de KB, RAG por red, una ronda de tools (como el TS), procedencia | `runtime/agent.rs`, `runtime/kb.rs`, `runtime/providers.rs` |
| Adaptador Rig → Star (roles y tools reales, streaming) | `llm.rs` |
| Sesión del Portal: handshake, `agentStart`, `chatRequest`, `kbDecision`, `chatCancel` con aborto real | `session.rs` |
| Adjuntos por IPFS nativo (DEC-0095): subida al Kubo local, libro de pines con leases, cuotas, barrido y auditoría | `ipfs/mod.rs`, `ipfs/ledger.rs`, cliente `ipfs` (galaxia-fhs) |
| API de administración en loopback (`/admin/ipfs/*`) con token | `admin.rs` |
| `/health` y `/status` con el formato del TS; apagado limpio con SIGTERM | `main.rs` |

Observación de latencia pendiente de caracterización estadística:

- El plazo predeterminado de pujas es 2 s. La ventana ya termina antes si
  llega el provider preferido (caso seguro porque la regla de selección lo
  hace ganador); para elegir entre otros providers se conserva el plazo
  completo. El tiempo real se registra como `bid_wait_ms` por mission.
- Una pregunta con KB ha tardado 14–30 s en muestras del laboratorio; hay que
  separar TTFT, espera de bids, provider, prompt e inferencia antes de atribuir
  esa duración a una implementación concreta.

## Configuración

Configuración del agente Rust:

| Variable | Default |
|---|---|
| `IDENTITY_KEY_PATH` | `./.fhs-identity-navigator.json` |
| `FHS_LISTEN_ADDRS` | `/ip4/0.0.0.0/tcp/4010/tls/ws` |
| `FHS_ANNOUNCE_ADDRS`, `FHS_BOOTSTRAP_ADDRS` | vacío |
| `TLS_CERT_PATH`, `TLS_KEY_PATH` | sin TLS en la API HTTP |
| `NODE_EXTRA_CA_CERTS` | certificados de confianza adicionales |
| `HOST`, `PORT` | `127.0.0.1`, `8090`; con `HOST` fuera de loopback, `TLS_CERT_PATH`/`TLS_KEY_PATH` son obligatorios |
| `FHS_VETOED_PROVIDERS` | DIDs separados por coma |
| `ATTACHMENT_MAX_BYTES` | `20971520` (20 MB); máximo 32 MB, el tope de protocolo |
| `IPFS_API_URL` | sin IPFS (adjuntos inline); `http://127.0.0.1:5001` para el Kubo local |
| `IPFS_API_TOKEN_FILE` | obligatoria con `IPFS_API_URL`: token del Navigator |
| `IPFS_NETWORK` | `public` (o `private`) |
| `ADMIN_ADDR` | `127.0.0.1:8099`; solo loopback |

`DEFAULT_BID_DEADLINE` conserva un máximo por oferta de 2 s; requests pueden
usar otro plazo. La llegada del provider preferido cierra antes esa ventana sin
cambiar al ganador definido por la política.

Y una propia: `FHS_ADVERTISE_AS_NAVIGATOR=true` hace que se anuncie como
`navigator`. Por defecto es `false`, para poder correrlo junto al TS sin que
el Portal lo tome.

### IPFS (DEC-0095)

Si el usuario elige "Vía IPFS" en el Portal, el Navigator sube el adjunto a su
Kubo local y el OCR lo lee por el suyo. La misión pide `document.ocr` **y**
`ipfs.native.<red>`; si no hay un OCR así, o este Navigator no tiene IPFS de
esa red, el turno falla con un error claro (nunca cae a inline).

- **Libro de pines** `ipfs-pins.json` junto a la identidad (`/data`): lease
  `uploading` antes del `add` real, `active` al confirmarse; al terminar el
  turno, 30 s de gracia tras un OCR exitoso y 5 min si hubo error,
  cancelación, turno muerto o reinicio. Un barrido cada minuto quita leases
  vencidos y despinea (idempotente, con reintentos); la auditoría (al arrancar
  y cada 10 min) reporta pines ajenos sin tocarlos. Si ninguna versión del
  libro valida, queda en cuarentena (`*.corrupt-*`) y no hay unpins
  automáticos hasta que el operador la retire.
- **Cuotas:** 1 subida por sesión y 4 en total, 1 GB de CIDs únicos, repo de
  Kubo bajo el 80 % y ≥ 2 GB libres; si no, `OVERLOADED`.
- **`/status`** incluye `ipfs` (degradado, pines, unpins pendientes, pines
  ajenos o perdidos).
- **Liberar un CID `reuse`** desde el host (el contenedor usa
  `--network host`; el token está en `/data/admin.token` y pasa por stdin):

  ```sh
  podman exec fhs-navigator cat /data/admin.token | sed 's/^/Authorization: Bearer /' \
    | curl -s -H @- http://127.0.0.1:8099/admin/ipfs/pins
  podman exec fhs-navigator cat /data/admin.token | sed 's/^/Authorization: Bearer /' \
    | curl -s -X POST -H @- "http://127.0.0.1:8099/admin/ipfs/release?cid=<cid>"
  ```

  `POST /admin/ipfs/release?cid=<cid>` solo quita la marca; el siguiente
  barrido hace el unpin.

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
  -v ~/secrets/ipfs/navigator.token:/secrets/ipfs.token:ro \
  -e IPFS_API_URL=http://127.0.0.1:5001 -e IPFS_API_TOKEN_FILE=/secrets/ipfs.token \
  galaxia-agent:dev
```

El contenedor no incluye `llama.cpp`: solo Star lo invoca.
