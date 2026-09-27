# Migración desde el Navigator TS

Evaluación del 2026-09-27 (lectura de código de los dos repos) y plan para
que `galaxIA-agent` reemplace a `galaxIA-Core/apps/navigator`, que sigue en
producción en Bastion.

## Dónde estamos

| | Navigator TS | galaxIA-agent |
|---|---|---|
| Tamaño | 3,621 líneas + `packages/fhs-node` | ~1,300 líneas |
| Transporte libp2p (WSS+TLS, Noise, yamux) | ✅ | ❌ sin dependencia libp2p |
| Descubrimiento: `NodeAdvertise` firmado por GossipSub → PeerCache | ✅ | ❌ snapshot vacío |
| offer / bid / assign firmados | ✅ | ❌ solo orden local |
| Stream directo `/fhs/v1/0.1.0` + handshake + Envelope firmado | ✅ | ❌ |
| Sesión del Portal por libp2p | ✅ | ❌ (`/ws` cierra al conectar) |
| OCR determinista con failover, RAG por red, KB (SPEC-KB-0002), procedencia | ✅ | ❌ |
| Streaming de Star al Portal | ✅ desde E2E-030 | ❌ (adaptador Rig con streaming simulado) |
| Política (scope, límites, rondas) | ✅ | ✅ |
| IDL | vía SDK | ✅ idéntico, verificado por sha256 |

## Lo que no hay que copiar del TS

- `chatCancel` no aborta el turno en curso.
- El proceso no atiende SIGTERM: `podman stop` espera 10 s y lo mata (el
  agente Rust ya se apaga limpio desde la fase 0).
- `PeerCache` no expira entradas por TTL ni por `lastSeen`.
- El LLM recibe solo system + un mensaje: no hay historial de conversación.
- `ProvenanceInfo` en el wire solo transporta IDs, sin nombres, y
  `dataExported` siempre viaja como `false`.
- Código muerto: `observability/trace.ts`, `identity.ts`, el filtro de
  capacidad y `recordSample` en P2P.

Ya corregido en TS (E2E-030): el streaming al Portal, el proveedor elegido
que no era el que ejecutaba, y el JSON del selector de KB que se habría visto
en el chat.

## Plan por fases

El TS sigue en producción hasta la fase 5. El agente Rust corre en paralelo
con **otra identidad y sin anunciarse como `navigator`**: el Portal usa
cualquier `NodeAdvertise` con `provider.id == "navigator"`. Todo en un solo
binario hasta el cambio.

| Fase | Qué | Compuerta |
|---|---|---|
| 0 ✅ | Fixtures dorados generados desde el TS: Envelopes, `NodeAdvertise`, offer/bid/assign firmados, `DynamicValue` (`tests/fixtures/wire.json`, generador en `galaxIA-Core/apps/navigator/scripts/export-wire-fixtures.ts`) | El Rust decodifica todo y verifica las firmas |
| 1 | Nodo rust-libp2p observador: WSS+TLS, Noise, yamux, Kademlia, GossipSub, reconexión al bootstrap, PeerCache con TTL, `/status` | En Bastion ve los mismos providers que el TS |
| 2 | Misión de chat a Star con streaming real; adaptador Rig con roles y tools reales | Un comando de prueba recibe deltas de Star |
| 3 | Tools: OCR con failover al siguiente provider, RAG por red, KB (portar `kb-matching.ts` y sus tests), procedencia | Mismas preguntas del laboratorio, mismo resultado que TS |
| 4 | Sesión del Portal por libp2p: todos los casos de `portal-session.ts` | El Portal funciona sin cambios contra el Rust |
| 5 | Cambio en Bastion con reversa (misma identidad `navigator-data`, imagen TS etiquetada) | `doctor.sh` en verde, chat/OCR/KB desde el navegador, reinicio de Bastion |

## Resultado de la fase 0 (2026-09-27)

`tests/fixtures/wire.json` (5 mensajes GossipSub, 4 Envelopes, 2
`DynamicValue`, llave de semilla fija) y `tests/wire_fixtures.rs`:

- Las cadenas de firma de `src/signing.rs` son idénticas a las del TS y las
  firmas Ed25519 del TS verifican en Rust. La llave pública sale del DID.
- `NodeAdvertise`, offer, bid y assign se re-codifican con prost byte por
  byte igual que protobuf-es. Ninguno contiene `map`, así que re-codificar
  para calcular el sha256 del beacon es seguro.
- **Hallazgo:** `DynamicObject.fields` y `ToolInputSchema.properties` son
  `map`. protobuf-es los escribe en orden de inserción y prost (`HashMap`)
  en orden arbitrario. Un Envelope con argumentos de tool de varias claves
  **no verificaría re-codificando**. Por eso los Envelopes recibidos se
  verifican con `verify_envelope_bytes`, que toma el payload crudo
  (`raw_envelope_payload`). Al enviar, se firma y se manda la misma
  codificación.
- Regenerar los fixtures:
  `cd galaxIA-Core/apps/navigator && npx tsx scripts/export-wire-fixtures.ts ../../../galaxIA-agent/tests/fixtures/wire.json`

## Coordinación

Codex y Claude no deben trabajar a la vez en el mismo checkout: el
2026-09-27 un commit de uno (`8b7b199`) se llevó cambios sin commitear del
otro.
