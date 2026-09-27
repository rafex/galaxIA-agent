# Migración desde el Navigator TS

Evaluación del 2026-09-27 (lectura de código de los dos repos) y plan para
que `galaxIA-agent` reemplace a `galaxIA-Core/apps/navigator`, que sigue en
producción en Bastion.

## Dónde estamos (2026-09-27)

| | Navigator TS | galaxIA-agent |
|---|---|---|
| Tamaño | 3,621 líneas + `packages/fhs-node` | ~5,700 líneas (con tests) |
| Transporte libp2p (WSS+TLS, Noise, yamux) | ✅ | ✅ |
| Descubrimiento: `NodeAdvertise` firmado por GossipSub → caché | ✅ sin expiración | ✅ con TTL + 30 s de gracia |
| offer / bid / assign firmados | ✅ | ✅ |
| Stream directo `/fhs/v1/0.1.0` + handshake + Envelope firmado | ✅ | ✅ |
| Sesión del Portal por libp2p | ✅ | ✅ |
| OCR con failover, RAG por red, KB (SPEC-KB-0002), procedencia | ✅ (el RAG por red nunca aportó: ver abajo) | ✅ |
| Streaming de Star al Portal | ✅ desde E2E-030 | ✅ |
| `chatCancel` aborta el turno | ❌ | ✅ |
| Apagado limpio con SIGTERM | ❌ | ✅ |
| Beacon en el DHT | ✅ desde E2E-032 (antes el put agotaba el tiempo) | ✅ firmado, guardado en Atlas |
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
| 1 ✅ | Nodo rust-libp2p observador: WSS+TLS, Noise, yamux, Kademlia, GossipSub, reconexión al bootstrap, PeerCache con TTL, `/status` | En Bastion ve los mismos providers que el TS |
| 2 ✅ | Misión de chat a Star con streaming real; adaptador Rig con roles y tools reales | Un comando de prueba recibe deltas de Star |
| 3 ✅ | Tools: OCR con failover al siguiente provider, RAG por red, KB (portar `kb-matching.ts` y sus tests), procedencia | Mismas preguntas del laboratorio, mismo resultado que TS |
| 4 ✅ | Sesión del Portal por libp2p: todos los casos de `portal-session.ts` | El Portal funciona sin cambios contra el Rust |
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

## Resultado de las fases 1 a 4 (2026-09-27)

Verificado desde el Mac contra el laboratorio (Atlas, Star, KB, RAG y OCR en
TS), con el agente sin anunciarse como `navigator`:

- **Fase 1.** Se une por WSS+TLS al Atlas de Bastion y ve a Star, KB, RAG y
  OCR con sus firmas verificadas. El certificado del laboratorio es
  autofirmado con `CA:TRUE` y rustls lo rechaza como certificado de hoja
  (`CaUsedAsEndEntity`): se resolvió con un verificador que acepta el
  certificado exacto de `NODE_EXTRA_CA_CERTS` (pin) y cae a webpki para el
  resto. Hace falta una copia de `libp2p-websocket` 0.46.0 con
  `tls::Config::from_rustls` (`vendor/libp2p-websocket/GALAXIA-PATCH.md`).
- **Fase 2.** `probe chat` y `probe rig` reciben deltas reales de Star.
- **Fase 3.** `probe tool` contra KB, RAG y OCR; `probe turn` recomienda la
  KB, la consulta, fusiona con RAG y la respuesta cita el artículo 3.
- **Fase 4.** `probe portal`: handshake → `kbRecommended` → `kbDecision` →
  deltas → `assistantCompleted` con KB y RAG en la procedencia, en 14 s.

Encontrado en el camino y **no** copiado del TS: `queryRagContext` espera
`{chunks}` pero el RAG devuelve un arreglo, así que en el TS el RAG por red
nunca aportó contexto. El Rust lee el arreglo.

Pendiente, sin bloquear la fase 5: cada misión espera los 2 s del plazo de
pujas (una pregunta con KB tarda 14–30 s) y los artefactos solo viajan en
línea (sin IPFS).

## Prueba con el cliente del Portal (2026-09-27)

`galaxIA-Core/apps/portal-chat/tests/e2e` corre el código de sesión del
navegador (js-libp2p, handshake, verificación de firmas re-codificando con
protobuf-es) contra una multiaddr. Contra el agente en sombra en Bastion
(`25b4e35`, por túnel SSH) pasan las tres, sin ninguna firma rechazada:

| Caso | Rust | TS en producción |
|---|---|---|
| Streaming con la KB recomendada | ✅ 13.7 s | ✅ |
| Adjunto (OCR) con RAG de red | ✅ 15.7 s | ❌ sin RAG en la procedencia (E2E-031, corregido sin desplegar) |
| Adjunto (OCR) con RAG local | ✅ 12.1 s | ✅ |

Lo que encontró y ya está corregido: a los 23 s del arranque el agente
todavía no conocía a Star ni al RAG y fallaba; ahora, durante los primeros
35 s, una búsqueda vacía espera el anuncio (`PeerCache::settle`). Con las
pruebas lanzadas 2 s después del arranque pasan igual.

El beacon del DHT: el Rust lo guardaba en Atlas, pero el Portal nunca
completaba la consulta porque kad-dht de js-libp2p descarta por defecto las
direcciones privadas y dejaba vacía su tabla de rutas (E2E-032, afectaba
también al TS). Corregido en `galaxIA-Core` `3cd80fa`: el Portal lee el
beacon firmado del Rust en ~1 s.
El Portal no envía `chatCancel`, así que la cancelación no forma parte del
flujo real.

## Fase 5: cambio en Bastion

1. Construir la imagen en Bastion y etiquetar la del TS para volver
   (`galaxia-navigator:ts-rollback`).
2. Correrla en sombra: otra identidad, otros puertos, sin
   `FHS_ADVERTISE_AS_NAVIGATOR`. Probar con `probe portal`.
3. Cambio: detener `fhs-navigator` (TS) y arrancar el agente con el volumen
   `navigator-data` (mismo DID y PeerId), los mismos puertos (4010, 8090),
   `FHS_ADVERTISE_AS_NAVIGATOR=true` y `--restart always`.
4. Compuerta: `doctor.sh` en verde; chat, OCR y KB desde el navegador; un
   reinicio de Bastion.
5. Reversa: detener el agente y arrancar de nuevo `fhs-navigator`.

## Coordinación

Codex y Claude no deben trabajar a la vez en el mismo checkout: el
2026-09-27 un commit de uno (`8b7b199`) se llevó cambios sin commitear del
otro.
