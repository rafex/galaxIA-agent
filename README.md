# galaxIA-agent

Agente soberano de Navigator implementado en Rust sobre [Rig 0.42.0](https://docs.rs/rig/0.42.0/rig/). El agente recibe una petición ya validada por su propia política, selecciona providers dentro del scope permitido y ejecuta Missions FHS hacia Star y Satellites.

## Estado de la implementación

**Todavía no reemplaza al Navigator TS** (`galaxIA-Core/apps/navigator`),
que sigue en producción. Evaluación completa y plan por fases en
[`docs/migracion-desde-ts.md`](docs/migracion-desde-ts.md).

Hecho y probado:

- supervisor multiagente como módulos de un solo binario (`PolicyAgent`,
  `DocumentAgent`, `RetrievalAgent`, `MissionManager`, `ResponseAgent`);
- `RequestPlan` determinista: scope, fuente de RAG, límites de contexto (en
  caracteres, con rechazo de OCR completo) y tres rondas máximas de tools;
- filtro de providers por scope, orden por reputación/latencia y failover
  local al siguiente provider;
- IDL FHS canónico en `protocol/fhs-protocol.proto`, idéntico al de
  `galaxIA` (`scripts/check-idl.sh`) y generado con `prost`;
- `StarCompletionModel`, adaptador Rig `CompletionModel` hacia Star a través
  del trait `FhsTransport`.

Pendiente (no existe todavía):

- **transporte libp2p**: el único `FhsTransport` es
  `UnconfiguredFhsTransport`, que siempre falla;
- lectura real de providers: `AtlasClient` es un snapshot en memoria que
  nadie llena, así que `POST /v1/chat` responde "no hay providers";
- offer/bid/assign por GossipSub, stream directo y handshake firmados;
- sesión del Portal por libp2p (`/ws` acepta la conexión y la cierra);
- OCR, RAG por red, recomendación de KB y procedencia;
- el adaptador de Rig envía el historial como un solo mensaje JSON y sin
  tools; `RemoteToolFactory` existe pero no se registra.

La responsabilidad de cada agente y sus límites de autoridad están descritos
en [`docs/arquitectura-multiagente.md`](docs/arquitectura-multiagente.md).

## Política de seguridad y contexto

El LLM no elige privacidad, RAG, provider ni tamaño de prompt. El OCR completo no forma parte del `RequestPlan`: solo se aceptan fragmentos recuperados (`DocumentChunk`). La ruta oficial del modelo es Star/FHS. `llama.cpp` queda fuera de este repositorio y solo Star puede invocarlo.

## Desarrollo

```sh
cargo fmt --check
cargo check
cargo test
```

El binario escucha en `0.0.0.0:8090` por defecto. Se puede cambiar con `GALAXIA_AGENT_BIND`.

## Contenedor

```sh
podman build -t galaxia-agent:dev .
podman run --rm --network host -e GALAXIA_AGENT_BIND=0.0.0.0:8090 galaxia-agent:dev
```

El contenedor no instala ni ejecuta `llama.cpp`; el adaptador FHS necesita conectarse al Star anunciado por la red.
