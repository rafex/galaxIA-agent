# galaxIA-agent

Agente soberano de Navigator implementado en Rust sobre [Rig 0.42.0](https://docs.rs/rig/0.42.0/rig/). El agente recibe una petición ya validada por su propia política, selecciona providers dentro del scope permitido y ejecuta Missions FHS hacia Star y Satellites.

## Estado de la implementación

Esta primera entrega contiene la base ejecutable y testeable del corte controlado:

- `RequestPlan` determinista: scope, RAG, límite de contexto y tres rondas máximas.
- Snapshot de providers compatible con discovery de Atlas.
- `MissionOffer → bid/selección → assign → ejecución`, con timeout y failover.
- IDL FHS canónico completo versionado en `protocol/fhs-protocol.proto` y generado con `prost`.
- `StarCompletionModel`, un adaptador Rig `CompletionModel` que solo habla con Star mediante `FhsTransport`.
- herramientas remotas Rig dinámicas para capabilities anunciadas por Satellites.
- API HTTP/WebSocket mínima de transición en `8090`.

La implementación productiva de libp2p FHS, la lectura real del snapshot Atlas y la traducción de todos los eventos al stream Portal son los siguientes cortes de integración; no se simulan como llamadas directas a `llama.cpp`.

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
