# Arquitectura multiagente de galaxIA

## Objetivo

El agente Rust no debe convertirse en un bloque monolítico que decida,
procese documentos, consulte conocimiento y ejecute Missions al mismo tiempo.
La arquitectura recomendada separa las responsabilidades por dominio y
mantiene un supervisor determinista como punto de coordinación.

```text
Portal Chat
    │
    ▼
SupervisorAgent (Navigator Rust/Rig)
    ├── PolicyAgent       → privacidad, autorización y límites
    ├── DocumentAgent     → referencias, adjuntos y frontera OCR
    ├── RetrievalAgent    → RAG local o RAG de GalaxIA
    ├── MissionManager    → oferta, bids, assign, timeout y failover
    └── ResponseAgent     → generación exclusivamente mediante Star/FHS
```

La separación es lógica en la primera etapa. Cada componente tiene una
interfaz Rust y puede convertirse posteriormente en un proceso o contenedor
independiente cuando la carga, el aislamiento o la topología de la red lo
justifiquen.

## Responsabilidades

### SupervisorAgent

Es la fachada que conserva el contrato del Navigator. Recibe la petición del
Portal, crea el `RequestPlan`, invoca a los agentes especializados y publica
los eventos correlacionados con `conversationId`, `requestId`, `missionId` y
`providerId`.

El supervisor coordina; no debe implementar las reglas internas de cada
dominio.

### PolicyAgent

Es determinista y no consulta al LLM. Valida:

- identidad de la conversación y de la petición;
- scope de privacidad;
- origen del RAG;
- tamaño máximo de contexto;
- número máximo de rondas de tools.

El LLM no puede cambiar estas decisiones. El resultado es un `RequestPlan` que
los agentes posteriores reciben como contrato de ejecución.

### DocumentAgent

Controla la frontera de documentos. Valida tamaños de adjuntos y que cada
fragmento recuperado tenga `chunkId`. (Objetivo, aún no implementado:
verificar digests de adjuntos.) La extracción OCR pertenece al Satellite OCR y se
coordina mediante una Mission FHS.

Regla obligatoria: el OCR completo nunca entra al prompt. El agente solo
entrega a recuperación y respuesta fragmentos identificados por `chunkId`.

### RetrievalAgent

Selecciona el contexto que ya fue autorizado por el `RequestPlan`:

- `Local`: fragmentos enviados por el RAG local del navegador;
- `Network`: fragmentos obtenidos del RAG de GalaxIA mediante FHS.

En esta primera etapa deduplica y ordena los fragmentos por relevancia. La
consulta remota será una Mission especializada; el agente no hace llamadas
directas al LLM ni mezcla automáticamente RAG local y de red.

### MissionManager

Administra el ciclo operativo FHS:

```text
MissionOffer → bids → MissionAssign → stream → completed/error
```

Selecciona providers autorizados, aplica timeout, limita intentos y realiza
failover o reasignación antes de propagar un error al Portal.

### ResponseAgent

Es el único componente que solicita generación. Usa el adaptador
`StarCompletionModel` de Rig y se comunica con Star mediante `FhsTransport`.
No llama directamente a `llama.cpp` y no puede seleccionar por sí mismo el
scope, el RAG, el provider ni el tamaño del contexto.

## Flujo de una petición documental

```text
Portal
  │
  ▼
SupervisorAgent
  │ 1. PolicyAgent → RequestPlan
  │ 2. DocumentAgent → valida adjunto/referencias
  │ 3. MissionManager → OCR Satellite
  │ 4. RetrievalAgent → recupera top-k fragmentos
  │ 5. MissionManager → Star
  ▼
Respuesta + procedencia + eventos Portal
```

Este es el flujo **objetivo**: hoy no hay transporte FHS, así que los pasos 3
y 5 no se ejecutan contra la red (ver README, "Pendiente").

El flujo de RAG local omite la Mission OCR/RAG de red cuando el navegador ya
entregó los fragmentos. El flujo de RAG de GalaxIA crea Missions hacia los
providers anunciados por Atlas.

## Límites de autoridad

| Componente | Puede decidir | No puede decidir |
| --- | --- | --- |
| `PolicyAgent` | scope, límites, autorización | contenido de la respuesta |
| `DocumentAgent` | validez y tamaño de referencias | política de privacidad |
| `RetrievalAgent` | orden y deduplicación de fragmentos | modelo o provider de generación |
| `MissionManager` | provider autorizado, timeout y failover | modificar el prompt para saltarse políticas |
| `ResponseAgent` | cómo pedir generación a Star | scope, RAG, tools autorizadas o límites |
| LLM en Star | redacción de la respuesta | cualquier política del sistema |

## Estado de esta etapa

La primera implementación deja estos componentes como módulos Rust dentro de
`galaxIA-agent`:

- `src/agent.rs`: fachada `SovereignAgent`, que actúa como supervisor;
- `src/policy.rs`: `PolicyAgent` y `RequestPlan`;
- `src/document.rs`: `DocumentAgent`;
- `src/retrieval.rs`: `RetrievalAgent`;
- `src/mission.rs`: `MissionManager`;
- `src/response.rs`: `ResponseAgent` y acceso a Star mediante Rig.

La separación de módulos no cambia todavía el IDL, el transporte FHS ni la
topología de contenedores. El siguiente corte puede extraer `DocumentAgent`,
`RetrievalAgent` y `MissionManager` como providers/satellites independientes
cuando sus transportes FHS reales estén disponibles.

## Reglas para futuras extensiones

1. Todo nuevo agente debe declarar explícitamente su entrada, salida y
   autoridad.
2. Los agentes deterministas deben validar antes de invocar cualquier LLM.
3. La comunicación entre procesos debe usar Missions FHS, no HTTP privado
   ad-hoc.
4. Ningún agente debe enviar envelopes internos de Rig al Portal.
5. Cada retry o reasignación debe conservar los identificadores de correlación.
6. No se agrega un agente autónomo si una función determinista basta para
   resolver el problema.
