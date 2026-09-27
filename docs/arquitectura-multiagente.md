# Arquitectura de galaxIA-agent

## Objetivo

El agente no debe volverse un bloque que decida, procese documentos, consulte
conocimiento y ejecute Missions al mismo tiempo sin fronteras claras. Las
responsabilidades se separan por módulo dentro de **un solo binario** hasta
el cambio en Bastion (decisión de la evaluación de migración). Separarlas en
procesos queda para cuando la carga o la topología lo justifiquen, y en ese
caso se comunican con Missions FHS.

```text
Portal (navegador, js-libp2p)
    │  stream /fhs/v1/0.1.0
    ▼
session.rs ─── una sesión por stream; un turno = una tarea (cancelable)
    │
    ▼
runtime/agent.rs (AgentRuntime) ─── decide, en código, qué pasa en el turno
    ├── runtime/providers.rs  → qué providers permite el scope; tools por provider
    ├── runtime/kb.rs         → recomendación de KB por cobertura
    ├── llm.rs (StarModel)    → generación, solo a través de Star
    └── p2p/client.rs         → misiones de chat y de tools
            │
            ▼
        p2p/mission.rs  → offer → bids → assign (GossipSub, firmados)
        p2p/node.rs     → swarm libp2p, caché de providers, streams
```

## Responsabilidades y límites

| Módulo | Decide | No decide |
|---|---|---|
| `session.rs` | traducir entre Envelopes del Portal y turnos; cancelar | nada del contenido del turno |
| `runtime/agent.rs` | orden del turno: OCR, KB, RAG, Star, tools; procedencia | la política de un provider remoto |
| `runtime/providers.rs` | qué providers caben en el scope pedido y qué tools anuncian | cuál gana la subasta |
| `runtime/kb.rs` | qué KB recomendar para la pregunta | si se usa: lo confirma el usuario (`kbDecision`) |
| `p2p/mission.rs` | ganador: el preferido si pujó; si no, confianza → reputación → latencia | qué pedirle al ganador |
| `llm.rs` | cómo pedir generación a Star (roles, tools, streaming) | scope, RAG, tools autorizadas |
| LLM en Star | la redacción de la respuesta y qué tool ofrecida llamar | cualquier política del sistema |

Reglas que el código ya aplica:

- El texto OCR completo no entra al prompt: se indexa en el RAG (del
  navegador o de la red) y al prompt solo llegan fragmentos recuperados.
- El LLM no elige scope, RAG, provider ni tamaño de contexto; eso lo fija la
  sesión con las preferencias del Portal.
- Una sola ronda de tools por turno, como el TS.
- Los Envelopes internos y los mensajes de Rig nunca llegan al Portal: la
  sesión los traduce a los eventos que `chat-view.ts` ya conoce.

## Reglas para extensiones

1. Todo módulo nuevo declara su entrada, su salida y su autoridad en la
   tabla de arriba.
2. Lo determinista se valida antes de invocar un LLM.
3. Entre procesos, Missions FHS; nada de HTTP privado ad hoc.
4. Cada reintento o failover conserva los identificadores de correlación.
5. No se agrega un agente autónomo si una función determinista basta.
