# Compatibilidad con Portal

El contrato que debe preservar el corte Rust es el stream de eventos que hoy
consume `apps/portal-chat/src/components/chat-view.ts`. El agente no debe
exponer envelopes Protobuf ni decisiones internas en la UI.

| Evento | Origen Rust | Correlación mínima |
| --- | --- | --- |
| `session` | apertura/cierre del stream Portal | `conversationId`, `requestId` |
| `agent.status` | fases del `RequestPlan` y Mission | `conversationId`, `requestId`, `missionId` |
| `llm.selected` | bid ganador de Star | `providerId`, `missionId` |
| `tool.selected` | capability autorizada | `providerId`, `missionId` |
| `tool.running` / `tool.completed` / `tool.error` | stream Satellite | `toolCallId`, `providerId`, `missionId` |
| `assistant.delta` / `assistant.completed` | stream Star | `providerId`, `missionId` |
| `ocr.extracted` | OCR determinístico | `documentId`, `missionId` |
| `node.online` / `node.lost` | Beacon/DHT/GossipSub local | `providerId` |
| `error` | política, transporte o provider | todos los IDs disponibles |

La primera capa Rust define `AgentEvent` con esos correladores. La adaptación
final a `Envelope.assistant_*`/`Envelope.error` se hará en el transporte FHS;
los campos internos de Rig y los envelopes no se envían al Portal.
