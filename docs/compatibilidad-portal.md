# Compatibilidad con el Portal

El agente habla con `apps/portal-chat` (commit `c74ef5d` o posterior) sin
cambios en el Portal: el mismo stream `/fhs/v1/0.1.0`, los mismos Envelopes
firmados y los mismos payloads que `portal-session.ts` del Navigator TS. La
traducción vive en `src/session.rs`.

## Del Portal al agente

| Payload | Qué hace el agente |
|---|---|
| `handshake` | responde `handshakeAck` con su DID y multiaddrs |
| `agentStart` | fija preferencias de la sesión: scope, fuente de RAG, modelo, máximo de KB por pregunta |
| `chatRequest` | inicia un turno (el último mensaje debe ser del usuario y no estar vacío); con adjunto, primero OCR |
| `kbDecision` | retoma el turno que esperaba la confirmación de KB, con o sin las KB |
| `chatCancel` | aborta la tarea del turno en curso |

## Del agente al Portal

| Evento interno (`runtime/events.rs`) | Payload |
|---|---|
| `Status` | `agentStatus` |
| `LlmSelected` | `starSelected` |
| `ToolSelected` | `toolSelected` |
| `AssistantDelta` | `assistantDelta` (deltas reales de Star) |
| `AssistantCompleted` | `assistantCompleted` con procedencia (IDs, nombres, `dataExported`) |
| `OcrExtracted` | `ocrExtracted` |
| `KbRecommended` | `kbRecommended` |
| `Error` | `error` |

`ToolRunning`, `ToolCompleted`, `ToolError` y `ProviderFailover` no tienen
payload en el IDL y no salen al Portal, igual que en el TS.

Verificado el 2026-09-27 con `probe portal` contra el laboratorio: handshake →
`kbRecommended` → `kbDecision` → estados → deltas → `assistantCompleted` con
KB y RAG en la procedencia, en 14 s.
