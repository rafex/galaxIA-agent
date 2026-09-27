use crate::policy::{DocumentChunk, RagSource, RequestPlan};
use std::collections::HashSet;

/// Contexto que el agente de respuesta puede entregar a Star.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrievedContext {
    pub source: RagSource,
    pub chunks: Vec<DocumentChunk>,
}

/// Agente de recuperación.
///
/// En el MVP recibe los fragmentos producidos por el RAG local del Portal o
/// por el Satellite RAG. La consulta de red se añadirá como una Mission FHS;
/// esta capa ya garantiza orden estable y deduplicación antes de construir el
/// prompt.
#[derive(Clone, Debug, Default)]
pub struct RetrievalAgent;

impl RetrievalAgent {
    pub fn retrieve(&self, plan: &RequestPlan) -> RetrievedContext {
        let mut seen = HashSet::new();
        let mut chunks: Vec<_> = plan
            .context
            .iter()
            .filter(|chunk| seen.insert(chunk.chunk_id.clone()))
            .cloned()
            .collect();
        chunks.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.chunk_index.cmp(&b.chunk_index))
        });
        RetrievedContext {
            source: plan.rag_source.clone(),
            chunks,
        }
    }

    pub fn apply(&self, plan: &mut RequestPlan) -> RetrievedContext {
        let context = self.retrieve(plan);
        plan.context = context.chunks.clone();
        context
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{AgentRequest, DocumentChunk, ModelPreferences};

    #[test]
    fn deduplicates_and_orders_retrieved_chunks() {
        let request = AgentRequest {
            conversation_id: "c".into(),
            request_id: "r".into(),
            message: "pregunta".into(),
            preferences: ModelPreferences::default(),
            document_id: None,
            document_context: vec![
                DocumentChunk {
                    chunk_id: "b".into(),
                    filename: "x".into(),
                    chunk_index: 1,
                    text: "bajo".into(),
                    score: 0.2,
                    source: None,
                },
                DocumentChunk {
                    chunk_id: "a".into(),
                    filename: "x".into(),
                    chunk_index: 0,
                    text: "alto".into(),
                    score: 0.9,
                    source: None,
                },
                DocumentChunk {
                    chunk_id: "a".into(),
                    filename: "x".into(),
                    chunk_index: 0,
                    text: "duplicado".into(),
                    score: 0.9,
                    source: None,
                },
            ],
            attachments: vec![],
        };
        let plan = RequestPlan::build(request).unwrap();
        let result = RetrievalAgent.retrieve(&plan);
        assert_eq!(result.chunks.len(), 2);
        assert_eq!(result.chunks[0].chunk_id, "a");
    }
}
