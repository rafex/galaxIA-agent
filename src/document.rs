use crate::policy::{AttachmentRef, DocumentChunk, RequestPlan};
use thiserror::Error;

/// Resultado de la inspección determinista de documentos.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentWork {
    pub document_id: Option<String>,
    pub attachments: Vec<AttachmentRef>,
    pub chunks: Vec<DocumentChunk>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DocumentError {
    #[error("el adjunto {filename} supera el límite de {limit_bytes} bytes")]
    AttachmentTooLarge {
        filename: String,
        limit_bytes: usize,
    },
    #[error("un fragmento documental no tiene chunkId")]
    MissingChunkId,
}

/// Agente especializado en la frontera de documentos.
///
/// La extracción OCR real pertenece al Satellite correspondiente. Este agente
/// solo valida referencias y fragmentos ya recuperados; por diseño nunca copia
/// el binario ni el OCR completo hacia el prompt del LLM.
#[derive(Clone, Debug)]
pub struct DocumentAgent {
    max_attachment_bytes: usize,
}

impl Default for DocumentAgent {
    fn default() -> Self {
        Self {
            max_attachment_bytes: 25 * 1024 * 1024,
        }
    }
}

impl DocumentAgent {
    pub fn new(max_attachment_bytes: usize) -> Self {
        Self {
            max_attachment_bytes,
        }
    }

    pub fn inspect(&self, plan: &RequestPlan) -> Result<DocumentWork, DocumentError> {
        for attachment in &plan.attachments {
            if attachment.size_bytes > self.max_attachment_bytes {
                return Err(DocumentError::AttachmentTooLarge {
                    filename: attachment.filename.clone(),
                    limit_bytes: self.max_attachment_bytes,
                });
            }
        }
        for chunk in &plan.context {
            if chunk.chunk_id.trim().is_empty() {
                return Err(DocumentError::MissingChunkId);
            }
        }
        Ok(DocumentWork {
            document_id: plan.document_id.clone(),
            attachments: plan.attachments.clone(),
            chunks: plan.context.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{AgentRequest, ModelPreferences, RagSource};

    #[test]
    fn inspects_references_without_copying_full_document() {
        let request = AgentRequest {
            conversation_id: "c".into(),
            request_id: "r".into(),
            message: "resume".into(),
            preferences: ModelPreferences {
                rag_source: RagSource::Network,
                ..ModelPreferences::default()
            },
            document_id: Some("doc-1".into()),
            document_context: vec![DocumentChunk {
                chunk_id: "chunk-1".into(),
                filename: "document.pdf".into(),
                chunk_index: 0,
                text: "fragmento relevante".into(),
                score: 0.9,
                source: Some("network".into()),
            }],
            attachments: vec![AttachmentRef {
                filename: "document.pdf".into(),
                size_bytes: 1024,
                digest: "sha256:test".into(),
            }],
        };
        let plan = crate::policy::RequestPlan::build(request).unwrap();
        let work = DocumentAgent::default().inspect(&plan).unwrap();
        assert_eq!(work.chunks.len(), 1);
        assert_eq!(work.attachments.len(), 1);
        assert!(work.chunks[0].text.len() < 100);
    }
}
