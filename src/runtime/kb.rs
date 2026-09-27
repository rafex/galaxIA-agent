//! Recomendación de bases de conocimiento (SPEC-KB-0001/0002). Puerto de
//! `kb-matching.ts`: cobertura de la pregunta sobre la descripción, sin
//! acentos ni palabras vacías, con variantes por sufijo.

use std::collections::BTreeSet;

use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

/// `knowledge.query` (providers P2P) y `kb.query` (SPEC-KB-0001), E2E-028.
pub const KB_CAPABILITY_IDS: [&str; 2] = ["knowledge.query", "kb.query"];

pub fn is_kb_capability(id: &str) -> bool {
    KB_CAPABILITY_IDS.contains(&id)
}

/// Fracción mínima de palabras de la pregunta que deben aparecer en la KB.
pub const KB_MATCH_THRESHOLD: f64 = 0.2;

const STOPWORDS: &[&str] = &[
    "el",
    "la",
    "los",
    "las",
    "un",
    "una",
    "unos",
    "unas",
    "lo",
    "al",
    "del",
    "de",
    "en",
    "con",
    "por",
    "para",
    "sin",
    "sobre",
    "entre",
    "hacia",
    "hasta",
    "desde",
    "segun",
    "y",
    "e",
    "o",
    "u",
    "ni",
    "que",
    "pero",
    "si",
    "no",
    "se",
    "su",
    "sus",
    "mi",
    "mis",
    "tu",
    "tus",
    "me",
    "te",
    "le",
    "les",
    "nos",
    "yo",
    "ella",
    "ello",
    "ellos",
    "ellas",
    "usted",
    "este",
    "esta",
    "estos",
    "estas",
    "ese",
    "esa",
    "eso",
    "esos",
    "esas",
    "es",
    "son",
    "ser",
    "hay",
    "como",
    "cual",
    "cuales",
    "quien",
    "quienes",
    "cuando",
    "donde",
    "cuanto",
    "cuanta",
    "cuantos",
    "cuantas",
    "muy",
    "mas",
    "dice",
    "dicen",
    "decir",
    "dime",
    "digame",
    "explica",
    "explicame",
    "explicar",
    "puedes",
    "podrias",
    "quiero",
    "saber",
    "sabes",
    "busca",
    "buscar",
    "ayuda",
    "ayudame",
    "habla",
    "hablame",
    "the",
    "a",
    "an",
    "of",
    "in",
    "on",
    "for",
    "to",
    "and",
    "or",
    "is",
    "are",
    "what",
    "which",
    "who",
    "how",
    "does",
    "do",
    "about",
    "tell",
    "please",
];

fn normalize(text: &str) -> String {
    text.to_lowercase()
        .nfd()
        .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
        .map(|c| {
            if c.is_alphanumeric() || c.is_whitespace() {
                c
            } else {
                ' '
            }
        })
        .collect()
}

/// Palabras con contenido: sin acentos ni palabras vacías; números sí.
pub fn content_tokens(text: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for token in normalize(text).split_whitespace() {
        if STOPWORDS.contains(&token) {
            continue;
        }
        let numeric = token.chars().all(|c| c.is_numeric());
        if (token.chars().count() > 1 || numeric) && seen.insert(token.to_string()) {
            out.push(token.to_string());
        }
    }
    out
}

fn same_word(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let (short, long) = if a.chars().count() <= b.chars().count() {
        (a, b)
    } else {
        (b, a)
    };
    let (sl, ll) = (short.chars().count(), long.chars().count());
    sl >= 5 && long.starts_with(short) && ll - sl <= 2
}

/// Fracción de las palabras con contenido de la pregunta presentes en la KB.
pub fn match_score(question: &str, kb_text: &str) -> f64 {
    let question = content_tokens(question);
    if question.is_empty() {
        return 0.0;
    }
    let kb = content_tokens(kb_text);
    let matched = question
        .iter()
        .filter(|q| kb.iter().any(|k| same_word(q, k)))
        .count();
    matched as f64 / question.len() as f64
}

/// Descripción + etiquetas de tema (no los `tool:<nombre>`).
pub fn match_text(description: &str, tags: &[String]) -> String {
    std::iter::once(description.to_string())
        .chain(tags.iter().filter(|t| !t.starts_with("tool:")).cloned())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Fragmentos de una respuesta de KB: arreglo o `{ chunks }` (E2E-029).
pub fn chunks_from(value: &Value) -> Vec<Value> {
    let list = match value {
        Value::Array(items) => items.clone(),
        Value::Object(map) => map
            .get("chunks")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        _ => vec![],
    };
    list.into_iter()
        .filter(|c| {
            c.get("text")
                .and_then(Value::as_str)
                .is_some_and(|t| !t.trim().is_empty())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const CONSTITUCION: &str = "Constitución Política de los Estados Unidos Mexicanos: derechos humanos, educación, soberanía nacional y forma de gobierno (artículos 1, 3, 39 y 40)";

    #[test]
    fn tokens_drop_accents_stopwords_and_keep_numbers() {
        assert_eq!(
            content_tokens("¿Qué dice el artículo 3 sobre la educación?"),
            ["articulo", "3", "educacion"]
        );
    }

    #[test]
    fn recommends_the_constitution_for_the_lab_question_only() {
        assert!(
            match_score("¿Qué dice el artículo 3 sobre la educación?", CONSTITUCION)
                >= KB_MATCH_THRESHOLD
        );
        assert!(match_score("que hora es en españa", CONSTITUCION) < KB_MATCH_THRESHOLD);
        assert_eq!(match_score("¿qué es?", CONSTITUCION), 0.0);
    }

    #[test]
    fn suffix_variants_but_not_short_prefixes() {
        assert_eq!(match_score("articulos", "el artículo"), 1.0);
        assert_eq!(match_score("naciones", "nación"), 1.0);
        assert_eq!(match_score("ley", "leyenda"), 0.0);
    }

    #[test]
    fn ignores_tool_tags_and_accepts_both_capability_ids() {
        assert_eq!(
            match_score("query", &match_text("Recetas", &["tool:kb_query".into()])),
            0.0
        );
        assert!(
            is_kb_capability("kb.query")
                && is_kb_capability("knowledge.query")
                && !is_kb_capability("document.query")
        );
    }

    #[test]
    fn chunks_from_array_or_object() {
        let chunk = json!({"text": "Artículo 3.", "score": 0.4});
        assert_eq!(chunks_from(&json!([chunk.clone()])), vec![chunk.clone()]);
        assert_eq!(
            chunks_from(&json!({"chunks": [chunk.clone()]})),
            vec![chunk]
        );
        assert!(chunks_from(&json!([{"text": " "}, {"score": 1}])).is_empty());
    }
}
