//! Conformidad en el límite de salida (SPEC-AUTH-0001): el contenido del
//! usuario solo sale del Navigator por el `Dispatcher`, que exige un `Grant`.
//! Este archivo vigila que ningún otro código llame a las funciones crudas del
//! SDK (ofertas, streams con providers, el Star, IPFS) ni consuma permisos.

use std::fs;
use std::path::{Path, PathBuf};

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Líneas de código (sin comentarios) con su número.
fn code_lines(path: &Path) -> Vec<(usize, String)> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .enumerate()
        .map(|(i, l)| (i + 1, l.trim().to_string()))
        .filter(|(_, l)| !l.starts_with("//") && !l.is_empty())
        .collect()
}

fn offenders(patterns: &[&str], allowed: &[&str]) -> Vec<String> {
    let mut files = Vec::new();
    rust_files(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let mut found = Vec::new();
    for file in files {
        let relative = file
            .strip_prefix(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if allowed.iter().any(|a| relative.ends_with(a)) {
            continue;
        }
        for (line, text) in code_lines(&file) {
            if patterns.iter().any(|p| text.contains(p)) {
                found.push(format!("{relative}:{line}: {text}"));
            }
        }
    }
    found
}

#[test]
fn only_the_dispatcher_sends_content_to_other_nodes() {
    let found = offenders(
        &[
            "client::call_tool(",
            "client::chat(",
            "run_mission_cycle(",
            ".complete_streaming(",
            "StarModel::new(",
            "client::open_session(",
        ],
        // `llm.rs` define StarModel (pub(crate)); el resto solo lo usa el Dispatcher.
        &["src/authorization/dispatcher.rs", "src/llm.rs"],
    );
    assert!(
        found.is_empty(),
        "estas líneas envían fuera del Dispatcher:\n{}",
        found.join("\n")
    );
}

#[test]
fn only_the_dispatcher_uploads_to_ipfs() {
    let found = offenders(
        &[".upload("],
        // `ipfs/` define el servicio y lo prueba; el único llamador es el Dispatcher.
        &["src/authorization/dispatcher.rs", "src/ipfs/mod.rs"],
    );
    assert!(
        found.is_empty(),
        "estas líneas suben a IPFS fuera del Dispatcher:\n{}",
        found.join("\n")
    );
}

#[test]
fn only_the_authorization_module_mints_or_consumes_grants() {
    let found = offenders(
        &["Grant {", ".consume(", "GrantInner {"],
        &[
            "src/authorization/mod.rs",
            "src/authorization/dispatcher.rs",
            "src/authorization/tests.rs",
        ],
    );
    assert!(
        found.is_empty(),
        "estas líneas crean o consumen permisos fuera de authorization/:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_runtime_has_no_ungated_way_to_reach_the_sdk_client() {
    // El runtime no importa el cliente de misiones del SDK.
    let found = offenders(
        &["p2p::client", "use crate::p2p::{client"],
        &["src/authorization/dispatcher.rs", "src/llm.rs"],
    );
    assert!(
        found.is_empty(),
        "estas líneas importan el cliente de misiones fuera del Dispatcher:\n{}",
        found.join("\n")
    );
}

#[test]
fn the_scanner_itself_detects_the_dispatcher_and_the_grant_module() {
    // Sin excepciones el escáner debe encontrar las llamadas del Dispatcher
    // y las construcciones de `Grant`: si no, la vigilancia estaría ciega.
    let sends = offenders(&["client::call_tool(", "client::chat("], &[]);
    assert!(
        sends.iter().any(|l| l.contains("dispatcher.rs")),
        "{sends:?}"
    );
    assert!(sends.iter().any(|l| l.contains("llm.rs")), "{sends:?}");
    let grants = offenders(&["Grant {"], &[]);
    assert!(
        grants.iter().any(|l| l.contains("authorization/mod.rs")),
        "{grants:?}"
    );
}

/// Código de producción: lo que hay antes del primer `#[cfg(test)]` de cada archivo.
fn production_offenders(patterns: &[&str]) -> Vec<String> {
    let mut files = Vec::new();
    rust_files(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let mut found = Vec::new();
    for file in files {
        let relative = file
            .strip_prefix(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if relative.ends_with("/tests.rs") {
            continue;
        }
        let source = fs::read_to_string(&file).unwrap();
        let production = source.split("#[cfg(test)]").next().unwrap_or_default();
        for (index, line) in production.lines().enumerate() {
            let text = line.trim();
            if text.starts_with("//") {
                continue;
            }
            if patterns.iter().any(|p| text.contains(p)) {
                found.push(format!("{relative}:{}: {text}", index + 1));
            }
        }
    }
    found
}

/// SPEC-CMD-0001: ningún comando está cableado en el Navigator. Sus nombres,
/// herramientas y capacidades salen de los anuncios y del registro cerrado.
#[test]
fn no_command_is_hardcoded_in_the_navigator() {
    let found = production_offenders(&[
        "\"/calc\"",
        "FHS_CALC_NODES",
        "prepare_calc",
        "run_calc",
        "calc-0",
        "arithmetic_solve",
        "math.arithmetic",
    ]);
    assert!(
        found.is_empty(),
        "estos comandos están cableados en el código:\n{}",
        found.join("\n")
    );
}
