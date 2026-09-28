//! Libro de pines del Navigator: **única fuente normativa** del ciclo de vida
//! de un adjunto IPFS (DEC-0095). Lógica pura (el reloj entra como `now` en
//! ms) más su persistencia atómica en `/data/ipfs-pins.json`.
//!
//! Cada CID lleva los leases de los turnos que lo usan. Un lease nace
//! `uploading` antes del `add` real (WAL), pasa a `active` al confirmarse y
//! recibe `release_after` cuando su turno termina: 30 s tras éxito, 5 min tras
//! error, cancelación, turno muerto o reinicio. Solo el barrido quita leases;
//! un CID sin leases y sin `reuse` queda con el unpin pendiente, que se
//! reintenta con backoff hasta lograrlo.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const SUCCESS_GRACE_MS: i64 = 30_000;
pub const FAILURE_GRACE_MS: i64 = 5 * 60_000;
const UNPIN_BASE_BACKOFF_MS: i64 = 30_000;
const UNPIN_MAX_BACKOFF_MS: i64 = 60 * 60_000;
const LEDGER_VERSION: u32 = 1;
const BACKUPS: usize = 3;
/// `turn_id` de los leases de limpieza (no pertenecen a ningún turno).
pub const CLEANUP_TURN: &str = "cleanup";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    Uploading,
    Active,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub turn_id: String,
    pub state: LeaseState,
    pub created_at: i64,
    pub release_after: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unpin {
    pub pending: bool,
    pub attempts: u32,
    pub next_attempt_at: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinEntry {
    /// Bytes del adjunto (no el tamaño UnixFS de Kubo); es lo que cuentan
    /// las cuotas.
    pub size: u64,
    pub reuse: bool,
    pub unpin: Unpin,
    pub leases: BTreeMap<String, Lease>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ledger {
    pub version: u32,
    pub pins: BTreeMap<String, PinEntry>,
}

impl Default for Ledger {
    fn default() -> Self {
        Self {
            version: LEDGER_VERSION,
            pins: BTreeMap::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReleaseError {
    #[error("CID desconocido")]
    Unknown,
    #[error("el CID no está marcado como reuse")]
    NotReuse,
}

/// Resultado de liberar un CID `reuse` (respuesta de la API de admin).
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct ReleaseOutcome {
    /// `true`: el CID ya no tiene leases y el siguiente barrido lo despinea.
    pub cleanup_scheduled: bool,
    /// Cuándo vence el último lease; `None` si algún turno lo sigue usando.
    pub release_after: Option<i64>,
}

impl Ledger {
    /// Paso 2 de la subida: lease `uploading` con el CID precalculado.
    pub fn begin_upload(
        &mut self,
        cid: &str,
        size: u64,
        reuse: bool,
        turn_id: &str,
        now: i64,
    ) -> String {
        let lease_id = uuid::Uuid::new_v4().to_string();
        let entry = self.pins.entry(cid.to_string()).or_default();
        entry.size = entry.size.max(size);
        entry.reuse |= reuse;
        // Un lease nuevo cancela un unpin pendiente (el barrido solo
        // despinea CIDs sin leases).
        entry.unpin = Unpin::default();
        entry.leases.insert(
            lease_id.clone(),
            Lease {
                turn_id: turn_id.into(),
                state: LeaseState::Uploading,
                created_at: now,
                release_after: None,
            },
        );
        lease_id
    }

    /// Paso 3 confirmado: el `add` devolvió el CID esperado.
    pub fn activate(&mut self, cid: &str, lease_id: &str) {
        if let Some(lease) = self
            .pins
            .get_mut(cid)
            .and_then(|e| e.leases.get_mut(lease_id))
        {
            lease.state = LeaseState::Active;
        }
    }

    /// Fija el vencimiento de un lease concreto (fallo de la subida).
    pub fn release_lease(&mut self, cid: &str, lease_id: &str, at: i64) {
        if let Some(lease) = self
            .pins
            .get_mut(cid)
            .and_then(|e| e.leases.get_mut(lease_id))
        {
            lease.release_after = Some(lease.release_after.map_or(at, |r| r.min(at)));
        }
    }

    /// Lease de limpieza para un CID que Kubo pudo fijar sin que lo esperáramos
    /// (CID devuelto distinto del precalculado); vence en `now`.
    pub fn add_cleanup(&mut self, cid: &str, size: u64, now: i64) {
        let entry = self.pins.entry(cid.to_string()).or_default();
        entry.size = entry.size.max(size);
        entry.leases.insert(
            uuid::Uuid::new_v4().to_string(),
            Lease {
                turn_id: CLEANUP_TURN.into(),
                state: LeaseState::Active,
                created_at: now,
                release_after: Some(now),
            },
        );
    }

    /// Transiciones 2 y 3 (fin del turno) y 4 (turno muerto): los leases del
    /// turno sin vencimiento vencen en `now + grace`. Devuelve si cambió algo.
    pub fn end_turn(&mut self, turn_id: &str, grace_ms: i64, now: i64) -> bool {
        let mut changed = false;
        for lease in self
            .pins
            .values_mut()
            .flat_map(|e| e.leases.values_mut())
            .filter(|l| l.turn_id == turn_id && l.release_after.is_none())
        {
            lease.release_after = Some(now + grace_ms);
            changed = true;
        }
        changed
    }

    /// Turnos con leases aún sin vencimiento (el barrido revisa si siguen vivos).
    pub fn live_turns(&self) -> BTreeSet<String> {
        self.pins
            .values()
            .flat_map(|e| e.leases.values())
            .filter(|l| l.release_after.is_none())
            .map(|l| l.turn_id.clone())
            .collect()
    }

    /// Transición 5 (arranque): todo lease del proceso anterior vence en
    /// `max(actual, now + 5 min)`. Un `reuse` que quedó `uploading` pierde la
    /// marca (se trata como efímero); se devuelven esos CIDs para reportarlos.
    pub fn startup(&mut self, now: i64) -> Vec<String> {
        let mut failed_reuse = Vec::new();
        for (cid, entry) in &mut self.pins {
            if entry.reuse
                && entry
                    .leases
                    .values()
                    .any(|l| l.state == LeaseState::Uploading)
            {
                entry.reuse = false;
                failed_reuse.push(cid.clone());
            }
            for lease in entry.leases.values_mut() {
                let floor = now + FAILURE_GRACE_MS;
                lease.release_after = Some(lease.release_after.map_or(floor, |r| r.max(floor)));
            }
        }
        failed_reuse
    }

    /// Transición 7: quita los leases vencidos y devuelve los CIDs cuyo unpin
    /// toca intentar ahora.
    pub fn sweep(&mut self, now: i64) -> Vec<String> {
        let mut due = Vec::new();
        for (cid, entry) in &mut self.pins {
            entry
                .leases
                .retain(|_, l| l.release_after.is_none_or(|r| r > now));
            if entry.leases.is_empty() && !entry.reuse {
                if !entry.unpin.pending {
                    entry.unpin = Unpin {
                        pending: true,
                        attempts: 0,
                        next_attempt_at: Some(now),
                    };
                }
                if entry.unpin.next_attempt_at.is_none_or(|t| t <= now) {
                    due.push(cid.clone());
                }
            }
        }
        due
    }

    /// `true` si el unpin de `cid` sigue siendo correcto (sin leases, sin
    /// `reuse`, pendiente). Se revisa justo antes de llamar a Kubo.
    pub fn unpin_still_due(&self, cid: &str) -> bool {
        self.pins
            .get(cid)
            .is_some_and(|e| e.unpin.pending && e.leases.is_empty() && !e.reuse)
    }

    pub fn unpinned(&mut self, cid: &str) {
        if self.unpin_still_due(cid) {
            self.pins.remove(cid);
        }
    }

    pub fn unpin_failed(&mut self, cid: &str, now: i64) {
        if let Some(entry) = self.pins.get_mut(cid) {
            entry.unpin.attempts += 1;
            let shift = entry.unpin.attempts.min(16);
            let backoff = (UNPIN_BASE_BACKOFF_MS << shift).min(UNPIN_MAX_BACKOFF_MS);
            entry.unpin.next_attempt_at = Some(now + backoff);
        }
    }

    /// Acción del operador: quita la marca `reuse`. El unpin lo hace el
    /// barrido, nunca esta llamada.
    pub fn release_reuse(&mut self, cid: &str, now: i64) -> Result<ReleaseOutcome, ReleaseError> {
        let entry = self.pins.get_mut(cid).ok_or(ReleaseError::Unknown)?;
        if !entry.reuse {
            return Err(ReleaseError::NotReuse);
        }
        entry.reuse = false;
        if entry.leases.is_empty() {
            self.add_cleanup(cid, 0, now);
            return Ok(ReleaseOutcome {
                cleanup_scheduled: true,
                release_after: Some(now),
            });
        }
        let release_after = entry
            .leases
            .values()
            .map(|l| l.release_after)
            .try_fold(i64::MIN, |acc, r| r.map(|r| acc.max(r)));
        Ok(ReleaseOutcome {
            cleanup_scheduled: false,
            release_after,
        })
    }

    /// Bytes de los CIDs únicos del libro, en cualquier estado y retención.
    pub fn unique_bytes(&self) -> u64 {
        self.pins.values().map(|e| e.size).sum()
    }

    /// CIDs que deberían estar fijados en Kubo (con un lease `active` o
    /// `reuse`); los `uploading` aún no lo están con certeza.
    pub fn expected_pins(&self) -> BTreeSet<String> {
        self.pins
            .iter()
            .filter(|(_, e)| {
                !e.unpin.pending
                    && (e.reuse || e.leases.values().any(|l| l.state == LeaseState::Active))
            })
            .map(|(cid, _)| cid.clone())
            .collect()
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != LEDGER_VERSION {
            return Err(format!("versión {} no soportada", self.version));
        }
        for cid in self.pins.keys() {
            match galaxia_fhs::ipfs::canonical_cid(cid) {
                Ok(canonical) if &canonical == cid => {}
                _ => return Err(format!("CID no canónico: {cid}")),
            }
        }
        Ok(())
    }
}

/// Resultado de cargar el libro al arrancar.
#[derive(Debug)]
pub struct Loaded {
    pub ledger: Ledger,
    /// Copia de respaldo usada porque la principal no validaba.
    pub recovered_from: Option<PathBuf>,
    /// Ninguna versión validaba: el archivo malo quedó aquí.
    pub quarantined: Option<PathBuf>,
}

/// Persistencia atómica del libro con 3 versiones rotadas.
#[derive(Clone, Debug)]
pub struct Store {
    path: PathBuf,
}

impl Store {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn backup(&self, n: usize) -> PathBuf {
        PathBuf::from(format!("{}.{n}", self.path.display()))
    }

    fn dir(&self) -> PathBuf {
        self.path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    }

    fn read(path: &Path) -> Option<Result<Ledger, String>> {
        let bytes = std::fs::read(path).ok()?;
        Some(
            serde_json::from_slice::<Ledger>(&bytes)
                .map_err(|e| e.to_string())
                .and_then(|l| l.validate().map(|()| l)),
        )
    }

    /// Principal → respaldos (más reciente primero). Si existe algo y nada
    /// valida, la principal va a cuarentena y se empieza un libro nuevo.
    pub fn load(&self, now: i64) -> std::io::Result<Loaded> {
        let candidates: Vec<PathBuf> = std::iter::once(self.path.clone())
            .chain((1..=BACKUPS).map(|n| self.backup(n)))
            .collect();
        let mut found_any = false;
        for (i, path) in candidates.iter().enumerate() {
            match Self::read(path) {
                Some(Ok(ledger)) => {
                    if i > 0 {
                        tracing::warn!(
                            "[ipfs] libro principal inválido; se recupera {}",
                            path.display()
                        );
                    }
                    return Ok(Loaded {
                        ledger,
                        recovered_from: (i > 0).then(|| path.clone()),
                        quarantined: None,
                    });
                }
                Some(Err(error)) => {
                    found_any = true;
                    tracing::error!("[ipfs] {} no valida: {error}", path.display());
                }
                None => {}
            }
        }
        if !found_any {
            return Ok(Loaded {
                ledger: Ledger::default(),
                recovered_from: None,
                quarantined: None,
            });
        }
        let quarantine = PathBuf::from(format!("{}.corrupt-{now}", self.path.display()));
        if self.path.exists() {
            std::fs::rename(&self.path, &quarantine)?;
        }
        Ok(Loaded {
            ledger: Ledger::default(),
            recovered_from: None,
            quarantined: Some(quarantine),
        })
    }

    /// Hay un libro en cuarentena: el operador debe revisarlo antes de que se
    /// vuelva a despinear nada automáticamente.
    pub fn has_quarantine(&self) -> bool {
        let Some(name) = self.path.file_name().and_then(|n| n.to_str()) else {
            return false;
        };
        let prefix = format!("{name}.corrupt-");
        std::fs::read_dir(self.dir())
            .map(|entries| {
                entries
                    .flatten()
                    .any(|e| e.file_name().to_string_lossy().starts_with(&prefix))
            })
            .unwrap_or(false)
    }

    /// Temporal en el mismo directorio, `fsync`, rotación, `rename` y `fsync`
    /// del directorio. 0600.
    pub fn save(&self, ledger: &Ledger) -> std::io::Result<()> {
        let dir = self.dir();
        std::fs::create_dir_all(&dir)?;
        let tmp = PathBuf::from(format!(
            "{}.tmp-{}",
            self.path.display(),
            std::process::id()
        ));
        {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&tmp)?;
            file.write_all(&serde_json::to_vec_pretty(ledger).map_err(std::io::Error::other)?)?;
            file.sync_all()?;
        }
        for n in (1..BACKUPS).rev() {
            let from = self.backup(n);
            if from.exists() {
                std::fs::rename(&from, self.backup(n + 1))?;
            }
        }
        if self.path.exists() {
            std::fs::rename(&self.path, self.backup(1))?;
        }
        std::fs::rename(&tmp, &self.path)?;
        std::fs::File::open(&dir)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    const B: &str = "bafkreig6punxegq6ayzlptye5x2qgleoz75j7gqijeqvfojg6gs2pz3f24";

    #[test]
    fn successful_turn_releases_after_the_short_grace() {
        let mut l = Ledger::default();
        let lease = l.begin_upload(A, 10, false, "t1", 0);
        assert!(l.sweep(1_000_000).is_empty(), "uploading sin vencimiento");
        l.activate(A, &lease);
        assert_eq!(l.expected_pins(), BTreeSet::from([A.to_string()]));
        assert!(l.end_turn("t1", SUCCESS_GRACE_MS, 100));
        assert!(l.sweep(100 + SUCCESS_GRACE_MS - 1).is_empty());
        assert_eq!(l.sweep(100 + SUCCESS_GRACE_MS), vec![A.to_string()]);
        assert!(l.unpin_still_due(A));
        l.unpinned(A);
        assert!(l.pins.is_empty());
    }

    #[test]
    fn two_leases_of_the_same_cid_keep_it_pinned_until_both_expire() {
        let mut l = Ledger::default();
        let a = l.begin_upload(A, 10, false, "t1", 0);
        let b = l.begin_upload(A, 10, false, "t2", 0);
        l.activate(A, &a);
        l.activate(A, &b);
        assert_eq!(l.unique_bytes(), 10, "un CID cuenta una vez");
        l.end_turn("t1", SUCCESS_GRACE_MS, 0);
        assert!(l.sweep(SUCCESS_GRACE_MS).is_empty());
        l.end_turn("t2", FAILURE_GRACE_MS, 0);
        assert!(l.sweep(FAILURE_GRACE_MS - 1).is_empty());
        assert_eq!(l.sweep(FAILURE_GRACE_MS), vec![A.to_string()]);
    }

    #[test]
    fn failed_unpin_backs_off_up_to_an_hour() {
        let mut l = Ledger::default();
        l.add_cleanup(A, 0, 0);
        assert_eq!(l.sweep(0), vec![A.to_string()]);
        l.unpin_failed(A, 0);
        assert!(l.sweep(UNPIN_BASE_BACKOFF_MS * 2 - 1).is_empty());
        assert_eq!(l.sweep(UNPIN_BASE_BACKOFF_MS * 2), vec![A.to_string()]);
        for _ in 0..20 {
            l.unpin_failed(A, 0);
        }
        assert_eq!(l.pins[A].unpin.next_attempt_at, Some(UNPIN_MAX_BACKOFF_MS));
    }

    #[test]
    fn a_new_lease_cancels_a_pending_unpin() {
        let mut l = Ledger::default();
        l.add_cleanup(A, 0, 0);
        assert_eq!(l.sweep(0), vec![A.to_string()]);
        l.begin_upload(A, 5, false, "t9", 1);
        assert!(!l.unpin_still_due(A));
        l.unpinned(A);
        assert!(l.pins.contains_key(A), "no se borra un CID en uso");
    }

    #[test]
    fn startup_gives_previous_leases_the_long_grace_and_drops_half_uploaded_reuse() {
        let mut l = Ledger::default();
        let a = l.begin_upload(A, 1, false, "viejo", 0);
        l.activate(A, &a);
        l.end_turn("viejo", SUCCESS_GRACE_MS, 0);
        l.begin_upload(B, 1, true, "viejo", 0); // reuse que no terminó
        let now = 1_000;
        assert_eq!(l.startup(now), vec![B.to_string()]);
        assert!(!l.pins[B].reuse);
        for entry in l.pins.values() {
            for lease in entry.leases.values() {
                assert_eq!(lease.release_after, Some(now + FAILURE_GRACE_MS));
            }
        }
        assert!(l.sweep(now + FAILURE_GRACE_MS - 1).is_empty());
        assert_eq!(l.sweep(now + FAILURE_GRACE_MS).len(), 2);
    }

    #[test]
    fn reuse_is_released_only_by_the_operator_and_unpinned_by_the_sweep() {
        let mut l = Ledger::default();
        let a = l.begin_upload(A, 1, true, "t1", 0);
        l.activate(A, &a);
        l.end_turn("t1", SUCCESS_GRACE_MS, 0);
        assert!(
            l.sweep(10 * FAILURE_GRACE_MS).is_empty(),
            "reuse no se libera"
        );
        assert_eq!(l.release_reuse(B, 0), Err(ReleaseError::Unknown));
        assert_eq!(
            l.release_reuse(A, 100),
            Ok(ReleaseOutcome {
                cleanup_scheduled: true,
                release_after: Some(100)
            })
        );
        assert_eq!(l.release_reuse(A, 100), Err(ReleaseError::NotReuse));
        assert_eq!(l.sweep(100), vec![A.to_string()]);
    }

    #[test]
    fn releasing_reuse_while_a_turn_uses_it_waits_for_that_lease() {
        let mut l = Ledger::default();
        let a = l.begin_upload(A, 1, true, "t1", 0);
        l.activate(A, &a);
        l.end_turn("t1", SUCCESS_GRACE_MS, 0);
        let b = l.begin_upload(A, 1, false, "t2", 0);
        l.activate(A, &b);
        let out = l.release_reuse(A, 10).unwrap();
        assert_eq!(
            out,
            ReleaseOutcome {
                cleanup_scheduled: false,
                release_after: None
            }
        );
        assert!(l.sweep(10 * FAILURE_GRACE_MS).is_empty(), "t2 sigue vivo");
        l.end_turn("t2", FAILURE_GRACE_MS, 0);
        assert_eq!(l.sweep(FAILURE_GRACE_MS), vec![A.to_string()]);
    }

    #[test]
    fn live_turns_only_lists_leases_without_deadline() {
        let mut l = Ledger::default();
        l.begin_upload(A, 1, false, "vivo", 0);
        l.begin_upload(B, 1, false, "terminado", 0);
        l.end_turn("terminado", SUCCESS_GRACE_MS, 0);
        assert_eq!(l.live_turns(), BTreeSet::from(["vivo".to_string()]));
    }

    fn tmp_store(name: &str) -> Store {
        let dir = std::env::temp_dir().join(format!("ledger-{name}-{}", uuid::Uuid::new_v4()));
        Store::new(dir.join("ipfs-pins.json"))
    }

    #[test]
    fn save_rotates_versions_and_load_recovers_from_a_backup() {
        let store = tmp_store("rotate");
        let mut l = Ledger::default();
        for i in 0..5 {
            l.begin_upload(A, i, false, "t", 0);
            store.save(&l).unwrap();
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(store.path())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        assert!(store.backup(3).exists() && !store.backup(4).exists());
        std::fs::write(store.path(), b"{basura").unwrap();
        let loaded = store.load(1).unwrap();
        assert_eq!(loaded.recovered_from, Some(store.backup(1)));
        assert!(loaded.quarantined.is_none());
        assert_eq!(loaded.ledger.pins[A].leases.len(), 4);
    }

    #[test]
    fn nothing_valid_goes_to_quarantine_and_blocks_unpins() {
        let store = tmp_store("quarantine");
        std::fs::create_dir_all(store.dir()).unwrap();
        std::fs::write(store.path(), br#"{"version":1,"pins":{"no-es-cid":{}}}"#).unwrap();
        assert!(!store.has_quarantine());
        let loaded = store.load(42).unwrap();
        assert!(loaded.ledger.pins.is_empty());
        assert!(loaded
            .quarantined
            .unwrap()
            .ends_with("ipfs-pins.json.corrupt-42"));
        assert!(store.has_quarantine());
        // Un libro nuevo se puede seguir guardando.
        store.save(&Ledger::default()).unwrap();
        assert!(store.load(43).unwrap().quarantined.is_none());
    }

    #[test]
    fn missing_file_is_an_empty_ledger() {
        let loaded = tmp_store("empty").load(0).unwrap();
        assert!(loaded.ledger.pins.is_empty() && loaded.quarantined.is_none());
    }
}
