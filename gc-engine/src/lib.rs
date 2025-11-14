use std::{
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use engine_api::{
    Engine, EngineError, EngineHealth, EngineResult, HealthStatus, PriorityHookDescription,
};
use log::{error, info, warn};
use thiserror::Error;
use window_space::{
    SymbolId, WindowSpaceController, WindowSpaceError,
    ledger::WindowRow,
    payload::{PayloadType, SlotKind, SlotStatus, TradeSlotKind},
    window::WindowIndex,
};

const DEFAULT_BATCH_LIMIT: usize = 128;
const DEFAULT_IDLE_BACKOFF_MS: u64 = 500;

pub struct GcEngine {
    inner: Arc<GcInner>,
}

#[derive(Clone)]
pub struct GcEngineConfig {
    pub label: String,
    pub batch_limit: usize,
    pub idle_backoff: Duration,
}

impl Default for GcEngineConfig {
    fn default() -> Self {
        Self {
            label: "dev".to_string(),
            batch_limit: DEFAULT_BATCH_LIMIT,
            idle_backoff: Duration::from_millis(DEFAULT_IDLE_BACKOFF_MS),
        }
    }
}

impl GcEngine {
    pub fn new(config: GcEngineConfig, controller: Arc<WindowSpaceController>) -> Self {
        Self {
            inner: GcInner::new(config, controller),
        }
    }
}

impl Engine for GcEngine {
    fn start(&self) -> EngineResult<()> {
        GcInner::start(&self.inner)
    }

    fn stop(&self) -> EngineResult<()> {
        self.inner.stop()
    }

    fn health(&self) -> EngineHealth {
        self.inner.health()
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription::default()
    }
}

struct GcInner {
    config: GcEngineConfig,
    controller: Arc<WindowSpaceController>,
    state: Mutex<EngineRuntimeState>,
    health: Mutex<EngineHealth>,
}

impl GcInner {
    fn new(config: GcEngineConfig, controller: Arc<WindowSpaceController>) -> Arc<Self> {
        Arc::new(Self {
            config,
            controller,
            state: Mutex::new(EngineRuntimeState::Stopped),
            health: Mutex::new(EngineHealth::new(HealthStatus::Stopped, None)),
        })
    }

    fn start(this: &Arc<Self>) -> EngineResult<()> {
        let mut guard = this.state.lock().unwrap();
        if matches!(*guard, EngineRuntimeState::Running(_)) {
            return Err(EngineError::AlreadyRunning);
        }
        this.set_health(HealthStatus::Starting, None);
        let cancel = Arc::new(AtomicBool::new(false));
        let runner = Arc::clone(this);
        let cancel_clone = Arc::clone(&cancel);
        let handle = thread::Builder::new()
            .name(format!("{}-gc", this.config.label))
            .spawn(move || runner.run(cancel_clone))
            .map_err(|err| EngineError::Failure {
                source: Box::new(err),
            })?;
        info!("[{}] gc engine starting", this.config.label);
        *guard = EngineRuntimeState::Running(ThreadBundle { cancel, handle });
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        let mut guard = self.state.lock().unwrap();
        let Some(bundle) = guard.take_running() else {
            return Err(EngineError::NotRunning);
        };
        bundle.cancel.store(true, Ordering::Relaxed);
        if let Err(err) = bundle.handle.join() {
            error!("[{}] gc join error: {:?}", self.config.label, err);
        }
        *guard = EngineRuntimeState::Stopped;
        self.set_health(HealthStatus::Stopped, None);
        Ok(())
    }

    fn health(&self) -> EngineHealth {
        self.health.lock().unwrap().clone()
    }

    fn run(self: Arc<Self>, cancel: Arc<AtomicBool>) {
        self.set_health(HealthStatus::Ready, None);
        while !cancel.load(Ordering::Relaxed) {
            match self.process_pass() {
                Ok(true) => continue,
                Ok(false) => thread::sleep(self.config.idle_backoff),
                Err(err) => {
                    self.set_health(HealthStatus::Degraded, Some(err.to_string()));
                    error!("[{}] gc pass failed: {}", self.config.label, err);
                    thread::sleep(self.config.idle_backoff);
                }
            }
        }
        self.set_health(HealthStatus::Stopped, None);
        info!("[{}] gc engine stopped", self.config.label);
    }

    fn process_pass(&self) -> Result<bool, GcError> {
        let mut processed = 0usize;
        let symbols = self.controller.symbols();
        for (symbol_id, symbol) in symbols {
            for target in GcTarget::ALL {
                processed += self.cleanup_symbol(
                    symbol_id,
                    &symbol,
                    *target,
                    self.config.batch_limit - processed,
                )?;
                if processed >= self.config.batch_limit {
                    return Ok(true);
                }
            }
        }
        Ok(processed > 0)
    }

    fn cleanup_symbol(
        &self,
        symbol_id: SymbolId,
        symbol: &str,
        target: GcTarget,
        budget: usize,
    ) -> Result<usize, GcError> {
        if budget == 0 {
            return Ok(0);
        }
        let trade_space = self.controller.trade_window_space();
        let mut cleaned = 0usize;
        trade_space
            .with_symbol_rows(symbol_id, |rows| {
                for row in rows {
                    if cleaned >= budget {
                        break;
                    }
                    let slot = row.slot(target.kind.index());
                    if slot.status != SlotStatus::Retire {
                        continue;
                    }
                    let window_idx = row.header.window_idx;
                    if let Err(err) = self.cleanup_slot(symbol, window_idx, target, slot) {
                        warn!(
                            "[{}] gc failed for {} {:?} window {}: {}",
                            self.config.label, symbol, target.kind, window_idx, err
                        );
                        continue;
                    }
                    cleaned += 1;
                }
            })
            .map_err(WindowSpaceError::from)?;
        Ok(cleaned)
    }

    fn cleanup_slot(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        target: GcTarget,
        slot: &window_space::payload::Slot,
    ) -> Result<(), GcError> {
        if slot.payload_id == 0 {
            self.controller
                .retire_slot(symbol, window_idx, SlotKind::Trade(target.kind))?;
            return Ok(());
        }
        match slot.payload_type {
            PayloadType::Quote => {
                let payload =
                    {
                        let stores = self.controller.payload_stores();
                        stores.quotes.get(slot.payload_id).cloned().ok_or(
                            GcError::MissingPayload {
                                payload_id: slot.payload_id,
                            },
                        )?
                    };
                self.delete_artifact(&payload.artifact_uri)?;
            }
            PayloadType::Aggressor => {
                let payload = {
                    let stores = self.controller.payload_stores();
                    stores.aggressor.get(slot.payload_id).cloned().ok_or(
                        GcError::MissingPayload {
                            payload_id: slot.payload_id,
                        },
                    )?
                };
                self.delete_artifact(&payload.artifact_uri)?;
            }
            PayloadType::Trade => {
                let payload =
                    {
                        let stores = self.controller.payload_stores();
                        stores.trades.get(slot.payload_id).cloned().ok_or(
                            GcError::MissingPayload {
                                payload_id: slot.payload_id,
                            },
                        )?
                    };
                self.delete_artifact(&payload.artifact_uri)?;
            }
            _ => {
                return Err(GcError::UnsupportedPayload {
                    payload_type: slot.payload_type,
                });
            }
        }
        self.controller
            .retire_slot(symbol, window_idx, SlotKind::Trade(target.kind))?;
        Ok(())
    }

    fn delete_artifact(&self, relative: &str) -> Result<(), GcError> {
        if relative.is_empty() {
            return Ok(());
        }
        let path = self.controller.config().state_dir().join(relative);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn set_health(&self, status: HealthStatus, detail: Option<String>) {
        let mut guard = self.health.lock().unwrap();
        guard.status = status;
        guard.detail = detail;
    }
}

#[derive(Clone, Copy)]
struct GcTarget {
    kind: TradeSlotKind,
}

impl GcTarget {
    const OPTION_QUOTES: Self = Self {
        kind: TradeSlotKind::OptionQuote,
    };
    const UNDERLYING_QUOTES: Self = Self {
        kind: TradeSlotKind::UnderlyingQuote,
    };
    const ALL: &'static [Self] = &[Self::OPTION_QUOTES, Self::UNDERLYING_QUOTES];
}

enum EngineRuntimeState {
    Stopped,
    Running(ThreadBundle),
}

impl EngineRuntimeState {
    fn take_running(&mut self) -> Option<ThreadBundle> {
        match std::mem::replace(self, EngineRuntimeState::Stopped) {
            EngineRuntimeState::Running(bundle) => Some(bundle),
            other => {
                *self = other;
                None
            }
        }
    }
}

struct ThreadBundle {
    cancel: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

#[derive(Debug, Error)]
enum GcError {
    #[error("window space error: {0}")]
    WindowSpace(#[from] WindowSpaceError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("missing payload id {payload_id}")]
    MissingPayload { payload_id: u32 },
    #[error("payload type {payload_type:?} not supported by gc")]
    UnsupportedPayload { payload_type: PayloadType },
}

impl From<GcError> for EngineError {
    fn from(value: GcError) -> Self {
        EngineError::Failure {
            source: Box::new(value),
        }
    }
}
