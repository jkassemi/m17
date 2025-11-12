use thiserror::Error;

pub type EngineResult<T> = Result<T, EngineError>;

pub trait Engine: Send + Sync {
    fn start(&self) -> EngineResult<()>;
    fn stop(&self) -> EngineResult<()>;
    fn health(&self) -> EngineHealth;
    fn describe_priority_hooks(&self) -> PriorityHookDescription;
}

#[derive(Clone, Copy, Debug)]
pub enum HealthStatus {
    Starting,
    Ready,
    Degraded,
    Failed,
    Stopped,
}

#[derive(Clone, Debug)]
pub struct EngineHealth {
    pub status: HealthStatus,
    pub detail: Option<String>,
}

impl EngineHealth {
    pub fn new(status: HealthStatus, detail: Option<String>) -> Self {
        Self { status, detail }
    }
}

impl Default for EngineHealth {
    fn default() -> Self {
        Self {
            status: HealthStatus::Stopped,
            detail: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PriorityHookDescription {
    pub supports_priority_regions: bool,
    pub notes: Option<String>,
}

impl Default for PriorityHookDescription {
    fn default() -> Self {
        Self {
            supports_priority_regions: false,
            notes: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("engine already running")]
    AlreadyRunning,
    #[error("engine is not running")]
    NotRunning,
    #[error("engine encountered an error: {source}")]
    Failure {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
