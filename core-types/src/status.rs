use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

/// Discrete health level exposed by each managed service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverallStatus {
    Ok,
    Warn,
    Crit,
}

impl Default for OverallStatus {
    fn default() -> Self {
        OverallStatus::Warn
    }
}

/// Lightweight gauge descriptor rendered in the TUI and exported as metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusGauge {
    pub label: String,
    pub value: f64,
    pub max: Option<f64>,
    pub unit: Option<String>,
    pub details: Option<String>,
}

/// Mutable backing structure for a service status snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub overall: OverallStatus,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub gauges: Vec<StatusGauge>,
}

/// Immutable snapshot returned to consumers (TUI, metrics, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStatusSnapshot {
    pub name: String,
    pub overall: OverallStatus,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub gauges: Vec<StatusGauge>,
}

/// Trait implemented by every orchestrated service to expose health.
pub trait ServiceStatusReporter: Send + Sync {
    fn service_name(&self) -> &'static str;
    fn status(&self) -> ServiceStatusSnapshot;
}

/// Metric sample emitted by a service-specific reporter.
#[derive(Debug, Clone)]
pub struct MetricSample {
    pub metric: String,
    pub value: f64,
    pub labels: Vec<(String, String)>,
}

impl MetricSample {
    pub fn gauge(metric: impl Into<String>, value: f64) -> Self {
        Self {
            metric: metric.into(),
            value,
            labels: Vec::new(),
        }
    }
}

/// Trait wiring service owned metrics into the shared Prometheus exporter.
pub trait ServiceMetricsReporter: Send + Sync {
    fn service_name(&self) -> &'static str;
    fn collect_metrics(&self) -> Vec<MetricSample>;
}

/// Shared handle helpers so services can mutate their own status safely.
#[derive(Clone)]
pub struct ServiceStatusHandle {
    name: &'static str,
    inner: Arc<RwLock<ServiceStatus>>,
}

impl ServiceStatusHandle {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            inner: Arc::new(RwLock::new(ServiceStatus::default())),
        }
    }

    pub fn service_name(&self) -> &'static str {
        self.name
    }

    pub fn update<F>(&self, mutator: F)
    where
        F: FnOnce(&mut ServiceStatus),
    {
        let mut guard = self.inner.write().expect("status poisoned");
        mutator(&mut guard);
    }

    pub fn set_overall(&self, status: OverallStatus) {
        self.update(|s| s.overall = status);
    }

    pub fn push_warning(&self, msg: impl Into<String>) {
        self.update(|s| s.warnings.push(msg.into()));
    }

    pub fn clear_warnings_matching(&self, predicate: impl Fn(&str) -> bool) {
        self.update(|s| s.warnings.retain(|w| !predicate(w)));
    }

    pub fn push_error(&self, msg: impl Into<String>) {
        self.update(|s| s.errors.push(msg.into()));
    }

    pub fn clear_errors_matching(&self, predicate: impl Fn(&str) -> bool) {
        self.update(|s| s.errors.retain(|e| !predicate(e)));
    }

    pub fn set_gauges(&self, gauges: Vec<StatusGauge>) {
        self.update(|s| s.gauges = gauges);
    }

    pub fn snapshot(&self) -> ServiceStatusSnapshot {
        let guard = self.inner.read().expect("status poisoned");
        ServiceStatusSnapshot {
            name: self.name.to_string(),
            overall: guard.overall,
            warnings: guard.warnings.clone(),
            errors: guard.errors.clone(),
            gauges: guard.gauges.clone(),
        }
    }

    pub fn overall(&self) -> OverallStatus {
        let guard = self.inner.read().expect("status poisoned");
        guard.overall
    }
}

impl ServiceStatusReporter for ServiceStatusHandle {
    fn service_name(&self) -> &'static str {
        self.name
    }

    fn status(&self) -> ServiceStatusSnapshot {
        self.snapshot()
    }
}

impl ServiceMetricsReporter for ServiceStatusHandle {
    fn service_name(&self) -> &'static str {
        self.name
    }

    fn collect_metrics(&self) -> Vec<MetricSample> {
        let guard = self.inner.read().expect("status poisoned");
        guard
            .gauges
            .iter()
            .map(|g| MetricSample {
                metric: format!("{}_{}", self.name.to_lowercase(), slugify(&g.label)),
                value: g.value,
                labels: g
                    .unit
                    .as_ref()
                    .map(|unit| vec![("unit".to_string(), unit.clone())])
                    .unwrap_or_default(),
            })
            .collect()
    }
}

fn slugify(label: &str) -> String {
    let mut out = String::with_capacity(label.len());
    for ch in label.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    out
}
