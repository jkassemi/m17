mod algorithm;
mod bootstrap;
mod consts;

use algorithm::DipComputation;
use bootstrap::bootstrap_p_value;
use consts::interpolate_p_value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DipError {
    EmptyInput,
    NonFiniteValue,
    NotSorted,
}

impl std::fmt::Display for DipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DipError::EmptyInput => write!(f, "dip statistic requires at least one value"),
            DipError::NonFiniteValue => write!(f, "input must not contain NaN or infinite values"),
            DipError::NotSorted => write!(f, "input slice must be sorted in non-decreasing order"),
        }
    }
}

impl std::error::Error for DipError {}

#[derive(Debug, Clone, Copy)]
pub struct DipStatOptions {
    pub sort_data: bool,
    pub allow_zero: bool,
    pub full_output: bool,
}

impl Default for DipStatOptions {
    fn default() -> Self {
        Self {
            sort_data: true,
            allow_zero: true,
            full_output: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DipStatResult {
    pub dip: f64,
    pub details: Option<DipDetails>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DipDetails {
    pub lo: usize,
    pub hi: usize,
    pub xl: f64,
    pub xu: f64,
    pub gcm: Vec<usize>,
    pub lcm: Vec<usize>,
    pub dip_index: Option<usize>,
}

pub fn dipstat(data: &[f64]) -> Result<f64, DipError> {
    dipstat_with_options(data, DipStatOptions::default()).map(|res| res.dip)
}

pub fn dipstat_with_options(
    data: &[f64],
    options: DipStatOptions,
) -> Result<DipStatResult, DipError> {
    let sorted = prepare_input(data, options.sort_data)?;
    let computation = algorithm::dip_statistic_internal(&sorted, options.allow_zero);
    let details = if options.full_output {
        Some(details_from_computation(&sorted, &computation))
    } else {
        None
    };
    Ok(DipStatResult {
        dip: computation.dip,
        details,
    })
}

fn prepare_input(data: &[f64], sort_data: bool) -> Result<Vec<f64>, DipError> {
    if data.is_empty() {
        return Err(DipError::EmptyInput);
    }
    if !data.iter().all(|v| v.is_finite()) {
        return Err(DipError::NonFiniteValue);
    }

    let mut owned = data.to_vec();
    if sort_data {
        owned.sort_by(|a, b| a.partial_cmp(b).unwrap());
    } else if !algorithm::is_sorted(&owned) {
        return Err(DipError::NotSorted);
    }
    Ok(owned)
}

fn details_from_computation(data: &[f64], comp: &DipComputation) -> DipDetails {
    let mut gcm = Vec::with_capacity(comp.gcm_len);
    let mut lcm = Vec::with_capacity(comp.lcm_len);

    if comp.gcm_len > 0 {
        for idx in 1..=comp.gcm_len {
            gcm.push(comp.gcm[idx].saturating_sub(1));
        }
    }
    if comp.lcm_len > 0 {
        for idx in 1..=comp.lcm_len {
            lcm.push(comp.lcm[idx].saturating_sub(1));
        }
    }

    let lo = comp.lo.saturating_sub(1).min(data.len() - 1);
    let hi = comp.hi.saturating_sub(1).min(data.len() - 1);

    DipDetails {
        lo,
        hi,
        xl: data[lo],
        xu: data[hi],
        gcm,
        lcm,
        dip_index: comp.dip_index.map(|idx| idx.saturating_sub(1)),
    }
}

#[derive(Debug, Clone)]
pub struct DipTestOptions {
    pub stat: DipStatOptions,
    pub p_value_method: PValueMethod,
}

impl Default for DipTestOptions {
    fn default() -> Self {
        Self {
            stat: DipStatOptions::default(),
            p_value_method: PValueMethod::Asymptotic,
        }
    }
}

#[derive(Debug, Clone)]
pub enum PValueMethod {
    Asymptotic,
    Bootstrap(BootstrapConfig),
}

#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    pub draws: usize,
    pub seed: Option<u64>,
    pub threads: Option<usize>,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            draws: 10_000,
            seed: None,
            threads: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DipTestResult {
    pub dip: f64,
    pub p_value: f64,
    pub details: Option<DipDetails>,
}

pub fn diptest(data: &[f64]) -> Result<DipTestResult, DipError> {
    diptest_with_options(data, DipTestOptions::default())
}

pub fn diptest_with_options(
    data: &[f64],
    options: DipTestOptions,
) -> Result<DipTestResult, DipError> {
    let stat_result = dipstat_with_options(data, options.stat)?;
    let n = data.len();

    let p_value = if n <= 3 {
        1.0
    } else {
        match &options.p_value_method {
            PValueMethod::Asymptotic => interpolate_p_value(n, stat_result.dip),
            PValueMethod::Bootstrap(cfg) => {
                bootstrap_p_value(stat_result.dip, n, cfg, options.stat.allow_zero)
            }
        }
    };

    Ok(DipTestResult {
        dip: stat_result.dip,
        p_value,
        details: stat_result.details,
    })
}
