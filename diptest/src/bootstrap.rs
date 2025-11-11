// Copyright (c) James Kassemi, SC, US. All rights reserved.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::distributions::{Distribution, Uniform};
use rand::SeedableRng;
use rand_pcg::Pcg64;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

use crate::{algorithm, BootstrapConfig};

pub(crate) fn bootstrap_p_value(
    dip_stat: f64,
    sample_size: usize,
    cfg: &BootstrapConfig,
    allow_zero: bool,
) -> f64 {
    if cfg.draws == 0 || sample_size == 0 {
        return 0.0;
    }

    let base_seed = seed_from_config(cfg);
    let draws = cfg.draws;

    let exceed = match cfg.threads {
        Some(threads) if threads <= 1 => {
            bootstrap_sequential(dip_stat, sample_size, draws, allow_zero, base_seed)
        }
        Some(threads) => {
            let pool = ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("failed to create rayon thread pool");
            pool.install(|| bootstrap_parallel(dip_stat, sample_size, draws, allow_zero, base_seed))
        }
        None => bootstrap_parallel(dip_stat, sample_size, draws, allow_zero, base_seed),
    };

    exceed as f64 / draws as f64
}

fn bootstrap_sequential(
    dip_stat: f64,
    sample_size: usize,
    draws: usize,
    allow_zero: bool,
    base_seed: u64,
) -> usize {
    let mut state = ThreadState::new(base_seed, 0, sample_size);
    (0..draws)
        .map(|_| state.run_iteration(dip_stat, allow_zero) as usize)
        .sum()
}

fn bootstrap_parallel(
    dip_stat: f64,
    sample_size: usize,
    draws: usize,
    allow_zero: bool,
    base_seed: u64,
) -> usize {
    let counter = AtomicU64::new(0);
    (0..draws)
        .into_par_iter()
        .map_init(
            || {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                ThreadState::new(base_seed, idx, sample_size)
            },
            |state, _| state.run_iteration(dip_stat, allow_zero) as usize,
        )
        .sum()
}

fn seed_from_config(cfg: &BootstrapConfig) -> u64 {
    if let Some(seed) = cfg.seed {
        return seed.max(1);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let mut value = now.as_nanos() as u64;
    if value == 0 {
        value = 0x853c49e6748fea9b;
    }
    value
}

struct ThreadState {
    rng: Pcg64,
    uniform: Uniform<f64>,
    sample: Vec<f64>,
}

impl ThreadState {
    fn new(base_seed: u64, idx: u64, sample_size: usize) -> Self {
        let seed = mix_seed(base_seed, idx);
        Self {
            rng: Pcg64::seed_from_u64(seed),
            uniform: Uniform::new(0.0, 1.0),
            sample: vec![0.0; sample_size],
        }
    }

    fn run_iteration(&mut self, dip_stat: f64, allow_zero: bool) -> bool {
        for value in &mut self.sample {
            *value = self.uniform.sample(&mut self.rng);
        }
        self.sample.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let comp = algorithm::dip_statistic_internal(&self.sample, allow_zero);
        dip_stat <= comp.dip
    }
}

fn mix_seed(base: u64, idx: u64) -> u64 {
    let mut seed = base ^ idx.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    if seed == 0 {
        seed = 1;
    }
    seed
}
