// Copyright (c) James Kassemi, SC, US. All rights reserved.
#[path = "data/sample_data.rs"]
mod sample_data;

use sample_data::TEST_SAMPLE;

use diptest::{
    dipstat, dipstat_with_options, diptest, diptest_with_options, BootstrapConfig, DipError,
    DipStatOptions, DipTestOptions, PValueMethod,
};

const TEST_SAMPLE_DIP: f64 = 0.015_963_859_849_937_5;
const TEST_SAMPLE_TABLE_PVAL: f64 = 0.086_774_151_453_139_5;

fn approx_eq(a: f64, b: f64, tol: f64) {
    assert!((a - b).abs() <= tol, "expected {a} ~= {b} within {tol}");
}

#[test]
fn dipstat_matches_reference() -> Result<(), DipError> {
    let dip = dipstat(&TEST_SAMPLE)?;
    approx_eq(dip, TEST_SAMPLE_DIP, 1e-12);
    Ok(())
}

#[test]
fn diptest_matches_reference() -> Result<(), DipError> {
    let res = diptest(&TEST_SAMPLE)?;
    approx_eq(res.dip, TEST_SAMPLE_DIP, 1e-12);
    approx_eq(res.p_value, TEST_SAMPLE_TABLE_PVAL, 5e-6);
    Ok(())
}

#[test]
fn dipstat_sorting_validation() {
    let data = TEST_SAMPLE;
    let opts = DipStatOptions {
        sort_data: false,
        ..DipStatOptions::default()
    };
    assert!(dipstat_with_options(&data, opts).is_err());

    let mut sorted = data;
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert!(dipstat_with_options(&sorted, opts).is_ok());
}

#[test]
fn diptest_bootstrap_runs() -> Result<(), DipError> {
    let options = DipTestOptions {
        stat: DipStatOptions::default(),
        p_value_method: PValueMethod::Bootstrap(BootstrapConfig {
            draws: 256,
            seed: Some(1234),
            threads: Some(2),
        }),
    };
    let res = diptest_with_options(&TEST_SAMPLE, options)?;
    assert!((0.0..=1.0).contains(&res.p_value));
    Ok(())
}
