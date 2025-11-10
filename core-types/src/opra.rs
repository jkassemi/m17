// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Helpers for working with OPRA option contract identifiers.

use chrono::NaiveDate;

/// Parse an OPRA contract string (e.g., `O:SPY241220P00720000`) into its
/// direction, strike, expiry (ns), and underlying symbol.
///
/// The `ts_ns_hint` parameter allows callers that already know a related
/// timestamp to provide a fallback for expiry when the contract string is
/// malformed. When present the hint is returned unchanged if parsing the
/// string fails.
pub fn parse_opra_contract(contract: &str, ts_ns_hint: Option<i64>) -> (char, f64, i64, String) {
    // Format: O:<UNDERLYING><YYMMDD><C|P><STRIKE 8 digits>
    let mut dir = 'C';
    let mut strike = 0.0f64;
    let mut expiry_ts_ns = ts_ns_hint.unwrap_or(0);
    let mut underlying = String::new();
    if let Some(sym) = contract.strip_prefix("O:") {
        if sym.len() >= 15 {
            let len = sym.len();
            let date_start = len - 15; // 6 date + 1 dir + 8 strike
            underlying = sym[..date_start].to_string();
            let y = &sym[date_start..date_start + 2];
            let m = &sym[date_start + 2..date_start + 4];
            let d = &sym[date_start + 4..date_start + 6];
            if let (Ok(yy), Ok(mm), Ok(dd)) = (y.parse::<u32>(), m.parse::<u32>(), d.parse::<u32>())
            {
                let year = 2000 + yy as i32;
                if let Some(date) = NaiveDate::from_ymd_opt(year, mm, dd) {
                    if let Some(ts) = date
                        .and_hms_opt(0, 0, 0)
                        .and_then(|dt| dt.and_utc().timestamp_nanos_opt())
                    {
                        expiry_ts_ns = ts;
                    }
                }
            }
            dir = sym.chars().nth(len - 9).unwrap_or('C');
            if let Ok(v) = sym[len - 8..].parse::<u32>() {
                strike = (v as f64) / 1000.0;
            }
        }
    }
    (dir, strike, expiry_ts_ns, underlying)
}

#[cfg(test)]
mod tests {
    use super::parse_opra_contract;

    #[test]
    fn test_parse_calls_and_puts() {
        let (dir, strike, expiry, underlying) = parse_opra_contract("O:SPY241220P00720000", None);
        assert_eq!(dir, 'P');
        assert_eq!(strike, 720.0);
        assert_eq!(underlying, "SPY");
        // 2024-12-20
        assert!(expiry > 0);

        let (dir2, strike2, _, und2) = parse_opra_contract("O:TSLA251219C00650000", None);
        assert_eq!(dir2, 'C');
        assert_eq!(strike2, 650.0);
        assert_eq!(und2, "TSLA");
    }
}
