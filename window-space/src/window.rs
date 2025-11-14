use time::{
    Date, Month, OffsetDateTime, Time, Weekday,
    macros::{date, time},
};

pub type WindowIndex = u32;

#[deprecated(
    since = "0.1.0",
    note = "MinuteIndex has been renamed to WindowIndex; update imports to window_space::WindowIndex"
)]
pub type MinuteIndex = WindowIndex;

/// Default windows per full trading session.
pub const WINDOWS_PER_SESSION: usize = 390;

/// Default duration (in seconds) of each window when constructing standard schedules.
pub const DEFAULT_WINDOW_DURATION_SECS: u32 = 60;

#[derive(Clone, Copy, Debug)]
pub struct WindowMeta {
    pub window_idx: WindowIndex,
    pub start_ts: i64,
    pub duration_secs: u32,
    pub schema_version: u32,
}

impl WindowMeta {
    pub fn new(
        window_idx: WindowIndex,
        start_ts: i64,
        duration_secs: u32,
        schema_version: u32,
    ) -> Self {
        Self {
            window_idx,
            start_ts,
            duration_secs,
            schema_version,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Window<'a> {
    meta: &'a WindowMeta,
}

impl<'a> Window<'a> {
    pub fn metadata(&self) -> &WindowMeta {
        self.meta
    }

    pub fn window_idx(&self) -> WindowIndex {
        self.meta.window_idx
    }

    #[deprecated(since = "0.1.0", note = "Use window_idx() instead")]
    pub fn minute_idx(&self) -> WindowIndex {
        self.window_idx()
    }

    pub fn start_ts(&self) -> i64 {
        self.meta.start_ts
    }

    pub fn duration_secs(&self) -> u32 {
        self.meta.duration_secs
    }

    pub fn schema_version(&self) -> u32 {
        self.meta.schema_version
    }
}

impl<'a> From<&'a WindowMeta> for Window<'a> {
    fn from(meta: &'a WindowMeta) -> Self {
        Self { meta }
    }
}

#[derive(Clone, Debug)]
pub struct WindowSpace {
    windows: Vec<WindowMeta>,
}

impl WindowSpace {
    pub fn new(windows: Vec<WindowMeta>) -> Self {
        Self { windows }
    }

    pub fn standard(session_start_ts: i64) -> Self {
        let mut builder = WindowSpaceBuilder::new();
        builder.add_session(
            session_start_ts,
            WINDOWS_PER_SESSION as WindowIndex,
            DEFAULT_WINDOW_DURATION_SECS,
            1,
        );
        builder.build()
    }

    pub fn from_range(config: &WindowRangeConfig) -> Self {
        Self::from_bounds(
            config.start_date(),
            config.end_date(),
            config.session_open,
            config.session_windows,
            config.window_duration_secs,
            config.schema_version,
        )
    }

    pub fn from_bounds(
        start: Date,
        end: Date,
        session_open: Time,
        session_windows: WindowIndex,
        window_duration_secs: u32,
        schema_version: u32,
    ) -> Self {
        let mut builder = WindowSpaceBuilder::new();
        let mut current = start;
        while current <= end {
            if !matches!(current.weekday(), Weekday::Saturday | Weekday::Sunday) {
                let session_start = session_start_timestamp(current, session_open);
                builder.add_session(
                    session_start,
                    session_windows,
                    window_duration_secs,
                    schema_version,
                );
            }
            current = current.next_day().expect("valid date range");
        }
        builder.build()
    }

    pub fn window(&self, window_idx: WindowIndex) -> Option<&WindowMeta> {
        self.windows.get(window_idx as usize)
    }

    #[deprecated(since = "0.1.0", note = "Use window() instead")]
    pub fn minute(&self, minute_idx: WindowIndex) -> Option<&WindowMeta> {
        self.window(minute_idx)
    }

    pub fn windows(&self) -> impl Iterator<Item = Window<'_>> {
        self.windows.iter().map(Window::from)
    }

    pub fn len(&self) -> usize {
        self.windows.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &WindowMeta> {
        self.windows.iter()
    }

    /// Returns the window index whose span contains `timestamp`.
    pub fn window_idx_for_timestamp(&self, timestamp: i64) -> Option<WindowIndex> {
        if self.windows.is_empty() {
            return None;
        }

        let mut low = 0;
        let mut high = self.windows.len();
        while low < high {
            let mid = (low + high) / 2;
            let meta = &self.windows[mid];
            let end_ts = meta.start_ts + meta.duration_secs as i64;
            if timestamp < meta.start_ts {
                high = mid;
            } else if timestamp >= end_ts {
                low = mid + 1;
            } else {
                return Some(meta.window_idx);
            }
        }
        None
    }

    #[deprecated(since = "0.1.0", note = "Use window_idx_for_timestamp() instead")]
    pub fn minute_idx_for_timestamp(&self, timestamp: i64) -> Option<WindowIndex> {
        self.window_idx_for_timestamp(timestamp)
    }
}

pub struct WindowSpaceBuilder {
    windows: Vec<WindowMeta>,
    next_idx: WindowIndex,
}

impl WindowSpaceBuilder {
    pub fn new() -> Self {
        Self {
            windows: Vec::new(),
            next_idx: 0,
        }
    }

    pub fn add_session(
        &mut self,
        session_start_ts: i64,
        windows: WindowIndex,
        window_duration_secs: u32,
        schema_version: u32,
    ) {
        for window in 0..windows {
            let idx = self.next_idx;
            self.next_idx = self.next_idx.checked_add(1).expect("window index overflow");
            let start_ts = session_start_ts + window as i64 * window_duration_secs as i64;
            self.windows.push(WindowMeta::new(
                idx,
                start_ts,
                window_duration_secs,
                schema_version,
            ));
        }
    }

    pub fn build(self) -> WindowSpace {
        WindowSpace::new(self.windows)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WindowRangeConfig {
    pub anchor_date: Date,
    pub months_back: u8,
    pub months_forward: u8,
    pub session_open: Time,
    pub session_windows: WindowIndex,
    pub window_duration_secs: u32,
    pub schema_version: u32,
}

impl Default for WindowRangeConfig {
    fn default() -> Self {
        Self {
            anchor_date: date!(2025 - 11 - 14),
            months_back: 6,
            months_forward: 3,
            session_open: time!(14:30:00),
            session_windows: WINDOWS_PER_SESSION as WindowIndex,
            window_duration_secs: DEFAULT_WINDOW_DURATION_SECS,
            schema_version: 1,
        }
    }
}

impl WindowRangeConfig {
    pub fn start_date(&self) -> Date {
        shift_months(self.anchor_date, -(self.months_back as i32))
    }

    pub fn end_date(&self) -> Date {
        shift_months(self.anchor_date, self.months_forward as i32)
    }
}

fn session_start_timestamp(date: Date, session_open: Time) -> i64 {
    let dt = OffsetDateTime::new_utc(date, session_open);
    dt.unix_timestamp()
}

fn shift_months(date: Date, delta: i32) -> Date {
    let total_months = date.year() * 12 + (date.month() as i32 - 1) + delta;
    let year = total_months.div_euclid(12);
    let month_index = total_months.rem_euclid(12) + 1;
    let month = Month::try_from(month_index as u8).expect("valid month");
    let mut day = date.day();
    loop {
        if let Ok(candidate) = Date::from_calendar_date(year, month, day) {
            return candidate;
        }
        day -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_lookup() {
        let ws = WindowSpace::standard(1_600_000_000);
        assert_eq!(ws.window_idx_for_timestamp(1_600_000_000), Some(0));
        assert_eq!(ws.window_idx_for_timestamp(1_600_000_059), Some(0));
        assert_eq!(ws.window_idx_for_timestamp(1_600_000_060), Some(1));
        assert_eq!(
            ws.window_idx_for_timestamp(1_600_000_060 + 60 * 10),
            Some(11)
        );
        assert_eq!(ws.window_idx_for_timestamp(1_599_999_999), None);
    }
}
