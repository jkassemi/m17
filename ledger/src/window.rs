use time::{
    Date, Month, OffsetDateTime, Time, Weekday,
    macros::{date, time},
};

pub type MinuteIndex = u32;

/// Default minutes per full trading session.
pub const MINUTES_PER_SESSION: usize = 390;

#[derive(Clone, Copy, Debug)]
pub struct WindowMeta {
    pub minute_idx: MinuteIndex,
    pub start_ts: i64,
    pub schema_version: u32,
}

impl WindowMeta {
    pub fn new(minute_idx: MinuteIndex, start_ts: i64, schema_version: u32) -> Self {
        Self {
            minute_idx,
            start_ts,
            schema_version,
        }
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
        builder.add_session(session_start_ts, MINUTES_PER_SESSION as MinuteIndex, 1);
        builder.build()
    }

    pub fn from_range(config: &WindowRangeConfig) -> Self {
        Self::from_bounds(
            config.start_date(),
            config.end_date(),
            config.session_open,
            config.session_minutes,
            config.schema_version,
        )
    }

    pub fn from_bounds(
        start: Date,
        end: Date,
        session_open: Time,
        session_minutes: MinuteIndex,
        schema_version: u32,
    ) -> Self {
        let mut builder = WindowSpaceBuilder::new();
        let mut current = start;
        while current <= end {
            if !matches!(current.weekday(), Weekday::Saturday | Weekday::Sunday) {
                let session_start = session_start_timestamp(current, session_open);
                builder.add_session(session_start, session_minutes, schema_version);
            }
            current = current.next_day().expect("valid date range");
        }
        builder.build()
    }

    pub fn minute(&self, minute_idx: MinuteIndex) -> Option<&WindowMeta> {
        self.windows.get(minute_idx as usize)
    }

    pub fn len(&self) -> usize {
        self.windows.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &WindowMeta> {
        self.windows.iter()
    }

    /// Returns the minute index whose window contains `timestamp`.
    pub fn minute_idx_for_timestamp(&self, timestamp: i64) -> Option<MinuteIndex> {
        if self.windows.is_empty() {
            return None;
        }

        let mut low = 0;
        let mut high = self.windows.len();
        while low < high {
            let mid = (low + high) / 2;
            let meta = &self.windows[mid];
            let end_ts = meta.start_ts + 60;
            if timestamp < meta.start_ts {
                high = mid;
            } else if timestamp >= end_ts {
                low = mid + 1;
            } else {
                return Some(meta.minute_idx);
            }
        }
        None
    }
}

pub struct WindowSpaceBuilder {
    windows: Vec<WindowMeta>,
    next_idx: MinuteIndex,
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
        minutes: MinuteIndex,
        schema_version: u32,
    ) {
        for minute in 0..minutes {
            let idx = self.next_idx;
            self.next_idx = self
                .next_idx
                .checked_add(1)
                .expect("window minute index overflow");
            let start_ts = session_start_ts + minute as i64 * 60;
            self.windows
                .push(WindowMeta::new(idx, start_ts, schema_version));
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
    pub session_minutes: MinuteIndex,
    pub schema_version: u32,
}

impl Default for WindowRangeConfig {
    fn default() -> Self {
        Self {
            anchor_date: date!(2025 - 11 - 12),
            months_back: 6,
            months_forward: 6,
            session_open: time!(14:30:00),
            session_minutes: MINUTES_PER_SESSION as MinuteIndex,
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
    fn minute_lookup() {
        let ws = WindowSpace::standard(1_600_000_000);
        assert_eq!(ws.minute_idx_for_timestamp(1_600_000_000), Some(0));
        assert_eq!(ws.minute_idx_for_timestamp(1_600_000_059), Some(0));
        assert_eq!(ws.minute_idx_for_timestamp(1_600_000_060), Some(1));
        assert_eq!(
            ws.minute_idx_for_timestamp(1_600_000_060 + 60 * 10),
            Some(11)
        );
        assert_eq!(ws.minute_idx_for_timestamp(1_599_999_999), None);
    }
}
