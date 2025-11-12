pub type MinuteIndex = u16;

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
        WindowSpaceBuilder::new()
            .session(session_start_ts, MINUTES_PER_SESSION as u16, 1)
            .build()
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

#[derive(Default)]
pub struct WindowSpaceBuilder {
    windows: Vec<WindowMeta>,
}

impl WindowSpaceBuilder {
    pub fn new() -> Self {
        Self {
            windows: Vec::new(),
        }
    }

    pub fn session(
        mut self,
        session_start_ts: i64,
        minutes: MinuteIndex,
        schema_version: u32,
    ) -> Self {
        assert!(
            self.windows.is_empty(),
            "WindowSpaceBuilder currently supports a single session"
        );
        for minute in 0..minutes {
            let idx = minute as MinuteIndex;
            let start_ts = session_start_ts + minute as i64 * 60;
            self.windows
                .push(WindowMeta::new(idx, start_ts, schema_version));
        }
        self
    }

    pub fn build(self) -> WindowSpace {
        WindowSpace::new(self.windows)
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
