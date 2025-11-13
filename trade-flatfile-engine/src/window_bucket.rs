pub trait HasTimestamp {
    fn timestamp_ns(&self) -> i64;
}

/// Collects records that fall inside the same logical window.
pub struct WindowBucket<T> {
    pub records: Vec<T>,
    pub first_ts: i64,
    pub last_ts: i64,
}

impl<T> WindowBucket<T> {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            first_ts: i64::MAX,
            last_ts: i64::MIN,
        }
    }

    pub fn last_timestamp(&self) -> Option<i64> {
        if self.records.is_empty() {
            None
        } else {
            Some(self.last_ts)
        }
    }

    pub fn observe(&mut self, record: T)
    where
        T: HasTimestamp,
    {
        let ts = record.timestamp_ns();
        self.first_ts = self.first_ts.min(ts);
        self.last_ts = self.last_ts.max(ts);
        self.records.push(record);
    }
}
