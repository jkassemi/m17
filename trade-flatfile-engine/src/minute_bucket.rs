pub trait HasTimestamp {
    fn timestamp_ns(&self) -> i64;
}

/// Collects records that fall inside the same minute window.
pub struct MinuteBucket<T> {
    pub records: Vec<T>,
    pub minute_start_ns: i64,
    pub first_ts: i64,
    pub last_ts: i64,
}

impl<T> MinuteBucket<T> {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            minute_start_ns: 0,
            first_ts: i64::MAX,
            last_ts: i64::MIN,
        }
    }

    pub fn observe(&mut self, record: T)
    where
        T: HasTimestamp,
    {
        let ts = record.timestamp_ns();
        if self.records.is_empty() {
            self.minute_start_ns = align_to_minute(ts);
        }
        self.first_ts = self.first_ts.min(ts);
        self.last_ts = self.last_ts.max(ts);
        self.records.push(record);
    }
}

const MINUTE_NS: i64 = 60 * 1_000_000_000;

fn align_to_minute(ts: i64) -> i64 {
    ts - (ts % MINUTE_NS)
}
