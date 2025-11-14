use std::{
    fs::OpenOptions,
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json;

/// Treasury curve snapshot applied across many symbols.
/// Engines store the scaled tenor array directly; no external artifact is needed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RfRatePayload {
    pub schema_version: u8,
    pub curve_id: u16,
    pub effective_ts: i64,
    pub next_effective_ts: i64,
    pub tenor_bps: [i32; 12],
}

/// Window-aligned batch of trades written by `trade-engine`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradeBatchPayload {
    pub schema_version: u8,
    pub window_ts: i64,
    pub batch_id: u32,
    pub first_trade_ts: i64,
    pub last_trade_ts: i64,
    pub record_count: u32,
    pub artifact_uri: String,
    pub checksum: u32,
}

/// Window-aligned batch of quotes/NBBO samples.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuoteBatchPayload {
    pub schema_version: u8,
    pub window_ts: i64,
    pub batch_id: u32,
    pub first_quote_ts: i64,
    pub last_quote_ts: i64,
    pub nbbo_sample_count: u32,
    pub artifact_uri: String,
    pub checksum: u32,
}

/// Derived aggressor/NBBO payload produced once trade+quote prereqs exist.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggressorPayload {
    pub schema_version: u8,
    pub window_ts: i64,
    pub trade_payload_id: u32,
    pub quote_payload_id: u32,
    pub observation_count: u32,
    pub artifact_uri: String,
    pub checksum: u32,
}

/// Greeks overlay materialized per symbol+window.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GreeksPayload {
    pub schema_version: u8,
    pub window_ts: i64,
    pub rf_rate_payload_id: u32,
    pub greeks_version: u32,
    pub artifact_uri: String,
    pub checksum: u32,
}

/// Aggregation payload persisted per symbol/window.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregationPayload {
    pub schema_version: u8,
    pub symbol: String,
    pub window: String,
    pub window_start_ns: i64,
    pub window_end_ns: i64,
    pub artifact_uri: String,
    pub checksum: u32,
}

pub struct MappingStore<T> {
    path: PathBuf,
    entries: Vec<T>,
}

impl<T> MappingStore<T>
where
    T: Serialize + DeserializeOwned + Clone,
{
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Ok(Self {
                path,
                entries: Vec::new(),
            });
        }

        let file = OpenOptions::new().read(true).open(&path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let record = serde_json::from_str(&line).map_err(json_err)?;
            entries.push(record);
        }
        Ok(Self { path, entries })
    }

    pub fn append(&mut self, payload: T) -> io::Result<u32> {
        let id = (self.entries.len() + 1) as u32;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let line = serde_json::to_vec(&payload).map_err(json_err)?;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        self.entries.push(payload);
        Ok(id)
    }

    pub fn get(&self, payload_id: u32) -> Option<&T> {
        if payload_id == 0 {
            return None;
        }
        self.entries.get((payload_id - 1) as usize)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

pub struct PayloadStores {
    pub rf_rate: MappingStore<RfRatePayload>,
    pub trades: MappingStore<TradeBatchPayload>,
    pub quotes: MappingStore<QuoteBatchPayload>,
    pub aggressor: MappingStore<AggressorPayload>,
    pub greeks: MappingStore<GreeksPayload>,
    pub aggregations: MappingStore<AggregationPayload>,
}

impl PayloadStores {
    pub fn load(base_dir: impl AsRef<Path>) -> io::Result<Self> {
        let base = base_dir.as_ref();
        Ok(Self {
            rf_rate: MappingStore::load(base.join("rf_rate.map"))?,
            trades: MappingStore::load(base.join("trades.map"))?,
            quotes: MappingStore::load(base.join("quotes.map"))?,
            aggressor: MappingStore::load(base.join("aggressor.map"))?,
            greeks: MappingStore::load(base.join("greeks.map"))?,
            aggregations: MappingStore::load(base.join("aggregations.map"))?,
        })
    }
}

fn json_err(err: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err)
}
