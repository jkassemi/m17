use std::{fs::File, io::Read, path::Path};

use arrow::record_batch::RecordBatch;
use chrono::Datelike;
use crc32fast::Hasher as Crc32;
use parquet::arrow::ArrowWriter;

use crate::{config::FlatfileRuntimeConfig, errors::FlatfileError};

pub struct ArtifactInfo {
    pub relative_path: String,
    pub checksum: u32,
}

pub fn write_record_batch(
    cfg: &FlatfileRuntimeConfig,
    relative_path: &str,
    batch: &RecordBatch,
) -> Result<ArtifactInfo, FlatfileError> {
    let path = cfg.state_dir.join(relative_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    let checksum = compute_checksum(&path)?;
    Ok(ArtifactInfo {
        relative_path: relative_path.to_string(),
        checksum,
    })
}

pub fn compute_checksum(path: &Path) -> Result<u32, FlatfileError> {
    let mut file = File::open(path)?;
    let mut hasher = Crc32::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

pub fn sanitized_symbol(symbol: &str) -> String {
    symbol
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

pub fn artifact_path(
    cfg: &FlatfileRuntimeConfig,
    family: &str,
    date: chrono::NaiveDate,
    symbol: &str,
    minute_idx: window_space::MinuteIndex,
    ext: &str,
) -> String {
    let safe_symbol = sanitized_symbol(symbol);
    format!(
        "trade-flatfile/artifacts/{}/{}/{}-{:02}-{:02}/{:04}/{}.{}",
        family,
        cfg.label,
        date.year(),
        date.month(),
        date.day(),
        minute_idx,
        safe_symbol,
        ext
    )
}
