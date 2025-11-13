use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

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
    let final_path = cfg.state_dir.join(relative_path);
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_relative = format!("{relative_path}.tmp");
    let tmp_path = cfg.state_dir.join(&tmp_relative);
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if tmp_path.exists() {
        fs::remove_file(&tmp_path)?;
    }
    let file = File::create(&tmp_path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    if final_path.exists() {
        fs::remove_file(&final_path)?;
    }
    fs::rename(&tmp_path, &final_path)?;
    let checksum = compute_checksum(&final_path)?;
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
    window_idx: window_space::WindowIndex,
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
        window_idx,
        safe_symbol,
        ext
    )
}

pub fn cleanup_partial_artifacts(
    cfg: &FlatfileRuntimeConfig,
    relative_path: &str,
) -> Result<(), FlatfileError> {
    let final_path = cfg.state_dir.join(relative_path);
    if final_path.exists() {
        fs::remove_file(&final_path)?;
    }
    let tmp_path = cfg.state_dir.join(format!("{relative_path}.tmp"));
    if tmp_path.exists() {
        fs::remove_file(tmp_path)?;
    }
    Ok(())
}
