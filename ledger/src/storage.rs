use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

use memmap2::MmapMut;
use time::OffsetDateTime;

const LEDGER_MAGIC: &[u8; 8] = b"M17LEDGR";
const LEDGER_HEADER_SIZE: usize = 64;
const ZERO_CHUNK_LEN: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct LedgerFileStats {
    pub path: PathBuf,
    pub created: bool,
    pub creation_duration: Option<Duration>,
    pub file_size: u64,
    pub created_at_s: i64,
}

pub struct LedgerFileOptions {
    pub path: PathBuf,
    pub minute_count: usize,
    pub max_symbols: usize,
    pub row_size: usize,
    pub schema_version: u32,
}

pub struct LedgerFile {
    _file: File,
    mmap: MmapMut,
    _options: LedgerFileOptions,
    _header: LedgerFileHeader,
}

impl LedgerFile {
    pub fn open(options: LedgerFileOptions) -> io::Result<(Self, LedgerFileStats)> {
        let total_len = compute_total_len(&options)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&options.path)?;
        let existed = file.metadata()?.len() == total_len;
        let (header, stats) = if existed {
            let header = read_header(&mut file)?;
            validate_header(&header, &options)?;
            (
                header,
                LedgerFileStats {
                    path: options.path.clone(),
                    created: false,
                    creation_duration: None,
                    file_size: total_len,
                    created_at_s: header.created_at_s,
                },
            )
        } else {
            file.set_len(0)?;
            let start = Instant::now();
            zero_fill(&mut file, total_len)?;
            let duration = start.elapsed();
            let header = LedgerFileHeader::new(&options);
            write_header(&mut file, &header)?;
            file.sync_data()?;
            (
                header,
                LedgerFileStats {
                    path: options.path.clone(),
                    created: true,
                    creation_duration: Some(duration),
                    file_size: total_len,
                    created_at_s: header.created_at_s,
                },
            )
        };
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok((
            Self {
                _file: file,
                mmap,
                _options: options,
                _header: header,
            },
            stats,
        ))
    }

    pub fn data_ptr(&self) -> *mut u8 {
        unsafe { self.mmap.as_ptr().add(LEDGER_HEADER_SIZE) as *mut u8 }
    }
}

fn compute_total_len(options: &LedgerFileOptions) -> io::Result<u64> {
    let minute_bytes = options
        .minute_count
        .checked_mul(options.row_size)
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "minute_count overflow"))?;
    let data_bytes = minute_bytes
        .checked_mul(options.max_symbols)
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "symbol count overflow"))?;
    let total = data_bytes
        .checked_add(LEDGER_HEADER_SIZE)
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "ledger size overflow"))?;
    Ok(total as u64)
}

fn zero_fill(file: &mut File, total_len: u64) -> io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let zero_block = vec![0u8; ZERO_CHUNK_LEN];
    let mut remaining = total_len;
    while remaining > 0 {
        let write_len = remaining.min(ZERO_CHUNK_LEN as u64) as usize;
        file.write_all(&zero_block[..write_len])?;
        remaining -= write_len as u64;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct LedgerFileHeader {
    schema_version: u32,
    minute_count: u64,
    row_size: u32,
    max_symbols: u32,
    created_at_s: i64,
}

impl LedgerFileHeader {
    fn new(options: &LedgerFileOptions) -> Self {
        Self {
            schema_version: options.schema_version,
            minute_count: options.minute_count as u64,
            row_size: options.row_size as u32,
            max_symbols: options.max_symbols as u32,
            created_at_s: OffsetDateTime::now_utc().unix_timestamp(),
        }
    }
}

fn read_header(file: &mut File) -> io::Result<LedgerFileHeader> {
    file.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; LEDGER_HEADER_SIZE];
    file.read_exact(&mut buf)?;
    if &buf[0..LEDGER_MAGIC.len()] != LEDGER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ledger file missing magic header",
        ));
    }
    let schema_version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    let minute_count = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let row_size = u32::from_le_bytes(buf[20..24].try_into().unwrap());
    let max_symbols = u32::from_le_bytes(buf[24..28].try_into().unwrap());
    let created_at_s = i64::from_le_bytes(buf[28..36].try_into().unwrap());
    Ok(LedgerFileHeader {
        schema_version,
        minute_count,
        row_size,
        max_symbols,
        created_at_s,
    })
}

fn write_header(file: &mut File, header: &LedgerFileHeader) -> io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; LEDGER_HEADER_SIZE];
    buf[..LEDGER_MAGIC.len()].copy_from_slice(LEDGER_MAGIC);
    buf[8..12].copy_from_slice(&header.schema_version.to_le_bytes());
    buf[12..20].copy_from_slice(&header.minute_count.to_le_bytes());
    buf[20..24].copy_from_slice(&header.row_size.to_le_bytes());
    buf[24..28].copy_from_slice(&header.max_symbols.to_le_bytes());
    buf[28..36].copy_from_slice(&header.created_at_s.to_le_bytes());
    file.write_all(&buf)?;
    Ok(())
}

fn validate_header(header: &LedgerFileHeader, options: &LedgerFileOptions) -> io::Result<()> {
    if header.schema_version != options.schema_version {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "ledger schema mismatch (file={}, expected={})",
                header.schema_version, options.schema_version
            ),
        ));
    }
    if header.minute_count != options.minute_count as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ledger minute count mismatch",
        ));
    }
    if header.row_size != options.row_size as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ledger row size mismatch",
        ));
    }
    if header.max_symbols != options.max_symbols as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "ledger symbol capacity mismatch",
        ));
    }
    Ok(())
}
