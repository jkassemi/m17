use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

use memmap2::MmapMut;
use time::OffsetDateTime;

const WINDOW_SPACE_MAGIC: &[u8; 8] = b"M17LEDGR";
const WINDOW_SPACE_HEADER_SIZE: usize = 64;
const ZERO_CHUNK_LEN: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct WindowSpaceFileStats {
    pub path: PathBuf,
    pub created: bool,
    pub creation_duration: Option<Duration>,
    pub file_size: u64,
    pub created_at_s: i64,
}

pub struct WindowSpaceFileOptions {
    pub path: PathBuf,
    pub window_count: usize,
    pub max_symbols: usize,
    pub row_size: usize,
    pub schema_version: u32,
}

pub struct WindowSpaceFile {
    _file: File,
    mmap: MmapMut,
    _options: WindowSpaceFileOptions,
    _header: WindowSpaceFileHeader,
}

impl WindowSpaceFile {
    pub fn open(options: WindowSpaceFileOptions) -> io::Result<(Self, WindowSpaceFileStats)> {
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
                WindowSpaceFileStats {
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
            let header = WindowSpaceFileHeader::new(&options);
            write_header(&mut file, &header)?;
            file.sync_data()?;
            (
                header,
                WindowSpaceFileStats {
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
        unsafe { self.mmap.as_ptr().add(WINDOW_SPACE_HEADER_SIZE) as *mut u8 }
    }
}

fn compute_total_len(options: &WindowSpaceFileOptions) -> io::Result<u64> {
    let window_bytes = options
        .window_count
        .checked_mul(options.row_size)
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "window_count overflow"))?;
    let data_bytes = window_bytes
        .checked_mul(options.max_symbols)
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "symbol count overflow"))?;
    let total = data_bytes
        .checked_add(WINDOW_SPACE_HEADER_SIZE)
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "window-space size overflow"))?;
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
struct WindowSpaceFileHeader {
    schema_version: u32,
    window_count: u64,
    row_size: u32,
    max_symbols: u32,
    created_at_s: i64,
}

impl WindowSpaceFileHeader {
    fn new(options: &WindowSpaceFileOptions) -> Self {
        Self {
            schema_version: options.schema_version,
            window_count: options.window_count as u64,
            row_size: options.row_size as u32,
            max_symbols: options.max_symbols as u32,
            created_at_s: OffsetDateTime::now_utc().unix_timestamp(),
        }
    }
}

fn read_header(file: &mut File) -> io::Result<WindowSpaceFileHeader> {
    file.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; WINDOW_SPACE_HEADER_SIZE];
    file.read_exact(&mut buf)?;
    if &buf[0..WINDOW_SPACE_MAGIC.len()] != WINDOW_SPACE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "window-space file missing magic header",
        ));
    }
    let schema_version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    let window_count = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let row_size = u32::from_le_bytes(buf[20..24].try_into().unwrap());
    let max_symbols = u32::from_le_bytes(buf[24..28].try_into().unwrap());
    let created_at_s = i64::from_le_bytes(buf[28..36].try_into().unwrap());
    Ok(WindowSpaceFileHeader {
        schema_version,
        window_count,
        row_size,
        max_symbols,
        created_at_s,
    })
}

fn write_header(file: &mut File, header: &WindowSpaceFileHeader) -> io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; WINDOW_SPACE_HEADER_SIZE];
    buf[..WINDOW_SPACE_MAGIC.len()].copy_from_slice(WINDOW_SPACE_MAGIC);
    buf[8..12].copy_from_slice(&header.schema_version.to_le_bytes());
    buf[12..20].copy_from_slice(&header.window_count.to_le_bytes());
    buf[20..24].copy_from_slice(&header.row_size.to_le_bytes());
    buf[24..28].copy_from_slice(&header.max_symbols.to_le_bytes());
    buf[28..36].copy_from_slice(&header.created_at_s.to_le_bytes());
    file.write_all(&buf)?;
    Ok(())
}

fn validate_header(
    header: &WindowSpaceFileHeader,
    options: &WindowSpaceFileOptions,
) -> io::Result<()> {
    if header.schema_version != options.schema_version {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "window-space schema mismatch (file={}, expected={})",
                header.schema_version, options.schema_version
            ),
        ));
    }
    if header.window_count != options.window_count as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "window-space window count mismatch",
        ));
    }
    if header.row_size != options.row_size as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "window-space row size mismatch",
        ));
    }
    if header.max_symbols != options.max_symbols as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "window-space symbol capacity mismatch",
        ));
    }
    Ok(())
}
