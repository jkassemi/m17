use thiserror::Error;

#[derive(Debug, Error)]
pub enum FlatfileError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sdk error: {0}")]
    Sdk(String),
    #[error("csv error: {0}")]
    Csv(#[from] csv_async::Error),
    #[error("window space error: {0}")]
    Ledger(#[from] window_space::WindowSpaceError),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}
