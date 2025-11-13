use thiserror::Error;
use window_space::WindowIndex;

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
    #[error("window metadata missing for window_idx {window_idx}")]
    MissingWindowMeta { window_idx: WindowIndex },
    #[error("out-of-order window: encountered {encountered} after closing {current}")]
    OutOfOrderWindow {
        current: WindowIndex,
        encountered: WindowIndex,
    },
    #[error("timestamp regression for {symbol} in window {window_idx}: {ts} < {last_ts}")]
    TimestampRegression {
        symbol: String,
        window_idx: WindowIndex,
        last_ts: i64,
        ts: i64,
    },
}
