use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type SymbolId = u16;

#[derive(Debug, Error)]
pub enum SymbolMapError {
    #[error("symbol limit of {limit} exhausted")]
    Exhausted { limit: SymbolId },
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Serialize, Deserialize)]
struct SymbolMapFile {
    symbols: Vec<String>,
}

/// Bi-directional symbol registry backed by a simple json file.
#[derive(Debug, Clone)]
pub struct SymbolMap {
    forward: Vec<String>,
    reverse: HashMap<String, SymbolId>,
    max_symbols: SymbolId,
    path: PathBuf,
}

impl SymbolMap {
    pub fn load_or_init(
        path: impl AsRef<Path>,
        max_symbols: SymbolId,
    ) -> Result<Self, SymbolMapError> {
        let path = path.as_ref().to_path_buf();
        let mut forward = if path.exists() {
            let bytes = fs::read(&path)?;
            if bytes.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice::<SymbolMapFile>(&bytes)
                    .map(|f| f.symbols)
                    .unwrap_or_default()
            }
        } else {
            Vec::new()
        };

        let limit = max_symbols as usize;
        if forward.len() > limit {
            forward.truncate(limit);
        }

        let mut reverse = HashMap::new();
        for (idx, symbol) in forward.iter().enumerate() {
            reverse.insert(symbol.clone(), idx as SymbolId);
        }

        Ok(Self {
            forward,
            reverse,
            max_symbols,
            path,
        })
    }

    pub fn persist(&self) -> Result<(), SymbolMapError> {
        let file = SymbolMapFile {
            symbols: self.forward.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&file).expect("serialize symbol map");
        fs::write(&self.path, bytes)?;
        Ok(())
    }

    pub fn resolve_or_insert(&mut self, symbol: &str) -> Result<SymbolId, SymbolMapError> {
        if let Some(id) = self.reverse.get(symbol) {
            return Ok(*id);
        }

        if self.forward.len() as SymbolId >= self.max_symbols {
            return Err(SymbolMapError::Exhausted {
                limit: self.max_symbols,
            });
        }

        let id = self.forward.len() as SymbolId;
        self.forward.push(symbol.to_string());
        self.reverse.insert(symbol.to_string(), id);
        Ok(id)
    }

    pub fn lookup(&self, id: SymbolId) -> Option<&str> {
        self.forward.get(id as usize).map(|s| s.as_str())
    }

    pub fn iter(&self) -> impl Iterator<Item = (SymbolId, &str)> {
        self.forward
            .iter()
            .enumerate()
            .map(|(idx, sym)| (idx as SymbolId, sym.as_str()))
    }

    pub fn len(&self) -> usize {
        self.forward.len()
    }

    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
