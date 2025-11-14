// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! WebSocket ingestion (stub).

use std::sync::Once;

pub mod worker;

static TLS_PROVIDER_ONCE: Once = Once::new();

fn ensure_tls_provider() {
    TLS_PROVIDER_ONCE.call_once(|| {
        if let Err(err) = rustls::crypto::ring::default_provider().install_default() {
            panic!("failed to install rustls crypto provider: {:?}", err);
        }
    });
}

pub(crate) use ensure_tls_provider as install_rustls_provider;
