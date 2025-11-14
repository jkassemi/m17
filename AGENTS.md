# Repository Guidelines

1. MINIMAL CHANGES. DO NOT EXTRAPOLATE OR EXTEND ENGINEERING BEYOND THE USER'S REQUEST.
2. The user struggles with ADHD. They may focus on a portion of your message during your response. Take this into consideration and confirm/adapt to the conversation as necessary.
3. The entire project here was written over the past two days. It's possible to move quickly. Unless it's helpful for your framing, avoid considering explicit timeframes or subjective assignment of words like "massive" or "major" for what may be relatively simple tasks given the velocity.
4. Let's always make sure we know the specifics of how a data file is sorted/ordered before we use it. And the production version schema. That would avoid a lot of problems.
5. Before you remove or refactor code, take a brief moment to consider what role it could have played or may play elsewhere.
6. Before you add or refactor code, think about whether you're adding an unnecessary layer of indirection.

## Project Structure & Module Organization

- Root is a Cargo workspace (`Cargo.toml`) with service crates under directories like `orchestrator/`, `treasury-ingestion-service/`, `tui/`, and shared libraries such as `core-types/`, `metrics/`, and `nbbo-cache/`.
- Runtime configuration now lives in code (`m17/src/config.rs`); there is no repo-level `config.toml`. Logs default to `orchestrator.log`.
- Tests reside alongside sources (e.g., `classifier/src/greeks.rs`), so `cargo test -p <crate>` discovers them automatically.

## Build, Test, and Development Commands

- `cargo fmt`: formats all Rust crates—run before opening a PR.
- `cargo check`: fast verify that every crate compiles; orchestrator depends on most others, so this surfaces integration issues early.
- `cargo test` or `cargo test -p <crate>`: executes unit/integration tests; prefer scoped runs when iterating on a single service.
- `cargo run -p orchestrator`: boots the legacy orchestrator (still expects a config file; we no longer ship one). `cargo run -p m17 dev` is the canonical path with hardcoded settings.
- It's fine (and preferred) to make real requests (within reason) to remote
APIs during testing for shape/content validation.

## Coding Style & Naming Conventions

- Use `cargo fmt`’s default style (4-space indent, trailing commas where possible).
- Configuration structs live in `core-types/src/config.rs` and follow `snake_case` TOML keys that map directly to Rust fields via Serde.
- Status/trait types are centralized in `core-types::status`; reuse those structs instead of redefining ad‑hoc status enums.

## Testing Guidelines

- Prefer crate-level tests colocated with their modules (`#[cfg(test)]` blocks). Use `tokio::test` for async flows (e.g., `treasury-ingestion-service`).
- Name tests after behavioral expectations (`test_enrich_batch_populates_greeks`). Include scenario-specific comments when logic is non-trivial.
- There is no mandatory coverage gate, but new features should ship with regression tests covering happy-path and error cases.

## Commit & Pull Request Guidelines

- Follow conventional, descriptive commit messages (“orchestrator: wire service status into metrics”)—present tense, scope prefix when reasonable.
- PRs should describe motivation, outline subsystem impact (e.g., “touches `core-types`, `metrics`, `tui`”), and link issues when applicable.
- Include verification steps (`cargo check`, relevant `cargo test` targets) and artifacts such as screenshots/log excerpts for UI or operational changes.

## Security & Configuration Tips

- Secrets (Massive API keys, flatfile credentials) stay out of git. For the new stack we read them from env vars in `m17/src/config.rs`; do not commit plaintext credentials anywhere.
- Services now treat missing treasury data as `CRIT`; ensure staging/prod environments expose reachable `/fed/v1/treasury-yields` endpoints before deploys.

## Mindset & Operational Expectations

- Treat every edit as production work: assume the orchestrator is running in live environments and changes must be bulletproof before merging.
- Avoid “prototype” language or shortcuts in code/comments; if functionality is missing, file an issue or implement it rather than leaving TODOs.
- Default to rigorous validation (e.g., `cargo check`, relevant `cargo test`, manual runbooks) before submitting a PR so reviewers focus on design, not stability gaps.
