impl Engine for OptionQuoteFlatfileEngine {
    fn start(&self) -> EngineResult<()> {
        self.inner
            .config
            .ensure_dirs()
            .map_err(|err| EngineError::Failure { source: err.into() })?;
        let mut guard = self.inner.state.lock();
        if matches!(*guard, EngineRuntimeState::Running(_)) {
            return Err(EngineError::AlreadyRunning);
        }
        let runtime = Runtime::new().map_err(|err| EngineError::Failure { source: err.into() })?;
        let cancel = CancellationToken::new();
        let runner = self.inner.clone();
        let cancel_clone = cancel.clone();
        let handle = runtime.spawn(async move {
            runner.run(cancel_clone).await;
        });
        *guard = EngineRuntimeState::Running(RuntimeBundle {
            runtime,
            handle,
            cancel,
        });
        info!(
            "[{}] option quote flatfile engine started",
            self.inner.config.label
        );
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        let mut guard = self.inner.state.lock();
        let bundle = match std::mem::replace(&mut *guard, EngineRuntimeState::Stopped) {
            EngineRuntimeState::Running(bundle) => bundle,
            EngineRuntimeState::Stopped => return Err(EngineError::NotRunning),
        };
        bundle.cancel.cancel();
        if let Err(err) = bundle.runtime.block_on(async { bundle.handle.await }) {
            error!("option quote flatfile engine join error: {err}");
        }
        info!(
            "[{}] option quote flatfile engine stopped",
            self.inner.config.label
        );
        Ok(())
    }

    fn health(&self) -> EngineHealth {
        let guard = self.inner.state.lock();
        match &*guard {
            EngineRuntimeState::Running(_) => EngineHealth::new(HealthStatus::Ready, None),
            EngineRuntimeState::Stopped => EngineHealth::new(HealthStatus::Stopped, None),
        }
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription {
            supports_priority_regions: false,
            notes: Some("Option quote flatfile ingestion".into()),
        }
    }
}

impl Engine for UnderlyingFlatfileEngine {
    fn start(&self) -> EngineResult<()> {
        self.inner
            .config
            .ensure_dirs()
            .map_err(|err| EngineError::Failure { source: err.into() })?;
        let mut guard = self.inner.state.lock();
        if matches!(*guard, EngineRuntimeState::Running(_)) {
            return Err(EngineError::AlreadyRunning);
        }
        let runtime = Runtime::new().map_err(|err| EngineError::Failure { source: err.into() })?;
        let cancel = CancellationToken::new();
        let runner = self.inner.clone();
        let cancel_clone = cancel.clone();
        let handle = runtime.spawn(async move {
            runner.run(cancel_clone).await;
        });
        *guard = EngineRuntimeState::Running(RuntimeBundle {
            runtime,
            handle,
            cancel,
        });
        info!(
            "[{}] underlying flatfile engine started",
            self.inner.config.label
        );
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        let mut guard = self.inner.state.lock();
        let bundle = match std::mem::replace(&mut *guard, EngineRuntimeState::Stopped) {
            EngineRuntimeState::Running(bundle) => bundle,
            EngineRuntimeState::Stopped => return Err(EngineError::NotRunning),
        };
        bundle.cancel.cancel();
        if let Err(err) = bundle.runtime.block_on(async { bundle.handle.await }) {
            error!("underlying flatfile engine join error: {err}");
        }
        info!(
            "[{}] underlying flatfile engine stopped",
            self.inner.config.label
        );
        Ok(())
    }

    fn health(&self) -> EngineHealth {
        let guard = self.inner.state.lock();
        match &*guard {
            EngineRuntimeState::Running(_) => EngineHealth::new(HealthStatus::Ready, None),
            EngineRuntimeState::Stopped => EngineHealth::new(HealthStatus::Stopped, None),
        }
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription {
            supports_priority_regions: false,
            notes: Some("Underlying trade/quote flatfile ingestion".into()),
        }
    }
}
