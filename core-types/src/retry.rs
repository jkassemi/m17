// Copyright (c) James Kassemi, SC, US. All rights reserved.
use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;

/// Simple jittered exponential backoff policy for async operations.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: usize,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter_pct: f64,
}

impl RetryPolicy {
    pub fn new(
        max_attempts: usize,
        base_delay_ms: u64,
        max_delay_ms: u64,
        jitter_pct: f64,
    ) -> Self {
        let clamped_attempts = max_attempts.max(1);
        let clamped_base = base_delay_ms.max(1);
        let clamped_max_delay = max_delay_ms.max(clamped_base);
        let clamped_jitter = jitter_pct.clamp(0.0, 1.0);
        Self {
            max_attempts: clamped_attempts,
            base_delay_ms: clamped_base,
            max_delay_ms: clamped_max_delay,
            jitter_pct: clamped_jitter,
        }
    }

    pub fn default_network() -> Self {
        Self::new(5, 250, 5_000, 0.25)
    }

    fn next_delay(&self, attempt: usize) -> Duration {
        let exp = 2_u64.saturating_pow(attempt as u32);
        let mut delay = self.base_delay_ms.saturating_mul(exp);
        if delay > self.max_delay_ms {
            delay = self.max_delay_ms;
        }
        let jitter = if self.jitter_pct > 0.0 {
            let mut rng = rand::thread_rng();
            let spread = (delay as f64 * self.jitter_pct) as i64;
            let delta = rng.gen_range(-(spread as i64)..=(spread as i64));
            delay.saturating_add_signed(delta)
        } else {
            delay
        };
        Duration::from_millis(jitter)
    }

    pub async fn retry_async<F, Fut, T, E>(&self, mut op: F) -> Result<T, E>
    where
        F: FnMut(usize) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let mut attempt = 0;
        loop {
            match op(attempt).await {
                Ok(val) => return Ok(val),
                Err(err) => {
                    attempt += 1;
                    if attempt >= self.max_attempts {
                        return Err(err);
                    }
                    let delay = self.next_delay(attempt - 1);
                    sleep(delay).await;
                }
            }
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(5, 250, 5_000, 0.2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::time::{advance, pause};

    #[test]
    fn new_clamps_input_parameters() {
        let policy = RetryPolicy::new(0, 0, 0, 2.0);
        assert_eq!(policy.max_attempts, 1);
        assert_eq!(policy.base_delay_ms, 1);
        assert_eq!(policy.max_delay_ms, 1);
        assert_eq!(policy.jitter_pct, 1.0);
    }

    #[test]
    fn next_delay_doubles_and_caps() {
        let policy = RetryPolicy::new(5, 100, 500, 0.0);
        let delays: Vec<_> = (0..5).map(|attempt| policy.next_delay(attempt)).collect();
        assert_eq!(delays[0], Duration::from_millis(100));
        assert_eq!(delays[1], Duration::from_millis(200));
        assert_eq!(delays[2], Duration::from_millis(400));
        assert_eq!(delays[3], Duration::from_millis(500)); // capped
        assert_eq!(delays[4], Duration::from_millis(500));
    }

    #[tokio::test]
    async fn retry_async_retries_until_success() {
        pause();
        let policy = RetryPolicy::new(3, 10, 10, 0.0);
        let attempts = Arc::new(AtomicUsize::new(0));
        let advancer = tokio::spawn(async {
            advance(Duration::from_millis(10)).await;
            advance(Duration::from_millis(10)).await;
        });

        let result: Result<&'static str, &str> = policy
            .retry_async(|attempt| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt < 2 {
                        Err("boom")
                    } else {
                        Ok("ok")
                    }
                }
            })
            .await;

        advancer.await.unwrap();
        assert_eq!(result.unwrap(), "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_async_stops_after_max_attempts() {
        pause();
        let policy = RetryPolicy::new(2, 5, 5, 0.0);
        let attempts = Arc::new(AtomicUsize::new(0));
        let advancer = tokio::spawn(async { advance(Duration::from_millis(5)).await });

        let result: Result<(), &str> = policy
            .retry_async(|_| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err("nope")
                }
            })
            .await;

        advancer.await.unwrap();
        assert_eq!(result, Err("nope"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
