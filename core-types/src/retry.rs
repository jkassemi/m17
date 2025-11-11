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
        Self {
            max_attempts: max_attempts.max(1),
            base_delay_ms: base_delay_ms.max(1),
            max_delay_ms: max_delay_ms.max(base_delay_ms),
            jitter_pct: jitter_pct.clamp(0.0, 1.0),
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
