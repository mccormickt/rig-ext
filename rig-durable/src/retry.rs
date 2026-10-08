//! Backend-neutral retry settings.

use std::time::Duration;

#[derive(Clone, Debug)]
pub enum BackoffStrategy {
    None,
    Fixed {
        delay: Duration,
    },
    Linear {
        base: Duration,
        max: Duration,
    },
    Exponential {
        base: Duration,
        multiplier: f64,
        max: Duration,
    },
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffStrategy,
    pub timeout: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: BackoffStrategy::Exponential {
                base: Duration::from_millis(100),
                multiplier: 2.0,
                max: Duration::from_secs(30),
            },
            timeout: None,
        }
    }
}

impl RetryPolicy {
    /// Total attempts, including the first call. Zero is treated as one.
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            ..Self::default()
        }
    }

    pub fn with_backoff(mut self, backoff: BackoffStrategy) -> Self {
        self.backoff = backoff;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        match self.backoff {
            BackoffStrategy::None => Duration::ZERO,
            BackoffStrategy::Fixed { delay } => delay,
            BackoffStrategy::Linear { base, max } => base.saturating_mul(attempt).min(max),
            BackoffStrategy::Exponential {
                base,
                multiplier,
                max,
            } => {
                let factor = multiplier.powi(attempt.saturating_sub(1) as i32);
                let nanos = (base.as_nanos() as f64 * factor) as u128;
                Duration::from_nanos(nanos.min(u64::MAX as u128) as u64).min(max)
            }
        }
    }
}

#[cfg(feature = "duroxide")]
impl From<duroxide::RetryPolicy> for RetryPolicy {
    fn from(policy: duroxide::RetryPolicy) -> Self {
        use duroxide::BackoffStrategy as D;
        Self {
            max_attempts: policy.max_attempts,
            timeout: policy.timeout,
            backoff: match policy.backoff {
                D::None => BackoffStrategy::None,
                D::Fixed { delay } => BackoffStrategy::Fixed { delay },
                D::Linear { base, max } => BackoffStrategy::Linear { base, max },
                D::Exponential {
                    base,
                    multiplier,
                    max,
                } => BackoffStrategy::Exponential {
                    base,
                    multiplier,
                    max,
                },
            },
        }
    }
}

#[cfg(feature = "duroxide")]
impl From<RetryPolicy> for duroxide::RetryPolicy {
    fn from(policy: RetryPolicy) -> Self {
        use duroxide::BackoffStrategy as D;
        Self {
            max_attempts: policy.max_attempts,
            timeout: policy.timeout,
            backoff: match policy.backoff {
                BackoffStrategy::None => D::None,
                BackoffStrategy::Fixed { delay } => D::Fixed { delay },
                BackoffStrategy::Linear { base, max } => D::Linear { base, max },
                BackoffStrategy::Exponential {
                    base,
                    multiplier,
                    max,
                } => D::Exponential {
                    base,
                    multiplier,
                    max,
                },
            },
        }
    }
}
