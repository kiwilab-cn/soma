//! Per-bucket request rate limiting.
//!
//! Rate limits live in each bucket's [`soma_meta::BucketMeta::rate_limit`] (set via
//! the admin API, default off). The S3 layer already loads the bucket's metadata on
//! the request path, so it passes the [`RateLimit`] in; this type only holds the
//! live token-bucket state, keyed by bucket name. Storage quotas are enforced in the
//! metadata transaction (see [`soma_meta`]), not here.
//!
//! Buckets without a configured rate (`rps == 0`) are unlimited, so rate limiting is
//! fully opt-in and adds nothing to the hot path when unconfigured.

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;
use soma_meta::RateLimit;

/// Live token-bucket state for one bucket.
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Per-bucket token-bucket rate limiter. Empty by default (no state until a
/// rate-limited bucket is first hit).
#[derive(Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    /// A fresh, empty limiter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Consume one request token for `bucket`, given its configured `limit`.
    /// Returns `true` if allowed, `false` if the bucket is currently over its rate.
    /// Buckets with no configured rate (`rps <= 0`) are always allowed and keep no
    /// state.
    pub fn allow(&self, bucket: &str, limit: RateLimit) -> bool {
        if limit.rps <= 0.0 {
            return true;
        }
        let burst = if limit.burst > 0.0 {
            limit.burst
        } else {
            limit.rps
        };
        let now = Instant::now();
        let mut buckets = self.buckets.lock();
        let b = buckets.entry(bucket.to_string()).or_insert(Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.duration_since(b.last).as_secs_f64();
        b.last = now;
        b.tokens = (b.tokens + elapsed * limit.rps).min(burst);
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn limit(rps: f64, burst: f64) -> RateLimit {
        RateLimit { rps, burst }
    }

    #[test]
    fn burst_then_throttle() {
        let rl = RateLimiter::new();
        let l = limit(1.0, 3.0); // 3 burst, 1/s refill
                                 // The first three requests drain the burst.
        assert!(rl.allow("b", l));
        assert!(rl.allow("b", l));
        assert!(rl.allow("b", l));
        // The fourth (immediately) is throttled.
        assert!(!rl.allow("b", l));
    }

    #[test]
    fn zero_rps_is_unlimited() {
        let rl = RateLimiter::new();
        let l = limit(0.0, 0.0);
        for _ in 0..100 {
            assert!(rl.allow("b", l));
        }
    }

    #[test]
    fn buckets_are_independent() {
        let rl = RateLimiter::new();
        let l = limit(1.0, 1.0);
        assert!(rl.allow("a", l));
        assert!(!rl.allow("a", l)); // a is now drained
        assert!(rl.allow("b", l)); // b has its own bucket
    }

    #[test]
    fn missing_burst_defaults_to_rps() {
        let rl = RateLimiter::new();
        let l = limit(2.0, 0.0); // burst unset → defaults to rps (2)
        assert!(rl.allow("b", l));
        assert!(rl.allow("b", l));
        assert!(!rl.allow("b", l));
    }
}
