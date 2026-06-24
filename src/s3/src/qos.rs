//! Multi-tenant QoS (M4c): per-tenant quotas and request rate limiting.
//!
//! A **tenant** is identified by its access key. [`QosPolicy`] holds each tenant's
//! configured limits and the live token-bucket state for rate limiting. Quotas
//! (bytes / object count) are *enforced* in the metadata transaction — this type
//! only supplies the per-tenant [`Quota`] to attach to a write. Rate limiting is
//! gateway-local: a token bucket per tenant, refilled at `rps` up to `burst`.
//!
//! Tenants without configured limits are unlimited (the default), so QoS is fully
//! opt-in and adds nothing to the hot path when unconfigured.

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;
use soma_meta::Quota;

/// A single tenant's limits.
#[derive(Debug, Clone, Copy)]
pub struct TenantPolicy {
    /// Max live bytes (0 = unlimited).
    pub max_bytes: u64,
    /// Max live object count (0 = unlimited).
    pub max_objects: u64,
    /// Sustained request rate per second (0 = no rate limit).
    pub rps: f64,
    /// Token-bucket burst capacity (requests).
    pub burst: f64,
}

/// Live token-bucket state for one tenant.
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Per-tenant quota lookup + rate limiting. Empty by default (no limits).
#[derive(Default)]
pub struct QosPolicy {
    tenants: HashMap<String, TenantPolicy>,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl QosPolicy {
    /// Build from a map of access key → limits.
    pub fn new(tenants: HashMap<String, TenantPolicy>) -> Self {
        Self {
            tenants,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Whether any tenant has limits configured (lets callers skip QoS work).
    pub fn is_empty(&self) -> bool {
        self.tenants.is_empty()
    }

    /// The quota to attach to a write for `tenant` (zeros = unlimited).
    pub fn quota(&self, tenant: &str) -> Quota {
        self.tenants
            .get(tenant)
            .map(|p| Quota {
                max_bytes: p.max_bytes,
                max_objects: p.max_objects,
            })
            .unwrap_or_default()
    }

    /// Consume one request token for `tenant`. Returns `true` if allowed, `false`
    /// if the tenant is currently over its rate limit. Tenants with no configured
    /// rate (`rps == 0`) are always allowed.
    pub fn allow_request(&self, tenant: &str) -> bool {
        let policy = match self.tenants.get(tenant) {
            Some(p) if p.rps > 0.0 => *p,
            _ => return true,
        };
        let now = Instant::now();
        let mut buckets = self.buckets.lock();
        let bucket = buckets.entry(tenant.to_string()).or_insert(Bucket {
            tokens: policy.burst,
            last: now,
        });
        let elapsed = now.duration_since(bucket.last).as_secs_f64();
        bucket.last = now;
        bucket.tokens = (bucket.tokens + elapsed * policy.rps).min(policy.burst);
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
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

    fn policy(rps: f64, burst: f64) -> QosPolicy {
        let mut m = HashMap::new();
        m.insert(
            "t".to_string(),
            TenantPolicy {
                max_bytes: 1000,
                max_objects: 5,
                rps,
                burst,
            },
        );
        QosPolicy::new(m)
    }

    #[test]
    fn quota_lookup() {
        let q = policy(0.0, 0.0);
        assert_eq!(
            q.quota("t"),
            Quota {
                max_bytes: 1000,
                max_objects: 5
            }
        );
        // Unknown tenant → unlimited.
        assert_eq!(q.quota("other"), Quota::default());
    }

    #[test]
    fn burst_then_throttle() {
        let q = policy(1.0, 3.0); // 3 burst, 1/s refill
                                  // The first three requests drain the burst.
        assert!(q.allow_request("t"));
        assert!(q.allow_request("t"));
        assert!(q.allow_request("t"));
        // The fourth (immediately) is throttled.
        assert!(!q.allow_request("t"));
    }

    #[test]
    fn unconfigured_tenant_is_unlimited() {
        let q = policy(1.0, 1.0);
        for _ in 0..100 {
            assert!(q.allow_request("nobody")); // no policy → always allowed
        }
    }

    #[test]
    fn zero_rps_is_unlimited() {
        let q = policy(0.0, 0.0); // quota set, but no rate limit
        for _ in 0..100 {
            assert!(q.allow_request("t"));
        }
    }
}
