//! Tiering policy.
//!
//! P2: `PopularityPolicy` implements the autotier EMA formula. v2.3 takes
//! the autotier defaults verbatim (5 years of production tuning):
//!
//! ```text
//! y[n] = MULTIPLIER * x / DAMPING + (1.0 - 1.0/DAMPING) * y[n-1]
//! ```
//!
//! - `x`: accesses-per-second over the period
//! - `MULTIPLIER = 3600` (converts to accesses-per-hour units)
//! - `DAMPING` ramps 50 000 → 1 000 000 over a week
//! - initial popularity = `MULTIPLIER * 0.238 ≈ 857` (D17)

use std::time::Duration;

use crate::index::TierId;

pub const MULTIPLIER: f64 = 3600.0;
pub const START_DAMPING: f64 = 50_000.0;
pub const FULL_DAMPING: f64 = 1_000_000.0;
pub const REACH_FULL_DAMPING_AFTER_SECS: f64 = 7.0 * 24.0 * 3600.0;
pub const AVG_USAGE_PER_SEC: f64 = 0.238;
pub const INITIAL_POPULARITY: f64 = MULTIPLIER * AVG_USAGE_PER_SEC;

/// Compute the dynamically-scaled DAMPING for a file `age_secs` old.
fn damping_for(age_secs: f64) -> f64 {
    if age_secs >= REACH_FULL_DAMPING_AFTER_SECS {
        FULL_DAMPING
    } else {
        let slope = (FULL_DAMPING - START_DAMPING) / REACH_FULL_DAMPING_AFTER_SECS;
        START_DAMPING + slope * age_secs
    }
}

/// One EMA step. `period_secs` = seconds since last update; `hits` = access
/// count in that window; `prev` = previous popularity.
pub fn ema_step(period_secs: f64, hits: u64, prev: f64, file_age_secs: f64) -> f64 {
    if period_secs <= 0.0 {
        return prev;
    }
    let x = hits as f64 / period_secs;
    let d = damping_for(file_age_secs);
    MULTIPLIER * x / d + (1.0 - 1.0 / d) * prev
}

pub trait TieringPolicy: Send + Sync {
    fn low_watermark(&self) -> f64;
    fn high_watermark(&self) -> f64;
    fn panic_watermark(&self) -> f64;
    fn tier_period(&self) -> Option<Duration>;
    fn min_age_to_evict(&self) -> Duration;
    fn initial_popularity(&self) -> f64;

    /// New file create: which tier to land on, given current fast-tier usage.
    fn tier_for_create(&self, fast_usage: f64) -> TierId {
        if fast_usage >= self.panic_watermark() {
            TierId::Slow
        } else {
            TierId::Fast
        }
    }
}

/// Default policy: EMA + 3 watermarks (D6, D17).
#[derive(Debug, Clone, Copy)]
pub struct PopularityPolicy {
    pub low_watermark: f64,
    pub high_watermark: f64,
    pub panic_watermark: f64,
    /// `None` means manual-only mode (D15: tier_period < 0).
    pub tier_period: Option<Duration>,
    pub min_age_to_evict: Duration,
}

impl Default for PopularityPolicy {
    fn default() -> Self {
        Self {
            low_watermark: 0.60,
            high_watermark: 0.85,
            panic_watermark: 0.95,
            tier_period: Some(Duration::from_secs(600)),
            min_age_to_evict: Duration::from_secs(300),
        }
    }
}

impl TieringPolicy for PopularityPolicy {
    fn low_watermark(&self) -> f64 {
        self.low_watermark
    }
    fn high_watermark(&self) -> f64 {
        self.high_watermark
    }
    fn panic_watermark(&self) -> f64 {
        self.panic_watermark
    }
    fn tier_period(&self) -> Option<Duration> {
        self.tier_period
    }
    fn min_age_to_evict(&self) -> Duration {
        self.min_age_to_evict
    }
    fn initial_popularity(&self) -> f64 {
        INITIAL_POPULARITY
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_popularity_is_nonzero() {
        // D17: new files must NOT start at 0, else they'd be the coldest
        // and immediately driven to slow.
        let p = INITIAL_POPULARITY;
        assert!(p > 0.0);
    }

    #[test]
    fn ema_stable_under_no_access() {
        // Zero hits over a period → popularity decays by factor (1 - 1/DAMPING).
        let prev = 1000.0;
        let next = ema_step(60.0, 0, prev, 0.0);
        assert!(next < prev);
        assert!(next > prev * 0.95); // small fractional change in 60s
    }

    #[test]
    fn ema_grows_under_access() {
        let prev = 100.0;
        let next = ema_step(60.0, 60, prev, 0.0);
        assert!(next > prev);
    }

    #[test]
    fn ema_single_access_small_change() {
        // One access during a window shouldn't make a cold file the hottest:
        // contribution is MULTIPLIER * (1/period) / DAMPING ~ tiny.
        let prev = 100.0;
        let next = ema_step(60.0, 1, prev, 0.0);
        let delta = next - prev;
        assert!(delta.abs() < 1.0, "delta = {delta}");
    }

    #[test]
    fn damping_grows_with_age() {
        let young = damping_for(60.0);
        let day = damping_for(86_400.0);
        let week = damping_for(7.0 * 86_400.0);
        assert!(young < day);
        assert!(day < week);
        assert_eq!(week, FULL_DAMPING);
    }

    #[test]
    fn panic_routes_to_slow() {
        let p = PopularityPolicy::default();
        assert_eq!(p.tier_for_create(0.5), TierId::Fast);
        assert_eq!(p.tier_for_create(0.96), TierId::Slow);
    }
}
