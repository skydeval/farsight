//! A token bucket as a state machine over a clock the caller supplies.
//!
//! A [`Bucket`] holds a fractional number of tokens and the time it was
//! last refilled. It has no rate of its own: every call names the [`Rate`]
//! in force, so a rate or a burst that changes at runtime (a reloaded
//! configuration, a per-key override) applies from the next call on.
//!
//! - **Refill.** `tokens = min(tokens + elapsed × per_sec, burst)`, where
//!   `elapsed` is the time since the previous refill, and zero if the
//!   clock handed in is earlier than that. The bucket then remembers the
//!   time handed in, earlier or not.
//! - **Cap.** The burst is applied at each refill: lowering it cuts the
//!   tokens down at the next call, raising it adds nothing by itself.
//! - **Take.** One token, if at least one whole token is there
//!   ([`Bucket::try_take`]), or if at least `reserve + 1` are
//!   ([`Bucket::try_take_above`]). Fractions accumulate between takes.
//! - **Zero rate.** Nothing is ever added; what the bucket holds can still
//!   be taken.
//! - **First use.** A bucket made with [`Bucket::full`] starts with the
//!   burst and the time given. One made with [`Bucket::unused`] starts
//!   with a stated number of tokens and no time: its first refill counts
//!   a stated head start as the time elapsed.
//!
//! What a refused caller is told (a wait, a `Retry-After`) is derived by
//! the caller from [`Bucket::secs_until_token`] and
//! [`Bucket::secs_until_full`], since each rounds and bounds it its own
//! way.

use std::time::{Duration, Instant};

/// A sustained rate and the most a bucket holds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rate {
    /// Tokens added per second.
    pub per_sec: f64,
    /// Most tokens the bucket holds; applied at each refill.
    pub burst: f64,
}

impl Rate {
    /// A rate whose burst is one second of it (`burst == per_sec`).
    pub const fn one_second(per_sec: f64) -> Rate {
        Rate {
            per_sec,
            burst: per_sec,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Refilled {
    /// Last refilled at this time.
    At(Instant),
    /// Never: the first refill counts this much as elapsed.
    Never(Duration),
}

/// The bucket's state.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bucket {
    tokens: f64,
    refilled: Refilled,
}

impl Bucket {
    /// A full bucket (`rate.burst` tokens), last refilled at `now`.
    pub const fn full(rate: Rate, now: Instant) -> Bucket {
        Bucket {
            tokens: rate.burst,
            refilled: Refilled::At(now),
        }
    }

    /// A bucket holding `tokens` that has never been refilled. Its first
    /// refill adds `head_start` worth of the rate, whatever the time.
    pub const fn unused(tokens: f64, head_start: Duration) -> Bucket {
        Bucket {
            tokens,
            refilled: Refilled::Never(head_start),
        }
    }

    /// The tokens held as of the last refill.
    pub const fn tokens(&self) -> f64 {
        self.tokens
    }

    /// Adds what `rate` yields for the time since the last refill, up to
    /// the burst, and remembers `now`.
    pub fn refill(&mut self, now: Instant, rate: Rate) {
        let elapsed = match self.refilled {
            Refilled::At(last) => now.saturating_duration_since(last),
            Refilled::Never(head_start) => head_start,
        };
        self.tokens = (self.tokens + elapsed.as_secs_f64() * rate.per_sec).min(rate.burst);
        self.refilled = Refilled::At(now);
    }

    /// Takes one token if a whole one is there. Does not refill.
    pub fn try_take(&mut self) -> bool {
        self.try_take_above(0.0)
    }

    /// Takes one token if that leaves at least `reserve`. Does not refill.
    pub fn try_take_above(&mut self, reserve: f64) -> bool {
        if self.tokens >= reserve + 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Refills, then takes one token if a whole one is there.
    pub fn take(&mut self, now: Instant, rate: Rate) -> bool {
        self.refill(now, rate);
        self.try_take()
    }

    /// Refills, then takes one token if that leaves at least `reserve`.
    pub fn take_above(&mut self, now: Instant, rate: Rate, reserve: f64) -> bool {
        self.refill(now, rate);
        self.try_take_above(reserve)
    }

    /// Seconds of `rate` until the bucket holds one whole token:
    /// `(1 − tokens) / per_sec`. Not positive when it already does;
    /// infinite or NaN at a zero rate.
    pub fn secs_until_token(&self, rate: Rate) -> f64 {
        (1.0 - self.tokens) / rate.per_sec
    }

    /// Seconds of `rate` until the bucket is full:
    /// `(burst − tokens) / per_sec`. Infinite or NaN at a zero rate.
    pub fn secs_until_full(&self, rate: Rate) -> f64 {
        (rate.burst - self.tokens) / rate.per_sec
    }

    /// Time since the last refill as of `now`; zero for a bucket never
    /// refilled or refilled after `now`.
    pub fn idle(&self, now: Instant) -> Duration {
        match self.refilled {
            Refilled::At(last) => now.saturating_duration_since(last),
            Refilled::Never(_) => Duration::ZERO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn burst_then_refuse_then_refill() {
        let rate = Rate {
            per_sec: 10.0,
            burst: 50.0,
        };
        let t0 = Instant::now();
        let mut b = Bucket::full(rate, t0);
        for i in 0..50 {
            assert!(b.take(t0, rate));
            assert_eq!(b.tokens(), f64::from(49 - i));
        }
        assert!(!b.take(t0, rate));
        assert_eq!(b.secs_until_token(rate), 0.1);
        assert_eq!(b.secs_until_full(rate), 5.0);
        // 0.1 s later one token is back.
        assert!(b.take(t0 + ms(100), rate));
        assert!(!b.take(t0 + ms(100), rate));
    }

    #[test]
    fn a_taker_with_a_reserve_leaves_it() {
        let rate = Rate {
            per_sec: 2.0,
            burst: 10.0,
        };
        let t0 = Instant::now();
        let mut b = Bucket::full(rate, t0);
        for _ in 0..5 {
            assert!(b.take_above(t0, rate, 5.0));
        }
        assert!(!b.take_above(t0, rate, 5.0));
        for _ in 0..5 {
            assert!(b.take(t0, rate));
        }
        assert!(!b.take(t0, rate));
        // Refilled to 6 after three seconds: one more above the reserve.
        let t1 = t0 + Duration::from_secs(3);
        assert!(b.take_above(t1, rate, 5.0));
        assert!(!b.take_above(t1, rate, 5.0));
    }

    #[test]
    fn fractions_accumulate_between_takes() {
        // 0.5 per second with room for one: a token every two seconds.
        let rate = Rate {
            per_sec: 0.5,
            burst: 1.0,
        };
        let t0 = Instant::now();
        let mut b = Bucket::full(rate, t0);
        assert!(b.take(t0, rate));
        assert!(!b.take(t0 + ms(1000), rate));
        assert_eq!(b.tokens(), 0.5);
        assert_eq!(b.secs_until_token(rate), 1.0);
        assert!(!b.take(t0 + ms(1500), rate));
        assert!(b.take(t0 + ms(2000), rate));
        assert_eq!(b.tokens(), 0.0);
    }

    #[test]
    fn a_zero_rate_never_refills() {
        let rate = Rate {
            per_sec: 0.0,
            burst: 2.0,
        };
        let t0 = Instant::now();
        let mut b = Bucket::full(rate, t0);
        assert!(b.take(t0, rate));
        assert!(b.take(t0, rate));
        assert!(!b.take(t0 + Duration::from_secs(86_400), rate));
        assert_eq!(b.tokens(), 0.0);
        assert_eq!(b.secs_until_token(rate), f64::INFINITY);
        // Full at a zero rate: 0 / 0.
        let full = Bucket::full(rate, t0);
        assert!(full.secs_until_full(rate).is_nan());
    }

    #[test]
    fn a_changed_rate_applies_from_the_next_call() {
        let slow = Rate {
            per_sec: 1.0,
            burst: 4.0,
        };
        let fast = Rate {
            per_sec: 10.0,
            burst: 4.0,
        };
        let t0 = Instant::now();
        let mut b = Bucket::full(slow, t0);
        for _ in 0..4 {
            assert!(b.take(t0, slow));
        }
        // The whole interval since the last refill is paid at the rate in
        // force at the call.
        b.refill(t0 + ms(200), fast);
        assert_eq!(b.tokens(), 2.0);
        b.refill(t0 + ms(1200), slow);
        assert_eq!(b.tokens(), 3.0);
    }

    #[test]
    fn a_lowered_burst_cuts_and_a_raised_one_adds_nothing() {
        let t0 = Instant::now();
        let big = Rate {
            per_sec: 1.0,
            burst: 20.0,
        };
        let small = Rate {
            per_sec: 1.0,
            burst: 3.0,
        };
        let mut b = Bucket::full(big, t0);
        b.refill(t0, small);
        assert_eq!(b.tokens(), 3.0);
        b.refill(t0, big);
        assert_eq!(b.tokens(), 3.0);
        b.refill(t0 + Duration::from_secs(2), big);
        assert_eq!(b.tokens(), 5.0);
    }

    #[test]
    fn a_clock_that_goes_back_adds_nothing_and_is_remembered() {
        let rate = Rate {
            per_sec: 1.0,
            burst: 10.0,
        };
        let t0 = Instant::now();
        let t5 = t0 + Duration::from_secs(5);
        let mut b = Bucket::full(rate, t5);
        for _ in 0..10 {
            assert!(b.take(t5, rate));
        }
        assert!(!b.take(t0, rate));
        assert_eq!(b.tokens(), 0.0);
        assert_eq!(b.idle(t0), Duration::ZERO);
        // The earlier time is the new reference.
        assert_eq!(b.idle(t5), Duration::from_secs(5));
        b.refill(t5, rate);
        assert_eq!(b.tokens(), 5.0);
    }

    #[test]
    fn an_unused_bucket_gets_its_head_start_once() {
        let t0 = Instant::now();
        let head = Duration::from_secs(1);
        // Below one token per second the head start is not a whole token.
        let slow = Rate {
            per_sec: 0.25,
            burst: 1.0,
        };
        let mut b = Bucket::unused(0.0, head);
        assert_eq!(b.idle(t0), Duration::ZERO);
        assert!(!b.take(t0, slow));
        assert_eq!(b.tokens(), 0.25);
        assert!(!b.take(t0, slow));
        assert_eq!(b.tokens(), 0.25);
        assert!(b.take(t0 + Duration::from_secs(3), slow));
        // At or above it the first take succeeds.
        let fast = Rate {
            per_sec: 3.0,
            burst: 3.0,
        };
        let mut b = Bucket::unused(0.0, head);
        assert!(b.take(t0, fast));
        assert_eq!(b.tokens(), 2.0);
    }

    #[test]
    fn one_second_rate() {
        assert_eq!(
            Rate::one_second(7.0),
            Rate {
                per_sec: 7.0,
                burst: 7.0
            }
        );
    }

    #[derive(Debug, Clone, Copy)]
    enum Op {
        Take,
        TakeAbove(f64),
        Refill,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => Just(Op::Take),
            1 => (0.0f64..8.0).prop_map(Op::TakeAbove),
            1 => Just(Op::Refill),
        ]
    }

    fn rate() -> impl Strategy<Value = Rate> {
        (
            prop_oneof![Just(0.0f64), 0.001f64..200.0],
            prop_oneof![Just(1.0f64), 1.0f64..100.0],
        )
            .prop_map(|(per_sec, burst)| Rate { per_sec, burst })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Over any sequence of calls at times that do not go back, the
        /// tokens granted never exceed `burst + rate × elapsed`, the bucket
        /// never goes negative or above its burst, and a reference model
        /// kept in whole milliseconds agrees on every decision that is not
        /// within rounding of the threshold.
        #[test]
        fn never_grants_more_than_burst_plus_rate_times_elapsed(
            rate in rate(),
            steps in prop::collection::vec((0u64..3_000, op()), 0..120),
        ) {
            let t0 = Instant::now();
            let mut b = Bucket::full(rate, t0);
            let mut model = rate.burst;
            let (mut at, mut granted) = (0u64, 0u32);
            for (dt, op) in steps {
                at += dt;
                let now = t0 + ms(at);
                model = (model + dt as f64 / 1000.0 * rate.per_sec).min(rate.burst);
                let reserve = match op {
                    Op::Take => Some(0.0),
                    Op::TakeAbove(r) => Some(r),
                    Op::Refill => None,
                };
                match reserve {
                    None => b.refill(now, rate),
                    Some(r) => {
                        let took = b.take_above(now, rate, r);
                        let margin = model - (r + 1.0);
                        if margin.abs() > 1e-6 {
                            prop_assert_eq!(took, margin > 0.0);
                        }
                        if took {
                            granted += 1;
                            model -= 1.0;
                        }
                    }
                }
                prop_assert!((b.tokens() - model).abs() < 1e-6);
                prop_assert!(b.tokens() >= 0.0 && b.tokens() <= rate.burst);
                let bound = rate.burst + rate.per_sec * (at as f64 / 1000.0);
                prop_assert!(f64::from(granted) <= bound + 1e-6);
            }
        }

        /// From one state, a later refill never leaves fewer tokens than an
        /// earlier one, and a take that succeeds early succeeds later.
        #[test]
        fn more_time_never_means_fewer_tokens(
            rate in rate(),
            spent in 0u32..100,
            early in 0u64..100_000,
            later in 0u64..100_000,
        ) {
            let t0 = Instant::now();
            let mut b = Bucket::full(rate, t0);
            for _ in 0..spent {
                b.try_take();
            }
            let (mut x, mut y) = (b, b);
            x.refill(t0 + ms(early), rate);
            y.refill(t0 + ms(early + later), rate);
            prop_assert!(y.tokens() >= x.tokens());
            prop_assert!(!x.try_take() || y.try_take());
        }

        /// Any order of times, any rate and burst, an unused bucket or a
        /// full one: no call panics, and the tokens stay within
        /// `[0, burst]` once refilled.
        #[test]
        fn total_over_arbitrary_clocks_and_rates(
            unused in any::<bool>(),
            head_ms in 0u64..10_000,
            steps in prop::collection::vec((0u64..100_000, rate(), any::<bool>()), 1..60),
        ) {
            let t0 = Instant::now();
            let mut b = if unused {
                Bucket::unused(0.0, ms(head_ms))
            } else {
                Bucket::full(steps[0].1, t0)
            };
            for (at, rate, take) in steps {
                let now = t0 + ms(at);
                let before = b.tokens();
                if take {
                    b.take(now, rate);
                } else {
                    b.refill(now, rate);
                }
                prop_assert!(b.tokens() >= 0.0 && b.tokens() <= rate.burst);
                // A zero rate adds nothing.
                if rate.per_sec == 0.0 {
                    prop_assert!(b.tokens() <= before);
                }
                prop_assert_eq!(b.idle(now), Duration::ZERO);
                let _ = (b.secs_until_token(rate), b.secs_until_full(rate));
            }
        }
    }
}
