use std::ops::{Deref, DerefMut};

use futures_util::TryStreamExt;
use indexmap::IndexMap;
use sqlx::MySqlPool;

use anyhow::{Context, Result};

/// Each user's total actual upload on a single torrent, keyed by user id.
/// Mirrors `history.actual_uploaded`. Only filled for torrents with
/// `upload_cap` enabled.
#[derive(Clone, Default)]
pub struct UploadTotalStore {
    inner: IndexMap<u32, u64>,
}

impl UploadTotalStore {
    pub fn new() -> UploadTotalStore {
        UploadTotalStore {
            inner: IndexMap::new(),
        }
    }

    /// Loads every user's total actual upload on one torrent.
    pub async fn from_db(db: &MySqlPool, torrent_id: u32) -> Result<UploadTotalStore> {
        sqlx::query!(
            r#"
                SELECT
                    history.user_id as `user_id: u32`,
                    history.actual_uploaded as `actual_uploaded: u64`
                FROM
                    history
                WHERE
                    history.torrent_id = ?
                    AND history.actual_uploaded > 0
            "#,
            torrent_id
        )
        .fetch(db)
        .try_fold(UploadTotalStore::new(), |mut store, history| async move {
            store.insert(history.user_id, history.actual_uploaded);

            Ok(store)
        })
        .await
        .context("Failed loading upload totals.")
    }
}

impl Deref for UploadTotalStore {
    type Target = IndexMap<u32, u64>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for UploadTotalStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Whether a user who uploaded `uploaded` bytes on a torrent of `size` bytes
/// has reached `threshold_percent` of the torrent size. A threshold or size of
/// 0 never caps.
///
/// Integer math only: widened to u128 so the multiplication can't overflow.
#[inline]
pub fn is_over_upload_cap(uploaded: u64, size: u64, threshold_percent: u64) -> bool {
    threshold_percent != 0
        && size != 0
        && uploaded as u128 * 100 >= size as u128 * threshold_percent as u128
}

/// The smallest total upload, in bytes, at which [`is_over_upload_cap`]
/// reports a user as capped on a torrent of `size` bytes. `None` when capping
/// is disabled, i.e. when `threshold_percent` or `size` is 0.
///
/// This is `size * threshold_percent / 100` rounded up, so that for every
/// `uploaded`:
///
/// ```text
/// is_over_upload_cap(uploaded, size, threshold_percent) == (uploaded >= cap)
/// ```
///
/// A cap that doesn't fit in a `u64` saturates to `u64::MAX`. No `u64` total
/// can exceed it, so nothing is lost by the saturation.
#[inline]
pub fn upload_cap_bytes(size: u64, threshold_percent: u64) -> Option<u64> {
    if threshold_percent == 0 || size == 0 {
        return None;
    }

    let cap = (size as u128 * threshold_percent as u128).div_ceil(100);

    Some(u64::try_from(cap).unwrap_or(u64::MAX))
}

/// How much of an announce's `uploaded_delta` may still be credited to a user
/// who had uploaded `previous_total` bytes on the torrent before it.
///
/// Upload counts towards the user's credited upload only while their total
/// is below the cap. The announce that crosses the cap is split: the part
/// below the cap is credited, the rest is not. Once the cap is reached,
/// nothing more is credited. The full `uploaded_delta` is still recorded as
/// actual upload by the caller; this only limits what is credited.
///
/// Both values are raw bytes, before upload factors. The caller applies the
/// factors to the result, so double upload doubles the remaining allowance
/// instead of letting the user past the cap.
///
/// With capping disabled (see [`upload_cap_bytes`]) the full delta is
/// creditable.
#[inline]
pub fn creditable_upload(
    previous_total: u64,
    uploaded_delta: u64,
    size: u64,
    threshold_percent: u64,
) -> u64 {
    match upload_cap_bytes(size, threshold_percent) {
        Some(cap) => uploaded_delta.min(cap.saturating_sub(previous_total)),
        None => uploaded_delta,
    }
}

#[cfg(test)]
mod tests {
    //! Tests for `is_over_upload_cap`, the check that decides whether a
    //! user's total upload on a torrent has reached the configured
    //! percentage of the torrent size, and for `upload_cap_bytes` and
    //! `creditable_upload`, which decide how much upload is still credited.

    use super::*;

    /// 499 bytes on a 100 byte torrent is 499%, one byte short of a 500%
    /// threshold, so the user must not be capped yet.
    #[test]
    fn upload_cap_below_threshold() {
        assert!(
            !is_over_upload_cap(499, 100, 500),
            "499% of the torrent size must not reach a 500% threshold"
        );
    }

    /// Reaching the threshold exactly counts as capped (`>=`, not `>`).
    #[test]
    fn upload_cap_at_threshold() {
        assert!(
            is_over_upload_cap(500, 100, 500),
            "exactly 500% of the torrent size must reach a 500% threshold"
        );
    }

    /// Anything past the threshold is capped.
    #[test]
    fn upload_cap_above_threshold() {
        assert!(
            is_over_upload_cap(501, 100, 500),
            "501% of the torrent size must exceed a 500% threshold"
        );
    }

    /// Thresholds below 100% work too: with 50%, a user is capped after
    /// uploading half the torrent size.
    #[test]
    fn upload_cap_threshold_below_100_percent() {
        assert!(
            !is_over_upload_cap(49, 100, 50),
            "49% of the torrent size must not reach a 50% threshold"
        );
        assert!(
            is_over_upload_cap(50, 100, 50),
            "50% of the torrent size must reach a 50% threshold"
        );
    }

    /// `UPLOAD_CAP_THRESHOLD=0` disables withholding, so nobody is
    /// capped no matter how much they uploaded.
    #[test]
    fn upload_cap_zero_threshold_never_caps() {
        assert!(
            !is_over_upload_cap(u64::MAX, 100, 0),
            "a threshold of 0 must disable capping"
        );
    }

    /// A torrent without a known size (older UNIT3D not sending `size`)
    /// must never cap anyone, otherwise every uploader would be withheld.
    #[test]
    fn upload_cap_zero_size_never_caps() {
        assert!(
            !is_over_upload_cap(u64::MAX, 0, 500),
            "a torrent size of 0 must disable capping"
        );
    }

    /// A user who uploaded nothing is never capped, even with the lowest
    /// possible threshold of 1%.
    #[test]
    fn upload_cap_nothing_uploaded() {
        assert!(
            !is_over_upload_cap(0, 100, 1),
            "a user without upload must never be capped"
        );
    }

    /// Both sides of the comparison multiply two u64 values. They are
    /// widened to u128 first, so the maximum inputs must give the correct
    /// answer instead of overflowing (which panics in debug builds and
    /// wraps in release builds).
    #[test]
    fn upload_cap_does_not_overflow() {
        // u64::MAX * 100 == u64::MAX * 100: exactly at the threshold
        assert!(
            is_over_upload_cap(u64::MAX, u64::MAX, 100),
            "uploading the full size of a u64::MAX torrent must reach a 100% threshold"
        );
        // u64::MAX * 100 < u64::MAX * 101: just below the threshold
        assert!(
            !is_over_upload_cap(u64::MAX, u64::MAX, 101),
            "uploading the full size of a u64::MAX torrent must not reach a 101% threshold"
        );
        // u64::MAX * 100 >= 1 * u64::MAX: uploading u64::MAX bytes on a
        // 1 byte torrent is far more than u64::MAX percent
        assert!(
            is_over_upload_cap(u64::MAX, 1, u64::MAX),
            "u64::MAX bytes on a 1 byte torrent must reach a u64::MAX% threshold"
        );
        // u64::MAX * 100 < u64::MAX * u64::MAX: the largest possible
        // right-hand side still fits in a u128 and is not reached
        assert!(
            !is_over_upload_cap(u64::MAX, u64::MAX, u64::MAX),
            "u64::MAX bytes on a u64::MAX torrent must not reach a u64::MAX% threshold"
        );
    }

    /// The cap in bytes is the threshold percentage of the size.
    #[test]
    fn upload_cap_bytes_is_threshold_share_of_size() {
        assert_eq!(upload_cap_bytes(1 << 30, 100), Some(1 << 30));
        assert_eq!(upload_cap_bytes(1 << 30, 500), Some(5 << 30));
        assert_eq!(upload_cap_bytes(200, 50), Some(100));
    }

    /// A cap that falls between two bytes rounds up: 3 bytes at 50% is 1.5
    /// bytes, and 1 byte doesn't reach it yet.
    #[test]
    fn upload_cap_bytes_rounds_up() {
        assert_eq!(upload_cap_bytes(3, 50), Some(2));
        assert!(!is_over_upload_cap(1, 3, 50));
        assert!(is_over_upload_cap(2, 3, 50));
    }

    /// Mirrors `is_over_upload_cap`: a threshold or size of 0 disables
    /// capping.
    #[test]
    fn upload_cap_bytes_disabled() {
        assert_eq!(upload_cap_bytes(100, 0), None);
        assert_eq!(upload_cap_bytes(0, 100), None);
    }

    /// A cap above `u64::MAX` saturates instead of wrapping to a small value,
    /// which would cap everyone almost immediately.
    #[test]
    fn upload_cap_bytes_saturates() {
        assert_eq!(upload_cap_bytes(u64::MAX, 200), Some(u64::MAX));
        assert_eq!(upload_cap_bytes(u64::MAX, u64::MAX), Some(u64::MAX));
    }

    /// Below the cap, the whole delta is credited.
    #[test]
    fn creditable_upload_below_cap() {
        assert_eq!(creditable_upload(0, 40, 100, 100), 40);
        assert_eq!(creditable_upload(50, 49, 100, 100), 49);
    }

    /// The announce that crosses the cap is credited only up to the cap.
    #[test]
    fn creditable_upload_crossing_cap_is_split() {
        assert_eq!(
            creditable_upload(90, 30, 100, 100),
            10,
            "only the 10 bytes below the cap may be credited"
        );
        assert_eq!(
            creditable_upload(0, 150, 100, 100),
            100,
            "a single announce past the cap is credited up to the cap"
        );
    }

    /// Reaching the cap exactly credits the last byte below it; the next
    /// byte isn't credited.
    #[test]
    fn creditable_upload_at_cap_boundary() {
        assert_eq!(creditable_upload(99, 1, 100, 100), 1);
        assert_eq!(creditable_upload(100, 1, 100, 100), 0);
    }

    /// Once the cap is reached, nothing more is credited, however much is
    /// uploaded.
    #[test]
    fn creditable_upload_after_cap_is_zero() {
        assert_eq!(creditable_upload(100, 1, 100, 100), 0);
        assert_eq!(creditable_upload(500, u64::MAX, 100, 100), 0);
    }

    /// With capping disabled, everything is credited.
    #[test]
    fn creditable_upload_without_cap_credits_everything() {
        assert_eq!(creditable_upload(1_000, 1_000, 100, 0), 1_000);
        assert_eq!(creditable_upload(1_000, 1_000, 0, 100), 1_000);
    }

    /// `creditable_upload` and `is_over_upload_cap` must agree on where the
    /// cap is, otherwise a user could be withheld from peer lists while
    /// still being credited, or the other way around. Checked over a grid of
    /// small values, including thresholds that don't divide the size evenly.
    #[test]
    fn creditable_upload_agrees_with_is_over_upload_cap() {
        for size in 1..=7 {
            for threshold_percent in [1, 33, 50, 99, 100, 101, 150, 250] {
                for previous_total in 0..=20 {
                    for uploaded_delta in 0..=20 {
                        let creditable = creditable_upload(
                            previous_total,
                            uploaded_delta,
                            size,
                            threshold_percent,
                        );
                        let credited_total = previous_total + creditable;

                        assert!(creditable <= uploaded_delta);

                        // Every credited byte was uploaded while still under
                        // the cap.
                        if creditable > 0 {
                            assert!(
                                !is_over_upload_cap(credited_total - 1, size, threshold_percent),
                                "credited a byte past the cap: size {size}, threshold \
                                 {threshold_percent}, previous {previous_total}, delta \
                                 {uploaded_delta}"
                            );
                        }

                        // Anything left uncredited is only left out because
                        // the cap was reached.
                        if creditable < uploaded_delta {
                            assert!(
                                is_over_upload_cap(credited_total, size, threshold_percent),
                                "withheld credit below the cap: size {size}, threshold \
                                 {threshold_percent}, previous {previous_total}, delta \
                                 {uploaded_delta}"
                            );
                        }
                    }
                }
            }
        }
    }
}
