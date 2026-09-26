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

#[cfg(test)]
mod tests {
    //! Tests for `is_over_upload_cap`, the check that decides whether a
    //! user's total upload on a torrent has reached the configured
    //! percentage of the torrent size.

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
}
