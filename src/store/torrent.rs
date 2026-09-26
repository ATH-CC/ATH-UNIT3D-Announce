use std::net::IpAddr;
use std::ops::{Deref, DerefMut};

use futures_util::TryStreamExt;
use indexmap::IndexMap;
use serde::Serialize;
use sqlx::MySqlPool;
use sqlx::types::chrono::{DateTime, Utc};

use anyhow::{Context, Result};

use crate::model::{peer_id::PeerId, torrent_status::TorrentStatus};
use crate::store::peer::{Index, Peer, PeerStore};
use crate::store::upload_total::{UploadTotalStore, is_over_upload_cap};

pub struct TorrentStore {
    inner: IndexMap<u32, Torrent>,
}

impl TorrentStore {
    pub fn new() -> TorrentStore {
        TorrentStore {
            inner: IndexMap::new(),
        }
    }

    pub async fn from_db(db: &MySqlPool, upload_cap_threshold: u64) -> Result<TorrentStore> {
        // Load one torrent per info hash. If multiple are found, prefer
        // undeleted torrents. If multiple are still found, prefer approved
        // torrents. If multiple are still found, prefer the oldest.
        let torrents = sqlx::query_as!(
            DBImportTorrent,
            r#"
                SELECT
                    torrents.id as `id: u32`,
                    torrents.status as `status: TorrentStatus`,
                    torrents.seeders as `seeders: u32`,
                    torrents.leechers as `leechers: u32`,
                    torrents.times_completed as `times_completed: u32`,
                    100 - LEAST(torrents.free, 100) as `download_factor: u8`,
                    IF(torrents.doubleup, 200, 100) as `upload_factor: u8`,
                    torrents.deleted_at IS NOT NULL as `is_deleted: bool`,
                    CAST(torrents.size AS UNSIGNED) as `size: u64`,
                    torrents.upload_cap as `upload_cap: bool`
                FROM
                    torrents
                JOIN (
                    SELECT
                        COALESCE(
                            MIN(CASE WHEN deleted_at IS NULL AND status = 1 THEN id END),
                            MIN(CASE WHEN deleted_at IS NULL AND status != 1 THEN id END),
                            MIN(CASE WHEN deleted_at IS NOT NULL THEN id END)
                        ) AS id
                    FROM
                        torrents
                    GROUP BY
                        info_hash
                ) AS distinct_torrents
                    ON distinct_torrents.id = torrents.id
            "#
        )
        .fetch(db)
        .try_fold(TorrentStore::new(), |mut store, torrent| async move {
            store.insert(
                torrent.id,
                Torrent {
                    id: torrent.id,
                    status: torrent.status,
                    seeders: torrent.seeders,
                    leechers: torrent.leechers,
                    times_completed: torrent.times_completed,
                    download_factor: torrent.download_factor,
                    upload_factor: torrent.upload_factor,
                    is_deleted: torrent.is_deleted,
                    size: torrent.size,
                    upload_cap: torrent.upload_cap,
                    upload_totals: UploadTotalStore::new(),
                    peers: PeerStore::new(),
                },
            );

            Ok(store)
        })
        .await
        .context("Failed loading torrents.")?;

        // Load each user's total upload into torrents with upload cap
        let torrents = sqlx::query!(
            r#"
                SELECT
                    history.user_id as `user_id: u32`,
                    history.torrent_id as `torrent_id: u32`,
                    history.actual_uploaded as `actual_uploaded: u64`
                FROM
                    history
                JOIN
                    torrents ON torrents.id = history.torrent_id
                WHERE
                    torrents.upload_cap = TRUE
                    AND history.actual_uploaded > 0
            "#
        )
        .fetch(db)
        .try_fold(torrents, |mut store, history| async move {
            store.entry(history.torrent_id).and_modify(|torrent| {
                torrent
                    .upload_totals
                    .insert(history.user_id, history.actual_uploaded);
            });

            Ok(store)
        })
        .await
        .context("Failed loading upload totals.")?;

        // Load peers into each torrent
        let mut torrents = sqlx::query!(
            r#"
                SELECT
                    INET6_NTOA(peers.ip) as `ip_address: IpAddr`,
                    peers.user_id as `user_id: u32`,
                    peers.torrent_id as `torrent_id: u32`,
                    peers.port as `port: u16`,
                    peers.seeder as `is_seeder: bool`,
                    peers.active as `is_active: bool`,
                    peers.visible as `is_visible: bool`,
                    peers.connectable as `is_connectable: bool`,
                    peers.updated_at as `updated_at: DateTime<Utc>`,
                    peers.uploaded as `uploaded: u64`,
                    peers.downloaded as `downloaded: u64`,
                    peers.peer_id as `peer_id: PeerId`
                FROM
                    peers
            "#
        )
        .fetch(db)
        .try_fold(torrents, |mut store, peer| async move {
            store.entry(peer.torrent_id).and_modify(|torrent| {
                torrent.peers.insert(
                    Index {
                        user_id: peer.user_id,
                        peer_id: peer.peer_id,
                    },
                    Peer {
                        ip_address: peer
                            .ip_address
                            .expect("INET6_NTOA failed to decode peer ip."),
                        port: peer.port,
                        is_seeder: peer.is_seeder,
                        is_active: peer.is_active,
                        is_visible: peer.is_visible,
                        is_connectable: peer.is_connectable,
                        has_sent_completed: false,
                        updated_at: peer
                            .updated_at
                            .expect("Peer with a null updated_at found in database."),
                        uploaded: peer.uploaded,
                        downloaded: peer.downloaded,
                        is_upload_capped: false,
                    },
                );
            });

            Ok(store)
        })
        .await
        .context("Failed loading peers.")?;

        for torrent in torrents.values_mut() {
            if torrent.upload_cap {
                torrent.refresh_upload_caps(upload_cap_threshold);
            }
        }

        Ok(torrents)
    }
}

impl Deref for TorrentStore {
    type Target = IndexMap<u32, Torrent>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for TorrentStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Clone, Default)]
pub struct DBImportTorrent {
    pub id: u32,
    pub status: TorrentStatus,
    pub seeders: u32,
    pub leechers: u32,
    pub times_completed: u32,
    pub download_factor: u8,
    pub upload_factor: u8,
    pub is_deleted: bool,
    pub size: u64,
    pub upload_cap: bool,
}

#[derive(Clone, Default, Serialize)]
pub struct Torrent {
    pub id: u32,
    pub status: TorrentStatus,
    pub is_deleted: bool,
    pub peers: PeerStore,
    pub seeders: u32,
    pub leechers: u32,
    pub times_completed: u32,
    pub download_factor: u8,
    pub upload_factor: u8,
    /// Torrent size in bytes.
    pub size: u64,
    /// When enabled, peers of users whose total upload on this torrent
    /// reaches `upload_cap_threshold` percent of the torrent size are
    /// withheld from peer lists.
    pub upload_cap: bool,
    #[serde(skip)]
    pub upload_totals: UploadTotalStore,
}

impl Torrent {
    /// Recomputes the cached `is_upload_capped` flag on every peer. Linear in
    /// the swarm size, so only call it when loading or updating a torrent,
    /// never per announce.
    pub fn refresh_upload_caps(&mut self, threshold_percent: u64) {
        let size = self.size;

        for (index, peer) in self.peers.iter_mut() {
            let uploaded = self.upload_totals.get(&index.user_id).copied().unwrap_or(0);

            peer.is_upload_capped = is_over_upload_cap(uploaded, size, threshold_percent);
        }
    }

    /// Adds `uploaded_delta` to the user's total upload on this torrent and
    /// refreshes the cached cap flag of their peers. Returns whether the
    /// user is capped afterwards, or false if upload cap is disabled.
    pub fn record_upload(
        &mut self,
        user_id: u32,
        peer_id: PeerId,
        uploaded_delta: u64,
        threshold_percent: u64,
    ) -> bool {
        if !self.upload_cap {
            return false;
        }

        let previous_total = self.upload_totals.get(&user_id).copied().unwrap_or(0);
        let total = previous_total.saturating_add(uploaded_delta);

        if uploaded_delta > 0 {
            self.upload_totals.insert(user_id, total);
        }

        let was_capped = is_over_upload_cap(previous_total, self.size, threshold_percent);
        let is_capped = is_over_upload_cap(total, self.size, threshold_percent);
        let announcing_peer_is_stale = self
            .peers
            .get(&Index { user_id, peer_id })
            .is_some_and(|peer| peer.is_upload_capped != is_capped);

        // Crossing the threshold, or a stale flag after a threshold change,
        // updates every client of the user. Both are rare.
        if was_capped != is_capped || announcing_peer_is_stale {
            for (index, peer) in self.peers.iter_mut() {
                if index.user_id == user_id {
                    peer.is_upload_capped = is_capped;
                }
            }
        }

        is_capped
    }
}

#[cfg(test)]
mod tests {
    //! Tests for tracking each user's total upload on a torrent and keeping
    //! the cached `is_upload_capped` flag on their peers correct.
    //!
    //! All tests use a 10 GiB torrent, so a 500% threshold is reached at
    //! 50 GiB of total upload.

    use super::*;

    const GIB: u64 = 1 << 30;

    /// Builds an active seed with the given cap flag.
    fn make_peer(is_upload_capped: bool) -> Peer {
        Peer {
            ip_address: IpAddr::from([127, 0, 0, 1]),
            port: 6881,
            is_seeder: true,
            is_active: true,
            is_visible: true,
            is_connectable: true,
            has_sent_completed: false,
            updated_at: Utc::now(),
            uploaded: 0,
            downloaded: 0,
            is_upload_capped,
        }
    }

    /// Builds a peer index. `peer_byte` fills the peer id, so different
    /// bytes give different clients of the same user.
    fn index(user_id: u32, peer_byte: u8) -> Index {
        Index {
            user_id,
            peer_id: PeerId([peer_byte; 20]),
        }
    }

    /// 10 GiB torrent with upload cap enabled.
    fn upload_capped_torrent() -> Torrent {
        Torrent {
            size: 10 * GIB,
            upload_cap: true,
            ..Default::default()
        }
    }

    /// Reads the cached cap flag of a peer that must exist.
    fn is_capped(torrent: &Torrent, index: Index) -> bool {
        torrent
            .peers
            .get(&index)
            .expect("peer must exist in the swarm")
            .is_upload_capped
    }

    /// Each announce adds its delta to the stored total, the same way the
    /// history queue adds to `history.actual_uploaded`.
    #[test]
    fn record_upload_accumulates_total() {
        let mut torrent = upload_capped_torrent();
        torrent.peers.insert(index(1, 1), make_peer(false));

        torrent.record_upload(1, PeerId([1; 20]), 3 * GIB, 500);
        torrent.record_upload(1, PeerId([1; 20]), 4 * GIB, 500);

        assert_eq!(
            torrent.upload_totals.get(&1),
            Some(&(7 * GIB)),
            "the total must be the sum of both deltas"
        );
    }

    /// Torrents without upload cap must not use memory for totals or
    /// cap anyone, however much they upload.
    #[test]
    fn record_upload_does_nothing_without_upload_cap() {
        let mut torrent = upload_capped_torrent();
        torrent.upload_cap = false;
        torrent.peers.insert(index(1, 1), make_peer(false));

        let capped = torrent.record_upload(1, PeerId([1; 20]), 100 * GIB, 500);

        assert!(!capped, "the user must not be reported as capped");
        assert!(
            torrent.upload_totals.is_empty(),
            "no total must be stored for a torrent without upload cap"
        );
        assert!(
            !is_capped(&torrent, index(1, 1)),
            "the peer's cap flag must stay unset"
        );
    }

    /// Leechers announce with a zero delta all the time. Storing a 0 entry
    /// for each of them would grow the map for nothing.
    #[test]
    fn record_upload_does_not_store_zero_deltas() {
        let mut torrent = upload_capped_torrent();
        torrent.peers.insert(index(1, 1), make_peer(false));

        torrent.record_upload(1, PeerId([1; 20]), 0, 500);

        assert!(
            torrent.upload_totals.is_empty(),
            "an announce without upload must not create a total"
        );
    }

    /// 49 GiB on a 10 GiB torrent is 490%, below the 500% threshold.
    #[test]
    fn record_upload_below_threshold_is_not_capped() {
        let mut torrent = upload_capped_torrent();
        torrent.peers.insert(index(1, 1), make_peer(false));

        let capped = torrent.record_upload(1, PeerId([1; 20]), 49 * GIB, 500);

        assert!(!capped, "490% must not reach a 500% threshold");
        assert!(
            !is_capped(&torrent, index(1, 1)),
            "the peer's cap flag must stay unset below the threshold"
        );
    }

    /// When one client pushes the user over the threshold, the user's other
    /// clients on the torrent must be capped immediately, not only after
    /// their own next announce.
    #[test]
    fn record_upload_caps_all_peers_of_user_when_crossing_threshold() {
        let mut torrent = upload_capped_torrent();
        torrent.peers.insert(index(1, 1), make_peer(false));
        torrent.peers.insert(index(1, 2), make_peer(false));
        torrent.peers.insert(index(2, 3), make_peer(false));

        torrent.record_upload(1, PeerId([1; 20]), 49 * GIB, 500);
        let capped = torrent.record_upload(1, PeerId([1; 20]), GIB, 500);

        assert!(capped, "50 GiB must reach the 500% threshold");
        assert!(
            is_capped(&torrent, index(1, 1)),
            "the announcing client must be capped"
        );
        assert!(
            is_capped(&torrent, index(1, 2)),
            "the user's other client must be capped without announcing"
        );
        assert!(
            !is_capped(&torrent, index(2, 3)),
            "peers of other users must not be touched"
        );
    }

    /// A user who is already over the threshold from earlier sessions
    /// (loaded from `history`) starts a new client. That client must be
    /// capped on its very first announce, even without uploading.
    #[test]
    fn record_upload_uses_existing_total_from_history() {
        let mut torrent = upload_capped_torrent();
        torrent.upload_totals.insert(1, 60 * GIB);
        torrent.peers.insert(index(1, 1), make_peer(false));

        let capped = torrent.record_upload(1, PeerId([1; 20]), 0, 500);

        assert!(capped, "a 60 GiB total from history must reach 500%");
        assert!(
            is_capped(&torrent, index(1, 1)),
            "the new client must be capped on its first announce"
        );
    }

    /// Simulates a config reload that raised the threshold from 500% to
    /// 1000%. The user's total doesn't change, so no threshold is crossed,
    /// but the announcing peer's stale flag must still be cleared.
    #[test]
    fn record_upload_uncaps_after_threshold_is_raised() {
        let mut torrent = upload_capped_torrent();
        torrent.upload_totals.insert(1, 60 * GIB);
        torrent.peers.insert(index(1, 1), make_peer(true));

        let capped = torrent.record_upload(1, PeerId([1; 20]), 0, 1000);

        assert!(!capped, "600% must not reach a 1000% threshold");
        assert!(
            !is_capped(&torrent, index(1, 1)),
            "the stale cap flag must be cleared on announce"
        );
    }

    /// After a threshold change, the first announce of any client must
    /// correct the flag on all of the user's clients.
    #[test]
    fn record_upload_uncaps_all_peers_of_user_after_threshold_is_raised() {
        let mut torrent = upload_capped_torrent();
        torrent.upload_totals.insert(1, 60 * GIB);
        torrent.peers.insert(index(1, 1), make_peer(true));
        torrent.peers.insert(index(1, 2), make_peer(true));

        torrent.record_upload(1, PeerId([1; 20]), 0, 1000);

        assert!(!is_capped(&torrent, index(1, 2)));
    }

    /// A torrent sent by an older UNIT3D has size 0. Nobody may be capped,
    /// but totals are still tracked so they're correct once the size is
    /// sent.
    #[test]
    fn record_upload_without_size_never_caps() {
        let mut torrent = upload_capped_torrent();
        torrent.size = 0;
        torrent.peers.insert(index(1, 1), make_peer(false));

        let capped = torrent.record_upload(1, PeerId([1; 20]), 100 * GIB, 500);

        assert!(!capped, "a torrent without size must not cap anyone");
        assert_eq!(
            torrent.upload_totals.get(&1),
            Some(&(100 * GIB)),
            "the total must still be tracked without a size"
        );
    }

    /// Seedtime is accrued in `history` by the flush query, based on the
    /// announce's `left` and event, not on anything capping touches. This
    /// test guards the tracker side of that: capping must only change
    /// `is_upload_capped` and leave every field that decides whether a peer
    /// counts as an active, visible seed exactly as it was. Otherwise a
    /// capped seed could drop out of the seeder counts or be treated as
    /// inactive.
    #[test]
    fn record_upload_capping_leaves_seeding_state_untouched() {
        let mut torrent = upload_capped_torrent();
        let seed = Peer {
            uploaded: 7,
            downloaded: 3,
            ..make_peer(false)
        };
        // The announcing client and a second client of the same user, which
        // is capped through the threshold-crossing scan
        torrent.peers.insert(index(1, 1), seed);
        torrent.peers.insert(index(1, 2), seed);

        let capped = torrent.record_upload(1, PeerId([1; 20]), 60 * GIB, 500);

        assert!(capped, "60 GiB must reach the 500% threshold");

        for peer_index in [index(1, 1), index(1, 2)] {
            let peer = torrent
                .peers
                .get(&peer_index)
                .expect("capping must not remove the peer from the swarm");

            assert!(peer.is_upload_capped, "the peer must be capped");
            assert!(peer.is_seeder, "a capped seed must stay a seed");
            assert!(peer.is_active, "a capped seed must stay active");
            assert!(peer.is_visible, "a capped seed must stay visible");
            assert!(peer.is_connectable, "capping must not change connectivity");
            assert_eq!(
                peer.updated_at, seed.updated_at,
                "capping must not change the last announce time"
            );
            assert_eq!(
                (peer.uploaded, peer.downloaded),
                (seed.uploaded, seed.downloaded),
                "capping must not change the client's reported counters"
            );
        }
    }

    /// On a stopped event the peer is removed from the swarm before the
    /// upload is recorded. The final delta must still count towards the
    /// total, and the missing peer must not cause a panic.
    #[test]
    fn record_upload_for_stopped_peer_still_updates_total() {
        let mut torrent = upload_capped_torrent();

        let capped = torrent.record_upload(1, PeerId([1; 20]), 60 * GIB, 500);

        assert!(capped, "60 GiB must reach the 500% threshold");
        assert_eq!(
            torrent.upload_totals.get(&1),
            Some(&(60 * GIB)),
            "the stopped peer's upload must be added to the total"
        );
    }

    /// Runs on startup and on API upserts: every peer's flag is recomputed
    /// from the stored totals, overwriting whatever it was before.
    #[test]
    fn refresh_upload_caps_sets_flags_from_totals() {
        let mut torrent = upload_capped_torrent();
        torrent.upload_totals.insert(1, 50 * GIB);
        torrent.upload_totals.insert(2, 10 * GIB);
        torrent.peers.insert(index(1, 1), make_peer(false));
        torrent.peers.insert(index(2, 2), make_peer(true));
        torrent.peers.insert(index(3, 3), make_peer(true));

        torrent.refresh_upload_caps(500);

        assert!(
            is_capped(&torrent, index(1, 1)),
            "exactly 500% must be capped"
        );
        assert!(
            !is_capped(&torrent, index(2, 2)),
            "100% must be uncapped, even if the flag was set before"
        );
        assert!(
            !is_capped(&torrent, index(3, 3)),
            "a user without a stored total must be uncapped"
        );
    }

    /// `UPLOAD_CAP_THRESHOLD=0` disables withholding, so a refresh
    /// must clear every flag.
    #[test]
    fn refresh_upload_caps_with_zero_threshold_uncaps_everyone() {
        let mut torrent = upload_capped_torrent();
        torrent.upload_totals.insert(1, 100 * GIB);
        torrent.peers.insert(index(1, 1), make_peer(true));

        torrent.refresh_upload_caps(0);

        assert!(
            !is_capped(&torrent, index(1, 1)),
            "a threshold of 0 must clear the cap flag"
        );
    }
}
