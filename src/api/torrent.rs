use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use serde::Deserialize;
use tracing::{error, info};

use anyhow::Result;

use crate::model::{info_hash::InfoHash, torrent_status::TorrentStatus};
use crate::state::AppState;
use crate::store::torrent::Torrent;
use crate::store::upload_total::UploadTotalStore;

#[derive(Clone, Deserialize)]
pub struct APIInsertTorrent {
    pub id: u32,
    pub status: TorrentStatus,
    pub info_hash: String,
    pub is_deleted: bool,
    pub seeders: u32,
    pub leechers: u32,
    pub times_completed: u32,
    pub download_factor: u8,
    pub upload_factor: u8,
    // Defaults keep older UNIT3D versions that don't send these working.
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub upload_cap: bool,
}

/// Whether upload totals must be loaded from `history`: the upload cap is on
/// now and wasn't on the torrent already in the store.
fn needs_upload_totals_load(upload_cap: bool, old_torrent: Option<&Torrent>) -> bool {
    upload_cap && !old_torrent.is_some_and(|old| old.upload_cap)
}

/// Freshly loaded totals when the upload cap was just switched on, the
/// in-memory ones while it stays on, and none when it's off.
fn select_upload_totals(
    upload_cap: bool,
    old_totals: UploadTotalStore,
    loaded_totals: Option<UploadTotalStore>,
) -> UploadTotalStore {
    if upload_cap {
        loaded_totals.unwrap_or(old_totals)
    } else {
        UploadTotalStore::new()
    }
}

pub async fn upsert(
    State(state): State<Arc<AppState>>,
    Json(torrent): Json<APIInsertTorrent>,
) -> StatusCode {
    if let Ok(info_hash) = InfoHash::from_str(&torrent.info_hash) {
        info!("Inserting torrent with id {}.", torrent.id);

        // Upload totals are only kept in memory while upload cap is
        // enabled, so they don't exist yet when it gets switched on and have
        // to be loaded from `history`.
        //
        // When the torrent already had upload cap, its in-memory totals
        // are kept instead of reloading them. They include every announce up
        // to now, while `history` lags behind by the updates still queued
        // for the next flush.
        //
        // The lock must not be held across the `.await` below: its guard can't be sent between
        // threads, and holding it would block every announce on every
        // torrent for as long as the query takes.
        let upload_cap_just_enabled = needs_upload_totals_load(
            torrent.upload_cap,
            state.stores.torrents.lock().get(&torrent.id),
        );

        // Loaded into a local first, while the old copy of the torrent is
        // still in the store and keeps serving announces. The swap below
        // happens under a single lock guard, so announces never see the
        // torrent missing.
        //
        // The loaded totals miss upload that is still queued for `history`,
        // up to one flush interval of it, including announces that happen
        // during the query. The old copy doesn't count them either, since it
        // has no upload cap yet. The upload does reach `history` on the
        // next flush, so the gap closes at the next restart or the next time
        // the flag is enabled.
        let loaded_upload_totals = if upload_cap_just_enabled {
            match UploadTotalStore::from_db(&state.pool, torrent.id).await {
                Ok(upload_totals) => Some(upload_totals),
                Err(e) => {
                    error!(
                        "Failed loading upload totals for torrent {}: {e}",
                        torrent.id
                    );

                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
        } else {
            None
        };

        let threshold_percent = state.config.load().upload_cap_threshold;

        {
            // Removed, rebuilt and reinserted under one guard, so no announce
            // sees the torrent missing from the store.
            let mut torrents = state.stores.torrents.lock();
            let old_torrent = torrents.swap_remove(&torrent.id).unwrap_or_default();

            let upload_totals = select_upload_totals(
                torrent.upload_cap,
                old_torrent.upload_totals,
                loaded_upload_totals,
            );

            let mut new_torrent = Torrent {
                id: torrent.id,
                status: torrent.status,
                is_deleted: torrent.is_deleted,
                seeders: torrent.seeders,
                leechers: torrent.leechers,
                times_completed: torrent.times_completed,
                download_factor: torrent.download_factor,
                upload_factor: torrent.upload_factor,
                size: torrent.size,
                upload_cap: torrent.upload_cap,
                upload_totals,
                peers: old_torrent.peers,
            };

            // Cached flags may be stale after loading totals or a size or
            // threshold change. They are ignored while the upload cap is off.
            if new_torrent.upload_cap {
                new_torrent.refresh_upload_caps(threshold_percent);
            }

            torrents.insert(torrent.id, new_torrent);
        }

        state
            .stores
            .infohash2id
            .write()
            .insert(info_hash, torrent.id);

        return StatusCode::OK;
    }

    StatusCode::BAD_REQUEST
}

#[derive(Clone, Deserialize)]
pub struct APIRemoveTorrent {
    pub id: u32,
}

pub async fn destroy(
    State(state): State<Arc<AppState>>,
    Json(torrent): Json<APIRemoveTorrent>,
) -> StatusCode {
    let mut torrent_guard = state.stores.torrents.lock();

    if let Some(torrent) = torrent_guard.get_mut(&torrent.id) {
        info!("Removing torrent with id {}.", torrent.id);
        torrent.is_deleted = true;

        return StatusCode::OK;
    }

    StatusCode::BAD_REQUEST
}

pub async fn show(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u32>,
) -> Result<Json<Torrent>, StatusCode> {
    state
        .stores
        .torrents
        .lock()
        .get(&id)
        .map(|torrent| Json(torrent.clone()))
        .ok_or(StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn torrent_with_upload_cap(upload_cap: bool) -> Torrent {
        Torrent {
            upload_cap,
            ..Default::default()
        }
    }

    fn totals(user_id: u32, uploaded: u64) -> UploadTotalStore {
        let mut store = UploadTotalStore::new();
        store.insert(user_id, uploaded);
        store
    }

    #[test]
    fn loads_totals_when_upload_cap_is_switched_on() {
        assert!(needs_upload_totals_load(
            true,
            Some(&torrent_with_upload_cap(false))
        ));
        assert!(needs_upload_totals_load(true, None));
        let chosen = select_upload_totals(true, UploadTotalStore::new(), Some(totals(1, 9)));
        assert_eq!(chosen.get(&1), Some(&9));
    }

    #[test]
    fn keeps_in_memory_totals_when_upload_cap_stays_on() {
        assert!(!needs_upload_totals_load(
            true,
            Some(&torrent_with_upload_cap(true))
        ));
        assert_eq!(
            select_upload_totals(true, totals(1, 7), None).get(&1),
            Some(&7)
        );
    }

    #[test]
    fn clears_totals_when_upload_cap_is_switched_off() {
        assert!(!needs_upload_totals_load(
            false,
            Some(&torrent_with_upload_cap(true))
        ));
        assert!(select_upload_totals(false, totals(1, 7), None).is_empty());
    }
}
