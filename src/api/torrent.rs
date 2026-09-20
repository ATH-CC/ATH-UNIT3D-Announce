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

pub async fn upsert(
    State(state): State<Arc<AppState>>,
    Json(torrent): Json<APIInsertTorrent>,
) -> StatusCode {
    if let Ok(info_hash) = InfoHash::from_str(&torrent.info_hash) {
        info!("Inserting torrent with id {}.", torrent.id);

        // Upload totals are only kept in memory while upload priority is
        // enabled, so they don't exist yet when it gets switched on and have
        // to be loaded from `history`.
        //
        // When the torrent already had upload priority, its in-memory totals
        // are kept instead of reloading them. They include every announce up
        // to now, while `history` lags behind by the updates still queued
        // for the next flush.
        //
        // The block limits the lock to this one lookup. The lock must not be
        // held across the `.await` below: its guard can't be sent between
        // threads, and holding it would block every announce on every
        // torrent for as long as the query takes.
        let is_newly_prioritized = torrent.upload_cap && {
            !state
                .stores
                .torrents
                .lock()
                .get(&torrent.id)
                .is_some_and(|old_torrent| old_torrent.upload_cap)
        };

        // Loaded into a local first, while the old copy of the torrent is
        // still in the store and keeps serving announces. The torrent is
        // only taken out of the store further down, and put back right
        // after, without awaiting anything in between. If it were taken out
        // before the query, every announce for it during the query would
        // fail with "torrent not found".
        //
        // The loaded totals miss upload that is still queued for `history`,
        // up to one flush interval of it, including announces that happen
        // during the query. The old copy doesn't count them either, since it
        // has no upload priority yet. The upload does reach `history` on the
        // next flush, so the gap closes at the next restart or the next time
        // the flag is enabled.
        let loaded_upload_totals = if is_newly_prioritized {
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

        let old_torrent = state.stores.torrents.lock().swap_remove(&torrent.id);
        let old_torrent = old_torrent.unwrap_or_default();

        let upload_totals = if torrent.upload_cap {
            loaded_upload_totals.unwrap_or(old_torrent.upload_totals)
        } else {
            // Free the memory once the flag is switched off
            UploadTotalStore::new()
        };

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

        // The peers came over with their cached cap flags, which may no
        // longer match: the totals may have just been loaded, and the size
        // or the configured threshold may have changed. Recompute them for
        // the whole swarm. When the flag is off, the stale flags are harmless
        // because peer lists ignore them, and they are recomputed if it's
        // switched back on.
        if new_torrent.upload_cap {
            new_torrent.refresh_upload_caps(state.config.load().upload_cap_threshold);
        }

        state.stores.torrents.lock().insert(torrent.id, new_torrent);

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
