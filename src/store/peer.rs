use std::fmt::Display;
use std::ops::{Deref, DerefMut};

use chrono::serde::ts_seconds;
use indexmap::IndexMap;
use serde::{Serialize, Serializer};
use sqlx::types::chrono::{DateTime, Utc};

use crate::model::peer_id::PeerId;

use crate::config::Config;

#[derive(Clone, Serialize)]
#[serde(transparent)]
pub struct PeerStore {
    inner: IndexMap<Index, Peer>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Index {
    pub user_id: u32,
    pub peer_id: PeerId,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Peer {
    pub ip_address: std::net::IpAddr,
    pub port: u16,
    pub is_seeder: bool,
    pub is_active: bool,
    pub is_visible: bool,
    pub is_connectable: bool,
    pub has_sent_completed: bool,
    #[serde(with = "ts_seconds")]
    pub updated_at: DateTime<Utc>,
    pub uploaded: u64,
    pub downloaded: u64,
    /// Cached result of `upload_total::is_over_upload_cap` for this peer's
    /// user. Only read when the torrent has `upload_cap` enabled.
    pub is_upload_capped: bool,
}

impl Peer {
    /// Determines if the peer should be included in the peer list
    #[inline(always)]
    pub fn is_included_in_peer_list(&self, config: &Config) -> bool {
        if config.require_peer_connectivity {
            self.is_active && self.is_visible && self.is_connectable
        } else {
            self.is_active && self.is_visible
        }
    }

    /// Determines if the peer should be included in the list of seeds
    #[inline(always)]
    pub fn is_included_in_seed_list(&self, config: &Config) -> bool {
        self.is_seeder && self.is_included_in_peer_list(config)
    }

    /// Determines if the peer should be included in the list of leeches
    #[inline(always)]
    pub fn is_included_in_leech_list(&self, config: &Config) -> bool {
        !self.is_seeder && self.is_included_in_peer_list(config)
    }

    /// Determines if the peer should be left out of peer lists because its
    /// user reached the upload cap on a torrent with upload priority enabled
    #[inline(always)]
    pub fn is_withheld(&self, withholds_capped_peers: bool) -> bool {
        withholds_capped_peers && self.is_upload_capped
    }
}

impl PeerStore {
    pub fn new() -> PeerStore {
        PeerStore {
            inner: IndexMap::new(),
        }
    }
}

impl Deref for PeerStore {
    type Target = IndexMap<Index, Peer>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PeerStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Default for PeerStore {
    fn default() -> Self {
        PeerStore::new()
    }
}

impl Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.user_id, self.peer_id)
    }
}

impl Serialize for Index {
    fn serialize<S>(&self, serializer: S) -> std::prelude::v1::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(test)]
mod tests {
    //! Tests for `Peer::is_withheld`, which combines the torrent-level
    //! switch with the peer's cached cap flag.

    use super::*;

    /// Builds an active, visible, connectable seed with the given cap flag.
    fn make_peer(is_upload_capped: bool) -> Peer {
        Peer {
            ip_address: std::net::IpAddr::from([127, 0, 0, 1]),
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

    /// A capped peer on a torrent with upload priority enabled is left out
    /// of peer lists.
    #[test]
    fn capped_peer_is_withheld_when_torrent_withholds() {
        assert!(
            make_peer(true).is_withheld(true),
            "a capped peer must be withheld when the torrent withholds capped peers"
        );
    }

    /// The cap flag can be stale after upload priority is switched off for
    /// a torrent. It must then be ignored.
    #[test]
    fn capped_peer_is_not_withheld_when_torrent_does_not_withhold() {
        assert!(
            !make_peer(true).is_withheld(false),
            "a stale cap flag must be ignored when the torrent doesn't withhold"
        );
    }

    /// A peer below the cap is always handed out, whatever the torrent
    /// setting is.
    #[test]
    fn uncapped_peer_is_never_withheld() {
        assert!(
            !make_peer(false).is_withheld(true),
            "an uncapped peer must not be withheld on a torrent that withholds"
        );
        assert!(
            !make_peer(false).is_withheld(false),
            "an uncapped peer must not be withheld on a torrent that doesn't withhold"
        );
    }
}
