// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::PeerId;
use crate::PeerInfo;
use anyhow::{format_err, Result};
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use rand::prelude::IteratorRandom;
use starcoin_types::block::BlockNumber;

pub trait PeerProvider {
    fn identify(&self) -> PeerId;

    fn best_peer(&self) -> BoxFuture<Result<Option<PeerInfo>>> {
        self.best_peer_set()
            .and_then(|mut peers| async move { Ok(peers.pop()) })
            .boxed()
    }

    /// get all peers and sort by difficulty decreasingly.
    fn best_peer_set(&self) -> BoxFuture<Result<Vec<PeerInfo>>>;

    fn get_peer(&self, peer_id: PeerId) -> BoxFuture<Result<Option<PeerInfo>>>;

    fn get_self_peer(&self) -> BoxFuture<Result<PeerInfo>> {
        let peer_id = self.identify();
        self.get_peer(peer_id)
            .and_then(|result| async move {
                result.ok_or_else(|| format_err!("Can not find peer by self id"))
            })
            .boxed()
    }

    fn best_peer_selector(&self) -> BoxFuture<Result<PeerSelector>> {
        self.best_peer_set()
            .and_then(|peers| async move { Ok(PeerSelector::new(peers)) })
            .boxed()
    }
}

pub struct PeerSelector {
    //TODO peers lazy load.
    peers: Vec<PeerInfo>,
}

impl PeerSelector {
    pub fn new(peers: Vec<PeerInfo>) -> Self {
        Self { peers }
    }

    pub fn filter<P>(self, predicate: P) -> Self
    where
        P: Fn(&PeerInfo) -> bool + Send + 'static,
    {
        Self::new(
            self.peers
                .into_iter()
                .filter(|peer| predicate(peer))
                .collect(),
        )
    }

    pub fn filter_by_block_number(self, block_number: BlockNumber) -> Self {
        self.filter(move |peer| peer.latest_header.number >= block_number)
    }

    pub fn random(self) -> Option<PeerInfo> {
        self.peers.into_iter().choose(&mut rand::thread_rng())
    }

    pub fn first(mut self) -> Option<PeerInfo> {
        self.peers.pop()
    }
}

impl IntoIterator for PeerSelector {
    type Item = PeerInfo;
    type IntoIter = std::vec::IntoIter<PeerInfo>;

    fn into_iter(self) -> Self::IntoIter {
        self.peers.into_iter()
    }
}
