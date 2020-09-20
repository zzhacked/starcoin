// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::PeerId;
use crate::PeerInfo;
use anyhow::{format_err, Result};
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use rand::prelude::IteratorRandom;

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

    fn select_peers<P>(&self, predicate: P) -> BoxFuture<Result<Vec<PeerInfo>>>
    where
        P: Fn(&PeerInfo) -> bool + Send + 'static,
    {
        self.best_peer_set()
            .and_then(|peers| async move {
                Ok(peers
                    .into_iter()
                    .filter(|peer| predicate(peer))
                    .collect::<Vec<PeerInfo>>())
            })
            .boxed()
    }

    fn select_one<P>(&self, predicate: P) -> BoxFuture<Result<Option<PeerInfo>>>
    where
        P: Fn(&PeerInfo) -> bool + Send + 'static,
    {
        self.select_peers(predicate)
            .and_then(|peers| async move {
                let peer = if peers.is_empty() {
                    None
                } else {
                    peers.into_iter().choose(&mut rand::thread_rng())
                };
                Ok(peer)
            })
            .boxed()
    }
}
