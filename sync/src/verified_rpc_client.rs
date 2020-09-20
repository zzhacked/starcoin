// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use anyhow::{format_err, Result};
use crypto::hash::HashValue;
use crypto::hash::PlainCryptoHash;
use logger::prelude::*;
use network_api::{PeerInfo, PeerProvider};
use starcoin_accumulator::node::AccumulatorStoreType;
use starcoin_accumulator::AccumulatorNode;
use starcoin_network_rpc_api::{
    gen_client::NetworkRpcClient, BlockBody, GetAccumulatorNodeByNodeHash, GetBlockHeaders,
    GetBlockHeadersByNumber, GetTxns, RawRpcClient, TransactionsData,
};
use starcoin_state_tree::StateNode;
use starcoin_types::{
    block::{BlockHeader, BlockInfo, BlockNumber},
    peer_info::PeerId,
    transaction::TransactionInfo,
};
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

pub trait RpcVerify<C: Clone> {
    fn filter<T, F>(&mut self, rpc_data: Vec<T>, hash_fun: F) -> Vec<T>
    where
        F: Fn(&T) -> C;
}

pub struct RpcEntryVerify<C: Clone + Eq + Hash> {
    entries: HashSet<C>,
}

impl<C: Clone + Eq + Hash> RpcVerify<C> for RpcEntryVerify<C> {
    fn filter<T, F>(&mut self, rpc_data: Vec<T>, hash_fun: F) -> Vec<T>
    where
        F: Fn(&T) -> C,
    {
        let mut rpc_data = rpc_data;
        let mut dirty_data = Vec::new();
        for data in rpc_data.as_slice().iter() {
            let hash = hash_fun(data);
            if !self.entries.contains(&hash) {
                dirty_data.push(hash);
            }
        }

        if !dirty_data.is_empty() {
            for hash in dirty_data.as_slice().iter() {
                rpc_data.retain(|d| hash_fun(d) != *hash);
            }
        }

        rpc_data
    }
}

impl From<&Vec<HashValue>> for RpcEntryVerify<HashValue> {
    fn from(data: &Vec<HashValue>) -> Self {
        let mut entries = HashSet::new();
        for hash in data.iter() {
            entries.insert(*hash);
        }
        Self { entries }
    }
}

impl From<&Vec<BlockNumber>> for RpcEntryVerify<BlockNumber> {
    fn from(data: &Vec<BlockNumber>) -> Self {
        let mut entries = HashSet::new();
        for number in data.iter() {
            entries.insert(*number);
        }
        Self { entries }
    }
}

impl From<&GetTxns> for RpcEntryVerify<HashValue> {
    fn from(data: &GetTxns) -> Self {
        let mut entries = HashSet::new();
        if let Some(ids) = data.clone().ids {
            for hash in ids.into_iter() {
                entries.insert(hash);
            }
        }
        Self { entries }
    }
}

impl From<&GetBlockHeadersByNumber> for RpcEntryVerify<BlockNumber> {
    fn from(data: &GetBlockHeadersByNumber) -> Self {
        let numbers: Vec<BlockNumber> = data.clone().into();
        let mut entries = HashSet::new();
        for number in numbers.into_iter() {
            entries.insert(number);
        }
        Self { entries }
    }
}

//TODO verify this const.
const STABILIZE_BLOCK_NUM: usize = 7;

/// Enhancement RpcClient, for verify rpc response by request.
#[derive(Clone)]
pub struct VerifiedRpcClient {
    peer_provider: Arc<dyn PeerProvider>,
    client: NetworkRpcClient,
}

impl VerifiedRpcClient {
    pub fn new<C, P>(raw_rpc_client: C, peer_provider: P) -> Self
    where
        C: RawRpcClient + Send + Sync + 'static,
        P: PeerProvider + 'static,
    {
        Self {
            peer_provider: Arc::new(peer_provider),
            client: NetworkRpcClient::new(raw_rpc_client),
        }
    }

    pub(crate) async fn get_peer(&self, peer_id: PeerId) -> Result<Option<PeerInfo>> {
        self.peer_provider.get_peer(peer_id).await
    }

    pub(crate) async fn best_peer(&self) -> Result<Option<PeerInfo>> {
        self.peer_provider.best_peer().await
    }

    pub async fn get_txns(&self, peer_id: PeerId, req: GetTxns) -> Result<TransactionsData> {
        let data = self.client.get_txns(peer_id, req.clone()).await?;
        if req.ids.is_some() {
            let mut verify_condition: RpcEntryVerify<HashValue> = (&req).into();
            let verified_txns = verify_condition
                .filter((*data.get_txns()).to_vec(), |txn| -> HashValue {
                    txn.crypto_hash()
                });
            Ok(TransactionsData {
                txns: verified_txns,
            })
        } else {
            Ok(data)
        }
    }

    pub async fn get_txn_infos(
        &self,
        peer_id: PeerId,
        block_id: HashValue,
    ) -> Result<Option<Vec<TransactionInfo>>> {
        self.client.get_txn_infos(peer_id, block_id).await
    }

    pub async fn get_headers_by_number(
        &self,
        peer_id: PeerId,
        req: GetBlockHeadersByNumber,
    ) -> Result<Vec<BlockHeader>> {
        let mut verify_condition: RpcEntryVerify<BlockNumber> = (&req).into();
        let data = self.client.get_headers_by_number(peer_id, req).await?;
        let verified_headers =
            verify_condition.filter(data, |header| -> BlockNumber { header.number() });
        Ok(verified_headers)
    }

    pub async fn get_headers_with_peer(
        &self,
        peer_id: PeerId,
        req: GetBlockHeaders,
        number: BlockNumber,
    ) -> Result<Vec<BlockHeader>> {
        let mut verify_condition: RpcEntryVerify<BlockNumber> =
            (&req.clone().into_numbers(number)).into();
        let data = self.client.get_headers_with_peer(peer_id, req).await?;
        let verified_headers =
            verify_condition.filter(data, |header| -> BlockNumber { header.number() });
        Ok(verified_headers)
    }

    pub async fn get_headers(
        &self,
        req: GetBlockHeaders,
        number: BlockNumber,
    ) -> Result<(Vec<BlockHeader>, PeerId)> {
        let selected_peer = self
            .peer_provider
            .best_peer_selector()
            .await?
            .filter_by_block_number(number + req.max_size as u64 + STABILIZE_BLOCK_NUM as u64)
            .random();

        if let Some(peer) = selected_peer {
            debug!("rpc select peer {:?}", peer);
            let peer_id = peer.peer_id.clone();
            self.get_headers_with_peer(peer_id.clone(), req, number)
                .await
                .map(|headers| (headers, peer_id))
        } else {
            Err(format_err!("Can not get peer when sync block header."))
        }
    }

    pub async fn get_header_by_hash(&self, hashes: Vec<HashValue>) -> Result<Vec<BlockHeader>> {
        if let Some(peer_info) = self.peer_provider.best_peer().await? {
            let mut verify_condition: RpcEntryVerify<HashValue> = (&hashes).into();
            let data: Vec<BlockHeader> = self
                .client
                .get_header_by_hash(peer_info.get_peer_id(), hashes)
                .await?;
            let verified_headers =
                verify_condition.filter(data, |header| -> HashValue { header.id() });
            Ok(verified_headers)
        } else {
            Err(format_err!("Can not get peer when sync block header."))
        }
    }

    pub async fn get_body_by_hash(
        &self,
        hashes: Vec<HashValue>,
        max_height: BlockNumber,
    ) -> Result<(Vec<BlockBody>, PeerId)> {
        let selected_peer = self
            .peer_provider
            .best_peer_selector()
            .await?
            .filter_by_block_number(max_height as u64 + STABILIZE_BLOCK_NUM as u64)
            .random();

        if let Some(peer_info) = selected_peer {
            let peer_id = peer_info.get_peer_id();
            debug!("rpc select peer {}", &peer_id);
            let mut verify_condition: RpcEntryVerify<HashValue> = (&hashes).into();
            let data: Vec<BlockBody> = self
                .client
                .get_body_by_hash(peer_id.clone(), hashes)
                .await?;
            let verified_bodies = verify_condition.filter(data, |body| -> HashValue { body.id() });
            Ok((verified_bodies, peer_id))
        } else {
            Err(format_err!("Can not get peer when sync block body."))
        }
    }

    pub async fn get_info_by_hash(
        &self,
        peer_id: PeerId,
        hashes: Vec<HashValue>,
    ) -> Result<Vec<BlockInfo>> {
        let mut verify_condition: RpcEntryVerify<HashValue> = (&hashes).into();
        let data = self.client.get_info_by_hash(peer_id, hashes).await?;
        let verified_infos =
            verify_condition.filter(data, |info| -> HashValue { *info.block_id() });
        Ok(verified_infos)
    }

    pub async fn get_state_node_by_node_hash(
        &self,
        peer_id: PeerId,
        node_key: HashValue,
    ) -> Result<StateNode> {
        if let Some(state_node) = self
            .client
            .get_state_node_by_node_hash(peer_id, node_key)
            .await?
        {
            let state_node_id = state_node.inner().hash();
            if node_key == state_node_id {
                Ok(state_node)
            } else {
                Err(format_err!(
                    "State node hash {:?} and node key {:?} mismatch.",
                    state_node_id,
                    node_key
                ))
            }
        } else {
            Err(format_err!(
                "State node is none by node key {:?}.",
                node_key
            ))
        }
    }

    pub async fn get_accumulator_node_by_node_hash(
        &self,
        peer_id: PeerId,
        node_key: HashValue,
        accumulator_type: AccumulatorStoreType,
    ) -> Result<AccumulatorNode> {
        if let Some(accumulator_node) = self
            .client
            .get_accumulator_node_by_node_hash(
                peer_id,
                GetAccumulatorNodeByNodeHash {
                    node_hash: node_key,
                    accumulator_storage_type: accumulator_type,
                },
            )
            .await?
        {
            let accumulator_node_id = accumulator_node.hash();
            if node_key == accumulator_node_id {
                Ok(accumulator_node)
            } else {
                Err(format_err!(
                    "Accumulator node hash {:?} and node key {:?} mismatch.",
                    accumulator_node_id,
                    node_key
                ))
            }
        } else {
            Err(format_err!(
                "Accumulator node is none by node key {:?}.",
                node_key
            ))
        }
    }
}
