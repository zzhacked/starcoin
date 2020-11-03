// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::verified_rpc_client::VerifiedRpcClient;
use anyhow::Result;
use chain::BlockChain;
use futures::future::BoxFuture;
use futures::FutureExt;
use starcoin_crypto::HashValue;
use starcoin_types::block::{Block, BlockInfo, BlockNumber};
use stream_task::{Generator, TaskEventCounterHandle, TaskFuture, TaskGenerator};

pub trait BlockIdFetcher: Send + Sync {
    fn fetch_block_ids(
        &self,
        start_number: BlockNumber,
        reverse: bool,
        max_size: usize,
    ) -> BoxFuture<Result<Vec<HashValue>>>;
}

impl BlockIdFetcher for VerifiedRpcClient {
    fn fetch_block_ids(
        &self,
        start_number: u64,
        reverse: bool,
        max_size: usize,
    ) -> BoxFuture<Result<Vec<HashValue>>> {
        self.get_block_ids(start_number, reverse, max_size).boxed()
    }
}

impl<T> BlockIdFetcher for Arc<T>
where
    T: BlockIdFetcher,
{
    fn fetch_block_ids(
        &self,
        start_number: u64,
        reverse: bool,
        max_size: usize,
    ) -> BoxFuture<'_, Result<Vec<HashValue>>> {
        BlockIdFetcher::fetch_block_ids(self.as_ref(), start_number, reverse, max_size)
    }
}

pub trait BlockFetcher: Send + Sync {
    fn fetch_block(&self, block_ids: Vec<HashValue>) -> BoxFuture<Result<Vec<Block>>>;
}

impl<T> BlockFetcher for Arc<T>
where
    T: BlockFetcher,
{
    fn fetch_block(&self, block_ids: Vec<HashValue>) -> BoxFuture<'_, Result<Vec<Block>>> {
        BlockFetcher::fetch_block(self.as_ref(), block_ids)
    }
}

pub trait BlockInfoFetcher: Send + Sync {
    fn fetch_block_infos(&self, block_ids: Vec<HashValue>) -> BoxFuture<Result<Vec<BlockInfo>>>;
}

impl<T> BlockInfoFetcher for Arc<T>
where
    T: BlockInfoFetcher,
{
    fn fetch_block_infos(
        &self,
        block_ids: Vec<HashValue>,
    ) -> BoxFuture<'_, Result<Vec<BlockInfo>>> {
        BlockInfoFetcher::fetch_block_infos(self.as_ref(), block_ids)
    }
}

mod accumulator_sync_task;
mod block_sync_task;
mod find_ancestor_task;
#[cfg(test)]
pub(crate) mod mock;
#[cfg(test)]
mod tests;

pub use accumulator_sync_task::{AccumulatorCollector, BlockAccumulatorSyncTask};
pub use block_sync_task::{BlockCollector, BlockSyncTask};
pub use find_ancestor_task::{AncestorCollector, FindAncestorTask};
use logger::prelude::*;
use network_api::PeerProvider;
use starcoin_accumulator::node::AccumulatorStoreType;
use starcoin_accumulator::MerkleAccumulator;
use starcoin_storage::Store;
use starcoin_vm_types::time::TimeService;
use std::sync::Arc;

pub fn full_sync_task<F>(
    current_block: HashValue,
    target: BlockInfo,
    time_service: Arc<dyn TimeService>,
    storage: Arc<dyn Store>,
    fetcher: F,
) -> TaskFuture<BlockChain>
where
    F: BlockIdFetcher + BlockFetcher + BlockInfoFetcher,
{
    let fetcher = Arc::new(fetcher);

    let current_block_number = start.block_accumulator_info.num_leaves - 1;

    let current_block_id = start.block_id;
    let find_ancestor_task = FindAncestorTask::new(current_block_number, 10, fetcher.clone());

    let event_handle = Arc::new(TaskEventCounterHandle::new());

    let target_block_accumulator = target.block_accumulator_info.clone();

    let start_block_accumulator_info = start.block_accumulator_info.clone();

    let chain_storage = storage.clone();
    let accumulator_task_fetcher = fetcher.clone();
    let block_task_fetcher = fetcher.clone();
    let sync_task = TaskGenerator::new(
        find_ancestor_task,
        3,
        15,
        1,
        AncestorCollector::new(Arc::new(MerkleAccumulator::new_with_info(
            start.block_accumulator_info.clone(),
            storage.get_accumulator_store(AccumulatorStoreType::Block),
        ))),
        event_handle.clone(),
    )
    .and_then(move |ancestor, event_handle| {
        debug!("find ancestor: {:?}", ancestor);
        let accumulator_sync_task = BlockAccumulatorSyncTask::new(
            ancestor.number + 1,
            target_block_accumulator.clone(),
            accumulator_task_fetcher,
            5,
        );
        Ok(TaskGenerator::new(
            accumulator_sync_task,
            3,
            15,
            1,
            AccumulatorCollector::new(
                storage.get_accumulator_store(AccumulatorStoreType::Block),
                start_block_accumulator_info,
                target_block_accumulator,
            ),
            event_handle,
        ))
    })
    .and_then(move |(start, accumulator), event_handle| {
        //start_number is include, so start from current_number + 1
        let block_sync_task =
            BlockSyncTask::new(accumulator, current_block_number + 1, block_task_fetcher, 3);
        let collector = BlockCollector::new(BlockChain::new(
            time_service,
            node2_head_block.id(),
            chain_storage,
        )?);
        Ok(TaskGenerator::new(
            block_sync_task,
            2,
            15,
            1,
            collector,
            event_handle,
        ))
    })
    .generate();
}
