// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::tasks::mock::SyncNodeMocker;
use crate::tasks::{
    AccumulatorCollector, AncestorCollector, BlockAccumulatorSyncTask, BlockCollector,
    BlockSyncTask, FindAncestorTask,
};
use anyhow::Result;
use logger::prelude::*;
use starcoin_accumulator::node::AccumulatorStoreType;
use starcoin_accumulator::{Accumulator, MerkleAccumulator};
use starcoin_chain_api::ChainReader;
use starcoin_chain_mock::BlockChain;
use starcoin_vm_types::genesis_config::{BuiltinNetworkID, ChainNetwork};
use std::sync::Arc;
use stream_task::{Generator, TaskEventCounterHandle, TaskGenerator};

#[stest::test]
pub async fn test_full_sync() -> Result<()> {
    let net1 = ChainNetwork::new_builtin(BuiltinNetworkID::Test);
    let mut node1 = SyncNodeMocker::new(net1, 1, 50)?;
    node1.produce_block(10)?;

    let arc_node1 = Arc::new(node1);

    let net2 = ChainNetwork::new_builtin(BuiltinNetworkID::Test);
    let time_service = net2.time_service();

    let node2 = SyncNodeMocker::new(net2, 1, 50)?;

    let target = arc_node1.chain.head().get_block_info(None)?.unwrap();

    let find_ancestor_task = FindAncestorTask::new(
        node2.chain.head().current_header().number,
        3,
        arc_node1.clone(),
    );
    let event_handle = Arc::new(TaskEventCounterHandle::new());
    let current_block_info = node2.chain.head().get_block_info(None)?.unwrap();
    let storage = node2.chain.head().get_storage();
    let accumulator = Arc::new(MerkleAccumulator::new_with_info(
        current_block_info.block_accumulator_info.clone(),
        storage.get_accumulator_store(AccumulatorStoreType::Block),
    ));
    let target_block_accumulator = target.block_accumulator_info.clone();
    let node2_head_block = node2.chain.head().head_block();
    let block_id_fetcher = arc_node1.clone();
    let block_fetcher = arc_node1.clone();
    let chain_storage = storage.clone();
    let sync_task = TaskGenerator::new(
        find_ancestor_task,
        3,
        15,
        1,
        AncestorCollector::new(accumulator),
        event_handle.clone(),
    )
    .and_then(move |ancestor, event_handle| {
        debug!("find ancestor: {:?}", ancestor);
        let accumulator_sync_task = BlockAccumulatorSyncTask::new(
            ancestor.number + 1,
            target_block_accumulator.clone(),
            block_id_fetcher,
            5,
        );
        Ok(TaskGenerator::new(
            accumulator_sync_task,
            3,
            15,
            1,
            AccumulatorCollector::new(
                storage.get_accumulator_store(AccumulatorStoreType::Block),
                current_block_info.block_accumulator_info.clone(),
                target_block_accumulator,
            ),
            event_handle,
        ))
    })
    .and_then(move |(start, accumulator), event_handle| {
        let block_sync_task = BlockSyncTask::new(accumulator, start.num_leaves, block_fetcher, 3);
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
    let result = sync_task.await?;
    assert_eq!(target.block_id, result.current_header().id());
    Ok(())
}
