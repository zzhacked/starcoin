// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::verified_rpc_client::VerifiedRpcClient;
use anyhow::{format_err, Result};
use futures::future::{BoxFuture, Future};
use futures::task::{Context, Poll};
use futures::{FutureExt, Stream};
use logger::prelude::*;
use starcoin_accumulator::accumulator_info::AccumulatorInfo;
use starcoin_accumulator::{Accumulator, AccumulatorTreeStore, MerkleAccumulator};
use starcoin_crypto::HashValue;
use starcoin_types::block::{Block, BlockNumber};
use std::cmp::Ordering;
use std::pin::Pin;
use std::sync::Arc;

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

pub trait BlockFetcher: Send + Sync {
    fn fetch(&self, block_id: HashValue) -> BoxFuture<Result<Option<Block>>>;
}

pub struct BlockAccumulatorSyncTask {
    accumulator: MerkleAccumulator,
    target: AccumulatorInfo,
    fetcher: Arc<dyn BlockIdFetcher>,
    batch_size: usize,
    fetch_task: Option<Pin<Box<dyn Future<Output = Result<Vec<HashValue>>> + Send>>>,
}

impl BlockAccumulatorSyncTask {
    pub fn new<F>(
        store: Arc<dyn AccumulatorTreeStore>,
        current: AccumulatorInfo,
        target: AccumulatorInfo,
        fetcher: F,
        batch_size: usize,
    ) -> Self
    where
        F: BlockIdFetcher + 'static,
    {
        let accumulator = MerkleAccumulator::new_with_info(current, store);
        Self {
            accumulator,
            target,
            fetcher: Arc::new(fetcher),
            batch_size,
            fetch_task: None,
        }
    }
}

impl Future for BlockAccumulatorSyncTask {
    type Output = Result<AccumulatorInfo>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let start = self.accumulator.num_leaves();
            let target = self.target.num_leaves;

            let mut max_size = (target - start) as usize;
            if max_size > self.batch_size {
                max_size = self.batch_size;
            }
            let fetcher = self.fetcher.clone();
            let block_ids_fut = self.fetch_task.get_or_insert_with(|| {
                async move {
                    debug!(
                        "Accumulator sync task: start_number: {}, target_number: {}",
                        start, target
                    );
                    fetcher.fetch_block_ids(start, false, max_size).await
                }
                .boxed()
            });

            match block_ids_fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    self.fetch_task = None;
                    match result {
                        Err(e) => {
                            //TODO add retry limit.
                            error!("Fetch block ids error: {:?}", e);
                            continue;
                        }
                        Ok(block_ids) => match self.accumulator.append(block_ids.as_slice()) {
                            Ok(_) => {
                                let current_leaves = self.accumulator.num_leaves();
                                match current_leaves.cmp(&self.target.num_leaves) {
                                    Ordering::Greater => {
                                        //This may be a case where BlockIdFetcher returns HashValues that exceeds max_size, which should never happen if the rpc client checks the return value correctly.
                                        return Poll::Ready(Err(format_err!("Unexpect status, maybe rpc client return invalid result: {:?}", block_ids)));
                                    }
                                    Ordering::Less => continue,
                                    Ordering::Equal => {
                                        let current_info = self.accumulator.get_info();
                                        return if self.target == current_info {
                                            Poll::Ready(Ok(current_info))
                                        } else {
                                            Poll::Ready(Err(format_err!("Verify accumulator root fail, expect: {:?}, but get: {:?}",self.target,current_info)))
                                        };
                                    }
                                }
                            }
                            Err(e) => return Poll::Ready(Err(e)),
                        },
                    }
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

pub struct BlockSyncTask {
    accumulator: MerkleAccumulator,
    current_number: BlockNumber,
    fetcher: Arc<dyn BlockFetcher>,
    fetch_task: Option<Pin<Box<dyn Future<Output = Result<Option<Block>>> + Send>>>,
}

impl BlockSyncTask {
    pub fn new<F>(accumulator: MerkleAccumulator, start_number: BlockNumber, fetcher: F) -> Self
    where
        F: BlockFetcher + 'static,
    {
        Self {
            accumulator,
            current_number: start_number,
            fetcher: Arc::new(fetcher),
            fetch_task: None,
        }
    }
}

impl Stream for BlockSyncTask {
    type Item = Block;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let current_number = self.current_number;
            let block_id = match self.accumulator.get_leaf(current_number) {
                Ok(Some(id)) => id,
                Ok(None) => {
                    return if current_number == self.accumulator.num_leaves() - 1 {
                        //task is finished.
                        Poll::Ready(None)
                    } else {
                        error!(
                            "Block sync task end by accumulator get leaf({}) return None",
                            current_number
                        );
                        Poll::Ready(None)
                    };
                }
                Err(e) => {
                    error!("Block sync task end by accumulator error: {:?}", e);
                    return Poll::Ready(None);
                }
            };
            let fetcher = self.fetcher.clone();
            let task_fut = self.fetch_task.get_or_insert_with(|| {
                async move {
                    debug!(
                        "Block sync task: current_number: {}, id: {:?}",
                        current_number, block_id
                    );
                    fetcher.fetch(block_id).await
                }
                .boxed()
            });
            match task_fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    self.fetch_task = None;
                    match result {
                        Err(e) => {
                            //TODO add retry limit.
                            error!("Fetch block ids error: {:?}", e);
                            continue;
                        }
                        Ok(Some(block)) => {
                            self.current_number = current_number + 1;
                            return Poll::Ready(Some(block));
                        }
                        Ok(None) => {
                            warn!(
                                "BlockFetcher return None when get block by {:?}, retry.",
                                block_id
                            );
                            continue;
                        }
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::future_task::{TaskError, TaskGenerator, TaskState};
    use futures::{FutureExt, StreamExt};
    use futures_timer::Delay;
    use pin_utils::core_reexport::time::Duration;
    use starcoin_accumulator::tree_store::mock::MockAccumulatorStore;
    use starcoin_accumulator::MerkleAccumulator;
    use starcoin_types::block::BlockHeader;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MockBlockIdFetcher {
        accumulator: MerkleAccumulator,
    }

    impl MockBlockIdFetcher {
        async fn fetch_block_ids_async(
            &self,
            start_number: u64,
            reverse: bool,
            max_size: usize,
        ) -> Result<Vec<HashValue>> {
            Delay::new(Duration::from_millis(100)).await;
            self.accumulator.get_leaves(start_number, reverse, max_size)
        }
    }

    impl BlockIdFetcher for MockBlockIdFetcher {
        fn fetch_block_ids(
            &self,
            start_number: u64,
            reverse: bool,
            max_size: usize,
        ) -> BoxFuture<Result<Vec<HashValue>>> {
            self.fetch_block_ids_async(start_number, reverse, max_size)
                .boxed()
        }
    }

    #[stest::test]
    async fn test_accumulator_sync() -> Result<()> {
        let store = Arc::new(MockAccumulatorStore::new());
        let accumulator = MerkleAccumulator::new_empty(store.clone());
        for _i in 0..100 {
            accumulator.append(&[HashValue::random()])?;
        }
        accumulator.flush().unwrap();
        let info0 = accumulator.get_info();
        assert_eq!(info0.num_leaves, 100);
        for _i in 0..100 {
            accumulator.append(&[HashValue::random()])?;
        }
        accumulator.flush().unwrap();
        let info1 = accumulator.get_info();
        assert_eq!(info1.num_leaves, 200);
        for i in 0..200 {
            accumulator.get_leaf(i).unwrap().unwrap();
        }
        let fetcher = MockBlockIdFetcher { accumulator };
        let store2 = MockAccumulatorStore::copy_from(store.as_ref());
        let sync_task =
            BlockAccumulatorSyncTask::new(Arc::new(store2), info0, info1.clone(), fetcher, 7);
        let info2 = sync_task.await?;
        assert_eq!(info1, info2);
        Ok(())
    }

    // #[derive(Clone)]
    // pub struct AccumulatorSyncState{
    //     accumulator: MerkleAccumulator,
    //     target: AccumulatorInfo,
    //     fetcher: Arc<dyn BlockIdFetcher>,
    // }
    //
    // impl TaskState for AccumulatorSyncState{
    //
    // }
    //
    // pub struct AccumulatorSyncTaskGenerator{
    // }
    //
    // impl TaskGenerator for AccumulatorSyncTaskGenerator{
    //     type State = AccumulatorSyncState;
    //     type Item = HashValue;
    //
    //     fn new_sub_task(state: Self::State) -> BoxFuture<'static, Result<Self::Item>> {
    //         unimplemented!()
    //     }
    //
    //     fn next_state(pre_state: &Self::State) -> Option<Self::State> {
    //         unimplemented!()
    //     }
    // }
    //
    // #[stest::test]
    // async fn test_accumulator_sync_by_future_task() -> Result<()> {
    //
    //     let store = Arc::new(MockAccumulatorStore::new());
    //     let accumulator = MerkleAccumulator::new_empty(store.clone());
    //     for _i in 0..100 {
    //         accumulator.append(&[HashValue::random()])?;
    //     }
    //     accumulator.flush().unwrap();
    //     let info0 = accumulator.get_info();
    //     assert_eq!(info0.num_leaves, 100);
    //     for _i in 0..100 {
    //         accumulator.append(&[HashValue::random()])?;
    //     }
    //     accumulator.flush().unwrap();
    //     let info1 = accumulator.get_info();
    //     assert_eq!(info1.num_leaves, 200);
    //
    //
    //     Ok(())
    // }

    #[derive(Default)]
    struct MockBlockFetcher {
        blocks: Mutex<HashMap<HashValue, Block>>,
    }

    impl MockBlockFetcher {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn put(&self, block: Block) {
            self.blocks.lock().unwrap().insert(block.id(), block);
        }
    }

    impl BlockFetcher for MockBlockFetcher {
        fn fetch(&self, block_id: HashValue) -> BoxFuture<Result<Option<Block>>> {
            let block = self.blocks.lock().unwrap().get(&block_id).cloned();
            async {
                Delay::new(Duration::from_millis(100)).await;
                Ok(block)
            }
            .boxed()
        }
    }

    fn build_block_fetcher(total_blocks: u64) -> (MockBlockFetcher, MerkleAccumulator) {
        let fetcher = MockBlockFetcher::new();

        let store = Arc::new(MockAccumulatorStore::new());
        let accumulator = MerkleAccumulator::new_empty(store.clone());
        for i in 0..total_blocks {
            let mut header = BlockHeader::random();
            header.number = i;
            let block = Block::new(header, vec![]);
            accumulator.append(&[block.id()]).unwrap();
            fetcher.put(block);
        }
        (fetcher, accumulator)
    }

    #[stest::test]
    async fn test_block_sync() -> Result<()> {
        let total_blocks = 100;
        let (fetcher, accumulator) = build_block_fetcher(total_blocks);
        let sync_task = BlockSyncTask::new(accumulator, 0, fetcher);
        let last_block_number = sync_task
            .map(|block| block.header().number as i64)
            .fold(-1, |parent, current| async move {
                //ensure return block is ordered
                assert_eq!(
                    parent + 1,
                    current,
                    "block sync task not return ordered blocks"
                );
                current
            })
            .await;

        assert_eq!(last_block_number as u64, total_blocks - 1);
        Ok(())
    }

    #[derive(Clone)]
    struct BlockSyncTaskState {
        accumulator: Arc<MerkleAccumulator>,
        current_number: BlockNumber,
        fetcher: Arc<dyn BlockFetcher>,
    }

    impl BlockSyncTaskState {
        pub fn new<F>(accumulator: MerkleAccumulator, start_number: BlockNumber, fetcher: F) -> Self
        where
            F: BlockFetcher + 'static,
        {
            Self {
                accumulator: Arc::new(accumulator),
                current_number: start_number,
                fetcher: Arc::new(fetcher),
            }
        }
    }

    impl TaskState for BlockSyncTaskState {}

    struct BlockSyncTaskGenerator {}

    impl TaskGenerator for BlockSyncTaskGenerator {
        type State = BlockSyncTaskState;
        type Item = Block;

        fn new_sub_task(state: Self::State) -> BoxFuture<'static, Result<Self::Item>> {
            async move {
                let hash = state.accumulator.get_leaf(state.current_number)?;
                match hash {
                    Some(hash) => state
                        .fetcher
                        .fetch(hash)
                        .await?
                        .ok_or_else(|| format_err!("Get block by id {:?} return None", hash)),
                    None => Err(TaskError::Fail(format_err!(
                        "Get leaf by number {:?} from accumulator return None",
                        state.current_number
                    ))
                    .into()),
                }
            }
            .boxed()
        }

        fn next_state(pre_state: &Self::State) -> Option<Self::State> {
            if pre_state.current_number >= pre_state.accumulator.num_leaves() - 1 {
                None
            } else {
                Some(BlockSyncTaskState {
                    accumulator: pre_state.accumulator.clone(),
                    current_number: pre_state.current_number + 1,
                    fetcher: pre_state.fetcher.clone(),
                })
            }
        }
    }

    #[stest::test]
    async fn test_block_sync_by_future_task() -> Result<()> {
        let total_blocks = 100;
        let (fetcher, accumulator) = build_block_fetcher(total_blocks);
        let blocks = BlockSyncTaskGenerator::new_task(
            BlockSyncTaskState::new(accumulator, 0, fetcher),
            5,
            5,
            vec![],
        )
        .await?;
        assert_eq!(blocks.len(), total_blocks as usize);

        blocks
            .into_iter()
            .map(|block| block.header().number as i64)
            .fold(-1, |parent, current| {
                assert_eq!(
                    parent + 1,
                    current,
                    "block sync task not return ordered blocks"
                );
                current
            });
        Ok(())
    }

    // #[stest::test]
    // async fn test_block_sync_buffered() -> Result<()> {
    //     let total_blocks = 100;
    //     let (fetcher, accumulator) = build_block_fetcher(total_blocks);
    //     let sync_task = BlockSyncTask::new(accumulator, 0, fetcher);
    //     let buffered_task = sync_task.buffered(7);
    //     let last_block_number = buffered_task
    //         .map(|block| block.header().number as i64)
    //         .fold(-1, |parent, current| async move {
    //             //ensure return block is ordered
    //             assert_eq!(
    //                 parent + 1,
    //                 current,
    //                 "block sync task not return ordered blocks"
    //             );
    //             current
    //         })
    //         .await;
    //
    //     assert_eq!(last_block_number as u64, total_blocks - 1);
    //     Ok(())
    // }
}
