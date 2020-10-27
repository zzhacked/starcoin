// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::verified_rpc_client::VerifiedRpcClient;
use anyhow::{format_err, Result};
use futures::future::{BoxFuture, Future};
use futures::task::{Context, Poll};
use futures::FutureExt;
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

pub trait BlockFetcher {
    fn fetch(&self, block_id: HashValue) -> BoxFuture<Result<Block>>;
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
            let block_ids_fut = self.fetch_task.get_or_insert_with(move || {
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
                    self.as_mut().fetch_task = None;
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use futures_timer::Delay;
    use pin_utils::core_reexport::time::Duration;
    use starcoin_accumulator::tree_store::mock::MockAccumulatorStore;
    use starcoin_accumulator::MerkleAccumulator;

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
        let info2 = async_std::task::spawn(sync_task).await?;
        assert_eq!(info1, info2);
        Ok(())
    }
}
