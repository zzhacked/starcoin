// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::collector::FutureTaskSink;
use crate::task_stream::FutureTaskStream;
use anyhow::Result;
use futures::{
    future::{abortable, AbortHandle, BoxFuture},
    stream::{self},
    FutureExt, SinkExt, StreamExt,
};
use std::fmt::Debug;
use thiserror::Error;

mod collector;
mod task_stream;

pub use collector::{CounterCollector, TaskResultCollector};

#[derive(Error, Debug)]
pub enum TaskError {
    /// directly break the task, do not retry.
    #[error("Task failed because break error: {0:?}")]
    BreakError(anyhow::Error),
    #[error(
        "Task failed because maximum number of retry attempts({0}) reached, last error: {1:?}"
    )]
    RetryLimitReached(usize, anyhow::Error),
    #[error("Task failed because collector error {0:?}")]
    CollectorError(anyhow::Error),
    #[error("Task has been canceled.")]
    Canceled,
}

impl TaskError {
    pub fn is_canceled(&self) -> bool {
        match self {
            Self::Canceled => true,
            _ => false,
        }
    }

    pub fn is_break_error(&self) -> bool {
        match self {
            Self::BreakError(_) => true,
            _ => false,
        }
    }

    pub fn is_retry_limit_reached(&self) -> bool {
        match self {
            Self::RetryLimitReached(_, _) => true,
            _ => false,
        }
    }

    pub fn is_collector_error(&self) -> bool {
        match self {
            Self::CollectorError(_) => true,
            _ => false,
        }
    }
}

pub trait TaskState: Sized + Clone + std::marker::Unpin + std::marker::Send {
    type Item: Debug + std::marker::Send;

    fn new_sub_task(self) -> BoxFuture<'static, Result<Vec<Self::Item>>>;
    fn next(&self) -> Option<Self>;
}

pub struct TaskHandle {
    inner: AbortHandle,
}

impl TaskHandle {
    pub(crate) fn new(inner: AbortHandle) -> Self {
        Self { inner }
    }

    pub fn cancel(&self) {
        self.inner.abort()
    }
}

pub struct TaskGenerator<S, C>
where
    S: TaskState,
    C: TaskResultCollector<S::Item>,
{
    init_state: S,
    buffer_size: usize,
    max_retry_times: u64,
    delay_milliseconds: u64,
    collector: C,
}

impl<S, C> TaskGenerator<S, C>
where
    S: TaskState + 'static,
    C: TaskResultCollector<S::Item> + 'static,
{
    pub fn new(
        init_state: S,
        buffer_size: usize,
        max_retry_times: u64,
        delay_milliseconds: u64,
        collector: C,
    ) -> Self {
        Self {
            init_state,
            buffer_size,
            max_retry_times,
            delay_milliseconds,
            collector,
        }
    }

    pub fn generate(self) -> (BoxFuture<'static, Result<C::Output, TaskError>>, TaskHandle) {
        let fut = async move {
            let stream = FutureTaskStream::new(
                self.init_state,
                self.max_retry_times,
                self.delay_milliseconds,
            );
            let mut buffered_stream = stream
                .buffered(self.buffer_size)
                .map(|result| {
                    let items = match result {
                        Ok(items) => items.into_iter().map(Ok).collect(),
                        Err(e) => vec![Err(e)],
                    };
                    stream::iter(items)
                })
                .flatten();
            let mut sink = FutureTaskSink::new(self.collector);
            sink.send_all(&mut buffered_stream).await?;
            let collector = sink.into_inner();
            collector.finish().map_err(TaskError::CollectorError)
        };
        let (abortable_fut, handle) = abortable(fut);
        (
            abortable_fut
                .map(|result| match result {
                    Ok(result) => result,
                    Err(_aborted) => Err(TaskError::Canceled),
                })
                .boxed(),
            TaskHandle::new(handle),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::format_err;
    use futures_timer::Delay;
    use log::debug;
    use pin_utils::core_reexport::time::Duration;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    struct MockTestConfig {
        max: u64,
        batch_size: u64,
        delay_time: u64,
        error_per_task: u64,
        break_at: Option<u64>,
        error_times: Mutex<HashMap<u64, AtomicU64>>,
    }

    impl MockTestConfig {
        pub fn new(
            max: u64,
            batch_size: u64,
            delay_time: u64,
            error_per_task: u64,
            break_at: Option<u64>,
        ) -> Self {
            Self {
                max,
                batch_size,
                delay_time,
                error_per_task,
                break_at,
                error_times: Mutex::new(HashMap::new()),
            }
        }

        pub fn new_with_delay(max: u64, delay_time: u64) -> Self {
            Self::new(max, 1, delay_time, 0, None)
        }

        pub fn new_with_error(max: u64, error_per_task: u64) -> Self {
            Self::new(max, 1, 1, error_per_task, None)
        }

        pub fn new_with_max(max: u64) -> Self {
            Self::new(max, 1, 1, 0, None)
        }

        pub fn new_with_break(max: u64, error_per_task: u64, break_at: u64) -> Self {
            Self::new(max, 1, 1, error_per_task, Some(break_at))
        }

        pub fn new_with_batch(max: u64, batch_size: u64) -> Self {
            Self::new(max, batch_size, 1, 0, None)
        }
    }

    #[derive(Clone)]
    struct MockTaskState {
        state: u64,
        config: Arc<MockTestConfig>,
    }

    impl MockTaskState {
        pub fn new(config: MockTestConfig) -> Self {
            Self {
                state: 0,
                config: Arc::new(config),
            }
        }
    }

    impl TaskState for MockTaskState {
        type Item = u64;

        fn new_sub_task(self) -> BoxFuture<'static, Result<Vec<Self::Item>>> {
            async move {
                if let Some(break_at) = self.config.break_at {
                    if self.state >= break_at {
                        return Err(TaskError::BreakError(format_err!(
                            "Break error at: {}",
                            self.state
                        ))
                        .into());
                    }
                }
                if self.config.delay_time > 0 {
                    Delay::new(Duration::from_millis(self.config.delay_time)).await;
                }
                if self.config.error_per_task > 0 {
                    let mut error_times = self.config.error_times.lock().unwrap();
                    let current_state_error_counter = error_times
                        .entry(self.state)
                        .or_insert_with(|| AtomicU64::new(0));
                    let current_state_error_times =
                        current_state_error_counter.fetch_add(1, Ordering::Relaxed);
                    if current_state_error_times <= self.config.error_per_task {
                        return Err(format_err!(
                            "return error for state: {}, error_times: {}",
                            self.state,
                            current_state_error_times
                        ));
                    }
                }
                Ok((self.state..self.state + self.config.batch_size)
                    .filter(|i| *i < self.config.max)
                    .map(|i| i * 2)
                    .collect())
            }
            .boxed()
        }

        fn next(&self) -> Option<Self> {
            if self.state >= self.config.max - 1 {
                None
            } else {
                let next = self.state + self.config.batch_size;
                Some(MockTaskState {
                    state: next,
                    config: self.config.clone(),
                })
            }
        }
    }

    #[stest::test]
    async fn test_future_task_stream() {
        let max = 100;
        let config = MockTestConfig::new_with_max(max);
        let mock_state = MockTaskState::new(config);
        let task = FutureTaskStream::new(mock_state, 0, 0);
        let results = task.buffered(10).collect::<Vec<_>>().await;
        assert_eq!(results.len() as u64, max);
    }

    #[stest::test]
    async fn test_future_task_counter() {
        let max = 100;
        let config = MockTestConfig::new_with_max(max);
        let mock_state = MockTaskState::new(config);
        let result = TaskGenerator::new(mock_state.clone(), 10, 0, 0, CounterCollector::new())
            .generate()
            .0
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), max);
    }

    #[stest::test]
    async fn test_future_task_batch() {
        let max = 100;
        for batch in 1..max {
            let config = MockTestConfig::new_with_batch(max, batch);
            let mock_state = MockTaskState::new(config);
            let result = TaskGenerator::new(mock_state.clone(), 10, 0, 0, CounterCollector::new())
                .generate()
                .0
                .await;
            assert!(result.is_ok(), "assert test batch {} fail.", batch);
            assert_eq!(result.unwrap(), max, "test batch {} fail.", batch);
        }
    }

    #[stest::test]
    async fn test_future_task_vec_collector() {
        let max = 100;
        let config = MockTestConfig::new_with_max(max);
        let mock_state = MockTaskState::new(config);
        let result = TaskGenerator::new(mock_state, 10, 0, 0, vec![])
            .generate()
            .0
            .await;
        //println!("{:?}", result);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len() as u64, max);
    }

    #[stest::test]
    async fn test_future_task_cancel() {
        let max = 100;
        let delay_time = 10;
        let config = MockTestConfig::new_with_delay(max, delay_time);
        let mock_state = MockTaskState::new(config);
        let counter = Arc::new(AtomicU64::new(0));
        let (fut, cancel_handle) = TaskGenerator::new(
            mock_state.clone(),
            5,
            0,
            0,
            CounterCollector::new_with_counter(counter.clone()),
        )
        .generate();
        let join_handle = async_std::task::spawn(fut);
        Delay::new(Duration::from_millis(delay_time * 5)).await;
        cancel_handle.cancel();
        let result = join_handle.await;
        assert!(result.is_err());
        let task_err = result.err().unwrap();
        assert!(task_err.is_canceled());
        let processed_messages = counter.load(Ordering::SeqCst);
        debug!("processed_messages before cancel: {}", processed_messages);
        assert!(processed_messages > 0 && processed_messages < max);
    }

    #[stest::test]
    async fn test_future_task_retry() {
        let max = 100;
        let max_retry_times = 5;
        let config = MockTestConfig::new_with_error(max, max_retry_times - 1);
        let mock_state = MockTaskState::new(config);
        let (fut, _cancel_handle) = TaskGenerator::new(
            mock_state.clone(),
            10,
            max_retry_times,
            1,
            CounterCollector::new(),
        )
        .generate();
        let counter = fut.await.unwrap();
        assert_eq!(counter, max);
    }

    #[stest::test]
    async fn test_future_task_retry_fail() {
        let max = 100;
        let max_retry_times = 5;
        let counter = Arc::new(AtomicU64::new(0));

        let config = MockTestConfig::new_with_error(max, max_retry_times);
        let mock_state = MockTaskState::new(config);
        let (fut, _cancel_handle) = TaskGenerator::new(
            mock_state.clone(),
            10,
            max_retry_times,
            1,
            CounterCollector::new_with_counter(counter.clone()),
        )
        .generate();
        let result = fut.await;
        assert!(result.is_err());
        let task_err = result.err().unwrap();
        assert!(task_err.is_retry_limit_reached());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[stest::test]
    async fn test_collector_error() {
        let max = 100;
        let config = MockTestConfig::new_with_max(max);
        let mock_state = MockTaskState::new(config);
        let result = TaskGenerator::new(mock_state, 10, 0, 0, |item| {
            //println!("collect error for: {:?}", item);
            Err(format_err!("collect error for: {:?}", item))
        })
        .generate()
        .0
        .await;
        assert!(result.is_err());
        let task_err = result.err().unwrap();
        assert!(task_err.is_collector_error());
    }

    #[stest::test]
    async fn test_break_error() {
        let max = 100;
        let break_at = 31;
        let max_retry_times = 5;
        let counter = Arc::new(AtomicU64::new(0));
        let config = MockTestConfig::new_with_break(max, max_retry_times - 1, break_at);
        let mock_state = MockTaskState::new(config);

        let (fut, _handle) = TaskGenerator::new(
            mock_state.clone(),
            10,
            max_retry_times,
            1,
            CounterCollector::new_with_counter(counter.clone()),
        )
        .generate();
        let result = fut.await;
        assert!(result.is_err());
        let task_err = result.err().unwrap();
        assert!(task_err.is_break_error());
        assert_eq!(break_at, counter.load(Ordering::SeqCst));
    }
}
