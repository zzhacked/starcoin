// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::collector::FutureTaskSink;
use crate::task_stream::FutureTaskStream;
use anyhow::Result;
use futures::task::{Context, Poll};
use futures::{
    future::{abortable, AbortHandle, BoxFuture},
    stream::{self},
    Future, FutureExt, SinkExt, StreamExt, TryFutureExt,
};
use std::any::type_name;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;

mod collector;
mod task_stream;

pub use collector::{CounterCollector, TaskResultCollector};

pub trait TaskEventHandle: Send + Sync {
    fn on_start(&self, name: String, total_items: Option<u64>);

    fn on_error(&self);

    fn on_ok(&self);

    fn on_retry(&self);

    fn on_item(&self);

    fn on_finish(&self);
}

#[derive(Clone)]
pub struct TaskEventCounter {
    name: Option<String>,
    total_items: Option<u64>,
    error_counter: Arc<AtomicU64>,
    ok_counter: Arc<AtomicU64>,
    retry_counter: Arc<AtomicU64>,
    item_counter: Arc<AtomicU64>,
}

impl TaskEventCounter {
    pub(crate) fn new() -> Self {
        Self {
            name: None,
            total_items: None,
            error_counter: Arc::new(AtomicU64::new(0)),
            ok_counter: Arc::new(AtomicU64::new(0)),
            retry_counter: Arc::new(AtomicU64::new(0)),
            item_counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl TaskEventHandle for TaskEventCounter {
    fn on_start(&self, name: String, total_items: Option<u64>) {}

    fn on_error(&self) {
        self.error_counter.fetch_add(1, Ordering::Release);
    }

    fn on_ok(&self) {
        self.ok_counter.fetch_add(1, Ordering::Release);
    }

    fn on_retry(&self) {
        self.retry_counter.fetch_add(1, Ordering::Release);
    }

    fn on_item(&self) {
        self.item_counter.fetch_add(1, Ordering::Release);
    }

    fn on_finish(&self) {
        unimplemented!()
    }
}

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

    fn name() -> &'static str {
        type_name::<Self>()
    }
    fn new_sub_task(self) -> BoxFuture<'static, Result<Vec<Self::Item>>>;
    fn next(&self) -> Option<Self>;
    fn total_items(&self) -> Option<u64> {
        None
    }
}

pub struct TaskHandle {
    inner: AbortHandle,
    is_done: Arc<AtomicBool>,
}

impl TaskHandle {
    pub(crate) fn new(inner: AbortHandle, is_done: Arc<AtomicBool>) -> Self {
        Self { inner, is_done }
    }

    pub fn cancel(&self) {
        self.inner.abort()
    }

    pub fn is_done(&self) -> bool {
        self.is_done.load(Ordering::SeqCst)
    }
}

pub struct TaskFuture<Output> {
    fut: BoxFuture<'static, Result<Output, TaskError>>,
}

impl<Output> TaskFuture<Output>
where
    Output: Send + 'static,
{
    pub fn new(fut: BoxFuture<'static, Result<Output, TaskError>>) -> Self {
        Self { fut }
    }

    pub fn with_handle(self) -> (BoxFuture<'static, Result<Output, TaskError>>, TaskHandle) {
        let (abortable_fut, handle) = abortable(self.fut);
        let is_done = Arc::new(AtomicBool::new(false));
        let fut_is_done = is_done.clone();
        (
            abortable_fut
                .map(move |result| {
                    fut_is_done.store(true, Ordering::SeqCst);
                    match result {
                        Ok(result) => result,
                        Err(_aborted) => Err(TaskError::Canceled),
                    }
                })
                .boxed(),
            TaskHandle::new(handle, is_done),
        )
    }
}

impl<Output> Future for TaskFuture<Output> {
    type Output = Result<Output, TaskError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.fut.as_mut()).poll(cx)
    }
}

pub trait Generator: Send {
    type State: TaskState;
    type Output: std::marker::Send;
    fn generate(self) -> TaskFuture<Self::Output>;
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
    event_handle: Arc<dyn TaskEventHandle>,
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
        delay_milliseconds_on_error: u64,
        collector: C,
        event_handle: Arc<dyn TaskEventHandle>,
    ) -> Self {
        Self {
            init_state,
            buffer_size,
            max_retry_times,
            delay_milliseconds: delay_milliseconds_on_error,
            collector,
            event_handle,
        }
    }

    pub fn generate(self) -> TaskFuture<C::Output> {
        let fut = async move {
            let stream = FutureTaskStream::new(
                self.init_state,
                self.max_retry_times,
                self.delay_milliseconds,
                self.event_handle.clone(),
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
            let mut sink = FutureTaskSink::new(self.collector, self.event_handle);
            sink.send_all(&mut buffered_stream).await?;
            let collector = sink.into_inner();
            collector.finish().map_err(TaskError::CollectorError)
        }
        .boxed();

        TaskFuture::new(fut)
    }
}

pub struct AndThenGenerator<G, C, S, M> {
    g1: G,
    buffer_size: usize,
    max_retry_times: u64,
    delay_milliseconds: u64,
    collector: C,
    init_state_map: M,
    event_handle: Arc<dyn TaskEventHandle>,
    init_state: PhantomData<S>,
}

impl<G, C, S, M> Generator for AndThenGenerator<G, C, S, M>
where
    G: Generator + 'static,
    S: TaskState + 'static,
    C: TaskResultCollector<S::Item> + 'static,
    M: FnOnce(G::Output) -> Result<S> + Send + 'static,
{
    type State = S;
    type Output = C::Output;

    fn generate(self) -> TaskFuture<Self::Output> {
        let Self {
            g1,
            buffer_size,
            max_retry_times,
            delay_milliseconds,
            collector,
            init_state_map,
            event_handle,
            init_state: _,
        } = self;
        let first_task = g1.generate();
        let then_fut = first_task
            .and_then(|output| async move {
                (init_state_map)(output).map_err(TaskError::CollectorError)
            })
            .and_then(move |init_state| {
                TaskGenerator::new(
                    init_state,
                    buffer_size,
                    max_retry_times,
                    delay_milliseconds,
                    collector,
                    event_handle,
                )
                .generate()
            })
            .boxed();
        TaskFuture::new(then_fut)
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
    async fn test_task_stream() {
        let max = 100;
        let config = MockTestConfig::new_with_max(max);
        let mock_state = MockTaskState::new(config);
        let task = FutureTaskStream::new(mock_state, 0, 0);
        let results = task.buffered(10).collect::<Vec<_>>().await;
        assert_eq!(results.len() as u64, max);
    }

    #[stest::test]
    async fn test_counter_collector() {
        let max = 100;
        let config = MockTestConfig::new_with_max(max);
        let mock_state = MockTaskState::new(config);
        let result = TaskGenerator::new(mock_state.clone(), 10, 0, 0, CounterCollector::new())
            .generate()
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), max);
    }

    #[stest::test]
    async fn test_stream_task_batch() {
        let max = 100;
        for batch in 1..max {
            let config = MockTestConfig::new_with_batch(max, batch);
            let mock_state = MockTaskState::new(config);
            let result = TaskGenerator::new(mock_state.clone(), 10, 0, 0, CounterCollector::new())
                .generate()
                .await;
            assert!(result.is_ok(), "assert test batch {} fail.", batch);
            assert_eq!(result.unwrap(), max, "test batch {} fail.", batch);
        }
    }

    #[stest::test]
    async fn test_vec_collector() {
        let max = 100;
        let config = MockTestConfig::new_with_max(max);
        let mock_state = MockTaskState::new(config);
        let result = TaskGenerator::new(mock_state, 10, 0, 0, vec![])
            .generate()
            .await;
        //println!("{:?}", result);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len() as u64, max);
    }

    #[stest::test]
    async fn test_task_cancel() {
        let max = 100;
        let delay_time = 10;
        let config = MockTestConfig::new_with_delay(max, delay_time);
        let mock_state = MockTaskState::new(config);
        let counter = Arc::new(AtomicU64::new(0));
        let fut = TaskGenerator::new(
            mock_state.clone(),
            5,
            0,
            0,
            CounterCollector::new_with_counter(counter.clone()),
        )
        .generate();
        let (fut, task_handle) = fut.with_handle();
        let join_handle = async_std::task::spawn(fut);
        Delay::new(Duration::from_millis(delay_time * 5)).await;
        assert_eq!(task_handle.is_done(), false);
        task_handle.cancel();
        let result = join_handle.await;
        assert!(result.is_err());

        assert_eq!(task_handle.is_done(), true);

        let task_err = result.err().unwrap();
        assert!(task_err.is_canceled());
        let processed_messages = counter.load(Ordering::SeqCst);
        debug!("processed_messages before cancel: {}", processed_messages);
        assert!(processed_messages > 0 && processed_messages < max);
    }

    #[stest::test]
    async fn test_task_retry() {
        let max = 100;
        let max_retry_times = 5;
        let config = MockTestConfig::new_with_error(max, max_retry_times - 1);
        let mock_state = MockTaskState::new(config);
        let fut = TaskGenerator::new(
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
    async fn test_task_retry_fail() {
        let max = 100;
        let max_retry_times = 5;
        let counter = Arc::new(AtomicU64::new(0));

        let config = MockTestConfig::new_with_error(max, max_retry_times);
        let mock_state = MockTaskState::new(config);
        let fut = TaskGenerator::new(
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

        let fut = TaskGenerator::new(
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
