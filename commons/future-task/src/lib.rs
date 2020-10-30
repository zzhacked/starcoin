// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Error, Result};
use futures::future::BoxFuture;
use futures::task::{Context, Poll};
use futures::{
    future::{abortable, AbortHandle},
    ready, Future, FutureExt, Sink, SinkExt, Stream, StreamExt, TryFuture, TryStream, TryStreamExt,
};
use futures_retry::{ErrorHandler, FutureFactory, FutureRetry, RetryPolicy};
use log::debug;
use pin_project::pin_project;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

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

#[derive(Clone)]
pub struct TaskErrorHandle {
    max_retry_times: u64,
    delay_milliseconds: u64,
}

impl TaskErrorHandle {
    pub fn new(max_retry_times: u64, delay_milliseconds: u64) -> Self {
        Self {
            max_retry_times,
            delay_milliseconds,
        }
    }
}

impl ErrorHandler<anyhow::Error> for TaskErrorHandle {
    type OutError = anyhow::Error;

    fn handle(&mut self, attempt: usize, error: Error) -> RetryPolicy<Self::OutError> {
        match error.downcast::<TaskError>() {
            Ok(task_err) => match task_err {
                TaskError::BreakError(e) => {
                    RetryPolicy::ForwardError(TaskError::BreakError(e).into())
                }
                TaskError::RetryLimitReached(attempt, error) => RetryPolicy::ForwardError(
                    TaskError::RetryLimitReached(attempt + 1, error).into(),
                ),
                TaskError::Canceled => RetryPolicy::ForwardError(TaskError::Canceled.into()),
                TaskError::CollectorError(e) => {
                    RetryPolicy::ForwardError(TaskError::CollectorError(e).into())
                }
            },
            Err(e) => {
                debug!("Task error: {:?}, attempt: {}", e, attempt);
                if attempt as u64 > self.max_retry_times {
                    RetryPolicy::ForwardError(TaskError::RetryLimitReached(attempt, e).into())
                } else if self.delay_milliseconds == 0 {
                    RetryPolicy::Repeat
                } else {
                    RetryPolicy::WaitRetry(Duration::from_millis(
                        self.delay_milliseconds * attempt as u64,
                    ))
                }
            }
        }
    }
}

pub trait TaskResultCollector<Item>: std::marker::Send + Unpin {
    type Output;

    fn collect(self: Pin<&mut Self>, item: Item) -> Result<()>;
    fn finish(self) -> Result<Self::Output>;
}

impl<Item, F> TaskResultCollector<Item> for F
where
    F: FnMut(Item) -> Result<()>,
    F: std::marker::Send + Unpin,
{
    type Output = ();

    fn collect(self: Pin<&mut Self>, item: Item) -> Result<()> {
        self.get_mut()(item)
    }

    fn finish(self) -> Result<Self::Output> {
        Ok(())
    }
}

impl<Item> TaskResultCollector<Item> for Vec<Item>
where
    Item: std::marker::Send + Unpin,
{
    type Output = Self;

    fn collect(self: Pin<&mut Self>, item: Item) -> Result<()> {
        self.get_mut().push(item);
        Ok(())
    }

    fn finish(self) -> Result<Self::Output> {
        Ok(self)
    }
}

#[derive(Clone, Default)]
pub struct CounterCollector {
    counter: Arc<AtomicU64>,
}

impl CounterCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_counter(counter: Arc<AtomicU64>) -> Self {
        Self { counter }
    }
}

impl<Item> TaskResultCollector<Item> for CounterCollector
where
    Item: std::marker::Send + Unpin,
{
    type Output = u64;

    fn collect(self: Pin<&mut Self>, _item: Item) -> Result<(), Error> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn finish(self) -> Result<Self::Output> {
        Ok(self.counter.load(Ordering::SeqCst))
    }
}

pub trait TaskState: Sized + Clone + std::marker::Unpin + std::marker::Send {
    type Item: Debug + std::marker::Send;

    fn new_sub_task(self) -> BoxFuture<'static, Result<Self::Item>>;
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

    pub fn generate(self) -> (BoxFuture<'static, Result<C::Output>>, TaskHandle) {
        let fut = async move {
            let stream = FutureTaskStream::new(
                self.init_state,
                self.max_retry_times,
                self.delay_milliseconds,
            );
            let mut buffered_stream = stream.buffered(self.buffer_size);
            let mut sink = FutureTaskSink::new(self.collector);
            sink.send_all(&mut buffered_stream).await?;
            let collector = sink.into_inner();
            collector.finish()
        };
        let (abortable_fut, handle) = abortable(fut);
        (
            abortable_fut
                .map(|result| match result {
                    Ok(result) => result,
                    Err(_aborted) => Err(TaskError::Canceled.into()),
                })
                .boxed(),
            TaskHandle::new(handle),
        )
    }
}

#[pin_project]
pub struct FutureTaskStream<S>
where
    S: TaskState,
{
    max_retry_times: u64,
    delay_milliseconds: u64,
    //#[pin]
    state: Option<S>,
}

impl<S> FutureTaskStream<S>
where
    S: TaskState,
{
    pub fn new(state: S, max_retry_times: u64, delay_milliseconds: u64) -> Self {
        Self {
            max_retry_times,
            delay_milliseconds,
            state: Some(state),
        }
    }
}

impl<S> Stream for FutureTaskStream<S>
where
    S: TaskState + 'static,
{
    type Item = BoxFuture<'static, Result<S::Item>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.state {
            Some(state) => {
                let error_action =
                    TaskErrorHandle::new(*this.max_retry_times, *this.delay_milliseconds);
                let state_to_factory = state.clone();

                let fut = async move {
                    let retry_fut = FutureRetry::new(
                        move || -> Self::Item { state_to_factory.clone().new_sub_task() },
                        error_action,
                    );
                    retry_fut
                        .map(|result| match result {
                            Ok((item, _attempt)) => Ok(item),
                            Err((e, _attempt)) => Err(e),
                        })
                        .await
                }
                .boxed();
                *this.state = state.next();
                Poll::Ready(Some(fut))
            }
            None => Poll::Ready(None),
        }
    }
}

#[pin_project]
pub struct FutureTaskSink<C> {
    #[pin]
    collector: C,
}

impl<C> FutureTaskSink<C> {
    pub fn new<Item>(collector: C) -> Self
    where
        C: TaskResultCollector<Item>,
    {
        Self { collector }
    }

    pub fn into_inner(self) -> C {
        self.collector
    }
}

impl<C, Item> Sink<Item> for FutureTaskSink<C>
where
    C: TaskResultCollector<Item>,
{
    type Error = anyhow::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let this = self.project();
        this.collector
            .collect(item)
            .map_err(|e| TaskError::CollectorError(e).into())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::format_err;
    use futures::future::BoxFuture;
    use futures::task::{Context, Poll};
    use futures::{Future, FutureExt, StreamExt, TryFuture};
    use futures_timer::Delay;
    use pin_utils::core_reexport::time::Duration;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct MockTaskState {
        counter: u64,
        max: u64,
        delay_time: u64,
        error_per_task: u64,
        break_at: Option<u64>,
        error_times: Arc<Mutex<HashMap<u64, AtomicU64>>>,
    }

    impl MockTaskState {
        pub fn new(max: u64) -> Self {
            Self {
                counter: 0,
                max,
                delay_time: 10,
                error_per_task: 0,
                break_at: None,
                error_times: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub fn new_with(max: u64, delay_time: u64, error_per_task: u64) -> Self {
            Self {
                counter: 0,
                max,
                delay_time,
                error_per_task,
                break_at: None,
                error_times: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub fn new_with_break(
            max: u64,
            delay_time: u64,
            error_per_task: u64,
            break_at: u64,
        ) -> Self {
            Self {
                counter: 0,
                max,
                delay_time,
                error_per_task,
                break_at: Some(break_at),
                error_times: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl TaskState for MockTaskState {
        type Item = u64;

        fn new_sub_task(self) -> BoxFuture<'static, Result<Self::Item>> {
            async move {
                if let Some(break_at) = self.break_at {
                    if self.counter == break_at {
                        return Err(TaskError::BreakError(format_err!(
                            "Break error at: {}",
                            self.counter
                        ))
                        .into());
                    }
                }
                if self.delay_time > 0 {
                    Delay::new(Duration::from_millis(self.delay_time)).await;
                }
                if self.error_per_task > 0 {
                    let mut error_times = self.error_times.lock().unwrap();
                    let current_state_error_counter =
                        error_times.entry(self.counter).or_insert(AtomicU64::new(0));
                    let current_state_error_times =
                        current_state_error_counter.fetch_add(1, Ordering::Relaxed);
                    if current_state_error_times <= self.error_per_task {
                        return Err(format_err!(
                            "return error for state: {}, error_times: {}",
                            self.counter,
                            current_state_error_times
                        ));
                    }
                }
                Ok(self.counter * 2)
            }
            .boxed()
        }

        fn next(&self) -> Option<Self> {
            let next = self.counter + 1;
            if next >= self.max {
                None
            } else {
                Some(MockTaskState {
                    counter: next,
                    max: self.max,
                    delay_time: self.delay_time,
                    error_per_task: self.error_per_task,
                    break_at: self.break_at.clone(),
                    error_times: self.error_times.clone(),
                })
            }
        }
    }

    #[stest::test]
    async fn test_future_task_stream() {
        let max = 100;
        let mock_state = MockTaskState::new(max);
        let task = FutureTaskStream::new(mock_state, 0, 0);
        let results = task.buffered(10).collect::<Vec<_>>().await;
        assert_eq!(results.len() as u64, max);
    }

    #[stest::test]
    async fn test_future_task_counter() {
        let max = 100;
        let mock_state = MockTaskState::new(max);
        let result = TaskGenerator::new(mock_state.clone(), 10, 0, 0, CounterCollector::new())
            .generate()
            .0
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), max);
    }
    #[stest::test]
    async fn test_future_task_vec_collector() {
        let max = 100;
        let mock_state = MockTaskState::new(max);
        let result = TaskGenerator::new(mock_state, 10, 0, 0, vec![])
            .generate()
            .0
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len() as u64, max);
    }

    #[stest::test]
    async fn test_future_task_cancel() {
        let max = 100;
        let mock_state = MockTaskState::new_with(max, 100, 0);
        let counter = Arc::new(AtomicU64::new(0));
        let (fut, cancel_handle) = TaskGenerator::new(
            mock_state.clone(),
            10,
            0,
            0,
            CounterCollector::new_with_counter(counter.clone()),
        )
        .generate();
        let join_handle = async_std::task::spawn(fut);
        Delay::new(Duration::from_millis(200)).await;
        cancel_handle.cancel();
        let result = join_handle.await;
        assert!(result.is_err());
        let task_err = result.err().unwrap().downcast::<TaskError>().unwrap();
        assert!(task_err.is_canceled());
        let processed_messages = counter.load(Ordering::SeqCst);
        debug!("processed_messages before cancel: {}", processed_messages);
        assert!(processed_messages > 0 && processed_messages < max);
    }

    #[stest::test]
    async fn test_future_task_retry() {
        let max = 100;
        let max_retry_times = 5;
        let mock_state = MockTaskState::new_with(max, 0, max_retry_times - 1);
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
        let mock_state = MockTaskState::new_with(max, 0, max_retry_times);
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
        let task_err = result.err().unwrap().downcast::<TaskError>().unwrap();
        assert!(task_err.is_retry_limit_reached());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[stest::test]
    async fn test_collector_error() {
        let max = 100;
        let mock_state = MockTaskState::new(max);
        let result = TaskGenerator::new(mock_state, 10, 0, 0, |item| {
            //println!("collect error for: {:?}", item);
            Err(format_err!("collect error for: {:?}", item))
        })
        .generate()
        .0
        .await;
        assert!(result.is_err());
        let task_err = result.err().unwrap().downcast::<TaskError>().unwrap();
        assert!(task_err.is_collector_error());
    }

    #[stest::test]
    async fn test_break_error() {
        let max = 100;
        let break_at = 31;
        let max_retry_times = 5;
        let counter = Arc::new(AtomicU64::new(0));
        let mock_state = MockTaskState::new_with_break(max, 0, 1, break_at);
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
        let task_err = result.err().unwrap().downcast::<TaskError>().unwrap();
        assert!(task_err.is_break_error());
        assert_eq!(break_at, counter.load(Ordering::SeqCst));
    }
}
