use anyhow::{Error, Result};
use futures::future::BoxFuture;
use futures::stream::{Buffered, Forward};
use futures::task::{Context, Poll};
use futures::{
    ready, Future, FutureExt, Sink, SinkExt, Stream, StreamExt, TryFuture, TryStream, TryStreamExt,
};
use futures_retry::{ErrorHandler, FutureFactory, FutureRetry, RetryPolicy};
use futures_timer::Delay;
use pin_project::pin_project;
use pin_utils::core_reexport::marker::PhantomData;
use std::fmt::Debug;
use std::pin::Pin;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TaskError {
    /// directly break the task, do not retry.
    #[error("Task failed, cause: {0:?}")]
    Fail(anyhow::Error),
    #[error("Maximum number of retry attempts({0}) reached, last error: {1:?}")]
    RetryAttemptsReached(usize, anyhow::Error),
    #[error("Task has been cancelled.")]
    Cancel,
}

#[derive(Clone)]
pub struct TaskErrorHandle {
    max_retry_times: usize,
}

impl TaskErrorHandle {
    pub fn new(max_retry_times: usize) -> Self {
        Self { max_retry_times }
    }
}

impl ErrorHandler<anyhow::Error> for TaskErrorHandle {
    type OutError = anyhow::Error;

    fn handle(&mut self, attempt: usize, error: Error) -> RetryPolicy<Self::OutError> {
        match error.downcast::<TaskError>() {
            Ok(task_err) => match task_err {
                TaskError::Fail(e) => RetryPolicy::ForwardError(TaskError::Fail(e).into()),
                TaskError::RetryAttemptsReached(attempt, error) => RetryPolicy::ForwardError(
                    TaskError::RetryAttemptsReached(attempt + 1, error).into(),
                ),
                TaskError::Cancel => RetryPolicy::ForwardError(TaskError::Cancel.into()),
            },
            Err(e) => {
                if attempt >= self.max_retry_times {
                    RetryPolicy::ForwardError(TaskError::RetryAttemptsReached(attempt, e).into())
                } else {
                    RetryPolicy::WaitRetry(Duration::from_secs(attempt as u64))
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

pub trait TaskState: Sized + Clone + std::marker::Unpin + std::marker::Send {}

pub trait TaskGenerator: std::marker::Send + Sized {
    type State: TaskState;
    type Item: Debug + std::marker::Send;
    fn new_sub_task(state: Self::State) -> BoxFuture<'static, Result<Self::Item>>;
    fn next_state(pre_state: &Self::State) -> Option<Self::State>;
    fn new_task<C>(
        init_state: Self::State,
        buffer_size: usize,
        max_retry_times: usize,
        collector: C,
    ) -> BoxFuture<'static, Result<C::Output>>
    where
        Self: 'static,
        C: TaskResultCollector<Self::Item> + 'static,
    {
        let stream = FutureTaskStream::<Self>::new(init_state, max_retry_times);
        let mut buffered_stream = stream.buffered(buffer_size).map(|result| match result {
            Ok((item, repeat)) => Ok(item),
            Err((e, repeat)) => Err(e),
        });

        async move {
            let mut sink = FutureTaskSink::new(collector);
            sink.send_all(&mut buffered_stream).await?;
            let collector = sink.into_inner();
            collector.finish()
        }
        .boxed()
    }
}

pub struct StateFutureFactory<G>
where
    G: TaskGenerator,
{
    state: G::State,
    generator: PhantomData<G>,
}

impl<G> StateFutureFactory<G>
where
    G: TaskGenerator,
{
    pub fn new(state: G::State) -> Self {
        Self {
            state,
            generator: PhantomData,
        }
    }
}

impl<G> FutureFactory for StateFutureFactory<G>
where
    G: TaskGenerator,
{
    type FutureItem = BoxFuture<'static, Result<G::Item>>;

    fn new(&mut self) -> Self::FutureItem {
        G::new_sub_task(self.state.clone())
    }
}

#[pin_project]
enum FutureTaskState<F> {
    NotStarted,
    WaitingForFuture(#[pin] F),
}

#[pin_project]
pub struct FutureTaskStream<G>
where
    G: TaskGenerator,
{
    generator: PhantomData<G>,
    action: TaskErrorHandle,
    //#[pin]
    state: Option<G::State>,
}

impl<G> FutureTaskStream<G>
where
    G: TaskGenerator,
{
    pub fn new(state: G::State, max_retry_times: usize) -> Self {
        Self {
            generator: PhantomData,
            action: TaskErrorHandle::new(max_retry_times),
            state: Some(state),
            //task_state: FutureTaskState::NotStarted,
        }
    }
}

impl<G> Stream for FutureTaskStream<G>
where
    G: TaskGenerator,
{
    type Item = FutureRetry<StateFutureFactory<G>, TaskErrorHandle>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let action = this.action.clone();
        //let state = this.state.project();
        match this.state {
            Some(state) => {
                let fut = FutureRetry::new(StateFutureFactory::<G>::new(state.clone()), action);
                //this.state.set(state.next());
                *this.state = G::next_state(state);
                return Poll::Ready(Some(fut));
                //state.next()
            }
            None => return Poll::Ready(None),
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

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let this = self.project();
        this.collector.collect(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::future_task::{TaskGenerator, TaskState};
    use futures::future::BoxFuture;
    use futures::task::{Context, Poll};
    use futures::{Future, FutureExt, StreamExt, TryFuture};
    use pin_utils::core_reexport::time::Duration;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct MockTaskState {
        counter: u64,
        max: u64,
    }

    impl MockTaskState {
        pub fn new(max: u64) -> Self {
            Self { counter: 0, max }
        }
    }

    impl TaskState for MockTaskState {}

    struct MockTaskGenerator {}

    impl TaskGenerator for MockTaskGenerator {
        type State = MockTaskState;
        type Item = u64;

        fn new_sub_task(state: MockTaskState) -> BoxFuture<'static, Result<Self::Item>> {
            async move {
                Delay::new(Duration::from_millis(10)).await;
                Ok(state.counter * 2)
            }
            .boxed()
        }

        fn next_state(pre_state: &Self::State) -> Option<Self::State> {
            let next = pre_state.counter + 1;
            if next >= pre_state.max {
                None
            } else {
                Some(MockTaskState {
                    counter: next,
                    max: pre_state.max,
                })
            }
        }
    }

    #[stest::test]
    async fn test_future_task_stream() {
        let mock_state = MockTaskState::new(100);
        let task = FutureTaskStream::<MockTaskGenerator>::new(mock_state, 3);
        task.buffered(10)
            .for_each(|result| async move { println!("{:?}", result) })
            .await;
    }

    #[stest::test]
    async fn test_future_task() {
        let mock_state = MockTaskState::new(100);
        let result = MockTaskGenerator::new_task(mock_state.clone(), 10, 5, |num| {
            println!("collect num:{}", num);
            Ok(())
        })
        .await;
        assert!(result.is_ok());

        let vec_collector: Vec<u64> = vec![];
        let result = MockTaskGenerator::new_task(mock_state, 10, 5, vec_collector).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 100);
    }
}
