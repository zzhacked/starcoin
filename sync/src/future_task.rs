use anyhow::Result;
use futures::future::BoxFuture;
use futures::task::{Context, Poll};
use futures::{ready, Future, Stream, TryFuture, TryStream, TryStreamExt};
use futures_retry::{ErrorHandler, FutureFactory, FutureRetry};
use futures_timer::Delay;
use pin_project::pin_project;
use pin_utils::core_reexport::marker::PhantomData;
use std::pin::Pin;

pub enum TaskError {
    TaskFail(anyhow::Error),
    Cancel,
}

pub struct TaskErrorHandle{
}

impl

pub trait TaskState: Sized + Clone + std::marker::Unpin {
    type FutureItem: TryFuture;

    fn next(&self) -> Option<Self>;
}

pub trait TaskGenerator<S>
where
    S: TaskState,
{
    fn new(state: &S) -> S::FutureItem;
}

pub struct StateFutureFactory<S, G>
where
    S: TaskState,
    G: TaskGenerator<S>,
{
    state: S,
    generator: PhantomData<G>,
}

impl<S, G> StateFutureFactory<S, G>
where
    S: TaskState,
    G: TaskGenerator<S>,
{
    pub fn new(state: S) -> Self {
        Self {
            state,
            generator: PhantomData,
        }
    }
}

impl<S, G> FutureFactory for StateFutureFactory<S, G>
where
    S: TaskState,
    G: TaskGenerator<S>,
{
    type FutureItem = S::FutureItem;

    fn new(&mut self) -> Self::FutureItem {
        G::new(&self.state)
    }
}

#[pin_project]
enum FutureTaskState<F> {
    NotStarted,
    WaitingForFuture(#[pin] F),
}

#[pin_project]
pub struct FutureTask<G, R, S>
where
    G: TaskGenerator<S>,
    S: TaskState,
{
    generator: PhantomData<G>,
    action: R,
    //#[pin]
    state: Option<S>,
}

impl<G, R, S> FutureTask<G, R, S>
where
    G: TaskGenerator<S>,
    R: ErrorHandler<<S::FutureItem as TryFuture>::Error> + Clone,
    S: TaskState,
{
    pub fn new(action: R, state: S) -> Self {
        Self {
            generator: PhantomData,
            action,
            state: Some(state),
            //task_state: FutureTaskState::NotStarted,
        }
    }
}

impl<G, R, S> Stream for FutureTask<G, R, S>
where
    G: TaskGenerator<S>,
    R: ErrorHandler<<S::FutureItem as TryFuture>::Error> + Clone,
    S: TaskState,
{
    type Item = FutureRetry<StateFutureFactory<S, G>, R>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let action = this.action.clone();
        //let state = this.state.project();
        match this.state {
            Some(state) => {
                let fut = FutureRetry::new(StateFutureFactory::<S, G>::new(state.clone()), action);
                //this.state.set(state.next());
                *this.state = state.next();
                return Poll::Ready(Some(fut));
                //state.next()
            }
            None => return Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::future_task::TaskGenerator;

    struct MockTaskGenerator {}

    #[stest::test]
    fn test_future_task() {

    }
}
