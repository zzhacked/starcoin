// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{TaskError, TaskState};
use anyhow::{Error, Result};
use futures::{
    future::BoxFuture,
    task::{Context, Poll},
    FutureExt, Stream,
};
use futures_retry::{ErrorHandler, FutureRetry, RetryPolicy};
use log::debug;
use pin_project::pin_project;
use std::pin::Pin;
use std::time::Duration;

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
    type OutError = TaskError;

    fn handle(&mut self, attempt: usize, error: Error) -> RetryPolicy<Self::OutError> {
        match error.downcast::<TaskError>() {
            Ok(task_err) => match task_err {
                TaskError::BreakError(e) => RetryPolicy::ForwardError(TaskError::BreakError(e)),
                TaskError::RetryLimitReached(attempt, error) => {
                    RetryPolicy::ForwardError(TaskError::RetryLimitReached(attempt + 1, error))
                }
                TaskError::Canceled => RetryPolicy::ForwardError(TaskError::Canceled),
                TaskError::CollectorError(e) => {
                    RetryPolicy::ForwardError(TaskError::CollectorError(e))
                }
            },
            Err(e) => {
                debug!("Task error: {:?}, attempt: {}", e, attempt);
                if attempt as u64 > self.max_retry_times {
                    RetryPolicy::ForwardError(TaskError::RetryLimitReached(attempt, e))
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

#[pin_project]
pub struct FutureTaskStream<S>
where
    S: TaskState,
{
    max_retry_times: u64,
    delay_milliseconds: u64,
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
    type Item = BoxFuture<'static, Result<Vec<S::Item>, TaskError>>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.state {
            Some(state) => {
                let error_action =
                    TaskErrorHandle::new(*this.max_retry_times, *this.delay_milliseconds);
                let state_to_factory = state.clone();

                let fut = async move {
                    let retry_fut = FutureRetry::new(
                        move || state_to_factory.clone().new_sub_task(),
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
