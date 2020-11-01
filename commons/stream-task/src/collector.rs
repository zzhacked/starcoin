// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::TaskError;
use anyhow::{Error, Result};
use futures::task::{Context, Poll};
use futures::Sink;
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub trait TaskResultCollector<Item>: std::marker::Send + Unpin {
    type Output: std::marker::Send;

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
    type Error = TaskError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let this = self.project();
        this.collector
            .collect(item)
            .map_err(TaskError::CollectorError)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
