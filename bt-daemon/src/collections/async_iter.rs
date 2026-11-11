// TODO: Delete this file and migrate callers to the standard async iterator
// interfaces once AsyncIterator and IntoAsyncIterator are available in stable Rust.
//! Stable-toolchain subset of the proposed standard async iteration traits.
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) trait AsyncIterator {
    type Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
}

pub(crate) trait IntoAsyncIterator {
    type Item;
    type IntoAsyncIter: AsyncIterator<Item = Self::Item>;
    fn into_async_iter(self) -> Self::IntoAsyncIter;
}

pub(crate) trait AsyncIteratorExt: AsyncIterator + Unpin {
    fn next(&mut self) -> Next<'_, Self>
    where
        Self: Sized,
    {
        Next(self)
    }
}
impl<T: AsyncIterator + Unpin> AsyncIteratorExt for T {}

pub(crate) struct Next<'a, T>(&'a mut T);
impl<T: AsyncIterator + Unpin> Future for Next<'_, T> {
    type Output = Option<T::Item>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.get_mut().0).poll_next(cx)
    }
}
