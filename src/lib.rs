#![forbid(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms, unreachable_pub)]
#![warn(clippy::all, clippy::pedantic, clippy::dbg_macro, clippy::todo)]
#![allow(clippy::module_name_repetitions)]

//! Keep the latest successful value from a stream in a shared, lock-free container.
//!
//! `Upstre` continuously consumes a stream and stores its latest yielded value in
//! [`ArcSwap`], making reads cheap and wait-free for read-heavy workloads.
//!
//! If the stream ends or returns an error, it notifies a completion handler,
//! sleeps for a configurable interval, and recreates the stream.

use std::{
    future::{ready, Future, Ready},
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};

use arc_swap::ArcSwap;
pub use arc_swap::Guard;
use futures_util::{Stream, TryStreamExt};
use thiserror::Error;
use tokio::{task::AbortHandle, time::sleep};

const DEFAULT_RETRY_GAP: Duration = Duration::from_secs(1);

/// Errors returned by [`UpstreBuilder::build`] and reported by callbacks.
///
/// This enum unifies stream item errors and stream termination into one type so
/// callers can handle them consistently.
#[derive(Error, Debug)]
pub enum UpstreError<E> {
    /// The wrapped stream-related error.
    #[error(transparent)]
    Stream(E),

    /// The stream ended without yielding further items.
    #[error("end of stream")]
    EndOfStream,
}

/// Storing the fresh value of a stream.
///
/// Each time the stream yields, ends or raises an error, the complete handler will be called and
/// the stream will be recreated.
///
/// [`Upstre`] is cheap to clone, and the clones will share the same value.
/// When all clones are dropped, the stream will be aborted.
#[derive(Debug)]
pub struct Upstre<T: Send + Sync + 'static> {
    place: Arc<ArcSwap<T>>,
    aborter: Arc<AbortOnDrop>,
}

impl<T: Send + Sync + 'static> Upstre<T> {
    /// Get the last value of the stream.
    #[must_use]
    pub fn value(&self) -> Guard<Arc<T>> {
        self.place.load()
    }
}

// manually implement `Clone` to allow `!Clone` T types.
impl<T: Send + Sync + 'static> Clone for Upstre<T> {
    fn clone(&self) -> Self {
        Self {
            place: self.place.clone(),
            aborter: self.aborter.clone(),
        }
    }
}

/// Builder for creating and running an [`Upstre`].
///
/// It controls how often stream recreation is retried and allows attaching an
/// async completion handler for values and terminal events.
pub struct UpstreBuilder<CH, T, E, CHFut>
where
    CH: Fn(Result<Arc<T>, UpstreError<E>>) -> CHFut + Send + 'static,
    T: Send + Sync + 'static,
    E: Send,
    CHFut: Future<Output = ()> + Send,
{
    complete_handler: CH,
    sleep: Duration,
    _p: PhantomData<(T, E, CHFut)>,
}

impl<CH, T, E, CHFut> UpstreBuilder<CH, T, E, CHFut>
where
    CH: Fn(Result<Arc<T>, UpstreError<E>>) -> CHFut + Send + 'static,
    T: Send + Sync + 'static,
    E: Send,
    CHFut: Future<Output = ()> + Send,
{
    /// Create a builder with a completion handler.
    ///
    /// The handler is called when:
    /// - a new value is produced (`Ok(Arc<T>)`),
    /// - the stream ends (`Err(UpstreError::EndOfStream)`),
    /// - the stream or stream creation fails (`Err(UpstreError::Stream(_))`).
    #[must_use]
    pub fn new(complete_handler: CH) -> Self {
        Self {
            complete_handler,
            sleep: DEFAULT_RETRY_GAP,
            _p: PhantomData,
        }
    }

    /// Set the sleep duration between retry attempts.
    #[must_use]
    pub fn sleep(self, dur: Duration) -> Self {
        Self { sleep: dur, ..self }
    }

    /// Build the [`Upstre`].
    ///
    /// This method waits for the first stream item to initialize storage. If the
    /// first stream is empty, it returns [`UpstreError::EndOfStream`].
    ///
    /// # Errors
    ///
    /// Returns [`UpstreError::Stream`] if stream creation or polling fails.
    /// Returns [`UpstreError::EndOfStream`] if the stream yields no item.
    ///
    /// ```rust
    /// use std::{
    ///     convert::Infallible,
    ///     future::{ready, Ready},
    /// };
    ///
    /// use futures_util::stream::{once, Once};
    ///
    /// use upstre::UpstreBuilder;
    ///
    /// async fn make_stream() -> Result<Once<Ready<Result<(), Infallible>>>, Infallible> {
    ///     Ok(once(ready(Ok(()))))
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// let place = UpstreBuilder::default().build(make_stream).await.unwrap();
    ///
    /// assert_eq!(**place.value(), ());
    /// # })
    /// ```
    pub async fn build<F, Fut, S>(self, stream_maker: F) -> Result<Upstre<T>, UpstreError<E>>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: Stream<Item = Result<T, E>> + Send + 'static,
    {
        let mut initial_stream = Box::pin(stream_maker().await.map_err(UpstreError::Stream)?);
        let initial_value = initial_stream
            .try_next()
            .await
            .map_err(UpstreError::Stream)?
            .ok_or(UpstreError::EndOfStream)?;

        let place = Arc::new(ArcSwap::from_pointee(initial_value));
        let place_cloned = place.clone();

        let task = async move {
            let mut stream = initial_stream;

            loop {
                let e = match stream.try_next().await {
                    Ok(Some(v)) => {
                        let v = Arc::new(v);
                        place_cloned.store(v.clone());
                        (self.complete_handler)(Ok(v)).await;
                        continue;
                    }
                    Ok(None) => UpstreError::EndOfStream,
                    Err(e) => UpstreError::Stream(e),
                };

                // call complete handler, usually for logging
                (self.complete_handler)(Err(e)).await;

                // prevent busy loop
                sleep(self.sleep).await;

                stream = loop {
                    match stream_maker().await {
                        Ok(s) => break Box::pin(s),
                        Err(e) => {
                            (self.complete_handler)(Err(UpstreError::Stream(e))).await;
                            sleep(self.sleep).await;
                        }
                    }
                };
            }
        };

        let task = tokio::spawn(task);

        Ok(Upstre {
            place,
            aborter: Arc::new(AbortOnDrop(task.abort_handle())),
        })
    }
}

impl<T, E> Default
    for UpstreBuilder<fn(Result<Arc<T>, UpstreError<E>>) -> Ready<()>, T, E, Ready<()>>
where
    T: Send + Sync + 'static,
    E: Send,
{
    fn default() -> Self {
        Self {
            complete_handler: |_| ready(()),
            sleep: DEFAULT_RETRY_GAP,
            _p: PhantomData,
        }
    }
}

/// Aborts a background task when it is dropped.
#[derive(Debug)]
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
