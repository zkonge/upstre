use std::{
    future::{ready, Future, Ready},
    marker::PhantomData,
    sync::{Arc, Weak},
    time::Duration,
};

pub use arc_swap::{ArcSwap, Guard};
use futures_util::{Stream, StreamExt};
use thiserror::Error;
use tokio::{task::AbortHandle, time::sleep};

const DEFAULT_RETRY_GAP: Duration = Duration::from_secs(1);

/// A consistant type wrapper for [`Option<Result<T, E>>`] in [`Stream`]
#[derive(Error, Debug)]
pub enum Error<E> {
    #[error(transparent)]
    Error(E),
    #[error("end of stream")]
    EndOfStream,
}

/// Define the retry logic while [`Stream`] ended.
#[non_exhaustive]
pub enum RetryPolicy {
    /// rebuild the stream
    Rebuild,
    /// continue polling the stream
    Continue,
}

/// Stores the latest value of a stream, with retry logic.
pub struct Upstre<T: Send + Sync + 'static> {
    place: Arc<ArcSwap<T>>,
    aborter: AbortHandle,
}

impl<T: Send + Sync + 'static> Upstre<T> {
    pub fn value(&self) -> Guard<Arc<T>> {
        self.place.load()
    }

    pub fn container(&self) -> Weak<ArcSwap<T>> {
        Arc::downgrade(&self.place)
    }
}

impl<T: Send + Sync + 'static> Drop for Upstre<T> {
    fn drop(&mut self) {
        self.aborter.abort();
    }
}

pub struct UpstreBuilder<EH, E, EHFut>
where
    EH: Fn(Error<E>) -> EHFut + Send + 'static,
    E: Send,
    EHFut: Future<Output = ()> + Send,
{
    error_handler: EH,
    retry_policy: RetryPolicy,
    sleep: Duration,
    _p: PhantomData<(E, EHFut)>,
}

impl<EH, E, EHFut> UpstreBuilder<EH, E, EHFut>
where
    EH: Fn(Error<E>) -> EHFut + Send + 'static,
    E: Send,
    EHFut: Future<Output = ()> + Send,
{
    /// A callback that receives errors while [`Stream`] ended and decide the [`RetryPolicy`].
    pub fn new(error_handler: EH) -> Self {
        Self {
            error_handler,
            retry_policy: RetryPolicy::Rebuild,
            sleep: DEFAULT_RETRY_GAP,
            _p: PhantomData,
        }
    }

    pub fn retry_policy(self, retry_policy: RetryPolicy) -> Self {
        Self {
            retry_policy,
            ..self
        }
    }

    pub fn sleep(self, dur: Duration) -> Self {
        Self { sleep: dur, ..self }
    }

    pub async fn build<F, Fut, S, T>(self, stream_maker: F) -> Result<Upstre<T>, Error<E>>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: Stream<Item = T> + Send + 'static,
        T: Send + Sync + 'static,
    {
        let mut initial_stream = Box::pin(stream_maker().await.map_err(Error::Error)?);
        let initial_value = initial_stream.next().await.ok_or(Error::EndOfStream)?;

        let place = Arc::new(ArcSwap::from_pointee(initial_value));
        let place_cloned = place.clone();

        let task = async move {
            let mut stream = initial_stream;

            loop {
                if let Some(v) = stream.next().await {
                    place_cloned.store(Arc::new(v));
                    continue;
                }

                // call error handler, usually for logging
                (self.error_handler)(Error::EndOfStream).await;

                // prevent busy loop
                sleep(self.sleep).await;

                match self.retry_policy {
                    RetryPolicy::Rebuild => (),
                    // continue polling the stream, not quite useful since
                    // the stream is already ended, but if the stream
                    // could be resumed (like gRPC), it's useful.
                    RetryPolicy::Continue => continue,
                }

                let new_stream = loop {
                    match stream_maker().await {
                        Ok(s) => break s,
                        Err(e) => {
                            (self.error_handler)(Error::Error(e)).await;
                            sleep(self.sleep).await;
                        }
                    }
                };

                stream = Box::pin(new_stream);
            }
        };

        let task = tokio::spawn(task);

        Ok(Upstre {
            place,
            aborter: task.abort_handle(),
        })
    }
}

impl<E> Default for UpstreBuilder<fn(Error<E>) -> Ready<()>, E, Ready<()>>
where
    E: Send,
{
    fn default() -> Self {
        Self {
            error_handler: |_| ready(()),
            retry_policy: RetryPolicy::Rebuild,
            sleep: DEFAULT_RETRY_GAP,
            _p: PhantomData,
        }
    }
}
