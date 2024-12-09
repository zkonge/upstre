use std::{
    future::{ready, Future, Ready},
    marker::PhantomData,
    sync::{Arc, Weak},
    time::Duration,
};

pub use arc_swap::{ArcSwap, Guard};
use futures_util::{Stream, TryStreamExt};
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

/// Storing the fresh value of a stream.
///
/// Each time the stream ends or raises an error, the error handler will be called and
/// the stream will be recreated.
#[derive(Debug)]
pub struct Upstre<T: Send + Sync + 'static> {
    place: Arc<ArcSwap<T>>,
    aborter: AbortHandle,
}

impl<T: Send + Sync + 'static> Upstre<T> {
    /// Get the last value of the stream.
    pub fn value(&self) -> Guard<Arc<T>> {
        self.place.load()
    }

    /// Get the container of the last value, preventing the the unnecessary [`Arc`] wrapper.
    ///
    /// The [`Arc<Upstre<T>>`] needs 3 dereferences to get the value,
    /// while the [`Weak<ArcSwap<T>>`] one time less.
    ///
    /// But in such case, the [`Weak`] reference is not guaranteed to be valid.
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
    sleep: Duration,
    _p: PhantomData<(E, EHFut)>,
}

impl<EH, E, EHFut> UpstreBuilder<EH, E, EHFut>
where
    EH: Fn(Error<E>) -> EHFut + Send + 'static,
    E: Send,
    EHFut: Future<Output = ()> + Send,
{
    /// The error handler receives errors while [`Stream`] raises error or ends.
    pub fn new(error_handler: EH) -> Self {
        Self {
            error_handler,
            sleep: DEFAULT_RETRY_GAP,
            _p: PhantomData,
        }
    }

    /// Set the sleep duration between retries.
    pub fn sleep(self, dur: Duration) -> Self {
        Self { sleep: dur, ..self }
    }

    /// Build the [`Upstre`].
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
    pub async fn build<F, Fut, S, T>(self, stream_maker: F) -> Result<Upstre<T>, Error<E>>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: Stream<Item = Result<T, E>> + Send + 'static,
        T: Send + Sync + 'static,
    {
        let mut initial_stream = Box::pin(stream_maker().await.map_err(Error::Error)?);
        let initial_value = initial_stream
            .try_next()
            .await
            .map_err(Error::Error)?
            .ok_or(Error::EndOfStream)?;

        let place = Arc::new(ArcSwap::from_pointee(initial_value));
        let place_cloned = place.clone();

        let task = async move {
            let mut stream = initial_stream;

            loop {
                let e = match stream.try_next().await {
                    Ok(Some(v)) => {
                        place_cloned.store(Arc::new(v));
                        continue;
                    }
                    Ok(None) => Error::EndOfStream,
                    Err(e) => Error::Error(e),
                };

                // call error handler, usually for logging
                (self.error_handler)(e).await;

                // prevent busy loop
                sleep(self.sleep).await;

                stream = loop {
                    match stream_maker().await {
                        Ok(s) => break Box::pin(s),
                        Err(e) => {
                            (self.error_handler)(Error::Error(e)).await;
                            sleep(self.sleep).await;
                        }
                    }
                };
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
            sleep: DEFAULT_RETRY_GAP,
            _p: PhantomData,
        }
    }
}
