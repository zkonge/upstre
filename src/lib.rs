use std::{
    future::{ready, Future, Ready},
    marker::PhantomData,
    sync::Arc,
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
/// Each time the stream yields, ends or raises an error, the complete handler will be called and
/// the stream will be recreated.
///
/// [`Upstre`] is cheap to clone, and the clones will share the same value.
/// When all clones are dropped, the stream will be aborted.
#[derive(Clone, Debug)]
pub struct Upstre<T: Send + Sync + 'static> {
    place: Arc<ArcSwap<T>>,
    _aborter: Arc<CancelOnDrop>,
}

impl<T: Send + Sync + 'static> Upstre<T> {
    /// Get the last value of the stream.
    pub fn value(&self) -> Guard<Arc<T>> {
        self.place.load()
    }
}

pub struct UpstreBuilder<CH, T, E, CHFut>
where
    CH: Fn(Result<Arc<T>, Error<E>>) -> CHFut + Send + 'static,
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
    CH: Fn(Result<Arc<T>, Error<E>>) -> CHFut + Send + 'static,
    T: Send + Sync + 'static,
    E: Send,
    CHFut: Future<Output = ()> + Send,
{
    /// The complete handler receives value while [`Stream`] yields or ends.
    pub fn new(complete_handler: CH) -> Self {
        Self {
            complete_handler,
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
    pub async fn build<F, Fut, S>(self, stream_maker: F) -> Result<Upstre<T>, Error<E>>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: Stream<Item = Result<T, E>> + Send + 'static,
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
                        let v = Arc::new(v);
                        place_cloned.store(v.clone());
                        (self.complete_handler)(Ok(v));
                        continue;
                    }
                    Ok(None) => Error::EndOfStream,
                    Err(e) => Error::Error(e),
                };

                // call complete handler, usually for logging
                (self.complete_handler)(Err(e)).await;

                // prevent busy loop
                sleep(self.sleep).await;

                stream = loop {
                    match stream_maker().await {
                        Ok(s) => break Box::pin(s),
                        Err(e) => {
                            (self.complete_handler)(Err(Error::Error(e))).await;
                            sleep(self.sleep).await;
                        }
                    }
                };
            }
        };

        let task = tokio::spawn(task);

        Ok(Upstre {
            place,
            _aborter: Arc::new(CancelOnDrop(task.abort_handle())),
        })
    }
}

impl<T, E> Default for UpstreBuilder<fn(Result<Arc<T>, Error<E>>) -> Ready<()>, T, E, Ready<()>>
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

#[derive(Debug)]
struct CancelOnDrop(AbortHandle);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
