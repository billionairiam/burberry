use std::{future::ready, pin::Pin, sync::Arc};

use async_trait::async_trait;
use eyre::Result;
use futures::{Stream, StreamExt};

pub type CollectorStream<'a, E> = Pin<Box<dyn Stream<Item = E> + Send + 'a>>;

#[async_trait]
pub trait Collector<E>: Send + Sync {
    fn name(&self) -> &str {
        "Unnamed"
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>>;
}

pub trait ActionSubmitter<A>: Send + Sync
where
    A: Send + Sync + 'static,
{
    fn submit(&self, action: A);
}

#[async_trait]
pub trait Strategy<E, A>: Send + Sync
where
    E: Send + Sync + 'static,
    A: Send + Sync + 'static,
{
    fn name(&self) -> &str {
        "Unnamed"
    }

    async fn sync_state(&mut self, _submitter: Arc<dyn ActionSubmitter<A>>) -> Result<()> {
        Ok(())
    }

    /// Events are delivered as `Arc<E>` so that broadcasting to N strategies costs N reference
    /// count bumps instead of N deep clones.
    async fn process_event(&mut self, event: Arc<E>, submitter: Arc<dyn ActionSubmitter<A>>);
}

pub struct CollectorMap<E, F> {
    inner: Box<dyn Collector<E>>,
    f: F,
}

impl<E, F> CollectorMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self {
            inner: collector,
            f,
        }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for CollectorMap<E1, F>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> E2 + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.inner.get_event_stream().await?;
        let stream = stream.map(&self.f);
        Ok(Box::pin(stream))
    }
}

pub struct CollectorFilterMap<E, F> {
    inner: Box<dyn Collector<E>>,
    f: F,
}

impl<E, F> CollectorFilterMap<E, F> {
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self {
            inner: collector,
            f,
        }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for CollectorFilterMap<E1, F>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> Option<E2> + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E2>> {
        let stream = self.inner.get_event_stream().await?;
        let f = &self.f;
        let stream = stream.filter_map(move |v| ready(f(v)));
        Ok(Box::pin(stream))
    }
}

#[async_trait]
pub trait Executor<A>: Send + Sync {
    fn name(&self) -> &str {
        "Unnamed"
    }

    /// Cheap synchronous filter checked by [`crate::Engine`] before scheduling an action.
    ///
    /// Returning `false` skips concurrency slot acquisition, task spawning, and async dispatch
    /// for actions this executor does not handle (such as unrelated enum variants behind
    /// [`ExecutorMap`] or [`ArcExecutorMap`]). Defaults to `true`.
    fn accepts(&self, _action: &A) -> bool {
        true
    }

    /// Actions are delivered as `Arc<A>` so that broadcasting to M executors costs M reference
    /// count bumps instead of M deep clones.
    async fn execute(&self, action: Arc<A>) -> Result<()>;
}

/// Predicate used by [`ExecutorMap`] and [`ArcExecutorMap`] to implement [`Executor::accepts`].
pub trait ActionPredicate<A>: Send + Sync {
    fn accepts(&self, action: &A) -> bool;
}

/// Default predicate that accepts all actions.
#[derive(Debug, Default, Clone, Copy)]
pub struct AcceptAll;

impl<A> ActionPredicate<A> for AcceptAll {
    fn accepts(&self, _action: &A) -> bool {
        true
    }
}

/// Closure-backed predicate for [`ExecutorMap::with_accepts`] and [`ArcExecutorMap::with_accepts`].
#[derive(Debug, Clone, Copy)]
pub struct FnPredicate<P>(pub P);

impl<A, P> ActionPredicate<A> for FnPredicate<P>
where
    P: Fn(&A) -> bool + Send + Sync,
{
    fn accepts(&self, action: &A) -> bool {
        (self.0)(action)
    }
}

pub struct ExecutorMap<A, F, P = AcceptAll> {
    inner: Box<dyn Executor<A>>,
    f: F,
    predicate: P,
}

impl<A, F> ExecutorMap<A, F, AcceptAll> {
    pub fn new(executor: Box<dyn Executor<A>>, f: F) -> Self {
        Self {
            inner: executor,
            f,
            predicate: AcceptAll,
        }
    }

    /// Attach a synchronous predicate checked before scheduling or cloning an action.
    pub fn with_accepts<P>(self, predicate: P) -> ExecutorMap<A, F, FnPredicate<P>> {
        ExecutorMap {
            inner: self.inner,
            f: self.f,
            predicate: FnPredicate(predicate),
        }
    }
}

#[async_trait]
impl<A1, A2, F, P> Executor<A1> for ExecutorMap<A2, F, P>
where
    A1: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(&A1) -> Option<A2> + Send + Sync + 'static,
    P: ActionPredicate<A1> + 'static,
{
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn accepts(&self, action: &A1) -> bool {
        self.predicate.accepts(action)
    }

    async fn execute(&self, action: Arc<A1>) -> Result<()> {
        // The mapping function borrows the action, so an executor that does not handle this
        // variant costs nothing beyond the match itself.
        match (self.f)(&action) {
            Some(action) => {
                if !self.inner.accepts(&action) {
                    return Ok(());
                }
                self.inner.execute(Arc::new(action)).await
            }
            None => Ok(()),
        }
    }
}

/// Like [`ExecutorMap`], but for action variants that already wrap their payload in `Arc<A2>`.
///
/// Forwards the cloned `Arc<A2>` directly to the inner executor without deep-cloning the inner
/// value or wrapping it in a second `Arc<Arc<A2>>`.
pub struct ArcExecutorMap<A, F, P = AcceptAll> {
    inner: Box<dyn Executor<A>>,
    f: F,
    predicate: P,
}

impl<A, F> ArcExecutorMap<A, F, AcceptAll> {
    pub fn new(executor: Box<dyn Executor<A>>, f: F) -> Self {
        Self {
            inner: executor,
            f,
            predicate: AcceptAll,
        }
    }

    /// Attach a synchronous predicate checked before scheduling an action.
    pub fn with_accepts<P>(self, predicate: P) -> ArcExecutorMap<A, F, FnPredicate<P>> {
        ArcExecutorMap {
            inner: self.inner,
            f: self.f,
            predicate: FnPredicate(predicate),
        }
    }
}

#[async_trait]
impl<A1, A2, F, P> Executor<A1> for ArcExecutorMap<A2, F, P>
where
    A1: Send + Sync + 'static,
    A2: Send + Sync + 'static,
    F: Fn(&A1) -> Option<Arc<A2>> + Send + Sync + 'static,
    P: ActionPredicate<A1> + 'static,
{
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn accepts(&self, action: &A1) -> bool {
        self.predicate.accepts(action)
    }

    async fn execute(&self, action: Arc<A1>) -> Result<()> {
        match (self.f)(&action) {
            Some(mapped) => {
                if !self.inner.accepts(&mapped) {
                    return Ok(());
                }
                self.inner.execute(mapped).await
            }
            None => Ok(()),
        }
    }
}
