#[macro_export]
macro_rules! map_boxed_executor {
    ($executor: expr, $variant: path) => {
        Box::new(
            $crate::ExecutorMap::new($executor, |action: &_| match action {
                $variant(value) => Some(value.clone()),
                _ => None,
            })
            .with_accepts(|action: &_| matches!(action, $variant(_))),
        )
    };
}

#[macro_export]
macro_rules! map_executor {
    ($executor: expr, $variant: path) => {
        $crate::map_boxed_executor!(Box::new($executor), $variant)
    };
}

/// Map an enum variant holding `Arc<T>` directly onto an `Executor<T>` without cloning `T` or
/// double-wrapping it in `Arc<Arc<T>>`.
#[macro_export]
macro_rules! map_arc_boxed_executor {
    ($executor: expr, $variant: path) => {
        Box::new(
            $crate::ArcExecutorMap::new($executor, |action: &_| match action {
                $variant(value) => Some(::std::sync::Arc::clone(value)),
                _ => None,
            })
            .with_accepts(|action: &_| matches!(action, $variant(_))),
        )
    };
}

/// Convenience wrapper around [`map_arc_boxed_executor!`] for an unboxed `Executor<T>`.
#[macro_export]
macro_rules! map_arc_executor {
    ($executor: expr, $variant: path) => {
        $crate::map_arc_boxed_executor!(Box::new($executor), $variant)
    };
}

#[macro_export]
macro_rules! map_boxed_collector {
    ($collector: expr, $variant: path) => {
        Box::new($crate::CollectorMap::new($collector, $variant))
    };
}

#[macro_export]
macro_rules! map_collector {
    ($collector: expr, $variant: path) => {
        $crate::map_boxed_collector!(Box::new($collector), $variant)
    };
}

#[macro_export]
macro_rules! submit_action {
    ($submitter: expr, $variant: path, $action: expr) => {
        $submitter.submit($variant($action));
    };
}
