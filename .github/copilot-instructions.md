# Copilot instructions for `burberry`

`burberry` is a Rust library crate — a fork of [paradigmxyz/artemis](https://github.com/paradigmxyz/artemis/)
providing an event-driven engine for MEV/blockchain bots. Edition 2021, MSRV 1.76.

## Build / test / lint

```bash
cargo build --all-features
cargo clippy --all-features --all-targets -- -D warnings
cargo fmt --all --check

# Feature combinations (all verified working, including --all-targets):
cargo check --all-features --all-targets
cargo check --no-default-features --all-targets
cargo check --no-default-features --features ethereum --all-targets
cargo check --no-default-features --features telegram --all-targets

# Examples require `ethereum` feature (they hit live RPC endpoints):
cargo run --example subscribe_mempool
cargo run --example map

cargo test --all-features            # full suite
cargo test --no-default-features     # core engine tests without optional deps
cargo test broadcast_does_not_deep_clone_payloads   # single test
cargo test -- --nocapture            # show println!/tracing output
```

Tests live in `tests/` (integration style) plus `#[cfg(test)]` modules for anything needing the
crate's optional dependencies (alloy's `Asserter` mock transport is only reachable from inside the
crate). `tests/arc_broadcast.rs` asserts clone counts and will fail if the `Arc` broadcast path
regresses. `tests/executor_runtime.rs` covers executor concurrency, per-executor config, timeouts,
graceful shutdown, and reconnect backoff; it is timing-based, so keep its sleep margins generous.
`poll_full_block_collector`'s tests drive a FIFO mock queue, so an unexpected extra RPC call
desyncs the queue and trips the 2s timeout.

## Architecture

The whole crate is four traits in `src/types.rs` wired together by `Engine` in `src/engine.rs`:

- `Collector<E>` — produces an async `CollectorStream<'_, E>` of events (blocks, logs, mempool txs, ticks).
- `Strategy<E, A>` — consumes events as `Arc<E>`, emits actions via an `Arc<dyn ActionSubmitter<A>>`.
  `sync_state` runs once before the event loop starts; a failure there aborts `Engine::run`.
- `ActionSubmitter<A>` — fire-and-forget action sink taking an owned `A`; **`submit` is sync and
  returns nothing**, so errors are logged, not propagated.
- `Executor<A>` — performs the action, receiving `Arc<A>` (send tx, post Telegram message, print),
  with an optional cheap synchronous `accepts(&self, action: &A) -> bool` pre-filter.

`Engine::run` builds two `tokio::sync::broadcast` channels (event and action, both capacity 512 by
default, tunable via `with_event_channel_capacity` / `with_action_channel_capacity`) and spawns every
collector, strategy, and executor into a `JoinSet`. Consequences to keep in mind:

- Broadcast means **every strategy sees every event and every executor sees every action** — routing
  is done by the `ExecutorMap`/`ArcExecutorMap`/`CollectorMap` adapters, not by the channels.
- **Payloads are broadcast as `Arc<E>` / `Arc<A>`.** tokio's broadcast clones the value once per
  receiver, so sending `E`/`A` directly would deep-clone a full `Block` (hundreds of KB) once per
  strategy. The engine wraps each payload in an `Arc` exactly once; additional subscribers only cost
  a refcount bump. This is why `E` and `A` are **not** required to be `Clone` (`A` must still be
  `Debug` for logging). `tests/arc_broadcast.rs` guards this with clone counters — keep it passing.
- Slow consumers get `RecvError::Lagged` (logged at `warn` for events, `error` for actions) and
  increment `EngineStats::{events_lagged, actions_lagged}` accessible via `engine.stats()`.
- **Executors are serial by default** — one action at a time, waiting forever. That default is
  deliberate: `TransactionSender` assigns nonces sequentially and breaks if actions interleave.
  Use `add_executor_with_config(exec, ExecutorConfig::new().with_concurrency(n).with_timeout(d))`
  to mix serial and concurrent executors in the same `Engine`, or set defaults with
  `with_executor_concurrency` / `with_executor_timeout`. Concurrent executors run in-flight tasks
  inside a per-executor `JoinSet` so dropping the engine `JoinSet` cancels them cleanly, and
  `Executor::accepts` filters out unrelated variants before consuming concurrency slots.
- `run` returns an error (`Box<dyn Error + Send + Sync>`) if any of collectors / strategies /
  executors is empty.
- **Collectors reconnect with exponential backoff + jitter.** A failed `get_event_stream` or an
  ended stream retries starting at `with_collector_retry_interval` (1s default) up to
  `with_collector_max_retry_interval` (30s default), resetting once a stream delivers an event. If
  all strategies exit and drop their receivers, collectors stop cleanly instead of spinning.
- **Cooperative shutdown**: pass a `tokio::sync::watch::Receiver<bool>` via `with_shutdown`. Flipping
  it to `true` stops collectors, lets strategies drain buffered events, and lets executors finish
  buffered and in-flight actions before `run_and_join` returns `Ok(())`.

### The map/adapter pattern

The typical bot defines one `Event` enum and one `Action` enum, then wraps single-purpose
collectors/executors so they fit those enums:

- `CollectorMap<E1, E2>` — `E1 -> E2` (used to wrap a concrete event into an enum variant).
- `CollectorFilterMap` — `E1 -> Option<E2>` (does not require `Copy` or `Clone` on the closure).
- `ExecutorMap<A2, F, P>` — `F: Fn(&A1) -> Option<A2>`; **the closure borrows the action**, and
  `.with_accepts(...)` wires up `Executor::accepts` so unrelated variants are skipped before
  cloning, scheduling, or spawning.
- `ArcExecutorMap<A2, F, P>` — `F: Fn(&A1) -> Option<Arc<A2>>`; forwards `Arc<A2>` payloads without
  cloning `A2` or double-wrapping `Arc<Arc<A2>>`.
- `ActionSubmitterMap` — same idea on the submit side (takes an owned `A1`).

The macros in `src/macros.rs` are the intended way to build these and take an enum *variant path*
as the second argument:

```rust
engine.add_collector(map_collector!(MempoolCollector::new(provider), Event::Transaction));
engine.add_executor(map_executor!(EchoExecutor::default(), Action::EchoBlock));
engine.add_executor(map_arc_executor!(RawTransactionSender::new(provider), Action::RawTx));
submit_action!(submitter, Action::EchoBlock, block.number);
```

Use `map_boxed_collector!` / `map_boxed_executor!` / `map_arc_boxed_executor!` when you already
hold a `Box<dyn ...>`.

## Conventions

- **Cargo features gate whole modules and examples.** `ethereum` (alloy + thiserror) and `telegram`
  (reqwest + serde_json) are both on by default. Any new alloy- or reqwest-dependent file must be
  declared *and* re-exported behind `#[cfg(feature = "...")]` in the parent `mod.rs`
  (see `src/collector/mod.rs`, `src/executor/mod.rs`, `src/action_submitter/mod.rs`), and examples
  that depend on optional features must declare `required-features` in `Cargo.toml`.
- **`ActionSubmitter::submit` must never block.** It is sync and runs inside the strategy's event
  loop, so anything slow has to be handed to a background worker. `TelegramSubmitter` pushes onto a
  bounded `mpsc` queue drained by one dedicated thread with its own runtime, drops messages when
  full, and tracks drops via `dropped_count()`.
- **Secrets must not appear in logs.** `Message`'s custom `Debug` impl redacts `bot_token`.
- **Outbound HTTP clients must set timeouts.** `TelegramMessageDispatcher`, `RawTransactionSender`,
  and `TransactionSender` all configure `timeout` / `connect_timeout` / `pool_idle_timeout`.
  `TelegramMessageDispatcher` skips error-report round-trips on transport failures so outages don't
  double the stall time, and exposes `with_base_url` / `with_timeout` for offline tests.
- **Executors propagate errors.** `Executor::execute` returns `Err` on delivery/signing/RPC errors
  so `Engine` logs once and updates `EngineStats::actions_failed`.
- **Public API is flat**: `src/lib.rs` re-exports `types::*`, `Engine`, `EngineStats`,
  `ExecutorConfig`, and `async_trait`.
- **Trait impls use `#[async_trait]`** and override `fn name(&self) -> &str` with the struct's own
  name.
- **Errors:** `eyre::Result` everywhere in trait signatures; use `.wrap_err("...")` for context.
  `thiserror` is only used for the ethereum-specific error types.
- **Streams** are built with `async_stream::stream! { loop { ... yield ... } }` and returned as
  `Box::pin(stream)`; use `tokio::time::interval` with `MissedTickBehavior::Skip` to avoid drift.
- **Polling collectors probe cheaply and catch up gaps.** `PollFullBlockCollector` probes with
  `eth_blockNumber` and fetches any skipped heights in order up to `max_catchup_blocks` (32 default)
  without advancing the cursor on a miss/error.
- **Per-block fetches use block hash and bounded retries.** `FullBlockCollector` fetches via
  `get_block_by_hash` (reorg-safe) and `LogsInBlockCollector` filters via `at_block_hash`; both
  retry transient misses/errors up to `max_retries`.
- **Mempool streams bound memory.** `MempoolCollector` caps buffered hashes (`with_max_buffered`,
  evicting oldest on burst) and requires `max_concurrent_requests >= 1`. Prefer
  `FullMempoolCollector` (`subscribe_full_pending_transactions`) against Reth/Geth/Erigon nodes.
- **Nonce lookup uses `pending`.** `TransactionSender` queries `get_transaction_count(account).pending()`
  and unwraps `Arc<TransactionRequest>` via `Arc::unwrap_or_clone`.
- `Cargo.lock` is gitignored — this is a library; don't commit it.
