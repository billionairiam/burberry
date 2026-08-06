# Copilot instructions for `burberry`

`burberry` is a Rust library crate — a fork of [paradigmxyz/artemis](https://github.com/paradigmxyz/artemis/)
providing an event-driven engine for MEV/blockchain bots. Edition 2021, MSRV 1.76.

## Build / test / lint

```bash
cargo build --all-features
cargo clippy --all-features --all-targets -- -D warnings
cargo fmt --all --check

# Feature combinations (all verified working):
cargo check --all-features
cargo check --no-default-features
cargo check --no-default-features --features ethereum
cargo check --no-default-features --features telegram

# Examples are the de-facto integration tests (they hit live RPC endpoints):
cargo run --example subscribe_mempool
cargo run --example map

cargo test --all-features            # full suite
cargo test broadcast_does_not_deep_clone_payloads   # single test
cargo test -- --nocapture            # show println!/tracing output
```

Tests live in `tests/` (integration style) plus `#[cfg(test)]` modules for anything needing the
crate's optional dependencies (alloy's `Asserter` mock transport is only reachable from inside the
crate). `tests/arc_broadcast.rs` asserts clone counts and will fail if the `Arc` broadcast path
regresses. `tests/executor_runtime.rs` covers executor concurrency and timeouts; it is timing-based,
so keep its sleep margins generous. `poll_full_block_collector`'s tests drive a FIFO mock queue, so
an unexpected extra RPC call desyncs the queue and trips the 2s timeout.

## Architecture

The whole crate is four traits in `src/types.rs` wired together by `Engine` in `src/engine.rs`:

- `Collector<E>` — produces an async `CollectorStream<'_, E>` of events (blocks, logs, mempool txs, ticks).
- `Strategy<E, A>` — consumes events as `Arc<E>`, emits actions via an `Arc<dyn ActionSubmitter<A>>`.
  `sync_state` runs once before the event loop starts; a failure there aborts `Engine::run`.
- `ActionSubmitter<A>` — fire-and-forget action sink taking an owned `A`; **`submit` is sync and
  returns nothing**, so errors are logged, not propagated.
- `Executor<A>` — performs the action, receiving `Arc<A>` (send tx, post Telegram message, print).

`Engine::run` builds two `tokio::sync::broadcast` channels (event and action, both capacity 512 by
default, tunable via `with_event_channel_capacity` / `with_action_channel_capacity`) and spawns every
collector, strategy, and executor into a `JoinSet`. Consequences to keep in mind:

- Broadcast means **every strategy sees every event and every executor sees every action** — routing
  is done by the `ExecutorMap`/`CollectorMap` adapters, not by the channels.
- **Payloads are broadcast as `Arc<E>` / `Arc<A>`.** tokio's broadcast clones the value once per
  receiver, so sending `E`/`A` directly would deep-clone a full `Block` (hundreds of KB) once per
  strategy. The engine wraps each payload in an `Arc` exactly once; additional subscribers only cost
  a refcount bump. This is why `E` and `A` are **not** required to be `Clone` (`A` must still be
  `Debug` for logging). `tests/arc_broadcast.rs` guards this with clone counters — keep it passing.
- Slow consumers get `RecvError::Lagged`, which is logged as a warning and the events are dropped.
- **Executors are serial by default** — one action at a time, waiting forever. That default is
  deliberate: `TransactionSender` assigns nonces sequentially and breaks if actions interleave.
  Opt into `with_executor_concurrency(n)` (semaphore-bounded `tokio::spawn` per action) and
  `with_executor_timeout(d)` only for executors that tolerate it. A hanging action with neither
  set will wedge that executor and make the action channel lag.
- `run` returns an error if any of collectors / strategies / executors is empty.
- **Collectors reconnect.** A failed `get_event_stream` or an ended stream is retried every
  `with_collector_retry_interval` (1s default) rather than killing the task — websocket
  subscriptions drop routinely and the RPC endpoint is often not up yet at boot.
- Loops never terminate on their own; `run_and_join` only returns when tasks panic or channels close.

### The map/adapter pattern

The typical bot defines one `Event` enum and one `Action` enum, then wraps single-purpose
collectors/executors so they fit those enums:

- `CollectorMap<E1, E2>` — `E1 -> E2` (used to wrap a concrete event into an enum variant).
- `CollectorFilterMap` — `E1 -> Option<E2>`.
- `ExecutorMap<A2, F>` — `F: Fn(&A1) -> Option<A2>`; **the closure borrows the action** so that an
  executor which doesn't handle this variant costs nothing. `None` means "not my variant", skipped.
- `ActionSubmitterMap` — same idea on the submit side (takes an owned `A1`).

The macros in `src/macros.rs` are the intended way to build these and take an enum *variant path*
as the second argument:

```rust
engine.add_collector(map_collector!(MempoolCollector::new(provider), Event::Transaction));
engine.add_executor(map_executor!(EchoExecutor::default(), Action::EchoBlock));
submit_action!(submitter, Action::EchoBlock, block.number);
```

Use `map_boxed_collector!` / `map_boxed_executor!` when you already hold a `Box<dyn ...>`.

`map_executor!` expands to `match action { Variant(value) => Some(value.clone()), _ => None }`
against a `&A`, so **action variant payloads must be `Clone`, and large ones should be wrapped in
`Arc`** (e.g. `Action::Submit(Arc<TransactionRequest>)`) — that clone is the one remaining copy on
the hot path.

## Conventions

- **Cargo features gate whole modules.** `ethereum` (alloy + thiserror) and `telegram`
  (reqwest + serde_json) are both on by default. Any new alloy- or reqwest-dependent file must be
  declared *and* re-exported behind `#[cfg(feature = "...")]` in the parent `mod.rs`
  (see `src/collector/mod.rs`, `src/executor/mod.rs`, `src/action_submitter/mod.rs`). Feature-optional
  dependencies use `optional = true` + `dep:` syntax in `Cargo.toml`. Always verify with
  `--no-default-features`: the crate must not rely on features that only arrive transitively via
  alloy or reqwest (tokio's `sync`/`time`/`macros` are declared explicitly for exactly this reason).
- **`ActionSubmitter::submit` must never block.** It is sync and runs inside the strategy's event
  loop, so anything slow has to be handed to a background worker. `TelegramSubmitter` pushes onto a
  bounded `mpsc` queue drained by one dedicated thread with its own runtime, and drops messages with
  an error when the queue is full rather than growing without bound.
- **Outbound HTTP clients must set timeouts.** `TelegramMessageDispatcher` configures
  `timeout` / `connect_timeout` / `pool_idle_timeout`; a client without them can hang an executor
  indefinitely. Its `with_base_url` exists so tests can point at a local socket instead of
  `api.telegram.org` — keep network-touching tests offline that way.
- **Public API is flat**: `src/lib.rs` re-exports `types::*`, `Engine`, and `async_trait`. Downstream
  code writes `burberry::{Strategy, Engine, async_trait}`; new core types belong in `types.rs`.
- **Trait impls use `#[async_trait]`** and override `fn name(&self) -> &str` with the struct's own
  name — the engine logs it via `tracing` (`name = collector.name()`), so an unnamed component shows
  up as `"Unnamed"`.
- **Errors:** `eyre::Result` everywhere in trait signatures; use `.wrap_err("...")` for context.
  `thiserror` is only used for the ethereum-specific error types.
- **Streams** are built with `async_stream::stream! { loop { ... yield ... } }` and returned as
  `Box::pin(stream)`; see `src/collector/interval_collector.rs` for the minimal template.
- **Polling collectors probe cheaply first.** `PollFullBlockCollector` calls `eth_blockNumber` each
  tick and only pulls the full block when the height advanced — a full mainnet block is megabytes,
  so fetching it on every tick wastes bandwidth and RPC quota. A block that isn't indexed yet must
  not advance the cursor, so the next tick retries it.
- **Per-block retries must be bounded.** `FullBlockCollector` retries the body fetch (nodes announce
  a header slightly before they can serve the body) but gives up after `max_retries`. Retrying is
  serial by design — block order matters — so an unbounded retry would stall the stream while new
  headers pile up in the subscription buffer and get dropped. A miss logs at `debug` (it's routine),
  an RPC error at `warn`, and only giving up logs at `error`.
- **Alloy providers** are passed around as `Arc<dyn Provider<_>>` and cloned with `Arc::clone`.
  Alloy is pinned to major version 1 — its API changes frequently across versions, so match the
  existing call style (`ProviderBuilder::new().connect_ws(ws)`).
- **Builder pattern** for config-heavy structs with many optionals (`MessageBuilder` in
  `src/executor/telegram_message.rs`): `Option<T>` fields, `fn x<T: Into<String>>(mut self, ..) -> Self`.
- `Cargo.lock` is gitignored — this is a library; don't commit it.
