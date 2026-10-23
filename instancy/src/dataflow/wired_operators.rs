//! The logic of every built-in operator.
//!
//! Each operator is an [`OperatorLogic`] run by an
//! [`OperatorShell`](crate::dataflow::shell::OperatorShell), which owns its
//! counted ports and all of their I/O (see [`crate::dataflow::shell`]). The
//! constructors here are called by operator factories during
//! materialization, with ports built by
//! [`ChannelEndpoints`](crate::dataflow::schedulable::ChannelEndpoints).
//!
//! **Lifecycle:**
//! 1. Build phase → logical operator (metadata in DataflowGraph)
//! 2. Materialize phase → shell with counted ports and its logic
//!
//! Type-erasure happens at the outer `SchedulableOperator` boundary only —
//! internally, operators work with concrete Rust types (no dynamic dispatch on data).

use std::any::Any;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::operators::handles::{InputHandle, NotifyContext, OutputHandle};
use crate::dataflow::shell::{CountedInput, CountedOutput, OperatorLogic, OperatorShell};
use crate::dataflow::stage::StageId;
use crate::error::{Error, Result};
use crate::order::Product;
use crate::progress::capability::Capability;
use crate::progress::frontier::Antichain;
use crate::progress::notificator::Notificator;
use crate::progress::timestamp::Timestamp;

/// The closure of a unary operator.
pub(crate) type UnaryFn<T, D1, D2> =
    Box<dyn FnMut(&mut InputHandle<T, D1>, &mut OutputHandle<T, D2>) -> Result<()> + Send>;

/// The closure of a binary operator.
pub(crate) type BinaryFn<T, D1, D2, D3> = Box<
    dyn FnMut(
            &mut InputHandle<T, D1>,
            &mut InputHandle<T, D2>,
            &mut OutputHandle<T, D3>,
        ) -> Result<()>
        + Send,
>;

/// The closure of a notifying unary operator.
pub(crate) type NotifyFn<T, D1, D2> = Box<
    dyn FnMut(
            &mut InputHandle<T, D1>,
            &mut OutputHandle<T, D2>,
            &mut NotifyContext<'_, T>,
        ) -> Result<()>
        + Send,
>;

// ---------------------------------------------------------------------------
// Unary
// ---------------------------------------------------------------------------

/// A unary operator: calls its closure whenever input is waiting.
pub(crate) struct UnaryLogic<T: Timestamp, D1: Send + 'static, D2: Send + 'static> {
    logic: UnaryFn<T, D1, D2>,
    /// A capability at `T::minimum()` held until the input ends (the
    /// broadcast pass-through's guard).
    guard: Option<Capability<T>>,
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static> OperatorLogic for UnaryLogic<T, D1, D2> {
    type Inputs = CountedInput<T, D1>;
    type Outputs = CountedOutput<T, D2>;
    type Time = T;

    fn wants_call(&self, input: &Self::Inputs) -> bool {
        input.has_pending() || (self.guard.is_some() && input.is_exhausted())
    }

    fn call(&mut self, input: &mut Self::Inputs, output: &mut Self::Outputs) -> Result<bool> {
        let did = input.has_pending();
        if did {
            (self.logic)(input.handle(), output.handle())?;
        }
        if input.is_exhausted() && !input.has_pending() {
            self.guard = None;
        }
        Ok(did)
    }

    fn holds_work(&self) -> bool {
        self.guard.is_some()
    }

    fn inputs_closed(&mut self) {
        self.guard = None;
    }
}

/// A unary operator running `logic`.
pub(crate) fn unary<T: Timestamp, D1: Send + 'static, D2: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    logic: impl FnMut(&mut InputHandle<T, D1>, &mut OutputHandle<T, D2>) -> Result<()> + Send + 'static,
    input: CountedInput<T, D1>,
    output: CountedOutput<T, D2>,
) -> OperatorShell<UnaryLogic<T, D1, D2>> {
    OperatorShell::new(
        name,
        index,
        stage_id,
        UnaryLogic {
            logic: Box::new(logic),
            guard: None,
        },
        input,
        output,
    )
}

/// A unary operator that holds an initial capability at `T::minimum()`
/// until its input ends.
pub(crate) fn unary_guarded<T: Timestamp, D1: Send + 'static, D2: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    logic: impl FnMut(&mut InputHandle<T, D1>, &mut OutputHandle<T, D2>) -> Result<()> + Send + 'static,
    input: CountedInput<T, D1>,
    output: CountedOutput<T, D2>,
) -> OperatorShell<UnaryLogic<T, D1, D2>> {
    let guard = Capability::seeded(T::minimum(), output.reporter().clone());
    OperatorShell::new(
        name,
        index,
        stage_id,
        UnaryLogic {
            logic: Box::new(logic),
            guard: Some(guard),
        },
        input,
        output,
    )
}

// ---------------------------------------------------------------------------
// Source (pre-loaded data)
// ---------------------------------------------------------------------------

/// A source that emits pre-loaded data, then releases its capability.
pub(crate) struct SourceLogic<T: Timestamp, D: Send + 'static> {
    /// Data to emit, in insertion order.
    pending: VecDeque<(T, Vec<D>)>,
    /// The source's initial capability, released once `pending` is handed
    /// to the output.
    capability: Option<Capability<T>>,
    /// The progress checker's count of `pending` (work held for the output).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check: Option<crate::progress::check::StoreCheck<T>>,
}

impl<T: Timestamp, D: Send + 'static> OperatorLogic for SourceLogic<T, D> {
    type Inputs = ();
    type Outputs = CountedOutput<T, D>;
    type Time = T;

    fn wants_call(&self, _: &()) -> bool {
        !self.pending.is_empty() || self.capability.is_some()
    }

    fn call(&mut self, _: &mut (), output: &mut Self::Outputs) -> Result<bool> {
        let did = !self.pending.is_empty();
        while let Some((time, data)) = self.pending.pop_front() {
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            if let Some(check) = &self.check {
                check.remove(&time, data.len());
            }
            output.handle().push_vec(time, data);
        }
        // Everything is handed to the output, where it is counted.
        self.capability = None;
        Ok(did)
    }

    fn holds_work(&self) -> bool {
        !self.pending.is_empty() || self.capability.is_some()
    }

    fn inputs_closed(&mut self) {
        self.capability = None;
    }
}

/// A source emitting `data`. With `seeded`, it holds its operator's
/// initial capability at `T::minimum()` (declared with the progress
/// tracker) until all data is emitted.
pub(crate) fn source<T: Timestamp, D: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    data: Vec<(T, Vec<D>)>,
    output: CountedOutput<T, D>,
    seeded: bool,
) -> OperatorShell<SourceLogic<T, D>> {
    let capability = seeded.then(|| Capability::seeded(T::minimum(), output.reporter().clone()));
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    let check = output.check_port().map(|port| {
        crate::progress::check::StoreCheck::new(port, crate::progress::check::Store::PendingOutput)
    });
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    if let Some(check) = &check {
        for (time, batch) in &data {
            check.add(time, batch.len());
        }
    }
    OperatorShell::new(
        name,
        index,
        stage_id,
        SourceLogic {
            pending: VecDeque::from(data),
            capability,
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check,
        },
        (),
        output,
    )
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// A sink: hands every batch it takes to `consume`.
pub(crate) struct SinkLogic<T: Timestamp, D: Send + 'static, F: FnMut(T, Vec<D>) + Send + 'static> {
    consume: F,
    _marker: std::marker::PhantomData<fn(T, D)>,
}

impl<T: Timestamp, D: Send + 'static, F: FnMut(T, Vec<D>) + Send + 'static> OperatorLogic
    for SinkLogic<T, D, F>
{
    type Inputs = CountedInput<T, D>;
    type Outputs = ();
    type Time = T;

    fn call(&mut self, input: &mut Self::Inputs, _: &mut ()) -> Result<bool> {
        let handle = input.handle();
        let mut did = false;
        while let Some((time, data)) = handle.next() {
            (self.consume)(time, data);
            did = true;
        }
        Ok(did)
    }
}

/// A sink calling `consume` with every batch.
pub(crate) fn sink<T: Timestamp, D: Send + 'static, F: FnMut(T, Vec<D>) + Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    consume: F,
    input: CountedInput<T, D>,
) -> OperatorShell<SinkLogic<T, D, F>> {
    OperatorShell::new(
        name,
        index,
        stage_id,
        SinkLogic {
            consume,
            _marker: std::marker::PhantomData,
        },
        input,
        (),
    )
}

// ---------------------------------------------------------------------------
// Binary
// ---------------------------------------------------------------------------

/// A binary operator: calls its closure whenever either input is waiting.
pub(crate) struct BinaryLogic<
    T: Timestamp,
    D1: Send + 'static,
    D2: Send + 'static,
    D3: Send + 'static,
> {
    logic: BinaryFn<T, D1, D2, D3>,
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static, D3: Send + 'static> OperatorLogic
    for BinaryLogic<T, D1, D2, D3>
{
    type Inputs = (CountedInput<T, D1>, CountedInput<T, D2>);
    type Outputs = CountedOutput<T, D3>;
    type Time = T;

    fn call(&mut self, inputs: &mut Self::Inputs, output: &mut Self::Outputs) -> Result<bool> {
        (self.logic)(inputs.0.handle(), inputs.1.handle(), output.handle())?;
        Ok(true)
    }
}

/// A binary operator running `logic`.
pub(crate) fn binary<T: Timestamp, D1: Send + 'static, D2: Send + 'static, D3: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    logic: impl FnMut(
        &mut InputHandle<T, D1>,
        &mut InputHandle<T, D2>,
        &mut OutputHandle<T, D3>,
    ) -> Result<()>
    + Send
    + 'static,
    input1: CountedInput<T, D1>,
    input2: CountedInput<T, D2>,
    output: CountedOutput<T, D3>,
) -> OperatorShell<BinaryLogic<T, D1, D2, D3>> {
    OperatorShell::new(
        name,
        index,
        stage_id,
        BinaryLogic {
            logic: Box::new(logic),
        },
        (input1, input2),
        output,
    )
}

// ---------------------------------------------------------------------------
// Concat
// ---------------------------------------------------------------------------

/// Merges N same-typed inputs into one output. Order within a time is kept
/// per input.
pub(crate) struct ConcatLogic<T: Timestamp, D: Send + 'static> {
    _marker: std::marker::PhantomData<fn(T, D)>,
}

impl<T: Timestamp, D: Send + 'static> OperatorLogic for ConcatLogic<T, D> {
    type Inputs = Vec<CountedInput<T, D>>;
    type Outputs = CountedOutput<T, D>;
    type Time = T;

    fn call(&mut self, inputs: &mut Self::Inputs, output: &mut Self::Outputs) -> Result<bool> {
        let output = output.handle();
        let mut did = false;
        for input in inputs.iter_mut() {
            while let Some((time, data)) = input.handle().next() {
                output.push_vec(time, data);
                did = true;
            }
        }
        Ok(did)
    }
}

/// A concat of `inputs`.
pub(crate) fn concat<T: Timestamp, D: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    inputs: Vec<CountedInput<T, D>>,
    output: CountedOutput<T, D>,
) -> OperatorShell<ConcatLogic<T, D>> {
    OperatorShell::new(
        name,
        index,
        stage_id,
        ConcatLogic {
            _marker: std::marker::PhantomData,
        },
        inputs,
        output,
    )
}

// ---------------------------------------------------------------------------
// Enter, leave, feedback: a loop's scope boundary
// ---------------------------------------------------------------------------

/// Enters a loop scope: `t` becomes `Product(t, TInner::minimum())`.
pub(crate) struct EnterLogic<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static> {
    _marker: std::marker::PhantomData<fn(TOuter, TInner, D)>,
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static> OperatorLogic
    for EnterLogic<TOuter, TInner, D>
where
    Product<TOuter, TInner>: Timestamp,
{
    type Inputs = CountedInput<TOuter, D>;
    type Outputs = CountedOutput<Product<TOuter, TInner>, D>;
    type Time = Product<TOuter, TInner>;

    fn call(&mut self, input: &mut Self::Inputs, output: &mut Self::Outputs) -> Result<bool> {
        let output = output.handle();
        let mut did = false;
        while let Some((time, data)) = input.handle().next() {
            output.push_vec(Product::new(time, TInner::minimum()), data);
            did = true;
        }
        Ok(did)
    }

    fn checks_justification(&self) -> bool {
        false
    }
}

/// A loop's enter operator.
pub(crate) fn enter<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    input: CountedInput<TOuter, D>,
    output: CountedOutput<Product<TOuter, TInner>, D>,
) -> OperatorShell<EnterLogic<TOuter, TInner, D>>
where
    Product<TOuter, TInner>: Timestamp,
{
    OperatorShell::new(
        name,
        index,
        stage_id,
        EnterLogic {
            _marker: std::marker::PhantomData,
        },
        input,
        output,
    )
}

/// Leaves a loop scope: `Product(outer, inner)` becomes `outer`.
pub(crate) struct LeaveLogic<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static> {
    _marker: std::marker::PhantomData<fn(TOuter, TInner, D)>,
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static> OperatorLogic
    for LeaveLogic<TOuter, TInner, D>
where
    Product<TOuter, TInner>: Timestamp,
{
    type Inputs = CountedInput<Product<TOuter, TInner>, D>;
    type Outputs = CountedOutput<TOuter, D>;
    type Time = TOuter;

    fn call(&mut self, input: &mut Self::Inputs, output: &mut Self::Outputs) -> Result<bool> {
        let output = output.handle();
        let mut did = false;
        while let Some((time, data)) = input.handle().next() {
            output.push_vec(time.outer, data);
            did = true;
        }
        Ok(did)
    }

    fn checks_justification(&self) -> bool {
        false
    }
}

/// A loop's leave operator.
pub(crate) fn leave<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    input: CountedInput<Product<TOuter, TInner>, D>,
    output: CountedOutput<TOuter, D>,
) -> OperatorShell<LeaveLogic<TOuter, TInner, D>>
where
    Product<TOuter, TInner>: Timestamp,
{
    OperatorShell::new(
        name,
        index,
        stage_id,
        LeaveLogic {
            _marker: std::marker::PhantomData,
        },
        input,
        output,
    )
}

/// A loop's feedback: advances the inner time by `summary`. A record whose
/// inner time would overflow is dropped.
pub(crate) struct FeedbackLogic<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static> {
    summary: TInner::Summary,
    _marker: std::marker::PhantomData<fn(TOuter, D)>,
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static> OperatorLogic
    for FeedbackLogic<TOuter, TInner, D>
where
    Product<TOuter, TInner>: Timestamp,
{
    type Inputs = CountedInput<Product<TOuter, TInner>, D>;
    type Outputs = CountedOutput<Product<TOuter, TInner>, D>;
    type Time = Product<TOuter, TInner>;

    fn call(&mut self, input: &mut Self::Inputs, output: &mut Self::Outputs) -> Result<bool> {
        use crate::progress::timestamp::PathSummary;
        let output = output.handle();
        let mut did = false;
        while let Some((time, data)) = input.handle().next() {
            if let Some(inner) = self.summary.results_in(&time.inner) {
                output.push_vec(Product::new(time.outer, inner), data);
            }
            did = true;
        }
        Ok(did)
    }

    fn checks_justification(&self) -> bool {
        false
    }
}

/// A loop's feedback operator.
pub(crate) fn feedback<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    summary: TInner::Summary,
    input: CountedInput<Product<TOuter, TInner>, D>,
    output: CountedOutput<Product<TOuter, TInner>, D>,
) -> OperatorShell<FeedbackLogic<TOuter, TInner, D>>
where
    Product<TOuter, TInner>: Timestamp,
{
    OperatorShell::new(
        name,
        index,
        stage_id,
        FeedbackLogic {
            summary,
            _marker: std::marker::PhantomData,
        },
        input,
        output,
    )
}

// ---------------------------------------------------------------------------
// Notify
// ---------------------------------------------------------------------------

/// A unary operator with frontier notifications (`unary_notify`, reduce,
/// fold, count, distinct, delay).
///
/// `ctx.notify_at(t)` holds an output capability at `t` and requests a
/// notification once the input frontier passes `t`;
/// `ctx.next_notification()` returns such a time and drops its capability.
/// The logic is called when input is waiting or a notification is ready.
pub(crate) struct NotifyLogic<T: Timestamp, D1: Send + 'static, D2: Send + 'static> {
    logic: NotifyFn<T, D1, D2>,
    /// Tracks when the input frontier passes requested times.
    notificator: Notificator<T>,
    /// Output capabilities held for requested notifications.
    held_capabilities: Vec<Capability<T>>,
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static> OperatorLogic
    for NotifyLogic<T, D1, D2>
{
    type Inputs = CountedInput<T, D1>;
    type Outputs = CountedOutput<T, D2>;
    type Time = T;

    fn wants_call(&self, input: &Self::Inputs) -> bool {
        input.has_pending() || self.notificator.has_ready()
    }

    fn call(&mut self, input: &mut Self::Inputs, output: &mut Self::Outputs) -> Result<bool> {
        // Times the operator holds at the start of this call: the available
        // input batches and the capabilities it holds.
        let activation_times: Antichain<T> = input
            .handle()
            .pending_times()
            .chain(self.held_capabilities.iter().map(|cap| cap.time()))
            .cloned()
            .collect();
        let reporter = output.reporter().clone();
        let mut ctx = NotifyContext::new(
            &mut self.notificator,
            &reporter,
            &mut self.held_capabilities,
            activation_times,
        );
        (self.logic)(input.handle(), output.handle(), &mut ctx)?;
        if let Some(error) = ctx.take_error() {
            return Err(error);
        }
        Ok(true)
    }

    fn holds_work(&self) -> bool {
        self.notificator.has_ready()
            || self.notificator.pending_count() > 0
            || !self.held_capabilities.is_empty()
    }

    fn update_frontier(&mut self, meet: &dyn Any) {
        if let Some(frontier) = meet.downcast_ref::<Antichain<T>>() {
            self.notificator.update_frontier(frontier);
        }
    }

    fn has_ready_notifications(&self) -> bool {
        self.notificator.has_ready()
    }

    fn inputs_closed(&mut self) {
        // No more input: every pending notification may fire.
        self.notificator.update_frontier(&Antichain::new());
    }
}

/// A notifying unary operator running `logic`, whose input frontier starts
/// at `initial_frontier`.
pub(crate) fn notify<T: Timestamp, D1: Send + 'static, D2: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    logic: impl FnMut(
        &mut InputHandle<T, D1>,
        &mut OutputHandle<T, D2>,
        &mut NotifyContext<'_, T>,
    ) -> Result<()>
    + Send
    + 'static,
    input: CountedInput<T, D1>,
    output: CountedOutput<T, D2>,
    initial_frontier: Antichain<T>,
) -> OperatorShell<NotifyLogic<T, D1, D2>> {
    OperatorShell::new(
        name,
        index,
        stage_id,
        NotifyLogic {
            logic: Box::new(logic),
            notificator: Notificator::new(initial_frontier),
            held_capabilities: Vec::new(),
        },
        input,
        output,
    )
}

// ---------------------------------------------------------------------------
// Async
// ---------------------------------------------------------------------------

/// Type alias for the async logic closure used by `unary_async`.
///
/// The closure receives a timestamp and a batch of input items, and returns
/// a future that produces a `Result<Vec<D2>>` (the output items for that batch).
pub type AsyncLogicFn<T, D1, D2> =
    Arc<dyn Fn(T, Vec<D1>) -> Pin<Box<dyn Future<Output = Result<Vec<D2>>> + Send>> + Send + Sync>;

/// A unary operator that runs each input batch as a tokio task (at most
/// `max_concurrency` at once) and emits results in completion order.
pub(crate) struct AsyncLogic<T: Timestamp, D1: Send + 'static, D2: Send + 'static> {
    name: String,
    logic: AsyncLogicFn<T, D1, D2>,
    max_concurrency: usize,
    /// Tasks spawned and not yet collected.
    in_flight: usize,
    result_tx: tokio::sync::mpsc::UnboundedSender<Result<(T, Vec<D2>)>>,
    result_rx: tokio::sync::mpsc::UnboundedReceiver<Result<(T, Vec<D2>)>>,
    /// Consumed input waiting for a task slot.
    pending_input: VecDeque<(T, Vec<D1>)>,
    tokio_handle: tokio::runtime::Handle,
    /// Woken when a task completes, so the operator collects its result.
    wake_handle: Option<WakeHandle>,
    /// The progress checker's counts of `pending_input` (at the input) and
    /// of running or uncollected tasks (at the output, one unit per task).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check_input: Option<crate::progress::check::StoreCheck<T>>,
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check_tasks: Option<crate::progress::check::StoreCheck<T>>,
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static> AsyncLogic<T, D1, D2> {
    /// Spawn tasks for pending input up to the concurrency limit.
    fn spawn_pending(&mut self) {
        while self.in_flight < self.max_concurrency {
            let Some((time, data)) = self.pending_input.pop_front() else {
                break;
            };
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            {
                if let Some(check) = &self.check_tasks {
                    check.add(&time, 1);
                }
                if let Some(check) = &self.check_input {
                    check.remove(&time, data.len());
                }
            }
            let logic = Arc::clone(&self.logic);
            let tx = self.result_tx.clone();
            let time_for_logic = time.clone();
            let wake = self.wake_handle.clone();
            let operator_name = self.name.clone();
            // A nested spawn turns a panic into a JoinError.
            let handle = self.tokio_handle.clone();
            self.tokio_handle.spawn(async move {
                let inner = handle.spawn(async move { logic(time_for_logic, data).await });
                let result = match inner.await {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(e)) => Err(e),
                    Err(join_err) => Err(Error::OperatorPanic {
                        operator: operator_name,
                        worker_index: None,
                        message: format!("unary_async task panicked: {join_err}"),
                    }),
                };
                let _ = tx.send(result.map(|output| (time, output)));
                if let Some(w) = wake {
                    w.notify();
                }
            });
            self.in_flight += 1;
        }
    }

    /// Collect completed results into the output session.
    fn collect_results(&mut self, output: &mut OutputHandle<T, D2>) -> Result<bool> {
        let mut collected = false;
        loop {
            match self.result_rx.try_recv() {
                Ok(Ok((time, data))) => {
                    self.in_flight -= 1;
                    #[cfg(any(debug_assertions, feature = "progress-check"))]
                    if let Some(check) = &self.check_tasks {
                        check.remove(&time, 1);
                    }
                    output.push_vec(time, data);
                    collected = true;
                }
                Ok(Err(e)) => {
                    self.in_flight -= 1;
                    return Err(e);
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        Ok(collected)
    }
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static> OperatorLogic for AsyncLogic<T, D1, D2> {
    type Inputs = CountedInput<T, D1>;
    type Outputs = CountedOutput<T, D2>;
    type Time = T;

    fn wants_call(&self, input: &Self::Inputs) -> bool {
        input.has_pending() || !self.pending_input.is_empty() || self.in_flight > 0
    }

    fn call(&mut self, input: &mut Self::Inputs, output: &mut Self::Outputs) -> Result<bool> {
        let collected = self.collect_results(output.handle())?;
        let handle = input.handle();
        let mut took = false;
        while let Some((time, data)) = handle.next() {
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            if let Some(check) = &self.check_input {
                check.add(&time, data.len());
            }
            self.pending_input.push_back((time, data));
            took = true;
        }
        self.spawn_pending();
        Ok(collected || took)
    }

    fn holds_work(&self) -> bool {
        !self.pending_input.is_empty() || self.in_flight > 0
    }

    fn waiting_for_async(&self) -> bool {
        self.in_flight > 0
    }

    fn checks_justification(&self) -> bool {
        false
    }
}

/// A `unary_async` operator.
#[allow(clippy::too_many_arguments)]
pub(crate) fn unary_async<T: Timestamp, D1: Send + 'static, D2: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    logic: AsyncLogicFn<T, D1, D2>,
    max_concurrency: usize,
    input: CountedInput<T, D1>,
    output: CountedOutput<T, D2>,
    tokio_handle: tokio::runtime::Handle,
) -> OperatorShell<AsyncLogic<T, D1, D2>> {
    let name = name.into();
    let (result_tx, result_rx) = tokio::sync::mpsc::unbounded_channel();
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    let check_input = input.check_port().map(|port| {
        crate::progress::check::StoreCheck::new(port, crate::progress::check::Store::AsyncInput)
    });
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    let check_tasks = output.check_port().map(|port| {
        crate::progress::check::StoreCheck::new(port, crate::progress::check::Store::AsyncTask)
    });
    let wake_handle = output.wake_handle().cloned();
    OperatorShell::new(
        name.clone(),
        index,
        stage_id,
        AsyncLogic {
            name,
            logic,
            max_concurrency: max_concurrency.max(1),
            in_flight: 0,
            result_tx,
            result_rx,
            pending_input: VecDeque::new(),
            tokio_handle,
            wake_handle,
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check_input,
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check_tasks,
        },
        input,
        output,
    )
}

// ---------------------------------------------------------------------------
// Branch, fork, ok/err: one input, two outputs
// ---------------------------------------------------------------------------

/// Routes each record to output 0 (predicate true) or output 1 (false).
///
/// N-way routing composes from several branches. Each record is moved, not
/// cloned (compare [`ForkLogic`]).
pub(crate) struct BranchLogic<T: Timestamp, D: Send + 'static> {
    predicate: Box<dyn FnMut(&T, &D) -> bool + Send>,
}

impl<T: Timestamp, D: Send + 'static> OperatorLogic for BranchLogic<T, D> {
    type Inputs = CountedInput<T, D>;
    type Outputs = (CountedOutput<T, D>, CountedOutput<T, D>);
    type Time = T;

    fn call(&mut self, input: &mut Self::Inputs, outputs: &mut Self::Outputs) -> Result<bool> {
        let (yes, no) = outputs;
        let mut did = false;
        while let Some((time, data)) = input.handle().next() {
            let mut true_session = yes.handle().session(time.clone());
            let mut false_session = no.handle().session(time);
            for item in data {
                if (self.predicate)(true_session.time(), &item) {
                    true_session.give(item);
                } else {
                    false_session.give(item);
                }
            }
            did = true;
        }
        Ok(did)
    }
}

/// A branch by `predicate`.
pub(crate) fn branch<T: Timestamp, D: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    predicate: impl FnMut(&T, &D) -> bool + Send + 'static,
    input: CountedInput<T, D>,
    true_output: CountedOutput<T, D>,
    false_output: CountedOutput<T, D>,
) -> OperatorShell<BranchLogic<T, D>> {
    OperatorShell::new(
        name,
        index,
        stage_id,
        BranchLogic {
            predicate: Box::new(predicate),
        },
        input,
        (true_output, false_output),
    )
}

/// Clones every record to both outputs.
pub(crate) struct ForkLogic<T: Timestamp, D: Clone + Send + 'static> {
    _marker: std::marker::PhantomData<fn(T, D)>,
}

impl<T: Timestamp, D: Clone + Send + 'static> OperatorLogic for ForkLogic<T, D> {
    type Inputs = CountedInput<T, D>;
    type Outputs = (CountedOutput<T, D>, CountedOutput<T, D>);
    type Time = T;

    fn call(&mut self, input: &mut Self::Inputs, outputs: &mut Self::Outputs) -> Result<bool> {
        let (left, right) = outputs;
        let mut did = false;
        while let Some((time, data)) = input.handle().next() {
            left.handle().push_vec(time.clone(), data.clone());
            right.handle().push_vec(time, data);
            did = true;
        }
        Ok(did)
    }
}

/// A fork.
pub(crate) fn fork<T: Timestamp, D: Clone + Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    input: CountedInput<T, D>,
    left: CountedOutput<T, D>,
    right: CountedOutput<T, D>,
) -> OperatorShell<ForkLogic<T, D>> {
    OperatorShell::new(
        name,
        index,
        stage_id,
        ForkLogic {
            _marker: std::marker::PhantomData,
        },
        input,
        (left, right),
    )
}

/// Applies a splitter to each record: `Ok(v)` goes to output 0, `Err(e)` to
/// output 1.
pub(crate) struct OkErrLogic<T: Timestamp, D: Send + 'static, O: Send + 'static, E: Send + 'static>
{
    splitter: Box<dyn FnMut(D) -> std::result::Result<O, E> + Send>,
    _marker: std::marker::PhantomData<fn(T)>,
}

impl<T: Timestamp, D: Send + 'static, O: Send + 'static, E: Send + 'static> OperatorLogic
    for OkErrLogic<T, D, O, E>
{
    type Inputs = CountedInput<T, D>;
    type Outputs = (CountedOutput<T, O>, CountedOutput<T, E>);
    type Time = T;

    fn call(&mut self, input: &mut Self::Inputs, outputs: &mut Self::Outputs) -> Result<bool> {
        let (ok, err) = outputs;
        let mut did = false;
        while let Some((time, data)) = input.handle().next() {
            let mut ok_session = ok.handle().session(time.clone());
            let mut err_session = err.handle().session(time);
            for item in data {
                match (self.splitter)(item) {
                    Ok(v) => ok_session.give(v),
                    Err(e) => err_session.give(e),
                }
            }
            did = true;
        }
        Ok(did)
    }
}

/// An ok/err split by `splitter`.
pub(crate) fn ok_err<T: Timestamp, D: Send + 'static, O: Send + 'static, E: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    splitter: impl FnMut(D) -> std::result::Result<O, E> + Send + 'static,
    input: CountedInput<T, D>,
    ok_output: CountedOutput<T, O>,
    err_output: CountedOutput<T, E>,
) -> OperatorShell<OkErrLogic<T, D, O, E>> {
    OperatorShell::new(
        name,
        index,
        stage_id,
        OkErrLogic {
            splitter: Box::new(splitter),
            _marker: std::marker::PhantomData,
        },
        input,
        (ok_output, err_output),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::communication::allocator::ChannelAllocator;
    use crate::dataflow::channels::envelope::{Envelope, Payload};
    use crate::dataflow::channels::pushpull::{Pull, Push};
    use crate::dataflow::schedulable::{ActivationOutcome, SchedulableOperator};
    use crate::progress::operate::{PortLedger, ProgressReporter};

    fn make_channel<T: Timestamp, D: Send + 'static>()
    -> crate::dataflow::channels::pushpull::ChannelPair<T, D, ()> {
        let mut alloc = ChannelAllocator::new();
        alloc.allocate()
    }

    /// A counted input on `pull` with a fresh ledger.
    pub(crate) fn input<T: Timestamp, D: Send + 'static>(
        pull: Box<dyn Pull<T, D>>,
    ) -> CountedInput<T, D> {
        CountedInput::new("test:input".into(), pull, PortLedger::new())
    }

    /// A counted output on `push` with a fresh ledger and reporter.
    pub(crate) fn output<T: Timestamp, D: Send + 'static>(
        push: Box<dyn Push<T, D>>,
    ) -> CountedOutput<T, D> {
        CountedOutput::new(
            "test:output".into(),
            push,
            PortLedger::new(),
            ProgressReporter::new(),
            None,
        )
    }

    fn drain<T: Timestamp, D>(pull: &mut Box<dyn Pull<T, D>>) -> Vec<(T, Vec<D>)> {
        let mut received = Vec::new();
        while let Some(env) = pull.pull() {
            if let Payload::Data { time, data } = env.payload {
                received.push((time, data));
            }
        }
        received
    }

    #[test]
    fn source_emits_all_data_in_order() {
        let ch = make_channel::<u64, i32>();
        let mut pull = ch.puller;
        let mut op = source(
            "source",
            1,
            StageId::new(0),
            vec![(0u64, vec![1, 2, 3]), (1u64, vec![4, 5])],
            output(ch.pusher),
            false,
        );
        assert_eq!(op.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(drain(&mut pull), vec![(0, vec![1, 2, 3]), (1, vec![4, 5])]);
    }

    #[test]
    fn unary_processes_data_and_counts_ports() {
        let ch_in = make_channel::<u64, i32>();
        let ch_out = make_channel::<u64, i32>();
        let mut push_in = ch_in.pusher;
        let mut pull_out = ch_out.puller;
        let consumed = PortLedger::new();
        let produced = PortLedger::new();
        let mut op = unary(
            "double",
            1,
            StageId::new(0),
            |input: &mut InputHandle<u64, i32>, output: &mut OutputHandle<u64, i32>| {
                while let Some((time, data)) = input.next() {
                    output.push_vec(time, data.into_iter().map(|x| x * 2).collect());
                }
                Ok(())
            },
            CountedInput::new("in".into(), ch_in.puller, consumed.clone()),
            CountedOutput::new(
                "out".into(),
                ch_out.pusher,
                produced.clone(),
                ProgressReporter::new(),
                None,
            ),
        );
        push_in.push(Envelope::data(0, vec![1, 2, 3])).unwrap();
        push_in.close();
        assert_eq!(op.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(drain(&mut pull_out), vec![(0, vec![2, 4, 6])]);
        assert_eq!(consumed.drain(), vec![(0, 3)]);
        assert_eq!(produced.drain(), vec![(0, 3)]);
    }

    #[test]
    fn unary_idle_when_no_input() {
        let ch_in = make_channel::<u64, i32>();
        let ch_out = make_channel::<u64, i32>();
        let mut op = unary(
            "noop",
            1,
            StageId::new(0),
            |_: &mut InputHandle<u64, i32>, _: &mut OutputHandle<u64, i32>| Ok(()),
            input(ch_in.puller),
            output(ch_out.pusher),
        );
        assert_eq!(op.activate().unwrap(), ActivationOutcome::Idle);
    }

    #[test]
    fn error_envelope_fails_the_operator() {
        let ch_in = make_channel::<u64, i32>();
        let ch_out = make_channel::<u64, i32>();
        let mut push_in = ch_in.pusher;
        let mut op = unary(
            "map",
            1,
            StageId::new(0),
            |input: &mut InputHandle<u64, i32>, output: &mut OutputHandle<u64, i32>| {
                while let Some((time, data)) = input.next() {
                    output.push_vec(time, data);
                }
                Ok(())
            },
            input(ch_in.puller),
            output(ch_out.pusher),
        );
        push_in
            .push(Envelope::error("upstream", "decode failed"))
            .unwrap();
        let error = op.activate().unwrap_err();
        assert!(error.to_string().contains("decode failed"), "{error}");
    }

    #[test]
    fn source_to_unary_to_sink_pipeline() {
        let ch1 = make_channel::<u64, i32>();
        let ch2 = make_channel::<u64, i32>();
        let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut src = source(
            "source",
            1,
            StageId::new(0),
            vec![(0u64, vec![10, 20, 30])],
            output(ch1.pusher),
            false,
        );
        let mut double = unary(
            "double",
            2,
            StageId::new(0),
            |input: &mut InputHandle<u64, i32>, output: &mut OutputHandle<u64, i32>| {
                while let Some((time, data)) = input.next() {
                    output.push_vec(time, data.into_iter().map(|x| x * 2).collect());
                }
                Ok(())
            },
            input(ch1.puller),
            output(ch2.pusher),
        );
        let sink_collected = Arc::clone(&collected);
        let mut snk = sink(
            "sink",
            3,
            StageId::new(0),
            move |time, data| sink_collected.lock().unwrap().push((time, data)),
            input(ch2.puller),
        );
        assert_eq!(src.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(double.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(snk.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(*collected.lock().unwrap(), vec![(0u64, vec![20, 40, 60])]);
    }

    #[test]
    fn branch_splits_by_predicate() {
        let ch_in = make_channel::<u64, i32>();
        let ch_true = make_channel::<u64, i32>();
        let ch_false = make_channel::<u64, i32>();
        let mut push_in = ch_in.pusher;
        let (mut pull_true, mut pull_false) = (ch_true.puller, ch_false.puller);
        let mut op = branch(
            "branch",
            1,
            StageId::new(0),
            |_t: &u64, x: &i32| x % 2 == 0,
            input(ch_in.puller),
            output(ch_true.pusher),
            output(ch_false.pusher),
        );
        push_in.push(Envelope::data(0, vec![1, 2, 3, 4])).unwrap();
        push_in.close();
        assert_eq!(op.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(drain(&mut pull_true), vec![(0, vec![2, 4])]);
        assert_eq!(drain(&mut pull_false), vec![(0, vec![1, 3])]);
    }

    #[test]
    fn ok_err_splits_results() {
        let ch_in = make_channel::<u64, std::result::Result<i32, String>>();
        let ch_ok = make_channel::<u64, i32>();
        let ch_err = make_channel::<u64, String>();
        let mut push_in = ch_in.pusher;
        let (mut pull_ok, mut pull_err) = (ch_ok.puller, ch_err.puller);
        let mut op = ok_err(
            "ok_err",
            1,
            StageId::new(0),
            |r: std::result::Result<i32, String>| r,
            input(ch_in.puller),
            output(ch_ok.pusher),
            output(ch_err.pusher),
        );
        push_in
            .push(Envelope::data(
                0,
                vec![Ok(1), Err("bad".to_string()), Ok(3)],
            ))
            .unwrap();
        push_in.close();
        assert_eq!(op.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(drain(&mut pull_ok), vec![(0, vec![1, 3])]);
        assert_eq!(drain(&mut pull_err), vec![(0, vec!["bad".to_string()])]);
    }

    #[test]
    fn fork_clones_to_both_outputs() {
        let ch_in = make_channel::<u64, i32>();
        let ch_left = make_channel::<u64, i32>();
        let ch_right = make_channel::<u64, i32>();
        let mut push_in = ch_in.pusher;
        let (mut pull_left, mut pull_right) = (ch_left.puller, ch_right.puller);
        let mut op = fork(
            "fork",
            1,
            StageId::new(0),
            input(ch_in.puller),
            output(ch_left.pusher),
            output(ch_right.pusher),
        );
        push_in.push(Envelope::data(0, vec![1, 2])).unwrap();
        push_in.close();
        assert_eq!(op.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(drain(&mut pull_left), vec![(0, vec![1, 2])]);
        assert_eq!(drain(&mut pull_right), vec![(0, vec![1, 2])]);
    }
}
