# Design: Lazy-Allocate Bounded Channels

**Item:** `mem-lazy-channels`
**Priority:** P1
**Status:** Design

## Problem

Bounded channels pre-allocate their internal `VecDeque` buffer at creation
time using `VecDeque::with_capacity(capacity)`. The default capacity is 1024.

A multi-worker dataflow with many exchange edges creates dozens of channels,
most of which may carry little or no data. Pre-allocating 1024 entries per
channel wastes memory — especially for wide fan-out topologies.

## Change

Replace `VecDeque::with_capacity(capacity)` with `VecDeque::with_capacity(4)` in all
channel constructors. The initial allocation of 4 covers typical minimum traffic
(data message + progress message + control messages) without triggering immediate
reallocation, while being much smaller than the default logical capacity of 1024.

The logical capacity limit (used for backpressure) is unchanged — only the initial
physical allocation is reduced. `VecDeque` grows via the standard doubling strategy
as data arrives, stabilizing at actual usage rather than the logical maximum.

### Sites to change

1. `dataflow/channels/bounded.rs:61` — main bounded channel
2. `communication/allocator.rs:96` — allocator local channel
3. `dataflow/channels/mock_network.rs:79` — mock byte channel

## Testing

- All existing tests must pass (backpressure behavior is unchanged)
- Clippy clean
