# Design: Document Reconnection Responsibility

**Item:** `net-doc-reconnect`
**Priority:** P2
**Status:** Design

## Problem

instancy has reconnection logic in `SharedTransport` (exponential backoff,
connection factory retry) but none of this is documented for users. The
`GUIDE.md`, `README.md`, and key struct doc comments are silent on:

- What happens when a TCP connection drops mid-dataflow
- Who is responsible for reconnection (library vs application)
- How the connection factory enables automatic reconnection
- What errors the application sees on permanent failure

## Changes

1. **Add "Connection Failure & Reconnection" section to GUIDE.md** covering:
   - SharedTransport automatic reconnect with backoff (100ms→1.6s, 5 attempts)
   - Connection factory role (if provided, library retries; if not, failure is permanent)
   - Application-level handling: `TransportError::ConnectionClosed`
   - Payload frames may be lost during reconnection window

2. **Add doc comments to `PeerConnection` and `TransportSession`** noting
   that these are pre-established connections with no built-in reconnection —
   reconnection is handled at the `SharedTransport` layer.

3. **Add doc comments to `SharedTransport` reconnect methods** summarizing
   the retry behavior inline.
