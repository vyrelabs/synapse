# Buffered Relay

## Purpose

Instead of pinging the underlying queue backend for every enqueue/dequeue op, which is expensive
and inefficient especially when it isn't co-located with the frontier.
To address this, the [`BufferedRelay`](./buffered.go) is an abstraction that uses pluggable
[`BufferPolicy`](./policy.go) to determine when URLs should be fetched/flushed from/to the
underlying producer/consumer.

