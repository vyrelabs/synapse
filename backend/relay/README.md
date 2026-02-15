# Relay

## Purpose

It is the core abstraction for I/O communication across components in the crawling lifecycle that
basically manages the dataflow b/w consumer and producer.

During the ingestion phase, it receives data from the consumer, the framework processes it, and
sends to the downstream storage medium via the producer. It's upto the caller or external
implementations to use it as _kind-of_ stream processor _subset_.

Internally, it provides three pluggable implementations (based on different scaling requirements):

For concurrent workloads:

1. [**Buffered Relay**](./buffered/buffered.go) batches urls locally before enqueuing.
   It maintains internal buffers for receiving/sending urls from/to the consumer end.
   Once the threshold are reached (based on the configured [`BufferPolicy`](./buffered/policy.go)),
   it performs bulk enqueue/dequeue operations to/from the underlying queue.

2. [**Direct Relay**](./direct/direct.go) provides a direct pass-through to the underlying queue.
   Basically, it synchronously performs enqueue/dequeue operations to/from the underlying queue
   without any intermediate buffering. This is useful when the queue is co-located with the frontier.

For non-concurrent workloads:

3. [**Sequential Relay**](./sequential/sequential.go) as the name suggests, synchronously consumes
   data from the underlying queue and dispatches to the producer end without intermediate buffering.
   This is useful for synchronous workloads or testing purposes.

