# Backend

## Purpose

It provides a fundamental abstraction layer for data persistence, queuing, and I/O communication
within the framework components. These abstractions decouple the framework's core logic from
specific infrastructure implementations.

Internally, the backend package provides the following core interfaces:

1. [**Store**](./store.go) defines a generic key-value interface for persistent data storage,
   supporting `Put`, `Get`, and `Delete` ops.

2. [**Cache**](./store.go) handles transient data with TTL, supporting `Put`, `Get`, `Purge` ops.

3. [**Queue**](./queue.go) provides abstraction for queue, decoupled into individual ends:
   [**Producer**](./queue.go) and [**Consumer**](./queue.go) and handles codec (serialization/deserialization).

4. [**Relay**](./relay/README.md) is the I/O communication bridge between components in
   the crawling lifecycle. It manages the dataflow b/w consumers and producers via pluggable
   strategies: [**Buffered**](./relay/buffered/README.md), [**Direct**](./relay/direct/direct.go),
   and [**Sequential**](./relay/sequential/sequential.go) relaying for different workloads

