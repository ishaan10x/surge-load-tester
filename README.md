# Surge 

A highly concurrent, low-level HTTP load testing CLI tool written in C. 

Surge is designed to benchmark HTTP APIs by simulating thousands of concurrent connections. It utilizes a custom non-blocking I/O engine and lock-free thread pooling to maximize hardware utilization, capable of saturating network interfaces with a minimal resource footprint.

## Architecture & Engineering Decisions

* **Event Multiplexing (`epoll`):** Utilizes Linux `epoll` in Edge-Triggered (`EPOLLET`) mode. This prevents the kernel from continuously polling ready file descriptors, allowing the event loop to scale to tens of thousands of concurrent sockets per thread with `O(1)` event notification overhead.
* **Non-Blocking I/O State Machine:** Raw sockets are configured with `O_NONBLOCK`. Connections are managed via a custom state machine (`CONNECTING`, `WRITING`, `READING`, `DONE`) to prevent thread blocking during network latency.
* **Cache-Line Aligned Thread Pool:** Worker threads are spawned using `pthreads` (1 per physical CPU core). To prevent CPU cache invalidation and false sharing, the `ThreadContext` structures are explicitly aligned to the 64-byte hardware cache line boundary (`__attribute__((aligned(64)))`).
* **Lock-Free Metrics Aggregation:** Standard mutexes degrade concurrency under heavy load. Surge utilizes C11 `<stdatomic.h>` with `memory_order_relaxed` for thread-safe global metric counters. 
* **Histograms:** Instead of storing individual latencies in an `O(N)` memory array, latencies are bucketed into thread-local histogram arrays and aggregated at teardown, allowing for memory-constant `p50`, `p95`, and `p99` percentile calculations.

## Fragmented Mode (`-x`)

Standard load balancers handle clean, rapid requests efficiently. Surge includes a `-x` flag to simulate degraded network conditions. When enabled, Surge intentionally fragments the outbound TCP buffer using a thread-safe PRNG (`rand_r`), dripping the HTTP request payload byte-by-byte to exhaust thread-per-request backends (like embedded Tomcat/Spring Boot).

## Performance Benchmark

Benchmarked on an 8-core CPU against a local NGINX target over the loopback interface, bypassing network latency to test pure TCP/HTTP multiplexing throughput.

**Command:** `./surge -h 127.0.0.1 -p 8080 -c 10000 -t 8 -d 30`

* **Concurrency:** 10,000 connections
* **Total Requests:** 638,470
* **Throughput:** 21,202.23 req/sec
* **Error Rate:** 0.0004%

**Latency Percentiles:**
* `p50:` 205.20 ms
* `p95:` 344.00 ms
* `p99:` 631.20 ms

## Build Instructions

To build the docker image from the root directory:

**Command:** `docker build -t surge .`

## Usage Instructions

To run the load tester:

**Command:** `docker run --rm --network host surge -h <host> [options]`

Arguments go after the image name and are as follows:
* `-h <host>` : Target hostname or IP address (Required).
* `-p <port>` : Target port number (Default: 80).
* `-P <path>` : HTTP path to request (Default: /).
* `-c <count>` : Number of concurrent connections to maintain (Default: 1).
* `-t <count>` : Number of worker threads. Set this to match your physical CPU core count to maximize efficiency without causing context-switching overhead (Default: 1).
* `-d <secs>` : Duration of the load test in seconds (Default: 10).
* `-x` : Fragment the outbound TCP buffer using a randomized PRNG, drip-feeding the HTTP request to exhaust server thread pools.