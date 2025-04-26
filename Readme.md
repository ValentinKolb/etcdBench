# etcd Benchmark

A benchmarking tool for comparing etcd performance under various configurations.

## Requirements

- Go 1.20+
- Docker and Docker Compose

## Quick Start

### Start etcd Cluster

```bash
docker compose up -d
```

This starts a multi-node etcd cluster locally.

### Run Benchmark

```bash
go run main.go
```

### Options

```
--value-size       Size of values in bytes (default: 128)
--large-value-size Size for SET-LARGE test in KB (default: 512)
--keys             Number of different keys (default: 100)
--threads          Number of threads (default: 10)
--csv              Path to save results as CSV
--sample-rate      Sample rate for latency (1 in N ops, default: 100, 0 to disable)
--skip             Benchmarks to skip (comma separated - e.g. "set,get")
--endpoints        etcd endpoints (default: "localhost:2379,localhost:2380,localhost:2381")
--timeout          Operation timeout in seconds (default: 5)
```
## Shutdown

```bash
docker compose down
```

## Results

Results show operations per second, latency (P50/P95/P99), and memory usage, allowing for direct comparison with other key-value stores like [dKV](https://github.com/ValentinKolb/dKV).