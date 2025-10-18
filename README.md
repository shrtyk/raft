# Raft in Go (MIT 6.5840)

This repository contains an implementation of the Raft consensus algorithm in Go for the MIT 6.5840 Distributed Systems course.

## Implemented Features

- [x] **3A**: Leader Election and Heartbeats
- [x] **3B**: Log Replication
- [x] **3C**: Persistence
- [x] **3D**: Log Compaction with Snapshots

## Running Tests

The core Raft implementation is in the `raft1/` directory. To run the tests:

```bash
go test
```

To run tests for a specific part (e.g., 3A), you can use the `-run` flag:

```bash
go test -run 3A
```
