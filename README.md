# MiniDB: Distributed Key-Value Store

MiniDB is a distributed key-value database built from scratch in Python. It is designed for educational purposes to demonstrate core concepts of distributed systems, including:

- **Sharding**: Consistent hashing for data distribution.
- **Replication**: Leader-based replication with configurable consistency levels.
- **Consensus**: Raft-inspired leader election.
- **Gossip Protocol**: Cluster membership and failure detection.
- **Persistence**: AOF (Append-Only File) and Snapshot persistence.
- **Anti-Entropy**: Merkle trees and version vectors for data repair.

## Project Structure

- `minidb/`: Core source code.
- `examples/`: Example scripts demonstrating usage.
- `tests/`: Test suite.

## Getting Started

### Prerequisites

- Python 3.8+

### Installation

No installation required. You can run the scripts directly from the source.

### Running a Demo Cluster

To reproduce the full cluster setup with 3 nodes:

```bash
python examples/demo_cluster.py
```

This will:
1. Start 3 database nodes locally.
2. Form a cluster.
3. Elect a leader.
4. Perform write and read operations.
5. Demonstrate failover and recovery.

### Running Tests

To verify the system functionality:

```bash
python tests/test_cluster.py
```

## Architecture

The system mimics the architecture of real-world databases like Cassandra and Redis Cluster.

- **Storage Engine**: In-memory dictionary with TTL support.
- **Network**: Custom TCP protocol (text-based, similar to RESP).
- **Cluster**: Decentralized P2P mesh for gossip, with a leader for strong consistency operations.

## Contributing

Feel free to explore the code in `minidb/` to understand the implementation of distributed primitives.
