# Distributed Systems Mandatory Activity 4

A Go implementation of the Ricart–Agrawala algorithm using gRPC. Three peer nodes coordinate access to an emulated critical section: a log message followed by a 10-second sleep.

## Prerequisites

- Go 1.27.0 or later, as specified in `go.mod`.
- Three terminals on the same machine.
- TCP ports 50051, 50052, and 50053 available.

Generated protobuf files are included, so `protoc` is not needed to run the system. Go downloads the dependencies on the first run if they are not already cached.

## Start the nodes

Open the `Ass4_Jan` directory in each terminal. Start one node per terminal using the following commands.

**Terminal 1**

```sh
go run . -nodeID 1 -port :50051
```

**Terminal 2**

```sh
go run . -nodeID 2 -port :50052
```

**Terminal 3**

```sh
go run . -nodeID 3 -port :50053
```

Peer discovery uses a hardcoded list of localhost ports. Use exactly these three unique node IDs and port assignments; the program does not validate this mapping. All three nodes must be running before requesting access.

## Request access

Press **Enter** in a node's terminal to request the critical section. Press Enter in the other terminals while the first request is active to demonstrate contention.

A node enters only after receiving permission from both peers. Competing requests are ordered by their Lamport request timestamp, with the lower node ID breaking ties. A node defers replies while holding the critical section, or while its own pending request has priority. Deferred replies are sent when it releases the critical section.

Each visit lasts approximately 10 seconds. For example, a successful request at node 1 produces these messages, with send and reply order potentially varying:

```text
Node 1: REQUESTING CS, timestamp=2
Node 1 sending REQUEST to Node 2
Node 1 sending REQUEST to Node 3
Node 1 received REPLY from Node 2
Node 1 received REPLY from Node 3
Node 1 is accessing the critical section
Critical section is released by node 1
```

The actual logs also include date and time prefixes. Later requests may have different timestamps.

## Run with the race detector

Add `-race` immediately after `go run` in each startup command:

```sh
go run -race . -nodeID 1 -port :50051
```

Repeat for nodes 2 and 3 using their corresponding IDs and ports.

## Failure behaviour

The system uses fixed membership and assumes that all three nodes remain available. Each permission RPC has a 60-second deadline. If any peer RPC fails or times out, the node abandons that attempt, returns to the released state, and releases its deferred replies. It does not automatically retry; press Enter again after all peers are available.

## Stop the system

Press **Ctrl+C** in each terminal.
