## Auction Project – Remaining Work

- [ ] **Implement leader-to-backup replication:** Establish a gRPC connection from the leader to the backup and forward accepted bids using `ForwardBid()`.
- [ ] **Handle bid acknowledgments:** Ensure the leader returns `SUCCESS` only after the bid has been successfully replicated.
- [ ] **Implement backup synchronization:** Keep `highestBid` and `highestBidder` consistent between nodes.
- [ ] **Implement leader failure detection:** Detect when the leader crashes or becomes unreachable.
- [ ] **Implement leader failover:** Promote the backup to leader when the original leader fails.
- [ ] **Handle client reconnection:** Ensure clients can continue bidding after failover.
- [ ] **Implement auction closing:** Handle the auction deadline and return `CLOSED` through `Result()`.
- [ ] **Test the distributed system:** Verify simultaneous bids, replication, leader crashes, recovery, and correct auction results.

**Important:** Registering the replication service is not enough. The leader must explicitly call `ForwardBid()` to replicate bids.

**Current progress:** Basic auction RPCs, leader/backup startup, and gRPC service registration are implemented or drafted.
