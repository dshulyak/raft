Raft
---

Self-written raft version to explore subtle liveness details, such as described in
 [Raft does not Guarantee Liveness in the face of Network Faults](https://decentralizedthoughts.github.io/2020-12-12-raft-liveness-full-omission/)

Current progress:

- [x] raft leader election and replication
- [x] linearizable reads without writing to a log
- [x] pre-vote phase. Candidate will not disrupt a cluster if it is lagging behind, like after partition was healed.
- [x] leader lease. Candidate will not disrupt a cluster if follower saw an AppendeEntries during min election timeout from the current leader. In some schedulling scenarios pre-vote phase is not enough, and candidate will be able to disrupt a cluster anyway, such as if candidate is partially connected to some nodes, that will stall the cluster.
- [x] check quorum. Leader will step down if didn't receive replies from majority during min election timeout. Leader lease actually introduce a liveness issue where partially connected leader will prevent follower from voting for any other leader, therefore check quorum is needed to mitigate this issue.
- [ ] membership
- [ ] snapshots
- [ ] parallel logs write on a leader

#### Example

[Instructions](./example/README.md) to setup an example with metrics and chaos-mesh for experiements with partial and full network partitions.
