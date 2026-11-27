# core

The order and ledger core of Open Exchange: one Java Aeron Cluster service that runs the order lifecycle, risk checks, holds and the money ledger (balances, holds, settlement) as a deterministic, replicated state machine.

Status: nightly prerelease. Not for production use.

User documentation (architecture, configuration, operations): https://openexchange.dev/docs

## Where it fits in v1

v1 is five repositories:

| repo | role |
|---|---|
| [match](https://github.com/openexch/match) | matching engine, its own Aeron Raft cluster (Java) |
| **core** (this repo) | order and ledger core cluster (Java) |
| [cluster-kit](https://github.com/openexch/cluster-kit) | cluster machinery shared by both engines (Java) |
| [cluster-tools](https://github.com/openexch/cluster-tools) | C++ cluster client, ME forwarder, settlement bridge, projectors |
| [tools](https://github.com/openexch/tools) | probe and test tooling |

v1 has no user-facing gateway yet.

## Modules

| module | what it is | jar |
|---|---|---|
| `core-common` | shared kernel: money value objects and the SBE wire codecs, generated at build time from `src/main/resources/sbe/money-schema.xml` (schema id 2, version 9; version 9 adds `genesisId` to OrderEventsEnd and CoreConfigReport, the recorded CoreConfig's genesis id, 0 before any CoreConfig is recorded) and `money-journal-schema.xml` (schema id 3, version 2: every record carries `logPosition`, the core log position of the entry that produced it) | library |
| `core-cluster` | the engine: domain model, deterministic engine, Aeron cluster service. Entry point `com.openexchange.core.Main` | `core-cluster/target/core-cluster.jar` |
| `core-bridge` | core client tools: `dump` (the money check's read-only ledger dump) and `set-money-journal on\|off`. The settlement bridge is the C++ `ct-bridge` in cluster-tools. Entry point `com.openexchange.core.bridge.BridgeMain` | `core-bridge/target/core-bridge.jar` |
| `core-projector` | follows the money journal (an Aeron Archive recording) and projects every money movement into PostgreSQL, resumable from the PostgreSQL high-water mark. Entry point `com.openexchange.core.projector.ProjectorMain` | `core-projector/target/core-projector.jar` |
| `core-loadgen` | money-path benchmark driver: hold, ME order, trade, settlement journal, bridge, settle, with latencies measured from the scheduled send time. Entry point `com.openexchange.core.loadgen.MoneyLoadGenerator`; the order load generator `com.openexchange.core.loadgen.CoreOrderLoadGenerator` (see Order load generator) | `core-loadgen/target/core-loadgen.jar` |

SBE schema ids are unique across the repos: `sbe-schema-ids.txt` lists every id in use (the same file in core, match, cluster-tools, gateway and tools), and `SbeSchemaIdRegistryTest` (core-common) fails when an id is listed twice or a schema file here is missing from the list or disagrees with its line. A new schema takes a free id in that file, in every repo, first.

`grafana/` holds Prometheus scrape config and alert rules.

## Build and test

Requirements: Java 21 and Maven.

Two dependencies come from other repos, at the exact versions pinned in `pom.xml`: `com.openexchange:cluster-kit` (`cluster-kit.version`) and `com.match:match-common` (`match.version`, `match.journal.version`). Either install them into `~/.m2` from local checkouts of cluster-kit and match, or resolve them from GitHub Packages with the CI settings file, which reads `GITHUB_ACTOR` and `GITHUB_TOKEN` (a token with `read:packages`):

```bash
export GITHUB_ACTOR=<your-github-user> GITHUB_TOKEN=<token>
mvn -s .mvn-settings.xml clean verify      # what CI runs: unit tests, determinism corpus, cluster boot smoke test
```

With the dependencies already in `~/.m2`, the Makefile targets work without `-s`:

| target | runs |
|---|---|
| `make build` | `mvn clean package -DskipTests` |
| `make test` | `mvn test` |
| `make determinism` | the in-process determinism, snapshot-replay, conservation and recovery tests |
| `make update-goldens` | regenerates the determinism golden files after an intentional output change; review the diff before committing |
| `make integration` | boots a real single-node embedded-driver cluster in-process and drives it over the wire |
| `make nightly` | the slow tests (100k-account restore, 1M sizing, large scenario, the long replica process tests) that CI runs on its nightly schedule |

Pull requests run `mvn verify -Pfast`, which leaves the long replica process tests to the nightly job (the shadow puller, leader-gate, re-bootstrap, member-replica pull and the two external-driver pull (`-Dcore.pull.external=true`) stories run in the nightly job with three members, and the 100k-account pull with `-Dcore.pull.100k=true`); a plain `mvn verify` runs everything. The nightly replica load rows (lag, supplier loss, leader loss, replica kill, wipe and re-bootstrap under order load) take every bound as a `-Dcore.lag.*` property; the supplier-loss bound `core.lag.peerloss.bound.s` defaults to 30 s because the recovery is the supplier's liveness timeout (about 3 s), the backup's 12 s cool-down, one backup query and the catch-up of the bytes written meanwhile, measured at 16 to 28 s on the dev box; 20 s failed one run in four. `integration` and `nightly` launch clusters. On a machine that also runs a live engine, pin them to spare cores, for example `taskset -c 20-23 make integration`.

The feed harness jar used by cluster-tools' compatibility tests is built with `mvn -s .mvn-settings.xml -pl core-cluster -am -Pfeed-harness -DskipTests package` and published on demand by `.github/workflows/feed-harness.yml` (run it by hand on the commit to publish) as the asset `core-feed-harness-<sha>.jar` of the single `feed-harness` prerelease.

## Configuration

Every setting is read from the environment first, then from a JVM system property, else a default. The sources of truth are:

- `core-cluster/src/main/java/com/openexchange/core/infrastructure/persistence/AeronCluster.java`: cluster membership and state directory.
- `core-cluster/src/main/java/com/openexchange/core/infrastructure/TransportConfig.java`: media driver and transport.
- `core-cluster/src/main/java/com/openexchange/core/infrastructure/persistence/SnapshotModeSettings.java`: snapshot stub and restore settings, each with its default and allowed range.
- `core-bridge/.../BridgeConfig.java` for the client tools and `core-projector/.../ProjectorConfig.java` for the projector.

The main engine settings:

| variable | default | meaning |
|---|---|---|
| `CLUSTER_NODE` | `0` | this member's id |
| `CLUSTER_ADDRESSES` | `localhost` | comma-separated member hosts, in member-id order |
| `CLUSTER_PORT_BASE` | `9300` | base of the member's port range |
| `CLUSTER_PORT_STRIDE` | `0` | per-member port offset; set `100` when several members share one host (read by cluster-kit) |
| `BASE_DIR` | `./core-node<id>` | cluster and archive state, the durable ledger. A path under `/tmp` or `/dev/shm` logs a warning at startup |
| `TRANSPORT_DRIVER_MODE` | `embedded` | `embedded`, or `external` to attach to a running `aeronmd` |
| `AERON_DIR` | Aeron default + `-core-<id>-driver` | media driver directory in external mode |
| `TRANSPORT_IDLE_MODE` | `backoff` | `busy_spin` for dedicated cores |
| `TRANSPORT_INTERFACE`, `TRANSPORT_MTU` (`8192`), `TRANSPORT_TERM_LENGTH` (`16m`), `TRANSPORT_LOG_TERM_LENGTH` (`64m`), `TRANSPORT_SNAPSHOT_TERM_LENGTH` (`64m`) | | transport tuning |
| `TRANSPORT_LOG_RCV_WINDOW` | min(16m, term/2), logged at boot | the log channel's receiver window (`rcv-wnd`), 64k..`TRANSPORT_LOG_TERM_LENGTH`/2. Unset, it derives min(16m, `TRANSPORT_LOG_TERM_LENGTH`/2) (a 16m term gets 8m, the fixture's 1m term 512k, 32m and 64m terms 16m) and the boot guard's line names it: `[BOOT-GUARD] log channel <uri> rcv-wnd 8m derived (min(16m, term/2) of 16m): ...`; a term below 128k cannot derive a legal window and refuses. An explicit value is taken as given (`rcv-wnd <w> set`); above half the term it refuses the boot |
| `TRANSPORT_LOG_SO_RCVBUF`, `TRANSPORT_LOG_SO_SNDBUF` | `16m` | the log channel's socket buffers (64k..1g); `SO_RCVBUF` below the window refuses the boot |
| `TRANSPORT_CONSENSUS_TERM_LENGTH` | `1m`, logged at boot | the consensus channel's term (members and the replica). Without it the channel took the driver's term (16m): two publications and two images of 48 MB per member of three. A power of two in [64k, 1g]; anything else refuses the boot (`[BOOT-GUARD] consensus term 1m default (aeron:udp?term-length=1m)`). Consensus messages are under 300 B, so even 64k carries them |
| `ARCHIVE_SEGMENT_LENGTH` | max(64m, `TRANSPORT_LOG_TERM_LENGTH`), logged at boot | the archive's segment file length (members and the replica). A log purge removes whole segments only, so a log keeps up to one segment below the floor; a small box or a test fixture sets it down (the docker fixture runs 1m) so its logs actually purge. A power of two in [64k, 1g] and at least `TRANSPORT_LOG_TERM_LENGTH` (Aeron would raise the log's segments to the term silently); anything else refuses the boot. The boot guard's line names it: `[BOOT-GUARD] archive segment 1m set (1048576 B; ...)`. `-Daeron.archive.segment.file.length` is not read: the core sets the length explicitly |
| `CORE_DRIVER_DIR_ACTIVE_MS` | `10000` | embedded mode: a driver directory whose heartbeat is at most this old belongs to a live driver and refuses the boot (2000..600000); external mode ignores it |
| `TRANSPORT_DRIVER_CLIENT_LIVENESS_MS` | `5000` | embedded mode: the member driver's client liveness timeout (1000..600000). It is also the longest whole-process pause (a frozen VM, a stopped container) a member survives: after a longer stall the member's own Aeron clients close themselves and the member exits 1 for a supervised restart. External mode takes `aeron.client.liveness.timeout` from the aeronmd's properties instead |
| `TRANSPORT_DRIVER_PUBLICATION_UNBLOCK_MS` | `10000` | embedded mode: the member driver's publication unblock timeout (2000..1200000); it must exceed `TRANSPORT_DRIVER_CLIENT_LIVENESS_MS` or the boot refuses (Aeron's rule). External mode: `aeron.publication.unblock.timeout` |
| `BALANCE_FEED_CHANNEL`, `BALANCE_FEED_STREAM_ID` | unset (off) | optional balance update side channel |
| `CORE_LEADER_HEARTBEAT_INTERVAL_MS` | `100` | how often the leader sends a heartbeat |
| `CORE_LEADER_HEARTBEAT_TIMEOUT_MS` | `1000` | how long followers wait without a heartbeat before they start an election; at least 3 x the interval |
| `CORE_ELECTION_TIMEOUT_MS` | `1000` | election round timeout |
| `CORE_NODE_PORT` | computed: `CLUSTER_PORT_BASE + node id x CLUSTER_PORT_STRIDE + 6` | HTTP probe port (`/health`, `/ready`, `/metrics`): the same on every host at stride 0. Optional; a value that disagrees with the computed port refuses the boot |

The log channel's socket buffers are probed at boot in both driver modes: when the kernel grants less than `TRANSPORT_LOG_SO_RCVBUF` / `TRANSPORT_LOG_SO_SNDBUF`, the member exits 1 with `CORE BOOT GUARD:` naming `net.core.rmem_max` / `net.core.wmem_max`, the current and the needed value. Every member host needs `net.core.rmem_max` and `net.core.wmem_max` >= 16777216 with the defaults (`sysctl -w`, persisted in `/etc/sysctl.d`). In external mode the URI's `rcv-wnd`, `so-rcvbuf` and `so-sndbuf` override the aeronmd's own defaults for the log image. A restart within `CORE_DRIVER_DIR_ACTIVE_MS` of a kill of the member's own embedded driver refuses until the bound passes: the process manager's restart delay must cover it.

The three timing settings decide the leaderless window after a leader dies (about heartbeat timeout plus one election) and how often load or a GC pause on a healthy leader is taken for a death. No client is acked before a quorum commits and applies its command, so a failover never loses an acked command; during the window ingress is refused and clients retry with their request id. Every member of a cluster must use the same values.

### Log retention and catch-up

The pruner and the cluster watchdogs come from cluster-kit; the core arms them on every member in both driver modes, with no setting of its own. A value outside its range refuses the boot. The retention floor applies when `SNAPSHOT_PRUNE_ENABLED` is on (the default). Details in cluster-kit's README ("Log retention and a member that was away", "Rejoin and peer reachability").

| variable | default | meaning |
|---|---|---|
| `RETENTION_FLOOR` | `apply` | `apply`: a member never purges log that another member may still need (the leader holds its purge at the lowest member position, a follower at the leader's log start). `report` only logs the floor |
| `RETENTION_MEMBER_WAIT_MS` | `1800000` | how long the floor waits for a member that does not move before the purge goes on without it (1000..604800000) |
| `RETENTION_MEMBER_WAIT_BYTES` | `2147483648` | how far behind the newest snapshot a member may fall before the floor stops waiting for it (1048576..1099511627776) |
| `RETENTION_PROBE_TIMEOUT_MS` | `5000` | bound on one read of the leader's log start from the leader's archive (100..60000) |
| `RETENTION_RECHECK_MAX_MS` | `60000` | longest wait between re-checks of a retention floor that stopped the last purge pass; a floor that rose is purged behind at once, without a new snapshot (1000..3600000) |
| `RETENTION_OFFBOX` | `off` | `required`: the purge never passes this member's off-box watermark (the position up to which the log is durably stored off the cluster's hosts), and a watermark that is missing, invalid, from another cluster or above the commit position holds the whole log until a correct one is set or an operator releases it. `report`: evaluated, logged and exported, not applied. `off` with a watermark file present refuses the start |
| `RETENTION_OFFBOX_ALARM_BYTES` | `2147483648` | log bytes held for the off-box copy above which every purge pass prints a CRITICAL line; an alarm, not a limit (1048576..1099511627776) |
| `RETENTION_OFFBOX_STALE_MS` | `3600000` | age of the watermark file above which every purge pass prints a CRITICAL line; it does not change the floor (1000..604800000) |
| `ENGINE_CATCHUP_REFUSALS` | `3` | failed catch-up attempts in a row after which a member reads the leader's log start; a member below it exits 1 (1..1000) |
| `ENGINE_CATCHUP_REFUSAL_WAIT_MS` | `30000` | the time bound next to the count: catch-up failing this long triggers the same check (1000..3600000) |
| `ENGINE_REJOIN_PEER_WINDOW_MS` | `10000` | how long a gap in the evidence of a reachable quorum is bridged before the rejoin watchdog's clock is cleared (1000..600000); keep it at or above the driver's publication connection timeout. `ENGINE_REJOIN_WAIT_MS` must exceed it or the boot refuses |
| `ENGINE_LEADER_ELECT_WAIT_MS` | `300000` | how long a leader elect may stay in LEADER_LOG_REPLICATION or LEADER_READY (Aeron gives these no timeout) while a quorum of members is reachable before it exits 1 for a supervised restart (10000..3600000); must exceed `ENGINE_REJOIN_PEER_WINDOW_MS` |
| `ENGINE_REJOIN_PROBE_TERM_LENGTH` | `65536` | term length in bytes of the rejoin watchdog's idle peer probe publications, opened sparse on their own term instead of the consensus channel's (a power of two, 65536..1073741824) |
| `ENGINE_REJOIN_PROBE_STREAM_ID` | `4306` | stream id of the rejoin watchdog's participation subscription on each member's own consensus endpoint, opened once the consensus module runs; the peer probes publish to it, so a member still loading its snapshot reads unreachable to its peers (1..2147483647, must differ from the consensus stream id; use the same value on every member) |

Settings renamed from earlier releases (for example `ASSETS_ROLE` to `CORE_ROLE`, `AE_AUTH_CREDENTIAL` to `CORE_AUTH_CREDENTIAL`, `BRIDGE_AE_PORT_BASE` to `BRIDGE_CORE_PORT_BASE`, `RCS_MODE` to `SNAPSHOT_MODE`) are refused: a process that still sees an old name, as a variable, property or flag, exits 1 and names the new one. The table is `core-common/src/main/java/com/openexchange/core/config/RetiredSettings.java`.

Ports are `CLUSTER_PORT_BASE + node id * CLUSTER_PORT_STRIDE + offset`, with offsets 1 archive control, 2 client ingress, 3 member, 4 log, 5 catchup transfer. With the defaults, clients connect to port 9302.

#### A copy off the cluster's hosts (`RETENTION_OFFBOX`)

With `RETENTION_OFFBOX=required` a member purges to the lowest of its own floor, the retention floor and its off-box watermark: the position, in bytes of the cluster log, up to which the log is durably stored off every host of the cluster. The watermark is a file on the member (`<clusterDir>/retention/offbox-watermark`) written by a command run on that member's host; this jar carries it:

```
java -cp core-cluster.jar com.openexchange.cluster.OffBoxWatermarkTool set --cluster-dir <dir> --engine core --cluster-id <n> --genesis-id <g> --log-position-bytes <p> --source <name>
```

`release`, `show` and `clear` are the other commands; the file format, the states and the exit codes are in the cluster-kit README. A lost, invalid or foreign watermark, or one above the member's commit position, holds the whole log: there is no wait and no give-up, only a correct `set` or an operator's `release`. `/ready` stays 200 while a member holds; the hold is the pruner's error, a CRITICAL line on every purge pass, and the gauges `core_retention_offbox_mode`, `core_retention_offbox_state`, `core_retention_offbox_position_bytes`, `core_retention_offbox_held_bytes`, `core_retention_offbox_age_ms` and `core_retention_offbox_released_bytes`. The file must carry `engine=core`, the member's cluster id and the genesis id of its recorded `CoreConfig`; a member that has not adopted a genesis yet is unbound and holds.

Turn it on only when every member runs a build that knows the setting: write the watermark on every member, then restart each with `RETENTION_OFFBOX=required`. To turn it off, `clear` the file on the member and restart with `off`; `off` with a file present refuses the start (exit 1).

#### Journal retention (`JOURNAL_RETENTION_MONEY`, `JOURNAL_RETENTION_ORDER`)

Each member keeps its money journal (stream 4101) and its order journal (stream 4102) as recordings in its own archive, and every process run opens a new recording. With the mode `readers` a member purges them itself, on its own thread (`journal-retention`, cluster-kit's `JournalRetention`): a record goes only when every registered reader has durably applied it, an active recording keeps two whole archive segments below its head, and a stopped recording of an earlier run is deleted whole once every reader has applied it and a newer recording has passed it. The tail the restore rules read is the same after a pass as before it. This does not depend on `SNAPSHOT_PRUNE_ENABLED`.

| variable | default | meaning |
|---|---|---|
| `JOURNAL_RETENTION_MONEY`, `JOURNAL_RETENTION_ORDER` | `off` | `off`: nothing of that journal is ever purged. `report`: evaluated, logged and exported, nothing purged. `readers`: purge behind the registered readers |
| `JOURNAL_RETENTION_REQUIRED_READERS_MONEY`, `JOURNAL_RETENTION_REQUIRED_READERS_ORDER` | empty | reader names, separated by commas, that must have a valid file on the member; one that has none holds the journal |
| `JOURNAL_RETENTION_POLL_MS` | `60000` | the interval between two passes (1000..3600000) |
| `JOURNAL_RETENTION_PASS_TIMEOUT_MS` | `10000` | the budget of one pass over one journal (1000..600000) |
| `JOURNAL_RETENTION_ALARM_BYTES` | `2147483648` | bytes of one journal a member holds above which every pass prints a CRITICAL line; an alarm, not a limit (1048576..1099511627776) |
| `JOURNAL_RETENTION_STALE_MS` | `3600000` | age of the oldest reader file above which every pass prints a CRITICAL line; it does not change the bound (1000..604800000) |

A reader is registered by a file on each member (`<clusterDir>/retention/journal-readers/<money|order>/<reader>`), written by a command run on that member's host; this jar carries it:

```
java -cp core-cluster.jar com.openexchange.cluster.journal.JournalReaderTool set --cluster-dir <dir> --engine core --cluster-id <n> --genesis-id <g> --journal money|order --reader <name> --applied-journal-seq <s> --source <name>
```

The first `set` registers the reader (0 is legal) and is done before the reader first starts; later ones deliver its cursor: every record of this genesis with `journalSeq` at or below the value is durably applied by that reader. `release`, `remove` and `show` are the other commands; the file format, the states and the exit codes are in the cluster-kit README ("Journal retention"). A journal with no reader file, with an invalid or foreign file, or with a reader above the member's own journal tail is held: there is no wait and no give-up, so a reader that died holds the journal until an operator's `release` or `remove`. With `RETENTION_OFFBOX=required` no record is purged whose log position is above the off-box watermark. The money journal's records carry no epoch, so its identity is the reader file's genesis id against the member's; the order journal's records carry the genesis id and a recording of another genesis is never touched.

`/ready` stays 200 while a member holds; under `readers` the hold is named in the detail line of `/health` (`journal-retention: ...`), on a CRITICAL line (`[JOURNAL-RETENTION] CRITICAL: ...`) and by the gauges, each labelled `journal="money"` or `journal="order"`: `core_journal_retention_mode`, `_state`, `_safe_seq`, `_tail_seq`, `_retained_bytes`, `_recordings`, `_recordings_foreign`, `_recordings_anomaly`, `_oldest_reader_age_ms`, `_purged_bytes_total`, `_purge_failures_total`, `_last_pass_age_ms`, and per reader `core_journal_retention_reader_applied_seq` and `_reader_released_seq`. `_retained_bytes` is exported under `off` too. The thread itself is `core_journal_retention_agent_state` (0 not running, 1 running, 2 the last pass failed, 3 dead until the member restarts, 4 did not start) with `core_journal_retention_pass_failures_total` per journal; one journal's failing pass does not stop the other's. `_recordings_anomaly` counts recordings that cannot be cut as far as their readers allow: a `journalSeq` that does not rise, or an unreadable record at the recording's start or at a segment base. A history reader that starts below what the members still hold (an empty projection database, a rebuild) finds a gap and stops: after retention the projection database is the only copy on the cluster's hosts of history below the bound. With age-based retention of the money history on as well (the projection's own delete, see "Money projector"), movements older than its window exist nowhere on the hosts, neither in a journal nor in the projection; only their per-asset totals are kept.

Turning it on, in this order, on a build every member runs:

1. Roll every member with both modes unset (`off`): nothing changes, and `core_journal_retention_retained_bytes` and `core_journal_retention_agent_state` appear.
2. Register every reader on every member (the first `set`, with its current cursor) and start whatever delivers the cursors.
3. Set `JOURNAL_RETENTION_REQUIRED_READERS_MONEY` and `JOURNAL_RETENTION_REQUIRED_READERS_ORDER` to the registered reader names on every member, so a reader file that is lost holds the journal instead of releasing it.
4. Set both modes to `report` on every member. Per member, `core_journal_retention_state` must be 0, `_safe_seq` must follow the readers' cursors, and the `report: would purge` lines must name what you expect.
5. `JOURNAL_RETENTION_ORDER=readers` only once the order projector has been tested against a member that purges under it (a purged source is left for another, a resume position below a recording's new start is dropped). Until then the order journal stays at `report`. Start with one follower, since readers prefer followers, and watch the projector for a halt or a gap before the other members follow.
6. `JOURNAL_RETENTION_MONEY` stays at `report` until it is confirmed on the installation that a genesis wipes the member's archive directory: the money journal's records carry no epoch, so recordings a previous genesis left in the same archive would be read as this genesis'.

Alert on the gauges and on their absence: a series that is missing (a member on an older build, an endpoint that does not answer, an agent that did not start) must page like a bad value. At least: `core_journal_retention_agent_state` absent or not 1; `core_journal_retention_last_pass_age_ms` absent or above three poll intervals; under `readers`, `core_journal_retention_state` absent or not 0 or 2; `core_journal_retention_recordings_anomaly` above 0; `core_journal_retention_pass_failures_total` rising; and `core_journal_retention_mode` 0 with `_retained_bytes` above the alarm bound.

### Order journal

Every member records one 216-byte record per order event (schema id 5, `core-common/src/main/resources/sbe/order-journal-schema.xml`) on stream 4102 in its own archive: journalSeq equals the event sequence, the epoch is the recorded CoreConfig genesis id, and each fill names its ME record. History readers follow it over the archive; the engine writes it after the event is applied, through a ring and a writer agent, so the service thread never waits on the archive while the ring has room.

| variable | default | meaning |
|---|---|---|
| `CORE_ORDER_JOURNAL_RING_BYTES` | `16777216` | the ring between the service thread and the writer (at least 2048) |
| `CORE_ORDER_JOURNAL_TERM_LENGTH` | `4m` | term length of the journal publication |
| `CORE_ORDER_JOURNAL_RING_ALARM_PCT`, `CORE_ORDER_JOURNAL_RING_ALARM_MS` | `50`, `5000` | readiness closes while, for the ALARM_MS, the ring stays above PCT full, or records wait and the archive-confirmed `recordedSeq` does not move. `PCT=100` switches off only the fill alarm |
| `CORE_ORDER_JOURNAL_STALL_LOG_MS`, `CORE_ORDER_JOURNAL_WRITER_IDLE_MAX_US` | `1000`, `1000` | stall log interval; the writer's longest idle sleep |
| `CORE_ORDER_JOURNAL_CLOSE_DRAIN_MS` | `2000` | a graceful stop drains the ring for at most this long; a kill does not drain |
| `CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS` | `10` | the member archive's replay cap |
| `CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS` | `2` | replays kept free for cluster catch-up; readers' budget is the cap minus this (shown on `/health`). The archive has one cap for all clients, so this is a budget readers hold to, not a slot the archive sets aside |
| `CORE_ARCHIVE_REPLAY_PROBE_STREAM` | `0` | the replay stream whose open replays `core_archive_replays_active` counts (0: the gauge reads -1) |

Restore: a SKIP whose image event sequence the local journal does not hold is abandoned for a path that replays the log; when none exists the SKIP is taken and the hole is declared, because the money state is correct. Holes are declared as `(D,S]` on `/health` and in `core_order_journal_holes_total`; an unreadable tail shows `unknown(,S]` and counts in `core_order_journal_tail_unreadable_total`. Hole ranges are kept in memory only, so after another restart at the same snapshot `/health` can read `holes=none` while the gap remains in the archive: a reader finds it by density. When a leader's archive stalls the leader stays leader and order entry stops.

### Money projector

The money journal is schema 3 version 2: every record carries `logPosition`, the core log position of the entry that produced it, the same on every member. The projector reads version 2 only (it parks with reason VERSION otherwise), and `money_journal.log_position` is NOT NULL. `log_position` is not unique (an epoch row and the movement it engages share one): consumers order by `(log_position, journal_seq)`.

A record at or below the PostgreSQL high-water is never inserted. It is compared field by field (legs and log position included) with its stored rows, and a difference or a missing row parks the projector with PAYLOAD_CONFLICT. A dense gap on one source commits everything below it, and the next epoch reads the next configured source; only a gap that every source shows at the same high-water parks the projector (GAP). While no source has filled a gap, `projector_gap_pending` is 1 and `projector_gap_high_water` is the committed high-water below it; if no other source delivers anything within `PROJECTOR_GAP_UNFILLED_PARK_MS` (default 60000) the projector parks with reason GAP. `PROJECTOR_HALT_ON_GAP=false` projects past a gap and leaves a hole below the high-water: a source that later delivers the skipped record parks it with PAYLOAD_CONFLICT, so use it only with a manual repair.

Idle mode: `PROJECTOR_IDLE_MODE=busy_spin|backoff|sleep` (default `backoff`; an unknown value refuses the start, exit 1), the stack profile's idle setting as the core members read it from `TRANSPORT_IDLE_MODE`. It sets the agent's idle, the archive clients' idle and, with `PROJECTOR_DRIVER_MODE=embedded`, every idle strategy of the projector's own media driver (an external driver is not the projector's to set). `sleep` is a 1 ms sleep: up to 1 ms of wake-up latency, nothing of the process spins or yields. The start log prints the effective mode (`[PROJECTOR] idle mode ...`, and `idleMode=` in the `starting` line).

The canonical dump (`PostgresMoneyJournalStore.CANONICAL_DUMP`) lists every column except `ingested_at`; two projections of one journal dump equal. Metrics: `projector_duplicate_records_total`, `projector_payload_conflicts_total`, `projector_pruned_records_total`, `projector_source_switches_total`, `projector_version_refusals_total`, `projector_gap_pending`, `projector_gap_high_water`.

Age-based retention of the money history (migration `V003__money_history_retention.sql`, applied like V001 and V002 as the projector's own login: the functions run as their owner and the projector reads the mark as the owner). The core's state and the money journal are the authority for every balance, and the gateway's movements view is display only; two kinds of reader still compute from the history, and the schema's own delete keeps both right:

- `money_history_prune_v1(before_ms, max_rows)`, called by an operator's retention job as a member of the role `oe_history_pruner` (EXECUTE on the function, no right on any table), deletes the oldest movements: a run of `journal_seq` without a hole from the first stored one, each with all of its legs. It stops before the newest movement (the projector's resume point), before the first movement whose `cluster_time_ms` is at or above `before_ms`, and after `max_rows`. An opening balance (no time) is as old as the next movement that has a time. It refuses (raises, deletes nothing) when a movement is stored at or below the mark, when the first stored one is not right above the mark, or when the rows it is about to delete have a hole, a movement without exactly its legs, or an unknown type: a lost row is never deleted around. A `before_ms` in the future is refused; there is no lower bound on the window (how long history must be kept is the operator's rule). It returns the rows deleted and what the history starts with afterwards (`first_kept_seq`, `first_kept_ms`, `newest_seq`).
- The mark, `money_journal_pruned`, written in the transaction that deletes: `through_seq` (every `journal_seq` at or below it was deleted, and no other) and `max_before_ms` (the greatest cutoff). The projector accepts a replayed record with no stored row only when its `journal_seq` is at or below `through_seq`, its time (when it has one) is below `max_before_ms`, and the first stored `journal_seq` is `through_seq + 1`, read in one snapshot after the rows; it counts it in `projector_pruned_records_total`. Every other missing row and every differing row parks it as before. Only the schema's functions write the mark, so rows deleted any other way are missing rows; `TRUNCATE money_journal` resets it. The money journal's records carry no genesis id, so the mark cannot name its journal: a journal of a later genesis read over a pruned database parks on its first timed record (its opening balances, which have no time, are accepted first). A mark the projector's login may not read (V003 applied as another role) parks it with reason RETENTION_MARK.
- The totals, `money_journal_pruned_totals`, written in the same transaction: per movement type and asset, the exact sum of `amount` (NUMERIC, no fraction, cannot overflow) and the number of the deleted openings, deposits and withdrawals; `money_journal_pruned.settles` counts the deleted settles, which add to no asset's sum. A reconciliation that sums the history (conservation per asset: deposits less withdrawals) reads the stored rows and the totals in one statement and gets the same number before and after a delete. A reader that needs every single movement (a replay from the first, a per-movement match) cannot be served: it must read the mark and report that it cannot judge.
- `money_history_adopt_v1(max_before_ms)`, owner only, for a database whose old rows another delete removed (first `journal_seq` above 1, mark 0; the delete function refuses there): it checks that the stored rows have no hole and every movement has its legs, sets the mark right below the first stored row with the cutoff the operator states, and records `totals_unknown_through`: a summing reader must then report that it cannot judge. The cutoff must be above every deleted movement's `cluster_time_ms`: the oldest kept movement's time + 1, or any later time up to now. The projector accepts a deleted movement only when its time is strictly below the cutoff, and a deleted movement can share its millisecond with the first kept one.

After a delete the balance movements view lists only the kept movements, and shows `delta` NULL for the oldest kept settle leg of a user and asset whose earlier leg is gone. With the money journal in `readers` mode as well, a movement older than the window then exists nowhere on the hosts: neither in a member's journal nor in the projection; only the totals of it remain. Applying V003 to a database that holds history takes one lock, SHARE ROW EXCLUSIVE on `money_journal` for the length of the migration's transaction, which reads no table: the projector's inserts wait for it, reads go on, and the cluster is not involved. The migration bounds its own wait (`lock_timeout` 5 s, `statement_timeout` 30 s): behind a long transaction it fails, changes nothing and is run again.

No rollback below this build once a delete has run: a projector older than V003 does not read the mark, so it parks with PAYLOAD_CONFLICT the first time a source delivers a deleted movement again (the next epoch that replays from below the mark), and an applier that records migrations refuses the older jar over the newer schema. Go forward, or rebuild the history from a journal that still holds it.

### Shadow pulls and the purge anchor

A member in `SNAPSHOT_MODE=shadow` can copy shadows from other archives (a peer member's, the replica's) with the shadow puller. It talks to a source only through the source's archive control channel and verifies every copy locally before it counts. Details in cluster-kit's `docs/shadow.md` ("Pulling shadows").

| variable | default | meaning |
|---|---|---|
| `SHADOW_PULL_SOURCES` | empty (disabled) | ordered `host:port` list of archive control endpoints (port offset 1) to pull from; empty logs `CRITICAL shadow puller disabled`; this node's own endpoint is refused at boot |
| `SHADOW_PULL_PERIOD_MS` | `5000` | listing period (500..3600000) |
| `SHADOW_PULL_STARTUP_WAIT_MS` | `60000` | the one bound of the startup pull (1000..86400000) |
| `SHADOW_PULL_TIMEOUT_MS` | `120000` | one steady-state copy (1000..86400000) |
| `SHADOW_PULL_RETRY_BACKOFF_MS` | `5000` | retry wait, doubling to 8x (100..3600000); keep it at or above 5000 when the endpoints below are fixed |
| `SHADOW_PULL_MAX_INFLIGHT` | `1` | copies at once (1..3); sources per node x inflight must stay at or below 3 per source |
| `SHADOW_PULL_KINDS` | `ALIGNED` | comma list of `ALIGNED,STANDBY,CADENCE,LOCAL`; anything wider logs CRITICAL |
| `SHADOW_PULL_REPLICATION_ENDPOINT`, `SHADOW_PULL_RESPONSE_ENDPOINT` | empty (`<host>:0`) | fixed `host:port` a source sends the copy to and answers on; set both on an isolated host and open them to every source |
| `SHADOW_ANCHOR_RULE` | `two-producers` | below the newest snapshot a member purges its log only behind an ALIGNED shadow two distinct producers wrote with one digest; `any-verified` (one producer is enough) boots with a CRITICAL line |

The leader never pulls: while a member leads, its puller lists nothing (`shadow_pull_skipped_leader_total`) and aborts any copy in flight (`shadow_pull_aborted_leader_total`). A member that holds one producer's ALIGNED at a position pulls another producer's copy of that position once, as the second witness the anchor needs. At start, when a stub snapshot has no local shadow to restore from (EQUAL, SKIP, TAIL), the member pulls one from its sources before it considers virtual genesis; the halt text then names what the pull tried (`(3) pull: tried <n> sources, <c> candidates, refused <r>, last: <reason>` or `(3) pull: disabled: SHADOW_PULL_SOURCES empty`). The consensus module waits for the service without a bound, so restart one member at a time while pulls are enabled. Metrics: `shadow_pull_total`, `shadow_pull_bytes_total`, `shadow_pull_retries_total`, `shadow_pull_refused_total{reason}`, `shadow_pull_source_unreachable_total`, `shadow_pull_skipped_leader_total`, `shadow_pull_aborted_leader_total`, `shadow_anchor_witnesses`, `shadow_floor_no_witness_total`, `shadow_anchor_rule_two_producers`.

### Follower producer

In `SNAPSHOT_MODE=shadow` one follower per term (the member after the leader in `CLUSTER_ADDRESSES` order, wrapping) writes an ALIGNED shadow of its own image at each committed snapshot position into its own archive; with the replica's copy it is the second producer the purge anchor needs. Details in cluster-kit's `docs/shadow.md` ("The follower producer").

| variable | default | meaning |
|---|---|---|
| `SHADOW_PRODUCER` | `designated-follower` | `none`: this member never writes an ALIGNED (CRITICAL at boot); set explicitly to `designated-follower` with `SNAPSHOT_MODE=off` logs CRITICAL and is ignored |
| `SHADOW_PRODUCER_BUFFER_MB` | `512` | the image buffer (1..2047); an image above it is a counted skip |
| `SHADOW_TERM_LENGTH` | `4m` | the shadow IPC term (64k..16m, power of two) |
| `SHADOW_WRITE_CONNECT_TIMEOUT_MS`, `SHADOW_WRITE_OFFER_TIMEOUT_MS`, `SHADOW_WRITE_RECORD_TIMEOUT_MS` | `10000`, `30000`, `60000` | writer timeouts (1..86400000) |
| `SHADOW_PRODUCER_WINDOW_BUDGET_PCT` | `50` | share of the log receiver window a serialize may fill at the recent ingress rate (1..100) |
| `SHADOW_PRODUCER_SERIALIZE_ESTIMATE_MS` | `1000` | serialize time assumed before the first one (1..600000) |
| `SHADOW_PRODUCER_RATE_WINDOW_MS` | `10000` | ingress rate horizon (1000..3600000) |
| `SHADOW_WITNESS_ALARM_MS` | `7200000` | how long the newest ALIGNED may have fewer than two producers before the floor-side CRITICAL (1000..604800000) |
| `SHADOW_LEADER_REMOTE_FLOOR` | `true` | while a member leads it purges its log behind VERIFIED ALIGNED shadows the other members attest they hold (it never pulls); `false`: a leader's log grows while it leads |
| `SHADOW_LEADER_REMOTE_HOLDERS` | `1` | how many other members must attest a floor before the leader purges behind it (1..64) |
| `SHADOW_LEADER_REMOTE_MAX_AGE_MS` | `30000` | a listing of the other members' attestations older than this feeds nothing (1000..3600000) |

The leader's archive is never a pull source while it leads (`shadow_pull_source_skipped_leader_total`); a `SHADOW_PULL_SOURCES` entry that names a member's archive endpoint under another spelling than `CLUSTER_ADDRESSES` refuses the boot. Metrics: `shadow_producer_designated` (1, 0, -1 leader unknown), `shadow_producer_written_total`, `shadow_producer_serialize_total`, `shadow_producer_serialize_ms_last`, `shadow_producer_last_position`, `shadow_producer_skipped_total{reason}`, `shadow_producer_deferred_window_total`, `shadow_producer_aborted_leader_total`, `shadow_producer_leader_mismatch_total`, `shadow_producer_serialize_failed_total`, `shadow_crashed_writes_purged_total`, `shadow_floor_witness_missing{kind}`, `shadow_pull_source_skipped_leader_total`. The floor-side line `no two-producer anchor for <n> min ... missing kind <R|F>` is raised by every follower; a leader does not evaluate it.

### Replica

`java -jar core-cluster.jar replica` (or `CORE_ROLE=replica`) starts a fourth process that is not a member: it follows the cluster through Aeron `ClusterBackup`, applies the log into its own engine and writes the shadow images members restore from. Behaviour, trust after a restart, fences and readiness are in cluster-kit's `docs/replica.md`. Every setting is parsed and range-checked at boot; a bad value exits 1 with an `CORE REPLICA BOOT GUARD:` line before anything starts. The cluster settings above (`CLUSTER_ADDRESSES`, `CLUSTER_PORT_BASE`, `CLUSTER_PORT_STRIDE`, `TRANSPORT_*`) apply as well.

| variable | default | meaning |
|---|---|---|
| `CLUSTER_REPLICA_SLOT` | `3` | port slot; must not collide with a member on the same host |
| `CLUSTER_REPLICA_ADDRESS` | `localhost` (only when every member is local) | address the members reach the replica on |
| `BASE_DIR` | `./core-replica<slot>` | the replica's archive: log copy and shadows. Keep it on disk |
| `CORE_NODE_PORT` | computed: `CLUSTER_PORT_BASE + slot x CLUSTER_PORT_STRIDE + 6` | `/health`, `/ready`, `/metrics`; optional, a disagreeing value refuses the boot |
| `REPLICA_BACKUP_SOURCE` | `FOLLOWER` | `FOLLOWER`, `LEADER` or `ANY`; `FOLLOWER` needs two or more members |
| `REPLICA_BACKUP_QUERY_INTERVAL_MS` | `60000` | how often the backup queries a member |
| `REPLICA_BACKUP_COOLDOWN_MS` | `12000` | wait after a backup reset |
| `REPLICA_VERIFY_WAIT_MS` | `180000` | stall bound for an unverified replica; at least 2 x query interval + cooldown |
| `REPLICA_ALIGNED_WAIT_MS` | `90000` | how long an ALIGNED trigger waits for a busy writer before it fences |
| `REPLICA_READY_MAX_LAG_BYTES` | `67108864` | recorded minus applied allowed for `/ready` |
| `REPLICA_READY_SHADOW_AGE_MS` | `1800000` | newest shadow age allowed for `/ready`; `0` disables |
| `REPLICA_READY_TIME_LAG_MS` | `3000` | wall clock minus cluster time of the last applied frame; `0` disables |
| `REPLICA_SHADOW_BUFFER_MB` | `512` | serialize buffer; an image larger than this fences |
| `REPLICA_MISMATCH_HOLD_MS` | `0` | an oracle mismatch holds the replica until it is stopped; a value (1..604800000) exits 1 after that long |
| `REPLICA_MISMATCH_DIR` | `BASE_DIR/mismatch` | where a mismatch dumps both images and `report.txt`; created and checked writable at boot |
| `REPLICA_MISMATCH_DUMP_MAX_MB` | `2048` | cap per dumped image file (0..16384); `0` writes the report only |
| `REPLICA_FENCE_PURGE_WAIT_MS` | `30000` | how long a history fence waits for the purge of its suspect shadows before exit 1 (1000..600000) |
| `REPLICA_MEMBER_CADENCE_MS` | `0` | the members' snapshot cadence; when set, `REPLICA_READY_SHADOW_AGE_MS` must cover it plus `REPLICA_VERIFY_WAIT_MS` |
| `SHADOW_PULL_SOURCES` (and the other `SHADOW_PULL_*` above) | empty (disabled) | members' archive endpoints; a fresh replica on a pruned log pulls an ALIGNED shadow from them (`restore_path` PULLED_SHADOW, code 3) |
| `SHADOW_REBOOTSTRAP_REPLAY_FAILURES` | `3` | replay-below-start refusals in a row before a re-bootstrap (0..1000; 0 counts and never acts) |
| `SHADOW_REBOOTSTRAP_MAX_CONSECUTIVE`, `SHADOW_REBOOTSTRAP_WINDOW_MS` | `3`, `3600000` | the loop guard: more re-bootstraps than this inside the window refuse with exit 1 (1..100; 60000..604800000) |
| `SHADOW_REBOOTSTRAP_KEEP` | `2` | moved-aside `cluster.rebootstrap-*` dirs kept (1..100) |
| `SHADOW_REBOOTSTRAP_NOT_TAKEN_LOG_MS` | `60000` | at most one `re-bootstrap not taken` line per interval (1000..86400000) |
| `REPLICA_DRIVER_DIR_ACTIVE_MS` | `10000` | embedded driver only: an existing `AERON_DIR` whose driver heartbeat is at most this old belongs to a live driver, and the replica refuses to start instead of taking over an aeronmd (2000..600000); a crashed driver's directory counts as stale only after this long |
| `TRANSPORT_DRIVER_CLIENT_LIVENESS_MS`, `TRANSPORT_DRIVER_PUBLICATION_UNBLOCK_MS` | `5000`, `10000` | embedded driver only: the members' client liveness and publication unblock timeouts, with the same ranges and the same rule (unblock above liveness), apply to the replica's driver too |

The remaining `REPLICA_*` settings (backup timeouts, catch-up buffers, driver MTU and term length, stream ids) are listed with their ranges in `core-cluster/src/main/java/com/openexchange/core/replica/ReplicaSettings.java`. After a normal restart the replica reports 503 `unverified continuation` until it has compared its state with the next member snapshot, about one member snapshot interval.

When the members pruned their logs past what the replica holds, the replica re-bootstraps instead of looping. After `SHADOW_REBOOTSTRAP_REPLAY_FAILURES` refusals it takes a VERIFIED ALIGNED above its log's stop position: one it already holds, or one it pulls from `SHADOW_PULL_SOURCES` and verifies. It logs `CRITICAL replica re-bootstrap: ...`, reads 503 `rebootstrap: restarting`, moves `BASE_DIR/cluster` to `BASE_DIR/cluster.rebootstrap-<utc>` after its backup and archive closed, and exits 3. Run the unit with `RestartForceExitStatus=3` (and `Restart=on-failure`); the restart restores from that shadow. When no source lists a newer shadow, the replica keeps running and reads 503 `re-bootstrap not taken: ...` (a single replica stays there until a second producer writes one; copy a VERIFIED shadow into `BASE_DIR/archive` or wipe `BASE_DIR`). Refusals start with `FATAL replica: re-bootstrap refused: ` and exit 1: the loop guard, or no sources and no local shadow above the stop position. Keep `CLUSTER_REPLICA_SLOT` stable for the life of `BASE_DIR`: the slot is the replica's producer identity for the two-producer anchor.

At every ALIGNED position the replica compares its state with the member snapshot its backup retrieved there. A
difference of the same history holds the replica instead of exiting: nothing is applied or written, `/ready` reads 503
`recovery=fenced:oracle mismatch at <P>: <detail>`, `/metrics` keeps serving (`shadow_oracle_mismatch_total`,
`shadow_oracle_mismatch_position`), and both images plus `report.txt` are in `REPLICA_MISMATCH_DIR/<P>/`. Read
`/metrics`, copy the dump, adjudicate offline, then stop the replica (exit 143) and wipe or keep `BASE_DIR`; a restart
re-latches at the same position. A member copy of another genesis, or a member snapshot the replica applied past
without a SNAPSHOT action, is a history fence: the replica's own shadows above its last equal compare are purged (on a restore start, only those above the restore point), then
it exits 1; wipe `BASE_DIR`. Switch a member to `SNAPSHOT_MODE=shadow` only when, over the agreed window,
`shadow_oracle_mismatch_total` and `shadow_oracle_other_genesis_total` did not move, `shadow_oracle_match_total` rose
by as much as `shadow_trigger_total{kind="ALIGNED"}`, and `shadow_replica_verified` is 1. Details in cluster-kit's
`docs/replica.md` ("Live oracle").

HTTP endpoints: each engine member serves `/health`, `/ready` and `/metrics` on its probe port. The projector serves them on `PROJECTOR_METRICS_PORT` (default 9601).

## Order load generator

`CoreOrderLoadGenerator` drives the core alone through one authenticated GATEWAY session (`CORE_AUTH_CREDENTIAL`): a ramp places `--open-orders` resting LIMIT GTC orders over `--users` and `--markets`, then a steady open-loop phase sends `--rate` slots per second for `--duration` seconds, a slot being one CreateOrder plus one CancelOrder of the oldest open order (`--mix create-only` sends the create alone). It ships in the same jar as a second main class:

```
java -cp core-loadgen/target/core-loadgen.jar com.openexchange.core.loadgen.CoreOrderLoadGenerator \
  --core-hosts 10.0.0.1,10.0.0.2,10.0.0.3 --core-port-base 9300 --rate 1000 --duration 60 --out results
```

- Identity: every CreateOrder, CancelOrder and Deposit carries a fresh 128-bit requestId (the core rejects a create without one); the clientOrderId is the requestId; the correlationId is the slot number.
- Elections: on a NewLeader event, or when an answer is older than `--reoffer-after-ms` (default 2000, the heartbeat timeout plus one election), the unanswered commands are offered once more with the same ids. The core answers a command it already applied with its stored decision (`duplicate` set) and applies nothing; such answers are counted as `replayed`. A command still unanswered when the run drains is `unanswered` and the run exits 1.
- The user-count rule: the core keeps 256 requestId decisions per user for 15 minutes, and the ramp's creates and the two `--prefund` deposits per user take requestIds from the same cap, so the run needs `--users` >= ceil((2 x rate x 900 + open orders) / (256 - deposits)), deposits 2 with `--prefund` and 0 without (7032 at 1000 slots/s with no ramp and no prefunding, 7481 with the default 100000 open orders and `--prefund`); below it the generator refuses to start rather than measure `TOO_MANY_PENDING_REQUESTS`.
- Measurement: create and cancel to their OrderAck from the slot's scheduled time (a stall is never hidden), the warm-up (`--warmup`) excluded; `summary.json` (build sha, settings, counts, rejects by reason, percentiles, the leader's `core_log_position` growth when `--metrics-url` is given) and `seconds.tsv` (per-second maxima) under `--out`.
- Optional: `--prefund` deposits to every user through a TOOL session (`CORE_TOOL_AUTH_CREDENTIAL`), `--gate-open` opens the global gate through an ADMIN session (`CORE_ADMIN_AUTH_CREDENTIAL`). `--aeron-dir` attaches to an external aeronmd, otherwise an embedded driver runs with `--idle busy_spin|backoff`. A bad setting exits 2 with one line naming the flag; `--help` lists them.
- In a test JVM the same `CoreOrderLoadGenerator.Session` runs against a rig's sessions (`tick` on the polling thread, the session as the egress listener).

## Releases

Release notes are in [GitHub Releases](https://github.com/openexch/core/releases). One version spans the v1 repos.

## License

Apache License 2.0.
