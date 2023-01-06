# Scylla

## About this fork

This is a fork of the original [Jepsen Test Suite](https://github.com/jepsen-io/scylla) [[1]](#1-original-repository-httpsgithubcomjepsen-ioscylla) for [ScyllaDB Open Source](https://www.scylladb.com/open-source-nosql-database/) with the following enhancements:
- A new [test](src/scylla/crdt_g_counter.clj) for the [CmRDT-like Counter](https://www.scylladb.com/open-source-nosql-database/)
- Supports [ScyllaDB Open Source 5.x](https://www.scylladb.com/open-source-nosql-database/5-x/) deployment
- Has a limited support to run the same tests using [Apache Cassandra](https://cassandra.apache.org/) instead of [ScyllaDB Open Source](https://www.scylladb.com/open-source-nosql-database/) as a database. Based on [[2]](#2-jepsen-for-cassandra-repository-httpsgithubcomriptanojepsentreecassandracassandra)
- Incorporates [Docker Setup](docker/) compatible with both [Apache Cassandra](https://cassandra.apache.org/) and [ScyllaDB Open Source](https://www.scylladb.com/open-source-nosql-database/) deployments. Based on [[3]](#3-docker-part-from-the-jepsen-framework-repository-httpsgithubcomjepsen-iojepsentreemaindocker)

## Overview

This repository implements Jepsen tests for Scylla.

You'll need a Jepsen environment, including a control node with a JVM and
[Leiningen](https://leiningen.org/), and a collection of Debian 10 nodes to
install the database on. There are instructions for setting up a Jepsen
environent [here](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment).

Once your environment is ready, you should be able to run something like:

```
lein run test -w list-append --concurrency 10n -r 100 --max-writes-per-key 100 --nemesis partition
```

This runs a single test using the `list-append` workload, which uses SERIAL reads, and LWT `UPDATE`s to append unique integers to CQL lists, then searches for anomalies which would indicate the resulting history is incompatible with strict serializability. The test uses ten times the number of DB nodes, runs approximately 100 requests per second, and writes up to 100 values per key. During the test, we introduce network partitions. For version 4.2, this might yield something like:

```clj
  :anomaly-types (:G-nonadjacent-realtime
                  :G-single-realtime
                  :cycle-search-timeout
                  :incompatible-order),
  ...
  :not #{:read-atomic :read-committed},
  :also-not
  #{:ROLA :causal-cerone :consistent-view :cursor-stability
    :forward-consistent-view :monotonic-atomic-view
    :monotonic-snapshot-read :monotonic-view
    :parallel-snapshot-isolation :prefix :repeatable-read :serializable
    :snapshot-isolation :strict-serializable
    :strong-session-serializable :strong-session-snapshot-isolation
    :strong-snapshot-isolation :update-serializable}},
 :valid? false}

Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻
```

## Running Tests

Use `lein run test -w ...` to run a single test. Use `lein run test-all ...` to
run a broad collection of workloads with a variety of nemeses. You can filter
`test-all` to just a specific workload or nemesis by using `-w` or `--nemesis`.

As with all Jepsen tests, you'll find detailed results, graphs, and node logs
in `store/latest`. `lein run serve` will start a web server on port 8080 for
browsing the `store` directory.

Most tests are tunable with command line options. See `lein run test --help`
for a full list of options.

## Running Tests in Docker

### Quick tips
Navigate into the docker directory
```
cd docker
```
Start new containers in dev mode (all changes in the current repository immediatly available in the control container)
```
./bin/up --dev -d
```
Connect to the control container
```
./bin/console
```
Shutdown all containers
```
./bin/down
```

## Troubleshooting

If you have trouble getting Scylla to start, check the node logs, and look at
`service scylla-server status` on a DB node.

You may need to up aio-max-nr, either on individual DB nodes, or, for
containers, on the container host.

```
echo 16777216 >/proc/sys/fs/aio-max-nr
```

## References

#### [1] Original repository: https://github.com/jepsen-io/scylla
#### [2] Jepsen for Cassandra repository: https://github.com/riptano/jepsen/tree/cassandra/cassandra
#### [3] Docker part from the Jepsen framework repository: https://github.com/jepsen-io/jepsen/tree/main/docker