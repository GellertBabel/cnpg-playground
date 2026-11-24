# Reproducing cloudnative-pg#8790: failover quorum loses acknowledged commits

This directory reproduces
[cloudnative-pg#8790](https://github.com/cloudnative-pg/cloudnative-pg/issues/8790)
on a local kind cluster. It contains a Go harness that opens the loss window
from outside the Postgres pods, and the `Cluster` manifest it runs against.

## 1. The idea

1. With `dataDurability: preferred`, the primary stops waiting for a replica
   that is down, so commits are acknowledged with only one copy.
2. When the replica comes back, the operator names it in
   `synchronous_standby_names` and records it in the `FailoverQuorum` object as
   soon as the pod is Ready, even if it has not caught up yet.
3. The quorum check only sees "named and Ready", so it reports
   `isStronglyConsistent: true` and promotes that replica if the primary dies
   now.
4. The commits acknowledged while the replica was away are lost, plus a few
   acknowledged after it was named again; those last ones are #8790 proper.
5. The fix (branch `dev/8790` on
   [GellertBabel/cloudnative-pg](https://github.com/GellertBabel/cloudnative-pg/tree/dev/8790))
   adds a barrier WAL position that the replica must reach before it can be
   promoted.

The harness makes this repeatable. It fences the replica, delays the WAL sent
to it by 2 seconds with `tc netem` on the primary's kind node, unfences it, and
when the replica turns Ready it waits a chosen offset, drops the WAL still in
flight with `iptables` and force-deletes the primary. Nothing inside the
Postgres pods is changed.

## 2. Prerequisites

- Linux with Docker, [kind](https://kind.sigs.k8s.io/), `kubectl`, `git`,
  `watch`, and Go 1.26 (Go 1.21 or later downloads the 1.26 toolchain on its
  own).
- A checkout of [cloudnative-pg](https://github.com/cloudnative-pg/cloudnative-pg)
  with the tools its `hack/setup-cluster.sh` needs (`make`, `helm`,
  `docker buildx`).
- A kind cluster you can throw away. The harness runs `docker exec` on the
  kind nodes, so other Kubernetes distributions do not work. Every attempt
  kills the primary.

This walkthrough does not use the playground's `scripts/setup.sh`. It uses
cloudnative-pg's `hack/setup-cluster.sh`, which builds the operator from
whatever branch you have checked out and runs a local registry
(`registry.dev`, port 5000 on your host) for the writer image.

Set two paths for the rest of this guide:

```bash
export CNPG_SRC=~/src/cloudnative-pg                            # your cloudnative-pg checkout
export REPRO=~/src/cnpg-playground/repro/8790-failover-quorum   # this directory
```

## 3. Set up one cluster

Build the operator from the branch you want to test (here `main`) and deploy it
on a new kind cluster:

```bash
cd "$CNPG_SRC"
git checkout main
./hack/setup-cluster.sh create
TEST_UPGRADE_TO_V1=false ./hack/setup-cluster.sh load
./hack/setup-cluster.sh deploy
```

`create` makes a kind cluster with one control plane and three workers. Its
name is `pg-operator-e2e-<kubernetes version>`, for example
`pg-operator-e2e-v1-37-0`; `kind get clusters` shows it. It also switches your
current `kubectl` context to `kind-<name>`. The harness always uses the current
context, so check it before every run:

```bash
kubectl config current-context
```

Create the Postgres cluster: 2 instances, `method: any`, `number: 1`,
`dataDurability: preferred`, `failoverQuorum: true`.

```bash
kubectl create namespace repro-8790
kubectl apply -n repro-8790 -f "$REPRO/cluster.yaml"
kubectl wait -n repro-8790 --for=condition=Ready cluster/repro-8790 --timeout=10m
```

Build the harness, and the writer image that the harness starts as a Job. You
push it through `localhost:5000`; the kind nodes pull it as
`registry.dev:5000/cnpg-repro-writer:latest`, which is the harness default.

```bash
cd "$REPRO/harness"
go build -o harness .
docker build -t localhost:5000/cnpg-repro-writer:latest .
docker push localhost:5000/cnpg-repro-writer:latest
```

## 4. Watch the window open by hand

You can see the cause without killing anything. Find the two instances:

```bash
PRIMARY=$(kubectl -n repro-8790 get cluster repro-8790 -o jsonpath='{.status.currentPrimary}')
REPLICA=$(kubectl -n repro-8790 get pods -l cnpg.io/cluster=repro-8790,cnpg.io/instanceRole=replica -o jsonpath='{.items[0].metadata.name}')
echo "primary=$PRIMARY replica=$REPLICA"
```

In a second terminal, watch what the operator records. In a third, watch what
the primary waits for. Set `PRIMARY` there too, or paste the name.

```bash
watch -n1 "kubectl -n repro-8790 get failoverquorum repro-8790 -o jsonpath='{.status}'"
```

```bash
watch -n1 "kubectl -n repro-8790 exec $PRIMARY -c postgres -- psql -XAtc 'SHOW synchronous_standby_names'"
```

Fence the replica. Its Postgres stops, and because of `preferred` the primary
stops waiting for it: `synchronous_standby_names` becomes empty and the
replica leaves the `FailoverQuorum` status. From now on every commit has one
copy only.

```bash
kubectl -n repro-8790 annotate cluster repro-8790 cnpg.io/fencedInstances="[\"$REPLICA\"]" --overwrite
```

Unfence it:

```bash
kubectl -n repro-8790 annotate cluster repro-8790 cnpg.io/fencedInstances-
```

Within seconds of the replica pod turning Ready, both watches show it again,
for example `ANY 1 ("repro-8790-2")`. Nothing checked that it has received the WAL written
while it was away. Here it catches up in a moment because nothing is writing.
The harness adds load and a WAL delay so that "named" comes well before
"caught up", and kills the primary in that gap.

Stop the two `watch` commands with Ctrl-C.

## 5. One real attempt

Check the setup first. This validates the cluster and the nodes and changes
nothing:

```bash
cd "$REPRO/harness"
./harness run --dry-run
```

It ends with `dry run ok`. Then run one attempt with a 1 second offset:

```bash
./harness run --offset 1s
```

What it does, step by step:

1. Starts the writer Job: 4 connections inserting rows into the table
   `marker`. Every acknowledged commit is recorded with its id and whether a
   standby was named in `synchronous_standby_names` at that moment.
2. Waits 10 seconds with healthy replication.
3. Fences the replica and waits until `synchronous_standby_names` on the
   primary is empty. The primary keeps acknowledging commits on its own.
4. Delays packets from the primary pod to the replica pod by 2 seconds
   (`tc netem` on the primary's node, matching only the two pod IPs).
5. After 20 seconds, unfences the replica. It restarts, connects, turns Ready
   and is named again, but its WAL arrives 2 seconds late.
6. 1 second after Ready, drops primary-to-replica packets on the replica's node
   (the delayed WAL never arrives) and force-deletes the primary pod.
7. Waits up to 5 minutes for a new primary, removes the network faults, reads
   the ids in `marker` on the new primary and compares them with the
   acknowledged ids.

The harness removes its faults, the fencing annotation and the writer Job at
the end, also on Ctrl-C. The end of a reproduced run looks like this (numbers
vary):

```text
hh:mm:ss replica repro-8790-2 Ready
hh:mm:ss offset 1s reached; cutting primary->replica and force-deleting the primary
hh:mm:ss at kill: FailoverQuorum: map[method:ANY primary:repro-8790-1 standbyNames:[repro-8790-2] standbyNumber:1]
hh:mm:ss new primary: repro-8790-2 (replica)
hh:mm:ss packets through the netem delay: <n> (err=<nil>)
hh:mm:ss acknowledged=<n> namedAtAck=<n> lost=<n> lostWhileNamed=<n>
hh:mm:ss lost ack: id=<id> ts=<time> named=false behind=true
...
hh:mm:ss outcome=reproduced
```

- `acknowledged`: commits the client saw succeed.
- `lost`: acknowledged ids missing on the new primary. Any value above 0 is
  data loss.
- `lostWhileNamed`: the lost commits that were acknowledged while the replica
  was named as synchronous (see section 7).
- `packets through the netem delay` must be above 0, otherwise the delay did
  not apply and the run counts as `harness_invalid`.
- When something was lost, the harness also prints the replication and
  promotion events from both Postgres pods (`log <pod> ...` lines).

The exit code is the outcome:

| Exit code | Outcome | Meaning |
|---|---|---|
| 0 | `reproduced` | acknowledged commits are missing |
| 1 | `error` | unexpected error |
| 2 | `harness_invalid` | preconditions not met, the result is not trustworthy |
| 3 | `trigger_missed` | the replica never turned Ready after unfencing |
| 10 | `nothing_lost` | a failover happened and every acknowledged commit survived |
| 11 | `no_failover` | nothing was promoted within 5 minutes; the faults were lifted and the data checked on whoever leads |

Now look at why the operator promoted:

```bash
kubectl -n cnpg-system logs deploy/cnpg-controller-manager --since=15m | grep -E 'Quorum check algorithm results|Failing over'
```

On `main` you see `Quorum check algorithm results` with
`"isStronglyConsistent":true` and the stale replica in `readSet`, followed by
`Failing over` with that replica as `newPrimary`.

The primary and the replica swap roles after each attempt, and the harness
finds them again on the next run.

## 6. Compare with the fix

> Covered by the earlier multi-target run (2026-11-23), not by the end-to-end
> test of this copy: `dev/8790` lost 0 of 9 attempts, and denied 9 of 9
> failovers, 6 of them by the barrier.

Build and deploy the operator from `dev/8790`. The image tag stays
`cloudnative-pg-testing:latest`, so restart the operator to pull the new build:

```bash
cd "$CNPG_SRC"
git remote add gellert https://github.com/GellertBabel/cloudnative-pg.git
git fetch gellert dev/8790
git checkout --detach gellert/dev/8790
TEST_UPGRADE_TO_V1=false ./hack/setup-cluster.sh load
./hack/setup-cluster.sh deploy
kubectl -n cnpg-system rollout restart deploy/cnpg-controller-manager
kubectl -n cnpg-system rollout status deploy/cnpg-controller-manager
```

Recreate the test cluster. Part of the fix runs in the instance manager inside
the Postgres pods, and existing pods keep the old one.

```bash
kubectl delete -n repro-8790 cluster repro-8790
kubectl apply -n repro-8790 -f "$REPRO/cluster.yaml"
kubectl wait -n repro-8790 --for=condition=Ready cluster/repro-8790 --timeout=10m
cd "$REPRO/harness"
./harness run --offset 1s
kubectl -n cnpg-system logs deploy/cnpg-controller-manager --since=15m | grep -E 'barrier|Failing over'
```

Expect `outcome=no_failover` (exit code 11) with `lost=0`. The attempt waits
the full 5 minutes for a promotion that does not come, then lifts the faults
and the old primary leads again. The operator log shows
`Replica has not received WAL up to the synchronous replication barrier, not promotable`
with the replica's `receivedLsn` below `barrierLSN`, and no `Failing over`.
The cost of the fix is availability: no failover until the replica reaches the
barrier.

## 7. lost and lostWhileNamed

- `lost` counts every acknowledged commit missing after the failover. Most of
  them were acknowledged while the replica was fenced and
  `synchronous_standby_names` was empty. `preferred` allows those commits to be
  asynchronous, so their loss on its own is the documented trade-off of
  `preferred`. The bug is that the quorum check then treats the returning
  replica as holding them.
- `lostWhileNamed` counts the lost commits acknowledged while the replica was
  already named in `synchronous_standby_names`. The cluster claimed synchronous
  replication for these and still lost them; that is #8790 proper. The count
  is approximate in both directions, because the writer reads the setting just
  after `COMMIT` returns.

## 8. Pitfalls

- Use offsets of 1s or 2s. At offset 0 the kill can land before the primary
  has republished `FailoverQuorum`; the operator then denies the failover with
  `Quorum check failed a unsupported synchronous nodes number`. That is a
  timing race, not a fix.
- One attempt proves little. Sweep a few offsets, 3 times each; the results go
  to `runs/sweep-<label>-<time>/results.csv`, one row per attempt:

  ```bash
  ./harness sweep --offsets 0,1,2 --repeats 3 --label main
  ```

- If the harness was killed hard (`kill -9`, a closed laptop), its faults stay
  on the nodes. Remove them, the fencing annotation and the writer Job. By
  default `cleanup` works on the worker nodes of the current context:

  ```bash
  ./harness cleanup
  kubectl -n repro-8790 annotate cluster repro-8790 cnpg.io/fencedInstances-
  kubectl -n repro-8790 delete job repro-8790-writer --ignore-not-found
  ```

- A restart of the Docker daemon can break name resolution on the kind nodes,
  so they cannot pull from `registry.dev` any more. Find the registry's address
  on the `kind` network and add it to `/etc/hosts` on each node
  (`kind get nodes --name <name>` lists the nodes):

  ```bash
  docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' registry.dev
  docker exec <node> sh -c 'grep -q "registry.dev # repro-8790" /etc/hosts || echo "<registry.dev IP on the kind network> registry.dev # repro-8790" >> /etc/hosts'
  ```

- Delete the cluster with `kind delete cluster --name <name>`, not with
  `./hack/setup-cluster.sh teardown`. The teardown also disconnects
  `registry.dev` from the `kind` network and removes that network, which breaks
  any other kind cluster you have.

## 9. Results from the earlier run

Run on 2026-11-23 with the original harness this copy was taken from, offsets
0, 1 and 2 seconds, 3 attempts each:

| Operator build | Commit | Attempts that lost acknowledged commits |
|---|---|---|
| `main` | `4a29ae801` | 7 of 9 |
| PR #11932 | `ea6da5868` | 8 of 9 |
| PR #11940 | `83f8f6a73` | 8 of 9 |
| PR #11561 | `9c92dde7a` | 8 of 9 |
| `dev/8790` | `85242e827` | 0 of 9 |

Every attempt without loss on `main` and the PRs was an offset-0 timing denial.
This covers one setup only: `preferred`, `failoverQuorum: true`, 2 instances,
and a primary force-deleted while the replica lagged.

## 10. Cleanup

```bash
kubectl delete namespace repro-8790
kind get clusters
kind delete cluster --name pg-operator-e2e-v1-37-0
rm "$REPRO/harness/harness"
docker rmi localhost:5000/cnpg-repro-writer:latest
```

Use the name `kind get clusters` printed. The `registry.dev` container and its
volume stay; other kind clusters from `hack/setup-cluster.sh` use them too.
