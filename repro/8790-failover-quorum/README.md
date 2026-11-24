# Reproducing cloudnative-pg#8790: failover quorum loses acknowledged commits

This directory reproduces
[cloudnative-pg#8790](https://github.com/cloudnative-pg/cloudnative-pg/issues/8790)
on a local kind cluster. It contains a Go harness that opens the loss window
from outside the Postgres pods, and the `Cluster` manifest it runs against.

Tested end to end on 2026-11-24 on kind with the operator built from `main`
(`4a29ae801`). Sections 3, 4, 5 and 9 ran as written, and so did the sweep,
the crash cleanup and the `teardown` + `create` recovery in section 8. The
first attempt at offset 1s reproduced the loss. Section 6 comes from an
earlier run. The registry network steps in section 8 were not run.

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

Set three variables for the rest of this guide. Your cloudnative-pg checkout:

```bash
export CNPG_SRC=~/src/cloudnative-pg
```

This directory:

```bash
export REPRO=~/src/cnpg-playground/repro/8790-failover-quorum
```

The name of the kind cluster, which `hack/setup-cluster.sh` reads:

```bash
export CLUSTER_NAME=kind
```

## 3. Set up one cluster

Every command below is one step. Run them one at a time and read the output
before going on. The checks (a) to (g) are the only things you need to verify.

Go to the cloudnative-pg checkout:

```bash
cd "$CNPG_SRC"
```

Pick the operator branch to test, here `main`:

```bash
git checkout main
```

Create a kind cluster named `kind` with one control plane and three workers,
plus the `registry.dev` registry:

```bash
./hack/setup-cluster.sh create
```

Build the operator image and push it to `registry.dev`:

```bash
TEST_UPGRADE_TO_V1=false ./hack/setup-cluster.sh load
```

Install the operator in the `cnpg-system` namespace:

```bash
./hack/setup-cluster.sh deploy
```

Check (a): `create` switched your current `kubectl` context, and the harness
always uses the current context. This must print `kind-kind`:

```bash
kubectl config current-context
```

Check (b): the operator is running. This must print `successfully rolled out`:

```bash
kubectl -n cnpg-system rollout status deploy/cnpg-controller-manager
```

Create the namespace for the test cluster:

```bash
kubectl create namespace repro-8790
```

Create the Postgres cluster: 2 instances, `method: any`, `number: 1`,
`dataDurability: preferred`, `failoverQuorum: true`:

```bash
kubectl apply -n repro-8790 -f "$REPRO/cluster.yaml"
```

Check (c): the cluster becomes Ready. This must print `condition met`:

```bash
kubectl -n repro-8790 wait --for=condition=Ready cluster/repro-8790 --timeout=900s
```

Go to the harness:

```bash
cd "$REPRO/harness"
```

Build the harness binary:

```bash
go build -o harness .
```

Build the writer image, which the harness starts as a Job:

```bash
docker build -t localhost:5000/cnpg-repro-writer:latest .
```

Push it to the registry. The kind nodes pull it as
`registry.dev:5000/cnpg-repro-writer:latest`, the harness default:

```bash
docker push localhost:5000/cnpg-repro-writer:latest
```

## 4. Watch the window open by hand

You can see the cause without killing anything. List the pods and their roles:

```bash
kubectl -n repro-8790 get pods -L cnpg.io/instanceRole
```

The commands below assume `repro-8790-1` is the primary and `repro-8790-2` the
replica. If the output shows the opposite, swap the two names.

In a second terminal, watch what the operator records:

```bash
watch -n1 -x kubectl -n repro-8790 get failoverquorum repro-8790 -o jsonpath='{.status}'
```

It shows
`{"method":"ANY","primary":"repro-8790-1","standbyNames":["repro-8790-2"],"standbyNumber":1}`.

In a third terminal, watch what the primary waits for:

```bash
watch -n1 -x kubectl -n repro-8790 exec repro-8790-1 -c postgres -- psql -XAtc 'SHOW synchronous_standby_names'
```

It shows `ANY 1 ("repro-8790-2")`.

Fence the replica:

```bash
kubectl -n repro-8790 annotate cluster repro-8790 cnpg.io/fencedInstances='["repro-8790-2"]' --overwrite
```

Its Postgres stops. Within about 30 seconds `synchronous_standby_names`
becomes empty and the `FailoverQuorum` status becomes `{}`. Because of
`preferred` the primary keeps accepting commits, and from now on every commit
has one copy only.

Unfence it:

```bash
kubectl -n repro-8790 annotate cluster repro-8790 cnpg.io/fencedInstances-
```

Within seconds of the replica pod turning Ready, both watches show it again:
`ANY 1 ("repro-8790-2")` and `"standbyNames":["repro-8790-2"]`. Nothing
checked that it has received the WAL written while it was away. Here it catches
up in a moment because nothing is writing. The harness adds load and a WAL
delay so that "named" comes well before "caught up", and kills the primary in
that gap.

Stop the two `watch` commands with Ctrl-C.

## 5. One real attempt

All commands in this section run in `$REPRO/harness`.

Check (d): the dry run. It finds the primary and the replica, checks that
replication is streaming with the replica in the quorum and that the two pods
run on different nodes, and changes nothing. It does not touch the nodes yet.
It must end with `dry run ok`:

```bash
./harness run --dry-run
```

Check (e): one attempt with a 1 second offset. It must end with
`outcome=reproduced`, and the `acknowledged=... lost=...` line must show `lost`
above 0:

```bash
./harness run --offset 1s
```

If it prints another outcome, run it again, up to 3 times, with `--offset 1s`
or `--offset 2s`.

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
the end, also on Ctrl-C. This is the end of the tested run, shortened:

```text
14:22:09 primary no longer waits for repro-8790-2 (FailoverQuorum: map[])
14:22:09 WAL delayed by 2s from repro-8790-1 to repro-8790-2; replica down for 20s
14:22:29 bringing replica repro-8790-2 back
14:22:38 replica repro-8790-2 Ready
14:22:39 offset 1s reached; cutting primary->replica and force-deleting the primary
14:22:39 at kill: FailoverQuorum: map[method:ANY primary:repro-8790-1 standbyNames:[repro-8790-2] standbyNumber:1]
14:22:56 new primary: repro-8790-2 (replica)
14:22:56 packets through the netem delay: 17 (err=<nil>)
14:22:56 acknowledged=44342 namedAtAck=6772 lost=37571 lostWhileNamed=1
14:22:56 lost ack: id=6772 ts=2026-11-24T14:22:08.318220889Z named=false behind=true
...
14:22:57 outcome=reproduced
```

- `acknowledged`: commits the client saw succeed.
- `lost`: acknowledged ids missing on the new primary. Any value above 0 is
  data loss.
- `lostWhileNamed`: the lost commits that were acknowledged while the replica
  was named as synchronous (see section 7).
- `packets through the netem delay` must be above 0, otherwise the delay did
  not apply and the run counts as `harness_invalid`.
- When something was lost, the harness also prints the replication and
  promotion events from both Postgres pods (`log <pod> ...` lines). The old
  primary was just deleted, so its line may only say
  `the server rejected our request`; that is expected.

The exit code is the outcome:

| Exit code | Outcome | Meaning |
|---|---|---|
| 0 | `reproduced` | acknowledged commits are missing |
| 1 | `error` | unexpected error |
| 2 | `harness_invalid` | preconditions not met, the result is not trustworthy |
| 3 | `trigger_missed` | the replica never turned Ready after unfencing |
| 10 | `nothing_lost` | a failover happened and every acknowledged commit survived |
| 11 | `no_failover` | nothing was promoted within 5 minutes; the faults were lifted and the data checked on whoever leads |

Check (f): the operator promoted the stale replica. This must print a
`Failing over` line with the replica as `newPrimary`:

```bash
kubectl -n cnpg-system logs deploy/cnpg-controller-manager | grep 'Failing over'
```

To see why it promoted, look at the quorum check just before it. On `main` it
shows `"isStronglyConsistent":true` with the stale replica in `readSet`:

```bash
kubectl -n cnpg-system logs deploy/cnpg-controller-manager | grep 'Quorum check algorithm results'
```

Check (g): remove any leftover faults from the worker nodes. It must print
`done`:

```bash
./harness cleanup
```

The primary and the replica swap roles after each attempt, and the harness
finds them again on the next run.

## 6. Compare with the fix

> Validated in the earlier multi-target run (2026-11-23), not in the
> end-to-end test of this copy.

That run used the original harness this copy was taken from, offsets 0, 1 and
2 seconds, 3 attempts each:

| Operator build | Commit | Attempts that lost acknowledged commits |
|---|---|---|
| `main` | `4a29ae801` | 7 of 9 |
| PR #11932 | `ea6da5868` | 8 of 9 |
| PR #11940 | `83f8f6a73` | 8 of 9 |
| PR #11561 | `9c92dde7a` | 8 of 9 |
| `dev/8790` | `85242e827` | 0 of 9 |

On `dev/8790` every failover was denied, 6 of the 9 by the barrier. Every
attempt without loss on `main` and the PRs was an offset-0 timing denial. This
covers one setup only: `preferred`, `failoverQuorum: true`, 2 instances, and a
primary force-deleted while the replica lagged.

To try it yourself, go to the cloudnative-pg checkout:

```bash
cd "$CNPG_SRC"
```

Fetch the fix branch:

```bash
git fetch https://github.com/GellertBabel/cloudnative-pg.git dev/8790
```

Check it out:

```bash
git checkout --detach FETCH_HEAD
```

Build and push the operator image:

```bash
TEST_UPGRADE_TO_V1=false ./hack/setup-cluster.sh load
```

Deploy it:

```bash
./hack/setup-cluster.sh deploy
```

The image tag stays `cloudnative-pg-testing:latest`, so restart the operator to
pull the new build:

```bash
kubectl -n cnpg-system rollout restart deploy/cnpg-controller-manager
```

Check (b) again:

```bash
kubectl -n cnpg-system rollout status deploy/cnpg-controller-manager
```

Part of the fix runs in the instance manager inside the Postgres pods, and
existing pods keep the old one. Delete the test cluster:

```bash
kubectl -n repro-8790 delete cluster repro-8790
```

Create it again:

```bash
kubectl apply -n repro-8790 -f "$REPRO/cluster.yaml"
```

Check (c) again:

```bash
kubectl -n repro-8790 wait --for=condition=Ready cluster/repro-8790 --timeout=900s
```

Run one attempt from `$REPRO/harness`:

```bash
./harness run --offset 1s
```

Expect `outcome=no_failover` (exit code 11) with `lost=0`. The attempt waits
the full 5 minutes for a promotion that does not come, then lifts the faults
and the old primary leads again. The operator log shows why:

```bash
kubectl -n cnpg-system logs deploy/cnpg-controller-manager | grep 'not promotable'
```

It prints
`Replica has not received WAL up to the synchronous replication barrier, not promotable`
with the replica's `receivedLsn` below `barrierLSN`, and there is no new
`Failing over`. The cost of the fix is availability: no failover until the
replica reaches the barrier.

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
  has republished `FailoverQuorum`. The operator then denies the failover with
  `Quorum check failed a unsupported synchronous nodes number`. That is a
  timing race, not a fix.
- One attempt proves little. Sweep a few offsets, 3 times each (about 15
  minutes). The results go to `runs/sweep-<label>-<time>/results.csv`, one row
  per attempt, and the sweep ends with a summary table:

  ```bash
  ./harness sweep --offsets 0,1,2 --repeats 3 --label main
  ```

  To try the sweep quickly first, run one attempt at offset 1s:

  ```bash
  ./harness sweep --offsets 1 --repeats 1 --label main
  ```

- If the harness was killed hard (`kill -9`, a closed laptop), its faults stay
  on the nodes. Remove them:

  ```bash
  ./harness cleanup
  ```

  Then remove the fencing annotation:

  ```bash
  kubectl -n repro-8790 annotate cluster repro-8790 cnpg.io/fencedInstances-
  ```

  Then delete the writer Job. It prints nothing if the Job is already gone:

  ```bash
  kubectl -n repro-8790 delete job repro-8790-writer --ignore-not-found
  ```

- After a Docker restart, `registry.dev` or the `kind` network can be stale,
  and the nodes can no longer pull images. Start over from the cloudnative-pg
  checkout. First remove the kind cluster and the `kind` network:

  ```bash
  ./hack/setup-cluster.sh teardown
  ```

  If the registry's own network `registry` is stale too, recreate it. These
  four steps were not run in the end-to-end test. Detach the registry from it:

  ```bash
  docker network disconnect -f registry registry.dev
  ```

  Remove the network. If this fails with `active endpoints`, a `load` was
  interrupted and left its builder attached; remove it with
  `docker buildx rm cnpg-builder` and try again:

  ```bash
  docker network rm registry
  ```

  Create the network again:

  ```bash
  docker network create registry
  ```

  Attach the registry to it. The registry keeps running throughout, because
  it was started with `--restart always`:

  ```bash
  docker network connect registry registry.dev
  ```

  Then create the cluster again and continue with `load` in section 3:

  ```bash
  ./hack/setup-cluster.sh create
  ```

- `./hack/setup-cluster.sh teardown` also disconnects `registry.dev` from the
  `kind` network (and removes the network if nothing else uses it). If you have
  other kind clusters, delete only this one:

  ```bash
  kind delete cluster --name kind
  ```

## 9. Cleanup

Delete the test cluster and its namespace:

```bash
kubectl delete namespace repro-8790
```

Delete the kind cluster:

```bash
kind delete cluster --name kind
```

Check that it is gone. This must print `No kind clusters found.` (or list
only your other clusters):

```bash
kind get clusters
```

Remove the harness binary:

```bash
rm "$REPRO/harness/harness"
```

Remove the sweep results, if you ran a sweep:

```bash
rm -r "$REPRO/harness/runs"
```

Remove the local writer image:

```bash
docker rmi localhost:5000/cnpg-repro-writer:latest
```

The `registry.dev` container and its volume stay; other kind clusters from
`hack/setup-cluster.sh` use them too.
