/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// markerDB is the database the writer connects to through the <cluster>-app
// secret, and so the database holding the marker table.
const markerDB = "app"

type attemptConfig struct {
	slot            int
	namespace       string
	cluster         string
	image           string
	payload         int
	payloadMode     string
	checkpointEvery string
	commitRate      float64
	writers         int
	netemDelay      string
	killOffset      time.Duration
	replicaDown     time.Duration
	baseline        time.Duration
	windowTimeout   time.Duration
	failoverWait    time.Duration
	dryRun          bool
}

// AttemptResult is what one attempt produced. NewPrimary is "replica" when the
// lagging replica was promoted and "old_primary" when, without a failover, the
// killed primary came back and led again.
type AttemptResult struct {
	Outcome    Outcome
	Verdict    Verdict
	NewPrimary string
	Err        error
}

func logf(format string, args ...any) {
	fmt.Printf("%s %s\n", time.Now().UTC().Format("15:04:05"), fmt.Sprintf(format, args...))
}

// runAttempt performs one attempt and returns the outcome. It always removes
// the network faults, the fencing annotation, and the writer Job.
func runAttempt(ctx context.Context, k *kubeClient, cfg attemptConfig) AttemptResult {
	if cfg.slot < 1 || cfg.slot > maxSlot {
		return AttemptResult{Outcome: OutcomeError, Err: fmt.Errorf("--slot must be 1..%d", maxSlot)}
	}
	primary, err := k.currentPrimary(ctx, cfg.namespace, cfg.cluster)
	if err != nil {
		return AttemptResult{Outcome: OutcomeError, Err: err}
	}
	if primary == "" {
		return AttemptResult{Outcome: OutcomeInvalid, Err: fmt.Errorf("no current primary")}
	}
	replica, err := k.replicaPod(ctx, cfg.namespace, cfg.cluster)
	if err != nil {
		return AttemptResult{Outcome: OutcomeInvalid, Err: err}
	}
	primaryPod, err := k.pod(ctx, cfg.namespace, primary)
	if err != nil {
		return AttemptResult{Outcome: OutcomeError, Err: err}
	}
	logf("primary=%s (%s) replica=%s (%s)", primary, primaryPod.Spec.NodeName, replica.Name, replica.Spec.NodeName)

	if err := k.waitReplicationState(ctx, cfg, primary, replica.Name, "streaming/quorum"); err != nil {
		return AttemptResult{Outcome: OutcomeInvalid, Err: err}
	}

	p4, p6 := podIPs(primaryPod.Status.PodIPs)
	r4, r6 := podIPs(replica.Status.PodIPs)
	faults := &netFaults{
		primaryNode: primaryPod.Spec.NodeName,
		replicaNode: replica.Spec.NodeName,
		primaryIP:   p4,
		replicaIP:   r4,
		primaryIP6:  p6,
		replicaIP6:  r6,
		delay:       cfg.netemDelay,
		slot:        cfg.slot,
		comment:     slotComment(cfg.slot),
	}
	logf("pod addresses: primary %s %s, replica %s %s", p4, p6, r4, r6)
	if faults.primaryNode == faults.replicaNode {
		return AttemptResult{Outcome: OutcomeInvalid, Err: fmt.Errorf("primary and replica share node %s", faults.primaryNode)}
	}
	store := &ackStore{}

	defer func() {
		faults.remove()
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		_ = k.setFenced(cctx, cfg.namespace, cfg.cluster, "")
		k.stopWriter(cctx, cfg.namespace, cfg.cluster)
	}()

	if cfg.dryRun {
		logf("dry run: would take %s down for %s with WAL delayed %s, cut on %s, kill %s at offset %s",
			replica.Name, cfg.replicaDown, cfg.netemDelay, faults.replicaNode, primary, cfg.killOffset)
		return AttemptResult{Outcome: OutcomeNothingLost}
	}

	if err := k.startWriter(ctx, cfg, replica.Name, store); err != nil {
		return AttemptResult{Outcome: OutcomeError, Err: err}
	}
	time.Sleep(cfg.baseline)
	total, violations := store.countNamedBehind()
	logf("baseline: %d acknowledged, %d named-and-behind", total, violations)
	if total == 0 {
		return AttemptResult{Outcome: OutcomeInvalid, Err: fmt.Errorf("writer produced no acks")}
	}

	// The delay goes on only after the replica is down: while it is still a
	// synchronous standby every commit would wait for the delayed WAL.
	logf("taking replica %s down", replica.Name)
	if err := k.setFenced(ctx, cfg.namespace, cfg.cluster, replica.Name); err != nil {
		return AttemptResult{Outcome: OutcomeInvalid, Err: err}
	}
	if err := k.waitSyncEmpty(ctx, cfg, primary); err != nil {
		return AttemptResult{Outcome: OutcomeInvalid, Err: err}
	}
	logf("primary no longer waits for %s (%s)", replica.Name, k.quorumState(ctx, cfg))
	if err := faults.addWALDelay(); err != nil {
		return AttemptResult{Outcome: OutcomeInvalid, Err: err}
	}
	logf("WAL delayed by %s from %s to %s; replica down for %s", cfg.netemDelay, primary, replica.Name, cfg.replicaDown)
	time.Sleep(cfg.replicaDown)

	logf("bringing replica %s back", replica.Name)
	if err := k.setFenced(ctx, cfg.namespace, cfg.cluster, ""); err != nil {
		return AttemptResult{Outcome: OutcomeError, Err: err}
	}
	readyAt, err := k.waitPodReady(ctx, cfg.namespace, replica.Name, cfg.windowTimeout)
	if err != nil {
		return AttemptResult{Outcome: OutcomeTriggerMiss, Err: err}
	}
	logf("replica %s Ready", replica.Name)

	sleepUntil(readyAt.Add(cfg.killOffset))
	logf("offset %s reached; cutting primary->replica and force-deleting the primary", cfg.killOffset)
	if err := faults.cut(); err != nil {
		return AttemptResult{Outcome: OutcomeError, Err: err}
	}
	if err := k.deletePodNow(ctx, cfg.namespace, primary); err != nil {
		return AttemptResult{Outcome: OutcomeError, Err: err}
	}
	logf("at kill: %s", k.quorumState(ctx, cfg))
	return runAfterKill(ctx, k, cfg, faults, store, primary)
}

// runAfterKill waits for the outcome of the kill and compares the
// acknowledged ids with whoever leads afterwards.
func runAfterKill(ctx context.Context, k *kubeClient, cfg attemptConfig, faults *netFaults,
	store *ackStore, primary string) AttemptResult {

	newPrimary, err := k.waitNewPrimary(ctx, cfg, primary)
	if err != nil {
		logf("FAILOVER not completed: %v (%s)", err, k.quorumState(ctx, cfg))
		// Nothing was promoted while the primary was unreachable. Lift the
		// faults and check that whoever leads afterwards has every ack.
		faults.remove()
		leader, lerr := k.waitAnyPrimary(ctx, cfg)
		if lerr != nil {
			return AttemptResult{Outcome: OutcomeNoFailover, Err: lerr}
		}
		acked := store.snapshot()
		presentIDs, err := k.readMarkerIDs(ctx, cfg.namespace, leader)
		if err != nil {
			return AttemptResult{Outcome: OutcomeNoFailover, Err: err}
		}
		v := computeVerdict(acked, presentIDs)
		logf("after the faults were lifted %s leads: acknowledged=%d lost=%d lostWhileNamed=%d",
			leader, v.Acknowledged, v.Lost, v.LostWhileNamed)
		role := "replica"
		if leader == primary {
			role = "old_primary"
		}
		if v.Lost > 0 {
			return AttemptResult{Outcome: OutcomeReproduced, Verdict: v, NewPrimary: role}
		}
		return AttemptResult{Outcome: OutcomeNoFailover, Verdict: v, NewPrimary: role}
	}
	logf("new primary: %s (replica)", newPrimary)

	pkts, perr := faults.netemPackets()
	logf("packets through the netem delay: %d (err=%v)", pkts, perr)
	if perr != nil || pkts == 0 {
		return AttemptResult{Outcome: OutcomeInvalid, NewPrimary: "replica",
			Err: fmt.Errorf("the WAL delay was not applied (packets=%d, err=%v)", pkts, perr)}
	}

	faults.remove()

	// Snapshot before reading: the writer keeps committing on the new primary,
	// and acks that arrive after the read must not be counted as lost.
	acked := store.snapshot()
	presentIDs, err := k.readMarkerIDs(ctx, cfg.namespace, newPrimary)
	if err != nil {
		return AttemptResult{Outcome: OutcomeError, NewPrimary: "replica", Err: err}
	}
	v := computeVerdict(acked, presentIDs)
	logf("acknowledged=%d namedAtAck=%d lost=%d lostWhileNamed=%d",
		v.Acknowledged, v.NamedAtAck, v.Lost, v.LostWhileNamed)
	logLostAcks(acked, v)
	logNamedBehind(acked)
	if v.Lost > 0 {
		for _, p := range []string{primary, newPrimary} {
			k.dumpPodLog(ctx, cfg.namespace, p, "postgres", 4*time.Minute)
		}
	}
	return AttemptResult{Outcome: outcomeFromVerdict(v), Verdict: v, NewPrimary: "replica"}
}

// logNamedBehind reports acknowledgements that returned while the standby was
// named but had not flushed the commit: a commit that did not wait although
// synchronous replication was configured.
func logNamedBehind(acks []Ack) {
	logf("%s", namedBehindSummary(acks))
}

func namedBehindSummary(acks []Ack) string {
	n := 0
	var first, last string
	for _, a := range acks {
		if a.Named && a.Behind {
			if n == 0 {
				first = a.TS
			}
			last = a.TS
			n++
		}
	}
	return fmt.Sprintf("named-and-behind acks: %d (first %s, last %s)", n, first, last)
}

// logLostAcks prints each lost acknowledgement so the run can be matched
// against the server logs afterwards.
func logLostAcks(acks []Ack, v Verdict) {
	for _, line := range lostAckLines(acks, v) {
		logf("%s", line)
	}
}

// lostAckLines lists up to 50 lost acknowledgements and then the latest
// acknowledgement that survived. It returns nothing when nothing was lost.
func lostAckLines(acks []Ack, v Verdict) []string {
	if len(v.LostIDs) == 0 {
		return nil
	}
	lost := make(map[int64]bool, len(v.LostIDs))
	for _, id := range v.LostIDs {
		lost[id] = true
	}
	var lines []string
	for _, a := range acks {
		if lost[a.ID] && len(lines) < 50 {
			lines = append(lines, fmt.Sprintf("lost ack: id=%d ts=%s named=%t behind=%t", a.ID, a.TS, a.Named, a.Behind))
		}
	}
	var last Ack
	for _, a := range acks {
		if !lost[a.ID] && a.TS > last.TS {
			last = a
		}
	}
	return append(lines, fmt.Sprintf("latest surviving ack: id=%d ts=%s named=%t", last.ID, last.TS, last.Named))
}

// quorumState renders the FailoverQuorum status for the log.
func (k *kubeClient) quorumState(ctx context.Context, cfg attemptConfig) string {
	return "FailoverQuorum: " + k.quorumStatus(ctx, cfg.namespace, cfg.cluster)
}

// waitSyncEmpty waits until the primary has stopped waiting for any standby.
func (k *kubeClient) waitSyncEmpty(ctx context.Context, cfg attemptConfig, primary string) error {
	deadline := time.Now().Add(cfg.windowTimeout)
	for time.Now().Before(deadline) {
		out, err := k.psql(ctx, cfg.namespace, primary, "postgres", "SHOW synchronous_standby_names")
		if err == nil && strings.TrimSpace(out) == "" {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("synchronous_standby_names not empty within %s after taking the replica down", cfg.windowTimeout)
}

// waitReplicationState waits until pg_stat_replication on the primary shows
// the replica as "<state>/<sync_state>".
func (k *kubeClient) waitReplicationState(ctx context.Context, cfg attemptConfig, primary, replica, want string) error {
	deadline := time.Now().Add(cfg.windowTimeout)
	q := fmt.Sprintf(
		"SELECT state || '/' || sync_state FROM pg_stat_replication WHERE application_name = '%s'", replica)
	for time.Now().Before(deadline) {
		out, err := k.psql(ctx, cfg.namespace, primary, "postgres", q)
		if err == nil && strings.TrimSpace(out) == want {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("replica %s not %s before start", replica, want)
}

// waitNewPrimary returns the first writable primary after the kill other than
// the killed one.
func (k *kubeClient) waitNewPrimary(ctx context.Context, cfg attemptConfig, oldPrimary string) (string, error) {
	deadline := time.Now().Add(cfg.failoverWait)
	time.Sleep(3 * time.Second)
	for time.Now().Before(deadline) {
		cur, err := k.currentPrimary(ctx, cfg.namespace, cfg.cluster)
		if err == nil && cur != "" && cur != oldPrimary {
			out, err := k.psql(ctx, cfg.namespace, cur, "postgres", "SELECT NOT pg_is_in_recovery()")
			if err == nil && strings.TrimSpace(out) == "t" {
				return cur, nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return "", fmt.Errorf("no writable new primary within %s", cfg.failoverWait)
}

// waitAnyPrimary returns the first writable primary, whoever it is.
func (k *kubeClient) waitAnyPrimary(ctx context.Context, cfg attemptConfig) (string, error) {
	deadline := time.Now().Add(cfg.failoverWait)
	for time.Now().Before(deadline) {
		if cur, err := k.currentPrimary(ctx, cfg.namespace, cfg.cluster); err == nil && cur != "" {
			out, err := k.psql(ctx, cfg.namespace, cur, "postgres", "SELECT NOT pg_is_in_recovery()")
			if err == nil && strings.TrimSpace(out) == "t" {
				return cur, nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return "", fmt.Errorf("no writable primary within %s after lifting the faults", cfg.failoverWait)
}

func (k *kubeClient) readMarkerIDs(ctx context.Context, ns, pod string) ([]int64, error) {
	out, err := k.psql(ctx, ns, pod, markerDB, "SELECT id FROM marker ORDER BY id")
	if err != nil {
		return nil, err
	}
	return parseIDs(out), nil
}

// parseIDs reads one id per line from psql's unaligned output, skipping
// blank and non-numeric lines.
func parseIDs(out string) []int64 {
	var ids []int64
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if n, err := strconv.ParseInt(line, 10, 64); err == nil {
			ids = append(ids, n)
		}
	}
	return ids
}

func sleepUntil(t time.Time) {
	if d := time.Until(t); d > 0 {
		time.Sleep(d)
	}
}

const netComment = "repro-8790"
