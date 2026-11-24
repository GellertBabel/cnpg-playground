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
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func TestParseIDs(t *testing.T) {
	got := parseIDs("1\n2\n\n  3 \nnot-a-number\n")
	if want := []int64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got := parseIDs(""); got != nil {
		t.Errorf("empty: got %v", got)
	}
}

func TestNamedBehindSummary(t *testing.T) {
	acks := []Ack{
		{ID: 1, TS: "t1", Named: true},
		{ID: 2, TS: "t2", Named: true, Behind: true},
		{ID: 3, TS: "t3", Behind: true},
		{ID: 4, TS: "t4", Named: true, Behind: true},
	}
	if got, want := namedBehindSummary(acks), "named-and-behind acks: 2 (first t2, last t4)"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestLostAckLines(t *testing.T) {
	acks := []Ack{
		{ID: 1, TS: "t1"},
		{ID: 2, TS: "t2", Named: true, Behind: true},
		{ID: 3, TS: "t3", Named: true},
	}
	v := computeVerdict(acks, []int64{1, 3})
	got := lostAckLines(acks, v)
	want := []string{
		"lost ack: id=2 ts=t2 named=true behind=true",
		"latest surviving ack: id=3 ts=t3 named=true",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := lostAckLines(acks, computeVerdict(acks, []int64{1, 2, 3})); got != nil {
		t.Errorf("nothing lost: got %q", got)
	}
}

func TestLostAckLinesCapsAt50(t *testing.T) {
	var acks []Ack
	for i := int64(1); i <= 60; i++ {
		acks = append(acks, Ack{ID: i, TS: "t"})
	}
	got := lostAckLines(acks, computeVerdict(acks, nil))
	if len(got) != 51 || !strings.HasPrefix(got[50], "latest surviving ack: id=0") {
		t.Errorf("got %d lines, last %q", len(got), got[len(got)-1])
	}
}

func TestRunAttemptRejectsBadSlot(t *testing.T) {
	for _, slot := range []int{0, maxSlot + 1} {
		res := runAttempt(context.Background(), nil, attemptConfig{slot: slot})
		if res.Outcome != OutcomeError || res.Err == nil {
			t.Errorf("slot %d: got %v %v", slot, res.Outcome, res.Err)
		}
	}
}

func TestSleepUntil(t *testing.T) {
	start := time.Now()
	sleepUntil(start.Add(-time.Second)) // in the past: returns at once
	sleepUntil(start.Add(30 * time.Millisecond))
	if d := time.Since(start); d < 30*time.Millisecond || d > time.Second {
		t.Errorf("slept %s", d)
	}
}

// fakeCluster is a kubeClient backed by fake API clients whose Cluster
// reports currentPrimary, and whose psql answers the harness's queries:
// pg_is_in_recovery with "t", the marker read with markerOut, and anything
// else with other.
func fakeCluster(t *testing.T, currentPrimary, markerOut, other string) (*kubeClient, *[]string) {
	t.Helper()
	k := dynamicClient(cnpgObject(clusterGVR, "Cluster", "repro-8790",
		map[string]any{"status": map[string]any{"currentPrimary": currentPrimary}}))
	k.typed = fake.NewClientset()
	var queries []string
	k.exec = func(_ context.Context, ns, pod, container string, cmd ...string) (string, error) {
		q := cmd[len(cmd)-1]
		queries = append(queries, fmt.Sprintf("%s/%s/%s db=%s: %s", ns, pod, container, cmd[len(cmd)-3], q))
		switch {
		case strings.Contains(q, "pg_is_in_recovery"):
			return "t\n", nil
		case strings.Contains(q, "FROM marker"):
			return markerOut, nil
		}
		return other, nil
	}
	return k, &queries
}

func testConfig() attemptConfig {
	return attemptConfig{namespace: "repro-8790", cluster: "repro-8790", windowTimeout: time.Second, failoverWait: time.Second}
}

func ackedStore(acks ...Ack) *ackStore {
	s := &ackStore{}
	for _, a := range acks {
		s.add(a)
	}
	return s
}

func TestRunAfterKillFailoverLosesAcks(t *testing.T) {
	f := useFakeDocker(t, map[string]string{"pg-worker: tc -s qdisc show dev eth0": tcOutput(17, 0)})
	k, queries := fakeCluster(t, "repro-8790-2", "1\n3\n", "")
	faults := ipv4Faults()
	faults.cutActive = true
	store := ackedStore(Ack{ID: 1}, Ack{ID: 2, Named: true, Behind: true}, Ack{ID: 3, Named: true})
	cfg := testConfig()
	cfg.failoverWait = 30 * time.Second

	res := runAfterKill(context.Background(), k, cfg, faults, store, "repro-8790-1")
	if res.Outcome != OutcomeReproduced || res.NewPrimary != "replica" || res.Err != nil {
		t.Fatalf("got %+v", res)
	}
	if v := res.Verdict; v.Acknowledged != 3 || v.Lost != 1 || v.LostWhileNamed != 1 || v.LostIDs[0] != 2 {
		t.Errorf("verdict %+v", v)
	}
	if faults.cutActive {
		t.Error("faults not removed")
	}
	last := (*queries)[len(*queries)-1]
	if last != "repro-8790/repro-8790-2/postgres db=app: SELECT id FROM marker ORDER BY id" {
		t.Errorf("marker read: %q", last)
	}
	if !strings.Contains(strings.Join(f.calls, "\n"), "iptables -w -t raw -D PREROUTING") {
		t.Errorf("cut not removed: %v", f.calls)
	}
}

func TestRunAfterKillWithoutDelayIsInvalid(t *testing.T) {
	useFakeDocker(t, map[string]string{"pg-worker: tc -s qdisc show dev eth0": tcOutput(0, 0)})
	k, _ := fakeCluster(t, "repro-8790-2", "1\n", "")
	cfg := testConfig()
	cfg.failoverWait = 30 * time.Second
	res := runAfterKill(context.Background(), k, cfg, ipv4Faults(), ackedStore(Ack{ID: 1}), "repro-8790-1")
	if res.Outcome != OutcomeInvalid || res.Err == nil {
		t.Errorf("got %+v", res)
	}
}

// Without a promotion the old primary leads again once the faults are lifted.
func TestRunAfterKillNoFailover(t *testing.T) {
	for _, c := range []struct {
		markers string
		want    Outcome
		lost    int
	}{{"1\n2\n", OutcomeNoFailover, 0}, {"1\n", OutcomeReproduced, 1}} {
		useFakeDocker(t, nil)
		k, _ := fakeCluster(t, "repro-8790-1", c.markers, "")
		res := runAfterKill(context.Background(), k, testConfig(), ipv4Faults(),
			ackedStore(Ack{ID: 1}, Ack{ID: 2}), "repro-8790-1")
		if res.Outcome != c.want || res.NewPrimary != "old_primary" || res.Verdict.Lost != c.lost || res.Err != nil {
			t.Errorf("markers %q: got %+v", c.markers, res)
		}
	}
}

func TestWaitSyncEmpty(t *testing.T) {
	k, queries := fakeCluster(t, "repro-8790-1", "", "\n")
	if err := k.waitSyncEmpty(context.Background(), testConfig(), "repro-8790-1"); err != nil {
		t.Fatal(err)
	}
	if (*queries)[0] != "repro-8790/repro-8790-1/postgres db=postgres: SHOW synchronous_standby_names" {
		t.Errorf("query %q", (*queries)[0])
	}
	k, _ = fakeCluster(t, "repro-8790-1", "", "ANY 1 (\"repro-8790-2\")\n")
	cfg := testConfig()
	cfg.windowTimeout = 10 * time.Millisecond
	if err := k.waitSyncEmpty(context.Background(), cfg, "repro-8790-1"); err == nil {
		t.Error("want a timeout while the standby is still named")
	}
}

func TestWaitReplicationState(t *testing.T) {
	k, queries := fakeCluster(t, "repro-8790-1", "", "streaming/quorum\n")
	if err := k.waitReplicationState(context.Background(), testConfig(), "repro-8790-1", "repro-8790-2", "streaming/quorum"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains((*queries)[0], "WHERE application_name = 'repro-8790-2'") {
		t.Errorf("query %q", (*queries)[0])
	}
}
