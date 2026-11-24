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
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// tcOutput mimics `tc -s qdisc show dev eth0` with the shared root and the
// netem qdiscs of slots 1 and 2.
func tcOutput(pkts1, pkts2 int) string {
	return fmt.Sprintf(`qdisc prio 1: root refcnt 2 bands 16 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
 Sent 90210 bytes 321 pkt (dropped 0, overlimits 0 requeues 0) 
 backlog 0b 0p requeues 0
qdisc netem 41: parent 1:2 limit 200000 delay 2s
 Sent 1234 bytes %d pkt (dropped 0, overlimits 0 requeues 0) 
 backlog 0b 0p requeues 0
qdisc netem 42: parent 1:3 limit 200000 delay 500ms
 Sent 99 bytes %d pkt (dropped 0, overlimits 0 requeues 0) 
 backlog 0b 0p requeues 0
`, pkts1, pkts2)
}

func TestParseNetemPacketsPerSlot(t *testing.T) {
	out := tcOutput(17, 5)
	for slot, want := range map[int]int{1: 17, 2: 5} {
		got, err := parseNetemPackets(out, slot)
		if err != nil || got != want {
			t.Errorf("slot %d: got %d, %v; want %d", slot, got, err, want)
		}
	}
}

func TestParseNetemPacketsZero(t *testing.T) {
	got, err := parseNetemPackets(tcOutput(0, 3), 1)
	if err != nil || got != 0 {
		t.Errorf("got %d, %v; want 0, nil", got, err)
	}
}

func TestParseNetemPacketsMissingSlot(t *testing.T) {
	if _, err := parseNetemPackets(tcOutput(1, 1), 3); err == nil {
		t.Error("want an error when the slot's netem qdisc is absent")
	}
}

func TestSlotLayout(t *testing.T) {
	cases := []struct {
		slot         int
		band, handle string
	}{{1, "1:2", "41:"}, {9, "1:a", "49:"}, {15, "1:10", "4f:"}}
	for _, c := range cases {
		if b, h := slotBand(c.slot), slotHandle(c.slot); b != c.band || h != c.handle {
			t.Errorf("slot %d: band %s handle %s, want %s %s", c.slot, b, h, c.band, c.handle)
		}
	}
}

func TestRuleHasComment(t *testing.T) {
	rule := `-A FORWARD -s 10.0.0.1/32 -m comment --comment repro-8790-s11 -j DROP`
	if ruleHasComment(rule, slotComment(1)) {
		t.Error("slot 1 must not match slot 11")
	}
	if !ruleHasComment(rule, slotComment(11)) {
		t.Error("slot 11 must match its own rule")
	}
	if !ruleHasComment(rule, netComment) {
		t.Error("the bare comment must match every slot")
	}
	if ruleHasComment(`-A FORWARD -m comment --comment other -j DROP`, netComment) {
		t.Error("foreign rules must not match")
	}
}

func TestPodIPs(t *testing.T) {
	v4, v6 := podIPs([]corev1.PodIP{{IP: "10.244.1.5"}, {IP: "fd00:10:244:1::5"}})
	if v4 != "10.244.1.5" || v6 != "fd00:10:244:1::5" {
		t.Errorf("dual-stack: got %q %q", v4, v6)
	}
	v4, v6 = podIPs([]corev1.PodIP{{IP: "10.244.2.7"}})
	if v4 != "10.244.2.7" || v6 != "" {
		t.Errorf("IPv4 only: got %q %q", v4, v6)
	}
}

func ipv4Faults() *netFaults {
	return &netFaults{
		primaryNode: "pg-worker", replicaNode: "pg-worker2",
		primaryIP: "10.244.1.5", replicaIP: "10.244.2.7",
		delay: "2s", slot: 1, comment: slotComment(1),
	}
}

func dualStackFaults() *netFaults {
	n := ipv4Faults()
	n.primaryIP6, n.replicaIP6 = "fd00::5", "fd00::7"
	return n
}

func TestCutRulesIPv4(t *testing.T) {
	rules := ipv4Faults().cutRules()
	if len(rules) != 1 {
		t.Fatalf("got %d rules, want 1", len(rules))
	}
	r := rules[0]
	want := "-s 10.244.1.5 -d 10.244.2.7 -m comment --comment repro-8790-s1 -j DROP"
	if r.node != "pg-worker2" || r.tool != "iptables" || strings.Join(r.spec, " ") != want {
		t.Errorf("got %s %s %q", r.node, r.tool, strings.Join(r.spec, " "))
	}
}

func TestCutRulesDualStack(t *testing.T) {
	rules := dualStackFaults().cutRules()
	if len(rules) != 2 {
		t.Fatalf("got %d rules, want 2", len(rules))
	}
	r := rules[1]
	want := "-s fd00::5 -d fd00::7 -m comment --comment repro-8790-s1 -j DROP"
	if r.node != "pg-worker2" || r.tool != "ip6tables" || strings.Join(r.spec, " ") != want {
		t.Errorf("got %s %s %q", r.node, r.tool, strings.Join(r.spec, " "))
	}
	for _, r := range rules {
		if !ruleHasComment("-A PREROUTING "+strings.Join(r.spec, " "), slotComment(1)) {
			t.Errorf("rule %v does not carry the slot comment", r.spec)
		}
	}
}

func TestDelaySteps(t *testing.T) {
	steps := ipv4Faults().delaySteps()
	if len(steps) != 2 {
		t.Fatalf("IPv4: got %d steps, want 2", len(steps))
	}
	wantQdisc := "tc qdisc replace dev eth0 parent 1:2 handle 41: netem delay 2s limit 200000"
	wantFilter := "tc filter add dev eth0 protocol ip parent 1: prio 11 u32 match ip src 10.244.1.5/32 match ip dst 10.244.2.7/32 flowid 1:2"
	if got := strings.Join(steps[0], " "); got != wantQdisc {
		t.Errorf("qdisc step %q, want %q", got, wantQdisc)
	}
	if got := strings.Join(steps[1], " "); got != wantFilter {
		t.Errorf("filter step %q, want %q", got, wantFilter)
	}
	steps = dualStackFaults().delaySteps()
	want6 := "tc filter add dev eth0 protocol ipv6 parent 1: prio 41 u32 match ip6 src fd00::5/128 match ip6 dst fd00::7/128 flowid 1:2"
	if len(steps) != 3 || strings.Join(steps[2], " ") != want6 {
		t.Errorf("dual-stack steps %v", steps)
	}
}

func TestAddWALDelayRefusesSharedNode(t *testing.T) {
	n := ipv4Faults()
	n.replicaNode = n.primaryNode
	if err := n.addWALDelay(); err == nil || !strings.Contains(err.Error(), "share node") {
		t.Errorf("got %v, want a shared-node error", err)
	}
	if n.delayActive {
		t.Error("no delay must be recorded as active")
	}
}

func TestDeleteArgs(t *testing.T) {
	out := `-P PREROUTING ACCEPT
-A PREROUTING -s 10.244.1.5/32 -d 10.244.2.7/32 -m comment --comment repro-8790-s1 -j DROP
-A PREROUTING -s 10.244.1.9/32 -d 10.244.2.9/32 -m comment --comment repro-8790-s2 -j DROP
-A PREROUTING -m comment --comment kube-proxy -j KUBE
`
	all := deleteArgs(out, netComment)
	if len(all) != 2 {
		t.Fatalf("slot 0: got %d deletions, want 2: %v", len(all), all)
	}
	want := "-D PREROUTING -s 10.244.1.5/32 -d 10.244.2.7/32 -m comment --comment repro-8790-s1 -j DROP"
	if got := strings.Join(all[0], " "); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if one := deleteArgs(out, slotComment(2)); len(one) != 1 || one[0][2] != "-s" || one[0][3] != "10.244.1.9/32" {
		t.Errorf("slot 2: got %v", one)
	}
}

func TestWithNodeLock(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	called := false
	if err := withNodeLock("pg-worker", func() error { called = true; return nil }); err != nil || !called {
		t.Errorf("err=%v called=%t", err, called)
	}
	wantErr := fmt.Errorf("boom")
	if err := withNodeLock("pg-worker", func() error { return wantErr }); err != wantErr {
		t.Errorf("got %v, want the callback's error", err)
	}
}

// fakeDocker stands in for `docker exec`: it records every command as
// "<node>: <args>" and answers from replies, keyed the same way.
type fakeDocker struct {
	calls   []string
	replies map[string]string
	fail    map[string]bool
}

func useFakeDocker(t *testing.T, replies map[string]string) *fakeDocker {
	t.Helper()
	t.Setenv("TMPDIR", t.TempDir()) // node lock files
	f := &fakeDocker{replies: replies, fail: map[string]bool{}}
	orig := dockerExec
	dockerExec = func(node string, args ...string) (string, error) {
		key := node + ": " + strings.Join(args, " ")
		f.calls = append(f.calls, key)
		if f.fail[key] {
			return "", fmt.Errorf("%s failed", key)
		}
		return f.replies[key], nil
	}
	t.Cleanup(func() { dockerExec = orig })
	return f
}

func (f *fakeDocker) expect(t *testing.T, want ...string) {
	t.Helper()
	if strings.Join(f.calls, "\n") != strings.Join(want, "\n") {
		t.Errorf("docker calls\n got:\n  %s\nwant:\n  %s", strings.Join(f.calls, "\n  "), strings.Join(want, "\n  "))
	}
}

const (
	noqueueRoot = "qdisc noqueue 0: root refcnt 2\n"
	prioOnly    = "qdisc prio 1: root refcnt 2 bands 16 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
)

// One attempt's fault lifecycle: delay, cut, packet count, removal.
func TestFaultLifecycle(t *testing.T) {
	f := useFakeDocker(t, map[string]string{
		"pg-worker: tc qdisc show dev eth0 root": noqueueRoot,
		"pg-worker: tc -s qdisc show dev eth0":   tcOutput(17, 0),
		"pg-worker: tc qdisc show dev eth0":      prioOnly,
	})
	n := ipv4Faults()
	if err := n.addWALDelay(); err != nil {
		t.Fatal(err)
	}
	if err := n.cut(); err != nil {
		t.Fatal(err)
	}
	if pkts, err := n.netemPackets(); err != nil || pkts != 17 {
		t.Errorf("packets %d, %v", pkts, err)
	}
	n.remove()
	n.remove() // idempotent: nothing left to remove
	cut := "-s 10.244.1.5 -d 10.244.2.7 -m comment --comment repro-8790-s1 -j DROP"
	f.expect(t,
		"pg-worker: tc qdisc show dev eth0 root",
		"pg-worker: tc qdisc add dev eth0 root handle 1: prio bands 16 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0",
		"pg-worker: tc qdisc replace dev eth0 parent 1:2 handle 41: netem delay 2s limit 200000",
		"pg-worker: tc filter add dev eth0 protocol ip parent 1: prio 11 u32 match ip src 10.244.1.5/32 match ip dst 10.244.2.7/32 flowid 1:2",
		"pg-worker2: iptables -w -t raw -I PREROUTING "+cut,
		"pg-worker: tc -s qdisc show dev eth0",
		"pg-worker2: iptables -w -t raw -D PREROUTING "+cut,
		"pg-worker: tc filter del dev eth0 parent 1: protocol ip prio 11",
		"pg-worker: tc filter del dev eth0 parent 1: protocol ipv6 prio 41",
		"pg-worker: tc qdisc del dev eth0 parent 1:2 handle 41:",
		"pg-worker: tc qdisc show dev eth0",
		"pg-worker: tc qdisc del dev eth0 root",
	)
}

// Another slot's delay keeps the shared root in place.
func TestEnsureRootSharedAndKept(t *testing.T) {
	shared := prioOnly + "qdisc netem 42: parent 1:3 limit 200000 delay 500ms\n"
	f := useFakeDocker(t, map[string]string{
		"pg-worker: tc qdisc show dev eth0 root": prioOnly,
		"pg-worker: tc qdisc show dev eth0":      shared,
	})
	if err := ensureRoot("pg-worker"); err != nil {
		t.Fatal(err)
	}
	if err := dropRootIfUnused("pg-worker"); err != nil {
		t.Fatal(err)
	}
	f.expect(t, "pg-worker: tc qdisc show dev eth0 root", "pg-worker: tc qdisc show dev eth0")
}

func TestEnsureRootUnexpected(t *testing.T) {
	useFakeDocker(t, map[string]string{"pg-worker: tc qdisc show dev eth0 root": "qdisc htb 1: root\n"})
	if err := ensureRoot("pg-worker"); err == nil || !strings.Contains(err.Error(), "run the cleanup subcommand") {
		t.Errorf("got %v", err)
	}
}

func TestAddWALDelayStepFailureIsCleanedUp(t *testing.T) {
	f := useFakeDocker(t, map[string]string{"pg-worker: tc qdisc show dev eth0 root": noqueueRoot})
	f.fail["pg-worker: tc qdisc replace dev eth0 parent 1:2 handle 41: netem delay 2s limit 200000"] = true
	n := ipv4Faults()
	if err := n.addWALDelay(); err == nil {
		t.Fatal("want the tc error")
	}
	if !n.delayActive {
		t.Error("a partial delay must still be removed by remove()")
	}
}

const leftoverRules = `-P PREROUTING ACCEPT
-A PREROUTING -s 10.244.1.5/32 -d 10.244.2.7/32 -m comment --comment repro-8790-s1 -j DROP
-A PREROUTING -s 10.244.1.9/32 -d 10.244.2.9/32 -m comment --comment repro-8790-s2 -j DROP
`

func TestCleanupLeftoversAllSlots(t *testing.T) {
	f := useFakeDocker(t, map[string]string{
		"w1: iptables -w -t raw -S PREROUTING": leftoverRules,
		"w1: tc qdisc show dev eth0":           prioOnly + "qdisc netem 41: parent 1:2 limit 200000 delay 2s\n",
		"w2: tc qdisc show dev eth0":           noqueueRoot,
	})
	if err := cleanupLeftovers([]string{"w1", "w2"}, 0); err != nil {
		t.Fatal(err)
	}
	f.expect(t,
		"w1: iptables -w -t raw -S PREROUTING",
		"w1: iptables -w -t raw -D PREROUTING -s 10.244.1.5/32 -d 10.244.2.7/32 -m comment --comment repro-8790-s1 -j DROP",
		"w1: iptables -w -t raw -D PREROUTING -s 10.244.1.9/32 -d 10.244.2.9/32 -m comment --comment repro-8790-s2 -j DROP",
		"w1: ip6tables -w -t raw -S PREROUTING",
		"w1: tc qdisc show dev eth0",
		"w1: tc qdisc del dev eth0 root",
		"w2: iptables -w -t raw -S PREROUTING",
		"w2: ip6tables -w -t raw -S PREROUTING",
		"w2: tc qdisc show dev eth0",
	)
}

func TestCleanupLeftoversOneSlot(t *testing.T) {
	f := useFakeDocker(t, map[string]string{
		"w1: iptables -w -t raw -S PREROUTING": leftoverRules,
		"w1: tc qdisc show dev eth0":           noqueueRoot,
	})
	if err := cleanupLeftovers([]string{"w1"}, 2); err != nil {
		t.Fatal(err)
	}
	f.expect(t,
		"w1: iptables -w -t raw -S PREROUTING",
		"w1: iptables -w -t raw -D PREROUTING -s 10.244.1.9/32 -d 10.244.2.9/32 -m comment --comment repro-8790-s2 -j DROP",
		"w1: ip6tables -w -t raw -S PREROUTING",
		"w1: tc filter del dev eth0 parent 1: protocol ip prio 12",
		"w1: tc filter del dev eth0 parent 1: protocol ipv6 prio 42",
		"w1: tc qdisc del dev eth0 parent 1:3 handle 42:",
		"w1: tc qdisc show dev eth0",
	)
}

func TestCleanupLeftoversNodeUnreachable(t *testing.T) {
	f := useFakeDocker(t, nil)
	f.fail["gone: iptables -w -t raw -S PREROUTING"] = true
	if err := cleanupLeftovers([]string{"gone"}, 0); err == nil {
		t.Error("want an error for an unreachable node")
	}
}
