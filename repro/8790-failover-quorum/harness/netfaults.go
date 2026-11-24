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
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	corev1 "k8s.io/api/core/v1"
)

// netFaults applies and removes the node-level faults used to open the loss
// window: a one-directional WAL delay on the primary's node and a packet drop
// on the replica's node. Both match only the two pod IPs, so no other workload
// on the cluster is affected. Nothing is changed inside the Postgres pods.
//
// Several harness processes can run at once, one per cluster. Each has a slot
// (1..maxSlot): its rules carry the comment "repro-8790-s<slot>" and its delay
// lives in its own band of a root prio qdisc shared by all slots on a node.
type netFaults struct {
	primaryNode string
	replicaNode string
	primaryIP   string
	replicaIP   string
	primaryIP6  string
	replicaIP6  string
	delay       string
	slot        int
	comment     string
	delayActive bool
	cutActive   bool
}

// prio supports 16 bands; band 1 carries all unfiltered traffic.
const maxSlot = 15

func slotComment(slot int) string { return fmt.Sprintf("%s-s%d", netComment, slot) }

// slot layout inside the shared root qdisc
func slotBand(slot int) string    { return fmt.Sprintf("1:%x", slot+1) }
func slotHandle(slot int) string  { return fmt.Sprintf("%x:", 0x40+slot) }
func slotPrio4(slot int) string   { return strconv.Itoa(10 + slot) }
func slotPrio6(slot int) string   { return strconv.Itoa(40 + slot) }
func netemMarker(slot int) string { return "qdisc netem " + slotHandle(slot) }

// podIPs returns the IPv4 and IPv6 address of a pod. On a dual-stack cluster
// a filter that only matched IPv4 would miss IPv6 replication.
func podIPs(ips []corev1.PodIP) (v4, v6 string) {
	for _, ip := range ips {
		if strings.Contains(ip.IP, ":") {
			v6 = ip.IP
		} else {
			v4 = ip.IP
		}
	}
	return
}

// dockerExec runs a command on a kind node; tests replace it.
var dockerExec = runDockerExec

func runDockerExec(node string, args ...string) (string, error) {
	full := append([]string{"exec", node}, args...)
	cmd := exec.Command("docker", full...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("docker exec %s %s: %v: %s",
			node, strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// iptablesExec waits for the xtables lock, since several harness processes
// may change rules on the same node at once.
func iptablesExec(node, tool string, args ...string) (string, error) {
	return dockerExec(node, append([]string{tool, "-w"}, args...)...)
}

// withNodeLock serialises qdisc changes on one node across harness processes.
func withNodeLock(node string, fn func() error) error {
	f, err := os.OpenFile(fmt.Sprintf("%s/repro-8790-tc-%s.lock", os.TempDir(), node), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	return fn()
}

// ensureRoot installs the shared 16-band prio root whose all-zero priomap
// leaves every flow in band 1, unless it is already there.
func ensureRoot(node string) error {
	root, err := dockerExec(node, "tc", "qdisc", "show", "dev", "eth0", "root")
	if err != nil {
		return err
	}
	switch {
	case strings.Contains(root, "qdisc prio 1:") && strings.Contains(root, "bands 16"):
		return nil
	case strings.Contains(root, "qdisc noqueue"):
		args := []string{"tc", "qdisc", "add", "dev", "eth0", "root", "handle", "1:", "prio", "bands", "16", "priomap"}
		for i := 0; i < 16; i++ {
			args = append(args, "0")
		}
		_, err := dockerExec(node, args...)
		return err
	default:
		return fmt.Errorf("%s eth0 has an unexpected root qdisc (%s); run the cleanup subcommand",
			node, strings.TrimSpace(root))
	}
}

// dropRootIfUnused removes the shared root once no slot has a delay on it.
func dropRootIfUnused(node string) error {
	out, err := dockerExec(node, "tc", "qdisc", "show", "dev", "eth0")
	if err != nil {
		return err
	}
	if strings.Contains(out, "qdisc prio 1:") && !strings.Contains(out, "qdisc netem") {
		_, err = dockerExec(node, "tc", "qdisc", "del", "dev", "eth0", "root")
	}
	return err
}

// removeSlotDelay deletes one slot's filters and netem qdisc on a node, then
// the shared root if it is no longer used. Missing pieces are not an error.
func removeSlotDelay(node string, slot int) error {
	return withNodeLock(node, func() error {
		for _, p := range []struct{ proto, prio string }{{"ip", slotPrio4(slot)}, {"ipv6", slotPrio6(slot)}} {
			_, _ = dockerExec(node, "tc", "filter", "del", "dev", "eth0", "parent", "1:", "protocol", p.proto, "prio", p.prio)
		}
		_, _ = dockerExec(node, "tc", "qdisc", "del", "dev", "eth0", "parent", slotBand(slot), "handle", slotHandle(slot))
		return dropRootIfUnused(node)
	})
}

// addWALDelay routes only primary->replica packets into this slot's netem band.
func (n *netFaults) addWALDelay() error {
	if n.primaryNode == n.replicaNode {
		return fmt.Errorf("primary and replica share node %s; the delay would not cross the wire", n.primaryNode)
	}
	return withNodeLock(n.primaryNode, func() error {
		if err := ensureRoot(n.primaryNode); err != nil {
			return err
		}
		n.delayActive = true // set before the steps so a partial failure is still cleaned up
		for _, s := range n.delaySteps() {
			if _, err := dockerExec(n.primaryNode, s...); err != nil {
				return err
			}
		}
		return nil
	})
}

// delaySteps are the tc commands, run on the primary's node, that put this
// slot's netem qdisc in place and steer primary->replica packets into it.
func (n *netFaults) delaySteps() [][]string {
	steps := [][]string{
		{"tc", "qdisc", "replace", "dev", "eth0", "parent", slotBand(n.slot), "handle", slotHandle(n.slot),
			"netem", "delay", n.delay, "limit", "200000"},
		{"tc", "filter", "add", "dev", "eth0", "protocol", "ip", "parent", "1:", "prio", slotPrio4(n.slot), "u32",
			"match", "ip", "src", n.primaryIP + "/32", "match", "ip", "dst", n.replicaIP + "/32", "flowid", slotBand(n.slot)},
	}
	if n.primaryIP6 != "" && n.replicaIP6 != "" {
		steps = append(steps, []string{"tc", "filter", "add", "dev", "eth0", "protocol", "ipv6", "parent", "1:",
			"prio", slotPrio6(n.slot), "u32", "match", "ip6", "src", n.primaryIP6 + "/128",
			"match", "ip6", "dst", n.replicaIP6 + "/128", "flowid", slotBand(n.slot)})
	}
	return steps
}

// netemPackets returns how many packets have passed through this slot's
// netem qdisc. Zero after the replica reconnected means the filter never
// matched and the delay was not applied, so the run proves nothing.
func (n *netFaults) netemPackets() (int, error) {
	out, err := dockerExec(n.primaryNode, "tc", "-s", "qdisc", "show", "dev", "eth0")
	if err != nil {
		return 0, err
	}
	return parseNetemPackets(out, n.slot)
}

// parseNetemPackets reads the packet count of a slot's netem qdisc from the
// output of `tc -s qdisc show`.
func parseNetemPackets(out string, slot int) (int, error) {
	marker := netemMarker(slot)
	lines := strings.Split(out, "\n")
	for i, line := range lines {
		if !strings.Contains(line, marker) {
			continue
		}
		for _, next := range lines[i+1:] {
			f := strings.Fields(next)
			if len(f) >= 5 && f[0] == "Sent" && f[4] == "pkt" {
				var pkts int
				if _, err := fmt.Sscanf(f[3], "%d", &pkts); err != nil {
					return 0, err
				}
				return pkts, nil
			}
			if strings.HasPrefix(strings.TrimSpace(next), "qdisc") {
				break
			}
		}
	}
	return 0, fmt.Errorf("%s not found in: %s", marker, strings.TrimSpace(out))
}

type nodeRule struct {
	node, tool string
	spec       []string
}

// cut drops primary->replica packets on the replica's node, in the raw table
// so it matches the original source and also discards packets already queued
// in the delay on the primary's node.
func (n *netFaults) cut() error {
	n.cutActive = true
	for _, r := range n.cutRules() {
		if _, err := iptablesExec(r.node, r.tool, append([]string{"-t", "raw", "-I", "PREROUTING"}, r.spec...)...); err != nil {
			return err
		}
	}
	return nil
}

func (n *netFaults) cutRules() []nodeRule {
	drop := func(src, dst string) []string {
		return []string{"-s", src, "-d", dst, "-m", "comment", "--comment", n.comment, "-j", "DROP"}
	}
	rules := []nodeRule{{n.replicaNode, "iptables", drop(n.primaryIP, n.replicaIP)}}
	if n.primaryIP6 != "" && n.replicaIP6 != "" {
		rules = append(rules, nodeRule{n.replicaNode, "ip6tables", drop(n.primaryIP6, n.replicaIP6)})
	}
	return rules
}

func (n *netFaults) remove() {
	if n.cutActive {
		for _, r := range n.cutRules() {
			_, _ = iptablesExec(r.node, r.tool, append([]string{"-t", "raw", "-D", "PREROUTING"}, r.spec...)...)
		}
		n.cutActive = false
	}
	if n.delayActive {
		_ = removeSlotDelay(n.primaryNode, n.slot)
		n.delayActive = false
	}
}

// cleanupLeftovers removes faults left by a run that died before its own
// cleanup. With slot > 0 it touches only that slot's rules and delay band, so
// it is safe while other slots are running; slot 0 removes every harness rule
// and the shared root qdisc, and must only be used when nothing is running.
func cleanupLeftovers(nodes []string, slot int) error {
	comment := netComment
	if slot > 0 {
		comment = slotComment(slot)
	}
	for _, node := range nodes {
		for _, tool := range []string{"iptables", "ip6tables"} {
			rules, err := iptablesExec(node, tool, "-t", "raw", "-S", "PREROUTING")
			if err != nil {
				return err
			}
			for _, del := range deleteArgs(rules, comment) {
				if _, err := iptablesExec(node, tool, append([]string{"-t", "raw"}, del...)...); err != nil {
					return err
				}
				fmt.Printf("%s: removed %s raw rule: %s\n", node, tool, strings.Join(del, " "))
			}
		}
		if slot > 0 {
			if err := removeSlotDelay(node, slot); err != nil {
				return err
			}
			continue
		}
		err := withNodeLock(node, func() error {
			qdisc, err := dockerExec(node, "tc", "qdisc", "show", "dev", "eth0")
			if err != nil {
				return err
			}
			if strings.Contains(qdisc, "qdisc netem") || strings.Contains(qdisc, "qdisc prio 1:") {
				if _, err := dockerExec(node, "tc", "qdisc", "del", "dev", "eth0", "root"); err != nil {
					return err
				}
				fmt.Printf("%s: removed delay qdisc\n", node)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// deleteArgs turns the harness rules in `iptables -S` output into the
// arguments that delete them ("-D CHAIN ..."). Rules without the comment are
// left alone.
func deleteArgs(rules, comment string) [][]string {
	var out [][]string
	for _, line := range strings.Split(rules, "\n") {
		if !ruleHasComment(line, comment) {
			continue
		}
		del := strings.Replace(strings.TrimSpace(line), "-A ", "-D ", 1)
		out = append(out, strings.Fields(del))
	}
	return out
}

// ruleHasComment matches the comment as a whole word, so slot 1 does not
// match slot 11, while the bare prefix (slot 0) matches every slot.
func ruleHasComment(rule, comment string) bool {
	f := strings.Fields(rule)
	for i := 0; i+1 < len(f); i++ {
		if f[i] != "--comment" {
			continue
		}
		c := strings.Trim(f[i+1], `"`)
		if c == comment || (comment == netComment && strings.HasPrefix(c, netComment)) {
			return true
		}
	}
	return false
}
