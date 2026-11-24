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
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

var (
	podLogKeep = regexp.MustCompile(`(?i)synchronous|promot|walreceiver|wal stream|replication|redo done|last completed transaction|timeline`)
	podLogMsg  = regexp.MustCompile(`"message":"((?:\\"|[^"])*)"`)
)

// dumpPodLog prints a container's recent replication and promotion events,
// so a lossy run carries its own server-side evidence.
func (k *kubeClient) dumpPodLog(ctx context.Context, ns, pod, container string, since time.Duration) {
	secs := int64(since.Seconds())
	raw, err := k.typed.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{
		Container: container, SinceSeconds: &secs, Timestamps: true,
	}).DoRaw(ctx)
	if err != nil {
		logf("log %s: %v", pod, err)
		return
	}
	for _, line := range filterPodLog(raw) {
		logf("log %s %s", pod, line)
	}
}

// filterPodLog keeps the PostgreSQL and instance manager log lines about
// replication, promotion and timelines. Each kept line is "<kubelet
// timestamp> <text>", where text is the JSON "message" field when there is
// one, cut to 300 characters.
func filterPodLog(raw []byte) []string {
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		if !podLogKeep.MatchString(line) {
			continue
		}
		ts, _, _ := strings.Cut(line, " ")
		text := line
		if m := podLogMsg.FindStringSubmatch(line); m != nil {
			text = m[1]
		}
		if len(text) > 300 {
			text = text[:300]
		}
		out = append(out, ts+" "+text)
	}
	return out
}
