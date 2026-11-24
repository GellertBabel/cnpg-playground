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
	"strings"
	"testing"
)

func TestFilterPodLog(t *testing.T) {
	raw := strings.Join([]string{
		`2026-11-23T12:00:01.1Z {"level":"info","msg":"record","record":{"message":"redo done at 0/D954788"}}`,
		`2026-11-23T12:00:01.2Z {"level":"info","msg":"record","record":{"message":"selected new timeline ID: 2"}}`,
		`2026-11-23T12:00:01.3Z {"level":"info","msg":"checkpoint starting: time"}`,
		`2026-11-23T12:00:01.4Z plain text: received promote request`,
		``,
	}, "\n")
	got := filterPodLog([]byte(raw))
	want := []string{
		"2026-11-23T12:00:01.1Z redo done at 0/D954788",
		"2026-11-23T12:00:01.2Z selected new timeline ID: 2",
		"2026-11-23T12:00:01.4Z 2026-11-23T12:00:01.4Z plain text: received promote request",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d lines %q, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d: %q, want %q", i, got[i], want[i])
		}
	}
}

func TestFilterPodLogTruncates(t *testing.T) {
	long := strings.Repeat("y", 500)
	got := filterPodLog([]byte(`ts {"message":"replication ` + long + `"}`))
	if len(got) != 1 || len(got[0]) != len("ts ")+300 {
		t.Errorf("got %d lines, first %d chars", len(got), len(got[0]))
	}
}

// The message field keeps escaped quotes.
func TestFilterPodLogEscapedQuotes(t *testing.T) {
	got := filterPodLog([]byte(`ts {"message":"started streaming WAL from \"primary\" timeline 1"}`))
	if len(got) != 1 || got[0] != `ts started streaming WAL from \"primary\" timeline 1` {
		t.Errorf("got %q", got)
	}
}
