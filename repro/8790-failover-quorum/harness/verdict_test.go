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
	"testing"
	"time"
)

func TestComputeVerdict(t *testing.T) {
	acks := []Ack{
		{ID: 1, Named: false, Behind: false},
		{ID: 2, Named: true, Behind: false},
		{ID: 3, Named: true, Behind: true},  // acknowledged while named, then lost
		{ID: 4, Named: false, Behind: true}, // acknowledged while not named, then lost
	}
	present := []int64{1, 2} // 3 and 4 missing on the new primary

	v := computeVerdict(acks, present)
	if v.Acknowledged != 4 {
		t.Errorf("acknowledged = %d, want 4", v.Acknowledged)
	}
	if v.NamedAtAck != 2 {
		t.Errorf("namedAtAck = %d, want 2", v.NamedAtAck)
	}
	if v.Lost != 2 {
		t.Errorf("lost = %d, want 2", v.Lost)
	}
	if v.LostWhileNamed != 1 {
		t.Errorf("lostWhileNamed = %d, want 1", v.LostWhileNamed)
	}
	if outcomeFromVerdict(v) != OutcomeReproduced {
		t.Errorf("outcome = %v, want reproduced", outcomeFromVerdict(v))
	}
}

func TestComputeVerdictNoLoss(t *testing.T) {
	acks := []Ack{{ID: 1, Named: true}, {ID: 2, Named: true}}
	v := computeVerdict(acks, []int64{1, 2, 3})
	if v.Lost != 0 {
		t.Errorf("lost = %d, want 0", v.Lost)
	}
	if outcomeFromVerdict(v) != OutcomeNothingLost {
		t.Errorf("outcome = %v, want nothing_lost", outcomeFromVerdict(v))
	}
}

func TestComputeVerdictDedup(t *testing.T) {
	// The same id logged twice, once not-named then named, counts once and as named.
	acks := []Ack{{ID: 7, Named: false}, {ID: 7, Named: true}}
	v := computeVerdict(acks, nil)
	if v.Acknowledged != 1 {
		t.Errorf("acknowledged = %d, want 1", v.Acknowledged)
	}
	if v.NamedAtAck != 1 {
		t.Errorf("namedAtAck = %d, want 1", v.NamedAtAck)
	}
	if v.Lost != 1 || v.LostWhileNamed != 1 {
		t.Errorf("lost = %d, lostWhileNamed = %d, want 1 and 1", v.Lost, v.LostWhileNamed)
	}
}

func TestParseOffsetsRange(t *testing.T) {
	got, err := parseOffsets("0:2:0.5")
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{0, 500 * time.Millisecond, time.Second, 1500 * time.Millisecond, 2 * time.Second}
	if len(got) != len(want) {
		t.Fatalf("got %d offsets %v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("offset[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestParseOffsetsList(t *testing.T) {
	got, err := parseOffsets("0, 1.25 ,3")
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{0, 1250 * time.Millisecond, 3 * time.Second}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("offset[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestParseOffsetsInvalid(t *testing.T) {
	for _, spec := range []string{"", "1:2", "a:b:c", "0:2:0", "nope"} {
		if _, err := parseOffsets(spec); err == nil {
			t.Errorf("parseOffsets(%q) = nil error, want error", spec)
		}
	}
}

// The README's exit-code table relies on these values and names.
func TestOutcomeCodesAndNames(t *testing.T) {
	cases := []struct {
		o    Outcome
		code int
		name string
	}{
		{OutcomeReproduced, 0, "reproduced"},
		{OutcomeError, 1, "error"},
		{OutcomeInvalid, 2, "harness_invalid"},
		{OutcomeTriggerMiss, 3, "trigger_missed"},
		{OutcomeNothingLost, 10, "nothing_lost"},
		{OutcomeNoFailover, 11, "no_failover"},
	}
	for _, c := range cases {
		if int(c.o) != c.code || c.o.String() != c.name {
			t.Errorf("outcome %d/%s, want %d/%s", int(c.o), c.o, c.code, c.name)
		}
	}
}

func TestComputeVerdictLostIDsSorted(t *testing.T) {
	acks := []Ack{{ID: 9}, {ID: 3}, {ID: 5}, {ID: 1}}
	v := computeVerdict(acks, []int64{5})
	want := []int64{1, 3, 9}
	if len(v.LostIDs) != len(want) {
		t.Fatalf("lost ids %v, want %v", v.LostIDs, want)
	}
	for i := range want {
		if v.LostIDs[i] != want[i] {
			t.Errorf("lost ids %v, want %v", v.LostIDs, want)
		}
	}
}
