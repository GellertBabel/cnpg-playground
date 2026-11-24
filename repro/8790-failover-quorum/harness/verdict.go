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

import "sort"

// Ack is one acknowledged commit as reported by a writer. named is whether
// synchronous_standby_names was non-empty when the commit returned; behind is
// whether the standby had not yet flushed the commit's WAL position. With
// working synchronous replication a named standby cannot be behind.
type Ack struct {
	ID     int64  `json:"id"`
	TS     string `json:"ts"`
	Named  bool   `json:"named"`
	Behind bool   `json:"behind"`
}

// Outcome is the result of one reproduction attempt. Its value is the exit
// code of the run subcommand.
type Outcome int

const (
	OutcomeReproduced  Outcome = 0  // acknowledged commits are missing on the new primary
	OutcomeError       Outcome = 1  // unexpected error
	OutcomeInvalid     Outcome = 2  // preconditions not met, result not trustworthy
	OutcomeTriggerMiss Outcome = 3  // the replica never turned Ready after unfencing
	OutcomeNothingLost Outcome = 10 // failover happened, every acknowledged commit survived
	OutcomeNoFailover  Outcome = 11 // no promotion within the timeout, nothing proven lost
)

func (o Outcome) String() string {
	switch o {
	case OutcomeReproduced:
		return "reproduced"
	case OutcomeNothingLost:
		return "nothing_lost"
	case OutcomeNoFailover:
		return "no_failover"
	case OutcomeTriggerMiss:
		return "trigger_missed"
	case OutcomeInvalid:
		return "harness_invalid"
	default:
		return "error"
	}
}

// Verdict compares the acknowledged commits against the ids present on the new
// primary after the failover. Any acknowledged id that is missing is data loss.
type Verdict struct {
	Acknowledged   int
	NamedAtAck     int // acknowledged while the standby was named (sync said safe)
	Lost           int
	LostWhileNamed int
	LostIDs        []int64
}

func computeVerdict(acks []Ack, presentIDs []int64) Verdict {
	present := make(map[int64]struct{}, len(presentIDs))
	for _, id := range presentIDs {
		present[id] = struct{}{}
	}

	// De-duplicate acknowledged ids; parallel writers never share ids, but a
	// retry could log one twice.
	acked := make(map[int64]bool) // id -> named at ack time
	for _, a := range acks {
		if _, seen := acked[a.ID]; !seen || a.Named {
			acked[a.ID] = acked[a.ID] || a.Named
		}
	}

	var v Verdict
	for id, named := range acked {
		v.Acknowledged++
		if named {
			v.NamedAtAck++
		}
		if _, ok := present[id]; !ok {
			v.Lost++
			if named {
				v.LostWhileNamed++
			}
			v.LostIDs = append(v.LostIDs, id)
		}
	}
	sort.Slice(v.LostIDs, func(i, j int) bool { return v.LostIDs[i] < v.LostIDs[j] })
	return v
}

// outcomeFromVerdict maps a verdict to an outcome once a failover has completed.
func outcomeFromVerdict(v Verdict) Outcome {
	if v.Lost > 0 {
		return OutcomeReproduced
	}
	return OutcomeNothingLost
}
