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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCSVColumns(t *testing.T) {
	want := "offset_s,attempt,outcome,acknowledged,named_at_ack,lost,lost_while_named,new_primary"
	if csvHeader != want {
		t.Errorf("header %q, want %q", csvHeader, want)
	}
	res := AttemptResult{Outcome: OutcomeReproduced, NewPrimary: "replica",
		Verdict: Verdict{Acknowledged: 100, NamedAtAck: 40, Lost: 30, LostWhileNamed: 2}}
	row := csvRow(1500*time.Millisecond, 2, res)
	if row != "1.50,2,reproduced,100,40,30,2,replica" {
		t.Errorf("row %q", row)
	}
	if n, m := len(strings.Split(row, ",")), len(strings.Split(csvHeader, ",")); n != m {
		t.Errorf("row has %d columns, header %d", n, m)
	}
}

// scripted returns an attemptFunc that plays the given outcomes in order and
// records the offsets it was called with.
func scripted(outcomes []Outcome, offsets *[]time.Duration) attemptFunc {
	i := 0
	return func(_ context.Context, cfg attemptConfig) AttemptResult {
		*offsets = append(*offsets, cfg.killOffset)
		o := outcomes[i]
		i++
		res := AttemptResult{Outcome: o, NewPrimary: "replica"}
		if o == OutcomeNoFailover {
			res.NewPrimary = "old_primary"
		}
		if o == OutcomeError || o == OutcomeInvalid || o == OutcomeTriggerMiss {
			res.Err = errors.New(o.String())
		}
		return res
	}
}

func sweepRows(t *testing.T, dir string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "results.csv"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func TestRunSweepRetriesInvalidRuns(t *testing.T) {
	sweepSettle = 0
	dir := t.TempDir()
	var offs []time.Duration
	outcomes := []Outcome{OutcomeReproduced, OutcomeInvalid, OutcomeNothingLost, OutcomeNoFailover, OutcomeReproduced}
	err := runSweep(context.Background(), scripted(outcomes, &offs), attemptConfig{}, "t",
		[]time.Duration{0, time.Second}, 2, dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{0, 0, 0, time.Second, time.Second}
	if len(offs) != len(want) {
		t.Fatalf("offsets %v, want %v", offs, want)
	}
	for i := range want {
		if offs[i] != want[i] {
			t.Errorf("offsets %v, want %v", offs, want)
		}
	}
	rows := sweepRows(t, dir)
	if len(rows) != 6 || rows[0] != csvHeader || !strings.HasPrefix(rows[2], "0.00,2,harness_invalid,") ||
		!strings.HasPrefix(rows[4], "1.00,1,no_failover,") || !strings.HasSuffix(rows[4], ",old_primary") {
		t.Errorf("rows %q", rows)
	}
}

func TestRunSweepStopsAfterThreeInvalid(t *testing.T) {
	sweepSettle = 0
	var offs []time.Duration
	err := runSweep(context.Background(), scripted([]Outcome{OutcomeInvalid, OutcomeInvalid, OutcomeInvalid}, &offs),
		attemptConfig{}, "t", []time.Duration{0}, 3, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "harness invalid 3 times") {
		t.Errorf("got %v", err)
	}
}

func TestRunSweepReplicaNeverReady(t *testing.T) {
	sweepSettle = 0
	var offs []time.Duration
	err := runSweep(context.Background(), scripted([]Outcome{OutcomeTriggerMiss, OutcomeTriggerMiss, OutcomeTriggerMiss}, &offs),
		attemptConfig{}, "t", []time.Duration{0}, 3, t.TempDir())
	if !errors.Is(err, errTriggerUnreachable) {
		t.Errorf("got %v, want errTriggerUnreachable", err)
	}
}

func TestRunSweepStopsOnError(t *testing.T) {
	sweepSettle = 0
	var offs []time.Duration
	err := runSweep(context.Background(), scripted([]Outcome{OutcomeReproduced, OutcomeError}, &offs),
		attemptConfig{}, "t", []time.Duration{0}, 3, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "unexpected error") || len(offs) != 2 {
		t.Errorf("got %v after %d attempts", err, len(offs))
	}
}

func TestRunSweepInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var offs []time.Duration
	err := runSweep(ctx, scripted(nil, &offs), attemptConfig{}, "t", []time.Duration{0}, 1, t.TempDir())
	if !errors.Is(err, context.Canceled) || len(offs) != 0 {
		t.Errorf("got %v after %d attempts", err, len(offs))
	}
}
