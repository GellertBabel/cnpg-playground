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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// parseOffsets accepts either "start:end:step" (e.g. "0:5:0.25") or a
// comma/space separated list (e.g. "0,0.5,1"). Values are seconds.
func parseOffsets(spec string) ([]time.Duration, error) {
	spec = strings.TrimSpace(spec)
	if strings.Contains(spec, ":") {
		parts := strings.Split(spec, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("range must be start:end:step, got %q", spec)
		}
		start, err1 := strconv.ParseFloat(parts[0], 64)
		end, err2 := strconv.ParseFloat(parts[1], 64)
		step, err3 := strconv.ParseFloat(parts[2], 64)
		if err1 != nil || err2 != nil || err3 != nil || step <= 0 || end < start {
			return nil, fmt.Errorf("invalid range %q", spec)
		}
		var out []time.Duration
		for v := start; v <= end+1e-9; v += step {
			out = append(out, time.Duration(v*float64(time.Second)))
		}
		return out, nil
	}
	fields := strings.FieldsFunc(spec, func(r rune) bool { return r == ',' || r == ' ' })
	var out []time.Duration
	for _, f := range fields {
		v, err := strconv.ParseFloat(f, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid offset %q", f)
		}
		out = append(out, time.Duration(v*float64(time.Second)))
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no offsets in %q", spec)
	}
	return out, nil
}

// errTriggerUnreachable ends a sweep whose replica never turns Ready after
// unfencing; that is a result about the configuration, not a harness failure.
var errTriggerUnreachable = errors.New("trigger unreachable under this configuration")

// exitTriggerUnreachable is the sweep's exit code for errTriggerUnreachable.
const exitTriggerUnreachable = 3

// csvHeader names the columns of results.csv, one row per attempt.
const csvHeader = "offset_s,attempt,outcome,acknowledged,named_at_ack,lost,lost_while_named,new_primary"

func csvRow(off time.Duration, attempt int, res AttemptResult) string {
	v := res.Verdict
	return fmt.Sprintf("%.2f,%d,%s,%d,%d,%d,%d,%s",
		off.Seconds(), attempt, res.Outcome, v.Acknowledged, v.NamedAtAck, v.Lost, v.LostWhileNamed, res.NewPrimary)
}

type sweepTally struct {
	offset                                                       time.Duration
	runs, reproduced, nothingLost, noFailover, other, oldPrimary int
}

// attemptFunc runs one attempt; runSweep takes it as a parameter so the sweep
// rules can be tested without a cluster.
type attemptFunc func(ctx context.Context, cfg attemptConfig) AttemptResult

// sweepSettle is the pause between attempts that lets the cluster settle.
var sweepSettle = 5 * time.Second

// runSweep runs each offset repeats times and records the outcome per run. It
// stops early after too many consecutive invalid runs or an unexpected error,
// since those mean the environment, not the timing, is the problem.
func runSweep(ctx context.Context, attempt attemptFunc, cfg attemptConfig, label string,
	offsets []time.Duration, repeats int, outDir string) error {

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	csvPath := filepath.Join(outDir, "results.csv")
	csv, err := os.Create(csvPath)
	if err != nil {
		return err
	}
	defer func() { _ = csv.Close() }()
	if _, err := fmt.Fprintln(csv, csvHeader); err != nil {
		return err
	}

	tallies := make([]sweepTally, 0, len(offsets))
	invalidStreak := 0
	const maxInvalidStreak = 3
	// The trigger does not depend on the offset, so repeated misses before
	// any valid run mean the replica never gets there under this load.
	missStreak, validTotal := 0, 0
	const maxMissStreak = 3
	// Invalid runs and missed triggers are retried, so every offset gets
	// `repeats` valid samples, within a bound.
	maxAttempts := repeats * 3

	for _, off := range offsets {
		t := sweepTally{offset: off}
		valid := 0
		for r := 1; valid < repeats && r <= maxAttempts; r++ {
			if ctx.Err() != nil {
				tallies = append(tallies, t)
				printSweep(label, csvPath, tallies)
				return fmt.Errorf("interrupted: %w", ctx.Err())
			}
			logf("=== %s offset=%.2fs attempt %d (valid %d/%d)", label, off.Seconds(), r, valid, repeats)
			attemptCfg := cfg
			attemptCfg.killOffset = off
			res := attempt(ctx, attemptCfg)
			v := res.Verdict
			_, _ = fmt.Fprintln(csv, csvRow(off, r, res))
			_ = csv.Sync()
			if res.Err != nil {
				logf("    %s: %v", res.Outcome, res.Err)
			} else {
				logf("    %s acknowledged=%d lost=%d new_primary=%s", res.Outcome, v.Acknowledged, v.Lost, res.NewPrimary)
			}

			t.runs++
			if res.NewPrimary == "old_primary" {
				t.oldPrimary++
			}
			switch res.Outcome {
			case OutcomeReproduced:
				t.reproduced++
				valid++
				validTotal++
				invalidStreak, missStreak = 0, 0
			case OutcomeNothingLost:
				t.nothingLost++
				valid++
				validTotal++
				invalidStreak, missStreak = 0, 0
			case OutcomeNoFailover:
				t.noFailover++
				valid++
				validTotal++
				invalidStreak, missStreak = 0, 0
			case OutcomeTriggerMiss:
				t.other++
				invalidStreak = 0
				missStreak++
				if missStreak >= maxMissStreak && validTotal == 0 {
					tallies = append(tallies, t)
					printSweep(label, csvPath, tallies)
					return fmt.Errorf("%w: %d misses in a row and no valid run", errTriggerUnreachable, missStreak)
				}
			case OutcomeInvalid:
				t.other++
				invalidStreak++
			default:
				t.other++
				tallies = append(tallies, t)
				printSweep(label, csvPath, tallies)
				return fmt.Errorf("unexpected error, stopping: %v", res.Err)
			}
			if invalidStreak >= maxInvalidStreak {
				tallies = append(tallies, t)
				printSweep(label, csvPath, tallies)
				return fmt.Errorf("harness invalid %d times in a row, stopping", invalidStreak)
			}
			// Let the cluster settle between attempts.
			select {
			case <-ctx.Done():
			case <-time.After(sweepSettle):
			}
		}
		tallies = append(tallies, t)
	}
	printSweep(label, csvPath, tallies)
	return nil
}

func printSweep(label, csvPath string, tallies []sweepTally) {
	fmt.Printf("\nsummary for %s (results in %s):\n", label, csvPath)
	fmt.Printf("  %-9s %5s %11s %13s %12s %6s %12s\n",
		"offset_s", "runs", "reproduced", "nothing_lost", "no_failover", "other", "old_primary")
	for _, t := range tallies {
		fmt.Printf("  %-9.2f %5d %11d %13d %12d %6d %12d\n",
			t.offset.Seconds(), t.runs, t.reproduced, t.nothingLost, t.noFailover, t.other, t.oldPrimary)
	}
}
