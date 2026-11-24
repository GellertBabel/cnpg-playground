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

// Command harness reproduces cloudnative-pg#8790 without changing anything
// inside the Postgres pods. It delays WAL from the primary to the replica with
// tc netem on the kind node, brings the lagging replica back so the operator
// names it, then at a chosen offset cuts the link and force-deletes the
// primary. If the operator promotes the lagging replica, acknowledged commits
// are lost. The sweep subcommand repeats this over several offsets.
//
// Subcommands:
//
//	writer   in-cluster: commit rows and print JSON-line acknowledgements
//	run      run one attempt at a fixed --offset
//	sweep    run a range of offsets, repeated, and tabulate loss per offset
//	cleanup  remove any leftover node-level faults from a crashed run
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "writer":
		if err := runWriter(ctx); err != nil {
			fatal(err)
		}
	case "run":
		cmdRun(ctx, os.Args[2:])
	case "sweep":
		cmdSweep(ctx, os.Args[2:])
	case "cleanup":
		cmdCleanup(ctx, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: harness <writer|run|sweep|cleanup> [flags]")
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

func addCommonFlags(fs *flag.FlagSet, cfg *attemptConfig) {
	fs.IntVar(&cfg.slot, "slot", 1, fmt.Sprintf("fault slot 1..%d; parallel harness processes need distinct slots", maxSlot))
	fs.StringVar(&cfg.namespace, "namespace", "repro-8790", "cluster namespace")
	fs.StringVar(&cfg.cluster, "cluster", "repro-8790", "CloudNativePG Cluster name")
	fs.StringVar(&cfg.image, "image", "registry.dev:5000/cnpg-repro-writer:latest", "writer image, as the kind nodes pull it")
	fs.IntVar(&cfg.payload, "payload", 20000, "row payload bytes")
	fs.StringVar(&cfg.payloadMode, "payload-mode", "fixed", "fixed (compressible) or random (incompressible)")
	fs.StringVar(&cfg.checkpointEvery, "checkpoint-every", "", "issue CHECKPOINT from the writer at this interval, e.g. 500ms (empty: off)")
	fs.Float64Var(&cfg.commitRate, "rate", 0, "maximum commits per second per writer (0: as fast as possible)")
	fs.IntVar(&cfg.writers, "writers", 4, "concurrent writers")
	fs.StringVar(&cfg.netemDelay, "netem-delay", "2s", "WAL delay primary->replica")
	fs.DurationVar(&cfg.replicaDown, "replica-down", 20*time.Second, "how long the replica stays fenced")
	fs.DurationVar(&cfg.baseline, "baseline", 10*time.Second, "warm-up before fencing")
	fs.DurationVar(&cfg.windowTimeout, "window-timeout", 120*time.Second, "max wait for replica Ready/streaming")
	fs.DurationVar(&cfg.failoverWait, "failover-timeout", 300*time.Second, "max wait for a new primary")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "validate setup without applying faults or killing")
}

// applyWindowDefault widens the Ready window for long WAL delays: the
// replica needs several delayed round trips just to reconnect.
func applyWindowDefault(fs *flag.FlagSet, cfg *attemptConfig) {
	set := false
	fs.Visit(func(f *flag.Flag) { set = set || f.Name == "window-timeout" })
	if d, err := time.ParseDuration(cfg.netemDelay); !set && err == nil && d >= 5*time.Second {
		cfg.windowTimeout = 5 * time.Minute
	}
}

func cmdRun(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var cfg attemptConfig
	addCommonFlags(fs, &cfg)
	offset := fs.Duration("offset", 0, "time after the replica turns Ready to cut and kill")
	_ = fs.Parse(args)
	cfg.killOffset = *offset

	k, err := newKubeClient()
	if err != nil {
		fatal(err)
	}
	res := runAttempt(ctx, k, cfg)
	if cfg.dryRun {
		if res.Err != nil {
			logf("dry run failed: %v", res.Err)
			os.Exit(int(res.Outcome))
		}
		logf("dry run ok")
		return
	}
	if res.Err != nil {
		logf("outcome=%s err=%v", res.Outcome, res.Err)
	} else {
		logf("outcome=%s", res.Outcome)
	}
	os.Exit(int(res.Outcome))
}

func cmdSweep(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("sweep", flag.ExitOnError)
	var cfg attemptConfig
	addCommonFlags(fs, &cfg)
	offsetsSpec := fs.String("offsets", "0:5:0.5", "offsets: start:end:step or a list, seconds")
	repeats := fs.Int("repeats", 3, "runs per offset")
	label := fs.String("label", "sweep", "label for the output directory")
	outRoot := fs.String("out", "runs", "directory for run artifacts")
	_ = fs.Parse(args)
	applyWindowDefault(fs, &cfg)

	offsets, err := parseOffsets(*offsetsSpec)
	if err != nil {
		fatal(err)
	}
	k, err := newKubeClient()
	if err != nil {
		fatal(err)
	}
	outDir := filepath.Join(*outRoot, fmt.Sprintf("sweep-%s-%s", *label, time.Now().UTC().Format("20060102T150405Z")))
	attempt := func(ctx context.Context, c attemptConfig) AttemptResult { return runAttempt(ctx, k, c) }
	if err := runSweep(ctx, attempt, cfg, *label, offsets, *repeats, outDir); err != nil {
		if errors.Is(err, errTriggerUnreachable) {
			fmt.Fprintf(os.Stderr, "result: %v\n", err)
			os.Exit(exitTriggerUnreachable)
		}
		fatal(err)
	}
}

func cmdCleanup(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("cleanup", flag.ExitOnError)
	nodes := fs.String("nodes", "", "comma-separated kind node names to clean (default: the worker nodes of the current kubectl context)")
	slot := fs.Int("slot", 0, "slot to clean; 0 cleans every slot and must only be used when no harness runs")
	_ = fs.Parse(args)
	if *slot < 0 || *slot > maxSlot {
		fatal(fmt.Errorf("slot must be 0..%d", maxSlot))
	}
	list := splitNodes(*nodes)
	if len(list) == 0 {
		k, err := newKubeClient()
		if err != nil {
			fatal(err)
		}
		if list, err = k.workerNodeNames(ctx); err != nil {
			fatal(err)
		}
	}
	if err := cleanupLeftovers(list, *slot); err != nil {
		fatal(err)
	}
	fmt.Println("done")
}

// splitNodes turns the --nodes value into node names, ignoring empty entries.
func splitNodes(spec string) []string {
	var out []string
	for _, n := range strings.Split(spec, ",") {
		if n = strings.TrimSpace(n); n != "" {
			out = append(out, n)
		}
	}
	return out
}
