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
	"flag"
	"reflect"
	"testing"
	"time"
)

func parseCommon(t *testing.T, args ...string) (*flag.FlagSet, attemptConfig) {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var cfg attemptConfig
	addCommonFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return fs, cfg
}

// The README and cluster.yaml rely on these defaults.
func TestCommonFlagDefaults(t *testing.T) {
	_, cfg := parseCommon(t)
	want := attemptConfig{
		slot:          1,
		namespace:     "repro-8790",
		cluster:       "repro-8790",
		image:         "registry.dev:5000/cnpg-repro-writer:latest",
		payload:       20000,
		payloadMode:   "fixed",
		writers:       4,
		netemDelay:    "2s",
		replicaDown:   20 * time.Second,
		baseline:      10 * time.Second,
		windowTimeout: 120 * time.Second,
		failoverWait:  300 * time.Second,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("defaults\n got %+v\nwant %+v", cfg, want)
	}
}

func TestCommonFlagsParse(t *testing.T) {
	_, cfg := parseCommon(t, "--slot", "3", "--netem-delay", "500ms", "--writers", "1", "--rate", "2", "--dry-run")
	if cfg.slot != 3 || cfg.netemDelay != "500ms" || cfg.writers != 1 || cfg.commitRate != 2 || !cfg.dryRun {
		t.Errorf("got %+v", cfg)
	}
}

func TestApplyWindowDefault(t *testing.T) {
	cases := []struct {
		args []string
		want time.Duration
	}{
		{nil, 120 * time.Second},
		{[]string{"--netem-delay", "10s"}, 5 * time.Minute},
		{[]string{"--netem-delay", "10s", "--window-timeout", "1m"}, time.Minute},
		{[]string{"--netem-delay", "bogus"}, 120 * time.Second},
	}
	for _, c := range cases {
		fs, cfg := parseCommon(t, c.args...)
		applyWindowDefault(fs, &cfg)
		if cfg.windowTimeout != c.want {
			t.Errorf("%v: window %s, want %s", c.args, cfg.windowTimeout, c.want)
		}
	}
}

func TestSplitNodes(t *testing.T) {
	if got := splitNodes(""); got != nil {
		t.Errorf("empty: got %v", got)
	}
	got := splitNodes("a-worker, a-worker2,,a-worker3 ")
	want := []string{"a-worker", "a-worker2", "a-worker3"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
