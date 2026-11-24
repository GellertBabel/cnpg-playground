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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestFixedPayload(t *testing.T) {
	p := newPayloadSource(100, false)
	if got := p(); got != strings.Repeat("x", 100) {
		t.Errorf("fixed payload = %q", got)
	}
}

func TestRandomPayloadSizeAndVariation(t *testing.T) {
	for _, size := range []int{1, 99, 100, 20000} {
		p := newPayloadSource(size, true)
		a, b := p(), p()
		if len(a) != size || len(b) != size {
			t.Fatalf("size %d: got lengths %d and %d", size, len(a), len(b))
		}
		if size >= 16 && a == b {
			t.Errorf("size %d: two random payloads are equal", size)
		}
	}
}

func TestCommitInterval(t *testing.T) {
	cases := map[float64]time.Duration{0: 0, -1: 0, 10: 100 * time.Millisecond, 0.5: 2 * time.Second}
	for rate, want := range cases {
		if got := commitInterval(rate); got != want {
			t.Errorf("commitInterval(%v) = %v, want %v", rate, got, want)
		}
	}
}

// writerJob sets every variable; these are the values the writer reads.
func TestWriterConfigFromEnv(t *testing.T) {
	t.Setenv("URI", "postgresql://app@repro-8790-rw:5432/app")
	t.Setenv("REPLICA", "repro-8790-2")
	t.Setenv("PAYLOAD_BYTES", "100")
	t.Setenv("WRITERS", "4")
	t.Setenv("PAYLOAD_MODE", "random")
	t.Setenv("CHECKPOINT_EVERY", "500ms")
	t.Setenv("COMMIT_RATE", "2.5")
	cfg, err := writerConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	want := writerConfig{uri: "postgresql://app@repro-8790-rw:5432/app", replica: "repro-8790-2",
		payload: 100, writers: 4, random: true, checkpoint: 500 * time.Millisecond, rate: 2.5}
	if cfg != want {
		t.Errorf("got %+v, want %+v", cfg, want)
	}
}

// The Job passes CHECKPOINT_EVERY="" and COMMIT_RATE="0" by default.
func TestWriterConfigFromEnvDefaults(t *testing.T) {
	t.Setenv("URI", "postgresql://x")
	t.Setenv("REPLICA", "r")
	t.Setenv("PAYLOAD_BYTES", "")
	t.Setenv("WRITERS", "not-a-number")
	t.Setenv("PAYLOAD_MODE", "fixed")
	t.Setenv("CHECKPOINT_EVERY", "")
	t.Setenv("COMMIT_RATE", "0")
	cfg, err := writerConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.payload != 20000 || cfg.writers != 1 || cfg.random || cfg.checkpoint != 0 || cfg.rate != 0 {
		t.Errorf("defaults: got %+v", cfg)
	}
}

func TestWriterConfigFromEnvErrors(t *testing.T) {
	cases := []map[string]string{
		{"URI": "", "REPLICA": "r"},
		{"URI": "u", "REPLICA": ""},
		{"URI": "u", "REPLICA": "r", "COMMIT_RATE": "-1"},
		{"URI": "u", "REPLICA": "r", "COMMIT_RATE": "fast"},
		{"URI": "u", "REPLICA": "r", "CHECKPOINT_EVERY": "often"},
	}
	for _, env := range cases {
		t.Run("", func(t *testing.T) {
			for _, k := range []string{"URI", "REPLICA", "COMMIT_RATE", "CHECKPOINT_EVERY"} {
				t.Setenv(k, env[k])
			}
			if _, err := writerConfigFromEnv(); err == nil {
				t.Errorf("env %v: want an error", env)
			}
		})
	}
}

type countingExecer struct{ n atomic.Int32 }

func (c *countingExecer) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if sql == "CHECKPOINT" {
		c.n.Add(1)
	}
	return pgconn.CommandTag{}, nil
}

func TestCheckpointLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	db := &countingExecer{}
	checkpointLoop(ctx, db, 20*time.Millisecond) // returns when ctx is done
	if n := db.n.Load(); n < 2 {
		t.Errorf("issued %d CHECKPOINTs in 120ms at a 20ms interval, want at least 2", n)
	}
}

func TestParseAckLine(t *testing.T) {
	a, ok := parseAckLine([]byte(`{"id":7,"ts":"2026-11-23T12:00:00Z","named":true,"behind":true}`))
	if !ok || a != (Ack{ID: 7, TS: "2026-11-23T12:00:00Z", Named: true, Behind: true}) {
		t.Errorf("got %+v, %t", a, ok)
	}
	for _, line := range []string{"", "NOTICE: table \"marker\" does not exist, skipping", `{"named":true}`} {
		if _, ok := parseAckLine([]byte(line)); ok {
			t.Errorf("%q parsed as an ack", line)
		}
	}
}
