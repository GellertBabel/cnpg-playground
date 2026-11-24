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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// writerConfig is read from the environment because the writer runs as an
// in-cluster Job.
type writerConfig struct {
	uri        string
	replica    string
	payload    int
	writers    int
	random     bool
	checkpoint time.Duration
	// rate caps commits per second per writer; 0 means unthrottled.
	rate float64
}

func writerConfigFromEnv() (writerConfig, error) {
	cfg := writerConfig{
		uri:     os.Getenv("URI"),
		replica: os.Getenv("REPLICA"),
	}
	if cfg.uri == "" || cfg.replica == "" {
		return cfg, fmt.Errorf("URI and REPLICA must be set")
	}
	cfg.payload = envInt("PAYLOAD_BYTES", 20000)
	cfg.writers = envInt("WRITERS", 1)
	cfg.random = os.Getenv("PAYLOAD_MODE") == "random"
	if v := os.Getenv("COMMIT_RATE"); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil || r < 0 {
			return cfg, fmt.Errorf("COMMIT_RATE: invalid %q", v)
		}
		cfg.rate = r
	}
	if v := os.Getenv("CHECKPOINT_EVERY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("CHECKPOINT_EVERY: %w", err)
		}
		cfg.checkpoint = d
	}
	return cfg, nil
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// runWriter creates the marker table and then runs WRITERS concurrent commit
// loops. Each acknowledged commit is printed as one JSON line on stdout. The
// per-commit named/behind check runs in the same session right after COMMIT.
func runWriter(ctx context.Context) error {
	cfg, err := writerConfigFromEnv()
	if err != nil {
		return err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.uri)
	if err != nil {
		return fmt.Errorf("parse URI: %w", err)
	}
	poolCfg.MaxConns = int32(cfg.writers + 2)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	if err := setupTable(ctx, pool); err != nil {
		return err
	}

	// One encoder guarded by a mutex: concurrent writers must not interleave
	// partial JSON lines.
	enc := json.NewEncoder(os.Stdout)
	var mu sync.Mutex
	emit := func(a Ack) {
		mu.Lock()
		_ = enc.Encode(a)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	if cfg.checkpoint > 0 {
		// Client-issued CHECKPOINTs keep the checkpointer busy, which can
		// delay its pickup of a synchronous_standby_names change.
		wg.Add(1)
		go func() {
			defer wg.Done()
			checkpointLoop(ctx, pool, cfg.checkpoint)
		}()
	}
	for i := 0; i < cfg.writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			writeLoop(ctx, pool, cfg.replica, newPayloadSource(cfg.payload, cfg.random), commitInterval(cfg.rate), emit)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// execer is the part of *pgxpool.Pool that checkpointLoop needs.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func checkpointLoop(ctx context.Context, db execer, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			_, _ = db.Exec(ctx, "CHECKPOINT")
		}
	}
}

// newPayloadSource returns a payload generator. Fixed payloads compress to a
// few hundred bytes in TOAST; random hex does not compress, so each row costs
// about its full size in WAL.
func newPayloadSource(size int, random bool) func() string {
	if !random {
		fixed := strings.Repeat("x", size)
		return func() string { return fixed }
	}
	buf := make([]byte, (size+1)/2)
	return func() string {
		_, _ = rand.Read(buf)
		return hex.EncodeToString(buf)[:size]
	}
}

func setupTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS marker")
	if err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	_, err = pool.Exec(ctx, "CREATE TABLE marker (id bigserial PRIMARY KEY, payload text)")
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

// commitInterval is the minimum time between commit starts for one writer.
func commitInterval(rate float64) time.Duration {
	if rate <= 0 {
		return 0
	}
	return time.Duration(float64(time.Second) / rate)
}

func writeLoop(ctx context.Context, pool *pgxpool.Pool, replica string, payload func() string,
	interval time.Duration, emit func(Ack)) {
	for ctx.Err() == nil {
		start := time.Now()
		ack, err := commitOne(ctx, pool, replica, payload())
		if err != nil {
			// A failover window makes commits fail transiently; keep going
			// until the context is cancelled.
			time.Sleep(100 * time.Millisecond)
			continue
		}
		emit(ack)
		if wait := interval - time.Since(start); wait > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(wait):
			}
		}
	}
}

// commitOne inserts one row, records the WAL position taken before COMMIT, and
// then reads whether the standby was named and whether it was behind that
// position. The named/behind read is a separate, non-blocking query so it
// never gates the commit.
func commitOne(ctx context.Context, pool *pgxpool.Pool, replica, payload string) (Ack, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return Ack{}, err
	}
	defer conn.Release()

	var id int64
	var lsn string
	tx, err := conn.Begin(ctx)
	if err != nil {
		return Ack{}, err
	}
	if err := tx.QueryRow(ctx,
		"INSERT INTO marker (payload) VALUES ($1) RETURNING id", payload,
	).Scan(&id); err != nil {
		_ = tx.Rollback(ctx)
		return Ack{}, err
	}
	if err := tx.QueryRow(ctx,
		"SELECT pg_current_wal_insert_lsn()::text",
	).Scan(&lsn); err != nil {
		_ = tx.Rollback(ctx)
		return Ack{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Ack{}, err
	}

	var named, behind bool
	_ = conn.QueryRow(ctx, `
		SELECT current_setting('synchronous_standby_names') <> '',
		       coalesce(
		           (SELECT flush_lsn FROM pg_stat_replication WHERE application_name = $1),
		           '0/0'
		       ) < $2::pg_lsn`,
		replica, lsn,
	).Scan(&named, &behind)

	return Ack{ID: id, TS: time.Now().UTC().Format(time.RFC3339Nano), Named: named, Behind: behind}, nil
}

// parseAckLine parses one line of the writer's stdout. Lines that are not
// acknowledgements (blank, log noise) return false.
func parseAckLine(line []byte) (Ack, bool) {
	var a Ack
	if err := json.Unmarshal(line, &a); err != nil || a.ID == 0 {
		return Ack{}, false
	}
	return a, true
}
