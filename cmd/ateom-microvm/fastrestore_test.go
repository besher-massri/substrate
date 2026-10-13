//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
)

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// With nothing set, every knob is off: the zero config is today's behavior.
func TestFastRestoreConfigDefaultsOff(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c != (fastRestoreConfig{}) {
		t.Errorf("default config = %+v, want the zero value", c)
	}
	if c.enabled() {
		t.Error("default config reports enabled")
	}
	for _, milli := range []int64{0, 250, 1000} {
		if got := c.activationMilliCPU(milli); got != milli {
			t.Errorf("activationMilliCPU(%d) = %d with the boost off", milli, got)
		}
		if c.boosted(milli) {
			t.Errorf("boosted(%d) with the boost off", milli)
		}
	}
}

func TestFastRestoreConfigFromEnv(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(map[string]string{
		envActivationCPUBoost:      "2",
		envParallelWakeup:          "true",
		envMemoryRestoreMode:       "OnDemand",
		envOnDemandBackgroundFetch: "false",
		envRestoreReadahead:        "1",
		envEarlyVMMLaunch:          "TRUE",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := fastRestoreConfig{
		boost: true, boostMilliCPU: 2000, parallelWakeup: true,
		memRestoreMode: ch.MemRestoreOnDemand, noBackgroundFetch: true,
		readahead: true, earlyVMMLaunch: true,
	}
	if c != want {
		t.Errorf("config = %+v, want %+v", c, want)
	}
	if !c.enabled() {
		t.Error("config with knobs set reports disabled")
	}
	if len(c.attrs()) == 0 {
		t.Error("attrs() is empty")
	}
}

func TestFastRestoreConfigRejectsBadValues(t *testing.T) {
	for _, env := range []map[string]string{
		{envActivationCPUBoost: "lots"},
		{envActivationCPUBoost: "-1"},
		{envActivationCPUBoost: "0m"},
		{envParallelWakeup: "sometimes"},
		{envMemoryRestoreMode: "fast"},
		{envOnDemandBackgroundFetch: "maybe"},
	} {
		if _, err := fastRestoreConfigFromEnv(envMap(env)); err == nil {
			t.Errorf("fastRestoreConfigFromEnv(%v) succeeded, want an error", env)
		}
	}
}

func TestParseCPUBoost(t *testing.T) {
	for _, tc := range []struct {
		in      string
		enabled bool
		milli   int64
	}{
		{"", false, 0},
		{"off", false, 0},
		{"0", false, 0},
		{"max", true, 0},
		{"unlimited", true, 0},
		{"1500m", true, 1500},
		{"1", true, 1000},
		{"0.25", true, 250},
		{" 2 ", true, 2000},
	} {
		enabled, milli, err := parseCPUBoost(tc.in)
		if err != nil {
			t.Errorf("parseCPUBoost(%q): %v", tc.in, err)
			continue
		}
		if enabled != tc.enabled || milli != tc.milli {
			t.Errorf("parseCPUBoost(%q) = (%v, %d), want (%v, %d)", tc.in, enabled, milli, tc.enabled, tc.milli)
		}
	}
}

func TestActivationMilliCPU(t *testing.T) {
	unlimited := fastRestoreConfig{boost: true}
	twoCores := fastRestoreConfig{boost: true, boostMilliCPU: 2000}
	for _, tc := range []struct {
		name    string
		c       fastRestoreConfig
		actor   int64
		want    int64
		boosted bool
	}{
		{"unlimited boost lifts a fractional actor", unlimited, 250, 0, true},
		{"a capped boost lifts a smaller actor", twoCores, 250, 2000, true},
		{"a capped boost never lowers a bigger actor", twoCores, 4000, 4000, false},
		{"an uncapped actor stays uncapped", twoCores, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.c.activationMilliCPU(tc.actor); got != tc.want {
				t.Errorf("activationMilliCPU(%d) = %d, want %d", tc.actor, got, tc.want)
			}
			if got := tc.c.boosted(tc.actor); got != tc.boosted {
				t.Errorf("boosted(%d) = %v, want %v", tc.actor, got, tc.boosted)
			}
		})
	}
}

func TestPickRestoreMemMode(t *testing.T) {
	ctx := context.Background()
	v52 := ch.VMMInfo{Version: "52.0.0"}
	v53 := ch.VMMInfo{Version: "53.0.0"}
	v53patched := ch.VMMInfo{Version: "53.0.0", Features: []string{"kvm", ch.FeatureRestoreBackgroundFetchPages}}
	for _, tc := range []struct {
		name     string
		c        fastRestoreConfig
		info     ch.VMMInfo
		wantMode string
		wantNoBG bool
	}{
		{"auto on v53 is eager", fastRestoreConfig{}, v53, ch.MemRestoreEager, false},
		{"auto on v52 is on-demand", fastRestoreConfig{}, v52, ch.MemRestoreOnDemand, false},
		{"forced copy on v52", fastRestoreConfig{memRestoreMode: ch.MemRestoreEager}, v52, ch.MemRestoreEager, false},
		{"forced on-demand on v52", fastRestoreConfig{memRestoreMode: ch.MemRestoreOnDemand}, v52, ch.MemRestoreOnDemand, false},
		{"forced on-demand on stock v53 falls back", fastRestoreConfig{memRestoreMode: ch.MemRestoreOnDemand, noBackgroundFetch: true}, v53, ch.MemRestoreEager, false},
		{"forced on-demand on patched v53 without opting out of prefault falls back", fastRestoreConfig{memRestoreMode: ch.MemRestoreOnDemand}, v53patched, ch.MemRestoreEager, false},
		{"forced on-demand on patched v53 disables background fetch", fastRestoreConfig{memRestoreMode: ch.MemRestoreOnDemand, noBackgroundFetch: true}, v53patched, ch.MemRestoreOnDemand, true},
		{"no-background-fetch alone keeps auto eager on v53", fastRestoreConfig{noBackgroundFetch: true}, v53patched, ch.MemRestoreEager, false},
		{"ondemand-prefault keeps the prefault on stock v53", fastRestoreConfig{memRestoreMode: ch.MemRestoreOnDemand, allowPrefault: true}, v53, ch.MemRestoreOnDemand, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode, noBG := tc.c.pickRestoreMemMode(ctx, tc.info)
			if mode != tc.wantMode || noBG != tc.wantNoBG {
				t.Errorf("pickRestoreMemMode = (%q, %v), want (%q, %v)", mode, noBG, tc.wantMode, tc.wantNoBG)
			}
		})
	}
}

func TestSharesInode(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "memory-ranges")
	if err := os.WriteFile(a, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if sharesInode(a) {
		t.Error("a single-link file shares its inode")
	}
	if err := os.Link(a, filepath.Join(dir, "cache")); err != nil {
		t.Fatal(err)
	}
	if !sharesInode(a) {
		t.Error("a hard-linked file does not share its inode")
	}
	if sharesInode(filepath.Join(dir, "missing")) {
		t.Error("a missing file shares its inode")
	}
}

// Readahead is best effort: missing files and real files both just return.
func TestReadaheadMemoryImage(t *testing.T) {
	dir := t.TempDir()
	readaheadMemoryImage(context.Background(), filepath.Join(dir, "missing"))
	f := filepath.Join(dir, "memory-ranges")
	if err := os.WriteFile(f, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	readaheadMemoryImage(context.Background(), f)
}

// readaheadDataExtents walks a sparse image (data, hole, data, trailing hole)
// without error, including chunks that straddle an extent end, and stops on a
// cancelled context.
func TestReadaheadDataExtents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory-ranges")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	const mib = 1 << 20
	if err := f.Truncate(16 * mib); err != nil {
		t.Fatal(err)
	}
	for _, off := range []int64{0, 6 * mib} {
		if _, err := f.WriteAt(make([]byte, 3*mib+12345), off); err != nil {
			t.Fatal(err)
		}
	}
	if err := readaheadDataExtents(context.Background(), int(f.Fd()), 16*mib, readaheadChunk); err != nil {
		t.Fatalf("readaheadDataExtents: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := readaheadDataExtents(ctx, int(f.Fd()), 16*mib, readaheadChunk); !errors.Is(err, context.Canceled) {
		t.Errorf("readaheadDataExtents with a cancelled context = %v, want context.Canceled", err)
	}
	empty, err := os.Create(filepath.Join(t.TempDir(), "empty"))
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if err := empty.Truncate(4 * mib); err != nil {
		t.Fatal(err)
	}
	if err := readaheadDataExtents(context.Background(), int(empty.Fd()), 4*mib, readaheadChunk); err != nil {
		t.Errorf("readaheadDataExtents on an all-hole file: %v", err)
	}
}

func TestFastRestoreConfigOnDemandPrefault(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(map[string]string{envMemoryRestoreMode: "ondemand-prefault"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.memRestoreMode != ch.MemRestoreOnDemand || !c.allowPrefault {
		t.Errorf("config = %+v, want OnDemand with the prefault allowed", c)
	}
}

func TestIsRestoringError(t *testing.T) {
	if !isRestoringError(errors.New(`PUT /api/v1/vm.snapshot: status 500: Error from API: ... VM on-demand memory restore is still in progress`)) {
		t.Error("missed cloud-hypervisor's restoring error")
	}
	if isRestoringError(errors.New("PUT /api/v1/vm.snapshot: status 500: VM is not running")) {
		t.Error("matched an unrelated error")
	}
}

// fakeSnapshotVMM answers vm.snapshot with cloud-hypervisor's "still restoring"
// error until refusals run out.
func fakeSnapshotVMM(t *testing.T, refusals int32) (*ch.Client, *atomic.Int32) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "ch.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/vm.snapshot" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if calls.Add(1) <= refusals {
			http.Error(w, "VM on-demand memory restore is still in progress", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})}
	go srv.Serve(lis)
	t.Cleanup(func() { _ = srv.Close() })
	return ch.NewClient(sock), &calls
}

func TestSnapshotWhilePrefaulting(t *testing.T) {
	client, calls := fakeSnapshotVMM(t, 2)
	if err := snapshotWhilePrefaulting(context.Background(), client, filepath.Join(t.TempDir(), "snap"), 5*time.Second); err != nil {
		t.Fatalf("snapshotWhilePrefaulting: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("vm.snapshot calls = %d, want 3", got)
	}

	client, _ = fakeSnapshotVMM(t, 1000)
	err := snapshotWhilePrefaulting(context.Background(), client, filepath.Join(t.TempDir(), "snap"), 120*time.Millisecond)
	if err == nil || !isRestoringError(err) {
		t.Errorf("snapshotWhilePrefaulting past its deadline = %v, want the restoring error", err)
	}
}

func TestAcquireGoldenSlotOverlay(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const page = 4096
	const totalSize = 8 * page

	baseFile := filepath.Join(dir, "base", "memory-ranges")
	if err := os.MkdirAll(filepath.Dir(baseFile), 0o700); err != nil {
		t.Fatal(err)
	}
	bf, err := os.Create(baseFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := bf.Truncate(totalSize); err != nil {
		t.Fatal(err)
	}
	// Populate pages 1 and 3 in base; leave pages 0, 2, 4..7 as holes.
	if _, err := bf.WriteAt(bytes.Repeat([]byte{0x11}, page), 1*page); err != nil {
		t.Fatal(err)
	}
	if _, err := bf.WriteAt(bytes.Repeat([]byte{0x33}, page), 3*page); err != nil {
		t.Fatal(err)
	}
	if err := bf.Close(); err != nil {
		t.Fatal(err)
	}

	// Run 12 sequential warm restores with different deltas to exercise slot
	// creation, overlay, eager commit, background reset, and slot reuse.
	for round := 0; round < 12; round++ {
		targetSnap := filepath.Join(dir, "target.bin")
		tf, err := os.Create(targetSnap)
		if err != nil {
			t.Fatal(err)
		}
		if err := tf.Truncate(totalSize); err != nil {
			t.Fatal(err)
		}
		// Keep page 1 == 0x11, zero out page 3 (was 0x33 in base!), write round-specific data to page 2 and page 5.
		if _, err := tf.WriteAt(bytes.Repeat([]byte{0x11}, page), 1*page); err != nil {
			t.Fatal(err)
		}
		if _, err := tf.WriteAt(bytes.Repeat([]byte{byte(0xA0 + round)}, page), 2*page); err != nil {
			t.Fatal(err)
		}
		if _, err := tf.WriteAt(bytes.Repeat([]byte{byte(0xB0 + round)}, page), 5*page); err != nil {
			t.Fatal(err)
		}
		if err := tf.Close(); err != nil {
			t.Fatal(err)
		}

		wantBytes, err := os.ReadFile(targetSnap)
		if err != nil {
			t.Fatal(err)
		}

		restoreMem := filepath.Join(dir, "restore-memory-ranges")
		if err := ch.CreateDeltaAgainstBase(ctx, baseFile, targetSnap, restoreMem); err != nil {
			t.Fatalf("round %d CreateDeltaAgainstBase: %v", round, err)
		}

		commit, abort, err := acquireGoldenSlotOverlay(ctx, baseFile, restoreMem, restoreMem)
		if err != nil {
			t.Fatalf("round %d acquireGoldenSlotOverlay: %v", round, err)
		}
		gotBytes, err := os.ReadFile(restoreMem)
		if err != nil {
			abort()
			t.Fatalf("round %d ReadFile(restoreMem): %v", round, err)
		}
		if !bytes.Equal(gotBytes, wantBytes) {
			abort()
			t.Fatalf("round %d reconstructed bytes mismatch with target", round)
		}
		commit(true)
		abort() // no-op after commit

		if _, err := os.Stat(restoreMem); !os.IsNotExist(err) {
			t.Fatalf("round %d restoreMem still exists after eager commit", round)
		}
		// Give background reset goroutine a moment to return the slot to the pool so the next round reuses it.
		time.Sleep(5 * time.Millisecond)
	}

	// Verify abort() rolls back deltaFile at outFile.
	restoreMem := filepath.Join(dir, "abort-memory-ranges")
	deltaContent := bytes.Repeat([]byte{0xEE}, totalSize)
	if err := os.WriteFile(restoreMem, deltaContent, 0o600); err != nil {
		t.Fatal(err)
	}
	_, abort, err := acquireGoldenSlotOverlay(ctx, baseFile, restoreMem, restoreMem)
	if err != nil {
		t.Fatalf("acquireGoldenSlotOverlay for abort: %v", err)
	}
	abort()
	rolledBack, err := os.ReadFile(restoreMem)
	if err != nil {
		t.Fatalf("ReadFile after abort: %v", err)
	}
	if !bytes.Equal(rolledBack, deltaContent) {
		t.Fatalf("abort did not restore original delta file")
	}
}
