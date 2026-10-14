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

	"golang.org/x/sys/unix"

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
		if commit(true, false) {
			t.Fatalf("round %d: an unpreserved eager commit kept the delta", round)
		}
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
	waitIdleSlots(t, goldenSlotPool(ctx, baseFile), 1)
}

func TestFastRestoreConfigTmpfsSlots(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(map[string]string{envTmpfsSlots: "true"}))
	if err != nil {
		t.Fatal(err)
	}
	if !c.tmpfsSlots || !c.enabled() {
		t.Errorf("config = %+v, want tmpfsSlots on and enabled", c)
	}
	for _, v := range []string{"", "false", "0"} {
		c, err := fastRestoreConfigFromEnv(envMap(map[string]string{envTmpfsSlots: v}))
		if err != nil {
			t.Fatal(err)
		}
		if c.tmpfsSlots {
			t.Errorf("%s=%q turned tmpfsSlots on", envTmpfsSlots, v)
		}
	}
	if _, err := fastRestoreConfigFromEnv(envMap(map[string]string{envTmpfsSlots: "ram"})); err == nil {
		t.Errorf("%s=ram accepted, want an error", envTmpfsSlots)
	}
}

// slotTestFixture is a golden base (pages 1 and 3 populated) under a fake
// <ateRoot>/golden-base-cache/<baseID>/, and helpers to build warm deltas.
type slotTestFixture struct {
	t        *testing.T
	dir      string
	baseFile string
	base     []byte
}

const slotTestPage = 4096
const slotTestSize = 8 * slotTestPage

func newSlotTestFixture(t *testing.T) *slotTestFixture {
	t.Helper()
	dir := t.TempDir()
	f := &slotTestFixture{t: t, dir: dir, baseFile: filepath.Join(dir, "golden-base-cache", "base1", "memory-ranges")}
	if err := os.MkdirAll(filepath.Dir(f.baseFile), 0o700); err != nil {
		t.Fatal(err)
	}
	bf, err := os.Create(f.baseFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := bf.Truncate(slotTestSize); err != nil {
		t.Fatal(err)
	}
	if _, err := bf.WriteAt(bytes.Repeat([]byte{0x11}, slotTestPage), 1*slotTestPage); err != nil {
		t.Fatal(err)
	}
	if _, err := bf.WriteAt(bytes.Repeat([]byte{0x33}, slotTestPage), 3*slotTestPage); err != nil {
		t.Fatal(err)
	}
	if err := bf.Close(); err != nil {
		t.Fatal(err)
	}
	if f.base, err = os.ReadFile(f.baseFile); err != nil {
		t.Fatal(err)
	}
	// Keep the background prewarm out of the way: the tests count slots.
	goldenPrewarmStarted.Store(f.baseFile, struct{}{})
	return f
}

// delta writes a warm delta of a guest that changed pages 2 and 5 (to b) and
// zeroed page 3, at path; it returns the full image the delta stands for and
// the delta's own bytes.
func (f *slotTestFixture) delta(path string, b byte) (full, delta []byte) {
	f.t.Helper()
	target := path + ".target"
	full = append([]byte(nil), f.base...)
	copy(full[2*slotTestPage:3*slotTestPage], bytes.Repeat([]byte{b}, slotTestPage))
	copy(full[3*slotTestPage:4*slotTestPage], make([]byte, slotTestPage))
	copy(full[5*slotTestPage:6*slotTestPage], bytes.Repeat([]byte{b + 1}, slotTestPage))
	if err := os.WriteFile(target, full, 0o600); err != nil {
		f.t.Fatal(err)
	}
	if err := ch.CreateDeltaAgainstBase(context.Background(), f.baseFile, target, path); err != nil {
		f.t.Fatal(err)
	}
	delta, err := os.ReadFile(path)
	if err != nil {
		f.t.Fatal(err)
	}
	return full, delta
}

// useTmpfsSlots points the slot pool at a temp "tmpfs" dir and makes hard
// links between it and the restore dir fail with EXDEV, as across a real tmpfs.
func useTmpfsSlots(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	prevRoot, prevLink := goldenTmpfsRoot, linkFile
	goldenTmpfsRoot = root
	linkFile = func(oldname, newname string) error {
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: unix.EXDEV}
	}
	t.Cleanup(func() { goldenTmpfsRoot, linkFile = prevRoot, prevLink })
	return root
}

// waitIdleSlots waits for the pool to hold n idle slots and returns them
// (drained), failing the test on timeout.
func waitIdleSlots(t *testing.T, pool *baseSlotPool, n int) []*goldenSlot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(pool.idle) < n {
		if time.Now().After(deadline) {
			t.Fatalf("pool has %d idle slots, want %d", len(pool.idle), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
	var out []*goldenSlot
	for len(pool.idle) > 0 {
		out = append(out, <-pool.idle)
	}
	return out
}

func isSymlinkUnder(t *testing.T, path, root string) bool {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		return false
	}
	target, err := os.Readlink(path)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(root, target)
	return err == nil && filepath.IsLocal(rel)
}

// A tmpfs pool mirrors the base, links slots by symlink, and on a preserved
// eager restore puts the delta back (keeping the marker) and recycles the slot.
func TestAcquireGoldenSlotOverlayTmpfsPreservedEager(t *testing.T) {
	ctx := context.Background()
	f := newSlotTestFixture(t)
	root := useTmpfsSlots(t)
	pool := getOrCreateBaseSlotPool(f.baseFile, root)

	for round := 0; round < 3; round++ {
		restoreMem := filepath.Join(f.dir, "local-snapshot", "memory-ranges")
		if err := os.MkdirAll(filepath.Dir(restoreMem), 0o700); err != nil {
			t.Fatal(err)
		}
		full, delta := f.delta(restoreMem, byte(0xA0+round))
		before, err := os.Stat(restoreMem)
		if err != nil {
			t.Fatal(err)
		}

		commit, abort, err := acquireGoldenSlotOverlay(ctx, f.baseFile, restoreMem, restoreMem)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if !isSymlinkUnder(t, restoreMem, root) {
			abort()
			t.Fatalf("round %d: restoreMem is not a symlink into the tmpfs pool", round)
		}
		got, err := os.ReadFile(restoreMem) // what cloud-hypervisor reads
		if err != nil || !bytes.Equal(got, full) {
			abort()
			t.Fatalf("round %d: reconstructed image mismatch (err %v)", round, err)
		}
		if !pool.staged.Load() || goldenBaseReadPath(f.baseFile) != pool.baseFile {
			t.Fatalf("round %d: base mirror not staged (read path %q)", round, goldenBaseReadPath(f.baseFile))
		}
		if mirror, err := os.ReadFile(pool.baseFile); err != nil || !bytes.Equal(mirror, f.base) {
			t.Fatalf("round %d: base mirror mismatch (err %v)", round, err)
		}

		if !commit(true, true) {
			t.Fatalf("round %d: preserved eager commit did not keep the delta", round)
		}
		abort() // no-op after commit
		after, err := os.Lstat(restoreMem)
		if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
			t.Fatalf("round %d: restoreMem is not the original delta again (err %v)", round, err)
		}
		if got, _ := os.ReadFile(restoreMem); !bytes.Equal(got, delta) {
			t.Fatalf("round %d: delta content changed", round)
		}
		if _, err := os.Lstat(restoreMem + ".delta.bak"); !os.IsNotExist(err) {
			t.Fatalf("round %d: delta backup left behind", round)
		}
		// The slot comes back reset to the base and is reused next round.
		for _, s := range waitIdleSlots(t, pool, 1) {
			if b, err := os.ReadFile(s.path); err != nil || !bytes.Equal(b, f.base) {
				t.Fatalf("round %d: recycled slot %s is not the base again (err %v)", round, s.path, err)
			}
			pool.idle <- s
		}
	}
}

func TestAcquireGoldenSlotOverlayTmpfsEagerAndAbort(t *testing.T) {
	ctx := context.Background()
	f := newSlotTestFixture(t)
	root := useTmpfsSlots(t)
	pool := getOrCreateBaseSlotPool(f.baseFile, root)

	restoreMem := filepath.Join(f.dir, "restore-state", "memory-ranges")
	if err := os.MkdirAll(filepath.Dir(restoreMem), 0o700); err != nil {
		t.Fatal(err)
	}
	f.delta(restoreMem, 0xC0)
	commit, _, err := acquireGoldenSlotOverlay(ctx, f.baseFile, restoreMem, restoreMem)
	if err != nil {
		t.Fatal(err)
	}
	if commit(true, false) {
		t.Fatal("unpreserved eager commit kept the delta")
	}
	if _, err := os.Lstat(restoreMem); !os.IsNotExist(err) {
		t.Fatalf("restoreMem still exists after an unpreserved eager commit (err %v)", err)
	}
	slots := waitIdleSlots(t, pool, 1)
	for _, s := range slots {
		pool.idle <- s
	}

	// abort puts the delta back and returns the slot.
	_, delta := f.delta(restoreMem, 0xD0)
	_, abort, err := acquireGoldenSlotOverlay(ctx, f.baseFile, restoreMem, restoreMem)
	if err != nil {
		t.Fatal(err)
	}
	abort()
	if fi, err := os.Lstat(restoreMem); err != nil || !fi.Mode().IsRegular() {
		t.Fatalf("restoreMem not a regular file after abort (err %v)", err)
	}
	if got, _ := os.ReadFile(restoreMem); !bytes.Equal(got, delta) {
		t.Fatal("abort did not restore the delta")
	}
	for _, s := range waitIdleSlots(t, pool, 1) {
		if b, _ := os.ReadFile(s.path); !bytes.Equal(b, f.base) {
			t.Fatalf("slot %s returned by abort is not the base", s.path)
		}
	}
}

// An OnDemand commit detaches the slot: a symlinked outFile is first replaced
// by a regular copy, so nothing dangles once the slot file is gone.
func TestAcquireGoldenSlotOverlayTmpfsOnDemandMaterializes(t *testing.T) {
	ctx := context.Background()
	f := newSlotTestFixture(t)
	root := useTmpfsSlots(t)

	restoreMem := filepath.Join(f.dir, "local-snapshot", "memory-ranges")
	if err := os.MkdirAll(filepath.Dir(restoreMem), 0o700); err != nil {
		t.Fatal(err)
	}
	full, _ := f.delta(restoreMem, 0xE0)
	commit, _, err := acquireGoldenSlotOverlay(ctx, f.baseFile, restoreMem, restoreMem)
	if err != nil {
		t.Fatal(err)
	}
	slotPath, err := os.Readlink(restoreMem)
	if err != nil {
		t.Fatal(err)
	}
	if commit(false, true) {
		t.Fatal("OnDemand commit kept the delta")
	}
	fi, err := os.Lstat(restoreMem)
	if err != nil || !fi.Mode().IsRegular() {
		t.Fatalf("restoreMem not materialized as a regular file (err %v)", err)
	}
	if got, _ := os.ReadFile(restoreMem); !bytes.Equal(got, full) {
		t.Fatal("materialized image mismatch")
	}
	if _, err := os.Stat(slotPath); !os.IsNotExist(err) {
		t.Fatalf("detached slot %s still exists (err %v)", slotPath, err)
	}
	if _, err := os.Lstat(restoreMem + ".delta.bak"); !os.IsNotExist(err) {
		t.Fatal("delta backup left behind")
	}
	_ = root
}

// A restore that died with outFile symlinked into a (now gone) slot gets its
// delta back on the next attempt.
func TestAcquireGoldenSlotOverlayRecoversInterruptedOverlay(t *testing.T) {
	ctx := context.Background()
	f := newSlotTestFixture(t)
	root := useTmpfsSlots(t)
	pool := getOrCreateBaseSlotPool(f.baseFile, root)

	restoreMem := filepath.Join(f.dir, "local-snapshot", "memory-ranges")
	if err := os.MkdirAll(filepath.Dir(restoreMem), 0o700); err != nil {
		t.Fatal(err)
	}
	full, _ := f.delta(restoreMem, 0x70)
	if err := os.Rename(restoreMem, restoreMem+".delta.bak"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(f.dir, "gone", "slot-1"), restoreMem); err != nil {
		t.Fatal(err)
	}
	commit, abort, err := acquireGoldenSlotOverlay(ctx, f.baseFile, restoreMem, restoreMem)
	if err != nil {
		t.Fatalf("acquire after an interrupted overlay: %v", err)
	}
	defer abort()
	if got, err := os.ReadFile(restoreMem); err != nil || !bytes.Equal(got, full) {
		t.Fatalf("reconstructed image mismatch (err %v)", err)
	}
	if !commit(true, true) {
		t.Fatal("preserved eager commit did not keep the delta")
	}
	waitIdleSlots(t, pool, 1)
}

// When the base cannot be mirrored onto the tmpfs, the disk pool serves.
func TestGoldenSlotPoolFallsBackToDisk(t *testing.T) {
	f := newSlotTestFixture(t)
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	prev := goldenTmpfsRoot
	goldenTmpfsRoot = filepath.Join(notADir, "root")
	t.Cleanup(func() { goldenTmpfsRoot = prev })

	pool := goldenSlotPool(context.Background(), f.baseFile)
	if pool.inTmpfs {
		t.Fatal("got the tmpfs pool although its mirror cannot be staged")
	}
	if pool.baseFile != f.baseFile || filepath.Dir(pool.slotDir) != filepath.Dir(f.baseFile) {
		t.Errorf("disk pool = base %q slots %q, want them next to %q", pool.baseFile, pool.slotDir, f.baseFile)
	}
	if got := goldenBaseReadPath(f.baseFile); got != f.baseFile {
		t.Errorf("goldenBaseReadPath = %q, want the disk base", got)
	}
}

func TestIsTmpfsMountPoint(t *testing.T) {
	if isTmpfsMountPoint(t.TempDir()) {
		t.Error("a fresh temp dir reported as a tmpfs mount point")
	}
	if _, ok := tmpfsFreeBytes("/dev/shm"); ok && !isTmpfsMountPoint("/dev/shm") {
		t.Error("/dev/shm is a tmpfs but not reported as a mount point")
	}
}

func TestFastRestoreConfigCPUWeightsAndCheckpointGate(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(map[string]string{
		envActivationCPUWeight:   "10000",
		envSteadyCPUWeight:       "10",
		envSelfCPUWeight:         "10000",
		envCheckpointConcurrency: "2",
		envCheckpointNice:        "19",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := fastRestoreConfig{activationCPUWeight: 10000, steadyCPUWeight: 10, selfCPUWeight: 10000, checkpointConcurrency: 2, checkpointNice: 19}
	if c != want {
		t.Errorf("config = %+v, want %+v", c, want)
	}
	if !c.enabled() {
		t.Error("config with knobs set reports disabled")
	}
	if c.activationWeight() != 10000 || c.steadyWeight() != 10 {
		t.Errorf("weights = %d/%d, want 10000/10", c.activationWeight(), c.steadyWeight())
	}
	for _, env := range []map[string]string{
		{envActivationCPUWeight: "10001"},
		{envSteadyCPUWeight: "-5"},
		{envSelfCPUWeight: "lots"},
		{envCheckpointConcurrency: "-1"},
		{envCheckpointNice: "20"},
	} {
		if _, err := fastRestoreConfigFromEnv(envMap(env)); err == nil {
			t.Errorf("fastRestoreConfigFromEnv(%v) succeeded, want an error", env)
		}
	}
}

// Setting only one of the two leaf weights puts the other back to the kernel
// default; setting neither leaves cpu.weight alone.
func TestActivationAndSteadyWeights(t *testing.T) {
	for _, tc := range []struct {
		c                  fastRestoreConfig
		activation, steady int
	}{
		{fastRestoreConfig{}, 0, 0},
		{fastRestoreConfig{activationCPUWeight: 5000}, 5000, 100},
		{fastRestoreConfig{steadyCPUWeight: 5}, 100, 5},
		{fastRestoreConfig{activationCPUWeight: 10000, steadyCPUWeight: 10}, 10000, 10},
	} {
		if a, s := tc.c.activationWeight(), tc.c.steadyWeight(); a != tc.activation || s != tc.steady {
			t.Errorf("%+v: weights = %d/%d, want %d/%d", tc.c, a, s, tc.activation, tc.steady)
		}
	}
}

// The gate never runs more than its cap at once and reports queueing.
func TestCheckpointGateCapsConcurrency(t *testing.T) {
	if newCheckpointGate(0, 0) != nil {
		t.Fatal("a gate with nothing set is not nil")
	}
	var nilGate *checkpointGate
	if _, err := nilGate.run(context.Background(), func() error { return errors.New("x") }); err == nil {
		t.Error("nil gate did not run fn")
	}
	g := newCheckpointGate(2, 0)
	var running, peak atomic.Int32
	var waited atomic.Int64
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			w, err := g.run(context.Background(), func() error {
				n := running.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				running.Add(-1)
				return nil
			})
			waited.Add(int64(w))
			done <- err
		}()
	}
	for i := 0; i < 8; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if p := peak.Load(); p != 2 {
		t.Errorf("peak concurrency = %d, want 2", p)
	}
	if waited.Load() == 0 {
		t.Error("no run reported waiting for a slot")
	}
	// A cancelled context gives up waiting for a slot.
	g1 := newCheckpointGate(1, 0)
	g1.sem <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g1.run(ctx, func() error { return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("run with a full gate and a cancelled context = %v, want context.Canceled", err)
	}
}

// With a niceness set, fn runs on a thread of that niceness, and the caller's
// thread keeps its own.
func TestCheckpointGateNice(t *testing.T) {
	g := newCheckpointGate(0, 19)
	var got int
	if _, err := g.run(context.Background(), func() error {
		p, err := unix.Getpriority(unix.PRIO_PROCESS, unix.Gettid())
		got = p
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// The raw getpriority syscall returns 20 - nice.
	if got != 20-19 {
		t.Errorf("checkpoint thread priority = %d (raw), want %d", got, 20-19)
	}
}

func TestFastRestoreConfigParallelPrep(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(map[string]string{envParallelPrep: "true"}))
	if err != nil {
		t.Fatal(err)
	}
	if !c.parallelPrep || !c.enabled() {
		t.Errorf("config = %+v, want parallelPrep on and enabled", c)
	}
	if _, err := fastRestoreConfigFromEnv(envMap(map[string]string{envParallelPrep: "often"})); err == nil {
		t.Error("parallel prep accepted an invalid value")
	}
}

// runAsync's join waits for fn once and keeps returning its result; noJoin
// never fails.
func TestRunAsyncJoin(t *testing.T) {
	if err := noJoin(); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var runs atomic.Int32
	join := runAsync(func() error {
		runs.Add(1)
		<-release
		return errors.New("boom")
	})
	joined := make(chan error, 1)
	go func() { joined <- join() }()
	select {
	case <-joined:
		t.Fatal("join returned before fn finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-joined; err == nil || err.Error() != "boom" {
		t.Fatalf("join = %v, want boom", err)
	}
	if err := join(); err == nil || err.Error() != "boom" {
		t.Errorf("second join = %v, want boom again", err)
	}
	if n := runs.Load(); n != 1 {
		t.Errorf("fn ran %d times, want 1", n)
	}
}

func TestFastRestoreConfigFastSandboxCleanup(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(map[string]string{envFastSandboxCleanup: "true"}))
	if err != nil {
		t.Fatal(err)
	}
	if !c.fastSandboxCleanup || !c.enabled() {
		t.Errorf("config = %+v, want fastSandboxCleanup on and enabled", c)
	}
	if c, err := fastRestoreConfigFromEnv(envMap(nil)); err != nil || c.fastSandboxCleanup {
		t.Errorf("default config = %+v, %v; want fastSandboxCleanup off", c, err)
	}
	if _, err := fastRestoreConfigFromEnv(envMap(map[string]string{envFastSandboxCleanup: "sometimes"})); err == nil {
		t.Error("fast sandbox cleanup accepted an invalid value")
	}
}
