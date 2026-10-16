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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
)

func TestParseMemTotal(t *testing.T) {
	got, err := parseMemTotal("MemTotal:       61706744 kB\nMemFree:        58432100 kB\n")
	if err != nil || got != 61706744<<10 {
		t.Errorf("parseMemTotal = %d, %v; want %d", got, err, int64(61706744)<<10)
	}
	for _, bad := range []string{"", "MemFree: 1 kB\n", "MemTotal: x kB\n", "MemTotal: 0 kB\n", "MemTotal: 5 MB\n"} {
		if _, err := parseMemTotal(bad); err == nil {
			t.Errorf("parseMemTotal(%q) succeeded, want an error", bad)
		}
	}
	if n, err := memTotalBytes(); err != nil || n <= 0 {
		t.Errorf("memTotalBytes() = %d, %v", n, err)
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

type slotTestFixture struct {
	t        *testing.T
	dir      string
	baseFile string
	base     []byte
}

const (
	slotTestPage = 4096
	slotTestSize = 8 * slotTestPage
)

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
	goldenPrewarmStarted.Store(f.baseFile, struct{}{})
	return f
}

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
	if _, err := ch.CreateSparseDeltaAgainstBase(context.Background(), f.baseFile, target, path); err != nil {
		f.t.Fatal(err)
	}
	delta, err := os.ReadFile(path)
	if err != nil {
		f.t.Fatal(err)
	}
	return full, delta
}

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

func TestAcquireGoldenSlotOverlay(t *testing.T) {
	ctx := context.Background()
	f := newSlotTestFixture(t)

	for round := 0; round < 12; round++ {
		restoreMem := filepath.Join(f.dir, "restore-memory-ranges")
		full, _ := f.delta(restoreMem, byte(0xA0+round))

		commit, abort, err := acquireGoldenSlotOverlay(ctx, f.baseFile, restoreMem, restoreMem)
		if err != nil {
			t.Fatalf("round %d acquireGoldenSlotOverlay: %v", round, err)
		}
		gotBytes, err := os.ReadFile(restoreMem)
		if err != nil {
			abort()
			t.Fatalf("round %d ReadFile(restoreMem): %v", round, err)
		}
		if !bytes.Equal(gotBytes, full) {
			abort()
			t.Fatalf("round %d reconstructed bytes mismatch with target", round)
		}
		if commit(true, false) {
			t.Fatalf("round %d: an unpreserved eager commit kept the delta", round)
		}
		abort()

		if _, err := os.Stat(restoreMem); !os.IsNotExist(err) {
			t.Fatalf("round %d restoreMem still exists after eager commit", round)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Verify abort() rolls back deltaFile at outFile.
	restoreMem := filepath.Join(f.dir, "abort-memory-ranges")
	deltaContent := bytes.Repeat([]byte{0xEE}, slotTestSize)
	if err := os.WriteFile(restoreMem, deltaContent, 0o600); err != nil {
		t.Fatal(err)
	}
	_, abort, err := acquireGoldenSlotOverlay(ctx, f.baseFile, restoreMem, restoreMem)
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
	waitIdleSlots(t, goldenSlotPool(ctx, f.baseFile), 1)
}

func TestSlotPoolConcurrentAcquireAndReleaseUnderContention(t *testing.T) {
	ctx := context.Background()
	f := newSlotTestFixture(t)
	root := useTmpfsSlots(t)
	pool := getOrCreateBaseSlotPool(f.baseFile, root)
	if !pool.ready(ctx) {
		t.Fatal("tmpfs pool not ready")
	}
	// Prewarm only 2 slots while running 10 concurrent restores to exercise both
	// idle slot reuse and on-demand slot allocation when the pool is exhausted.
	pool.replenishOneSlot()
	pool.replenishOneSlot()

	const workers = 10
	const iterations = 4
	var wg sync.WaitGroup
	errCh := make(chan error, workers*iterations)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for iter := 0; iter < iterations; iter++ {
				restoreMem := filepath.Join(f.dir, fmt.Sprintf("worker-%d-iter-%d", workerID, iter), "memory-ranges")
				if err := os.MkdirAll(filepath.Dir(restoreMem), 0o700); err != nil {
					errCh <- err
					return
				}
				b := byte(0x20 + workerID*iterations + iter)
				target := restoreMem + ".target"
				full := append([]byte(nil), f.base...)
				copy(full[2*slotTestPage:3*slotTestPage], bytes.Repeat([]byte{b}, slotTestPage))
				copy(full[3*slotTestPage:4*slotTestPage], make([]byte, slotTestPage))
				copy(full[5*slotTestPage:6*slotTestPage], bytes.Repeat([]byte{b + 1}, slotTestPage))
				if err := os.WriteFile(target, full, 0o600); err != nil {
					errCh <- err
					return
				}
				if _, err := ch.CreateSparseDeltaAgainstBase(ctx, f.baseFile, target, restoreMem); err != nil {
					errCh <- err
					return
				}

				commit, abort, err := acquireGoldenSlotOverlay(ctx, f.baseFile, restoreMem, restoreMem)
				if err != nil {
					errCh <- err
					return
				}
				got, err := os.ReadFile(restoreMem)
				if err != nil {
					abort()
					errCh <- err
					return
				}
				if !bytes.Equal(got, full) {
					abort()
					errCh <- fmt.Errorf("worker %d iter %d reconstructed bytes mismatch", workerID, iter)
					return
				}
				commit(true, false)
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Verify idle slots in the pool are surgically reset back to f.base.
	time.Sleep(25 * time.Millisecond)
	slots := waitIdleSlots(t, pool, 1)
	for _, s := range slots {
		got, err := os.ReadFile(s.path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, f.base) {
			t.Fatalf("idle slot %s does not match golden base after reset", s.path)
		}
		pool.idle <- s
	}
}

func TestAcquireGoldenSlotCompactAndReset(t *testing.T) {
	ctx := context.Background()
	f := newSlotTestFixture(t)
	root := useTmpfsSlots(t)
	pool := getOrCreateBaseSlotPool(f.baseFile, root)
	if !pool.ready(ctx) {
		t.Fatal("tmpfs pool not ready")
	}
	pool.replenishOneSlot()

	target := filepath.Join(f.dir, "compact-target.bin")
	full := append([]byte(nil), f.base...)
	copy(full[2*slotTestPage:3*slotTestPage], bytes.Repeat([]byte{0xCC}, slotTestPage))
	copy(full[3*slotTestPage:4*slotTestPage], make([]byte, slotTestPage))
	if err := os.WriteFile(target, full, 0o600); err != nil {
		t.Fatal(err)
	}

	metaFile := filepath.Join(f.dir, memoryRangesDiffMetaFile)
	dataFile := filepath.Join(f.dir, memoryRangesDiffDataFile)
	if _, err := ch.CreateDeltaAgainstBase(ctx, f.baseFile, target, metaFile, dataFile); err != nil {
		t.Fatal(err)
	}

	outFile := filepath.Join(f.dir, "compact-restore", "memory-ranges")
	if err := os.MkdirAll(filepath.Dir(outFile), 0o700); err != nil {
		t.Fatal(err)
	}
	lease, err := acquireGoldenSlotCompact(ctx, f.baseFile, metaFile, dataFile, outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !lease.fromIdle {
		t.Fatal("expected prewarmed idle slot")
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		lease.abort()
		t.Fatal(err)
	}
	if !bytes.Equal(got, full) {
		lease.abort()
		t.Fatal("reconstructed compact delta slot does not match target")
	}
	lease.commit(true, false)

	slots := waitIdleSlots(t, pool, 1)
	resetBytes, err := os.ReadFile(slots[0].path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(resetBytes, f.base) {
		t.Fatal("slot was not surgically reset to base after compact delta overlay")
	}
}

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
		got, err := os.ReadFile(restoreMem)
		if err != nil || !bytes.Equal(got, full) {
			abort()
			t.Fatalf("round %d: reconstructed image mismatch (err %v)", round, err)
		}
		if !pool.staged.Load() || goldenBaseReadPath(f.baseFile) != pool.baseFile {
			t.Fatalf("round %d: base mirror not staged (read path %q)", round, goldenBaseReadPath(f.baseFile))
		}
		if !commit(true, true) {
			t.Fatalf("round %d: preserved eager commit did not keep the delta", round)
		}
		abort()

		after, err := os.Lstat(restoreMem)
		if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
			t.Fatalf("round %d: restoreMem is not the original delta file again (err %v)", round, err)
		}
		if gotDelta, _ := os.ReadFile(restoreMem); !bytes.Equal(gotDelta, delta) {
			t.Fatalf("round %d: delta content changed", round)
		}
		slots := waitIdleSlots(t, pool, 1)
		pool.idle <- slots[0]
	}
}

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

func claimedSlotFixture(t *testing.T, restoreSub string) (f *slotTestFixture, pool *baseSlotPool, lease *goldenSlotLease, restoreMem string, full, delta []byte) {
	t.Helper()
	ctx := context.Background()
	f = newSlotTestFixture(t)
	root := useTmpfsSlots(t)
	pool = getOrCreateBaseSlotPool(f.baseFile, root)
	if !pool.ready(ctx) {
		t.Fatal("tmpfs pool not ready")
	}
	pool.replenishOneSlot()
	restoreMem = filepath.Join(f.dir, restoreSub, "memory-ranges")
	if err := os.MkdirAll(filepath.Dir(restoreMem), 0o700); err != nil {
		t.Fatal(err)
	}
	full, delta = f.delta(restoreMem, 0x5A)
	var err error
	if lease, err = acquireGoldenSlot(ctx, f.baseFile, restoreMem, restoreMem); err != nil {
		t.Fatal(err)
	}
	if lease.slot == nil || !lease.fromIdle || !lease.pool.inTmpfs {
		t.Fatalf("lease = %+v, want a pre-staged tmpfs slot", lease)
	}
	return f, pool, lease, restoreMem, full, delta
}

func assertSlotKept(t *testing.T, pool *baseSlotPool, slot string, full []byte) {
	t.Helper()
	if got, err := os.ReadFile(slot); err != nil || !bytes.Equal(got, full) {
		t.Errorf("claimed slot %s was reset or removed (err %v)", slot, err)
	}
	time.Sleep(20 * time.Millisecond)
	for n := len(pool.idle); n > 0; n-- {
		s := <-pool.idle
		if s.path == slot {
			t.Errorf("claimed slot %s went back to the pool", slot)
		}
		pool.idle <- s
	}
}

func TestGoldenSlotLeaseClaimPreservedCommit(t *testing.T) {
	_, pool, lease, restoreMem, full, delta := claimedSlotFixture(t, "local-snapshot")
	before, err := os.Stat(restoreMem + ".delta.bak")
	if err != nil {
		t.Fatal(err)
	}
	slot := lease.claimForVM()
	if slot == "" || slot != lease.slot.path {
		t.Fatalf("claimForVM = %q, want %q", slot, lease.slot.path)
	}
	if again := lease.claimForVM(); again != "" {
		t.Errorf("second claimForVM = %q, want none", again)
	}
	for _, s := range waitIdleSlots(t, pool, 1) {
		if s.path == slot {
			t.Fatal("the replacement is the claimed slot")
		}
		pool.idle <- s
	}

	if !lease.commit(true, true) {
		t.Error("preserved commit of a claimed slot did not keep the delta")
	}
	lease.abort()
	after, err := os.Lstat(restoreMem)
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		t.Fatalf("restoreMem is not the original delta again (err %v)", err)
	}
	if got, _ := os.ReadFile(restoreMem); !bytes.Equal(got, delta) {
		t.Error("delta content changed")
	}
	assertSlotKept(t, pool, slot, full)
}

func TestGoldenSlotLeaseClaimUnpreservedCommit(t *testing.T) {
	_, pool, lease, restoreMem, full, _ := claimedSlotFixture(t, "restore-state")
	slot := lease.claimForVM()
	if lease.commit(true, false) {
		t.Error("unpreserved commit kept the delta")
	}
	if _, err := os.Lstat(restoreMem); !os.IsNotExist(err) {
		t.Errorf("restoreMem still exists after an unpreserved commit (err %v)", err)
	}
	if _, err := os.Lstat(restoreMem + ".delta.bak"); !os.IsNotExist(err) {
		t.Error("delta backup left behind")
	}
	assertSlotKept(t, pool, slot, full)
}

func TestGoldenSlotLeaseClaimAbort(t *testing.T) {
	_, pool, lease, restoreMem, full, delta := claimedSlotFixture(t, "local-snapshot")
	slot := lease.claimForVM()
	lease.abort()
	if fi, err := os.Lstat(restoreMem); err != nil || !fi.Mode().IsRegular() {
		t.Fatalf("restoreMem not the delta after abort (err %v)", err)
	}
	if got, _ := os.ReadFile(restoreMem); !bytes.Equal(got, delta) {
		t.Error("abort did not put the delta back")
	}
	assertSlotKept(t, pool, slot, full)
}

func TestGoldenSlotLeaseOrigin(t *testing.T) {
	ctx := context.Background()
	f := newSlotTestFixture(t)
	useTmpfsSlots(t)
	restoreMem := filepath.Join(f.dir, "restore-state", "memory-ranges")
	if err := os.MkdirAll(filepath.Dir(restoreMem), 0o700); err != nil {
		t.Fatal(err)
	}
	f.delta(restoreMem, 0x21)
	lease, err := acquireGoldenSlot(ctx, f.baseFile, restoreMem, restoreMem)
	if err != nil {
		t.Fatal(err)
	}
	if lease.slot == nil || lease.fromIdle {
		t.Errorf("lease from an empty pool = %+v, want a slot not from the pool", lease)
	}
	lease.abort()

	t.Setenv("ATEWORKER_MICROVM_DISABLE_GOLDEN_SLOT_POOL", "true")
	lease, err = acquireGoldenSlot(ctx, f.baseFile, restoreMem, restoreMem)
	if err != nil {
		t.Fatal(err)
	}
	if lease.slot != nil || lease.claimForVM() != "" {
		t.Errorf("merged-on-disk lease = %+v, want nothing to claim", lease)
	}
}

const (
	chFixtures      = "internal/ch/testdata/filebacked"
	fixtureGuestRAM = 402653184
)

func zeroCopyRestoreDir(t *testing.T) (restoreDir, slot string, cfg, state []byte) {
	t.Helper()
	restoreDir = t.TempDir()
	for _, f := range []string{"config.json", "state.json"} {
		b, err := os.ReadFile(filepath.Join(chFixtures, "memfd-"+f))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(restoreDir, f), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	slot = filepath.Join(t.TempDir(), "slot-7")
	sf, err := os.Create(slot)
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.Truncate(fixtureGuestRAM); err != nil {
		t.Fatal(err)
	}
	sf.Close()
	cfg, state = readSnapshot(t, restoreDir)
	return restoreDir, slot, cfg, state
}

func readSnapshot(t *testing.T, dir string) (cfg, state []byte) {
	t.Helper()
	var err error
	if cfg, err = os.ReadFile(filepath.Join(dir, "config.json")); err != nil {
		t.Fatal(err)
	}
	if state, err = os.ReadFile(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	}
	return cfg, state
}

func testLease(t *testing.T, slot string, fromIdle bool) (*goldenSlotLease, *baseSlotPool) {
	t.Helper()
	dir := t.TempDir()
	base := filepath.Join(dir, "base-memory-ranges")
	if err := os.WriteFile(base, bytes.Repeat([]byte{1}, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	pool := &baseSlotPool{srcFile: base, baseFile: base, slotDir: filepath.Join(dir, "slots"), inTmpfs: true, idle: make(chan *goldenSlot, 1)}
	return &goldenSlotLease{
		commit:   func(bool, bool) bool { return false },
		abort:    func() {},
		slot:     &goldenSlot{path: slot},
		pool:     pool,
		fromIdle: fromIdle,
		claimed:  new(atomic.Bool),
	}, pool
}

func memorySection(t *testing.T, cfg []byte) json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(cfg, &m); err != nil {
		t.Fatal(err)
	}
	return m["memory"]
}

func TestStartZeroCopyRestoreFallbacks(t *testing.T) {
	ctx := context.Background()
	restoreDir, slot, cfg, state := zeroCopyRestoreDir(t)
	unchanged := func() {
		t.Helper()
		c, s := readSnapshot(t, restoreDir)
		if !bytes.Equal(c, cfg) || !bytes.Equal(s, state) {
			t.Fatal("a fallback modified the restore directory")
		}
	}
	idle, _ := testLease(t, slot, true)
	staged, _ := testLease(t, slot, false)
	merged := &goldenSlotLease{commit: func(bool, bool) bool { return false }, abort: func() {}}
	disk, _ := testLease(t, slot, true)
	disk.pool.inTmpfs = false

	for _, tc := range []struct {
		name  string
		lease *goldenSlotLease
		want  string
	}{
		{"not a golden-delta restore", nil, zeroCopyNoGoldenDelta},
		{"merged on the node disk", merged, zeroCopyMergedOnDisk},
		{"disk pool", disk, zeroCopyDiskSlot},
		{"slot staged for this restore", staged, zeroCopyNoIdleSlot},
	} {
		zc, why, err := startZeroCopyRestore(ctx, "actor-1", restoreDir, tc.lease)
		if zc != nil || err != nil || why != tc.want {
			t.Errorf("%s: startZeroCopyRestore = %v, %q, %v; want fallback %q", tc.name, zc, why, err, tc.want)
		}
		unchanged()
	}
	if idle.claimed.Load() {
		t.Fatal("a fallback claimed the slot")
	}

	zeroCopyBackoff.Store("actor-2", struct{}{})
	if zc, why, err := startZeroCopyRestore(ctx, "actor-2", restoreDir, idle); zc != nil || err != nil || why != zeroCopyPreviousFailure {
		t.Errorf("after a failure: startZeroCopyRestore = %v, %q, %v; want %q", zc, why, err, zeroCopyPreviousFailure)
	}
	unchanged()
	if _, still := zeroCopyBackoff.Load("actor-2"); still {
		t.Error("the backoff outlived the attempt it skipped")
	}

	hotplug := bytes.Replace(cfg, []byte(`"hotplug_method":"Acpi",`), []byte(`"hotplug_method":"Acpi","hotplug_size":1073741824,`), 1)
	if err := os.WriteFile(filepath.Join(restoreDir, "config.json"), hotplug, 0o600); err != nil {
		t.Fatal(err)
	}
	if zc, why, err := startZeroCopyRestore(ctx, "actor-1", restoreDir, idle); zc != nil || err != nil || why != zeroCopyLayout {
		t.Errorf("hotplug snapshot: startZeroCopyRestore = %v, %q, %v; want %q", zc, why, err, zeroCopyLayout)
	}
	if c, _ := readSnapshot(t, restoreDir); !bytes.Equal(c, hotplug) {
		t.Error("a rejected snapshot was modified")
	}
	if idle.claimed.Load() {
		t.Error("a rejected snapshot claimed the slot")
	}
}

func TestStartZeroCopyRestoreAndAbandon(t *testing.T) {
	ctx := context.Background()
	restoreDir, slot, cfg, state := zeroCopyRestoreDir(t)
	lease, pool := testLease(t, slot, true)

	zc, why, err := startZeroCopyRestore(ctx, "actor-3", restoreDir, lease)
	if err != nil || zc == nil || why != "" {
		t.Fatalf("startZeroCopyRestore = %v, %q, %v; want zero-copy", zc, why, err)
	}
	if zc.slot != slot || zc.slotBase != pool.baseFile || zc.slotIno == 0 {
		t.Errorf("zeroCopyRestore = %+v", zc)
	}
	if !bytes.Equal(zc.origMemory, memorySection(t, cfg)) {
		t.Errorf("origMemory = %s, want the snapshot's %s", zc.origMemory, memorySection(t, cfg))
	}
	if !lease.claimed.Load() || lease.claimForVM() != "" {
		t.Error("the slot was not claimed for the VM")
	}
	waitIdleSlots(t, pool, 1)

	newCfg, newState := readSnapshot(t, restoreDir)
	if mem := string(memorySection(t, newCfg)); !strings.Contains(mem, `"size":0,`) || !strings.Contains(mem, `"file":"`+slot+`"`) {
		t.Errorf("rewritten memory section %s does not map the slot", mem)
	}
	if bytes.Equal(newState, state) {
		t.Error("state.json not rewritten")
	}

	zc.abandon(ctx, "actor-3")
	gotCfg, gotState := readSnapshot(t, restoreDir)
	if !bytes.Equal(gotCfg, cfg) || !bytes.Equal(gotState, state) {
		t.Error("abandon did not put the restore directory back byte for byte")
	}
	if _, err := os.Stat(slot); !os.IsNotExist(err) {
		t.Errorf("abandon left the slot (err %v)", err)
	}
	if _, ok := zeroCopyBackoff.LoadAndDelete("actor-3"); !ok {
		t.Error("abandon did not make the next attempt copy guest RAM")
	}
}

func TestDiscardMemSlot(t *testing.T) {
	s := &AteWorkerService{}
	slot := filepath.Join(t.TempDir(), "slot-1")
	if err := os.WriteFile(slot, []byte("ram"), 0o600); err != nil {
		t.Fatal(err)
	}
	ra := &runningActor{memSlot: slot}
	s.discardMemSlot(context.Background(), "actor", ra)
	if _, err := os.Stat(slot); !os.IsNotExist(err) {
		t.Errorf("slot still there (err %v)", err)
	}
	if ra.memSlot != "" {
		t.Error("memSlot not cleared")
	}
	s.discardMemSlot(context.Background(), "actor", ra)
	s.discardMemSlot(context.Background(), "actor", &runningActor{memSlot: slot})
}

func TestCheckSlotSnapshot(t *testing.T) {
	dir := t.TempDir()
	slot := filepath.Join(t.TempDir(), "slot-1")
	if err := os.WriteFile(slot, []byte("ram"), 0o600); err != nil {
		t.Fatal(err)
	}
	ra := slotActor(t, slot)
	if err := checkSlotSnapshot(dir, ra); err != nil {
		t.Errorf("checkSlotSnapshot = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory-ranges"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkSlotSnapshot(dir, ra); err == nil {
		t.Error("accepted a snapshot with memory-ranges")
	}
	os.Remove(filepath.Join(dir, "memory-ranges"))
	os.Remove(slot)
	if err := os.WriteFile(slot, []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkSlotSnapshot(dir, ra); err == nil {
		t.Error("accepted a replaced slot")
	}
	os.Remove(slot)
	if err := checkSlotSnapshot(dir, ra); err == nil {
		t.Error("accepted a missing slot")
	}
}

func slotActor(t *testing.T, slot string) *runningActor {
	t.Helper()
	fi, err := os.Stat(slot)
	if err != nil {
		t.Fatal(err)
	}
	ra := &runningActor{memSlot: slot}
	if st, ok := statDevIno(fi); ok {
		ra.memSlotDev, ra.memSlotIno = st[0], st[1]
	}
	return ra
}

func statDevIno(fi os.FileInfo) ([2]uint64, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return [2]uint64{}, false
	}
	return [2]uint64{uint64(st.Dev), st.Ino}, true
}

func fakeFileBackedVMM(t *testing.T, withMemory bool) *ch.Client {
	t.Helper()
	dir, err := os.MkdirTemp("", "zcvmm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "api.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/vm.snapshot" {
			http.NotFound(w, r)
			return
		}
		var body struct {
			DestinationURL string `json:"destination_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		dst := strings.TrimPrefix(body.DestinationURL, "file://")
		for _, f := range []string{"config.json", "state.json"} {
			b, err := os.ReadFile(filepath.Join(chFixtures, "filebacked-"+f))
			if err == nil {
				err = os.WriteFile(filepath.Join(dst, f), b, 0o644)
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		if withMemory {
			_ = os.WriteFile(filepath.Join(dst, "memory-ranges"), []byte("ram"), 0o644)
		}
		w.WriteHeader(http.StatusNoContent)
	})}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return ch.NewClient(sock)
}

func zeroCopyCheckpointFixture(t *testing.T, withBase bool) (checkpointDir string, ra *runningActor, base string, slotImage []byte) {
	t.Helper()
	const page = 4096
	ateRoot := t.TempDir()
	checkpointDir = filepath.Join(ateRoot, "actors", "actor-9", "checkpoint-state")
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		t.Fatal(err)
	}
	baseImage := make([]byte, 64*page)
	for i := 0; i < 32; i++ {
		copy(baseImage[i*page:], bytes.Repeat([]byte{byte(i + 1)}, page))
	}
	slotImage = append([]byte(nil), baseImage...)
	copy(slotImage[3*page:], bytes.Repeat([]byte{0xEE}, page))  // changed
	copy(slotImage[5*page:], make([]byte, page))                // zeroed
	copy(slotImage[40*page:], bytes.Repeat([]byte{0x77}, page)) // was a hole
	base = filepath.Join(ateRoot, "golden-base-cache", "golden-1", "memory-ranges")
	if withBase {
		if err := os.MkdirAll(filepath.Dir(base), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(base, baseImage, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	slot := filepath.Join(t.TempDir(), "slot-3")
	if err := os.WriteFile(slot, slotImage, 0o600); err != nil {
		t.Fatal(err)
	}
	memfdCfg, err := os.ReadFile(filepath.Join(chFixtures, "memfd-config.json"))
	if err != nil {
		t.Fatal(err)
	}
	ra = slotActor(t, slot)
	ra.baseID = "golden-1"
	ra.snapshotIsSelfContained = true
	if withBase {
		ra.memSlotBase = base
	}
	ra.origMemory = memorySection(t, memfdCfg)
	return checkpointDir, ra, base, slotImage
}

func assertCanonical(t *testing.T, checkpointDir string) {
	t.Helper()
	memfdCfg, err := os.ReadFile(filepath.Join(chFixtures, "memfd-config.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, state := readSnapshot(t, checkpointDir)
	if !bytes.Equal(memorySection(t, cfg), memorySection(t, memfdCfg)) {
		t.Errorf("checkpoint memory section %s, want the memfd one", memorySection(t, cfg))
	}
	if !strings.Contains(string(state), `\"memory_ranges\":{\"data\":[{\"gpa\":0,\"length\":402653184}]}`) {
		t.Error("checkpoint state.json lacks the memfd memory_ranges table")
	}
	entries, _ := os.ReadDir(checkpointDir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			t.Errorf("temp file %s left in the checkpoint", e.Name())
		}
	}
}

func TestSnapshotVMStateFromSlotDeflates(t *testing.T) {
	ctx := context.Background()
	checkpointDir, ra, base, slotImage := zeroCopyCheckpointFixture(t, true)
	client := fakeFileBackedVMM(t, false)
	if _, err := (&AteWorkerService{}).snapshotVMState(ctx, client, ra, "actor-9", checkpointDir); err != nil {
		t.Fatal(err)
	}
	merged := filepath.Join(t.TempDir(), "merged")
	if err := ch.MergeSparseOverlay(ctx, base, filepath.Join(checkpointDir, "memory-ranges"), merged); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(merged); !bytes.Equal(got, slotImage) {
		t.Error("golden base + checkpoint delta is not the guest's RAM")
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, goldenDeltaMarkerFile)); err != nil {
		t.Errorf("golden-delta marker missing: %v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(checkpointDir, baseIDFile)); string(b) != "golden-1" {
		t.Errorf("base-id = %q", b)
	}
	assertCanonical(t, checkpointDir)
	if _, err := os.Stat(ra.memSlot); err != nil {
		t.Errorf("the checkpoint removed the slot the VM still maps: %v", err)
	}
}

func TestSnapshotVMStateFromSlotCompactDelta(t *testing.T) {
	t.Setenv("ATEWORKER_MICROVM_DELTA_FORMAT", "compact")
	ctx := context.Background()
	checkpointDir, ra, base, slotImage := zeroCopyCheckpointFixture(t, true)
	client := fakeFileBackedVMM(t, false)
	if _, err := (&AteWorkerService{}).snapshotVMState(ctx, client, ra, "actor-9", checkpointDir); err != nil {
		t.Fatal(err)
	}
	reconstructed := filepath.Join(t.TempDir(), "reconstructed")
	if err := ch.RestoreFromDelta(
		ctx,
		base,
		filepath.Join(checkpointDir, memoryRangesDiffMetaFile),
		filepath.Join(checkpointDir, memoryRangesDiffDataFile),
		reconstructed,
	); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(reconstructed); !bytes.Equal(got, slotImage) {
		t.Error("golden base + compact checkpoint delta is not the guest's RAM")
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, goldenDeltaMarkerFile)); err != nil {
		t.Errorf("golden-delta marker missing: %v", err)
	}
	assertCanonical(t, checkpointDir)
}

func TestSnapshotVMStateFromSlotCopiesWithoutBase(t *testing.T) {
	ctx := context.Background()
	checkpointDir, ra, _, slotImage := zeroCopyCheckpointFixture(t, false)
	client := fakeFileBackedVMM(t, false)
	if _, err := (&AteWorkerService{}).snapshotVMState(ctx, client, ra, "actor-9", checkpointDir); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(checkpointDir, "memory-ranges")); !bytes.Equal(got, slotImage) {
		t.Error("memory-ranges is not a copy of the guest's RAM")
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, goldenDeltaMarkerFile)); !os.IsNotExist(err) {
		t.Errorf("golden-delta marker written for an undeflated checkpoint (err %v)", err)
	}
	assertCanonical(t, checkpointDir)
}

func TestSnapshotVMStateFromSlotRejectsDumpedMemory(t *testing.T) {
	checkpointDir, ra, _, _ := zeroCopyCheckpointFixture(t, true)
	client := fakeFileBackedVMM(t, true)
	if _, err := (&AteWorkerService{}).snapshotVMState(context.Background(), client, ra, "actor-9", checkpointDir); err == nil {
		t.Error("checkpoint accepted a memory-ranges cloud-hypervisor should not have written")
	}
}

func setup167MBSlotBenchFixture(b *testing.B) (baseFile, slotFile, deltaFile string) {
	b.Helper()
	const logicalSize = 384 * 1024 * 1024
	const residentSize = 167 * 1024 * 1024
	const dirtySize = 7 * 1024 * 1024
	const chunkSize = 1024 * 1024

	dir := b.TempDir()
	baseFile = filepath.Join(dir, "base-167mb.bin")
	slotFile = filepath.Join(dir, "slot-167mb.bin")
	deltaFile = filepath.Join(dir, "delta-7mb.bin")

	bf, err := os.OpenFile(baseFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	if err := bf.Truncate(logicalSize); err != nil {
		b.Fatal(err)
	}
	chunk := bytes.Repeat([]byte{0x42}, chunkSize)
	for off := int64(0); off < residentSize; off += chunkSize {
		if _, err := bf.WriteAt(chunk, off); err != nil {
			b.Fatal(err)
		}
	}
	if err := bf.Close(); err != nil {
		b.Fatal(err)
	}

	if err := ch.CopySparseFileFast(context.Background(), baseFile, slotFile); err != nil {
		b.Fatal(err)
	}

	df, err := os.OpenFile(deltaFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	if err := df.Truncate(logicalSize); err != nil {
		b.Fatal(err)
	}
	dirtyChunk := bytes.Repeat([]byte{0x99}, chunkSize)
	for i := int64(0); i < dirtySize/chunkSize; i++ {
		if _, err := df.WriteAt(dirtyChunk, (i*20+4)*chunkSize); err != nil {
			b.Fatal(err)
		}
	}
	if err := df.Close(); err != nil {
		b.Fatal(err)
	}
	return baseFile, slotFile, deltaFile
}

func BenchmarkSlotPool_OverlayAndReset_167MB(b *testing.B) {
	ctx := context.Background()
	baseFile, slotFile, deltaFile := setup167MBSlotBenchFixture(b)
	b.SetBytes(167 * 1024 * 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extents, _, err := ch.OverlaySparseRegionsRecorded(ctx, deltaFile, slotFile)
		if err != nil {
			b.Fatal(err)
		}
		if err := ch.RestoreSparseRegions(ctx, baseFile, slotFile, extents); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFullBaseCopyAndOverlay_167MB(b *testing.B) {
	ctx := context.Background()
	baseFile, _, deltaFile := setup167MBSlotBenchFixture(b)
	outFile := filepath.Join(b.TempDir(), "out-167mb.bin")
	b.SetBytes(167 * 1024 * 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ch.CopySparseFileFast(ctx, baseFile, outFile); err != nil {
			b.Fatal(err)
		}
		if _, _, err := ch.OverlaySparseRegionsRecorded(ctx, deltaFile, outFile); err != nil {
			b.Fatal(err)
		}
	}
}
