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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
)

func TestFastRestoreConfigZeroCopy(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.zeroCopyRestore || c.zeroCopyNeedsTmpfs {
		t.Errorf("defaults = %+v, want zero-copy off", c)
	}

	c, err = fastRestoreConfigFromEnv(envMap(map[string]string{envZeroCopyRestore: "true"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.zeroCopyRestore || !c.zeroCopyNeedsTmpfs || c.enabled() {
		t.Errorf("zero-copy without tmpfs slots = %+v, want it off and flagged for the startup warning", c)
	}

	c, err = fastRestoreConfigFromEnv(envMap(map[string]string{envZeroCopyRestore: "true", envTmpfsSlots: "true"}))
	if err != nil {
		t.Fatal(err)
	}
	if !c.zeroCopyRestore || c.zeroCopyNeedsTmpfs || !c.enabled() {
		t.Errorf("zero-copy with tmpfs slots = %+v, want it on", c)
	}
	found := false
	for _, a := range c.attrs() {
		if strings.Contains(slogValue(a), "zero_copy_restore=true") {
			found = true
		}
	}
	if !found {
		t.Errorf("startup attrs %v do not report zero_copy_restore=true", c.attrs())
	}

	for _, v := range []string{"", "false", "0"} {
		c, err := fastRestoreConfigFromEnv(envMap(map[string]string{envZeroCopyRestore: v, envTmpfsSlots: "true"}))
		if err != nil {
			t.Fatal(err)
		}
		if c.zeroCopyRestore {
			t.Errorf("%s=%q turned zero-copy on", envZeroCopyRestore, v)
		}
	}
	if _, err := fastRestoreConfigFromEnv(envMap(map[string]string{envZeroCopyRestore: "sometimes"})); err == nil {
		t.Errorf("%s=sometimes accepted, want an error", envZeroCopyRestore)
	}
}

func slogValue(a any) string {
	if s, ok := a.(interface{ String() string }); ok {
		return s.String()
	}
	return ""
}

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

// claimedSlotFixture acquires a pre-staged tmpfs slot for a warm delta.
func claimedSlotFixture(t *testing.T, restoreSub string) (f *slotTestFixture, pool *baseSlotPool, lease *goldenSlotLease, restoreMem string, full, delta []byte) {
	t.Helper()
	ctx := context.Background()
	f = newSlotTestFixture(t)
	root := useTmpfsSlots(t)
	pool = getOrCreateBaseSlotPool(f.baseFile, root)
	if !pool.ready(ctx) {
		t.Fatal("tmpfs pool not ready")
	}
	pool.replenishOneSlot() // pre-staged, so the restore takes it from the pool
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

// assertSlotKept checks that a claimed slot still holds the guest's image and
// is not (and does not become) one of the pool's idle slots.
func assertSlotKept(t *testing.T, pool *baseSlotPool, slot string, full []byte) {
	t.Helper()
	if got, err := os.ReadFile(slot); err != nil || !bytes.Equal(got, full) {
		t.Errorf("claimed slot %s was reset or removed (err %v)", slot, err)
	}
	time.Sleep(20 * time.Millisecond) // a stray background reset would land by now
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
	// The pool stages a replacement.
	for _, s := range waitIdleSlots(t, pool, 1) {
		if s.path == slot {
			t.Fatal("the replacement is the claimed slot")
		}
		pool.idle <- s
	}

	if !lease.commit(true, true) {
		t.Error("preserved commit of a claimed slot did not keep the delta")
	}
	lease.abort() // no-op after commit
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

// A slot staged for the restore (empty pool) is not from the pool, and a
// merge on the node disk has no slot to claim.
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
	pool := lease.pool
	lease.abort()
	waitIdleSlots(t, pool, 1)

	t.Setenv("ATEOM_MICROVM_DISABLE_GOLDEN_SLOT_POOL", "true")
	lease, err = acquireGoldenSlot(ctx, f.baseFile, restoreMem, restoreMem)
	if err != nil {
		t.Fatal(err)
	}
	if lease.slot != nil || lease.claimForVM() != "" {
		t.Errorf("merged-on-disk lease = %+v, want nothing to claim", lease)
	}
}

const chFixtures = "internal/ch/testdata/filebacked"

const fixtureGuestRAM = 402653184

// zeroCopyRestoreDir is a restore directory holding the memfd fixture
// snapshot, plus a sparse guest RAM slot of its size.
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

// testLease is a lease on slot from a tmpfs pool whose base is a small file.
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

	// After a failed zero-copy attempt the actor's next restore copies once.
	zeroCopyBackoff.Store("actor-2", struct{}{})
	if zc, why, err := startZeroCopyRestore(ctx, "actor-2", restoreDir, idle); zc != nil || err != nil || why != zeroCopyPreviousFailure {
		t.Errorf("after a failure: startZeroCopyRestore = %v, %q, %v; want %q", zc, why, err, zeroCopyPreviousFailure)
	}
	unchanged()
	if _, still := zeroCopyBackoff.Load("actor-2"); still {
		t.Error("the backoff outlived the attempt it skipped")
	}

	// A layout cloud-hypervisor cannot map from a file.
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
	waitIdleSlots(t, pool, 1) // the pool restocked

	// restoreDir now maps the slot.
	newCfg, newState := readSnapshot(t, restoreDir)
	if mem := string(memorySection(t, newCfg)); !strings.Contains(mem, `"size":0,`) || !strings.Contains(mem, `"file":"`+slot+`"`) {
		t.Errorf("rewritten memory section %s does not map the slot", mem)
	}
	if bytes.Equal(newState, state) {
		t.Error("state.json not rewritten")
	}

	// A failed restore puts restoreDir back, deletes the slot and makes the
	// actor's next attempt copy.
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
	s := &AteomService{}
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
	s.discardMemSlot(context.Background(), "actor", ra) // a second teardown is a no-op
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
	// A different file at the slot's path is not the guest's RAM.
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

// fakeFileBackedVMM serves vm.snapshot on a unix socket like cloud-hypervisor
// does for a VM whose RAM is file-backed: it writes the fixture's file-backed
// config.json and state.json, and (withMemory) a memory-ranges.
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

// zeroCopyCheckpointFixture lays out <ateRoot>/actors/<uid>/checkpoint-state
// and a guest RAM slot that differs from the golden base (cached under
// <ateRoot>/golden-base-cache/<baseID> when withBase) in a few pages.
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
	ra.memSlotBase = base
	ra.origMemory = memorySection(t, memfdCfg)
	return checkpointDir, ra, base, slotImage
}

// assertCanonical checks that a checkpoint is in the standard memfd format:
// the memfd fixture's memory section and memory_ranges table.
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
	if _, err := (&AteomService{}).snapshotVMState(ctx, client, ra, "actor-9", checkpointDir); err != nil {
		t.Fatal(err)
	}
	// memory-ranges is the slot's delta against the base: base + delta = slot.
	merged := filepath.Join(t.TempDir(), "merged")
	if err := ch.MergeSparseOverlay(ctx, base, filepath.Join(checkpointDir, "memory-ranges"), merged); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(merged); !bytes.Equal(got, slotImage) {
		t.Error("golden base + checkpoint delta is not the guest's RAM")
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, goldenDeltaFile)); err != nil {
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

func TestSnapshotVMStateFromSlotCopiesWithoutBase(t *testing.T) {
	ctx := context.Background()
	checkpointDir, ra, _, slotImage := zeroCopyCheckpointFixture(t, false)
	client := fakeFileBackedVMM(t, false)
	if _, err := (&AteomService{}).snapshotVMState(ctx, client, ra, "actor-9", checkpointDir); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(checkpointDir, "memory-ranges")); !bytes.Equal(got, slotImage) {
		t.Error("memory-ranges is not a copy of the guest's RAM")
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, goldenDeltaFile)); !os.IsNotExist(err) {
		t.Errorf("golden-delta marker written for an undeflated checkpoint (err %v)", err)
	}
	assertCanonical(t, checkpointDir)
}

func TestSnapshotVMStateFromSlotRejectsDumpedMemory(t *testing.T) {
	checkpointDir, ra, _, _ := zeroCopyCheckpointFixture(t, true)
	client := fakeFileBackedVMM(t, true)
	if _, err := (&AteomService{}).snapshotVMState(context.Background(), client, ra, "actor-9", checkpointDir); err == nil {
		t.Error("checkpoint accepted a memory-ranges cloud-hypervisor should not have written")
	}
}
