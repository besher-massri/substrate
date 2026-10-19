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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// writeSnapshotConfig writes a config.json holding the given fs devices (plus the
// vsock and console entries every snapshot has) into a fresh snapshot dir. The
// serial device is the debug-mode one, present here so the rewrite is exercised
// against a snapshot that has both.
func writeSnapshotConfig(t *testing.T, fsDevices []map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	cfg := map[string]any{
		"vsock":   map[string]any{"cid": 3, "socket": "/run/vc/vm/golden/clh.sock"},
		"console": map[string]any{"mode": "File", "file": "/run/vc/vm/golden/console.log"},
		"serial":  map[string]any{"mode": "File", "file": "/run/vc/vm/golden/serial.log"},
		"fs":      fsDevices,
		"payload": map[string]any{"kernel": "/var/lib/ate/actors/golden/sandbox-assets/kata-kernel", "cmdline": "console=hvc0"},
		"pmem":    []map[string]any{{"id": guestImagePmemID, "file": "/var/lib/ate/actors/golden/sandbox-assets/kata-image", "discard_writes": true}},
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshaling config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), b, 0o600); err != nil {
		t.Fatalf("writing config.json: %v", err)
	}
	return dir
}

// mutateSnapshotConfig rewrites dir's config.json through mutate.
func mutateSnapshotConfig(t *testing.T, dir string, mutate func(cfg map[string]any)) {
	t.Helper()
	cfgPath := filepath.Join(dir, "config.json")
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	mutate(cfg)
	b, err = json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgPath, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// readFsSockets returns the rewritten config's fs sockets keyed by device tag.
func readFsSockets(t *testing.T, dir string) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("reading config.json: %v", err)
	}
	var cfg struct {
		Vsock  map[string]any `json:"vsock"`
		Serial map[string]any `json:"serial"`
		Fs     []struct {
			Tag    string `json:"tag"`
			Socket string `json:"socket"`
		} `json:"fs"`
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatalf("parsing rewritten config: %v", err)
	}
	got := map[string]string{}
	for _, f := range cfg.Fs {
		got[f.Tag] = f.Socket
	}
	return got
}

func TestRewriteSnapshotPaths(t *testing.T) {
	const id = "actor-uid"
	// The restoring actor's own staged kernel and guest image.
	const (
		kernel = "/var/lib/ate/actors/actor-uid/sandbox-assets/kata-kernel"
		image  = "/var/lib/ate/actors/actor-uid/sandbox-assets/kata-image"
	)

	t.Run("overlay lower only", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		if err := rewriteSnapshotPaths(dir, id, kernel, image); err != nil {
			t.Fatalf("rewriteSnapshotPaths: %v", err)
		}
		if got, want := readFsSockets(t, dir)[kata.FsTag], kata.VirtiofsdSocketPath(id); got != want {
			t.Errorf("%s socket = %q, want %q", kata.FsTag, got, want)
		}
	})

	t.Run("unknown tag is an error", func(t *testing.T) {
		// "ateUpper", "ateDurable", and "ateCSI" are retired multi-share tags:
		// they never appear in snapshots this code produces, and one showing up
		// must fail loudly rather than be silently repointed.
		for _, tag := range []string{"somethingElse", "ateUpper", "ateDurable", "ateCSI"} {
			dir := writeSnapshotConfig(t, []map[string]any{
				{"tag": tag, "socket": "/run/vc/vm/golden/other.sock"},
			})
			if err := rewriteSnapshotPaths(dir, id, kernel, image); err == nil {
				t.Fatalf("rewriteSnapshotPaths accepted fs tag %q, want an error", tag)
			}
		}
	})

	// A snapshot config that lacks or mangles the two per-actor boot paths
	// is broken: skipping their rewrite would restore with the source
	// actor's, so each shape is an error.
	for name, mutate := range map[string]func(cfg map[string]any){
		"missing payload":            func(cfg map[string]any) { delete(cfg, "payload") },
		"malformed payload":          func(cfg map[string]any) { cfg["payload"] = "vmlinux" },
		"malformed pmem entry":       func(cfg map[string]any) { cfg["pmem"] = []any{"not a device"} },
		"missing pmem list":          func(cfg map[string]any) { delete(cfg, "pmem") },
		"missing guest image device": func(cfg map[string]any) { cfg["pmem"] = []any{} },
	} {
		t.Run(name+" is an error", func(t *testing.T) {
			dir := writeSnapshotConfig(t, []map[string]any{
				{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
			})
			mutateSnapshotConfig(t, dir, mutate)
			if err := rewriteSnapshotPaths(dir, id, kernel, image); err == nil {
				t.Fatalf("rewriteSnapshotPaths accepted a config with %s, want an error", name)
			}
		})
	}

	t.Run("unknown pmem device is an error", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		cfgPath := filepath.Join(dir, "config.json")
		b, err := os.ReadFile(cfgPath)
		if err != nil {
			t.Fatal(err)
		}
		var cfg map[string]any
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatal(err)
		}
		cfg["pmem"] = []map[string]any{{"id": "scratch-disk", "file": "/somewhere/else"}}
		b, err = json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfgPath, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := rewriteSnapshotPaths(dir, id, kernel, image); err == nil {
			t.Fatal("rewriteSnapshotPaths accepted a pmem device it does not know, want an error")
		}
	})

	t.Run("vsock, console, kernel and image are repointed", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		if err := rewriteSnapshotPaths(dir, id, kernel, image); err != nil {
			t.Fatalf("rewriteSnapshotPaths: %v", err)
		}
		b, err := os.ReadFile(filepath.Join(dir, "config.json"))
		if err != nil {
			t.Fatalf("reading config.json: %v", err)
		}
		var cfg struct {
			Vsock   struct{ Socket string } `json:"vsock"`
			Console struct{ File string }   `json:"console"`
			Serial  struct{ File string }   `json:"serial"`
			Payload struct{ Kernel string } `json:"payload"`
			Pmem    []struct {
				ID   string `json:"id"`
				File string `json:"file"`
			} `json:"pmem"`
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatalf("parsing rewritten config: %v", err)
		}
		if want := kata.VsockSocketPath(id); cfg.Vsock.Socket != want {
			t.Errorf("vsock socket = %q, want %q", cfg.Vsock.Socket, want)
		}
		// The source actor's staged kernel and image may be gone; the
		// snapshot must boot from this actor's.
		if cfg.Payload.Kernel != kernel {
			t.Errorf("payload kernel = %q, want %q", cfg.Payload.Kernel, kernel)
		}
		if len(cfg.Pmem) != 1 || cfg.Pmem[0].ID != guestImagePmemID || cfg.Pmem[0].File != image {
			t.Errorf("pmem = %+v, want the %q device at %q", cfg.Pmem, guestImagePmemID, image)
		}
		if want := kata.ConsoleLogPath(id); cfg.Console.File != want {
			t.Errorf("console file = %q, want %q", cfg.Console.File, want)
		}
		if want := kata.SerialLogPath(id); cfg.Serial.File != want {
			t.Errorf("serial file = %q, want %q", cfg.Serial.File, want)
		}
	})

	// atelet stages a local checkpoint into the restore dir by hard-linking it, so
	// the config.json rewritten here can share an inode with the actor's cached
	// pause snapshot. Writing in place would rewrite that snapshot's config too.
	t.Run("a hardlinked config is not written through", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		cfgPath := filepath.Join(dir, "config.json")
		before, err := os.ReadFile(cfgPath)
		if err != nil {
			t.Fatalf("reading config.json: %v", err)
		}
		cached := filepath.Join(t.TempDir(), "config.json")
		if err := os.Link(cfgPath, cached); err != nil {
			t.Fatalf("linking config.json: %v", err)
		}

		if err := rewriteSnapshotPaths(dir, id, kernel, image); err != nil {
			t.Fatalf("rewriteSnapshotPaths: %v", err)
		}
		if got, want := readFsSockets(t, dir)[kata.FsTag], kata.VirtiofsdSocketPath(id); got != want {
			t.Errorf("%s socket = %q, want %q", kata.FsTag, got, want)
		}
		got, err := os.ReadFile(cached)
		if err != nil {
			t.Fatalf("reading the linked config.json: %v", err)
		}
		if !bytes.Equal(got, before) {
			t.Errorf("the linked config.json was rewritten: got %q, want the original %q", got, before)
		}
		// Nothing may be left behind for atelet to ship as part of the snapshot.
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.Name() != "config.json" {
				t.Errorf("stray file left in the restore dir: %q", e.Name())
			}
		}
	})

	t.Run("unchanged socket paths leave config.json untouched", func(t *testing.T) {
		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.json")
		cfg := map[string]any{
			"vsock":   map[string]any{"cid": 3, "socket": kata.VsockSocketPath(id)},
			"console": map[string]any{"mode": "File", "file": kata.ConsoleLogPath(id)},
			"serial":  map[string]any{"mode": "File", "file": kata.SerialLogPath(id)},
			"fs": []map[string]any{
				{"tag": kata.FsTag, "socket": kata.VirtiofsdSocketPath(id)},
			},
			"payload": map[string]any{"kernel": kernel, "cmdline": "console=hvc0"},
			"pmem":    []map[string]any{{"id": guestImagePmemID, "file": image, "discard_writes": true}},
		}
		b, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("marshaling config: %v", err)
		}
		if err := os.WriteFile(cfgPath, b, 0o600); err != nil {
			t.Fatalf("writing config.json: %v", err)
		}
		fiBefore, err := os.Stat(cfgPath)
		if err != nil {
			t.Fatalf("stat before: %v", err)
		}
		inoBefore := fiBefore.Sys().(*syscall.Stat_t).Ino

		if err := rewriteSnapshotPaths(dir, id, kernel, image); err != nil {
			t.Fatalf("rewriteSnapshotPaths: %v", err)
		}

		fiAfter, err := os.Stat(cfgPath)
		if err != nil {
			t.Fatalf("stat after: %v", err)
		}
		inoAfter := fiAfter.Sys().(*syscall.Stat_t).Ino
		if inoAfter != inoBefore {
			t.Errorf("config.json inode changed (%d -> %d); expected no rewrite when socket paths already match", inoBefore, inoAfter)
		}
	})
}

func TestRestoreWorkloadRejectsEmptyRestoreDir(t *testing.T) {
	s := &AteWorkerService{
		locks:    actorlock.New(),
		inFlight: actorlock.NewInFlight(),
	}

	noDirs := validRestoreRequest()
	noDirs.ActorDirs = nil
	noRestoreDir := validRestoreRequest()
	noRestoreDir.ActorDirs.RestoreDir = ""
	for _, tc := range []struct {
		name string
		req  *ateworkerpb.RestoreWorkloadRequest
		want string
	}{
		{name: "nil actor_dirs", req: noDirs, want: "actor_dirs"},
		{name: "empty restore_dir", req: noRestoreDir, want: "restore_dir"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RestoreWorkload(context.Background(), tc.req)
			if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RestoreWorkload() = %v, want InvalidArgument naming %s", err, tc.want)
			}
		})
	}
}

func TestMaybeDropStagedMemoryImage(t *testing.T) {
	for _, tc := range []struct {
		name               string
		memMode            string
		preserveRestoreDir bool
		wantRemoved        bool
	}{
		{
			name:               "eager staging dir removes memory-ranges",
			memMode:            ch.MemRestoreEager,
			preserveRestoreDir: false,
			wantRemoved:        true,
		},
		{
			name:               "eager preserved dir keeps memory-ranges",
			memMode:            ch.MemRestoreEager,
			preserveRestoreDir: true,
			wantRemoved:        false,
		},
		{
			name:               "on-demand staging dir keeps memory-ranges",
			memMode:            ch.MemRestoreOnDemand,
			preserveRestoreDir: false,
			wantRemoved:        false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			memPath := filepath.Join(dir, "memory-ranges")
			if err := os.WriteFile(memPath, []byte("ram"), 0o600); err != nil {
				t.Fatalf("writing memory-ranges: %v", err)
			}

			maybeDropStagedMemoryImage(context.Background(), dir, tc.memMode, tc.preserveRestoreDir)

			_, err := os.Stat(memPath)
			if tc.wantRemoved && !os.IsNotExist(err) {
				t.Errorf("memory-ranges still exists (stat err = %v), want removed", err)
			}
			if !tc.wantRemoved && err != nil {
				t.Errorf("memory-ranges missing (stat err = %v), want preserved", err)
			}
		})
	}
}

func TestMergeOnDemandDeltaPreservesRestoreSource(t *testing.T) {
	const pageSize = 4096
	const fileSize = 2 * pageSize

	basePage0 := bytes.Repeat([]byte{0xAA}, pageSize)
	deltaPage1 := bytes.Repeat([]byte{0xBB}, pageSize)
	wantMerged := append(append([]byte{}, basePage0...), deltaPage1...)

	writePages := func(t *testing.T, path string, off int64, data []byte) {
		t.Helper()
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := f.Truncate(fileSize); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(data, off); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("preserveRestoreSource=true keeps single-link base in restoreSourceDir", func(t *testing.T) {
		restoreDir := t.TempDir()
		checkpointDir := t.TempDir()
		basePath := filepath.Join(restoreDir, "memory-ranges")
		deltaPath := filepath.Join(checkpointDir, "memory-ranges")

		writePages(t, basePath, 0, basePage0)
		writePages(t, deltaPath, pageSize, deltaPage1)

		if err := mergeOnDemandDelta(context.Background(), restoreDir, checkpointDir, true); err != nil {
			t.Fatalf("mergeOnDemandDelta: %v", err)
		}

		// The preserved local snapshot's memory-ranges must still exist and hold
		// its original contents.
		gotBase, err := os.ReadFile(basePath)
		if err != nil {
			t.Fatalf("base memory-ranges was removed from preserved restoreDir: %v", err)
		}
		wantBase := append(append([]byte{}, basePage0...), make([]byte, pageSize)...)
		if !bytes.Equal(gotBase, wantBase) {
			t.Errorf("base memory-ranges was mutated in preserved restoreDir")
		}

		gotMerged, err := os.ReadFile(deltaPath)
		if err != nil {
			t.Fatalf("reading merged memory-ranges: %v", err)
		}
		if !bytes.Equal(gotMerged, wantMerged) {
			t.Errorf("merged memory-ranges mismatch")
		}
	})

	t.Run("preserveRestoreSource=false moves expendable base into checkpointDir", func(t *testing.T) {
		restoreDir := t.TempDir()
		checkpointDir := t.TempDir()
		basePath := filepath.Join(restoreDir, "memory-ranges")
		deltaPath := filepath.Join(checkpointDir, "memory-ranges")

		writePages(t, basePath, 0, basePage0)
		writePages(t, deltaPath, pageSize, deltaPage1)

		if err := mergeOnDemandDelta(context.Background(), restoreDir, checkpointDir, false); err != nil {
			t.Fatalf("mergeOnDemandDelta: %v", err)
		}

		if _, err := os.Stat(basePath); !os.IsNotExist(err) {
			t.Errorf("expendable base memory-ranges still in restoreDir (stat err = %v), want moved", err)
		}
		gotMerged, err := os.ReadFile(deltaPath)
		if err != nil {
			t.Fatalf("reading merged memory-ranges: %v", err)
		}
		if !bytes.Equal(gotMerged, wantMerged) {
			t.Errorf("merged memory-ranges mismatch")
		}
	})
}

func TestNewReseedNonce(t *testing.T) {
	a, err := newReseedNonce()
	if err != nil {
		t.Fatalf("newReseedNonce: %v", err)
	}
	if len(a) != 32 {
		t.Fatalf("nonce length = %d, want 32", len(a))
	}
	// The reseed only makes clones of one snapshot diverge if each restore mixes in
	// distinct bytes, so two nonces must never be equal.
	b, err := newReseedNonce()
	if err != nil {
		t.Fatalf("newReseedNonce (second call): %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two nonces are identical; reseed would not make clones diverge")
	}
}

type fakeReseeder struct {
	calls int
	fn    func(ctx context.Context, attempt int, data []byte) error
}

func (f *fakeReseeder) ReseedRandomDev(ctx context.Context, data []byte) error {
	f.calls++
	return f.fn(ctx, f.calls, data)
}

func TestReseedGuestCRNG(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		f := &fakeReseeder{
			fn: func(_ context.Context, _ int, data []byte) error {
				if len(data) != 32 {
					t.Errorf("nonce length = %d, want 32", len(data))
				}
				return nil
			},
		}
		if err := reseedGuestCRNG(context.Background(), f); err != nil {
			t.Fatalf("reseedGuestCRNG: %v", err)
		}
		if f.calls != 1 {
			t.Errorf("calls = %d, want 1", f.calls)
		}
	})

	t.Run("retries transient timeout and succeeds", func(t *testing.T) {
		var firstNonce []byte
		f := &fakeReseeder{
			fn: func(_ context.Context, attempt int, data []byte) error {
				if attempt == 1 {
					firstNonce = append([]byte(nil), data...)
					return context.DeadlineExceeded
				}
				if !bytes.Equal(data, firstNonce) {
					t.Errorf("retry nonce changed across attempts")
				}
				return nil
			},
		}
		if err := reseedGuestCRNG(context.Background(), f); err != nil {
			t.Fatalf("reseedGuestCRNG: %v", err)
		}
		if f.calls != 2 {
			t.Errorf("calls = %d, want 2", f.calls)
		}
	})

	t.Run("fails after exhausting all attempts", func(t *testing.T) {
		sentinel := errors.New("guest agent stalled")
		f := &fakeReseeder{
			fn: func(_ context.Context, _ int, _ []byte) error {
				return sentinel
			},
		}
		err := reseedGuestCRNG(context.Background(), f)
		if !errors.Is(err, sentinel) {
			t.Fatalf("reseedGuestCRNG error = %v, want %v", err, sentinel)
		}
		if f.calls != reseedMaxAttempts {
			t.Errorf("calls = %d, want %d", f.calls, reseedMaxAttempts)
		}
	})

	t.Run("does not retry when ttrpc connection is closed", func(t *testing.T) {
		f := &fakeReseeder{
			fn: func(_ context.Context, _ int, _ []byte) error {
				return ttrpc.ErrClosed
			},
		}
		err := reseedGuestCRNG(context.Background(), f)
		if !errors.Is(err, ttrpc.ErrClosed) {
			t.Fatalf("reseedGuestCRNG error = %v, want %v", err, ttrpc.ErrClosed)
		}
		if f.calls != 1 {
			t.Errorf("calls = %d, want 1 (closed connection cannot recover)", f.calls)
		}
	})

	t.Run("stops retrying when caller context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		f := &fakeReseeder{
			fn: func(_ context.Context, _ int, _ []byte) error {
				cancel()
				return context.Canceled
			},
		}
		err := reseedGuestCRNG(ctx, f)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reseedGuestCRNG error = %v, want %v", err, context.Canceled)
		}
		if f.calls != 1 {
			t.Errorf("calls = %d, want 1", f.calls)
		}
	})

	t.Run("respects ATEWORKER_MICROVM_CRNG_TIMEOUT_MS override", func(t *testing.T) {
		t.Setenv("ATEWORKER_MICROVM_CRNG_TIMEOUT_MS", "15000")
		if got := reseedAttemptTimeoutForRestore(); got != 15*time.Second {
			t.Fatalf("reseedAttemptTimeoutForRestore() = %v, want 15s", got)
		}
	})
}

func TestDifferentialSnapshotCheckpointAndRestoreRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name               string
		deltaFormat        string
		preserveRestoreDir bool
	}{
		{name: "sparse deflate staging dir", deltaFormat: "sparse", preserveRestoreDir: false},
		{name: "sparse deflate preserved local snapshot", deltaFormat: "sparse", preserveRestoreDir: true},
		{name: "compact delta staging dir", deltaFormat: "compact", preserveRestoreDir: false},
		{name: "compact delta preserved local snapshot", deltaFormat: "compact", preserveRestoreDir: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ATEWORKER_MICROVM_DIFF_SNAPSHOT", "true")
			t.Setenv("ATEWORKER_MICROVM_DELTA_FORMAT", tc.deltaFormat)

			ctx := context.Background()
			ateRoot := t.TempDir()
			baseDir := filepath.Join(ateRoot, "actors")
			if err := os.MkdirAll(baseDir, 0o700); err != nil {
				t.Fatal(err)
			}
			origActorsDir := nodepath.ActorsDir
			nodepath.ActorsDir = baseDir
			t.Cleanup(func() { nodepath.ActorsDir = origActorsDir })
			s := &AteWorkerService{}

			const baseID = "golden-uid"
			const actorUID = "actor-1"
			const size = 2 << 20 // 2 MiB
			const pageSize = 4096

			baseImg := make([]byte, size)
			for i := 0; i < size; i += 2 * pageSize {
				for j := 0; j < pageSize; j++ {
					baseImg[i+j] = byte((i/pageSize + j) % 251 + 1)
				}
			}
			warmImg := append([]byte(nil), baseImg...)
			// Dirty 4 pages.
			for _, p := range []int{0, 10, 40, 200} {
				off := p * pageSize
				for j := 0; j < pageSize; j++ {
					warmImg[off+j] = 0xEE
				}
			}
			wantHash := sha256.Sum256(warmImg)

			// 1. Cold restore caches golden base.
			coldRestoreDir := filepath.Join(ateRoot, "cold-restore")
			if err := os.MkdirAll(coldRestoreDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(coldRestoreDir, "memory-ranges"), baseImg, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.reconstructDeltaIfNeeded(ctx, actorUID, baseID, coldRestoreDir, false); err != nil {
				t.Fatalf("cold reconstructDeltaIfNeeded: %v", err)
			}
			if _, err := os.Stat(goldenBaseMemoryRangesPath(baseID)); err != nil {
				t.Fatalf("golden base cache not populated on cold restore: %v", err)
			}

			// 2. Warm checkpoint deflates against cached golden base.
			checkpointDir := filepath.Join(ateRoot, "warm-checkpoint")
			if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(checkpointDir, "config.json"), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(checkpointDir, "state.json"), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(checkpointDir, baseIDFile), []byte(baseID), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(checkpointDir, "memory-ranges"), warmImg, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := s.maybeDeflateCheckpointAgainstGoldenBase(ctx, baseID, actorUID, checkpointDir); err != nil {
				t.Fatalf("maybeDeflateCheckpointAgainstGoldenBase: %v", err)
			}
			if !isDeltaSnapshot(checkpointDir) {
				t.Fatal("expected checkpointDir to be marked as delta snapshot")
			}

			// 3. Warm restore reconstructs byte-for-byte identical memory-ranges.
			vmmDir, vmmPreserve, err := s.reconstructDeltaIfNeeded(ctx, actorUID, baseID, checkpointDir, tc.preserveRestoreDir)
			if err != nil {
				t.Fatalf("warm reconstructDeltaIfNeeded: %v", err)
			}
			if vmmPreserve {
				t.Fatal("expected vmmPreserve=false after delta reconstruction")
			}
			if tc.preserveRestoreDir {
				if vmmDir == checkpointDir {
					t.Fatal("expected preserved delta snapshot to stage into separate restore dir")
				}
				if !isDeltaSnapshot(checkpointDir) {
					t.Fatal("expected preserved delta snapshot directory to retain delta markers")
				}
			}

			gotBytes, err := os.ReadFile(filepath.Join(vmmDir, "memory-ranges"))
			if err != nil {
				t.Fatalf("reading reconstructed memory-ranges: %v", err)
			}
			if gotHash := sha256.Sum256(gotBytes); gotHash != wantHash {
				t.Fatalf("reconstructed SHA256 = %x, want %x", gotHash, wantHash)
			}
		})
	}
}

func TestRPCsRejectUntrustedRuntimeAssetPaths(t *testing.T) {
	s := &AteWorkerService{}
	ctx := context.Background()
	assets := map[string]string{assetCH: "/bin/sh"}
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			req := validRunRequest()
			req.RuntimeAssetPaths = assets
			_, err := s.RunWorkload(ctx, req)
			return err
		},
		"RestoreWorkload": func() error {
			req := validRestoreRequest()
			req.RuntimeAssetPaths = assets
			_, err := s.RestoreWorkload(ctx, req)
			return err
		},
	} {
		err := call()
		if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "runtime_asset_paths") {
			t.Errorf("%s() = %v, want InvalidArgument naming runtime_asset_paths", name, err)
		}
	}
}

// TestValidateRuntimeAssetPathsRequiresTheActorsOwnDir pins that the root
// the asset paths are checked against is derived from the actor UID, not
// taken from the request.
func TestValidateRuntimeAssetPathsRequiresTheActorsOwnDir(t *testing.T) {
	orig := nodepath.ActorsDir
	nodepath.ActorsDir = t.TempDir()
	t.Cleanup(func() { nodepath.ActorsDir = orig })
	stage := func(t *testing.T, actorUID string) string {
		t.Helper()
		dir := nodepath.ActorSandboxAssetsDir(actorUID)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, assetCH), nil, 0o555); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	own, other := stage(t, "actor-a"), stage(t, "actor-b")
	for _, tc := range []struct {
		name    string
		dir     string
		asset   string
		wantErr bool
	}{
		{"asset in the actor's own dir", own, filepath.Join(own, assetCH), false},
		{"asset outside the actor's dir", own, "/bin/sh", true},
		{"another actor's dir named as the root", other, filepath.Join(other, assetCH), true},
		{"the root named as the root", "/", "/bin/sh", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRuntimeAssetPaths("actor-a", &ateworkerpb.ActorDirs{SandboxAssetsDir: tc.dir}, map[string]string{assetCH: tc.asset})
			if (err != nil) != tc.wantErr {
				t.Errorf("validateRuntimeAssetPaths(actor-a, %q, %q) = %v, wantErr %v", tc.dir, tc.asset, err, tc.wantErr)
			}
		})
	}
}

// TestRestoreMemoryFidelityRequiresKernelAndImage pins that a memory restore
// without the kernel or the guest image among its asset paths fails before
// it prepares or cleans up anything: the snapshot's boot paths are repointed
// at them, so there is nothing to restore with.
func TestRestoreMemoryFidelityRequiresKernelAndImage(t *testing.T) {
	// A zero service cannot prepare egress or clean up sandbox state; the
	// check must come first for this test to return at all.
	s := &AteWorkerService{}
	for name, paths := range map[string]map[string]string{
		"no assets":  nil,
		"no kernel":  {assetImage: "/node/actors/actor-a/sandbox-assets/kata-image"},
		"no image":   {assetKernel: "/node/actors/actor-a/sandbox-assets/kata-kernel"},
		"empty path": {assetKernel: "", assetImage: "/node/actors/actor-a/sandbox-assets/kata-image"},
	} {
		t.Run(name, func(t *testing.T) {
			p := actorBootParams{actorUID: "actor-a", assetPaths: paths}
			err := s.restoreMemoryFidelity(t.Context(), p, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, t.TempDir(), false, time.Now())
			if err == nil || !strings.Contains(err.Error(), assetKernel) || !strings.Contains(err.Error(), assetImage) {
				t.Fatalf("restoreMemoryFidelity with %s = %v, want an error naming %q and %q", name, err, assetKernel, assetImage)
			}
		})
	}
}

func BenchmarkRestorePrep_ParallelVsSequential(b *testing.B) {
	const id = "bench-actor-uid"
	const kernel = "/var/lib/ate/actors/bench-actor-uid/sandbox-assets/kata-kernel"
	const image = "/var/lib/ate/actors/bench-actor-uid/sandbox-assets/kata-image"

	setupDir := func(b *testing.B) string {
		b.Helper()
		dir := b.TempDir()
		cfg := map[string]any{
			"vsock":   map[string]any{"cid": 3, "socket": "/run/vc/vm/golden/clh.sock"},
			"console": map[string]any{"mode": "File", "file": "/run/vc/vm/golden/console.log"},
			"serial":  map[string]any{"mode": "File", "file": "/run/vc/vm/golden/serial.log"},
			"fs":      []map[string]any{{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"}},
			"payload": map[string]any{"kernel": "/var/lib/ate/actors/golden/sandbox-assets/kata-kernel", "cmdline": "console=hvc0"},
			"pmem":    []map[string]any{{"id": guestImagePmemID, "file": "/var/lib/ate/actors/golden/sandbox-assets/kata-image", "discard_writes": true}},
		}
		raw, err := json.Marshal(cfg)
		if err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "config.json"), raw, 0o600); err != nil {
			b.Fatal(err)
		}
		return dir
	}

	b.Run("SequentialPrep", func(b *testing.B) {
		dir := setupDir(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			// Simulate sequential PrepareEgress RPC + lowers/VMM restore + config.json rewrite
			time.Sleep(2 * time.Millisecond)
			time.Sleep(2 * time.Millisecond)
			if err := rewriteSnapshotPaths(dir, id, kernel, image); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ParallelPrep", func(b *testing.B) {
		dir := setupDir(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			egressDone := make(chan error, 1)
			go func() {
				// Simulate async PrepareEgress RPC overlapped across lowers + VMM restore
				time.Sleep(2 * time.Millisecond)
				egressDone <- nil
			}()
			reconstructDone := make(chan error, 1)
			go func() {
				reconstructDone <- rewriteSnapshotPaths(dir, id, kernel, image)
			}()
			// Simulate lowers + VMM restore overlapped with async PrepareEgress and config.json rewrite
			time.Sleep(2 * time.Millisecond)
			if err := <-reconstructDone; err != nil {
				b.Fatal(err)
			}
			if err := <-egressDone; err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkApplyMemoryRangesDelta(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	const residentSize = 64 << 20 // 64 MiB
	const pageSize = 4096

	baseFile := filepath.Join(dir, "golden-base")
	targetFile := filepath.Join(dir, "target-full")
	deltaSparseFile := filepath.Join(dir, "delta-sparse")

	chunk := bytes.Repeat([]byte{0x5A}, 1<<20)
	bf, err := os.Create(baseFile)
	if err != nil {
		b.Fatal(err)
	}
	tf, err := os.Create(targetFile)
	if err != nil {
		b.Fatal(err)
	}
	for written := 0; written < residentSize; written += len(chunk) {
		if _, err := bf.Write(chunk); err != nil {
			b.Fatal(err)
		}
		if _, err := tf.Write(chunk); err != nil {
			b.Fatal(err)
		}
	}
	bf.Close()
	dirtyPage := bytes.Repeat([]byte{0xA5}, pageSize)
	for p := 0; p < residentSize/pageSize; p += 25 {
		if _, err := tf.WriteAt(dirtyPage, int64(p*pageSize)); err != nil {
			b.Fatal(err)
		}
	}
	tf.Close()
	if err := ch.CopySparseFileFast(ctx, targetFile, deltaSparseFile); err != nil {
		b.Fatal(err)
	}
	if _, _, err := ch.DeflateAgainstBase(ctx, baseFile, deltaSparseFile); err != nil {
		b.Fatal(err)
	}

	b.Run("FullCopy_64MB", func(b *testing.B) {
		outFile := filepath.Join(dir, "full-copy-out")
		b.SetBytes(residentSize)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := ch.CopySparseFileFast(ctx, targetFile, outFile); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("DifferentialOverlayOnSlot_64MB", func(b *testing.B) {
		slotFile := filepath.Join(dir, "slot-out")
		if err := ch.CopySparseFileFast(ctx, baseFile, slotFile); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(residentSize)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, _, err := ch.OverlaySparseRegionsRecorded(ctx, deltaSparseFile, slotFile); err != nil {
				b.Fatal(err)
			}
		}
	})
}

