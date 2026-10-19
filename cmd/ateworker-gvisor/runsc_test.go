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
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

var testActorDirs = &ateworkerpb.ActorDirs{
	RootDir:      "/node/actors/test-actor-123",
	OciBundleDir: "/node/actors/test-actor-123/bundle",
}

// writeSpec builds a workload container's spec from the request and the pause
// container's from ateworker's own description.
func TestWriteSpec(t *testing.T) {
	root := t.TempDir()
	actorDirs := &ateworkerpb.ActorDirs{RootDir: root, OciBundleDir: filepath.Join(root, "bundles")}
	r := &runsc{
		actorUID:   "uid-a",
		actorDirs:  actorDirs,
		containers: []*ateworkerpb.Container{{Name: "app", ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app", "serve"}}}},
	}
	for name, wantArgs := range map[string][]string{
		"app":                  {"/app", "serve"},
		ocispec.PauseContainer: {"/pause"},
	} {
		bundle := ociBundlePath(actorDirs, name)
		if err := os.MkdirAll(bundle, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := r.writeSpec(name); err != nil {
			t.Fatalf("writeSpec(%q) = %v", name, err)
		}
		b, err := os.ReadFile(filepath.Join(bundle, "config.json"))
		if err != nil {
			t.Fatal(err)
		}
		var spec specs.Spec
		if err := json.Unmarshal(b, &spec); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(spec.Process.Args, wantArgs) {
			t.Errorf("%s args = %v, want %v", name, spec.Process.Args, wantArgs)
		}
		if got := spec.Annotations["io.kubernetes.cri.container-name"]; got != name {
			t.Errorf("%s container-name annotation = %q, want the spec shaped for gVisor", name, got)
		}
		if name == ocispec.PauseContainer && len(spec.Process.Capabilities.Bounding) != 0 {
			t.Errorf("pause capabilities = %v, want none", spec.Process.Capabilities.Bounding)
		}
	}
	if err := r.writeSpec("missing"); err == nil {
		t.Error("writeSpec() = nil for a container outside the workload spec")
	}
}

func mustFlagStore(t *testing.T) *flagStore {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "flags.json")
	const testFlagsJSON = `{
  "globalFlags": [
    "-global1",
    "-global2"
  ],
  "commands": {
    "restore": {
      "globalFlags": ["-restore-global"],
      "subcommandFlags": ["-restore-sub1", "-restore-sub2"]
    },
    "start": {
      "globalFlags": ["-start-global"]
    },
    "list": {
      "subcommandFlags": ["-list-sub"]
    }
  }
}`
	if err := os.WriteFile(cfgPath, []byte(testFlagsJSON), 0o600); err != nil {
		t.Fatalf("failed to write test flags.json: %v", err)
	}
	store, err := newFlagStore(t.Context(), cfgPath)
	if err != nil {
		t.Fatalf("newFlagStore(%s) failed: %v", cfgPath, err)
	}
	return store
}

func TestBuildArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}

	for _, tc := range []struct {
		name          string
		command       string
		containerName string
		extraArgs     []string
		want          []string
	}{
		{
			name:          "kill",
			command:       "kill",
			containerName: "",
			extraArgs:     []string{"my-container", "SIGTERM"},
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"kill",
				"my-container",
				"SIGTERM",
			},
		},
		{
			name:          "wait",
			command:       "wait",
			containerName: "my-container",
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"wait",
				"my-container",
			},
		},
		{
			name:          "wait -restore",
			command:       "wait",
			containerName: "my-container",
			extraArgs:     []string{"-restore"},
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"wait",
				"-restore",
				"my-container",
			},
		},
		{
			name:          "wait -fsrestore",
			command:       "wait",
			containerName: "my-container",
			extraArgs:     []string{"-fsrestore"},
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"wait",
				"-fsrestore",
				"my-container",
			},
		},
		{
			name:          "list",
			command:       "list",
			containerName: "",
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"list",
				"-list-sub",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := r.buildArgs(tc.command, tc.containerName, tc.extraArgs...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("buildArgs(%q, %q, %v) = %v, want %v", tc.command, tc.containerName, tc.extraArgs, got, tc.want)
			}
		})
	}
}

func TestCheckpointArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}
	checkpointDir := "/node/actors/test-actor-123/checkpoints/snap-1"

	got := r.checkpointArgs(ocispec.PauseContainer, checkpointDir, []string{"rootfs/=app:/", "fs/=app:/var/data"})
	want := []string{
		"-global1",
		"-global2",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"checkpoint",
		"-image-path", checkpointDir,
		"-fs-checkpoint-paths", "rootfs/=app:/",
		"-fs-checkpoint-paths", "fs/=app:/var/data",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("checkpointArgs() = %v, want %v", got, want)
	}
}

func TestFsCheckpointArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}
	checkpointDir := "/node/actors/test-actor-123/checkpoints/snap-1"

	got := r.fsCheckpointArgs(ocispec.PauseContainer, checkpointDir, []string{"rootfs/=app:/", "fs/=app:/var/data"})
	want := []string{
		"-global1",
		"-global2",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"fscheckpoint",
		"-image-path", checkpointDir,
		"-path", "rootfs/=app:/",
		"-path", "fs/=app:/var/data",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("fsCheckpointArgs() = %v, want %v", got, want)
	}
}

func TestRestoreArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}
	checkpointDir := "/node/actors/test-actor-123/checkpoints/snap-1"

	got := r.restoreArgs(ocispec.PauseContainer, checkpointDir)
	want := []string{
		"-global1",
		"-global2",
		"-restore-global",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"restore",
		"-restore-sub1",
		"-restore-sub2",
		"-bundle", "/node/actors/test-actor-123/bundle/" + ocispec.PauseContainer,
		"-image-path", checkpointDir,
		"-pid-file", "/node/actors/test-actor-123/pidfiles/" + ocispec.PauseContainer + ".pid",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("restoreArgs() = %v, want %v", got, want)
	}
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

func TestProductionInternalFlagsOverlay2Memory(t *testing.T) {
	store, err := newFlagStore(t.Context(), filepath.Join("kodata", "internal_flags.json"))
	if err != nil {
		t.Fatalf("newFlagStore(kodata/internal_flags.json) failed: %v", err)
	}
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     store,
	}

	t.Run("default root:memory on create start restore", func(t *testing.T) {
		if got := overlay2Flag(); got != "--overlay2=root:memory" {
			t.Fatalf("overlay2Flag() = %q, want \"--overlay2=root:memory\"", got)
		}
		for _, cmd := range []string{"create", "start", "restore"} {
			args := r.buildArgs(cmd, "glutton")
			if !slices.Contains(args, "--overlay2=root:memory") {
				t.Errorf("buildArgs(%q) = %v, want --overlay2=root:memory", cmd, args)
			}
		}
		for _, cmd := range []string{"checkpoint", "delete", "list", "wait"} {
			args := r.buildArgs(cmd, "glutton")
			if slices.ContainsFunc(args, isOverlay2Arg) {
				t.Errorf("buildArgs(%q) = %v, did not expect --overlay2 flag", cmd, args)
			}
		}
	})

	t.Run("ATEWORKER_GVISOR_OVERLAY2 override", func(t *testing.T) {
		t.Setenv("ATEWORKER_GVISOR_OVERLAY2", "all:memory")
		if got := overlay2Flag(); got != "--overlay2=all:memory" {
			t.Fatalf("overlay2Flag() = %q, want \"--overlay2=all:memory\"", got)
		}
		for _, cmd := range []string{"create", "start", "restore"} {
			args := r.buildArgs(cmd, "glutton")
			if !slices.Contains(args, "--overlay2=all:memory") {
				t.Errorf("buildArgs(%q) = %v, want --overlay2=all:memory", cmd, args)
			}
			if slices.Contains(args, "--overlay2=root:memory") {
				t.Errorf("buildArgs(%q) = %v, should have replaced default --overlay2=root:memory", cmd, args)
			}
		}
	})

	t.Run("legacy ATEOM_GVISOR_OVERLAY2 fallback", func(t *testing.T) {
		t.Setenv("ATEOM_GVISOR_OVERLAY2", "root:self")
		if got := overlay2Flag(); got != "--overlay2=root:self" {
			t.Fatalf("overlay2Flag() = %q, want \"--overlay2=root:self\"", got)
		}
		args := r.restoreArgs("glutton", "/tmp/ckpt")
		if !slices.Contains(args, "--overlay2=root:self") {
			t.Errorf("restoreArgs() = %v, want --overlay2=root:self", args)
		}
	})

	t.Run("disable via none", func(t *testing.T) {
		t.Setenv("ATEWORKER_GVISOR_OVERLAY2", "none")
		if got := overlay2Flag(); got != "" {
			t.Fatalf("overlay2Flag() = %q, want empty string when disabled", got)
		}
		for _, cmd := range []string{"create", "start", "restore"} {
			args := r.buildArgs(cmd, "glutton")
			if slices.ContainsFunc(args, isOverlay2Arg) {
				t.Errorf("buildArgs(%q) with ATEWORKER_GVISOR_OVERLAY2=none = %v, want no --overlay2 flag", cmd, args)
			}
		}
	})
}

type mockContainerWaiter struct {
	waitCalls atomic.Int32
	waitFn    func(ctx context.Context, name string) error
}

func (m *mockContainerWaiter) cmdWait(ctx context.Context, name string) error {
	m.waitCalls.Add(1)
	return m.waitFn(ctx, name)
}

func TestVerifyContainersRunningAndWakeupLiveness(t *testing.T) {
	containers := []*ateworkerpb.Container{
		{Name: "init"},
		{Name: "glutton"},
	}

	t.Run("all containers stay running across verification window", func(t *testing.T) {
		waiter := &mockContainerWaiter{
			waitFn: func(ctx context.Context, _ string) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
		if err := verifyContainersRunning(t.Context(), waiter, containers, 20*time.Millisecond); err != nil {
			t.Fatalf("verifyContainersRunning() = %v, want nil", err)
		}
		if got := waiter.waitCalls.Load(); got != 2 {
			t.Errorf("waitCalls = %d, want 2 (init + glutton)", got)
		}
	})

	t.Run("container exiting early fails fast", func(t *testing.T) {
		waiter := &mockContainerWaiter{
			waitFn: func(ctx context.Context, name string) error {
				if name == "glutton" {
					return nil // exited immediately
				}
				<-ctx.Done()
				return ctx.Err()
			},
		}
		err := verifyContainersRunning(t.Context(), waiter, containers, 250*time.Millisecond)
		if err == nil || !strings.Contains(err.Error(), `application container "glutton" exited`) {
			t.Fatalf("verifyContainersRunning() = %v, want error mentioning glutton exited", err)
		}
	})

	t.Run("fast wakeup probe (<250ms) spawns zero wait subprocesses", func(t *testing.T) {
		waiter := &mockContainerWaiter{
			waitFn: func(ctx context.Context, _ string) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
		err := waitForWakeupProbesWithLiveness(t.Context(), waiter, containers, wakeupLivenessCheckDelay, func(context.Context) error {
			return nil
		})
		if err != nil {
			t.Fatalf("waitForWakeupProbesWithLiveness() = %v, want nil", err)
		}
		if got := waiter.waitCalls.Load(); got != 0 {
			t.Errorf("waitCalls = %d, want 0 on fast wakeup probe", got)
		}
	})

	t.Run("dead container cancels slow wakeup probe after liveness delay", func(t *testing.T) {
		waiter := &mockContainerWaiter{
			waitFn: func(_ context.Context, name string) error {
				if name == "init" {
					return errors.New("container not running")
				}
				return nil
			},
		}
		start := time.Now()
		err := waitForWakeupProbesWithLiveness(t.Context(), waiter, containers, 15*time.Millisecond, func(probeCtx context.Context) error {
			<-probeCtx.Done()
			return probeCtx.Err()
		})
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("waitForWakeupProbesWithLiveness() = nil, want error when container exits")
		}
		if elapsed > 2*time.Second {
			t.Errorf("elapsed = %v, expected fast cancellation", elapsed)
		}
	})
}

func TestVerifyMemoryCheckpointPages(t *testing.T) {
	t.Run("rejects 86016-byte pause-only pages.img", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "pages.img"), make([]byte, 86016), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyMemoryCheckpointPages(dir, 1); err == nil {
			t.Fatal("verifyMemoryCheckpointPages succeeded on 86,016-byte pause-only pages.img, want error")
		}
	})

	t.Run("rejects tiny pause-only pages.img.zstd", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "pages.img.zstd"), make([]byte, 8192), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyMemoryCheckpointPages(dir, 1); err == nil {
			t.Fatal("verifyMemoryCheckpointPages succeeded on 8,192-byte pause-only pages.img.zstd, want error")
		}
	})

	t.Run("accepts normal app pages.img and pages.img.zstd", func(t *testing.T) {
		dirRaw := t.TempDir()
		if err := os.WriteFile(filepath.Join(dirRaw, "pages.img"), make([]byte, 256*1024), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyMemoryCheckpointPages(dirRaw, 1); err != nil {
			t.Fatalf("verifyMemoryCheckpointPages(256KiB pages.img) = %v, want nil", err)
		}

		dirZstd := t.TempDir()
		if err := os.WriteFile(filepath.Join(dirZstd, "pages.img.zstd"), make([]byte, 128*1024), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := verifyMemoryCheckpointPages(dirZstd, 1); err != nil {
			t.Fatalf("verifyMemoryCheckpointPages(128KiB pages.img.zstd) = %v, want nil", err)
		}
	})
}

func BenchmarkBuildArgsOverlay2(b *testing.B) {
	store, err := newFlagStore(b.Context(), filepath.Join("kodata", "internal_flags.json"))
	if err != nil {
		b.Fatalf("newFlagStore: %v", err)
	}
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "bench-actor-123",
		actorDirs: testActorDirs,
		flags:     store,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = r.restoreArgs("glutton", "/node/actors/bench-actor-123/restore")
	}
}

func BenchmarkWaitForWakeupProbesWithLiveness_FastPath(b *testing.B) {
	containers := []*ateworkerpb.Container{{Name: "init"}, {Name: "glutton"}}
	waiter := &mockContainerWaiter{
		waitFn: func(ctx context.Context, _ string) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := waitForWakeupProbesWithLiveness(ctx, waiter, containers, wakeupLivenessCheckDelay, func(context.Context) error {
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVerifyMemoryCheckpointPages(b *testing.B) {
	dir := b.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pages.img"), make([]byte, 256*1024), 0o600); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := verifyMemoryCheckpointPages(dir, 1); err != nil {
			b.Fatal(err)
		}
	}
}

