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

package kata

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/nodepath"
)

func TestIsSandboxProcess(t *testing.T) {
	const id = "0e16c0fa-5c06-4b48-8742-4068635c08c1"
	// atelet stages each actor's VMM under that actor's own directory.
	staged := nodepath.ActorsDir + "/" + id + "/sandbox-assets/cloud-hypervisor"
	const other = "7d3b1a9c-2e4f-4a6b-9c8d-0f1e2d3c4b5a"
	otherStaged := nodepath.ActorsDir + "/" + other + "/sandbox-assets/cloud-hypervisor"
	argv := func(args ...string) string { return strings.Join(args, "\x00") + "\x00" }
	for _, tc := range []struct {
		name    string
		cmdline string
		exe     string
		want    bool
	}{
		{"staged VMM", argv(staged, "--api-socket", "/run/vc/vm/"+id+"/clh-api.sock"), staged, true},
		{"staged virtiofsd", argv(staged, "--socket-path=/run/vc/vm/"+id+"/virtiofsd.sock"), staged, true},
		{"VMM by name", argv("cloud-hypervisor", "--api-socket", "/run/vc/vm/"+id+"/clh-api.sock"), "/usr/bin/cloud-hypervisor", true},
		{"another sandbox", argv(otherStaged, "--api-socket", "/run/vc/vm/"+other+"/clh-api.sock"), otherStaged, false},
		{"another sandbox's VMM naming the id", argv(otherStaged, "--api-socket", "/run/vc/vm/"+other+"/clh-api.sock", "--log", "/tmp/"+id), otherStaged, false},
		{"unrelated process naming the id", argv("/bin/sh", "-c", "echo "+id), "/bin/busybox", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSandboxProcess(id, tc.cmdline, tc.exe); got != tc.want {
				t.Errorf("isSandboxProcess() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCleanupSandboxStateOpts_SkipWhenDirsAbsentAndCgroupKilled(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	sharedDir := filepath.Join(tmp, "shared", "actor-clean")
	vmDir := filepath.Join(tmp, "vm", "actor-clean")

	// 1. Neither directory exists: fast cleanup is a two-stat no-op.
	CleanupSandboxStateOpts(ctx, "actor-clean", CleanupOptions{
		SkipIfDirsAbsent: true,
		SkipOrphanScan:   true,
		BaseDirs:         []string{sharedDir, vmDir},
	})

	// 2. Create leftover directories and files; verify fast cleanup unmounts and removes them.
	if err := os.MkdirAll(filepath.Join(sharedDir, "mounts"), 0o755); err != nil {
		t.Fatalf("MkdirAll sharedDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sharedDir, "mounts", "stale.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.MkdirAll(vmDir, 0o755); err != nil {
		t.Fatalf("MkdirAll vmDir: %v", err)
	}

	CleanupSandboxStateOpts(ctx, "actor-clean", CleanupOptions{
		SkipIfDirsAbsent: true,
		SkipOrphanScan:   true,
		BaseDirs:         []string{sharedDir, vmDir},
	})

	for _, d := range []string{sharedDir, vmDir} {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			t.Errorf("expected %q to be removed, stat err = %v", d, err)
		}
	}

	// 3. Verify env toggle controls DefaultCleanupOptions.
	t.Setenv(EnvFastSandboxCleanup, "false")
	if FastSandboxCleanupEnabled() {
		t.Errorf("FastSandboxCleanupEnabled() = true when %s=false", EnvFastSandboxCleanup)
	}
	t.Setenv(EnvFastSandboxCleanup, "true")
	if !FastSandboxCleanupEnabled() {
		t.Errorf("FastSandboxCleanupEnabled() = false when %s=true", EnvFastSandboxCleanup)
	}
}

func BenchmarkCleanupSandboxState_FastVsLegacy(b *testing.B) {
	ctx := context.Background()
	tmp := b.TempDir()
	missingShared := filepath.Join(tmp, "shared", "bench-actor")
	missingVM := filepath.Join(tmp, "vm", "bench-actor")
	baseDirs := []string{missingShared, missingVM}

	b.Run("fast_skip_absent_and_cgroup_killed", func(b *testing.B) {
		opts := CleanupOptions{
			SkipIfDirsAbsent: true,
			SkipOrphanScan:   true,
			BaseDirs:         baseDirs,
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			CleanupSandboxStateOpts(ctx, "bench-actor", opts)
		}
	})

	b.Run("legacy_mountinfo_and_proc_scan", func(b *testing.B) {
		opts := CleanupOptions{
			SkipIfDirsAbsent: false,
			SkipOrphanScan:   false,
			BaseDirs:         baseDirs,
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			CleanupSandboxStateOpts(ctx, "bench-actor", opts)
		}
	})
}
