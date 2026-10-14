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

	"github.com/agent-substrate/substrate/internal/nodepath"
)

func TestIsSandboxProcess(t *testing.T) {
	const id = "0e16c0fa-5c06-4b48-8742-4068635c08c1"
	staged := nodepath.StaticFilesDir + "/runsc-ed81e9b2"
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
		{"another sandbox", argv(staged, "--api-socket", "/run/vc/vm/other/clh-api.sock"), staged, false},
		{"unrelated process naming the id", argv("/bin/sh", "-c", "echo "+id), "/bin/busybox", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSandboxProcess(id, tc.cmdline, tc.exe); got != tc.want {
				t.Errorf("isSandboxProcess() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAllAbsent(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing")
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		paths []string
		want  bool
	}{
		{"none exists", []string{missing, filepath.Join(missing, "child")}, true},
		{"one exists", []string{missing, dir}, false},
		// ENOTDIR is not a clean "does not exist": treat it as present.
		{"under a file", []string{filepath.Join(file, "child")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := allAbsent(tc.paths); got != tc.want {
				t.Errorf("allAbsent(%q) = %v, want %v", tc.paths, got, tc.want)
			}
		})
	}
}

// The shortcuts only drop the mount-table read when no sandbox dir exists and
// the orphan scan when asked to; the zero options keep the full sweep.
func TestCleanupSandboxStateOptsShortcuts(t *testing.T) {
	const id = "4f3c3a8e-2b1d-4c55-9a0e-cleanup-test" // no such sandbox
	oldRead, oldKill := readMountinfo, killOrphans
	t.Cleanup(func() { readMountinfo, killOrphans = oldRead, oldKill })
	var reads, scans int
	readMountinfo = func() ([]byte, error) { reads++; return nil, nil }
	killOrphans = func(context.Context, string) { scans++ }
	for _, tc := range []struct {
		name                string
		opts                CleanupOptions
		wantReads, wantScan int
	}{
		{"full sweep", CleanupOptions{}, 1, 1},
		{"dirs absent", CleanupOptions{SkipIfDirsAbsent: true}, 0, 1},
		{"no orphan scan", CleanupOptions{SkipOrphanScan: true}, 1, 0},
		{"both", CleanupOptions{SkipIfDirsAbsent: true, SkipOrphanScan: true}, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, scans = 0, 0
			CleanupSandboxStateOpts(context.Background(), id, tc.opts)
			if reads != tc.wantReads || scans != tc.wantScan {
				t.Errorf("mountinfo reads = %d, orphan scans = %d; want %d, %d", reads, scans, tc.wantReads, tc.wantScan)
			}
		})
	}
}
