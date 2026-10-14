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
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/nodepath"
)

// CleanupOptions are opt-in shortcuts for CleanupSandboxStateOpts. The zero
// value is CleanupSandboxState's full sweep.
type CleanupOptions struct {
	// SkipIfDirsAbsent skips reading the mount table when none of the sandbox
	// dirs exists: nothing can be mounted under a path that does not exist in
	// this mount namespace (an unlinked mountpoint is detached everywhere), and
	// there is nothing to remove. Reading mountinfo costs time proportional to
	// every mount of every actor on the node.
	SkipIfDirsAbsent bool
	// SkipOrphanScan skips the /proc scan for orphaned VMM/virtiofsd
	// processes. Only for callers that have already killed every process the
	// sandbox can have left, e.g. by emptying the actor's cgroup leaf that all
	// of them were spawned into. The scan reads the cmdline of every process
	// in the namespace (three per running actor).
	SkipOrphanScan bool
}

// readMountinfo and killOrphans are swapped out by tests.
var (
	readMountinfo = func() ([]byte, error) { return os.ReadFile("/proc/self/mountinfo") }
	killOrphans   = killOrphanedSandboxProcesses
)

// CleanupSandboxState removes leftover host-side state for a sandbox id (the
// virtio-fs shared sandbox dir and the per-VM runtime dir), lazily unmounting
// anything still mounted underneath them first, and kills orphaned per-sandbox
// processes. ateom owns the cloud-hypervisor boot directly (no kata shim, no
// containerd), so a failed Create does not fully self-clean; the deterministic
// sandbox id (= actor name) then collides on the next attempt: "listen unix
// .../virtiofsd.sock: bind: address already in use", "Could not bind mount
// .../shared/sandboxes/<id>/mounts", "directory not empty". Calling this
// before each run gives a clean slate.
//
// Removal is gated on the unmounting: the shared tree holds bind mounts whose
// SOURCES belong to someone else (the durable-dir and CSI volume dirs are
// atelet's — see BindIntoShare), and a RemoveAll that walks into a live bind
// deletes the actor's data through it. So a dir is removed only once every
// mount beneath it is known detached; otherwise it is left in place for the
// next sweep (the stagers tolerate a leftover dir — they unmount stale binds
// and reuse mountpoints).
func CleanupSandboxState(ctx context.Context, id string) {
	CleanupSandboxStateOpts(ctx, id, CleanupOptions{})
}

// CleanupSandboxStateOpts is CleanupSandboxState with opt-in shortcuts.
func CleanupSandboxStateOpts(ctx context.Context, id string, opts CleanupOptions) {
	dirs := []string{
		filepath.Join("/run/kata-containers/shared/sandboxes", id),
		filepath.Join(vcVMDir, id),
	}
	if !opts.SkipIfDirsAbsent || !allAbsent(dirs) {
		removeSandboxDirs(ctx, dirs)
	}
	if !opts.SkipOrphanScan {
		killOrphans(ctx, id)
	}
}

// allAbsent reports whether none of paths exists. Anything other than a
// clean "does not exist" counts as present.
func allAbsent(paths []string) bool {
	for _, p := range paths {
		if _, err := os.Lstat(p); !errors.Is(err, fs.ErrNotExist) {
			return false
		}
	}
	return true
}

// removeSandboxDirs lazily unmounts everything mounted under dirs, then
// removes each dir whose mounts all detached.
func removeSandboxDirs(ctx context.Context, dirs []string) {
	removable := make(map[string]bool, len(dirs))
	if b, err := readMountinfo(); err != nil {
		// Without the mount table there is no telling what is still mounted
		// under the dirs, so none of them is provably safe to remove.
		slog.WarnContext(ctx, "Cannot read mountinfo; leaving sandbox dirs in place",
			slog.Any("err", err))
	} else {
		for _, d := range dirs {
			removable[d] = true
		}
		type mount struct{ mp, dir string }
		var mounts []mount
		for _, line := range strings.Split(string(b), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 5 {
				continue
			}
			mp := fields[4] // mount point
			for _, d := range dirs {
				if mp == d || strings.HasPrefix(mp, d+"/") {
					mounts = append(mounts, mount{mp: mp, dir: d})
					break
				}
			}
		}
		// Deepest paths first so child mounts unmount before their parents.
		sort.Slice(mounts, func(i, j int) bool { return len(mounts[i].mp) > len(mounts[j].mp) })
		for _, m := range mounts {
			if err := unix.Unmount(m.mp, unix.MNT_DETACH); err != nil {
				slog.WarnContext(ctx, "Failed to unmount leftover sandbox mount",
					slog.String("mount", m.mp), slog.Any("err", err))
				removable[m.dir] = false
			}
		}
	}
	for _, d := range dirs {
		if !removable[d] {
			slog.WarnContext(ctx, "Leaving sandbox dir in place: mounts under it were not all detached",
				slog.String("dir", d))
			continue
		}
		if err := os.RemoveAll(d); err != nil {
			slog.WarnContext(ctx, "Failed to remove leftover sandbox dir",
				slog.String("dir", d), slog.Any("err", err))
		}
	}
}

// killOrphanedSandboxProcesses kills orphaned per-sandbox processes
// (cloud-hypervisor / virtiofsd) left by a prior killed attempt: a canceled
// Create leaves the CH it spawned running (reparented to us) holding guest RAM
// and stale sockets. Matched strictly by the sandbox id (an actor UUID)
// appearing in the cmdline, so nothing unrelated can match.
func killOrphanedSandboxProcesses(ctx context.Context, id string) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return
	}
	for _, e := range entries {
		pid, perr := strconv.Atoi(e.Name())
		if perr != nil || pid == os.Getpid() {
			continue
		}
		cmdline, rerr := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if rerr != nil {
			continue
		}
		exe, _ := os.Readlink(filepath.Join("/proc", e.Name(), "exe"))
		if isSandboxProcess(id, string(cmdline), exe) {
			if err := unix.Kill(pid, unix.SIGKILL); err != nil {
				slog.WarnContext(ctx, "Failed to kill orphaned sandbox process",
					slog.Int("pid", pid), slog.String("exe", exe), slog.Any("err", err))
			}
		}
	}
}

// isSandboxProcess reports whether a process is a VMM or virtiofsd serving the
// sandbox id. Both carry id in their socket paths. atelet stages their binaries
// under content-addressed names, so the executable's location identifies them
// rather than its name.
func isSandboxProcess(id, cmdline, exe string) bool {
	if !strings.Contains(cmdline, id) {
		return false
	}
	if strings.HasPrefix(exe, nodepath.StaticFilesDir+"/") {
		return true
	}
	argv0 := strings.SplitN(cmdline, "\x00", 2)[0]
	return strings.Contains(argv0, "cloud-hypervisor") || strings.Contains(argv0, "virtiofsd")
}
