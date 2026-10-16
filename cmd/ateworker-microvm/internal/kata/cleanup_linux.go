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
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/nodepath"
)

// EnvFastSandboxCleanup controls whether CleanupSandboxState skips mountinfo
// and /proc scans when sandbox directories do not exist on disk or when the
// actor's cgroup leaf has already been killed. Enabled by default; set to
// "false" / "0" / "off" to force legacy unconditional mountinfo + /proc scans.
const (
	EnvFastSandboxCleanup       = "ATEWORKER_MICROVM_FAST_SANDBOX_CLEANUP"
	EnvLegacyFastSandboxCleanup = "ATEOM_MICROVM_FAST_SANDBOX_CLEANUP"
)

// FastSandboxCleanupEnabled reports whether fast sandbox state cleanup is
// enabled (default true unless explicitly disabled via env).
func FastSandboxCleanupEnabled() bool {
	for _, key := range []string{EnvFastSandboxCleanup, EnvLegacyFastSandboxCleanup} {
		if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
			switch strings.ToLower(raw) {
			case "0", "false", "no", "off":
				return false
			default:
				return true
			}
		}
	}
	return true
}

// CleanupOptions configures optional fast-paths in CleanupSandboxStateOpts.
type CleanupOptions struct {
	// SkipIfDirsAbsent checks whether any sandbox directory exists on disk
	// before scanning mounts or calling RemoveAll.
	SkipIfDirsAbsent bool
	// SkipOrphanScan skips scanning /proc for orphaned per-sandbox processes
	// (e.g. when cgroup.KillActorLeaf has already killed the actor cgroup leaf,
	// or on a pre-run sweep when no prior sandbox directory existed on disk).
	SkipOrphanScan bool
	// BaseDirs overrides the default sandbox state directories when non-empty
	// (used by unit tests and benchmarks).
	BaseDirs []string
}

// DefaultCleanupOptions returns the default CleanupOptions honoring
// ATEWORKER_MICROVM_FAST_SANDBOX_CLEANUP / ATEOM_MICROVM_FAST_SANDBOX_CLEANUP.
func DefaultCleanupOptions() CleanupOptions {
	fast := FastSandboxCleanupEnabled()
	return CleanupOptions{
		SkipIfDirsAbsent: fast,
	}
}

// CleanupSandboxState removes leftover host-side state for a sandbox id (the
// virtio-fs shared sandbox dir and the per-VM runtime dir), lazily unmounting
// anything still mounted underneath them first, and kills orphaned per-sandbox
// processes. ateworker owns the cloud-hypervisor boot directly (no kata shim, no
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
	CleanupSandboxStateOpts(ctx, id, DefaultCleanupOptions())
}

// CleanupSandboxStateOpts removes leftover host-side state for a sandbox id
// using the provided CleanupOptions.
func CleanupSandboxStateOpts(ctx context.Context, id string, opts CleanupOptions) {
	dirs := opts.BaseDirs
	if len(dirs) == 0 {
		dirs = []string{
			filepath.Join("/run/kata-containers/shared/sandboxes", id),
			filepath.Join(vcVMDir, id),
		}
	}

	activeDirs := dirs
	if opts.SkipIfDirsAbsent {
		activeDirs = make([]string, 0, len(dirs))
		for _, d := range dirs {
			if _, err := os.Stat(d); err == nil || !errors.Is(err, os.ErrNotExist) {
				activeDirs = append(activeDirs, d)
			}
		}
	}

	if len(activeDirs) > 0 {
		if opts.SkipIfDirsAbsent {
			// Fast path: use imagecache.UnmountAllUnder (which uses statx MNT_ID
			// before falling back to mountinfo) on each existing sandbox dir.
			for _, d := range activeDirs {
				if err := imagecache.UnmountAllUnder(d); err != nil {
					slog.WarnContext(ctx, "Leaving sandbox dir in place: mounts under it were not all detached",
						slog.String("dir", d), slog.Any("err", err))
					continue
				}
				if err := os.RemoveAll(d); err != nil {
					slog.WarnContext(ctx, "Failed to remove leftover sandbox dir",
						slog.String("dir", d), slog.Any("err", err))
				}
			}
		} else {
			unmountAndRemoveLegacy(ctx, activeDirs)
		}
	}

	if opts.SkipOrphanScan {
		return
	}
	if opts.SkipIfDirsAbsent && len(activeDirs) == 0 {
		// Neither sandbox state dir existed on disk, so no prior sandbox
		// execution left behind orphaned sockets/processes for this id.
		return
	}
	killOrphanedSandboxProcesses(ctx, id)
}

func unmountAndRemoveLegacy(ctx context.Context, dirs []string) {
	removable := make(map[string]bool, len(dirs))
	if b, err := os.ReadFile("/proc/self/mountinfo"); err != nil {
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

func killOrphanedSandboxProcesses(ctx context.Context, id string) {
	// Kill orphaned per-sandbox processes (cloud-hypervisor / virtiofsd) left by
	// a prior killed attempt: a canceled Create leaves the CH it spawned running
	// (reparented to the init) holding guest RAM and stale sockets. Matched strictly by
	// the sandbox id (an actor UUID) appearing in the cmdline, so nothing
	// unrelated can match.
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
// under the actor's own directory, and the sandbox id is the actor UID, so an
// executable under an actor's directory settles it: this sandbox's if it is
// id's directory, another sandbox's otherwise, whatever the command line
// says. Only a binary from elsewhere is judged by its name.
func isSandboxProcess(id, cmdline, exe string) bool {
	if !strings.Contains(cmdline, id) {
		return false
	}
	if strings.HasPrefix(exe, nodepath.ActorsDir+"/") {
		return strings.HasPrefix(exe, nodepath.ActorSandboxAssetsDir(id)+"/")
	}
	argv0 := strings.SplitN(cmdline, "\x00", 2)[0]
	return strings.Contains(argv0, "cloud-hypervisor") || strings.Contains(argv0, "virtiofsd")
}
