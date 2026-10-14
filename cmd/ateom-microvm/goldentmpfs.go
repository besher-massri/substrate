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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// The tmpfs behind envTmpfsSlots. The worker container's own /dev/shm is the
// runtime's default 64 MiB per-pod tmpfs, far too small for a ~170 MiB golden
// base plus its slots, so ateom mounts a private tmpfs instead. The mount
// lives in the worker container's mount namespace, which the VMMs it launches
// share (they open memory-ranges through the slot symlinks), and goes away
// with it: nothing leaks onto the node. Its pages are charged to the worker
// pod like any other memory it uses.
const (
	// goldenTmpfsMountDir is where ateom mounts it (container-local).
	goldenTmpfsMountDir = "/run/ateom-golden-slots"
	// goldenTmpfsSize caps it. A pool needs ~(1 + goldenTmpfsSlotPoolCap)
	// times the golden base's populated size (~2.3 GiB for a 170 MiB base);
	// the cap reserves nothing.
	goldenTmpfsSize = 8 << 30
	// goldenTmpfsShmDir is the fallback when the mount fails: a directory on
	// /dev/shm, used only when /dev/shm has goldenTmpfsMinFree to spare.
	goldenTmpfsShmDir  = "/dev/shm/ate-golden-slots"
	goldenTmpfsMinFree = 2 << 30
)

// setupGoldenSlotTmpfs prepares the tmpfs directory for envTmpfsSlots and
// returns it, or "" (with a warning) when there is none to use, in which case
// the slot pool stays on the node disk as without the knob.
func setupGoldenSlotTmpfs(ctx context.Context) string {
	dir, huge, mountErr := mountGoldenSlotTmpfs(goldenTmpfsMountDir, goldenTmpfsSize)
	if mountErr == nil {
		slog.InfoContext(ctx, "Golden slot tmpfs mounted", slog.String("dir", dir),
			slog.Int64("size_bytes", goldenTmpfsSize), slog.String("huge", huge))
		return dir
	}
	if free, ok := tmpfsFreeBytes("/dev/shm"); ok && free >= goldenTmpfsMinFree {
		if err := os.MkdirAll(goldenTmpfsShmDir, 0o700); err == nil {
			slog.InfoContext(ctx, "Golden slot tmpfs: using /dev/shm", slog.String("dir", goldenTmpfsShmDir),
				slog.Int64("free_bytes", free), slog.Any("mount_err", mountErr))
			return goldenTmpfsShmDir
		}
	}
	slog.WarnContext(ctx, "Golden slot tmpfs unavailable; the golden slot pool stays on the node disk",
		slog.Any("mount_err", mountErr))
	return ""
}

// mountGoldenSlotTmpfs mounts a private tmpfs of size bytes on dir (unless one
// is already mounted there) and reports the huge= policy it got. It asks for
// huge=within_size first, so the base mirror and slots sit in 2 MiB pages
// where the kernel can provide them, and retries without on EINVAL (a kernel
// without shmem THP).
func mountGoldenSlotTmpfs(dir string, size int64) (mounted, huge string, err error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", err
	}
	if isTmpfsMountPoint(dir) {
		return dir, "existing", nil
	}
	const flags = unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC
	opts := fmt.Sprintf("size=%d,mode=0700", size)
	huge = "within_size"
	err = unix.Mount("ateom-golden-slots", dir, "tmpfs", flags, opts+",huge="+huge)
	if errors.Is(err, unix.EINVAL) {
		huge = "never"
		err = unix.Mount("ateom-golden-slots", dir, "tmpfs", flags, opts)
	}
	if err != nil {
		return "", "", fmt.Errorf("mount tmpfs on %s: %w", dir, err)
	}
	return dir, huge, nil
}

// isTmpfsMountPoint reports whether dir is the root of a tmpfs mount.
func isTmpfsMountPoint(dir string) bool {
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil || fs.Type != unix.TMPFS_MAGIC {
		return false
	}
	var st, parent unix.Stat_t
	if unix.Stat(dir, &st) != nil || unix.Stat(filepath.Dir(dir), &parent) != nil {
		return false
	}
	return st.Dev != parent.Dev
}

// tmpfsFreeBytes returns the space left on the tmpfs holding dir.
func tmpfsFreeBytes(dir string) (int64, bool) {
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil || fs.Type != unix.TMPFS_MAGIC {
		return 0, false
	}
	return int64(fs.Bavail) * fs.Bsize, true
}
