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
	"strconv"
	"strings"

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
//
// zeroCopy (envZeroCopyRestore) sizes the tmpfs to the node's memory instead
// of goldenTmpfsSize, since restored guests' RAM then lives on it: a full
// tmpfs turns the guest's next page fault into a SIGBUS that kills its VMM,
// so the pod's memory limit must be what binds, as for a memfd. It reports
// whether the tmpfs can hold guest RAM: ateom's own mount, of that size, with
// 2 MiB pages (on 4 KiB host pages every guest page touch is an EPT fault,
// which made a restored guest's wakeup ~75 ms slower on a nested-virt bench).
func setupGoldenSlotTmpfs(ctx context.Context, zeroCopy bool) (dir string, guestRAMReady bool) {
	size := int64(goldenTmpfsSize)
	if zeroCopy {
		mt, err := memTotalBytes()
		if err != nil {
			slog.WarnContext(ctx, "Could not read the node's memory size; zero-copy restores stay off", slog.Any("err", err))
			zeroCopy = false
		} else {
			size = mt
		}
	}
	dir, huge, mountErr := mountGoldenSlotTmpfs(goldenTmpfsMountDir, size, zeroCopy)
	if mountErr == nil {
		slog.InfoContext(ctx, "Golden slot tmpfs mounted", slog.String("dir", dir),
			slog.Int64("size_bytes", size), slog.String("huge", huge))
		return dir, zeroCopy && huge == "within_size"
	}
	if free, ok := tmpfsFreeBytes("/dev/shm"); ok && free >= goldenTmpfsMinFree {
		if err := os.MkdirAll(goldenTmpfsShmDir, 0o700); err == nil {
			slog.InfoContext(ctx, "Golden slot tmpfs: using /dev/shm", slog.String("dir", goldenTmpfsShmDir),
				slog.Int64("free_bytes", free), slog.Any("mount_err", mountErr))
			return goldenTmpfsShmDir, false
		}
	}
	slog.WarnContext(ctx, "Golden slot tmpfs unavailable; the golden slot pool stays on the node disk",
		slog.Any("mount_err", mountErr))
	return "", false
}

// mountGoldenSlotTmpfs mounts a private tmpfs of size bytes on dir (unless one
// is already mounted there) and reports the huge= policy it got. It asks for
// huge=within_size first, so the base mirror and slots sit in 2 MiB pages
// where the kernel can provide them, and retries without on EINVAL (a kernel
// without shmem THP). With resize, a tmpfs already mounted on dir is
// remounted to size (and huge=within_size) instead of taken as it is.
func mountGoldenSlotTmpfs(dir string, size int64, resize bool) (mounted, huge string, err error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", err
	}
	const flags = unix.MS_NOSUID | unix.MS_NODEV | unix.MS_NOEXEC
	var remount uintptr
	if isTmpfsMountPoint(dir) {
		if !resize {
			return dir, "existing", nil
		}
		remount = unix.MS_REMOUNT
	}
	opts := fmt.Sprintf("size=%d,mode=0700", size)
	huge = "within_size"
	err = unix.Mount("ateom-golden-slots", dir, "tmpfs", flags|remount, opts+",huge="+huge)
	if errors.Is(err, unix.EINVAL) {
		huge = "never"
		err = unix.Mount("ateom-golden-slots", dir, "tmpfs", flags|remount, opts)
	}
	if err != nil {
		return "", "", fmt.Errorf("mount tmpfs on %s: %w", dir, err)
	}
	return dir, huge, nil
}

// memTotalBytes is MemTotal from /proc/meminfo, in bytes.
func memTotalBytes() (int64, error) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	return parseMemTotal(string(b))
}

func parseMemTotal(meminfo string) (int64, error) {
	for _, line := range strings.Split(meminfo, "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == "MemTotal:" {
			kb, err := strconv.ParseInt(f[1], 10, 64)
			if err != nil || kb <= 0 || (len(f) > 2 && f[2] != "kB") {
				return 0, fmt.Errorf("bad MemTotal line %q", line)
			}
			return kb << 10, nil
		}
	}
	return 0, errors.New("no MemTotal in /proc/meminfo")
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
