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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
)

const (
	// goldenTmpfsMountDir is the container-local private tmpfs mount point for
	// golden base mirrors and pre-staged memory-ranges slots.
	goldenTmpfsMountDir = "/run/ateworker-golden-slots"
	// goldenTmpfsSize caps the private tmpfs when zero-copy restore is not enabled.
	goldenTmpfsSize = 8 << 30
	// goldenTmpfsShmDir is the fallback directory on /dev/shm when mounting a
	// private tmpfs fails and /dev/shm has at least goldenTmpfsMinFree free.
	goldenTmpfsShmDir  = "/dev/shm/ate-microvm-slots"
	goldenTmpfsMinFree = 2 << 30

	goldenSlotPoolCap           = 8
	goldenSlotPrewarmCount      = 6
	goldenTmpfsSlotPoolCap      = 12
	goldenTmpfsSlotPrewarmCount = 8
)

// Fallback reasons logged when zero-copy restore is enabled on the worker but a
// specific restore falls back to copying guest RAM.
const (
	zeroCopyNoGoldenDelta   = "no_golden_delta"
	zeroCopyMergedOnDisk    = "merged_on_disk"
	zeroCopyDiskSlot        = "disk_slot"
	zeroCopyNoIdleSlot      = "no_idle_slot"
	zeroCopyPreviousFailure = "previous_attempt_failed"
	zeroCopyLayout          = "layout"
	zeroCopyRewriteFailed   = "rewrite_failed"
)

func tmpfsSlotsEnabled() bool {
	return microvmEnvBool("TMPFS_SLOTS") || microvmEnvBool("ZERO_COPY_RESTORE")
}

func zeroCopyRestoreEnabled() bool {
	return microvmEnvBool("ZERO_COPY_RESTORE")
}

func slotPoolDisabled() bool {
	return microvmEnvBool("DISABLE_GOLDEN_SLOT_POOL")
}

// goldenTmpfsRoot is the tmpfs directory holding the golden base mirrors and
// slot pools when tmpfs slots are active; "" keeps them on the node disk next
// to the golden-base cache.
var goldenTmpfsRoot string

// linkFile is os.Link; tests override it to simulate cross-filesystem links.
var linkFile = os.Link

var (
	goldenPools          sync.Map // poolKey(baseFile, tmpfsRoot) -> *baseSlotPool
	goldenPrewarmStarted sync.Map // baseFile -> struct{}
	zeroCopyBackoff      sync.Map // actorUID -> struct{}
)

// setupGoldenSlotTmpfs prepares the tmpfs directory for tmpfsSlotsEnabled() and
// returns it, or "" when no usable tmpfs is available.
func setupGoldenSlotTmpfs(ctx context.Context, zeroCopy bool) (dir string, guestRAMReady bool) {
	size := int64(goldenTmpfsSize)
	if zeroCopy {
		mt, err := memTotalBytes()
		if err != nil {
			slog.WarnContext(ctx, "Could not read node memory size; zero-copy restores stay off", slog.Any("err", err))
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
	slog.WarnContext(ctx, "Golden slot tmpfs unavailable; golden slot pool stays on node disk",
		slog.Any("mount_err", mountErr))
	return "", false
}

// mountGoldenSlotTmpfs mounts a private tmpfs of size bytes on dir (unless one
// is already mounted there) and reports the huge= policy it got.
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
	err = unix.Mount("ateworker-golden-slots", dir, "tmpfs", flags|remount, opts+",huge="+huge)
	if errors.Is(err, unix.EINVAL) {
		huge = "never"
		err = unix.Mount("ateworker-golden-slots", dir, "tmpfs", flags|remount, opts)
	}
	if err != nil {
		return "", "", fmt.Errorf("mount tmpfs on %s: %w", dir, err)
	}
	return dir, huge, nil
}

// memTotalBytes returns MemTotal from /proc/meminfo in bytes.
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

// tmpfsFreeBytes returns the available space on the tmpfs holding dir.
func tmpfsFreeBytes(dir string) (int64, bool) {
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil || fs.Type != unix.TMPFS_MAGIC {
		return 0, false
	}
	return int64(fs.Bavail) * fs.Bsize, true
}

func sharesInode(path string) bool {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return false
	}
	return st.Nlink > 1
}

type goldenSlot struct {
	path string
}

type baseSlotPool struct {
	srcFile  string
	baseFile string
	slotDir  string
	inTmpfs  bool
	prewarm  int
	idle     chan *goldenSlot
	nextID   atomic.Uint64

	stageOnce sync.Once
	stageErr  error
	staged    atomic.Bool
}

func poolKey(baseFile, tmpfsRoot string) string {
	return tmpfsRoot + "\x00" + baseFile
}

func newBaseSlotPool(baseFile, tmpfsRoot string) *baseSlotPool {
	pid := os.Getpid()
	p := &baseSlotPool{
		srcFile:  baseFile,
		baseFile: baseFile,
		slotDir:  filepath.Join(filepath.Dir(baseFile), fmt.Sprintf("slots-%d", pid)),
		prewarm:  goldenSlotPrewarmCount,
	}
	capacity := goldenSlotPoolCap
	if tmpfsRoot != "" {
		sum := sha256.Sum256([]byte(baseFile))
		dir := filepath.Join(tmpfsRoot, fmt.Sprintf("%s-%x", filepath.Base(filepath.Dir(baseFile)), sum[:4]))
		p.baseFile = filepath.Join(dir, "base-memory-ranges")
		p.slotDir = filepath.Join(dir, fmt.Sprintf("slots-%d", pid))
		p.inTmpfs = true
		p.prewarm = goldenTmpfsSlotPrewarmCount
		capacity = goldenTmpfsSlotPoolCap
	}
	p.idle = make(chan *goldenSlot, capacity)
	return p
}

func getOrCreateBaseSlotPool(baseFile, tmpfsRoot string) *baseSlotPool {
	key := poolKey(baseFile, tmpfsRoot)
	if v, ok := goldenPools.Load(key); ok {
		return v.(*baseSlotPool)
	}
	pool := newBaseSlotPool(baseFile, tmpfsRoot)
	actual, loaded := goldenPools.LoadOrStore(key, pool)
	if loaded {
		return actual.(*baseSlotPool)
	}
	if !pool.inTmpfs {
		baseDir := filepath.Dir(baseFile)
		if entries, err := os.ReadDir(baseDir); err == nil {
			for _, e := range entries {
				if !e.IsDir() || !strings.HasPrefix(e.Name(), "slots-") {
					continue
				}
				pidStr := strings.TrimPrefix(e.Name(), "slots-")
				pid, perr := strconv.Atoi(pidStr)
				if perr != nil || pid == os.Getpid() {
					continue
				}
				if kerr := unix.Kill(pid, 0); errors.Is(kerr, unix.ESRCH) {
					_ = os.RemoveAll(filepath.Join(baseDir, e.Name()))
				}
			}
		}
	}
	_ = os.MkdirAll(pool.slotDir, 0o700)
	return pool
}

func (p *baseSlotPool) ready(ctx context.Context) bool {
	if !p.inTmpfs {
		return true
	}
	p.stageOnce.Do(func() {
		t := time.Now()
		if err := mirrorGoldenBase(context.WithoutCancel(ctx), p.srcFile, p.baseFile); err != nil {
			p.stageErr = err
			_ = os.RemoveAll(filepath.Dir(p.baseFile))
			slog.WarnContext(ctx, "Could not mirror golden base onto slot tmpfs; using node disk",
				slog.String("base", p.srcFile), slog.Any("err", err))
			return
		}
		p.staged.Store(true)
		slog.InfoContext(ctx, "Mirrored golden base onto slot tmpfs",
			slog.String("base", p.srcFile), slog.String("mirror", p.baseFile), slog.Duration("took", time.Since(t)))
	})
	return p.stageErr == nil
}

func mirrorGoldenBase(ctx context.Context, src, dst string) error {
	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	if di, err := os.Stat(dst); err == nil && di.Mode().IsRegular() && di.Size() == si.Size() {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	_ = os.Remove(tmp)
	if err := ch.CopySparseFileFast(ctx, src, tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func goldenSlotPool(ctx context.Context, baseFile string) *baseSlotPool {
	if root := goldenTmpfsRoot; root != "" {
		if p := getOrCreateBaseSlotPool(baseFile, root); p.ready(ctx) {
			return p
		}
	}
	return getOrCreateBaseSlotPool(baseFile, "")
}

func goldenBaseReadPath(baseFile string) string {
	if root := goldenTmpfsRoot; root != "" {
		if v, ok := goldenPools.Load(poolKey(baseFile, root)); ok {
			if p := v.(*baseSlotPool); p.staged.Load() {
				return p.baseFile
			}
		}
	}
	return baseFile
}

func maybePrewarmGoldenSlots(baseFile string) {
	if slotPoolDisabled() {
		return
	}
	if _, started := goldenPrewarmStarted.Load(baseFile); started {
		return
	}
	if _, err := os.Stat(baseFile); err != nil {
		return
	}
	if _, started := goldenPrewarmStarted.LoadOrStore(baseFile, struct{}{}); started {
		return
	}
	go func() {
		pool := goldenSlotPool(context.Background(), baseFile)
		for i := 0; i < pool.prewarm; i++ {
			pool.replenishOneSlot()
		}
	}()
}

func (p *baseSlotPool) replenishOneSlot() {
	if err := os.MkdirAll(p.slotDir, 0o700); err != nil {
		return
	}
	slotPath := filepath.Join(p.slotDir, fmt.Sprintf("slot-%d", p.nextID.Add(1)))
	if err := ch.CopySparseFileFast(context.Background(), p.baseFile, slotPath); err != nil {
		_ = os.Remove(slotPath)
		return
	}
	select {
	case p.idle <- &goldenSlot{path: slotPath}:
	default:
		_ = os.Remove(slotPath)
	}
}

func (p *baseSlotPool) resetAndReturnSlot(slot *goldenSlot, dirtyRanges []ch.SparseExtent) {
	if slot == nil {
		return
	}
	if err := ch.RestoreSparseRegions(context.Background(), p.baseFile, slot.path, dirtyRanges); err != nil {
		_ = os.Remove(slot.path)
		return
	}
	select {
	case p.idle <- slot:
	default:
		_ = os.Remove(slot.path)
	}
}

func linkSlot(slotPath, outFile string) (symlinked bool, err error) {
	err = linkFile(slotPath, outFile)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, unix.EXDEV) {
		return false, err
	}
	if err := os.Symlink(slotPath, outFile); err != nil {
		return false, err
	}
	return true, nil
}

func materializeSlot(ctx context.Context, slotPath, outFile string) error {
	tmp := outFile + ".materialize"
	_ = os.Remove(tmp)
	if err := ch.CopySparseFileFast(ctx, slotPath, tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, outFile); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func recoverInterruptedOverlay(outFile string) {
	deltaBackup := outFile + ".delta.bak"
	fi, err := os.Lstat(outFile)
	if err != nil || fi.Mode()&os.ModeSymlink == 0 {
		return
	}
	if _, err := os.Lstat(deltaBackup); err != nil {
		return
	}
	_ = os.Rename(deltaBackup, outFile)
}

// goldenSlotLease represents a restore's hold on a slot image linked at outFile.
type goldenSlotLease struct {
	commit func(eager, preserve bool) (keepDelta bool)
	abort  func()

	slot     *goldenSlot
	pool     *baseSlotPool
	fromIdle bool
	claimed  *atomic.Bool
}

// claimForVM hands the lease's slot to the VM about to map it as guest RAM
// (zero-copy restore). A replacement slot is staged in the background.
func (l *goldenSlotLease) claimForVM() string {
	if l == nil || l.slot == nil || l.claimed == nil || !l.claimed.CompareAndSwap(false, true) {
		return ""
	}
	go l.pool.replenishOneSlot()
	return l.slot.path
}

func acquireGoldenSlotOverlay(ctx context.Context, baseFile, deltaFile, outFile string) (commit func(eager, preserve bool) (keepDelta bool), abort func(), err error) {
	l, err := acquireGoldenSlot(ctx, baseFile, deltaFile, outFile)
	if err != nil {
		return nil, nil, err
	}
	return l.commit, l.abort, nil
}

// acquireGoldenSlot overlays deltaFile's populated pages onto a pre-staged
// golden-base slot file and links it at outFile for vm.restore.
func acquireGoldenSlot(ctx context.Context, baseFile, deltaFile, outFile string) (*goldenSlotLease, error) {
	noopLease := &goldenSlotLease{commit: func(bool, bool) bool { return false }, abort: func() {}}
	if slotPoolDisabled() {
		if err := ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile); err != nil {
			return nil, err
		}
		return noopLease, nil
	}
	recoverInterruptedOverlay(outFile)
	maybePrewarmGoldenSlots(baseFile)
	bi, err := os.Stat(baseFile)
	if err != nil {
		return nil, fmt.Errorf("stat base %q: %w", baseFile, err)
	}
	di, err := os.Stat(deltaFile)
	if err != nil {
		return nil, fmt.Errorf("stat delta %q: %w", deltaFile, err)
	}
	if bi.Size() != di.Size() {
		return nil, fmt.Errorf("acquireGoldenSlotOverlay: size mismatch base=%d delta=%d", bi.Size(), di.Size())
	}

	pool := goldenSlotPool(ctx, baseFile)
	slot, fromIdle, err := acquireRawSlotFromPool(ctx, pool, bi.Size())
	if err != nil {
		if !pool.inTmpfs {
			return nil, fmt.Errorf("staging golden slot: %w", err)
		}
		slog.WarnContext(ctx, "Could not stage golden slot on tmpfs; merging on node disk",
			slog.String("base", baseFile), slog.Any("err", err))
		if err := ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile); err != nil {
			return nil, err
		}
		return noopLease, nil
	}

	dirtyRanges, _, oerr := ch.OverlaySparseRegionsRecorded(ctx, deltaFile, slot.path)
	if oerr != nil {
		_ = os.Remove(slot.path)
		return nil, oerr
	}

	return bindSlotToOutFile(ctx, pool, slot, fromIdle, dirtyRanges, baseFile, deltaFile, outFile, func() error {
		return ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile)
	})
}

// acquireGoldenSlotCompact overlays an ADIF compact delta (metaFile + dataFile)
// onto a pre-staged golden-base slot and links it at outFile.
func acquireGoldenSlotCompact(ctx context.Context, baseFile, metaFile, dataFile, outFile string) (*goldenSlotLease, error) {
	noopLease := &goldenSlotLease{commit: func(bool, bool) bool { return false }, abort: func() {}}
	if slotPoolDisabled() {
		if err := ch.RestoreFromDelta(ctx, baseFile, metaFile, dataFile, outFile); err != nil {
			return nil, err
		}
		return noopLease, nil
	}
	maybePrewarmGoldenSlots(baseFile)
	bi, err := os.Stat(baseFile)
	if err != nil {
		return nil, fmt.Errorf("stat base %q: %w", baseFile, err)
	}
	pool := goldenSlotPool(ctx, baseFile)
	slot, fromIdle, err := acquireRawSlotFromPool(ctx, pool, bi.Size())
	if err != nil {
		if !pool.inTmpfs {
			return nil, fmt.Errorf("staging golden slot: %w", err)
		}
		slog.WarnContext(ctx, "Could not stage golden slot on tmpfs; restoring compact delta on node disk",
			slog.String("base", baseFile), slog.Any("err", err))
		if err := ch.RestoreFromDelta(ctx, baseFile, metaFile, dataFile, outFile); err != nil {
			return nil, err
		}
		return noopLease, nil
	}

	dirtyRanges, _, oerr := ch.OverlayDeltaFilesRecorded(ctx, metaFile, dataFile, slot.path)
	if oerr != nil {
		_ = os.Remove(slot.path)
		return nil, oerr
	}

	return bindSlotToOutFile(ctx, pool, slot, fromIdle, dirtyRanges, baseFile, "", outFile, func() error {
		return ch.RestoreFromDelta(ctx, baseFile, metaFile, dataFile, outFile)
	})
}

func acquireRawSlotFromPool(ctx context.Context, pool *baseSlotPool, expectedSize int64) (slot *goldenSlot, fromIdle bool, err error) {
	for {
		select {
		case s := <-pool.idle:
			if st, serr := os.Stat(s.path); serr == nil && st.Size() == expectedSize && !sharesInode(s.path) {
				slot = s
				break
			}
			_ = os.Remove(s.path)
			continue
		default:
		}
		break
	}
	fromIdle = slot != nil
	if slot == nil {
		slotPath := filepath.Join(pool.slotDir, fmt.Sprintf("slot-%d", pool.nextID.Add(1)))
		err := os.MkdirAll(pool.slotDir, 0o700)
		if err == nil {
			err = ch.CopySparseFileFast(ctx, pool.baseFile, slotPath)
		}
		if err != nil {
			_ = os.Remove(slotPath)
			return nil, false, err
		}
		slot = &goldenSlot{path: slotPath}
	}
	return slot, fromIdle, nil
}

func bindSlotToOutFile(
	ctx context.Context,
	pool *baseSlotPool,
	slot *goldenSlot,
	fromIdle bool,
	dirtyRanges []ch.SparseExtent,
	baseFile, deltaFile, outFile string,
	diskFallback func() error,
) (*goldenSlotLease, error) {
	noopLease := &goldenSlotLease{commit: func(bool, bool) bool { return false }, abort: func() {}}
	hasBackup := deltaFile != "" && deltaFile == outFile
	deltaBackup := outFile + ".delta.bak"
	if hasBackup {
		_ = os.Remove(deltaBackup)
		if err := os.Rename(outFile, deltaBackup); err != nil {
			go pool.resetAndReturnSlot(slot, dirtyRanges)
			return nil, fmt.Errorf("backup delta %q: %w", outFile, err)
		}
	} else {
		_ = os.Remove(outFile)
	}

	symlinked, err := linkSlot(slot.path, outFile)
	if err != nil {
		if hasBackup {
			_ = os.Rename(deltaBackup, outFile)
		}
		go pool.resetAndReturnSlot(slot, dirtyRanges)
		if err := diskFallback(); err != nil {
			return nil, err
		}
		return noopLease, nil
	}

	bg := context.WithoutCancel(ctx)
	detach := func() {
		if hasBackup {
			_ = os.Remove(deltaBackup)
		}
		if symlinked {
			if err := materializeSlot(bg, slot.path, outFile); err != nil {
				slog.WarnContext(ctx, "Could not copy golden slot off tmpfs; keeping it",
					slog.String("slot", slot.path), slog.Any("err", err))
				return
			}
		}
		_ = os.Remove(slot.path)
		go pool.replenishOneSlot()
	}

	claimed := new(atomic.Bool)
	var once sync.Once
	commitFn := func(eager, preserve bool) (keepDelta bool) {
		once.Do(func() {
			if claimed.Load() {
				keepDelta = commitClaimedSlot(ctx, outFile, deltaBackup, hasBackup, preserve)
				return
			}
			switch {
			case eager && !preserve:
				if hasBackup {
					_ = os.Remove(deltaBackup)
				}
				_ = os.Remove(outFile)
				go pool.resetAndReturnSlot(slot, dirtyRanges)
			case eager && pool.inTmpfs:
				if hasBackup {
					if err := os.Rename(deltaBackup, outFile); err != nil {
						slog.WarnContext(ctx, "Could not put golden delta back; detaching slot instead",
							slog.String("delta", deltaBackup), slog.Any("err", err))
						detach()
						return
					}
				} else {
					_ = os.Remove(outFile)
				}
				keepDelta = true
				go pool.resetAndReturnSlot(slot, dirtyRanges)
			default:
				detach()
			}
		})
		return keepDelta
	}
	abortFn := func() {
		once.Do(func() {
			_ = os.Remove(outFile)
			if hasBackup {
				_ = os.Rename(deltaBackup, outFile)
			}
			if !claimed.Load() {
				go pool.resetAndReturnSlot(slot, dirtyRanges)
			}
		})
	}
	return &goldenSlotLease{
		commit:   commitFn,
		abort:    abortFn,
		slot:     slot,
		pool:     pool,
		fromIdle: fromIdle,
		claimed:  claimed,
	}, nil
}

func commitClaimedSlot(ctx context.Context, outFile, deltaBackup string, hasBackup, preserve bool) (keepDelta bool) {
	if !preserve {
		_ = os.Remove(deltaBackup)
		_ = os.Remove(outFile)
		return false
	}
	if !hasBackup {
		_ = os.Remove(outFile)
		return true
	}
	if err := os.Rename(deltaBackup, outFile); err != nil {
		slog.WarnContext(ctx, "Could not put golden delta back; next restore from this snapshot will recover it",
			slog.String("delta", deltaBackup), slog.Any("err", err))
	}
	return true
}

// zeroCopyRestore tracks a restore whose guest RAM is a claimed golden slot.
type zeroCopyRestore struct {
	restoreDir       string
	slot             string
	slotDev, slotIno uint64
	slotBase         string
	origMemory       json.RawMessage
	reverted         bool
}

// startZeroCopyRestore claims lease's slot and rewrites restoreDir so that
// vm.restore maps the slot directly as guest RAM.
func startZeroCopyRestore(ctx context.Context, actorUID, restoreDir string, lease *goldenSlotLease) (zc *zeroCopyRestore, fallback string, err error) {
	switch {
	case lease == nil:
		return nil, zeroCopyNoGoldenDelta, nil
	case lease.slot == nil:
		return nil, zeroCopyMergedOnDisk, nil
	case !lease.pool.inTmpfs:
		return nil, zeroCopyDiskSlot, nil
	case !lease.fromIdle:
		return nil, zeroCopyNoIdleSlot, nil
	}
	if _, failed := zeroCopyBackoff.LoadAndDelete(actorUID); failed {
		return nil, zeroCopyPreviousFailure, nil
	}
	slot := lease.slot.path
	var st unix.Stat_t
	if err := unix.Stat(slot, &st); err != nil {
		return nil, zeroCopyRewriteFailed, nil
	}
	orig, err := ch.UseFileBackedMemory(restoreDir, slot)
	if err != nil {
		if errors.Is(err, ch.ErrUnsupportedLayout) {
			slog.InfoContext(ctx, "Snapshot guest memory cannot be mapped from golden slot; copying it",
				slog.String("id", actorUID), slog.Any("reason", err))
			return nil, zeroCopyLayout, nil
		}
		if cerr := ch.CanonicalizeFileBackedSnapshot(restoreDir, nil); cerr != nil {
			return nil, "", fmt.Errorf("while mapping golden slot as guest RAM: %w (and snapshot is not restorable as is: %v)", err, cerr)
		}
		slog.WarnContext(ctx, "Could not map golden slot as guest RAM; copying it",
			slog.String("id", actorUID), slog.Any("err", err))
		return nil, zeroCopyRewriteFailed, nil
	}
	if lease.claimForVM() == "" {
		if err := ch.CanonicalizeFileBackedSnapshot(restoreDir, orig); err != nil {
			return nil, "", fmt.Errorf("while putting snapshot back: %w", err)
		}
		return nil, zeroCopyRewriteFailed, nil
	}
	return &zeroCopyRestore{
		restoreDir: restoreDir,
		slot:       slot,
		slotDev:    uint64(st.Dev),
		slotIno:    st.Ino,
		slotBase:   lease.pool.baseFile,
		origMemory: orig,
	}, "", nil
}

func (z *zeroCopyRestore) revert() error {
	if z.reverted {
		return nil
	}
	if err := ch.CanonicalizeFileBackedSnapshot(z.restoreDir, z.origMemory); err != nil {
		return err
	}
	z.reverted = true
	return nil
}

func (z *zeroCopyRestore) abandon(ctx context.Context, actorUID string) {
	if err := z.revert(); err != nil {
		slog.ErrorContext(ctx, "Could not put restore directory back after failed zero-copy restore",
			slog.String("id", actorUID), slog.String("dir", z.restoreDir), slog.Any("err", err))
	}
	removeMemSlot(ctx, actorUID, z.slot)
	zeroCopyBackoff.Store(actorUID, struct{}{})
}

func removeMemSlot(ctx context.Context, actorUID, slot string) {
	if err := os.Remove(slot); err != nil && !os.IsNotExist(err) {
		slog.WarnContext(ctx, "Could not delete golden slot that held actor guest RAM",
			slog.String("id", actorUID), slog.String("slot", slot), slog.Any("err", err))
	}
}

// discardMemSlot deletes the golden slot holding ra's guest RAM, if any.
// Call only once the actor's VMM and virtiofsd have exited.
func (s *AteWorkerService) discardMemSlot(ctx context.Context, actorUID string, ra *runningActor) {
	s.actorsMu.Lock()
	slot := ra.memSlot
	ra.memSlot = ""
	s.actorsMu.Unlock()
	if slot != "" {
		removeMemSlot(ctx, actorUID, slot)
	}
}

func checkSlotSnapshot(checkpointDir string, ra *runningActor) error {
	if _, err := os.Lstat(filepath.Join(checkpointDir, "memory-ranges")); err == nil {
		return fmt.Errorf("cloud-hypervisor wrote memory-ranges for a guest whose RAM is golden slot %s", ra.memSlot)
	} else if !os.IsNotExist(err) {
		return err
	}
	var st unix.Stat_t
	if err := unix.Stat(ra.memSlot, &st); err != nil {
		return fmt.Errorf("while checking golden slot holding guest RAM: %w", err)
	}
	if uint64(st.Dev) != ra.memSlotDev || st.Ino != ra.memSlotIno {
		return fmt.Errorf("golden slot %s is no longer the file the guest RAM is mapped from", ra.memSlot)
	}
	return nil
}

func copySlotMemory(ctx context.Context, actorUID string, ra *runningActor, checkpointMem string) error {
	t := time.Now()
	if err := ch.CopySparseFileFast(ctx, ra.memSlot, checkpointMem); err != nil {
		_ = os.Remove(checkpointMem)
		return fmt.Errorf("while copying guest memory from golden slot: %w", err)
	}
	slog.InfoContext(ctx, "Copied guest memory from golden slot", slog.String("id", actorUID),
		slog.String("checkpoint_mode", "slot_copy"), slog.Duration("checkpoint_delta_from_slot", time.Since(t)))
	return nil
}
