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
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
)

// Opt-in restore-latency knobs. Every one of them defaults to off, which keeps
// today's behavior exactly; they are read once at startup from the environment
// (the controller forwards ATEOM_* variables to worker pods).
const (
	// envActivationCPUBoost lifts the actor's cgroup CPU cap while it is being
	// restored or cold-booted, until its wakeup probe passes, then applies the
	// actor's own limit. "max" removes the cap; "<n>m" (millicores) or "<n>"
	// (cores, may be fractional) sets it. A boost below the actor's own limit
	// never lowers it. Unset, "", "0" or "off" disables it.
	//
	// Why: the VMM and virtiofsd run in the actor's leaf, so at a 250m actor
	// every CPU-bound restore step (VMM start-up, the eager guest-RAM copy, the
	// guest's own wakeup) runs at a quarter of a core and takes ~4x longer.
	envActivationCPUBoost = "ATEOM_MICROVM_ACTIVATION_CPU_BOOST"
	// envParallelWakeup ("true") runs the kata-agent dial + guest CRNG reseed
	// concurrently with the wakeup probe instead of before it. The restore still
	// succeeds only once both have, so the actor is never reported running with
	// randomness shared with its clones.
	envParallelWakeup = "ATEOM_MICROVM_PARALLEL_WAKEUP"
	// envMemoryRestoreMode overrides how guest RAM is restored: "auto" (default:
	// restoreMemMode's version check), "copy", "ondemand" or
	// "ondemand-prefault". "ondemand" is only honored on a VMM that does not
	// prefault unconditionally, or on one that can be told not to (see
	// envOnDemandBackgroundFetch); otherwise ateom logs and restores eagerly,
	// because at a fractional CPU cap the unconditional prefault both starves
	// the guest and makes vm.snapshot fail until it completes.
	// "ondemand-prefault" restores OnDemand and keeps the VMM's background
	// prefault: the guest wakes while the rest of RAM streams in behind it. Use
	// it only with the activation CPU boost (the prefault then runs at full
	// speed) and with a cloud-hypervisor whose prefault skips snapshot holes
	// (upstream c91a6543, after v53.0); v53.0 itself fills all of guest RAM,
	// which also bloats every later snapshot to the full RAM size.
	envMemoryRestoreMode = "ATEOM_MICROVM_MEMORY_RESTORE_MODE"
	// envOnDemandBackgroundFetch ("false") asks the VMM to serve only the pages
	// the guest touches after an OnDemand restore, with no background prefault
	// of the rest. It needs a cloud-hypervisor that advertises
	// ch.FeatureRestoreBackgroundFetchPages in vmm.ping.
	envOnDemandBackgroundFetch = "ATEOM_MICROVM_ONDEMAND_BACKGROUND_FETCH"
	// envRestoreReadahead ("true") starts reading the memory image's data into
	// the page cache (readahead(2) in 128 KiB calls, see readaheadMemoryImage) as
	// soon as a restore starts, so the disk reads run deep and overlap the
	// rootfs, network and VMM set-up instead of landing inside vm.restore as one
	// sequential 128 KiB stream. It matters when the image is not cached, e.g. a
	// local snapshot written minutes ago on a busy node.
	envRestoreReadahead = "ATEOM_MICROVM_RESTORE_READAHEAD"
	// envEarlyVMMLaunch ("true") starts the restore's cloud-hypervisor process
	// while the rootfs, virtiofsd and tap are still being set up, instead of
	// after them. The VMM only needs its cgroup leaf and api-socket path.
	envEarlyVMMLaunch = "ATEOM_MICROVM_EARLY_VMM_LAUNCH"
)

// fastRestoreConfig is the parsed set of opt-in restore-latency knobs.
type fastRestoreConfig struct {
	// boost enables the activation CPU boost; boostMilliCPU is its cap in
	// millicores, 0 meaning no cap at all.
	boost         bool
	boostMilliCPU int64
	// parallelWakeup overlaps the agent dial + CRNG reseed with the probe.
	parallelWakeup bool
	// memRestoreMode is "" (auto), ch.MemRestoreEager or ch.MemRestoreOnDemand.
	memRestoreMode string
	// allowPrefault keeps an OnDemand restore even on a VMM that prefaults the
	// rest of guest RAM in the background ("ondemand-prefault").
	allowPrefault bool
	// noBackgroundFetch asks a capable VMM not to prefault after OnDemand.
	noBackgroundFetch bool
	// readahead starts page-cache readahead of the memory image early.
	readahead bool
	// earlyVMMLaunch overlaps the VMM launch with rootfs/tap set-up.
	earlyVMMLaunch bool
}

// enabled reports whether any knob is on, for the startup log.
func (c fastRestoreConfig) enabled() bool {
	return c.boost || c.parallelWakeup || c.memRestoreMode != "" || c.noBackgroundFetch || c.readahead || c.earlyVMMLaunch
}

// logValue renders the config for the startup log.
func (c fastRestoreConfig) attrs() []any {
	boost := "off"
	if c.boost {
		boost = "max"
		if c.boostMilliCPU > 0 {
			boost = fmt.Sprintf("%dm", c.boostMilliCPU)
		}
	}
	mode := c.memRestoreMode
	if mode == "" {
		mode = "auto"
	} else if c.allowPrefault {
		mode += "+prefault"
	}
	return []any{
		slog.String("activation_cpu_boost", boost),
		slog.Bool("parallel_wakeup", c.parallelWakeup),
		slog.String("memory_restore_mode", mode),
		slog.Bool("ondemand_no_background_fetch", c.noBackgroundFetch),
		slog.Bool("restore_readahead", c.readahead),
		slog.Bool("early_vmm_launch", c.earlyVMMLaunch),
	}
}

// fastRestoreConfigFromEnv parses the knobs; getenv is os.Getenv outside tests.
func fastRestoreConfigFromEnv(getenv func(string) string) (fastRestoreConfig, error) {
	var c fastRestoreConfig
	var err error
	if c.boost, c.boostMilliCPU, err = parseCPUBoost(getenv(envActivationCPUBoost)); err != nil {
		return fastRestoreConfig{}, fmt.Errorf("%s: %w", envActivationCPUBoost, err)
	}
	for _, b := range []struct {
		env string
		dst *bool
	}{
		{envParallelWakeup, &c.parallelWakeup},
		{envRestoreReadahead, &c.readahead},
		{envEarlyVMMLaunch, &c.earlyVMMLaunch},
	} {
		if *b.dst, err = parseBoolEnv(getenv(b.env), false); err != nil {
			return fastRestoreConfig{}, fmt.Errorf("%s: %w", b.env, err)
		}
	}
	bgFetch, err := parseBoolEnv(getenv(envOnDemandBackgroundFetch), true)
	if err != nil {
		return fastRestoreConfig{}, fmt.Errorf("%s: %w", envOnDemandBackgroundFetch, err)
	}
	c.noBackgroundFetch = !bgFetch
	switch v := strings.ToLower(strings.TrimSpace(getenv(envMemoryRestoreMode))); v {
	case "", "auto":
	case "copy", "eager":
		c.memRestoreMode = ch.MemRestoreEager
	case "ondemand", "on-demand", "lazy":
		c.memRestoreMode = ch.MemRestoreOnDemand
	case "ondemand-prefault":
		c.memRestoreMode = ch.MemRestoreOnDemand
		c.allowPrefault = true
	default:
		return fastRestoreConfig{}, fmt.Errorf("%s: want auto, copy, ondemand or ondemand-prefault, got %q", envMemoryRestoreMode, v)
	}
	return c, nil
}

// parseBoolEnv parses a boolean env value, def when unset.
func parseBoolEnv(v string, def bool) (bool, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return def, nil
	}
	return strconv.ParseBool(v)
}

// parseCPUBoost parses envActivationCPUBoost: "max" means no cap (0), "<n>m"
// millicores, "<n>" cores.
func parseCPUBoost(v string) (enabled bool, milliCPU int64, err error) {
	v = strings.ToLower(strings.TrimSpace(v))
	switch v {
	case "", "0", "off", "false", "none":
		return false, 0, nil
	case "max", "unlimited", "true", "on":
		return true, 0, nil
	}
	if m, ok := strings.CutSuffix(v, "m"); ok {
		n, err := strconv.ParseInt(m, 10, 64)
		if err != nil || n <= 0 {
			return false, 0, fmt.Errorf("invalid millicores %q", v)
		}
		return true, n, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 || math.IsInf(f, 0) || math.IsNaN(f) {
		return false, 0, fmt.Errorf("want max, <n>m or <cores>, got %q", v)
	}
	return true, int64(math.Ceil(f * 1000)), nil
}

// activationMilliCPU is the leaf CPU cap to use while an actor whose limit is
// actorMilli is being activated: actorMilli when the boost is off, otherwise
// the larger of the two (0, no cap, wins over any number).
func (c fastRestoreConfig) activationMilliCPU(actorMilli int64) int64 {
	if !c.boost || actorMilli <= 0 {
		return actorMilli
	}
	if c.boostMilliCPU == 0 {
		return 0
	}
	return max(actorMilli, c.boostMilliCPU)
}

// boosted reports whether activationMilliCPU differs from the actor's limit,
// i.e. whether the cap has to be put back once the actor is up.
func (c fastRestoreConfig) boosted(actorMilli int64) bool {
	return c.activationMilliCPU(actorMilli) != actorMilli
}

// pickRestoreMemMode applies the envMemoryRestoreMode override on top of
// restoreMemMode, and says whether to ask the VMM not to prefault after an
// OnDemand restore. The override never selects OnDemand on a VMM whose
// unconditional prefault it cannot switch off: that combination starves the
// guest's wakeup and fails every checkpoint until the prefault ends.
func (c fastRestoreConfig) pickRestoreMemMode(ctx context.Context, info ch.VMMInfo) (mode string, disableBackgroundFetch bool) {
	auto := restoreMemMode(ctx, info)
	switch c.memRestoreMode {
	case ch.MemRestoreEager:
		return ch.MemRestoreEager, false
	case ch.MemRestoreOnDemand:
		if !info.PrefaultsUnconditionally() {
			return ch.MemRestoreOnDemand, false
		}
		if c.allowPrefault {
			return ch.MemRestoreOnDemand, false
		}
		if c.noBackgroundFetch && info.HasFeature(ch.FeatureRestoreBackgroundFetchPages) {
			return ch.MemRestoreOnDemand, true
		}
		slog.WarnContext(ctx, "OnDemand restore requested, but this cloud-hypervisor prefaults unconditionally and cannot be told not to; restoring eagerly",
			slog.String("vmm_version", info.Version), slog.Bool("no_background_fetch_requested", c.noBackgroundFetch),
			slog.Any("vmm_features", info.Features))
		return auto, false
	}
	if auto == ch.MemRestoreOnDemand && c.noBackgroundFetch && info.HasFeature(ch.FeatureRestoreBackgroundFetchPages) {
		return auto, true
	}
	return auto, false
}

// readaheadChunk is the size of each readahead(2) call. The kernel honors at
// most max(read_ahead_kb, max_sectors_kb) per call (force_page_cache_ra), which
// is 128–256 KiB on default GKE disks, so one big call would prefetch only its
// first chunk; many small ones queue the whole image.
const readaheadChunk = 128 << 10

// readaheadMemoryImage asks the kernel to start reading path's data extents
// into the page cache now, so the eager restore that follows finds them cached
// (or in flight) instead of reading the image with one sequential 128 KiB
// stream. It only submits I/O and returns; it skips holes, which would
// otherwise fill the page cache with zero pages. Errors are only logged at
// debug level: this is an optimization.
func readaheadMemoryImage(ctx context.Context, path string) {
	f, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.DebugContext(ctx, "restore readahead: open failed", slog.String("path", path), slog.Any("err", err))
		}
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return
	}
	if err := readaheadDataExtents(ctx, int(f.Fd()), st.Size(), readaheadChunk); err != nil {
		slog.DebugContext(ctx, "restore readahead failed", slog.String("path", path), slog.Any("err", err))
	}
}

// readaheadDataExtents issues readahead(2) over every data extent of fd below
// size, in calls of at most chunk bytes.
func readaheadDataExtents(ctx context.Context, fd int, size, chunk int64) error {
	for off := int64(0); off < size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				return nil // no data past off
			}
			return err
		}
		hole, err := unix.Seek(fd, data, unix.SEEK_HOLE)
		if err != nil {
			hole = size
		}
		hole = min(hole, size)
		for o := data; o < hole; o += chunk {
			n := min(chunk, hole-o)
			if _, _, e := unix.Syscall(unix.SYS_READAHEAD, uintptr(fd), uintptr(o), uintptr(n)); e != 0 {
				return e
			}
		}
		off = hole
	}
	return nil
}

// sharesInode reports whether path has other hard links (for example the node's
// golden-base cache or a retained local snapshot). An OnDemand restore source
// that shares its inode must not be merged into in place.
func sharesInode(path string) bool {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return false
	}
	return st.Nlink > 1
}

// vmmLaunchResult is what a concurrent (early) ch.LaunchVMM produced.
type vmmLaunchResult struct {
	cmd    *exec.Cmd
	client *ch.Client
	err    error
	took   time.Duration
}

// snapshotWhilePrefaulting snapshots the paused guest, retrying for up to wait
// while the VMM refuses because its OnDemand background prefault has not
// finished yet (cloud-hypervisor >= 53 rejects vm.snapshot until then). Only
// "ondemand-prefault" restores can be in that state.
func snapshotWhilePrefaulting(ctx context.Context, client *ch.Client, dir string, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		err := client.Snapshot(ctx, dir)
		if err == nil || !isRestoringError(err) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// isRestoringError reports whether err is cloud-hypervisor refusing an
// operation because an OnDemand restore is still faulting in guest RAM.
func isRestoringError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "on-demand memory restore is still in progress") || strings.Contains(msg, "VmRestoring")
}

const (
	goldenSlotPoolCap      = 8
	goldenSlotPrewarmCount = 6
)

var goldenPools sync.Map // baseFile string -> *baseSlotPool

type goldenSlot struct {
	path string
}

type baseSlotPool struct {
	baseFile    string
	slotDir     string
	idle        chan *goldenSlot
	prewarmOnce sync.Once
	nextID      atomic.Uint64
}

func getOrCreateBaseSlotPool(baseFile string) *baseSlotPool {
	if v, ok := goldenPools.Load(baseFile); ok {
		return v.(*baseSlotPool)
	}
	baseDir := filepath.Dir(baseFile)
	slotDir := filepath.Join(baseDir, fmt.Sprintf("slots-%d", os.Getpid()))
	pool := &baseSlotPool{
		baseFile: baseFile,
		slotDir:  slotDir,
		idle:     make(chan *goldenSlot, goldenSlotPoolCap),
	}
	actual, loaded := goldenPools.LoadOrStore(baseFile, pool)
	if loaded {
		return actual.(*baseSlotPool)
	}
	// Best-effort cleanup of slot directories from dead ateom-microvm PIDs.
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
	_ = os.MkdirAll(slotDir, 0o700)
	return pool
}

// maybePrewarmGoldenSlots asynchronously populates reusable golden-base slot
// files once baseFile exists so warm restores never copy the 168 MiB base on
// the critical path.
func maybePrewarmGoldenSlots(baseFile string) {
	if os.Getenv("ATEOM_MICROVM_DISABLE_GOLDEN_SLOT_POOL") == "true" {
		return
	}
	if _, err := os.Stat(baseFile); err != nil {
		return
	}
	pool := getOrCreateBaseSlotPool(baseFile)
	pool.prewarmOnce.Do(func() {
		go func() {
			for i := 0; i < goldenSlotPrewarmCount; i++ {
				pool.replenishOneSlot()
			}
		}()
	})
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

func (p *baseSlotPool) resetAndReturnSlot(slot *goldenSlot, dirtyRanges []ch.ByteRange) {
	if slot == nil {
		return
	}
	bf, err := os.Open(p.baseFile)
	if err != nil {
		_ = os.Remove(slot.path)
		return
	}
	defer bf.Close()
	sf, err := os.OpenFile(slot.path, os.O_RDWR, 0o600)
	if err != nil {
		_ = os.Remove(slot.path)
		return
	}
	rerr := ch.RestoreSparseRegions(bf, sf, dirtyRanges)
	cerr := sf.Close()
	if rerr != nil || cerr != nil {
		_ = os.Remove(slot.path)
		return
	}
	select {
	case p.idle <- slot:
	default:
		_ = os.Remove(slot.path)
	}
}

// acquireGoldenSlotOverlay overlays deltaFile's populated pages onto a
// pre-staged golden-base slot file (~7 MiB write instead of a 168 MiB base copy)
// and links it at outFile for vm.restore.
//
// Call commit(true) once an eager vm.restore finishes (unlinks outFile and
// asynchronously resets the modified ~7 MiB ranges of the slot for reuse), or
// commit(false) for OnDemand/preserved restores (detaches the slot so outFile
// remains owned exclusively by the actor). Defer abort() so any failure before
// commit restores deltaFile at outFile and returns the reset slot to the pool.
func acquireGoldenSlotOverlay(ctx context.Context, baseFile, deltaFile, outFile string) (commit func(eager bool), abort func(), err error) {
	if os.Getenv("ATEOM_MICROVM_DISABLE_GOLDEN_SLOT_POOL") == "true" {
		if err := ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile); err != nil {
			return nil, nil, err
		}
		return func(bool) {}, func() {}, nil
	}
	maybePrewarmGoldenSlots(baseFile)
	bi, err := os.Stat(baseFile)
	if err != nil {
		return nil, nil, fmt.Errorf("stat base %q: %w", baseFile, err)
	}
	di, err := os.Stat(deltaFile)
	if err != nil {
		return nil, nil, fmt.Errorf("stat delta %q: %w", deltaFile, err)
	}
	if bi.Size() != di.Size() {
		return nil, nil, fmt.Errorf("acquireGoldenSlotOverlay: size mismatch base=%d delta=%d", bi.Size(), di.Size())
	}

	pool := getOrCreateBaseSlotPool(baseFile)
	var slot *goldenSlot
	for {
		select {
		case s := <-pool.idle:
			if st, serr := os.Stat(s.path); serr == nil && st.Size() == bi.Size() && !sharesInode(s.path) {
				slot = s
				break
			}
			_ = os.Remove(s.path)
			continue
		default:
		}
		break
	}
	if slot == nil {
		if err := os.MkdirAll(pool.slotDir, 0o700); err != nil {
			return nil, nil, err
		}
		slotPath := filepath.Join(pool.slotDir, fmt.Sprintf("slot-%d", pool.nextID.Add(1)))
		if err := ch.CopySparseFileFast(ctx, baseFile, slotPath); err != nil {
			_ = os.Remove(slotPath)
			return nil, nil, fmt.Errorf("staging golden slot: %w", err)
		}
		slot = &goldenSlot{path: slotPath}
	}

	df, err := os.Open(deltaFile)
	if err != nil {
		pool.resetAndReturnSlot(slot, nil)
		return nil, nil, err
	}
	sf, err := os.OpenFile(slot.path, os.O_RDWR, 0o600)
	if err != nil {
		df.Close()
		_ = os.Remove(slot.path)
		return nil, nil, err
	}
	dirtyRanges, oerr := ch.OverlaySparseRegionsRecorded(df, sf)
	scloseErr := sf.Close()
	df.Close()
	if oerr != nil || scloseErr != nil {
		_ = os.Remove(slot.path)
		if oerr != nil {
			return nil, nil, oerr
		}
		return nil, nil, scloseErr
	}

	deltaBackup := outFile + ".delta.bak"
	_ = os.Remove(deltaBackup)
	if err := os.Rename(outFile, deltaBackup); err != nil {
		go pool.resetAndReturnSlot(slot, dirtyRanges)
		return nil, nil, fmt.Errorf("backup delta %q: %w", outFile, err)
	}
	if err := os.Link(slot.path, outFile); err != nil {
		_ = os.Rename(deltaBackup, outFile)
		go pool.resetAndReturnSlot(slot, dirtyRanges)
		if err := ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile); err != nil {
			return nil, nil, err
		}
		return func(bool) {}, func() {}, nil
	}

	var once sync.Once
	commitFn := func(eager bool) {
		once.Do(func() {
			_ = os.Remove(deltaBackup)
			if eager {
				_ = os.Remove(outFile)
				go pool.resetAndReturnSlot(slot, dirtyRanges)
			} else {
				_ = os.Remove(slot.path)
				go pool.replenishOneSlot()
			}
		})
	}
	abortFn := func() {
		once.Do(func() {
			_ = os.Remove(outFile)
			_ = os.Rename(deltaBackup, outFile)
			go pool.resetAndReturnSlot(slot, dirtyRanges)
		})
	}
	return commitFn, abortFn, nil
}
