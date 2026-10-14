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
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/internal/ateomcgroup"
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
	// envTmpfsSlots ("true") keeps the golden-base slot pool, and a mirror of
	// each golden base it is built from, on a tmpfs (see goldentmpfs.go)
	// instead of the node disk. Warm restores then overlay their delta onto a
	// RAM-backed slot, vm.restore copies guest RAM from memory, and checkpoints
	// deflate against the RAM mirror: no slot write, hole punch or base read
	// touches ext4 or its journal. It also stops preserved (node-cache)
	// restores from detaching their slot (see acquireGoldenSlotOverlay), which
	// cost a 168 MiB slot copy on the node disk per warm restore.
	envTmpfsSlots = "ATEOM_MICROVM_TMPFS_SLOTS"
	// envActivationCPUWeight (1-10000) is the cgroup cpu.weight of an actor's
	// leaf while it is being activated (restored or cold-booted), until its
	// wakeup probe passes; envSteadyCPUWeight (1-10000) is its weight from then
	// on. The kernel gives every leaf 100. The actors' leaves are siblings of
	// each other and of the ateom's own leaf, so contended CPU goes to them in
	// proportion to their weights: 10000 while activating against 10 for each
	// running actor gives an activation ~1000x the share of a running guest,
	// while running actors keep sharing equally among themselves. Setting one
	// of the two makes the other default to 100.
	envActivationCPUWeight = "ATEOM_MICROVM_ACTIVATION_CPU_WEIGHT"
	envSteadyCPUWeight     = "ATEOM_MICROVM_STEADY_CPU_WEIGHT"
	// envSelfCPUWeight (1-10000) is the cpu.weight of the ateom's own leaf
	// (see ateomcgroup.Delegate), which does the host side of every activation
	// (rootfs, network and cgroup set-up, the delta overlay) beside the actors.
	envSelfCPUWeight = "ATEOM_MICROVM_SELF_CPU_WEIGHT"
	// envCheckpointConcurrency (>= 1) caps how many of this worker's
	// checkpoints deflate their memory image against the golden base at once;
	// envCheckpointNice (1-19) runs each deflation on a thread of that
	// niceness, below the ateom's own restore work. Checkpoint packaging then
	// cannot burst across many cores while activations need them. Note that a
	// deflation still delays the checkpoint (and the RPC that waits for it,
	// PauseActor included) while it queues or runs slower.
	envCheckpointConcurrency = "ATEOM_MICROVM_CHECKPOINT_CONCURRENCY"
	envCheckpointNice        = "ATEOM_MICROVM_CHECKPOINT_NICE"
	// envParallelPrep ("true") takes two steps of a restore's set-up off its
	// critical path: minting the actor's egress certificate (an RPC to atelet)
	// and rewriting the snapshot's config.json both run concurrently with the
	// rest of the set-up. The rewrite is joined before vm.restore reads the
	// config and the egress before the guest resumes, so a failed mint still
	// fails the restore before the workload runs.
	envParallelPrep = "ATEOM_MICROVM_PARALLEL_PREP"
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
	// tmpfsSlots keeps the golden-base slot pool and base mirrors on a tmpfs.
	tmpfsSlots bool
	// activationCPUWeight, steadyCPUWeight and selfCPUWeight are cgroup
	// cpu.weight values (0: unset).
	activationCPUWeight int
	steadyCPUWeight     int
	selfCPUWeight       int
	// checkpointConcurrency caps concurrent checkpoint deflations (0: no cap);
	// checkpointNice is their thread niceness (0: inherited).
	checkpointConcurrency int
	checkpointNice        int
	// parallelPrep overlaps the egress mint and the config rewrite with the
	// rest of a restore's set-up.
	parallelPrep bool
}

// enabled reports whether any knob is on, for the startup log.
func (c fastRestoreConfig) enabled() bool {
	return c.boost || c.parallelWakeup || c.memRestoreMode != "" || c.noBackgroundFetch || c.readahead || c.earlyVMMLaunch || c.tmpfsSlots ||
		c.activationCPUWeight > 0 || c.steadyCPUWeight > 0 || c.selfCPUWeight > 0 || c.checkpointConcurrency > 0 || c.checkpointNice > 0 || c.parallelPrep
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
		slog.Bool("tmpfs_slots", c.tmpfsSlots),
		slog.Int("activation_cpu_weight", c.activationCPUWeight),
		slog.Int("steady_cpu_weight", c.steadyCPUWeight),
		slog.Int("self_cpu_weight", c.selfCPUWeight),
		slog.Int("checkpoint_concurrency", c.checkpointConcurrency),
		slog.Int("checkpoint_nice", c.checkpointNice),
		slog.Bool("parallel_prep", c.parallelPrep),
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
		{envTmpfsSlots, &c.tmpfsSlots},
		{envParallelPrep, &c.parallelPrep},
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
	for _, n := range []struct {
		env    string
		dst    *int
		lo, hi int
	}{
		{envActivationCPUWeight, &c.activationCPUWeight, ateomcgroup.MinCPUWeight, ateomcgroup.MaxCPUWeight},
		{envSteadyCPUWeight, &c.steadyCPUWeight, ateomcgroup.MinCPUWeight, ateomcgroup.MaxCPUWeight},
		{envSelfCPUWeight, &c.selfCPUWeight, ateomcgroup.MinCPUWeight, ateomcgroup.MaxCPUWeight},
		{envCheckpointConcurrency, &c.checkpointConcurrency, 1, math.MaxInt32},
		{envCheckpointNice, &c.checkpointNice, 1, 19},
	} {
		if *n.dst, err = parseIntEnv(getenv(n.env), n.lo, n.hi); err != nil {
			return fastRestoreConfig{}, fmt.Errorf("%s: %w", n.env, err)
		}
	}
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

// parseIntEnv parses an integer env value in [lo, hi]; unset or "0" is 0 (off).
func parseIntEnv(v string, lo, hi int) (int, error) {
	v = strings.TrimSpace(v)
	if v == "" || v == "0" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("want an integer in [%d, %d] (0 or unset for off), got %q", lo, hi, v)
	}
	return n, nil
}

// activationWeight is the cpu.weight an actor's leaf gets while it is being
// activated, or 0 to leave the leaf's weight alone.
func (c fastRestoreConfig) activationWeight() int {
	switch {
	case c.activationCPUWeight > 0:
		return c.activationCPUWeight
	case c.steadyCPUWeight > 0:
		// A reused leaf may still hold a steady weight from its last activation.
		return ateomcgroup.DefaultCPUWeight
	}
	return 0
}

// steadyWeight is the cpu.weight an actor's leaf gets once its wakeup probe
// passes, or 0 to leave the leaf's weight alone.
func (c fastRestoreConfig) steadyWeight() int {
	switch {
	case c.steadyCPUWeight > 0:
		return c.steadyCPUWeight
	case c.activationCPUWeight > 0:
		return ateomcgroup.DefaultCPUWeight
	}
	return 0
}

// noJoin is a join for work that already ran (or never runs).
func noJoin() error { return nil }

// runAsync starts fn in its own goroutine and returns a join: it waits for fn
// the first time it is called and returns fn's error on every call.
func runAsync(fn func() error) func() error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	var once sync.Once
	var err error
	return func() error {
		once.Do(func() { err = <-done })
		return err
	}
}

// ckptGate is the opt-in throttle on checkpoint deflation (see
// envCheckpointConcurrency), set once at startup; nil means none.
var ckptGate *checkpointGate

// checkpointGate caps concurrent checkpoint work and lowers its priority.
type checkpointGate struct {
	sem  chan struct{} // nil: no cap
	nice int           // 0: inherited priority
}

// newCheckpointGate returns the gate for the given cap and niceness, or nil
// when neither is set.
func newCheckpointGate(concurrency, nice int) *checkpointGate {
	if concurrency <= 0 && nice <= 0 {
		return nil
	}
	g := &checkpointGate{nice: nice}
	if concurrency > 0 {
		g.sem = make(chan struct{}, concurrency)
	}
	return g
}

// run runs fn once a slot is free (returning how long it waited for one) and,
// with a niceness set, on a dedicated thread of that niceness. A nil gate runs
// fn directly.
func (g *checkpointGate) run(ctx context.Context, fn func() error) (waited time.Duration, err error) {
	if g == nil {
		return 0, fn()
	}
	if g.sem != nil {
		t := time.Now()
		select {
		case g.sem <- struct{}{}:
		case <-ctx.Done():
			return time.Since(t), ctx.Err()
		}
		waited = time.Since(t)
		defer func() { <-g.sem }()
	}
	if g.nice <= 0 {
		return waited, fn()
	}
	done := make(chan error, 1)
	go func() {
		// Never unlocked: the thread exits with this goroutine instead of going
		// back to the scheduler's pool with the lowered priority.
		runtime.LockOSThread()
		if err := unix.Setpriority(unix.PRIO_PROCESS, unix.Gettid(), g.nice); err != nil {
			slog.DebugContext(ctx, "could not lower the checkpoint thread's priority", slog.Any("err", err))
		}
		done <- fn()
	}()
	return waited, <-done
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
	// A tmpfs pool recycles its slots on every eager restore, preserved ones
	// included (they are never detached, see acquireGoldenSlotOverlay), so it
	// can afford a deeper pool for restore bursts: ~170 MiB of RAM per slot.
	goldenTmpfsSlotPoolCap      = 12
	goldenTmpfsSlotPrewarmCount = 8
)

// goldenTmpfsRoot is the tmpfs directory holding the golden base mirrors and
// slot pools when envTmpfsSlots is on (see setupGoldenSlotTmpfs); "" keeps
// them on the node disk next to the golden-base cache. Set once at startup,
// before any restore or checkpoint runs.
var goldenTmpfsRoot string

// linkFile is os.Link; tests swap it to simulate a cross-filesystem link.
var linkFile = os.Link

var goldenPools sync.Map // poolKey(baseFile, tmpfsRoot) -> *baseSlotPool

// goldenPrewarmStarted records the golden bases whose pool prewarm has been
// started (baseFile -> struct{}), so it is kicked off once per base.
var goldenPrewarmStarted sync.Map

type goldenSlot struct {
	path string
}

type baseSlotPool struct {
	// srcFile is the node's golden-base cache file this pool serves (the
	// immutable <ateRoot>/golden-base-cache/<baseID>/memory-ranges).
	srcFile string
	// baseFile is what slots are staged and reset from: srcFile itself, or its
	// mirror on the tmpfs (inTmpfs).
	baseFile string
	slotDir  string
	inTmpfs  bool
	prewarm  int
	idle     chan *goldenSlot
	nextID   atomic.Uint64

	// stageOnce mirrors srcFile to baseFile for a tmpfs pool; stageErr is its
	// outcome, staged reports success without waiting.
	stageOnce sync.Once
	stageErr  error
	staged    atomic.Bool
}

func poolKey(baseFile, tmpfsRoot string) string {
	return tmpfsRoot + "\x00" + baseFile
}

// newBaseSlotPool describes the pool for baseFile: on the node disk next to
// it, or under tmpfsRoot when that is set. The tmpfs directory is named after
// the base id plus a hash of baseFile, so two caches never share a mirror.
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
		// Best-effort cleanup of slot directories from dead ateom-microvm PIDs.
		// (A tmpfs pool's directory dies with this process's mount namespace.)
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

// ready stages a tmpfs pool's mirror of the golden base on first use and
// reports whether the pool can serve slots. A disk pool is always ready.
func (p *baseSlotPool) ready(ctx context.Context) bool {
	if !p.inTmpfs {
		return true
	}
	p.stageOnce.Do(func() {
		t := time.Now()
		// Shared by every caller, so not bound to the first one's deadline.
		if err := mirrorGoldenBase(context.WithoutCancel(ctx), p.srcFile, p.baseFile); err != nil {
			p.stageErr = err
			_ = os.RemoveAll(filepath.Dir(p.baseFile))
			slog.WarnContext(ctx, "Could not mirror the golden base onto the slot tmpfs; using the node disk for it",
				slog.String("base", p.srcFile), slog.Any("err", err))
			return
		}
		p.staged.Store(true)
		slog.InfoContext(ctx, "Mirrored golden base onto the slot tmpfs",
			slog.String("base", p.srcFile), slog.String("mirror", p.baseFile), slog.Duration("took", time.Since(t)))
	})
	return p.stageErr == nil
}

// mirrorGoldenBase copies the immutable golden base src to dst (temp file +
// rename), keeping its holes, unless dst already holds a copy of that size.
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

// goldenSlotPool returns the pool serving the golden base baseFile: the tmpfs
// one when envTmpfsSlots set a tmpfs up and the base could be mirrored onto it
// (mirroring it on first use), else the one on the node disk.
func goldenSlotPool(ctx context.Context, baseFile string) *baseSlotPool {
	if root := goldenTmpfsRoot; root != "" {
		if p := getOrCreateBaseSlotPool(baseFile, root); p.ready(ctx) {
			return p
		}
	}
	return getOrCreateBaseSlotPool(baseFile, "")
}

// goldenBaseReadPath is where to read the golden base baseFile from: its tmpfs
// mirror once one is staged, else baseFile itself. Never stages anything.
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

// maybePrewarmGoldenSlots asynchronously populates reusable golden-base slot
// files once baseFile exists so warm restores never copy the 168 MiB base on
// the critical path.
func maybePrewarmGoldenSlots(baseFile string) {
	if os.Getenv("ATEOM_MICROVM_DISABLE_GOLDEN_SLOT_POOL") == "true" {
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

// linkSlot makes outFile name the slot file. A hard link is used when both are
// on one filesystem; a slot on the tmpfs is on another one (EXDEV), so outFile
// becomes a symlink to it instead: cloud-hypervisor opens memory-ranges with a
// plain open(2), which follows it.
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

// materializeSlot replaces outFile (a symlink to slotPath) with a regular copy
// of the slot, atomically, so outFile stays a complete image after the slot
// file goes away.
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

// recoverInterruptedOverlay undoes what a restore that died between linking a
// slot at outFile and its commit/abort left behind when that link is a symlink
// (into a tmpfs that may be gone): it puts the original delta back.
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

// acquireGoldenSlotOverlay overlays deltaFile's populated pages onto a
// pre-staged golden-base slot file (~7 MiB write instead of a 168 MiB base copy)
// and links it at outFile for vm.restore. The original delta is kept at
// outFile+".delta.bak" until commit or abort.
//
// Once vm.restore has finished, call commit(eager, preserve), where eager says
// guest RAM was copied in full (so nothing reads outFile any more) and
// preserve says the restore directory is a preserved snapshot (for example
// the node-local actor snapshot cache). It returns whether outFile holds the
// original delta again, in which case the caller must keep the golden-delta
// marker; otherwise outFile is now a complete image (or gone) and the marker
// must go:
//
//   - eager, not preserved: outFile is unlinked and the slot's modified
//     ~7 MiB is reset in the background for reuse.
//   - eager, preserved, tmpfs pool: the delta is put back at outFile and the
//     slot is reset for reuse. A RAM-backed slot is never detached, and no
//     symlink into it is ever left behind.
//   - otherwise (OnDemand, where the VMM keeps paging from the slot inode, or a
//     preserved restore from a disk pool): the slot is detached so outFile
//     stays a complete image owned by the actor (copied off the tmpfs first if
//     outFile is a symlink), and a fresh slot is staged in the background.
//
// Defer abort() so any failure before commit restores deltaFile at outFile and
// returns the reset slot to the pool.
func acquireGoldenSlotOverlay(ctx context.Context, baseFile, deltaFile, outFile string) (commit func(eager, preserve bool) (keepDelta bool), abort func(), err error) {
	noopCommit := func(bool, bool) bool { return false }
	if os.Getenv("ATEOM_MICROVM_DISABLE_GOLDEN_SLOT_POOL") == "true" {
		if err := ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile); err != nil {
			return nil, nil, err
		}
		return noopCommit, func() {}, nil
	}
	recoverInterruptedOverlay(outFile)
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

	pool := goldenSlotPool(ctx, baseFile)
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
		slotPath := filepath.Join(pool.slotDir, fmt.Sprintf("slot-%d", pool.nextID.Add(1)))
		err := os.MkdirAll(pool.slotDir, 0o700)
		if err == nil {
			err = ch.CopySparseFileFast(ctx, pool.baseFile, slotPath)
		}
		if err != nil {
			_ = os.Remove(slotPath)
			if !pool.inTmpfs {
				return nil, nil, fmt.Errorf("staging golden slot: %w", err)
			}
			// The tmpfs is full or failing: rebuild the image on the node disk
			// as before the slot pool existed, rather than failing the restore.
			slog.WarnContext(ctx, "Could not stage a golden slot on the tmpfs; merging on the node disk",
				slog.String("base", baseFile), slog.Any("err", err))
			if err := ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile); err != nil {
				return nil, nil, err
			}
			return noopCommit, func() {}, nil
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
	symlinked, err := linkSlot(slot.path, outFile)
	if err != nil {
		_ = os.Rename(deltaBackup, outFile)
		go pool.resetAndReturnSlot(slot, dirtyRanges)
		if err := ch.MergeSparseOverlay(ctx, baseFile, deltaFile, outFile); err != nil {
			return nil, nil, err
		}
		return noopCommit, func() {}, nil
	}

	// Detaching keeps outFile a complete image without the slot file; it may
	// copy the slot, so it must not be bound to the restore's deadline.
	bg := context.WithoutCancel(ctx)
	detach := func() {
		_ = os.Remove(deltaBackup)
		if symlinked {
			if err := materializeSlot(bg, slot.path, outFile); err != nil {
				// outFile still points at the slot: leave the slot file in place.
				slog.WarnContext(ctx, "Could not copy the golden slot off the tmpfs; keeping it",
					slog.String("slot", slot.path), slog.Any("err", err))
				return
			}
		}
		_ = os.Remove(slot.path)
		go pool.replenishOneSlot()
	}
	var once sync.Once
	commitFn := func(eager, preserve bool) (keepDelta bool) {
		once.Do(func() {
			switch {
			case eager && !preserve:
				_ = os.Remove(deltaBackup)
				_ = os.Remove(outFile)
				go pool.resetAndReturnSlot(slot, dirtyRanges)
			case eager && pool.inTmpfs:
				// Guest RAM is already copied, so nothing reads outFile any more:
				// the preserved snapshot gets its own delta back.
				if err := os.Rename(deltaBackup, outFile); err != nil {
					slog.WarnContext(ctx, "Could not put the golden delta back; detaching the slot instead",
						slog.String("delta", deltaBackup), slog.Any("err", err))
					detach()
					return
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
			_ = os.Rename(deltaBackup, outFile)
			go pool.resetAndReturnSlot(slot, dirtyRanges)
		})
	}
	return commitFn, abortFn, nil
}
