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
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
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
	// restoreMemMode's version check), "copy", or "ondemand". "ondemand" is only
	// honored on a VMM that does not prefault unconditionally, or on one that
	// can be told not to (see envOnDemandBackgroundFetch); otherwise ateom logs
	// and restores eagerly, because an unconditional prefault both starves the
	// guest and makes vm.snapshot fail until it completes.
	envMemoryRestoreMode = "ATEOM_MICROVM_MEMORY_RESTORE_MODE"
	// envOnDemandBackgroundFetch ("false") asks the VMM to serve only the pages
	// the guest touches after an OnDemand restore, with no background prefault
	// of the rest. It needs a cloud-hypervisor that advertises
	// ch.FeatureRestoreBackgroundFetchPages in vmm.ping.
	envOnDemandBackgroundFetch = "ATEOM_MICROVM_ONDEMAND_BACKGROUND_FETCH"
	// envRestoreReadahead ("true") asks the kernel to start reading the memory
	// image into the page cache (POSIX_FADV_WILLNEED) as soon as a restore
	// starts, so the disk reads overlap the rootfs, network and VMM set-up
	// instead of landing inside vm.restore.
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
	default:
		return fastRestoreConfig{}, fmt.Errorf("%s: want auto, copy or ondemand, got %q", envMemoryRestoreMode, v)
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

// readaheadMemoryImage asks the kernel to start reading path into the page
// cache now (POSIX_FADV_WILLNEED). It returns immediately; the reads happen in
// the background. Errors are only logged: this is an optimization.
func readaheadMemoryImage(ctx context.Context, path string) {
	f, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.DebugContext(ctx, "restore readahead: open failed", slog.String("path", path), slog.Any("err", err))
		}
		return
	}
	defer f.Close()
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_WILLNEED); err != nil {
		slog.DebugContext(ctx, "restore readahead: fadvise failed", slog.String("path", path), slog.Any("err", err))
	}
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
