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
	"strconv"
	"strings"
	"sync"

	"github.com/agent-substrate/substrate/internal/ateomcgroup"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/wakeupprobe"
)

// Opt-in restore/checkpoint latency knobs for ateom-gvisor. Every knob defaults
// to off, preserving baseline behavior unless its ATEOM_GVISOR_* environment
// variable is set (atecontroller forwards ATEOM_* variables to worker pods).
const (
	// envActivationCPUBoost lifts the actor sandbox's cgroup CPU cap after
	// `runsc create _pause` has sized the sentry via --cpu-num-from-quota,
	// keeping it uncapped (or raised) through pause/app restore and wakeup probe,
	// then restores the actor's declared limit before serving traffic.
	// "max" removes the cap; "<n>m" (millicores) or "<n>" (cores) sets it.
	envActivationCPUBoost = "ATEOM_GVISOR_ACTIVATION_CPU_BOOST"
	// envActivationCPUWeight (1-10000) is the cgroup cpu.weight of the sandbox
	// leaf (<actorUID>-_pause) during activation, checkpoint, and teardown.
	envActivationCPUWeight = "ATEOM_GVISOR_ACTIVATION_CPU_WEIGHT"
	// envSteadyCPUWeight (1-10000) is the cgroup cpu.weight of the sandbox leaf
	// once its wakeup probe passes.
	envSteadyCPUWeight = "ATEOM_GVISOR_STEADY_CPU_WEIGHT"
	// envSelfCPUWeight (1-10000) is the cgroup cpu.weight of the ateom's own
	// leaf (/sys/fs/cgroup/ateom), where ateom-gvisor and all runsc CLI
	// subprocesses execute.
	envSelfCPUWeight = "ATEOM_GVISOR_SELF_CPU_WEIGHT"
	// envParallelPrep ("true") overlaps egress certificate minting, sandbox
	// network setup, durable-dir untar, and pause/app rootfs overlay mounts +
	// OCI spec generation during RestoreWorkload and RunWorkload.
	envParallelPrep = "ATEOM_GVISOR_PARALLEL_PREP"
	// envFastWakeupProbe ("true") uses adaptive short per-attempt timeouts on
	// the initial wakeup probe polls so a TCP SYN dropped while `runsc restore
	// -background` finishes attaching the netstack is retried in milliseconds
	// rather than blocking for RequestTimeout (250ms).
	envFastWakeupProbe = "ATEOM_GVISOR_FAST_WAKEUP_PROBE"
	// envFastCleanup ("true") skips the pre-delete `runsc state` subprocesses in
	// cleanupContainers (`runsc delete -force` is already idempotent) and skips
	// redundant `runsc kill` + `runsc wait` after a MEMORY checkpoint has
	// already stopped the sandbox.
	envFastCleanup = "ATEOM_GVISOR_FAST_CLEANUP"
)

type fastRestoreConfig struct {
	boost               bool
	boostMilliCPU       int64
	activationCPUWeight int
	steadyCPUWeight     int
	selfCPUWeight       int
	parallelPrep        bool
	fastWakeupProbe     bool
	fastCleanup         bool
}

func (c fastRestoreConfig) enabled() bool {
	return c.boost || c.activationCPUWeight > 0 || c.steadyCPUWeight > 0 || c.selfCPUWeight > 0 ||
		c.parallelPrep || c.fastWakeupProbe || c.fastCleanup
}

func (c fastRestoreConfig) attrs() []any {
	boost := "off"
	if c.boost {
		boost = "max"
		if c.boostMilliCPU > 0 {
			boost = fmt.Sprintf("%dm", c.boostMilliCPU)
		}
	}
	return []any{
		slog.String("activation_cpu_boost", boost),
		slog.Int("activation_cpu_weight", c.activationCPUWeight),
		slog.Int("steady_cpu_weight", c.steadyCPUWeight),
		slog.Int("self_cpu_weight", c.selfCPUWeight),
		slog.Bool("parallel_prep", c.parallelPrep),
		slog.Bool("fast_wakeup_probe", c.fastWakeupProbe),
		slog.Bool("fast_cleanup", c.fastCleanup),
	}
}

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
		{envParallelPrep, &c.parallelPrep},
		{envFastWakeupProbe, &c.fastWakeupProbe},
		{envFastCleanup, &c.fastCleanup},
	} {
		if *b.dst, err = parseBoolEnv(getenv(b.env), false); err != nil {
			return fastRestoreConfig{}, fmt.Errorf("%s: %w", b.env, err)
		}
	}
	for _, n := range []struct {
		env    string
		dst    *int
		lo, hi int
	}{
		{envActivationCPUWeight, &c.activationCPUWeight, ateomcgroup.MinCPUWeight, ateomcgroup.MaxCPUWeight},
		{envSteadyCPUWeight, &c.steadyCPUWeight, ateomcgroup.MinCPUWeight, ateomcgroup.MaxCPUWeight},
		{envSelfCPUWeight, &c.selfCPUWeight, ateomcgroup.MinCPUWeight, ateomcgroup.MaxCPUWeight},
	} {
		if *n.dst, err = parseIntEnv(getenv(n.env), n.lo, n.hi); err != nil {
			return fastRestoreConfig{}, fmt.Errorf("%s: %w", n.env, err)
		}
	}
	return c, nil
}

func parseBoolEnv(v string, def bool) (bool, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return def, nil
	}
	return strconv.ParseBool(v)
}

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

func (c fastRestoreConfig) activationWeight() int {
	switch {
	case c.activationCPUWeight > 0:
		return c.activationCPUWeight
	case c.steadyCPUWeight > 0:
		return ateomcgroup.DefaultCPUWeight
	}
	return 0
}

func (c fastRestoreConfig) steadyWeight() int {
	switch {
	case c.steadyCPUWeight > 0:
		return c.steadyCPUWeight
	case c.activationCPUWeight > 0:
		return ateomcgroup.DefaultCPUWeight
	}
	return 0
}

func (c fastRestoreConfig) activationMilliCPU(actorMilli int64) int64 {
	if !c.boost || actorMilli <= 0 {
		return actorMilli
	}
	if c.boostMilliCPU == 0 {
		return 0
	}
	return max(actorMilli, c.boostMilliCPU)
}

func (c fastRestoreConfig) boosted(actorMilli int64) bool {
	return c.activationMilliCPU(actorMilli) != actorMilli
}

func noJoin() error { return nil }

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

func (s *AteomService) cgroupRootDir() string {
	if s.cgroupRoot != "" {
		return s.cgroupRoot
	}
	return defaultCgroupRoot
}

func sandboxCgroupLeafName(actorUID string) string {
	return ocispec.GVisorCgroupLeaf(actorUID, sandboxCgroupContainer)
}

// applyActivationCgroup elevates the sandbox cgroup leaf (<actorUID>-_pause)
// right after `runsc create _pause` has created the leaf and sized the sentry's
// vCPU count from the steady quota via --cpu-num-from-quota.
func (s *AteomService) applyActivationCgroup(ctx context.Context, actorUID string, actorMilli int64) {
	if !s.actorCgroups {
		return
	}
	leaf := sandboxCgroupLeafName(actorUID)
	root := s.cgroupRootDir()
	if w := s.fast.activationWeight(); w > 0 {
		if err := ateomcgroup.SetActorLeafCPUWeightUnder(root, leaf, w); err != nil {
			slog.WarnContext(ctx, "Could not set activation cpu.weight on sandbox cgroup",
				slog.String("actorUID", actorUID), slog.Int("weight", w), slog.Any("err", err))
		}
	}
	if s.fast.boosted(actorMilli) {
		boostMilli := s.fast.activationMilliCPU(actorMilli)
		if err := ateomcgroup.SetActorLeafCPULimitUnder(root, leaf, boostMilli); err != nil {
			slog.WarnContext(ctx, "Could not set activation cpu.max on sandbox cgroup",
				slog.String("actorUID", actorUID), slog.Int64("milliCPU", boostMilli), slog.Any("err", err))
		}
	}
}

// applySteadyCgroup restores the actor's declared CPU limit and steady
// cpu.weight once the wakeup probe passes.
func (s *AteomService) applySteadyCgroup(ctx context.Context, actorUID string, actorMilli int64) {
	if !s.actorCgroups {
		return
	}
	leaf := sandboxCgroupLeafName(actorUID)
	root := s.cgroupRootDir()
	if s.fast.boosted(actorMilli) {
		if err := ateomcgroup.SetActorLeafCPULimitUnder(root, leaf, actorMilli); err != nil {
			slog.WarnContext(ctx, "Could not restore steady cpu.max on sandbox cgroup",
				slog.String("actorUID", actorUID), slog.Int64("milliCPU", actorMilli), slog.Any("err", err))
		}
	}
	if w := s.fast.steadyWeight(); w > 0 {
		if err := ateomcgroup.SetActorLeafCPUWeightUnder(root, leaf, w); err != nil {
			slog.WarnContext(ctx, "Could not set steady cpu.weight on sandbox cgroup",
				slog.String("actorUID", actorUID), slog.Int("weight", w), slog.Any("err", err))
		}
	}
}

// raiseSandboxCgroupForTeardown elevates the sandbox cgroup's cpu.weight and
// cpu.max before checkpointing or tearing down the workload so `runsc
// checkpoint` (which serializes memory inside the sentry) and `runsc delete`
// are not throttled by a fractional steady CPU limit.
func (s *AteomService) raiseSandboxCgroupForTeardown(ctx context.Context, actorUID string) {
	if !s.actorCgroups {
		return
	}
	leaf := sandboxCgroupLeafName(actorUID)
	root := s.cgroupRootDir()
	if w := s.fast.activationWeight(); w > 0 {
		if err := ateomcgroup.SetActorLeafCPUWeightUnder(root, leaf, w); err != nil {
			slog.WarnContext(ctx, "Could not raise sandbox cpu.weight for checkpoint/teardown",
				slog.String("actorUID", actorUID), slog.Int("weight", w), slog.Any("err", err))
		}
	}
	if s.fast.boost {
		if err := ateomcgroup.SetActorLeafCPULimitUnder(root, leaf, s.fast.boostMilliCPU); err != nil {
			slog.WarnContext(ctx, "Could not raise sandbox cpu.max for checkpoint/teardown",
				slog.String("actorUID", actorUID), slog.Int64("milliCPU", s.fast.boostMilliCPU), slog.Any("err", err))
		}
	}
}

// restoreSteadyCgroupByUID puts the running actor's sandbox cgroup back to its
// steady cpu.max and cpu.weight after a VOLUMES checkpoint or failed MEMORY
// checkpoint.
func (s *AteomService) restoreSteadyCgroupByUID(ctx context.Context, actorUID string) {
	if !s.actorCgroups {
		return
	}
	var actorMilli int64
	s.actorsMu.RLock()
	if h := s.actors[actorUID]; h != nil && h.session != nil && h.session.rcmd != nil {
		actorMilli = h.session.rcmd.size.MilliCPU
	}
	s.actorsMu.RUnlock()
	s.applySteadyCgroup(ctx, actorUID, actorMilli)
}

func (s *AteomService) waitForWakeupProbes(ctx context.Context, containers []*ateompb.Container, actorUID string) error {
	dial := wakeupprobe.DialFunc(s.sandboxDialer(actorUID))
	if s.fast.fastWakeupProbe {
		return wakeupprobe.WaitAllFast(ctx, containers, ateomnet.ActorVethIP, dial)
	}
	return wakeupprobe.WaitAll(ctx, containers, ateomnet.ActorVethIP, dial)
}
