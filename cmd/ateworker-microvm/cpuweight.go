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
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/sizing"
)

// Opt-in cgroup CPU priority and activation boost knobs for ateworker-microvm.
// Both ATEWORKER_MICROVM_* and legacy ATEOM_MICROVM_* environment variables are
// honored.
const (
	envRestoreCPUWeight          = "ATEWORKER_MICROVM_RESTORE_CPU_WEIGHT"
	envLegacyRestoreCPUWeight    = "ATEOM_MICROVM_RESTORE_CPU_WEIGHT"
	envActivationCPUWeight       = "ATEWORKER_MICROVM_ACTIVATION_CPU_WEIGHT"
	envLegacyActivationCPUWeight = "ATEOM_MICROVM_ACTIVATION_CPU_WEIGHT"

	envSteadyCPUWeight       = "ATEWORKER_MICROVM_STEADY_CPU_WEIGHT"
	envLegacySteadyCPUWeight = "ATEOM_MICROVM_STEADY_CPU_WEIGHT"

	envSelfCPUWeight       = "ATEWORKER_MICROVM_SELF_CPU_WEIGHT"
	envLegacySelfCPUWeight = "ATEOM_MICROVM_SELF_CPU_WEIGHT"

	envActivationCPUBoost       = "ATEWORKER_MICROVM_ACTIVATION_CPU_BOOST"
	envLegacyActivationCPUBoost = "ATEOM_MICROVM_ACTIVATION_CPU_BOOST"
)

type cpuPriorityConfig struct {
	boost               bool
	boostMilliCPU       int64
	activationCPUWeight int
	steadyCPUWeight     int
	selfCPUWeight       int
}

func (c cpuPriorityConfig) enabled() bool {
	return c.boost || c.activationCPUWeight > 0 || c.steadyCPUWeight > 0 || c.selfCPUWeight > 0
}

func (c cpuPriorityConfig) attrs() []any {
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
	}
}

func firstEnv(getenv func(string) string, keys ...string) (string, string) {
	for _, k := range keys {
		if v := strings.TrimSpace(getenv(k)); v != "" {
			return k, v
		}
	}
	if len(keys) > 0 {
		return keys[0], ""
	}
	return "", ""
}

func cpuPriorityConfigFromEnv(getenv func(string) string) (cpuPriorityConfig, error) {
	var c cpuPriorityConfig
	var err error

	boostKey, boostRaw := firstEnv(getenv, envActivationCPUBoost, envLegacyActivationCPUBoost)
	if c.boost, c.boostMilliCPU, err = parseCPUBoost(boostRaw); err != nil {
		return cpuPriorityConfig{}, fmt.Errorf("%s: %w", boostKey, err)
	}

	actKey, actRaw := firstEnv(getenv, envRestoreCPUWeight, envActivationCPUWeight, envLegacyRestoreCPUWeight, envLegacyActivationCPUWeight)
	if c.activationCPUWeight, err = parseWeightEnv(actRaw); err != nil {
		return cpuPriorityConfig{}, fmt.Errorf("%s: %w", actKey, err)
	}

	steadyKey, steadyRaw := firstEnv(getenv, envSteadyCPUWeight, envLegacySteadyCPUWeight)
	if c.steadyCPUWeight, err = parseWeightEnv(steadyRaw); err != nil {
		return cpuPriorityConfig{}, fmt.Errorf("%s: %w", steadyKey, err)
	}

	selfKey, selfRaw := firstEnv(getenv, envSelfCPUWeight, envLegacySelfCPUWeight)
	if c.selfCPUWeight, err = parseWeightEnv(selfRaw); err != nil {
		return cpuPriorityConfig{}, fmt.Errorf("%s: %w", selfKey, err)
	}

	return c, nil
}

func parseWeightEnv(v string) (int, error) {
	v = strings.TrimSpace(v)
	if v == "" || v == "0" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < cgroup.MinCPUWeight || n > cgroup.MaxCPUWeight {
		return 0, fmt.Errorf("want an integer in [%d, %d] (0 or unset for off), got %q",
			cgroup.MinCPUWeight, cgroup.MaxCPUWeight, v)
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

// activationWeight is the cpu.weight to set on the actor's leaf during
// boot/restore and checkpoint/teardown, or 0 to leave the kernel default. When
// only steadyCPUWeight is set, activation explicitly resets a reused leaf to
// DefaultCPUWeight (100) before steady lowers it.
func (c cpuPriorityConfig) activationWeight() int {
	switch {
	case c.activationCPUWeight > 0:
		return c.activationCPUWeight
	case c.steadyCPUWeight > 0:
		return cgroup.DefaultCPUWeight
	}
	return 0
}

// steadyWeight is the cpu.weight to set on the actor's leaf once its wakeup
// probe passes, or 0 if weight management is off. When only activationCPUWeight
// is set, steady restores the kernel default (100).
func (c cpuPriorityConfig) steadyWeight() int {
	switch {
	case c.steadyCPUWeight > 0:
		return c.steadyCPUWeight
	case c.activationCPUWeight > 0:
		return cgroup.DefaultCPUWeight
	}
	return 0
}

// activationSize returns the SandboxSize to open the actor's cgroup leaf with
// during boot/restore.
func (c cpuPriorityConfig) activationSize(size sizing.SandboxSize) sizing.SandboxSize {
	if !c.boost || size.MilliCPU <= 0 {
		return size
	}
	if c.boostMilliCPU == 0 {
		return sizing.SandboxSize{MilliCPU: 0, MemoryBytes: size.MemoryBytes}
	}
	return sizing.SandboxSize{MilliCPU: max(size.MilliCPU, c.boostMilliCPU), MemoryBytes: size.MemoryBytes}
}

// boosted reports whether activationSize changed the CPU cap for size.
func (c cpuPriorityConfig) boosted(size sizing.SandboxSize) bool {
	return c.activationSize(size).MilliCPU != size.MilliCPU
}
