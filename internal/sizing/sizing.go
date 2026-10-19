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

// Package sizing right-sizes a sandbox to the actor's declared resource limits.
// The actor's limits arrive over the worker RPCs (RunWorkload / RestoreWorkload)
// as a SandboxSize. ateworker-gvisor calls ApplyToOCISpec to write them into the
// sandbox container's OCI spec, which runsc then applies to the host cgroup
// leaf. ateworker-microvm does not call ApplyToOCISpec: it uses SandboxSize only to
// size the VM itself (VCPUs, and MemoryBytes via resolveGuestMemMiB). A
// micro-VM container's own cgroup limit comes instead from that container's
// declared `spec.containers[].resources`, written into its OCI spec by atelet.
package sizing

import (
	"math"
	"os"
	"strconv"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	// cpuQuotaPeriodMicros is the cgroup v2 cpu.max period (100ms, the kernel
	// default) against which the CPU quota is expressed.
	cpuQuotaPeriodMicros = 100000

	// DefaultBurstFloorMilliCPU is the 1-core (1000m) minimum container CFS CPU
	// quota floor applied when ATE_ENABLE_ACTOR_CPU_BURST_FLOOR=true. Enforcing a
	// 1-core minimum CFS quota on sub-core actor containers prevents multi-thread
	// runtime startups (e.g. Go runtime initialization and gVisor systrap threads)
	// from exhausting a 25ms/100ms quota mid-boot while keeping pod scheduling
	// requests proportional.
	DefaultBurstFloorMilliCPU int64 = 1000

	// EnvActorCPULimitFloor configures an explicit minimum container CFS CPU
	// limit floor (e.g. "1", "1000m", "500m").
	EnvActorCPULimitFloor = "ATE_ACTOR_CPU_LIMIT_FLOOR"

	// EnvEnableActorCPUBurstFloor enables the default 1-core (1000m) minimum
	// container CFS CPU limit floor when set to a truthy boolean ("true", "1").
	EnvEnableActorCPUBurstFloor = "ATE_ENABLE_ACTOR_CPU_BURST_FLOOR"
)

// ParseCPULimitFloorMilli parses a CPU limit floor string ("1", "1000m", "0.5")
// into millicores. Empty, "0", "off", "false", or "none" returns 0 (disabled).
func ParseCPULimitFloorMilli(raw string) (int64, bool) {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch v {
	case "", "0", "off", "false", "none", "disabled":
		return 0, true
	}
	if m, ok := strings.CutSuffix(v, "m"); ok {
		n, err := strconv.ParseInt(strings.TrimSpace(m), 10, 64)
		if err != nil || n < 0 {
			return 0, false
		}
		return n, true
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return int64(math.Ceil(f * 1000)), true
}

// ConfiguredCPULimitFloorMilli returns the opt-in minimum container CFS CPU
// limit floor in millicores from ATE_ACTOR_CPU_LIMIT_FLOOR or
// ATE_ENABLE_ACTOR_CPU_BURST_FLOOR, or 0 when neither is enabled.
func ConfiguredCPULimitFloorMilli() int64 {
	if raw := strings.TrimSpace(os.Getenv(EnvActorCPULimitFloor)); raw != "" {
		if floor, ok := ParseCPULimitFloorMilli(raw); ok {
			return floor
		}
	}
	if raw := strings.TrimSpace(os.Getenv(EnvEnableActorCPUBurstFloor)); raw != "" {
		if enabled, err := strconv.ParseBool(raw); err == nil && enabled {
			return DefaultBurstFloorMilliCPU
		}
	}
	return 0
}

// EffectiveContainerMilliCPU returns milliCPU raised to at least the configured
// opt-in CFS burst floor when milliCPU > 0. An unset (<= 0) limit stays 0.
func EffectiveContainerMilliCPU(milliCPU int64) int64 {
	return EffectiveContainerMilliCPUWithFloor(milliCPU, ConfiguredCPULimitFloorMilli())
}

// EffectiveContainerMilliCPUWithFloor returns milliCPU raised to at least
// floorMilliCPU when both milliCPU > 0 and floorMilliCPU > 0.
func EffectiveContainerMilliCPUWithFloor(milliCPU, floorMilliCPU int64) int64 {
	if milliCPU <= 0 {
		return 0
	}
	if floorMilliCPU > milliCPU {
		return floorMilliCPU
	}
	return milliCPU
}

// SandboxSize is the sandbox's target size, derived from the actor's declared
// resource limits. A zero field means "unset".
type SandboxSize struct {
	// MilliCPU is the CPU limit in millicores (1000 = one core), or 0 if unset.
	MilliCPU int64
	// MemoryBytes is the memory limit in bytes, or 0 if unset.
	MemoryBytes int64
}

// FromLimits builds a SandboxSize from an actor's declared limits (millicores and
// bytes) as carried on the worker RPCs. It is runtime-agnostic; both ateworker-gvisor
// and ateworker-microvm call it. Negative values are clamped to zero ("unset").
func FromLimits(milliCPU, memoryBytes int64) SandboxSize {
	if milliCPU < 0 {
		milliCPU = 0
	}
	if memoryBytes < 0 {
		memoryBytes = 0
	}
	return SandboxSize{MilliCPU: milliCPU, MemoryBytes: memoryBytes}
}

// VCPUs converts the CPU limit to a whole vCPU count for the micro-VM, rounding
// up so a fractional limit still yields a usable core (minimum 1 when a limit is
// set). Returns 0 when the CPU limit is unset, letting the caller keep its
// default.
func (s SandboxSize) VCPUs() int {
	if s.MilliCPU <= 0 {
		return 0
	}
	v := (s.MilliCPU + 999) / 1000
	if v < 1 {
		v = 1
	}
	return int(v)
}

// ApplyToOCISpec writes the pod's CPU/memory limits into the container OCI spec's
// linux.resources so the sandbox cgroup is created with the right values. This is
// the gVisor sandbox-cgroup path: only ateworker-gvisor calls it, so runsc applies the
// result to the host cgroup leaf. Fields that are unset in SandboxSize are left
// untouched, preserving any existing values on the spec. When an opt-in CPU burst
// floor is configured via ATE_ACTOR_CPU_LIMIT_FLOOR or
// ATE_ENABLE_ACTOR_CPU_BURST_FLOOR, sub-floor positive CPU limits are raised to
// the floor for the container CFS quota while s.MilliCPU remains unchanged.
func (s SandboxSize) ApplyToOCISpec(spec *specs.Spec) {
	s.ApplyToOCISpecWithFloor(spec, ConfiguredCPULimitFloorMilli())
}

// ApplyToOCISpecWithFloor is ApplyToOCISpec with an explicit minimum millicore
// floor for the container's CFS quota. A floorMilliCPU <= 0 applies s.MilliCPU
// as-is.
func (s SandboxSize) ApplyToOCISpecWithFloor(spec *specs.Spec, floorMilliCPU int64) {
	if s.MilliCPU <= 0 && s.MemoryBytes <= 0 {
		return
	}
	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	if spec.Linux.Resources == nil {
		spec.Linux.Resources = &specs.LinuxResources{}
	}
	if s.MilliCPU > 0 {
		if spec.Linux.Resources.CPU == nil {
			spec.Linux.Resources.CPU = &specs.LinuxCPU{}
		}
		effectiveMilli := EffectiveContainerMilliCPUWithFloor(s.MilliCPU, floorMilliCPU)
		period := uint64(cpuQuotaPeriodMicros)
		quota := effectiveMilli * cpuQuotaPeriodMicros / 1000
		spec.Linux.Resources.CPU.Period = &period
		spec.Linux.Resources.CPU.Quota = &quota
	}
	if s.MemoryBytes > 0 {
		if spec.Linux.Resources.Memory == nil {
			spec.Linux.Resources.Memory = &specs.LinuxMemory{}
		}
		limit := s.MemoryBytes
		spec.Linux.Resources.Memory.Limit = &limit
	}
}
