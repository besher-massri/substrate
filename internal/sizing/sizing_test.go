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

package sizing

import (
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestFromLimits(t *testing.T) {
	got := FromLimits(&ateworkerpb.Resources{CpuMillicores: 1500, MemoryBytes: 2147483648})
	if got.MilliCPU != 1500 || got.MemoryBytes != 2147483648 {
		t.Fatalf("FromLimits() = %+v", got)
	}
}

func TestFromLimitsClampsNegative(t *testing.T) {
	got := FromLimits(&ateworkerpb.Resources{CpuMillicores: -5, MemoryBytes: -1})
	if got.MilliCPU != 0 || got.MemoryBytes != 0 {
		t.Fatalf("FromLimits() = %+v, want zero", got)
	}
}

func TestVCPUs(t *testing.T) {
	cases := []struct {
		milli int64
		want  int
	}{
		{0, 0},
		{1, 1},
		{999, 1},
		{1000, 1},
		{1001, 2},
		{2500, 3},
		{4000, 4},
	}
	for _, c := range cases {
		if got := (SandboxSize{MilliCPU: c.milli}).VCPUs(); got != c.want {
			t.Errorf("VCPUs(%d) = %d, want %d", c.milli, got, c.want)
		}
	}
}

func TestApplyToOCISpec(t *testing.T) {
	spec := &specs.Spec{}
	(SandboxSize{MilliCPU: 2000, MemoryBytes: 1073741824}).ApplyToOCISpec(spec)

	if spec.Linux == nil || spec.Linux.Resources == nil {
		t.Fatal("resources not set")
	}
	cpu := spec.Linux.Resources.CPU
	if cpu == nil || cpu.Quota == nil || cpu.Period == nil {
		t.Fatal("cpu not set")
	}
	if *cpu.Period != cpuQuotaPeriodMicros || *cpu.Quota != 2*cpuQuotaPeriodMicros {
		t.Errorf("cpu = quota %d period %d", *cpu.Quota, *cpu.Period)
	}
	mem := spec.Linux.Resources.Memory
	if mem == nil || mem.Limit == nil || *mem.Limit != 1073741824 {
		t.Errorf("memory limit not set correctly: %+v", mem)
	}
}

func TestApplyToOCISpecPreservesExistingAndSkipsUnset(t *testing.T) {
	shares := uint64(1024)
	spec := &specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{
		CPU: &specs.LinuxCPU{Shares: &shares},
	}}}
	// Only memory set; CPU limit unset must not clobber existing shares and must
	// not add a quota.
	(SandboxSize{MemoryBytes: 512}).ApplyToOCISpec(spec)

	if spec.Linux.Resources.CPU.Shares == nil || *spec.Linux.Resources.CPU.Shares != 1024 {
		t.Error("existing cpu shares clobbered")
	}
	if spec.Linux.Resources.CPU.Quota != nil {
		t.Error("cpu quota set despite unset MilliCPU")
	}
	if spec.Linux.Resources.Memory == nil || *spec.Linux.Resources.Memory.Limit != 512 {
		t.Error("memory limit not applied")
	}
}

func TestApplyToOCISpecNoopWhenEmpty(t *testing.T) {
	spec := &specs.Spec{}
	(SandboxSize{}).ApplyToOCISpec(spec)
	if spec.Linux != nil {
		t.Error("empty SandboxSize mutated spec")
	}
}

func TestParseCPULimitFloorMilli(t *testing.T) {
	cases := []struct {
		raw   string
		want  int64
		valid bool
	}{
		{"", 0, true},
		{"0", 0, true},
		{"off", 0, true},
		{"false", 0, true},
		{"none", 0, true},
		{"disabled", 0, true},
		{"1", 1000, true},
		{"1.5", 1500, true},
		{"0.25", 250, true},
		{"1000m", 1000, true},
		{" 500M ", 500, true},
		{"0m", 0, true},
		{"-1", 0, false},
		{"-250m", 0, false},
		{"invalid", 0, false},
		{"NaN", 0, false},
		{"Inf", 0, false},
		{"1e30", 0, false},
		{"999999999999999999m", 0, false},
	}
	for _, tc := range cases {
		got, ok := ParseCPULimitFloorMilli(tc.raw)
		if ok != tc.valid || got != tc.want {
			t.Errorf("ParseCPULimitFloorMilli(%q) = (%d, %v), want (%d, %v)",
				tc.raw, got, ok, tc.want, tc.valid)
		}
	}
}

func TestApplyToOCISpecWithCPUBurstFloor(t *testing.T) {
	t.Run("default unset preserves fractional quota", func(t *testing.T) {
		spec := &specs.Spec{}
		size := SandboxSize{MilliCPU: 250, MemoryBytes: 536870912}
		size.ApplyToOCISpec(spec)
		if got := *spec.Linux.Resources.CPU.Quota; got != 25000 {
			t.Fatalf("default Quota for 250m = %d, want 25000", got)
		}
		if size.MilliCPU != 250 {
			t.Fatalf("size.MilliCPU mutated to %d, want 250", size.MilliCPU)
		}
	})

	t.Run("ATE_ENABLE_ACTOR_CPU_BURST_FLOOR=true enforces 1000m floor on sub-core actor", func(t *testing.T) {
		t.Setenv(EnvEnableActorCPUBurstFloor, "true")
		spec := &specs.Spec{}
		size := SandboxSize{MilliCPU: 250, MemoryBytes: 536870912}
		size.ApplyToOCISpec(spec)
		if got := *spec.Linux.Resources.CPU.Quota; got != 100000 {
			t.Fatalf("Quota with burst floor for 250m = %d, want 100000 (1 core)", got)
		}
		// Proportional MilliCPU on SandboxSize stays 250m.
		if size.MilliCPU != 250 {
			t.Fatalf("size.MilliCPU = %d, want 250", size.MilliCPU)
		}
		// Above-floor actor (2000m) keeps its 2000m quota.
		spec2 := &specs.Spec{}
		(SandboxSize{MilliCPU: 2000}).ApplyToOCISpec(spec2)
		if got := *spec2.Linux.Resources.CPU.Quota; got != 200000 {
			t.Fatalf("Quota with burst floor for 2000m = %d, want 200000", got)
		}
	})

	t.Run("ATE_ACTOR_CPU_LIMIT_FLOOR=1 and 500m and explicit disable override", func(t *testing.T) {
		t.Setenv(EnvActorCPULimitFloor, "1")
		spec := &specs.Spec{}
		(SandboxSize{MilliCPU: 250}).ApplyToOCISpec(spec)
		if got := *spec.Linux.Resources.CPU.Quota; got != 100000 {
			t.Fatalf("Quota with ATE_ACTOR_CPU_LIMIT_FLOOR=1 for 250m = %d, want 100000", got)
		}

		t.Setenv(EnvActorCPULimitFloor, "500m")
		spec500 := &specs.Spec{}
		(SandboxSize{MilliCPU: 250}).ApplyToOCISpec(spec500)
		if got := *spec500.Linux.Resources.CPU.Quota; got != 50000 {
			t.Fatalf("Quota with ATE_ACTOR_CPU_LIMIT_FLOOR=500m for 250m = %d, want 50000", got)
		}

		// Explicit "0" on ATE_ACTOR_CPU_LIMIT_FLOOR overrides ATE_ENABLE_ACTOR_CPU_BURST_FLOOR=true.
		t.Setenv(EnvEnableActorCPUBurstFloor, "true")
		t.Setenv(EnvActorCPULimitFloor, "0")
		specDisabled := &specs.Spec{}
		(SandboxSize{MilliCPU: 250}).ApplyToOCISpec(specDisabled)
		if got := *specDisabled.Linux.Resources.CPU.Quota; got != 25000 {
			t.Fatalf("Quota with ATE_ACTOR_CPU_LIMIT_FLOOR=0 override for 250m = %d, want 25000", got)
		}
	})
}

func BenchmarkFromLimitsWithBurstFloor(b *testing.B) {
	limits := &ateworkerpb.Resources{CpuMillicores: 250, MemoryBytes: 536870912}
	b.Run("floor=disabled", func(b *testing.B) {
		b.Setenv(EnvActorCPULimitFloor, "")
		b.Setenv(EnvEnableActorCPUBurstFloor, "")
		spec := &specs.Spec{}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			size := FromLimits(limits)
			size.ApplyToOCISpec(spec)
		}
	})

	b.Run("floor=1000m", func(b *testing.B) {
		b.Setenv(EnvActorCPULimitFloor, "1000m")
		b.Setenv(EnvEnableActorCPUBurstFloor, "")
		spec := &specs.Spec{}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			size := FromLimits(limits)
			size.ApplyToOCISpec(spec)
		}
	})
}
