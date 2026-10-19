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
	"testing"

	"go.ate.dev/substrate/internal/sizing"
)

func TestCPUPriorityConfigFromEnv(t *testing.T) {
	t.Run("defaults all off", func(t *testing.T) {
		cfg, err := cpuPriorityConfigFromEnv(func(string) string { return "" })
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.enabled() {
			t.Errorf("default config enabled() = true, want false: %+v", cfg)
		}
		if cfg.activationWeight() != 0 || cfg.steadyWeight() != 0 {
			t.Errorf("weights = (%d, %d), want (0, 0)", cfg.activationWeight(), cfg.steadyWeight())
		}
	})

	t.Run("ATEWORKER_MICROVM_RESTORE_CPU_WEIGHT and STEADY_CPU_WEIGHT", func(t *testing.T) {
		env := map[string]string{
			envRestoreCPUWeight:   "10000",
			envSteadyCPUWeight:    "100",
			envSelfCPUWeight:      "10000",
			envActivationCPUBoost: "max",
		}
		cfg, err := cpuPriorityConfigFromEnv(func(k string) string { return env[k] })
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cfg.enabled() {
			t.Fatal("cfg.enabled() = false, want true")
		}
		if cfg.activationWeight() != 10000 || cfg.steadyWeight() != 100 {
			t.Errorf("weights = (%d, %d), want (10000, 100)", cfg.activationWeight(), cfg.steadyWeight())
		}
		size := sizing.SandboxSize{MilliCPU: 250, MemoryBytes: 512 << 20}
		if !cfg.boosted(size) || cfg.activationSize(size).MilliCPU != 0 {
			t.Errorf("activationSize(250m) = %+v (boosted=%v), want MilliCPU=0 boosted=true",
				cfg.activationSize(size), cfg.boosted(size))
		}
	})

	t.Run("legacy ATEOM_MICROVM_* fallback", func(t *testing.T) {
		env := map[string]string{
			envLegacyRestoreCPUWeight:    "5000",
			envLegacySteadyCPUWeight:     "50",
			envLegacyActivationCPUBoost:  "2000m",
		}
		cfg, err := cpuPriorityConfigFromEnv(func(k string) string { return env[k] })
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.activationWeight() != 5000 || cfg.steadyWeight() != 50 {
			t.Errorf("weights = (%d, %d), want (5000, 50)", cfg.activationWeight(), cfg.steadyWeight())
		}
		size := sizing.SandboxSize{MilliCPU: 250}
		if got := cfg.activationSize(size).MilliCPU; got != 2000 {
			t.Errorf("activationSize(250m) with 2000m boost = %d, want 2000", got)
		}
	})

	t.Run("invalid weights rejected", func(t *testing.T) {
		for _, env := range []map[string]string{
			{envRestoreCPUWeight: "10001"},
			{envSteadyCPUWeight: "-5"},
			{envSelfCPUWeight: "notanumber"},
			{envActivationCPUBoost: "-100m"},
		} {
			if _, err := cpuPriorityConfigFromEnv(func(k string) string { return env[k] }); err == nil {
				t.Errorf("cpuPriorityConfigFromEnv(%v) succeeded, want error", env)
			}
		}
	})
}
