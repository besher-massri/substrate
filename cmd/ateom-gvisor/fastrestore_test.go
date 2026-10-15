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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/sizing"
)

func TestFastRestoreConfigFromEnv(t *testing.T) {
	t.Run("defaults all off", func(t *testing.T) {
		cfg, err := fastRestoreConfigFromEnv(func(string) string { return "" })
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.enabled() {
			t.Errorf("default config enabled() = true, want false: %+v", cfg)
		}
		if cfg.activationWeight() != 0 || cfg.steadyWeight() != 0 {
			t.Errorf("weights = (%d, %d), want (0, 0)", cfg.activationWeight(), cfg.steadyWeight())
		}
		if cfg.boosted(250) {
			t.Error("boosted(250) = true on default config")
		}
	})

	t.Run("max boost and all flags enabled", func(t *testing.T) {
		env := map[string]string{
			envActivationCPUBoost:  "max",
			envActivationCPUWeight: "10000",
			envSteadyCPUWeight:     "100",
			envSelfCPUWeight:       "10000",
			envParallelPrep:        "true",
			envFastWakeupProbe:     "true",
			envFastCleanup:         "true",
		}
		cfg, err := fastRestoreConfigFromEnv(func(k string) string { return env[k] })
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cfg.enabled() {
			t.Fatal("cfg.enabled() = false, want true")
		}
		if !cfg.boost || cfg.boostMilliCPU != 0 {
			t.Errorf("boost = (%v, %d), want (true, 0)", cfg.boost, cfg.boostMilliCPU)
		}
		if got := cfg.activationMilliCPU(250); got != 0 {
			t.Errorf("activationMilliCPU(250) = %d, want 0 (unlimited)", got)
		}
		if !cfg.boosted(250) {
			t.Error("boosted(250) = false, want true")
		}
		if cfg.activationWeight() != 10000 || cfg.steadyWeight() != 100 || cfg.selfCPUWeight != 10000 {
			t.Errorf("weights = (%d, %d, %d), want (10000, 100, 10000)",
				cfg.activationWeight(), cfg.steadyWeight(), cfg.selfCPUWeight)
		}
		if !cfg.parallelPrep || !cfg.fastWakeupProbe || !cfg.fastCleanup {
			t.Errorf("bool flags = (%v, %v, %v), want all true",
				cfg.parallelPrep, cfg.fastWakeupProbe, cfg.fastCleanup)
		}
	})

	t.Run("millicores and cores boost parsing", func(t *testing.T) {
		cfg, err := fastRestoreConfigFromEnv(func(k string) string {
			if k == envActivationCPUBoost {
				return "2000m"
			}
			return ""
		})
		if err != nil {
			t.Fatalf("2000m: %v", err)
		}
		if got := cfg.activationMilliCPU(250); got != 2000 {
			t.Errorf("activationMilliCPU(250) = %d, want 2000", got)
		}
		if got := cfg.activationMilliCPU(4000); got != 4000 {
			t.Errorf("activationMilliCPU(4000) = %d, want 4000 (never lowers steady cap)", got)
		}

		cfgCores, err := fastRestoreConfigFromEnv(func(k string) string {
			if k == envActivationCPUBoost {
				return "1.5"
			}
			return ""
		})
		if err != nil {
			t.Fatalf("1.5 cores: %v", err)
		}
		if cfgCores.boostMilliCPU != 1500 {
			t.Errorf("boostMilliCPU = %d, want 1500", cfgCores.boostMilliCPU)
		}
	})

	t.Run("invalid values rejected", func(t *testing.T) {
		for _, env := range []map[string]string{
			{envActivationCPUBoost: "bogus"},
			{envActivationCPUWeight: "10001"},
			{envSteadyCPUWeight: "-5"},
			{envSelfCPUWeight: "abc"},
			{envParallelPrep: "notabool"},
			{envFastWakeupProbe: "notabool"},
			{envFastCleanup: "notabool"},
		} {
			if _, err := fastRestoreConfigFromEnv(func(k string) string { return env[k] }); err == nil {
				t.Errorf("fastRestoreConfigFromEnv(%v) succeeded, want error", env)
			}
		}
	})
}

func TestSandboxCgroupLifecycleBoostAndSteadyRestore(t *testing.T) {
	root := t.TempDir()
	actorUID := "uid-gvisor-1"
	leafDir := filepath.Join(root, sandboxCgroupLeafName(actorUID))
	if err := os.MkdirAll(leafDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cpuMaxPath := filepath.Join(leafDir, "cpu.max")
	cpuWeightPath := filepath.Join(leafDir, "cpu.weight")
	// Simulate runsc create _pause having written the steady 250m quota and default weight 100.
	if err := os.WriteFile(cpuMaxPath, []byte("25000 100000"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cpuWeightPath, []byte("100"), 0o644); err != nil {
		t.Fatal(err)
	}

	s := &AteomService{
		cgroupRoot:   root,
		actorCgroups: true,
		actors: map[string]*hostedActor{
			actorUID: {
				session: &workloadSession{
					rcmd: &runsc{size: sizing.SandboxSize{MilliCPU: 250}},
				},
			},
		},
		fast: fastRestoreConfig{
			boost:               true,
			boostMilliCPU:       0, // max
			activationCPUWeight: 10000,
			steadyCPUWeight:     100,
		},
	}

	ctx := context.Background()

	// 1. After runsc create _pause: applyActivationCgroup lifts cpu.max to max and cpu.weight to 10000.
	s.applyActivationCgroup(ctx, actorUID, 250)
	if got, _ := os.ReadFile(cpuMaxPath); string(got) != "max 100000" {
		t.Errorf("activation cpu.max = %q, want \"max 100000\"", got)
	}
	if got, _ := os.ReadFile(cpuWeightPath); string(got) != "10000" {
		t.Errorf("activation cpu.weight = %q, want \"10000\"", got)
	}

	// 2. After wakeup probe: applySteadyCgroup restores 250m cap and weight 100.
	s.applySteadyCgroup(ctx, actorUID, 250)
	if got, _ := os.ReadFile(cpuMaxPath); string(got) != "25000 100000" {
		t.Errorf("steady cpu.max = %q, want \"25000 100000\"", got)
	}
	if got, _ := os.ReadFile(cpuWeightPath); string(got) != "100" {
		t.Errorf("steady cpu.weight = %q, want \"100\"", got)
	}

	// 3. Before checkpoint/teardown: raiseSandboxCgroupForTeardown lifts cap and weight again.
	s.raiseSandboxCgroupForTeardown(ctx, actorUID)
	if got, _ := os.ReadFile(cpuMaxPath); string(got) != "max 100000" {
		t.Errorf("teardown cpu.max = %q, want \"max 100000\"", got)
	}
	if got, _ := os.ReadFile(cpuWeightPath); string(got) != "10000" {
		t.Errorf("teardown cpu.weight = %q, want \"10000\"", got)
	}

	// 4. If checkpoint fails before teardown: restoreSteadyCgroupByUID restores steady limits.
	s.restoreSteadyCgroupByUID(ctx, actorUID)
	if got, _ := os.ReadFile(cpuMaxPath); string(got) != "25000 100000" {
		t.Errorf("restored steady cpu.max = %q, want \"25000 100000\"", got)
	}
	if got, _ := os.ReadFile(cpuWeightPath); string(got) != "100" {
		t.Errorf("restored steady cpu.weight = %q, want \"100\"", got)
	}
}

func TestEnsureSpecWritesOncePerContainer(t *testing.T) {
	root := t.TempDir()
	actorDirs := &ateompb.ActorDirs{RootDir: root, OciBundleDir: filepath.Join(root, "bundles")}
	r := &runsc{
		actorUID:   "uid-a",
		actorDirs:  actorDirs,
		containers: []*ateompb.Container{{Name: "app", ContainerSpec: &ateompb.ContainerSpec{Args: []string{"/app"}}}},
	}
	bundle := ociBundlePath(actorDirs, ocispec.PauseContainer)
	if err := os.MkdirAll(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureSpec(ocispec.PauseContainer); err != nil {
		t.Fatalf("first ensureSpec: %v", err)
	}
	cfgPath := filepath.Join(bundle, "config.json")
	if err := os.WriteFile(cfgPath, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureSpec(ocispec.PauseContainer); err != nil {
		t.Fatalf("second ensureSpec: %v", err)
	}
	got, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "sentinel" {
		t.Errorf("second ensureSpec overwrote config.json with %q", got)
	}
}

func TestRunAsyncMemoizesResult(t *testing.T) {
	var runs atomic.Int32
	wantErr := errors.New("boom")
	join := runAsync(func() error {
		runs.Add(1)
		return wantErr
	})
	for range 3 {
		if err := join(); !errors.Is(err, wantErr) {
			t.Fatalf("join() = %v, want %v", err, wantErr)
		}
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("fn ran %d times, want 1", got)
	}
}
