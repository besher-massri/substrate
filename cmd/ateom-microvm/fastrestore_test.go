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
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
)

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// With nothing set, every knob is off: the zero config is today's behavior.
func TestFastRestoreConfigDefaultsOff(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c != (fastRestoreConfig{}) {
		t.Errorf("default config = %+v, want the zero value", c)
	}
	if c.enabled() {
		t.Error("default config reports enabled")
	}
	for _, milli := range []int64{0, 250, 1000} {
		if got := c.activationMilliCPU(milli); got != milli {
			t.Errorf("activationMilliCPU(%d) = %d with the boost off", milli, got)
		}
		if c.boosted(milli) {
			t.Errorf("boosted(%d) with the boost off", milli)
		}
	}
}

func TestFastRestoreConfigFromEnv(t *testing.T) {
	c, err := fastRestoreConfigFromEnv(envMap(map[string]string{
		envActivationCPUBoost:      "2",
		envParallelWakeup:          "true",
		envMemoryRestoreMode:       "OnDemand",
		envOnDemandBackgroundFetch: "false",
		envRestoreReadahead:        "1",
		envEarlyVMMLaunch:          "TRUE",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := fastRestoreConfig{
		boost: true, boostMilliCPU: 2000, parallelWakeup: true,
		memRestoreMode: ch.MemRestoreOnDemand, noBackgroundFetch: true,
		readahead: true, earlyVMMLaunch: true,
	}
	if c != want {
		t.Errorf("config = %+v, want %+v", c, want)
	}
	if !c.enabled() {
		t.Error("config with knobs set reports disabled")
	}
	if len(c.attrs()) == 0 {
		t.Error("attrs() is empty")
	}
}

func TestFastRestoreConfigRejectsBadValues(t *testing.T) {
	for _, env := range []map[string]string{
		{envActivationCPUBoost: "lots"},
		{envActivationCPUBoost: "-1"},
		{envActivationCPUBoost: "0m"},
		{envParallelWakeup: "sometimes"},
		{envMemoryRestoreMode: "fast"},
		{envOnDemandBackgroundFetch: "maybe"},
	} {
		if _, err := fastRestoreConfigFromEnv(envMap(env)); err == nil {
			t.Errorf("fastRestoreConfigFromEnv(%v) succeeded, want an error", env)
		}
	}
}

func TestParseCPUBoost(t *testing.T) {
	for _, tc := range []struct {
		in      string
		enabled bool
		milli   int64
	}{
		{"", false, 0},
		{"off", false, 0},
		{"0", false, 0},
		{"max", true, 0},
		{"unlimited", true, 0},
		{"1500m", true, 1500},
		{"1", true, 1000},
		{"0.25", true, 250},
		{" 2 ", true, 2000},
	} {
		enabled, milli, err := parseCPUBoost(tc.in)
		if err != nil {
			t.Errorf("parseCPUBoost(%q): %v", tc.in, err)
			continue
		}
		if enabled != tc.enabled || milli != tc.milli {
			t.Errorf("parseCPUBoost(%q) = (%v, %d), want (%v, %d)", tc.in, enabled, milli, tc.enabled, tc.milli)
		}
	}
}

func TestActivationMilliCPU(t *testing.T) {
	unlimited := fastRestoreConfig{boost: true}
	twoCores := fastRestoreConfig{boost: true, boostMilliCPU: 2000}
	for _, tc := range []struct {
		name    string
		c       fastRestoreConfig
		actor   int64
		want    int64
		boosted bool
	}{
		{"unlimited boost lifts a fractional actor", unlimited, 250, 0, true},
		{"a capped boost lifts a smaller actor", twoCores, 250, 2000, true},
		{"a capped boost never lowers a bigger actor", twoCores, 4000, 4000, false},
		{"an uncapped actor stays uncapped", twoCores, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.c.activationMilliCPU(tc.actor); got != tc.want {
				t.Errorf("activationMilliCPU(%d) = %d, want %d", tc.actor, got, tc.want)
			}
			if got := tc.c.boosted(tc.actor); got != tc.boosted {
				t.Errorf("boosted(%d) = %v, want %v", tc.actor, got, tc.boosted)
			}
		})
	}
}

func TestPickRestoreMemMode(t *testing.T) {
	ctx := context.Background()
	v52 := ch.VMMInfo{Version: "52.0.0"}
	v53 := ch.VMMInfo{Version: "53.0.0"}
	v53patched := ch.VMMInfo{Version: "53.0.0", Features: []string{"kvm", ch.FeatureRestoreBackgroundFetchPages}}
	for _, tc := range []struct {
		name     string
		c        fastRestoreConfig
		info     ch.VMMInfo
		wantMode string
		wantNoBG bool
	}{
		{"auto on v53 is eager", fastRestoreConfig{}, v53, ch.MemRestoreEager, false},
		{"auto on v52 is on-demand", fastRestoreConfig{}, v52, ch.MemRestoreOnDemand, false},
		{"forced copy on v52", fastRestoreConfig{memRestoreMode: ch.MemRestoreEager}, v52, ch.MemRestoreEager, false},
		{"forced on-demand on v52", fastRestoreConfig{memRestoreMode: ch.MemRestoreOnDemand}, v52, ch.MemRestoreOnDemand, false},
		{"forced on-demand on stock v53 falls back", fastRestoreConfig{memRestoreMode: ch.MemRestoreOnDemand, noBackgroundFetch: true}, v53, ch.MemRestoreEager, false},
		{"forced on-demand on patched v53 without opting out of prefault falls back", fastRestoreConfig{memRestoreMode: ch.MemRestoreOnDemand}, v53patched, ch.MemRestoreEager, false},
		{"forced on-demand on patched v53 disables background fetch", fastRestoreConfig{memRestoreMode: ch.MemRestoreOnDemand, noBackgroundFetch: true}, v53patched, ch.MemRestoreOnDemand, true},
		{"no-background-fetch alone keeps auto eager on v53", fastRestoreConfig{noBackgroundFetch: true}, v53patched, ch.MemRestoreEager, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode, noBG := tc.c.pickRestoreMemMode(ctx, tc.info)
			if mode != tc.wantMode || noBG != tc.wantNoBG {
				t.Errorf("pickRestoreMemMode = (%q, %v), want (%q, %v)", mode, noBG, tc.wantMode, tc.wantNoBG)
			}
		})
	}
}

func TestSharesInode(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "memory-ranges")
	if err := os.WriteFile(a, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if sharesInode(a) {
		t.Error("a single-link file shares its inode")
	}
	if err := os.Link(a, filepath.Join(dir, "cache")); err != nil {
		t.Fatal(err)
	}
	if !sharesInode(a) {
		t.Error("a hard-linked file does not share its inode")
	}
	if sharesInode(filepath.Join(dir, "missing")) {
		t.Error("a missing file shares its inode")
	}
}

// Readahead is best effort: missing files and real files both just return.
func TestReadaheadMemoryImage(t *testing.T) {
	dir := t.TempDir()
	readaheadMemoryImage(context.Background(), filepath.Join(dir, "missing"))
	f := filepath.Join(dir, "memory-ranges")
	if err := os.WriteFile(f, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	readaheadMemoryImage(context.Background(), f)
}
