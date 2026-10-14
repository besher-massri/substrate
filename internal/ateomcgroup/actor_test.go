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

package ateomcgroup

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestCPUMax(t *testing.T) {
	for _, tc := range []struct {
		milli int64
		want  string
	}{
		{0, "max 100000"},
		{500, "50000 100000"},
		{2000, "200000 100000"},
		// Below the kernel's 1ms floor.
		{5, "1000 100000"},
	} {
		if got := cpuMax(tc.milli); got != tc.want {
			t.Errorf("cpuMax(%d) = %q, want %q", tc.milli, got, tc.want)
		}
	}
}

// A leaf left by an earlier incarnation is reused, and its old cap replaced.
func TestOpenActorLeafReusesAndResetsALeftover(t *testing.T) {
	root := t.TempDir()
	// A regular directory stands in for cgroupfs, which provides cpu.max itself.
	if err := os.Mkdir(filepath.Join(root, "uid-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "uid-a", "cpu.max"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, milli := range []int64{500, 0} {
		leaf, err := openActorLeaf(root, "uid-a", milli)
		if err != nil {
			t.Fatal(err)
		}
		if err := leaf.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(root, "uid-a", "cpu.max"))
		if err != nil {
			t.Fatal(err)
		}
		if want := cpuMax(milli); string(got) != want {
			t.Errorf("cpu.max = %q, want %q", got, want)
		}
	}
	if err := os.Remove(filepath.Join(root, "uid-a", "cpu.max")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := removeActorLeaf(root, "uid-a"); err != nil {
			t.Fatalf("removeActorLeaf: %v", err)
		}
	}
}

// Without the cpu controller there is no cpu.max; the leaf still isolates.
func TestOpenActorLeafWithoutTheCPUController(t *testing.T) {
	root := t.TempDir()
	leaf, err := openActorLeaf(root, "uid-a", 500)
	if err != nil {
		t.Fatalf("openActorLeaf without cpu.max: %v", err)
	}
	_ = leaf.Close()
	if _, err := os.Stat(filepath.Join(root, "uid-a", "cpu.max")); !os.IsNotExist(err) {
		t.Errorf("cpu.max was created: %v", err)
	}
}

func TestActorLeafRejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{"", ".", "..", "a/b", "../escape", workerLeaf, "nul\x00"} {
		if _, err := openActorLeaf(t.TempDir(), name, 0); err == nil {
			t.Errorf("openActorLeaf(%q) succeeded, want an error", name)
		}
	}
}

func TestNilActorLeaf(t *testing.T) {
	var leaf *ActorLeaf
	if leaf.SysProcAttr() != nil {
		t.Error("a nil leaf set SysProcAttr")
	}
	if err := leaf.Close(); err != nil {
		t.Error(err)
	}
}

// SetCPULimit swaps the cap of a live leaf (the activation boost ends this
// way), even after Close, and is a no-op without the cpu controller.
func TestActorLeafSetCPULimit(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "uid-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "uid-a", "cpu.max"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	leaf, err := openActorLeaf(root, "uid-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := leaf.Close(); err != nil {
		t.Fatal(err)
	}
	for _, milli := range []int64{250, 0, 2000} {
		if err := leaf.SetCPULimit(milli); err != nil {
			t.Fatalf("SetCPULimit(%d): %v", milli, err)
		}
		got, err := os.ReadFile(filepath.Join(root, "uid-a", "cpu.max"))
		if err != nil {
			t.Fatal(err)
		}
		if want := cpuMax(milli); string(got) != want {
			t.Errorf("after SetCPULimit(%d) cpu.max = %q, want %q", milli, got, want)
		}
	}

	bare, err := openActorLeaf(t.TempDir(), "uid-b", 500)
	if err != nil {
		t.Fatal(err)
	}
	_ = bare.Close()
	if err := bare.SetCPULimit(250); err != nil {
		t.Errorf("SetCPULimit without cpu.max: %v", err)
	}
	var nilLeaf *ActorLeaf
	if err := nilLeaf.SetCPULimit(250); err != nil {
		t.Errorf("SetCPULimit on a nil leaf: %v", err)
	}
}

// SetCPUWeight writes cpu.weight on a live leaf (activation and steady-state
// priority), even after Close; it rejects out-of-range weights, and is a no-op
// without the cpu controller or on a nil leaf.
func TestActorLeafSetCPUWeight(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "uid-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"cpu.max", "cpu.weight"} {
		if err := os.WriteFile(filepath.Join(root, "uid-a", f), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	leaf, err := openActorLeaf(root, "uid-a", 250)
	if err != nil {
		t.Fatal(err)
	}
	if err := leaf.Close(); err != nil {
		t.Fatal(err)
	}
	for _, w := range []int{MaxCPUWeight, 10, MinCPUWeight, DefaultCPUWeight} {
		if err := leaf.SetCPUWeight(w); err != nil {
			t.Fatalf("SetCPUWeight(%d): %v", w, err)
		}
		got, err := os.ReadFile(filepath.Join(root, "uid-a", "cpu.weight"))
		if err != nil {
			t.Fatal(err)
		}
		if want := strconv.Itoa(w); string(got) != want {
			t.Errorf("after SetCPUWeight(%d) cpu.weight = %q, want %q", w, got, want)
		}
	}
	for _, w := range []int{0, -1, MaxCPUWeight + 1} {
		if err := leaf.SetCPUWeight(w); err == nil {
			t.Errorf("SetCPUWeight(%d) succeeded, want an error", w)
		}
	}

	bare, err := openActorLeaf(t.TempDir(), "uid-b", 500)
	if err != nil {
		t.Fatal(err)
	}
	_ = bare.Close()
	if err := bare.SetCPUWeight(10); err != nil {
		t.Errorf("SetCPUWeight without cpu.weight: %v", err)
	}
	var nilLeaf *ActorLeaf
	if err := nilLeaf.SetCPUWeight(10); err != nil {
		t.Errorf("SetCPUWeight on a nil leaf: %v", err)
	}
}

// SetWorkerCPUWeight targets the ateom's own leaf beside the actors'.
func TestSetWorkerCPUWeight(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, workerLeaf), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, workerLeaf, "cpu.weight"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := setWorkerCPUWeightUnder(root, 10000); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, workerLeaf, "cpu.weight")); string(got) != "10000" {
		t.Errorf("worker leaf cpu.weight = %q, want 10000", got)
	}
	if err := setWorkerCPUWeightUnder(root, 0); err == nil {
		t.Error("setWorkerCPUWeightUnder(0) succeeded, want an error")
	}
}

// SetActorLeafCPUWeight weights an existing leaf by name and is a no-op for a
// leaf that is gone.
func TestSetActorLeafCPUWeight(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "uid-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"cpu.max", "cpu.weight"} {
		if err := os.WriteFile(filepath.Join(root, "uid-a", f), []byte("keep"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := setActorLeafCPUWeightUnder(root, "uid-a", 10000); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, "uid-a", "cpu.weight")); string(got) != "10000" {
		t.Errorf("cpu.weight = %q, want 10000", got)
	}
	if got, _ := os.ReadFile(filepath.Join(root, "uid-a", "cpu.max")); string(got) != "keep" {
		t.Errorf("cpu.max changed to %q", got)
	}
	if err := setActorLeafCPUWeightUnder(root, "uid-gone", 10000); err != nil {
		t.Errorf("weighting a missing leaf: %v", err)
	}
	if err := setActorLeafCPUWeightUnder(root, "../escape", 10000); err == nil {
		t.Error("weighting an unsafe leaf name succeeded")
	}
}
