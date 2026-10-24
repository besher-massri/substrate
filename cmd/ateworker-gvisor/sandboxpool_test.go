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
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/sizing"
)

func TestRootfsSignature(t *testing.T) {
	ckptDir := t.TempDir()
	rootfsDir := filepath.Join(ckptDir, "rootfs")
	if err := os.MkdirAll(rootfsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootfsDir, fsCheckpointManifest), []byte("manifest-v1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootfsDir, "pages_meta.img"), []byte("meta-v1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootfsDir, "multitar.img"), []byte("tar-v1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootfsDir, "pages.img"), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	sig1, ok := rootfsSignature(ckptDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, nil)
	if !ok {
		t.Fatal("expected rootfsSignature to succeed on empty pages.img MEMORY checkpoint")
	}
	sig2, ok := rootfsSignature(ckptDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, nil)
	if !ok || sig1 != sig2 {
		t.Fatalf("expected deterministic signature, got %x vs %x (ok=%v)", sig1, sig2, ok)
	}

	// Non-MEMORY fidelity must return false.
	if _, ok := rootfsSignature(ckptDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, nil); ok {
		t.Fatal("expected ROOTFS fidelity to return false")
	}

	// Durable volumes must return false.
	if _, ok := rootfsSignature(ckptDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, []string{"vol1"}); ok {
		t.Fatal("expected durable volumes to return false")
	}

	// Non-empty pages.img must return false.
	if err := os.WriteFile(filepath.Join(rootfsDir, "pages.img"), []byte{1}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := rootfsSignature(ckptDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, nil); ok {
		t.Fatal("expected non-empty pages.img to return false")
	}
}

func TestRunscOverrides(t *testing.T) {
	actorDirs := &ateworkerpb.ActorDirs{
		RootDir:      "/actor/root",
		OciBundleDir: "/actor/bundles",
	}
	r := &runsc{
		actorUID:              "actor-1",
		actorDirs:             actorDirs,
		stateDirOverride:      "/pool/sbp-1/runsc-state",
		pidDirOverride:        "/pool/sbp-1/pidfiles",
		sandboxBundleOverride: "/pool/sbp-1/bundles/_sandbox",
	}
	if got := r.stateDir(); got != "/pool/sbp-1/runsc-state" {
		t.Fatalf("stateDir() = %q, want /pool/sbp-1/runsc-state", got)
	}
	if got := r.pidFile(ocispec.SandboxID); got != "/pool/sbp-1/pidfiles/_sandbox.pid" {
		t.Fatalf("pidFile(_sandbox) = %q, want /pool/sbp-1/pidfiles/_sandbox.pid", got)
	}
	if got := r.bundlePath(ocispec.SandboxID); got != "/pool/sbp-1/bundles/_sandbox" {
		t.Fatalf("bundlePath(_sandbox) = %q, want /pool/sbp-1/bundles/_sandbox", got)
	}
	if got := r.bundlePath("glutton"); got != "/actor/bundles/glutton" {
		t.Fatalf("bundlePath(glutton) = %q, want /actor/bundles/glutton", got)
	}
}

func TestEnsureConfiguredStagesSidecars(t *testing.T) {
	tmp := t.TempDir()
	srcBinDir := filepath.Join(tmp, "src-bin")
	srcSidecarDir := filepath.Join(srcBinDir, "gvisor-bin")
	if err := os.MkdirAll(srcSidecarDir, 0o700); err != nil {
		t.Fatal(err)
	}
	runscPath := filepath.Join(srcBinDir, "runsc")
	if err := os.WriteFile(runscPath, []byte("fake-runsc"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, sidecar := range []string{"gvisor_sentry", "gvisor-proc-umounter", "checkpointgofer"} {
		if err := os.WriteFile(filepath.Join(srcSidecarDir, sidecar), []byte("fake-"+sidecar), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	ckptDir := filepath.Join(tmp, "ckpt")
	rootfsDir := filepath.Join(ckptDir, "rootfs")
	if err := os.MkdirAll(rootfsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{fsCheckpointManifest, "multitar.img", "pages_meta.img", "pages.img"} {
		if err := os.WriteFile(filepath.Join(rootfsDir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	sig, ok := rootfsSignature(ckptDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, nil)
	if !ok {
		t.Fatal("expected rootfsSignature to succeed")
	}

	poolDir := filepath.Join(tmp, "pool")
	p := &sandboxPool{
		rootDir:     poolDir,
		replenishCh: make(chan struct{}, 4),
		readyCh:     make(chan *prewarmedSandbox, 4),
	}
	if err := p.ensureConfigured(runscPath, ckptDir, sig, sizing.SandboxSize{MilliCPU: 248, MemoryBytes: 512 << 20}); err != nil {
		t.Fatalf("ensureConfigured() error = %v", err)
	}
	for _, rel := range []string{
		"bin/runsc",
		"bin/gvisor-bin/gvisor_sentry",
		"bin/gvisor-bin/gvisor-proc-umounter",
		"bin/gvisor-bin/checkpointgofer",
	} {
		if _, err := os.Stat(filepath.Join(poolDir, rel)); err != nil {
			t.Fatalf("expected staged binary %s: %v", rel, err)
		}
	}
}
