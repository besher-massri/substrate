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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/netns"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/sizing"
)

const (
	defaultSandboxPoolDir         = "/var/lib/ate/sandbox-pool"
	defaultSandboxPoolCap         = 16
	defaultSandboxPoolConcurrency = 3
	maxCachedRootfsManifestBytes  = 4096
	maxCachedRootfsTarBytes       = 16384
)

// prewarmedSandbox holds a pre-created gVisor _sandbox instance (created via
// `runsc create -no-root-container --fs-restore-image-path=<cachedRootfsDir> _sandbox`
// in the background) together with its pre-configured network namespace veth pair,
// cgroup leaf, pinned sentry process, and runsc state directory.
type prewarmedSandbox struct {
	slotID           string
	slotDir          string
	stateDir         string
	pidDir           string
	sandboxBundleDir string
	cgroupLeaf       string
	tempNSName       string
	vethPair         actornet.PrewarmedVethPair
	hasVethPair      bool
	sandbox          *sandboxProcess
	sig              [32]byte
	size             sizing.SandboxSize
}

// sandboxPool maintains a bounded pool of pre-created gVisor _sandbox
// instances for MEMORY restores whose rootfs filesystem checkpoint has an
// empty pages.img and a small static metadata bundle, eliminating the ~135 ms
// `runsc create -no-root-container _sandbox` step from the critical path of
// RestoreWorkload.
type sandboxPool struct {
	rootDir         string
	cgroupRoot      string
	cgroupDelegated bool
	egressPort      uint16
	flags           *flagStore

	mu          sync.Mutex
	configured  bool
	closed      bool
	runscPath   string
	rootfsDir   string
	sig         [32]byte
	size        sizing.SandboxSize
	replenishCh chan struct{}
	readyCh     chan *prewarmedSandbox
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	seq         atomic.Uint64
}

// newSandboxPool initializes a sandboxPool that stages pre-warmed sandboxes
// under defaultSandboxPoolDir when ATEWORKER_SANDBOX_POOL is not "false".
func newSandboxPool(cgroupRoot string, cgroupDelegated bool, egressPort uint16, flags *flagStore) *sandboxPool {
	if os.Getenv("ATEWORKER_SANDBOX_POOL") == "false" || flags == nil || egressPort == 0 {
		return nil
	}
	_ = os.RemoveAll(defaultSandboxPoolDir)
	if err := os.MkdirAll(filepath.Join(defaultSandboxPoolDir, "slots"), 0o700); err != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &sandboxPool{
		rootDir:         defaultSandboxPoolDir,
		cgroupRoot:      cgroupRoot,
		cgroupDelegated: cgroupDelegated,
		egressPort:      egressPort,
		flags:           flags,
		replenishCh:     make(chan struct{}, defaultSandboxPoolCap),
		readyCh:         make(chan *prewarmedSandbox, defaultSandboxPoolCap),
		cancel:          cancel,
	}
	for range defaultSandboxPoolConcurrency {
		p.wg.Add(1)
		go p.replenishLoop(ctx)
	}
	return p
}

// rootfsSignature computes the SHA-256 content signature of the rootfs
// filesystem checkpoint bundle in checkpointDir when the checkpoint is a
// MEMORY snapshot with no durable-dir fs/ checkpoint and a 0-byte
// rootfs/pages.img. Returns false when the checkpoint does not qualify for
// pre-warmed sandbox pooling.
func rootfsSignature(checkpointDir string, fidelity ateworkerpb.SnapshotFidelity, durableVolumes []string) ([32]byte, bool) {
	var zero [32]byte
	if fidelity != ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY || len(durableVolumes) > 0 {
		return zero, false
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, resources.FSCheckpointSubdir, fsCheckpointManifest)); err == nil {
		return zero, false
	}
	rootfsDir := filepath.Join(checkpointDir, resources.RootfsCheckpointSubdir)
	pagesInfo, err := os.Stat(filepath.Join(rootfsDir, "pages.img"))
	if err != nil || !pagesInfo.Mode().IsRegular() || pagesInfo.Size() != 0 {
		return zero, false
	}
	manifestBytes, err := readSmallRegularFile(filepath.Join(rootfsDir, fsCheckpointManifest), maxCachedRootfsManifestBytes)
	if err != nil {
		return zero, false
	}
	metaBytes, err := readSmallRegularFile(filepath.Join(rootfsDir, "pages_meta.img"), maxCachedRootfsManifestBytes)
	if err != nil {
		return zero, false
	}
	tarBytes, err := readSmallRegularFile(filepath.Join(rootfsDir, "multitar.img"), maxCachedRootfsTarBytes)
	if err != nil {
		return zero, false
	}
	h := sha256.New()
	_, _ = h.Write(manifestBytes)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(metaBytes)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(tarBytes)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, true
}

// readSmallRegularFile reads path if it is a regular file with size <= maxBytes.
func readSmallRegularFile(path string, maxBytes int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxBytes {
		return nil, fmt.Errorf("file %q exceeds size bound %d", path, maxBytes)
	}
	return os.ReadFile(path)
}

// tryAcquire pops a pre-warmed sandbox matching (sig, size) from p if one is
// ready, or configures p and triggers background replenishment when empty.
func (p *sandboxPool) tryAcquire(runscPath, checkpointDir string, sig [32]byte, size sizing.SandboxSize) *prewarmedSandbox {
	if p == nil {
		return nil
	}
	if err := p.ensureConfigured(runscPath, checkpointDir, sig, size); err != nil {
		slog.Warn("Failed to configure prewarmed sandbox pool", slog.Any("err", err))
		return nil
	}
	for {
		select {
		case psb := <-p.readyCh:
			p.signalReplenish()
			if psb == nil {
				continue
			}
			if psb.sig != sig || psb.size != size || !psb.isAlive() {
				p.destroyPrewarmed(context.Background(), psb)
				continue
			}
			return psb
		default:
			p.signalReplenish()
			return nil
		}
	}
}

// ensureConfigured caches the runsc binary and the 4 rootfs checkpoint files
// under p.rootDir on first compatible restore and wakes the replenishers.
func (p *sandboxPool) ensureConfigured(runscPath, checkpointDir string, sig [32]byte, size sizing.SandboxSize) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("sandbox pool closed")
	}
	if p.configured && p.sig == sig && p.size == size {
		return nil
	}
	binDir := filepath.Join(p.rootDir, "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		return err
	}
	cachedRunsc := filepath.Join(binDir, "runsc")
	if _, err := os.Stat(cachedRunsc); err != nil {
		if err := linkOrCopyExecutable(runscPath, cachedRunsc); err != nil {
			return fmt.Errorf("staging runsc binary: %w", err)
		}
	}
	srcSidecarDir := filepath.Join(filepath.Dir(runscPath), "gvisor-bin")
	if entries, err := os.ReadDir(srcSidecarDir); err == nil {
		dstSidecarDir := filepath.Join(binDir, "gvisor-bin")
		if err := os.MkdirAll(dstSidecarDir, 0o700); err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.Type().IsRegular() {
				continue
			}
			dstSidecar := filepath.Join(dstSidecarDir, entry.Name())
			if _, err := os.Stat(dstSidecar); err != nil {
				if err := linkOrCopyExecutable(filepath.Join(srcSidecarDir, entry.Name()), dstSidecar); err != nil {
					return fmt.Errorf("staging runsc sidecar %s: %w", entry.Name(), err)
				}
			}
		}
	}
	sigHex := hex.EncodeToString(sig[:8])
	cachedRootfsDir := filepath.Join(p.rootDir, "fs", sigHex, resources.RootfsCheckpointSubdir)
	if err := os.MkdirAll(cachedRootfsDir, 0o700); err != nil {
		return err
	}
	srcRootfsDir := filepath.Join(checkpointDir, resources.RootfsCheckpointSubdir)
	for _, name := range []string{fsCheckpointManifest, "multitar.img", "pages_meta.img", "pages.img"} {
		dstPath := filepath.Join(cachedRootfsDir, name)
		if _, err := os.Stat(dstPath); err == nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join(srcRootfsDir, name))
		if err != nil {
			return fmt.Errorf("reading %s: %w", name, err)
		}
		if err := os.WriteFile(dstPath, b, 0o600); err != nil {
			return fmt.Errorf("writing %s: %w", name, err)
		}
	}
	p.runscPath = cachedRunsc
	p.rootfsDir = cachedRootfsDir
	p.sig = sig
	p.size = size
	p.configured = true
	p.signalReplenish()
	return nil
}

// linkOrCopyExecutable hard-links src to dst, falling back to an atomic copy
// if src and dst are on different filesystems.
func linkOrCopyExecutable(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func (p *sandboxPool) signalReplenish() {
	select {
	case p.replenishCh <- struct{}{}:
	default:
	}
}

// replenishLoop runs in the background and keeps p.readyCh topped up to
// defaultSandboxPoolCap pre-warmed sandboxes.
func (p *sandboxPool) replenishLoop(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.replenishCh:
		}
		for ctx.Err() == nil && len(p.readyCh) < cap(p.readyCh) {
			p.mu.Lock()
			configured := p.configured && !p.closed
			runscPath := p.runscPath
			rootfsDir := p.rootfsDir
			sig := p.sig
			size := p.size
			p.mu.Unlock()
			if !configured {
				break
			}
			psb, err := p.createOne(ctx, runscPath, rootfsDir, sig, size)
			if err != nil {
				if ctx.Err() == nil {
					slog.WarnContext(ctx, "Failed to pre-warm gVisor sandbox", slog.Any("err", err))
				}
				break
			}
			select {
			case p.readyCh <- psb:
			default:
				p.destroyPrewarmed(ctx, psb)
				break
			}
		}
	}
}

// createOne creates a single pre-warmed `_sandbox` instance with its own slot
// directory, pre-configured veth network namespace, cgroup leaf, and pinned
// sentry process.
func (p *sandboxPool) createOne(ctx context.Context, runscPath, rootfsDir string, sig [32]byte, size sizing.SandboxSize) (*prewarmedSandbox, error) {
	seq := p.seq.Add(1)
	slotID := fmt.Sprintf("sbp-%d", seq)
	slotDir := filepath.Join(p.rootDir, "slots", slotID)
	stateDir := filepath.Join(slotDir, "runsc-state")
	pidDir := filepath.Join(slotDir, "pidfiles")
	sandboxBundleDir := filepath.Join(slotDir, "bundles", ocispec.SandboxID)
	for _, dir := range []string{stateDir, pidDir, sandboxBundleDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			_ = os.RemoveAll(slotDir)
			return nil, err
		}
	}

	cgroupLeaf := ocispec.GVisorCgroupLeaf(slotID, ocispec.SandboxID)
	if p.cgroupDelegated && p.cgroupRoot != "" {
		_ = cgroup.RemoveActorLeafUnder(p.cgroupRoot, cgroupLeaf)
	}

	pair, err := actornet.AcquirePrewarmedVethPair(ctx, p.egressPort)
	if err != nil {
		_ = os.RemoveAll(slotDir)
		return nil, fmt.Errorf("acquiring prewarmed veth pair: %w", err)
	}
	tempNSName := nodepath.ActorNetNSName(slotID)
	if err := netns.BindNamed(tempNSName, pair.RuntimeNS); err != nil {
		actornet.ReleasePrewarmedVethPair(pair)
		_ = os.RemoveAll(slotDir)
		return nil, fmt.Errorf("binding temp netns %s: %w", tempNSName, err)
	}

	spec := ocispec.BuildSandbox(slotID)
	ocispec.ShapeGVisor(spec, ocispec.GVisorOptions{
		ActorUID:      slotID,
		ContainerName: ocispec.SandboxID,
		Size:          size,
	})
	size.ApplyToOCISpecWithFloor(spec, transitionBurstFloorMilliCPU)
	// Use default shares during background replenishment so active RestoreWorkload
	// and CheckpointWorkload transitions (running at MaxCPUWeight = 10000) always
	// preempt background sandbox creation; once created, cpu.weight is raised to
	// MaxCPUWeight below while the idle sentry sleeps on its control socket.
	if err := ocispec.Save(sandboxBundleDir, spec); err != nil {
		_ = netns.RemoveNamed(tempNSName)
		actornet.ReleasePrewarmedVethPair(pair)
		_ = os.RemoveAll(slotDir)
		return nil, fmt.Errorf("saving prewarmed sandbox spec: %w", err)
	}

	rcmd := &runsc{
		path:                  runscPath,
		actorUID:              slotID,
		size:                  size,
		flags:                 p.flags,
		stateDirOverride:      stateDir,
		pidDirOverride:        pidDir,
		sandboxBundleOverride: sandboxBundleDir,
	}
	rcmd.markSpecWritten(ocispec.SandboxID)

	createArgs := rcmd.createArgs(ocispec.SandboxID, []string{"--fs-restore-image-path", rootfsDir})
	cmd := exec.CommandContext(ctx, runscPath, createArgs...)
	cmd.Env = runscCmdEnv()
	// Leave Stdout and Stderr nil so os/exec attaches /dev/null as an *os.File
	// without spawning a pipe-draining goroutine; runsc create daemonizes
	// runsc-sandbox which inherits stdout/stderr onto fd 254/255 and would hold
	// a pipe open for the lifetime of the sandbox.
	if err := cmd.Run(); err != nil {
		_ = netns.RemoveNamed(tempNSName)
		actornet.ReleasePrewarmedVethPair(pair)
		if p.cgroupDelegated && p.cgroupRoot != "" {
			_ = cgroup.KillLeafUnder(context.Background(), p.cgroupRoot, cgroupLeaf)
			_ = cgroup.RemoveActorLeafUnder(p.cgroupRoot, cgroupLeaf)
		}
		_ = imagecache.UnmountAllUnder(stateDir)
		_ = os.RemoveAll(slotDir)
		return nil, fmt.Errorf("runsc create prewarmed _sandbox: %w", err)
	}

	pidFile := filepath.Join(pidDir, ocispec.SandboxID+".pid")
	sandbox, err := openSandboxProcessAt(pidFile)
	if err != nil {
		_ = rcmd.cmdDelete(context.Background(), ocispec.SandboxID)
		_ = netns.RemoveNamed(tempNSName)
		actornet.ReleasePrewarmedVethPair(pair)
		if p.cgroupDelegated && p.cgroupRoot != "" {
			_ = cgroup.KillLeafUnder(context.Background(), p.cgroupRoot, cgroupLeaf)
			_ = cgroup.RemoveActorLeafUnder(p.cgroupRoot, cgroupLeaf)
		}
		_ = imagecache.UnmountAllUnder(stateDir)
		_ = os.RemoveAll(slotDir)
		return nil, fmt.Errorf("pinning prewarmed sandbox process: %w", err)
	}

	if p.cgroupDelegated && p.cgroupRoot != "" {
		_ = cgroup.SetActorLeafCPUMaxUnder(p.cgroupRoot, cgroupLeaf, transitionSandboxBurstMilliCPU)
		_ = cgroup.SetActorLeafCPUWeightUnder(p.cgroupRoot, cgroupLeaf, cgroup.MaxCPUWeight)
	}

	return &prewarmedSandbox{
		slotID:           slotID,
		slotDir:          slotDir,
		stateDir:         stateDir,
		pidDir:           pidDir,
		sandboxBundleDir: sandboxBundleDir,
		cgroupLeaf:       cgroupLeaf,
		tempNSName:       tempNSName,
		vethPair:         pair,
		hasVethPair:      true,
		sandbox:          sandbox,
		sig:              sig,
		size:             size,
	}, nil
}

// isAlive reports whether the pre-warmed sandbox sentry process is still alive
// without spawning a pidfd watcher goroutine.
func (psb *prewarmedSandbox) isAlive() bool {
	if psb == nil || psb.sandbox == nil {
		return false
	}
	err := unix.PidfdSendSignal(psb.sandbox.pidfd, 0, nil, 0)
	return err == nil || errors.Is(err, unix.EPERM)
}

// destroyPrewarmed tears down an unused pre-warmed sandbox and removes its
// slot directory, cgroup leaf, and network namespaces.
func (p *sandboxPool) destroyPrewarmed(ctx context.Context, psb *prewarmedSandbox) {
	if psb == nil {
		return
	}
	if psb.sandbox != nil {
		psb.sandbox.Close()
		psb.sandbox = nil
	}
	if p.cgroupDelegated && p.cgroupRoot != "" && psb.cgroupLeaf != "" {
		_ = cgroup.KillLeafUnder(ctx, p.cgroupRoot, psb.cgroupLeaf)
		_ = cgroup.RemoveActorLeafUnder(p.cgroupRoot, psb.cgroupLeaf)
	}
	if psb.tempNSName != "" {
		_ = netns.RemoveNamed(psb.tempNSName)
		psb.tempNSName = ""
	}
	if psb.hasVethPair {
		actornet.ReleasePrewarmedVethPair(psb.vethPair)
		psb.hasVethPair = false
	}
	cleanupPrewarmedSlotDir(psb.slotDir)
}

// cleanupPrewarmedSlotDir unmounts any residual mounts under slotDir/runsc-state
// and removes slotDir.
func cleanupPrewarmedSlotDir(slotDir string) {
	if slotDir == "" {
		return
	}
	stateDir := filepath.Join(slotDir, "runsc-state")
	_ = imagecache.UnmountAllUnder(stateDir)
	if err := os.RemoveAll(slotDir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Warn("Failed to remove prewarmed sandbox slot dir", slog.String("slotDir", slotDir), slog.Any("err", err))
	}
}

// Close stops background replenishment and tears down all queued pre-warmed
// sandboxes.
func (p *sandboxPool) Close(ctx context.Context) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.cancel()
	p.mu.Unlock()
	p.wg.Wait()
	for {
		select {
		case psb := <-p.readyCh:
			p.destroyPrewarmed(ctx, psb)
		default:
			return
		}
	}
}
