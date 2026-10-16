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
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/sizing"
)

const (
	// defaultOverlay2Config is the default runsc --overlay2 configuration for
	// sandbox creation, start, and restore. Using a memory-backed root overlay
	// keeps container rootfs upper layers inside sentry memory across
	// checkpoint/restore cycles instead of referencing per-sandbox host-backed
	// filestore files (which breaks multi-container warm restores).
	defaultOverlay2Config = "root:memory"

	// envGVisorOverlay2 overrides the runsc --overlay2 configuration (e.g.
	// "root:memory", "all:memory", or "none" to disable).
	envGVisorOverlay2       = "ATEWORKER_GVISOR_OVERLAY2"
	envLegacyGVisorOverlay2 = "ATEOM_GVISOR_OVERLAY2"

	// wakeupLivenessCheckDelay is the post-restore liveness verification delay
	// before spawning background `runsc wait` checks on restored containers.
	// Fast restores (<250ms) spawn zero extra subprocesses while a container
	// that exits immediately after restore fails fast in ~250ms instead of 30s.
	wakeupLivenessCheckDelay = 250 * time.Millisecond

	// minUncompressedAppPagesBytes is the minimum size of an uncompressed
	// `pages.img` file when at least one application subcontainer is running
	// alongside `/pause` (an empty `/pause`-only sandbox writes 21 pages =
	// 86,016 bytes).
	minUncompressedAppPagesBytes int64 = 128 * 1024
	// minCompressedAppPagesBytes is the minimum size of a zstd-compressed
	// `pages.img.zstd` file when at least one application subcontainer is
	// running alongside `/pause`.
	minCompressedAppPagesBytes int64 = 32 * 1024
)

// overlay2EnvOverride inspects ATEWORKER_GVISOR_OVERLAY2 (falling back to
// ATEOM_GVISOR_OVERLAY2). It returns whether an override was set, whether
// overlay2 was explicitly disabled ("none"/"off"/"false"), and the normalized
// "--overlay2=<val>" flag when enabled.
func overlay2EnvOverride() (flag string, disabled bool, set bool) {
	raw := strings.TrimSpace(os.Getenv(envGVisorOverlay2))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv(envLegacyGVisorOverlay2))
	}
	if raw == "" {
		return "", false, false
	}
	switch strings.ToLower(raw) {
	case "none", "off", "false", "disabled", "0":
		return "", true, true
	}
	if strings.HasPrefix(raw, "--overlay2=") || strings.HasPrefix(raw, "-overlay2=") {
		return raw, false, true
	}
	return "--overlay2=" + raw, false, true
}

// overlay2Flag returns the effective runsc --overlay2 global flag, defaulting
// to "--overlay2=root:memory" unless overridden or disabled via environment.
func overlay2Flag() string {
	if flag, disabled, set := overlay2EnvOverride(); set {
		if disabled {
			return ""
		}
		return flag
	}
	return "--overlay2=" + defaultOverlay2Config
}

func isOverlay2Command(command string) bool {
	switch command {
	case "create", "start", "restore":
		return true
	default:
		return false
	}
}

func isOverlay2Arg(arg string) bool {
	return strings.HasPrefix(arg, "--overlay2=") || strings.HasPrefix(arg, "-overlay2=") ||
		arg == "--overlay2" || arg == "-overlay2"
}

// applyOverlay2Flag applies any ATEWORKER_GVISOR_OVERLAY2 / ATEOM_GVISOR_OVERLAY2
// environment override to globalFlags for create, start, and restore commands.
func applyOverlay2Flag(command string, globalFlags []string) []string {
	if !isOverlay2Command(command) {
		return globalFlags
	}
	flag, disabled, set := overlay2EnvOverride()
	if !set {
		return globalFlags
	}
	filtered := make([]string, 0, len(globalFlags)+1)
	for i := 0; i < len(globalFlags); i++ {
		arg := globalFlags[i]
		if (arg == "--overlay2" || arg == "-overlay2") && i+1 < len(globalFlags) {
			i++
			continue
		}
		if isOverlay2Arg(arg) {
			continue
		}
		filtered = append(filtered, arg)
	}
	if !disabled && flag != "" {
		filtered = append(filtered, flag)
	}
	return filtered
}

type runsc struct {
	path     string
	actorUID string
	// actorDirs are the actor's directories, from the request.
	actorDirs *ateworkerpb.ActorDirs
	// size is the actor's declared limits.
	size sizing.SandboxSize
	// durableVolumes are the durable-dir volume names declared to the sandbox.
	durableVolumes []string
	// containers are the workload's containers, from the request.
	containers []*ateworkerpb.Container
	// flags holds the configured flags; must not be nil.
	flags *flagStore
}

// buildArgs constructs the runsc argument vector for command, appending
// containerName at the end if non-empty:
//
//	<combined globalFlags> -root <stateDir> <command> <subcommandFlags> <extraArgs> [<containerName>]
func (r *runsc) buildArgs(command, containerName string, extraArgs ...string) []string {
	cfg := r.flags.get()
	globalFlags := applyOverlay2Flag(command, cfg.globalFlags(command))
	subcommandFlags := cfg.subcommandFlags(command)

	args := make([]string, 0,
		len(globalFlags)+2+1+len(subcommandFlags)+len(extraArgs)+1)
	args = append(args, globalFlags...)
	args = append(args, "-root", runscStateDir(r.actorDirs))
	args = append(args, command)
	args = append(args, subcommandFlags...)
	args = append(args, extraArgs...)
	if containerName != "" {
		args = append(args, containerName)
	}
	return args
}

// durableVolumeNames returns the sorted, deduplicated durable-dir volume names
// mounted by workload containers.
func durableVolumeNames(spec *ateworkerpb.WorkloadSpec) []string {
	var names []string
	for _, c := range spec.GetContainers() {
		for _, m := range c.GetDurableDirVolumeMounts() {
			names = append(names, m.GetName())
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// pauseContainer is the sandbox root container. It only reaps, so it gets no
// capabilities and mounts nothing.
func pauseContainer() *ateworkerpb.Container {
	return &ateworkerpb.Container{
		Name:          ocispec.PauseContainer,
		ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/pause"}},
	}
}

// writeSpec builds the container's OCI spec, shapes it for gVisor, and saves it
// into the container's bundle.
func (r *runsc) writeSpec(containerName string) error {
	container := pauseContainer()
	if containerName != ocispec.PauseContainer {
		i := slices.IndexFunc(r.containers, func(c *ateworkerpb.Container) bool { return c.GetName() == containerName })
		if i < 0 {
			return fmt.Errorf("container %q is not in the workload spec", containerName)
		}
		container = r.containers[i]
	}
	spec := ocispec.Build(ocispec.Options{ActorUID: r.actorUID, ActorDirs: r.actorDirs, Container: container})
	ocispec.ShapeGVisor(spec, ocispec.GVisorOptions{
		ActorUID:       r.actorUID,
		ActorDirs:      r.actorDirs,
		ContainerName:  containerName,
		DurableVolumes: r.durableVolumes,
		Size:           r.size,
		ResolvConf:     resolvConfPath(r.actorDirs),
	})
	return ocispec.Save(ociBundlePath(r.actorDirs, containerName), spec)
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string, additionalArgs []string) error {
	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	if err := r.writeSpec(containerName); err != nil {
		return fmt.Errorf("while writing the OCI spec for %q: %w", containerName, err)
	}

	args := make([]string, 0, 4+len(additionalArgs))
	args = append(args,
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-pid-file", pidFilePath(r.actorDirs, containerName),
	)
	args = append(args, additionalArgs...)

	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("create", containerName, args...)...,
	)
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc create`: %w", err)
	}

	return nil
}

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc start", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("start", containerName)...)
	cmd.Stdout = out
	cmd.Stderr = out

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc start`: %w", err)
	}

	return nil
}

// checkpointArgs builds the argv for `runsc checkpoint <container>`. Factored
// out so the argument construction can be unit-tested without executing runsc.
func (r *runsc) checkpointArgs(containerName, checkpointPath string, fsCheckpointPaths []string) []string {
	extra := []string{"-image-path", checkpointPath}
	if len(fsCheckpointPaths) > 0 {
		extra = append(extra, "-fs-checkpoint-paths", strings.Join(fsCheckpointPaths, ","))
	}
	return r.buildArgs("checkpoint", containerName, extra...)
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string, fsCheckpointPaths []string) error {
	slog.InfoContext(ctx, "About to run runsc checkpoint", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.checkpointArgs(containerName, checkpointPath, fsCheckpointPaths)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc checkpoint`: %w", err)
	}
	return nil
}

// fsCheckpointArgs builds the argv for `runsc fscheckpoint <container>`.
// Factored out so the argument construction can be unit-tested without
// executing runsc.
func (r *runsc) fsCheckpointArgs(containerName, checkpointPath string, fsCheckpointPaths []string) []string {
	extra := make([]string, 0, 2+2*len(fsCheckpointPaths))
	extra = append(extra, "-image-path", filepath.Join(checkpointPath, resources.FSCheckpointSubdir))
	for _, ddv := range fsCheckpointPaths {
		extra = append(extra, "-path", ddv)
	}
	return r.buildArgs("fscheckpoint", containerName, extra...)
}

func (r *runsc) cmdFsCheckpoint(ctx context.Context, containerName, checkpointPath string, fsCheckpointPaths []string) error {
	slog.InfoContext(ctx, "About to run runsc fscheckpoint", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx, r.path, r.fsCheckpointArgs(containerName, checkpointPath, fsCheckpointPaths)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc fscheckpoint`: %w", err)
	}
	return nil
}

// restoreArgs builds the argv for `runsc restore <container>`. Factored out so
// the argument construction can be unit-tested without executing runsc.
func (r *runsc) restoreArgs(containerName, checkpointPath string) []string {
	return r.buildArgs("restore", containerName,
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-image-path", checkpointPath,
		"-pid-file", pidFilePath(r.actorDirs, containerName),
	)
}

// We take a checkpoint only of the root container of the sandbox, but we need
// to call restore on each container, using the same checkpoint.
func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	slog.InfoContext(ctx, "About to run runsc restore", slog.String("container", containerName))

	if err := r.writeSpec(containerName); err != nil {
		return fmt.Errorf("while writing the OCI spec for %q: %w", containerName, err)
	}

	cmd := exec.CommandContext(ctx, r.path, r.restoreArgs(containerName, checkpointPath)...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc restore`: %w", err)
	}
	return nil
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("delete", containerName)...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("while running `runsc delete`: %w", err)
	}
	return nil
}

func (r *runsc) cmdState(ctx context.Context, containerName string) error {
	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("state", containerName)...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc state`: %w", err)
	}
	return nil
}

// cmdList returns the container IDs runsc has a record of.
func (r *runsc) cmdList(ctx context.Context) ([]string, error) {
	cmd := exec.CommandContext(
		ctx,
		r.path,
		r.buildArgs("list", "")...,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("while running `runsc list`: %w", err)
	}
	return strings.Fields(out.String()), nil
}

// cmdKill sends signal to the given container's process(es) inside the gVisor
// sandbox. Used during graceful shutdown to propagate SIGTERM to the actor.
func (r *runsc) cmdKill(ctx context.Context, containerName, signal string) error {
	slog.InfoContext(ctx, "About to run runsc kill", slog.String("container", containerName), slog.String("signal", signal))

	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("kill", "", containerName, signal)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc kill`: %w", err)
	}
	return nil
}

// runWait runs `runsc wait [extraArgs...] <container>` and blocks until the
// waited-on condition completes.
func (r *runsc) runWait(ctx context.Context, containerName string, extraArgs ...string) error {
	cmd := exec.CommandContext(ctx, r.path, r.buildArgs("wait", containerName, extraArgs...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runsc wait`: %w", err)
	}
	return nil
}

// cmdWait blocks until the given container's process exits. Used during
// graceful shutdown to confirm the actor has stopped before ateworker exits.
func (r *runsc) cmdWait(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc wait", slog.String("container", containerName))
	return r.runWait(ctx, containerName)
}

// cmdWaitRestore blocks until background page loading from `runsc restore -background` completes.
func (r *runsc) cmdWaitRestore(ctx context.Context, containerName string) error {
	return r.runWait(ctx, containerName, "-restore")
}

// cmdWaitFSRestore blocks until background filesystem page loading from
// `--fs-restore-image-path` completes for the given container.
func (r *runsc) cmdWaitFSRestore(ctx context.Context, containerName string) error {
	return r.runWait(ctx, containerName, "-fsrestore")
}

// containerWaiter is the slice of *runsc used to verify container liveness
// after restore.
type containerWaiter interface {
	cmdWait(ctx context.Context, containerName string) error
}

// verifyMemoryCheckpointPages fails fast when a MEMORY checkpoint directory for
// an actor with application containers contains only `/pause` pages because an
// application container exited before `runsc checkpoint`.
func verifyMemoryCheckpointPages(checkpointDir string, appContainers int) error {
	if appContainers <= 0 || checkpointDir == "" {
		return nil
	}
	if info, err := os.Stat(filepath.Join(checkpointDir, "pages.img")); err == nil {
		if info.Size() <= minUncompressedAppPagesBytes {
			return fmt.Errorf("application container exited before memory checkpoint: %s/pages.img is only %d bytes (<= %d bytes pause-only threshold)",
				checkpointDir, info.Size(), minUncompressedAppPagesBytes)
		}
		return nil
	}
	if info, err := os.Stat(filepath.Join(checkpointDir, "pages.img.zstd")); err == nil {
		if info.Size() <= minCompressedAppPagesBytes {
			return fmt.Errorf("application container exited before memory checkpoint: %s/pages.img.zstd is only %d bytes (<= %d bytes pause-only threshold)",
				checkpointDir, info.Size(), minCompressedAppPagesBytes)
		}
		return nil
	}
	return nil
}

// verifyContainersRunning checks that none of the restored containers exit
// within window (e.g. 250ms). A container whose `cmdWait` remains blocked until
// window elapses is still running; a container whose `cmdWait` returns before
// window elapses has exited or failed to restore.
func verifyContainersRunning(ctx context.Context, waiter containerWaiter, containers []*ateworkerpb.Container, window time.Duration) error {
	if waiter == nil || len(containers) == 0 || window <= 0 {
		return nil
	}
	return waitForWakeupProbesWithLiveness(ctx, waiter, containers, 0, func(probeCtx context.Context) error {
		timer := time.NewTimer(window)
		defer timer.Stop()
		select {
		case <-probeCtx.Done():
			return probeCtx.Err()
		case <-timer.C:
			return nil
		}
	})
}

// waitForWakeupProbesWithLiveness runs probeWait while guarding against
// application containers that exit or fail to start after restore. After delay
// (default 250ms), it launches background `cmdWait` checks on each container
// and cancels probeWait immediately if any container exits before probeWait
// succeeds.
func waitForWakeupProbesWithLiveness(
	ctx context.Context,
	waiter containerWaiter,
	containers []*ateworkerpb.Container,
	delay time.Duration,
	probeWait func(context.Context) error,
) error {
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var exitedErr error
	var exitedOnce sync.Once
	if waiter != nil && len(containers) > 0 {
		go func() {
			if delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-probeCtx.Done():
					return
				case <-timer.C:
				}
			}
			for _, ac := range containers {
				name := ac.GetName()
				if name == "" {
					continue
				}
				go func(containerName string) {
					err := waiter.cmdWait(probeCtx, containerName)
					if probeCtx.Err() != nil {
						return
					}
					exitedOnce.Do(func() {
						if err != nil {
							exitedErr = fmt.Errorf("application container %q not running during wakeup probe: %w", containerName, err)
						} else {
							exitedErr = fmt.Errorf("application container %q exited before wakeup probe succeeded", containerName)
						}
						cancel()
					})
				}(name)
			}
		}()
	}

	err := probeWait(probeCtx)
	if exitedErr != nil {
		return exitedErr
	}
	return err
}
