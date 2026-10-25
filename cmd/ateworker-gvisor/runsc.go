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
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/sizing"
	"golang.org/x/sys/unix"
)

// wakeupLivenessCheckDelay is the grace period before waitForWakeupProbesWithLiveness
// spawns background `runsc wait` checks on application containers. Probes that
// succeed within this window return without spawning extra runsc subprocesses,
// while a container that exits during startup or restore cancels the probe
// wait after at most this delay instead of waiting for the full probe timeout.
const wakeupLivenessCheckDelay = 250 * time.Millisecond

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

	specMu       sync.Mutex
	specsWritten map[string]bool
}

// buildArgs constructs the runsc argument vector for command, appending
// containerName at the end if non-empty:
//
//	<combined globalFlags> -root <stateDir> <command> <subcommandFlags> <extraArgs> [<containerName>]
func (r *runsc) buildArgs(command, containerName string, extraArgs ...string) []string {
	cfg := r.flags.get()
	globalFlags := cfg.globalFlags(command)
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

// noRootContainerArgs returns the flag that tells runsc the sandbox has no
// root container. runsc refuses the flag for any other container.
func noRootContainerArgs(containerName string) []string {
	if containerName == ocispec.SandboxID {
		return []string{"-no-root-container"}
	}
	return nil
}

// runscCmdEnv returns the environment variables for runsc subprocesses,
// bounding GOMAXPROCS to 1 unless already set in the environment so short-lived
// runsc CLI processes do not initialize multiple Go P structs and OS threads on
// high-core hosts (runsc strips GOMAXPROCS when spawning runsc-sandbox and
// runsc-gofer, which size their GOMAXPROCS from the sandbox cgroup's cpu.max).
func runscCmdEnv() []string {
	env := os.Environ()
	for _, kv := range env {
		if strings.HasPrefix(kv, "GOMAXPROCS=") {
			return env
		}
	}
	return append(env, "GOMAXPROCS=1")
}

// execRunsc constructs an exec.Cmd for r.path with runscCmdEnv configured.
func (r *runsc) execRunsc(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, r.path, args...)
	cmd.Env = runscCmdEnv()
	return cmd
}

// defaultRunscAffinityCPUs is the default number of host CPUs included in the
// per-actor CPU affinity slice inherited by runsc and its child processes
// (gvisor-sentry-prewarmer, runsc-sandbox, and runsc-gofer) on high-core-count
// hosts. Because runsc strips GOMAXPROCS and sets GODEBUG=containermaxprocs=0
// when spawning its child processes, Go's runtime initialization in each child
// sizes its initial P count and OS thread pool from sched_getaffinity(2) and
// trims capabilities across all threads via AllThreadsSyscall. Bounding the
// inherited affinity mask to an actor-hashed slice of 8 CPUs (or the actor's
// declared CPU ceiling when larger) avoids 180-CPU runtime initialization on
// large nodes while distributing actors uniformly across all host CPUs.
const defaultRunscAffinityCPUs = 8

// hostAvailableCPUs returns the sorted list of CPU IDs in the current process's
// initial scheduling affinity mask.
var hostAvailableCPUs = sync.OnceValue(func() []int {
	var mask unix.CPUSet
	if err := unix.SchedGetaffinity(0, &mask); err != nil {
		return nil
	}
	return cpuSetToSlice(&mask)
})

// cpuSetToSlice returns the sorted CPU indices set in mask.
func cpuSetToSlice(mask *unix.CPUSet) []int {
	count := mask.Count()
	if count <= 0 {
		return nil
	}
	const maxCPUSetBits = len(unix.CPUSet{}) * 64
	cpus := make([]int, 0, count)
	for cpu := 0; cpu < maxCPUSetBits && len(cpus) < count; cpu++ {
		if mask.IsSet(cpu) {
			cpus = append(cpus, cpu)
		}
	}
	return cpus
}

// actorAffinityWidth returns the target number of CPUs for an actor's runsc
// affinity slice, using at least defaultRunscAffinityCPUs and scaling up when
// the actor's declared CPU limit exceeds defaultRunscAffinityCPUs cores.
func actorAffinityWidth(milliCPU int64) int {
	ceilCPUs := int((milliCPU + 999) / 1000)
	return max(defaultRunscAffinityCPUs, ceilCPUs)
}

// selectActorCPUSet constructs a CPU affinity mask containing width CPUs chosen
// as a deterministic circular window over availCPUs hashed by actorUID. Because
// the window wraps around len(availCPUs), every available host CPU has an
// identical selection probability with no edge bias.
func selectActorCPUSet(actorUID string, availCPUs []int, width int) unix.CPUSet {
	var out unix.CPUSet
	n := len(availCPUs)
	if n == 0 {
		return out
	}
	if width <= 0 || width >= n || actorUID == "" {
		for _, cpu := range availCPUs {
			out.Set(cpu)
		}
		return out
	}
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(actorUID); i++ {
		h ^= uint32(actorUID[i])
		h *= prime32
	}
	start := int(h % uint32(n))
	for i := range width {
		out.Set(availCPUs[(start+i)%n])
	}
	return out
}

// runCmd executes cmd, temporarily bounding the calling OS thread's CPU
// affinity mask around cmd.Start() when the host has more CPUs than the actor's
// target affinity slice so the spawned runsc process and its children inherit
// the bounded mask while the worker thread immediately restores its prior mask.
func (r *runsc) runCmd(cmd *exec.Cmd) error {
	return r.runCmdWithAvail(cmd, hostAvailableCPUs(), actorAffinityWidth(r.size.MilliCPU))
}

// runCmdWithAvail executes cmd using availCPUs and width to compute the child's
// inherited CPU affinity mask when len(availCPUs) > width.
func (r *runsc) runCmdWithAvail(cmd *exec.Cmd, availCPUs []int, width int) error {
	if r.actorUID == "" || width <= 0 || len(availCPUs) <= width {
		return cmd.Run()
	}
	sliceMask := selectActorCPUSet(r.actorUID, availCPUs, width)

	runtime.LockOSThread()
	var prevMask unix.CPUSet
	if err := unix.SchedGetaffinity(0, &prevMask); err != nil {
		runtime.UnlockOSThread()
		return cmd.Run()
	}
	if err := unix.SchedSetaffinity(0, &sliceMask); err != nil {
		runtime.UnlockOSThread()
		return cmd.Run()
	}
	startErr := cmd.Start()
	_ = unix.SchedSetaffinity(0, &prevMask)
	runtime.UnlockOSThread()
	if startErr != nil {
		return startErr
	}
	return cmd.Wait()
}

// transitionBurstFloorMilliCPU is the minimum container CFS CPU quota floor
// (4000m = 4.0 vCPUs, cpu.max = "400000 100000") written into the OCI spec for
// runsc create and runsc restore. Because runsc's --cpu-num-from-quota reads
// the newly created sandbox cgroup's cpu.max to size the sentry's GOMAXPROCS
// and --cpu-num (and runsc spawns runsc-gofer into the sandbox cgroup with
// GOMAXPROCS stripped from cmd.Env so Go 1.27's cgroup-aware runtime sizes
// runsc-gofer's GOMAXPROCS from the sandbox cgroup's cpu.max), a 4000m floor
// initializes a 4-P runtime for both sentry and gofer while giving startup and
// restore 4 cores of burst headroom from the start of cmdRestore(_sandbox);
// steady-state cpu.max is enforced after wakeup probes complete.
const transitionBurstFloorMilliCPU int64 = 4 * sizing.DefaultBurstFloorMilliCPU

// maxOCICPUShares is the Linux cgroup v1 MAX_SHARES value (1 << 18 = 262144)
// that runsc's cgroup v2 installer maps to cpu.weight = 10000 (MaxCPUWeight)
// when creating the sandbox and container cgroups before spawning child
// processes via CLONE_INTO_CGROUP.
const maxOCICPUShares uint64 = 262144

// applyTransitionCPUShares sets Linux.Resources.CPU.Shares to maxOCICPUShares
// (262144) on spec so runsc's cgroup v2 installation writes cpu.weight = 10000
// before spawning the sandbox or gofer processes via CLONE_INTO_CGROUP.
func applyTransitionCPUShares(spec *specs.Spec) {
	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	if spec.Linux.Resources == nil {
		spec.Linux.Resources = &specs.LinuxResources{}
	}
	if spec.Linux.Resources.CPU == nil {
		spec.Linux.Resources.CPU = &specs.LinuxCPU{}
	}
	shares := maxOCICPUShares
	spec.Linux.Resources.CPU.Shares = &shares
}

// writeSpec builds the container's OCI spec, shapes it for gVisor, and saves it
// into the container's bundle. During creation and restore, the CPU quota floor
// is raised to at least transitionBurstFloorMilliCPU (4000m) and CPU shares are
// set to maxOCICPUShares (262144 -> cpu.weight = 10000) so runsc create
// initializes the fresh cgroup at transition priority with --cpu-num=4 and
// GOMAXPROCS=4; steady-state cpu.max and cpu.weight are enforced after wakeup
// probes complete.
func (r *runsc) writeSpec(containerName string) error {
	bundlePath := ociBundlePath(r.actorDirs, containerName)
	var spec *specs.Spec
	if containerName == ocispec.SandboxID {
		// atelet makes only the application containers' bundles.
		if err := os.MkdirAll(bundlePath, 0o700); err != nil {
			return fmt.Errorf("while creating bundle %q: %w", bundlePath, err)
		}
		spec = ocispec.BuildSandbox(r.actorUID)
	} else {
		i := slices.IndexFunc(r.containers, func(c *ateworkerpb.Container) bool { return c.GetName() == containerName })
		if i < 0 {
			return fmt.Errorf("container %q is not in the workload spec", containerName)
		}
		spec = ocispec.Build(ocispec.Options{ActorUID: r.actorUID, ActorDirs: r.actorDirs, Container: r.containers[i]})
	}
	ocispec.ShapeGVisor(spec, ocispec.GVisorOptions{
		ActorUID:       r.actorUID,
		ActorDirs:      r.actorDirs,
		ContainerName:  containerName,
		DurableVolumes: r.durableVolumes,
		Size:           r.size,
	})
	r.size.ApplyToOCISpecWithFloor(spec, transitionBurstFloorMilliCPU)
	applyTransitionCPUShares(spec)
	return ocispec.Save(bundlePath, spec)
}

// ensureSpec writes the container's OCI spec into its bundle directory once
// per runsc instance, skipping redundant spec generation and disk writes when
// the spec was already pre-staged during parallel startup or restore
// preparation or written by an earlier cmdCreate before cmdRestore.
func (r *runsc) ensureSpec(containerName string) error {
	r.specMu.Lock()
	if r.specsWritten[containerName] {
		r.specMu.Unlock()
		return nil
	}
	r.specMu.Unlock()
	if err := r.writeSpec(containerName); err != nil {
		return err
	}
	r.specMu.Lock()
	if r.specsWritten == nil {
		r.specsWritten = make(map[string]bool)
	}
	r.specsWritten[containerName] = true
	r.specMu.Unlock()
	return nil
}

// createArgs builds the argv for `runsc create <container>`. Factored out so
// the argument construction can be unit-tested without executing runsc.
func (r *runsc) createArgs(containerName string, additionalArgs []string) []string {
	args := append(noRootContainerArgs(containerName),
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-pid-file", pidFilePath(r.actorDirs, containerName),
	)
	args = append(args, additionalArgs...)
	return r.buildArgs("create", containerName, args...)
}

func (r *runsc) cmdCreate(ctx context.Context, out io.Writer, containerName string, additionalArgs []string) error {
	slog.InfoContext(ctx, "About to run runsc create", slog.String("container", containerName))

	if err := r.ensureSpec(containerName); err != nil {
		return fmt.Errorf("while writing the OCI spec for %q: %w", containerName, err)
	}

	cmd := r.execRunsc(ctx, r.createArgs(containerName, additionalArgs)...)
	cmd.Stdout = out
	cmd.Stderr = out

	err := r.runCmd(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc create`: %w", err)
	}

	return nil
}

func (r *runsc) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	slog.InfoContext(ctx, "About to run runsc start", slog.String("container", containerName))

	cmd := r.execRunsc(ctx, r.buildArgs("start", containerName)...)
	cmd.Stdout = out
	cmd.Stderr = out

	err := r.runCmd(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc start`: %w", err)
	}

	return nil
}

// checkpointArgs builds the argv for `runsc checkpoint <container>`. Factored
// out so the argument construction can be unit-tested without executing runsc.
func (r *runsc) checkpointArgs(containerName, checkpointPath string, fsCheckpointPaths []string) []string {
	extra := make([]string, 0, 2+2*len(fsCheckpointPaths))
	extra = append(extra, "-image-path", checkpointPath)
	for _, p := range fsCheckpointPaths {
		extra = append(extra, "-fs-checkpoint-paths", p)
	}
	return r.buildArgs("checkpoint", containerName, extra...)
}

func (r *runsc) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string, fsCheckpointPaths []string) error {
	slog.InfoContext(ctx, "About to run runsc checkpoint", slog.String("container", containerName))

	cmd := r.execRunsc(ctx, r.checkpointArgs(containerName, checkpointPath, fsCheckpointPaths)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := r.runCmd(cmd)
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
	extra = append(extra, "-image-path", checkpointPath)
	for _, ddv := range fsCheckpointPaths {
		extra = append(extra, "-path", ddv)
	}
	return r.buildArgs("fscheckpoint", containerName, extra...)
}

func (r *runsc) cmdFsCheckpoint(ctx context.Context, containerName, checkpointPath string, fsCheckpointPaths []string) error {
	slog.InfoContext(ctx, "About to run runsc fscheckpoint", slog.String("container", containerName))

	cmd := r.execRunsc(ctx, r.fsCheckpointArgs(containerName, checkpointPath, fsCheckpointPaths)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := r.runCmd(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc fscheckpoint`: %w", err)
	}
	return nil
}

// restoreArgs builds the argv for `runsc restore <container>`. Factored out so
// the argument construction can be unit-tested without executing runsc.
func (r *runsc) restoreArgs(containerName, checkpointPath string, additionalArgs []string) []string {
	args := append(noRootContainerArgs(containerName),
		"-bundle", ociBundlePath(r.actorDirs, containerName),
		"-image-path", checkpointPath,
		"-pid-file", pidFilePath(r.actorDirs, containerName),
	)
	args = append(args, additionalArgs...)
	return r.buildArgs("restore", containerName, args...)
}

// We take a checkpoint only of the sandbox, but we need to call restore on each
// container, using the same checkpoint. Because runsc restore embeds Create and
// automatically creates the container from its bundle config.json when not yet
// created, calling cmdRestore directly avoids a separate runsc create CLI
// subprocess for both the sandbox and application containers.
func (r *runsc) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string, additionalArgs []string) error {
	slog.InfoContext(ctx, "About to run runsc restore", slog.String("container", containerName))

	if err := r.ensureSpec(containerName); err != nil {
		return fmt.Errorf("while writing the OCI spec for %q: %w", containerName, err)
	}

	cmd := r.execRunsc(ctx, r.restoreArgs(containerName, checkpointPath, additionalArgs)...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := r.runCmd(cmd); err != nil {
		return fmt.Errorf("while running `runsc restore`: %w", err)
	}
	return nil
}

func (r *runsc) cmdDelete(ctx context.Context, containerName string) error {
	cmd := r.execRunsc(
		ctx,
		r.buildArgs("delete", containerName)...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := r.runCmd(cmd)
	if err != nil {
		return fmt.Errorf("while running `runsc delete`: %w", err)
	}
	return nil
}

func (r *runsc) cmdState(ctx context.Context, containerName string) error {
	cmd := r.execRunsc(
		ctx,
		r.buildArgs("state", containerName)...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := r.runCmd(cmd); err != nil {
		return fmt.Errorf("while running `runsc state`: %w", err)
	}
	return nil
}

// cmdList returns the container IDs runsc has a record of.
func (r *runsc) cmdList(ctx context.Context) ([]string, error) {
	cmd := r.execRunsc(
		ctx,
		r.buildArgs("list", "")...,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := r.runCmd(cmd); err != nil {
		return nil, fmt.Errorf("while running `runsc list`: %w", err)
	}
	return strings.Fields(out.String()), nil
}

// cmdKill sends signal to the given container's process(es) inside the gVisor
// sandbox. Used during graceful shutdown to propagate SIGTERM to the actor.
func (r *runsc) cmdKill(ctx context.Context, containerName, signal string) error {
	slog.InfoContext(ctx, "About to run runsc kill", slog.String("container", containerName), slog.String("signal", signal))

	cmd := r.execRunsc(ctx, r.buildArgs("kill", "", containerName, signal)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := r.runCmd(cmd); err != nil {
		return fmt.Errorf("while running `runsc kill`: %w", err)
	}
	return nil
}

// runWait runs `runsc wait [extraArgs...] <container>` and blocks until the
// waited-on condition completes.
func (r *runsc) runWait(ctx context.Context, containerName string, extraArgs ...string) error {
	cmd := r.execRunsc(ctx, r.buildArgs("wait", containerName, extraArgs...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := r.runCmd(cmd); err != nil {
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

// containerWaiter is the slice of *runsc used to watch for unexpected
// container exits while wakeup probes are in flight.
type containerWaiter interface {
	cmdWait(ctx context.Context, containerName string) error
}

// wakeupLivenessMonitor tracks container liveness while HTTP wakeup probes are
// in flight and cancels the probe context if any application container exits.
type wakeupLivenessMonitor struct {
	probeCtx   context.Context
	cancel     context.CancelFunc
	waiter     containerWaiter
	containers []*ateworkerpb.Container
	exitedOnce sync.Once
	exitedErr  error
	wg         sync.WaitGroup
}

// run launches a background cmdWait for each named container in m.containers
// and cancels m.probeCtx on the first container that exits or fails cmdWait
// while m.probeCtx is still active.
func (m *wakeupLivenessMonitor) run() {
	defer m.wg.Done()
	if m.probeCtx.Err() != nil {
		return
	}
	var innerWG sync.WaitGroup
	for _, ac := range m.containers {
		name := ac.GetName()
		if name == "" {
			continue
		}
		innerWG.Add(1)
		go func(containerName string) {
			defer innerWG.Done()
			err := m.waiter.cmdWait(m.probeCtx, containerName)
			if m.probeCtx.Err() != nil {
				return
			}
			m.exitedOnce.Do(func() {
				if err != nil {
					m.exitedErr = fmt.Errorf("application container %q not running during wakeup probe: %w", containerName, err)
				} else {
					m.exitedErr = fmt.Errorf("application container %q exited before wakeup probe succeeded", containerName)
				}
				m.cancel()
			})
		}(name)
	}
	innerWG.Wait()
}

// waitForWakeupProbesWithLiveness runs probeWait while monitoring application
// containers for early exit. If probeWait is still running after delay, it
// starts a background cmdWait on each container in containers and cancels
// probeWait's context as soon as any container exits or fails its wait call,
// returning the container exit error instead of waiting for probeWait's full
// timeout. If probeWait completes before delay elapses, the timer is stopped
// and no cmdWait calls or background goroutines are launched.
func waitForWakeupProbesWithLiveness(
	ctx context.Context,
	waiter containerWaiter,
	containers []*ateworkerpb.Container,
	delay time.Duration,
	probeWait func(context.Context) error,
) error {
	if waiter == nil || len(containers) == 0 {
		return probeWait(ctx)
	}

	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	m := &wakeupLivenessMonitor{
		probeCtx:   probeCtx,
		cancel:     cancel,
		waiter:     waiter,
		containers: containers,
	}
	m.wg.Add(1)
	var timer *time.Timer
	if delay > 0 {
		timer = time.AfterFunc(delay, m.run)
	} else {
		go m.run()
	}

	err := probeWait(probeCtx)
	stopped := timer != nil && timer.Stop()
	cancel()
	if !stopped {
		m.wg.Wait()
	}
	if m.exitedErr != nil {
		return m.exitedErr
	}
	return err
}
