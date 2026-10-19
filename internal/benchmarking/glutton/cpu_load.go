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

package glutton

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gluttonpb "go.ate.dev/substrate/internal/proto/glutton"
)

// UseCPU replaces the currently-running CPU load pool. A num_cores of 0
// stops all CPU load. See UseCPURequest for how (num_cores, duty_cycle,
// cycle_length_ms, cap_at_gomaxprocs) shape the load.
func (s *Service) UseCPU(_ context.Context, req *gluttonpb.UseCPURequest) (*gluttonpb.UseCPUResponse, error) {
	if req.GetNumCores() < 0 {
		return nil, status.Error(codes.InvalidArgument, "num_cores must be non-negative")
	}
	if duty := req.GetDutyCycle(); duty < 0 || duty > 1 {
		return nil, status.Errorf(codes.InvalidArgument, "duty_cycle %v must be in [0, 1]", duty)
	}
	if req.GetCycleLengthMs() < 0 {
		return nil, status.Error(codes.InvalidArgument, "cycle_length_ms must be non-negative")
	}

	cycle := time.Duration(req.GetCycleLengthMs()) * time.Millisecond
	// cap_at_gomaxprocs defaults to true when unset.
	capAtMax := true
	if req.CapAtGomaxprocs != nil {
		capAtMax = *req.CapAtGomaxprocs
	}

	n := s.cpu.Set(int(req.GetNumCores()), req.GetDutyCycle(), cycle, capAtMax)
	return &gluttonpb.UseCPUResponse{NumCores: int32(n)}, nil
}

// EnvGluttonCPUBurnSliceMS overrides the default CPU burn cycle duration in
// milliseconds when a UseCPURequest has cycle_length_ms=0.
const EnvGluttonCPUBurnSliceMS = "GLUTTON_CPU_BURN_SLICE_MS"

// cpuLoadDefaultCycle is applied when a request has cycle_length_ms=0 and
// GLUTTON_CPU_BURN_SLICE_MS is not set. A 200ms slice lets sub-core actors
// yield between short burn chunks instead of spinning for 1000ms uninterrupted.
const cpuLoadDefaultCycle = 200 * time.Millisecond

// cpuBurnBatchIterations is the number of LCG iterations between
// clock_gettime(CLOCK_THREAD_CPUTIME_ID) reads so syscall traps (especially
// under gVisor systrap) do not dominate the spin loop.
const cpuBurnBatchIterations = 65536

// DefaultCPULoadCycle returns the effective default CPU burn cycle duration,
// honoring GLUTTON_CPU_BURN_SLICE_MS when set to a positive integer.
func DefaultCPULoadCycle() time.Duration {
	if raw := strings.TrimSpace(os.Getenv(EnvGluttonCPUBurnSliceMS)); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return cpuLoadDefaultCycle
}

// cpuBurnSink absorbs the inner loop's arithmetic so the compiler can't
// prove the work is dead and elide it.
var cpuBurnSink atomic.Uint64

// cpuLoad manages a pool of goroutines that consume CPU at a target
// (num_cores x duty_cycle) rate. Set replaces the running pool; Stop
// tears it down.
type cpuLoad struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	done   *sync.WaitGroup
	n      int
}

// Set spins numCores goroutines, each pinned to an OS thread and
// targeting dutyCycle fraction of one core per cycle. Any previously
// running pool is stopped first. When capAtGomaxprocs is true, numCores
// is clamped to GOMAXPROCS to avoid oversubscription (which pins cores
// at 100% and starves individual goroutines below their target). Returns
// the number of goroutines actually started.
func (c *cpuLoad) Set(numCores int, dutyCycle float64, cycleLen time.Duration, capAtGomaxprocs bool) int {
	if numCores < 0 {
		numCores = 0
	}
	if capAtGomaxprocs {
		if maxN := runtime.GOMAXPROCS(0); numCores > maxN {
			numCores = maxN
		}
	}
	if cycleLen <= 0 {
		cycleLen = DefaultCPULoadCycle()
	}
	if dutyCycle < 0 {
		dutyCycle = 0
	} else if dutyCycle > 1 {
		dutyCycle = 1
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.stopLocked()
	if numCores == 0 {
		return 0
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	c.cancel = cancel
	c.done = wg
	c.n = numCores
	for i := 0; i < numCores; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cpuLoadWorker(ctx, dutyCycle, cycleLen)
		}()
	}
	return numCores
}

// Stop tears down any running pool.
func (c *cpuLoad) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopLocked()
}

// stopLocked cancels the running pool, if any, and waits for its
// goroutines to exit. c.mu must be held.
func (c *cpuLoad) stopLocked() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	c.done.Wait()
	c.cancel = nil
	c.done = nil
	c.n = 0
}

// N returns the current pool size.
func (c *cpuLoad) N() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// cpuLoadWorker pins itself to one OS thread so CLOCK_THREAD_CPUTIME_ID
// tracks this goroutine's own execution, then alternates between
// spinning on real work and sleeping. The work loop measures CPU time
// (not wall time) so CFS throttling or scheduler preemption cannot fool
// it into thinking its budget is spent when it was frozen off-CPU.
func cpuLoadWorker(ctx context.Context, duty float64, cycle time.Duration) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	workBudget := time.Duration(float64(cycle) * duty)
	sleepFor := cycle - workBudget

	var x uint64 = 1
	for {
		select {
		case <-ctx.Done():
			cpuBurnSink.Store(x)
			return
		default:
		}

		if workBudget > 0 {
			start := threadCPUTime()
			for {
				// A batch between clock reads keeps the CPU-time syscall cost
				// from dominating the loop's own load (especially under gVisor).
				for i := 0; i < cpuBurnBatchIterations; i++ {
					x = x*1103515245 + 12345
				}
				if threadCPUTime()-start >= workBudget {
					break
				}
			}
		}

		if sleepFor > 0 {
			timer := time.NewTimer(sleepFor)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				cpuBurnSink.Store(x)
				return
			case <-timer.C:
			}
		}
	}
}

// threadCPUTime returns CPU time consumed by the current OS thread.
// The counter only advances while the thread is on-CPU, so preemption
// and CFS throttling are excluded. Panics if the syscall fails, which
// should be impossible for this clock id on any supported OS.
func threadCPUTime() time.Duration {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_THREAD_CPUTIME_ID, &ts); err != nil {
		panic(fmt.Errorf("clock_gettime(CLOCK_THREAD_CPUTIME_ID): %w", err))
	}
	return time.Duration(ts.Sec)*time.Second + time.Duration(ts.Nsec)
}
