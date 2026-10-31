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
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/internal/benchmarking/boomer/agentsession"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	bmetrics "go.ate.dev/substrate/internal/benchmarking/boomer/metrics"
	gluttonpb "go.ate.dev/substrate/internal/proto/glutton"
	"google.golang.org/protobuf/proto"
)

const (
	// EnvEnvelopeUseLPA enables the Personal Assistant Light (LPA) multi-step
	// agent session workload inside GluttonUser when set to "true". Defaults to
	// false so standard GluttonUser runs are unaffected.
	EnvEnvelopeUseLPA = "ENVELOPE_USE_LPA"

	// EnvEnvelopeLPAScript optionally overrides the embedded agentsession script
	// name loaded when ENVELOPE_USE_LPA=true. Defaults to "personal-assistant-light".
	EnvEnvelopeLPAScript = "ENVELOPE_LPA_SCRIPT"

	// EnvEnvelopeLPACPUScale scales per-step burn_cpu durations when
	// ENVELOPE_USE_LPA=true. Defaults to 0.02, matching the 24h-to-30m time
	// compression factor in benchmarking/README.md.
	EnvEnvelopeLPACPUScale = "ENVELOPE_LPA_CPU_SCALE"

	// EnvEnvelopeLPADwellScale scales per-step dwell sleeps inside RunScript
	// when ENVELOPE_USE_LPA=true. Defaults to 0 because E-Nrun-1M actor live
	// hold time before suspend is governed by --min-live-time / --max-live-time.
	EnvEnvelopeLPADwellScale = "ENVELOPE_LPA_DWELL_SCALE"

	// EnvEnvelopeLPASizeScale scales per-step RAM and disk operation byte sizes
	// when ENVELOPE_USE_LPA=true. Defaults to 1.0 (unscaled).
	EnvEnvelopeLPASizeScale = "ENVELOPE_LPA_SIZE_SCALE"

	defaultLPAScriptName = "personal-assistant-light"
	defaultLPACPUScale   = 0.02
	defaultLPADwellScale = 0.0
	defaultLPASizeScale  = 1.0
	runScriptPath        = "/runscript"
)

// lpaConfig holds the parsed environment configuration and embedded script for
// ENVELOPE_USE_LPA=true.
type lpaConfig struct {
	enabled         bool
	scriptName      string
	cpuScale        float64
	dwellScale      float64
	sizeScale       float64
	script          *agentsession.Script
	ingestBuf       []byte
	stepBodies      [][]byte
	validStartSteps []uint16
	loadErr         error
}

var (
	lpaCfgOnce        sync.Once
	lpaCfgVal         lpaConfig
	globalLPAPhaseSeq atomic.Uint64
)

// validPostBootstrapStartSteps returns the 1-based step indices (1..len(reqs)-1)
// from which an actor can execute a complete full cycle of post-bootstrap steps
// (startIdx..len(reqs)-1 followed by 1..startIdx-1) using the initial RAM and
// disk keys populated during step 0 plus keys created along the cycle. Actors
// completing step 0 are staggered across these valid entry points so a 1M-actor
// fleet does not hit multi-MiB ingest steps in a single lockstep wave.
func validPostBootstrapStartSteps(reqs []*gluttonpb.RunScriptRequest) []uint16 {
	if len(reqs) <= 1 || reqs[0] == nil {
		return nil
	}
	ram0 := make(map[string]bool)
	disk0 := make(map[string]bool)
	for _, st := range reqs[0].GetScript().GetSteps() {
		for _, r := range st.GetOperation().GetRequests() {
			switch k := r.GetKind().(type) {
			case *gluttonpb.Request_WriteRam:
				if k.WriteRam.GetWriteMode() != gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE {
					ram0[k.WriteRam.GetKey()] = true
				}
			case *gluttonpb.Request_WriteDisk:
				disk0[k.WriteDisk.GetKey()] = true
			case *gluttonpb.Request_Ingest:
				disk0[k.Ingest.GetKey()] = true
			}
		}
	}

	n := len(reqs) - 1
	valid := make([]uint16, 0, n)
	for startIdx := 1; startIdx < len(reqs); startIdx++ {
		ram := make(map[string]bool, len(ram0)+8)
		for k, v := range ram0 {
			ram[k] = v
		}
		disk := make(map[string]bool, len(disk0)+8)
		for k, v := range disk0 {
			disk[k] = v
		}
		ok := true
		for offset := 0; offset < n && ok; offset++ {
			stepIdx := 1 + (startIdx-1+offset)%n
			if reqs[stepIdx] == nil {
				continue
			}
			for _, st := range reqs[stepIdx].GetScript().GetSteps() {
				for _, r := range st.GetOperation().GetRequests() {
					switch k := r.GetKind().(type) {
					case *gluttonpb.Request_WriteRam:
						if k.WriteRam.GetWriteMode() == gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE {
							if !ram[k.WriteRam.GetKey()] {
								ok = false
							}
						} else {
							ram[k.WriteRam.GetKey()] = true
						}
					case *gluttonpb.Request_ReadRam:
						if !ram[k.ReadRam.GetKey()] {
							ok = false
						}
					case *gluttonpb.Request_WriteDisk:
						disk[k.WriteDisk.GetKey()] = true
					case *gluttonpb.Request_Ingest:
						disk[k.Ingest.GetKey()] = true
					case *gluttonpb.Request_ReadDisk:
						if !disk[k.ReadDisk.GetKey()] {
							ok = false
						}
					}
				}
			}
		}
		if ok {
			valid = append(valid, uint16(startIdx))
		}
	}
	return valid
}

// nextLPAStepIndex computes the actor's next step index after completing idx.
// Step 0 (0000_gateway_start) runs once per actor on first wake to initialize
// persistent RAM and disk state, then transitions to a round-robin staggered
// step in validStartSteps (1..N-1). Subsequent wakes advance sequentially
// through the daily loop (1..N-1, wrapping from N-1 back to 1).
func nextLPAStepIndex(idx int, cfg lpaConfig) uint16 {
	if cfg.script == nil || len(cfg.script.Steps) <= 1 {
		return 0
	}
	if idx == 0 {
		if len(cfg.validStartSteps) > 0 {
			seq := globalLPAPhaseSeq.Add(1) - 1
			return cfg.validStartSteps[seq%uint64(len(cfg.validStartSteps))]
		}
		return 1
	}
	if idx+1 < len(cfg.script.Steps) {
		return uint16(idx + 1)
	}
	return 1
}

// loadLPAConfigFromEnv parses the ENVELOPE_USE_LPA environment variables and
// loads the configured agentsession script when enabled, pre-marshaling each
// step's RunScriptRequest protobuf once so concurrent VUs share the immutable
// request byte slices without per-request heap allocations.
func loadLPAConfigFromEnv() lpaConfig {
	enabled := strings.EqualFold(strings.TrimSpace(os.Getenv(EnvEnvelopeUseLPA)), "true")
	if !enabled {
		return lpaConfig{enabled: false}
	}
	scriptName := strings.TrimSpace(os.Getenv(EnvEnvelopeLPAScript))
	if scriptName == "" {
		scriptName = defaultLPAScriptName
	}
	cpuScale := parseEnvFloat(EnvEnvelopeLPACPUScale, defaultLPACPUScale)
	dwellScale := parseEnvFloat(EnvEnvelopeLPADwellScale, defaultLPADwellScale)
	sizeScale := parseEnvFloat(EnvEnvelopeLPASizeScale, defaultLPASizeScale)

	script, err := agentsession.Load(scriptName)
	var ingestBuf []byte
	var stepBodies [][]byte
	var validStartSteps []uint16
	if err == nil && script != nil {
		ingestBuf = agentsession.MakeIngestBuf(script.Steps)
		stepBodies = make([][]byte, len(script.Steps))
		stepReqs := make([]*gluttonpb.RunScriptRequest, len(script.Steps))
		for i, step := range script.Steps {
			req := step.ToRunScriptRequest(cpuScale, dwellScale, sizeScale, ingestBuf)
			stepReqs[i] = req
			if req == nil {
				continue
			}
			b, mErr := proto.Marshal(req)
			if mErr != nil {
				err = mErr
				break
			}
			stepBodies[i] = b
		}
		validStartSteps = validPostBootstrapStartSteps(stepReqs)
	}
	return lpaConfig{
		enabled:         true,
		scriptName:      scriptName,
		cpuScale:        cpuScale,
		dwellScale:      dwellScale,
		sizeScale:       sizeScale,
		script:          script,
		ingestBuf:       ingestBuf,
		stepBodies:      stepBodies,
		validStartSteps: validStartSteps,
		loadErr:         err,
	}
}

// parseEnvFloat reads an environment variable as a non-negative float64,
// returning def when unset or invalid.
func parseEnvFloat(key string, def float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return def
	}
	return v
}

// currentLPAConfig returns the cached process-wide LPA configuration.
func currentLPAConfig() lpaConfig {
	lpaCfgOnce.Do(func() {
		lpaCfgVal = loadLPAConfigFromEnv()
	})
	return lpaCfgVal
}

// lpaEnabled reports whether ENVELOPE_USE_LPA=true is active.
func lpaEnabled() bool {
	return currentLPAConfig().enabled
}

// buildLPARunScriptRequest converts an agentsession.Step into a consolidated
// gluttonpb.RunScriptRequest that executes all RAM, disk, ingest, CPU burn, and
// optional dwell operations sequentially inside the sandbox in a single HTTP
// round-trip.
func buildLPARunScriptRequest(step agentsession.Step, cfg lpaConfig) *gluttonpb.RunScriptRequest {
	return step.ToRunScriptRequest(cfg.cpuScale, cfg.dwellScale, cfg.sizeScale, cfg.ingestBuf)
}

// runLPAStep executes the actor's next step from the configured LPA script via
// POST /runscript, recording step 0 (initial gateway bootstrap) under
// Step_<name> and subsequent daily steps (1..N-1) under LPAStep without
// double-counting in boomer stats, and advancing u.lpaStepIdx on success.
func (u *gluttonActor) runLPAStep(ctx context.Context) {
	cfg := currentLPAConfig()
	if cfg.loadErr != nil || cfg.script == nil || len(cfg.script.Steps) == 0 {
		errMsg := "LPA script not loaded"
		if cfg.loadErr != nil {
			errMsg = cfg.loadErr.Error()
		}
		bmetrics.RecordFailure("http", "LPAStep", userClass, 0, errMsg)
		return
	}

	idx := int(u.lpaStepIdx) % len(cfg.script.Steps)
	step := cfg.script.Steps[idx]
	metricName := "LPAStep"
	if idx == 0 {
		metricName = "Step_" + step.Name
	}

	var body []byte
	if idx < len(cfg.stepBodies) {
		body = cfg.stepBodies[idx]
	} else {
		scriptReq := buildLPARunScriptRequest(step, cfg)
		if scriptReq != nil {
			var err error
			body, err = proto.Marshal(scriptReq)
			if err != nil {
				bmetrics.RecordFailure("http", metricName, userClass, 0, err.Error())
				return
			}
		}
	}
	if len(body) == 0 {
		u.ping(ctx)
		u.lpaStepIdx = nextLPAStepIndex(idx, cfg)
		return
	}

	ctx, span := u.cfg.Tracer.Start(ctx, metricName)
	defer span.End()

	start := time.Now()
	httpResp, respBody, err := u.doHTTPPost(ctx, runScriptPath, body)
	clientLatency := time.Since(start)
	if err != nil {
		boomerutil.LogSampledTrace(span, metricName, clientLatency, boomerutil.SourceClient, err)
		bmetrics.RecordFailure("http", metricName, userClass, clientLatency, err.Error())
		return
	}
	if httpResp.StatusCode >= 400 {
		httpErr := fmt.Errorf("HTTP %d: %s", httpResp.StatusCode, strings.TrimSpace(string(respBody)))
		boomerutil.LogSampledTrace(span, metricName, clientLatency, boomerutil.SourceClient, httpErr)
		bmetrics.RecordFailure("http", metricName, userClass, clientLatency, httpErr.Error())
		return
	}

	scriptResp := &gluttonpb.RunScriptResponse{}
	if err := proto.Unmarshal(respBody, scriptResp); err != nil {
		boomerutil.LogSampledTrace(span, metricName, clientLatency, boomerutil.SourceClient, err)
		bmetrics.RecordFailure("http", metricName, userClass, clientLatency, err.Error())
		return
	}

	u.lpaStepIdx = nextLPAStepIndex(idx, cfg)
	boomerutil.LogSampledTrace(span, metricName, clientLatency, boomerutil.SourceClient, nil)
	bmetrics.RecordSuccess("http", metricName, userClass, clientLatency, int64(len(respBody)))
}
