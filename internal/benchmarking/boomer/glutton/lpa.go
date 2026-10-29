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
	enabled    bool
	scriptName string
	cpuScale   float64
	dwellScale float64
	sizeScale  float64
	script     *agentsession.Script
	ingestBuf  []byte
	loadErr    error
}

var (
	lpaCfgOnce sync.Once
	lpaCfgVal  lpaConfig
)

// loadLPAConfigFromEnv parses the ENVELOPE_USE_LPA environment variables and
// loads the configured agentsession script when enabled.
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
	if err == nil && script != nil {
		ingestBuf = agentsession.MakeIngestBuf(script.Steps)
	}
	return lpaConfig{
		enabled:    true,
		scriptName: scriptName,
		cpuScale:   cpuScale,
		dwellScale: dwellScale,
		sizeScale:  sizeScale,
		script:     script,
		ingestBuf:  ingestBuf,
		loadErr:    err,
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
// POST /runscript, recording both aggregate LPAStep and per-step Step_<name>
// latency metrics in boomer stats and advancing u.lpaStepIdx on success.
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

	idx := u.lpaStepIdx % len(cfg.script.Steps)
	step := cfg.script.Steps[idx]
	stepMetric := "Step_" + step.Name

	scriptReq := buildLPARunScriptRequest(step, cfg)
	if scriptReq == nil {
		u.ping(ctx)
		u.lpaStepIdx = (idx + 1) % len(cfg.script.Steps)
		return
	}

	ctx, span := u.cfg.Tracer.Start(ctx, "LPAStep")
	defer span.End()

	body, err := proto.Marshal(scriptReq)
	if err != nil {
		bmetrics.RecordFailure("http", "LPAStep", userClass, 0, err.Error())
		bmetrics.RecordFailure("step", stepMetric, userClass, 0, err.Error())
		return
	}

	start := time.Now()
	httpResp, respBody, err := u.doHTTPPost(ctx, runScriptPath, body)
	clientLatency := time.Since(start)
	if err != nil {
		boomerutil.LogSampledTrace(span, "LPAStep", clientLatency, boomerutil.SourceClient, err)
		bmetrics.RecordFailure("http", "LPAStep", userClass, clientLatency, err.Error())
		bmetrics.RecordFailure("step", stepMetric, userClass, clientLatency, err.Error())
		return
	}
	if httpResp.StatusCode >= 400 {
		httpErr := fmt.Errorf("HTTP %d: %s", httpResp.StatusCode, strings.TrimSpace(string(respBody)))
		boomerutil.LogSampledTrace(span, "LPAStep", clientLatency, boomerutil.SourceClient, httpErr)
		bmetrics.RecordFailure("http", "LPAStep", userClass, clientLatency, httpErr.Error())
		bmetrics.RecordFailure("step", stepMetric, userClass, clientLatency, httpErr.Error())
		return
	}

	scriptResp := &gluttonpb.RunScriptResponse{}
	if err := proto.Unmarshal(respBody, scriptResp); err != nil {
		boomerutil.LogSampledTrace(span, "LPAStep", clientLatency, boomerutil.SourceClient, err)
		bmetrics.RecordFailure("http", "LPAStep", userClass, clientLatency, err.Error())
		bmetrics.RecordFailure("step", stepMetric, userClass, clientLatency, err.Error())
		return
	}

	u.lpaStepIdx = (idx + 1) % len(cfg.script.Steps)
	boomerutil.LogSampledTrace(span, "LPAStep", clientLatency, boomerutil.SourceClient, nil)
	bmetrics.RecordSuccess("http", "LPAStep", userClass, clientLatency, int64(len(respBody)))
	bmetrics.RecordSuccess("step", stepMetric, userClass, clientLatency, 0)
}
