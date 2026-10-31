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
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"unsafe"

	"go.ate.dev/substrate/internal/benchmarking/boomer/agentsession"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	gluttonsvc "go.ate.dev/substrate/internal/benchmarking/glutton"
	gluttonpb "go.ate.dev/substrate/internal/proto/glutton"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"
)

func resetLPAConfigForTest() {
	lpaCfgOnce = sync.Once{}
	lpaCfgVal = lpaConfig{}
	globalLPAPhaseSeq.Store(0)
}

func TestLPADisabledByDefault(t *testing.T) {
	t.Setenv(EnvEnvelopeUseLPA, "")
	resetLPAConfigForTest()
	t.Cleanup(resetLPAConfigForTest)

	if lpaEnabled() {
		t.Fatalf("lpaEnabled() = true, want false by default")
	}
}

func TestLPAEnabledLoadsPersonalAssistantLightAndExecutesAllSteps(t *testing.T) {
	t.Setenv(EnvEnvelopeUseLPA, "true")
	t.Setenv(EnvEnvelopeLPACPUScale, "0.001")
	t.Setenv(EnvEnvelopeLPADwellScale, "0")
	t.Setenv(EnvEnvelopeLPASizeScale, "0.01")
	resetLPAConfigForTest()
	t.Cleanup(resetLPAConfigForTest)

	if !lpaEnabled() {
		t.Fatalf("lpaEnabled() = false, want true when ENVELOPE_USE_LPA=true")
	}
	cfg := currentLPAConfig()
	if cfg.loadErr != nil {
		t.Fatalf("loadLPAConfigFromEnv error: %v", cfg.loadErr)
	}
	if cfg.script == nil || len(cfg.script.Steps) != 61 {
		t.Fatalf("loaded script steps = %d, want 61", len(cfg.script.Steps))
	}
	if len(cfg.validStartSteps) != 58 {
		t.Fatalf("len(cfg.validStartSteps) = %d, want 58 (all 60 daily steps except 43 and 44)", len(cfg.validStartSteps))
	}

	svc, err := gluttonsvc.New(t.TempDir())
	if err != nil {
		t.Fatalf("gluttonsvc.New: %v", err)
	}
	defer svc.Close()

	var serverErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != runScriptPath {
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
			return
		}
		reqBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req gluttonpb.RunScriptRequest
		if err := proto.Unmarshal(reqBytes, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := svc.RunScript(r.Context(), &req)
		if err != nil {
			serverErr = err
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	}))
	defer srv.Close()

	actor := &gluttonActor{
		cfg: &userclass.Config{
			Atespace:   "test-atespace",
			RouterURL:  srv.URL,
			HTTPClient: srv.Client(),
			Tracer:     otel.Tracer("test"),
		},
		actorName: "sb-test-lpa",
	}

	// Execute step 0 (bootstrap) plus all 60 daily steps (1..60) and verify
	// step 60 wraps around to step 1 (0300_dreaming) rather than re-running
	// step 0 (0000_gateway_start).
	for i := 0; i <= len(cfg.script.Steps); i++ {
		wantIdx := i + 1
		if i == len(cfg.script.Steps)-1 {
			wantIdx = 1
		} else if i == len(cfg.script.Steps) {
			wantIdx = 2
		}
		actor.runLPAStep(context.Background())
		if serverErr != nil {
			t.Fatalf("RunScript failed at iteration %d: %v", i, serverErr)
		}
		if int(actor.lpaStepIdx) != wantIdx {
			t.Fatalf("after iteration %d, actor.lpaStepIdx = %d, want %d", i, actor.lpaStepIdx, wantIdx)
		}
	}

	// Verify that actors staggered across all 58 valid post-bootstrap entry
	// points execute their initial post-bootstrap step cleanly after step 0.
	globalLPAPhaseSeq.Store(0)
	for k, wantStart := range cfg.validStartSteps {
		a := &gluttonActor{
			cfg:       actor.cfg,
			actorName: "sb-test-stagger",
		}
		a.runLPAStep(context.Background()) // step 0
		if a.lpaStepIdx != wantStart {
			t.Fatalf("staggered actor %d lpaStepIdx after step 0 = %d, want %d", k, a.lpaStepIdx, wantStart)
		}
		a.runLPAStep(context.Background()) // initial staggered step
		if serverErr != nil {
			t.Fatalf("staggered actor %d failed on step %d (%s): %v", k, wantStart, cfg.script.Steps[wantStart].Name, serverErr)
		}
	}
}

func TestLPAAllScriptsValidStartStepsFullTwoLapSimulation(t *testing.T) {
	cases := []struct {
		scriptName    string
		wantSteps     int
		wantValidLen  int
	}{
		{scriptName: "personal-assistant-light", wantSteps: 61, wantValidLen: 58},
		{scriptName: "coding-session", wantSteps: 20, wantValidLen: 3},
		{scriptName: "personal-assistant", wantSteps: 61, wantValidLen: 58},
	}

	for _, tc := range cases {
		t.Run(tc.scriptName, func(t *testing.T) {
			t.Setenv(EnvEnvelopeUseLPA, "true")
			t.Setenv(EnvEnvelopeLPAScript, tc.scriptName)
			t.Setenv(EnvEnvelopeLPACPUScale, "0")
			t.Setenv(EnvEnvelopeLPADwellScale, "0")
			t.Setenv(EnvEnvelopeLPASizeScale, "0.001")
			resetLPAConfigForTest()
			t.Cleanup(resetLPAConfigForTest)

			cfg := currentLPAConfig()
			if cfg.loadErr != nil {
				t.Fatalf("loadLPAConfigFromEnv(%s): %v", tc.scriptName, cfg.loadErr)
			}
			if cfg.script == nil || len(cfg.script.Steps) != tc.wantSteps {
				t.Fatalf("%s steps = %d, want %d", tc.scriptName, len(cfg.script.Steps), tc.wantSteps)
			}
			if len(cfg.validStartSteps) != tc.wantValidLen {
				t.Fatalf("%s len(validStartSteps) = %d (%v), want %d", tc.scriptName, len(cfg.validStartSteps), cfg.validStartSteps, tc.wantValidLen)
			}

			for k, wantStart := range cfg.validStartSteps {
				svc, err := gluttonsvc.New(t.TempDir())
				if err != nil {
					t.Fatalf("gluttonsvc.New: %v", err)
				}
				var serverErr error
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					reqBytes, _ := io.ReadAll(r.Body)
					var req gluttonpb.RunScriptRequest
					if err := proto.Unmarshal(reqBytes, &req); err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					resp, err := svc.RunScript(r.Context(), &req)
					if err != nil {
						serverErr = err
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
					out, _ := proto.Marshal(resp)
					w.Header().Set("Content-Type", "application/x-protobuf")
					_, _ = w.Write(out)
				}))

				globalLPAPhaseSeq.Store(uint64(k))
				a := &gluttonActor{
					cfg: &userclass.Config{
						Atespace:   "test-atespace",
						RouterURL:  srv.URL,
						HTTPClient: srv.Client(),
						Tracer:     otel.Tracer("test"),
					},
					actorName: "sb-test-2lap",
				}
				totalTurns := 1 + 2*(len(cfg.script.Steps)-1)
				for turn := 0; turn < totalTurns; turn++ {
					stepBefore := a.lpaStepIdx
					a.runLPAStep(context.Background())
					if serverErr != nil {
						srv.Close()
						svc.Close()
						t.Fatalf("%s start=%d failed at turn %d (step %d %s): %v",
							tc.scriptName, wantStart, turn, stepBefore, cfg.script.Steps[stepBefore].Name, serverErr)
					}
					if turn == 0 && a.lpaStepIdx != wantStart {
						srv.Close()
						svc.Close()
						t.Fatalf("%s after step 0 got lpaStepIdx=%d, want %d", tc.scriptName, a.lpaStepIdx, wantStart)
					}
				}
				srv.Close()
				svc.Close()
			}
		})
	}
}

func TestGluttonActorStructSizeAndHTTPShardDistribution(t *testing.T) {
	if sz := unsafe.Sizeof(gluttonActor{}); sz > 64 {
		t.Fatalf("unsafe.Sizeof(gluttonActor{}) = %d, want <= 64 bytes", sz)
	}
	clients := []*http.Client{{}, {}, {}, {}}
	cfg := &userclass.Config{HTTPClients: clients}
	seen := make(map[*http.Client]int)
	for i := 0; i < 40; i++ {
		a := &gluttonActor{cfg: cfg}
		seen[a.httpClient()]++
	}
	for idx, c := range clients {
		if seen[c] != 10 {
			t.Fatalf("shard %d selected %d times across 40 first-call actors, want 10", idx, seen[c])
		}
	}
}

func TestBuildLPARunScriptRequestHonorsDwellAndCPUScales(t *testing.T) {
	script, err := agentsession.Load("personal-assistant-light")
	if err != nil {
		t.Fatalf("agentsession.Load: %v", err)
	}
	step := script.Steps[0] // 0000_gateway_start
	req := buildLPARunScriptRequest(step, lpaConfig{
		enabled:    true,
		cpuScale:   0.02,
		dwellScale: 0,
		sizeScale:  1.0,
	})
	if req == nil || req.GetScript() == nil || len(req.GetScript().GetSteps()) == 0 {
		t.Fatalf("buildLPARunScriptRequest returned empty script")
	}
	for _, st := range req.GetScript().GetSteps() {
		for _, r := range st.GetOperation().GetRequests() {
			if _, ok := r.GetKind().(*gluttonpb.Request_Sleep); ok {
				t.Fatalf("expected dwell sleep to be omitted when dwellScale == 0")
			}
		}
	}
}
