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

package agentsession

import (
	"math"
	"strconv"

	gluttonpb "go.ate.dev/substrate/internal/proto/glutton"
)

// maxScaledCPUParallelism caps per-step BurnCPU goroutines when running
// envelope-scaled scripts across high-density actor pools.
const maxScaledCPUParallelism int32 = 2

// MakeIngestBuf pre-generates a shared read-only random payload sized to the
// largest ingest operation across steps so callers can slice it without
// per-request allocation.
func MakeIngestBuf(steps []Step) []byte {
	return makeIngestBuf(steps)
}

// scaleBytes scales raw by scale, clamping to at least 1 byte when both raw
// and scale are positive.
func scaleBytes(raw int64, scale float64) int64 {
	if raw <= 0 || scale <= 0 {
		return 0
	}
	if scale == 1.0 {
		return raw
	}
	scaled := int64(math.Round(float64(raw) * scale))
	if scaled < 1 {
		scaled = 1
	}
	return scaled
}

// ToRunScriptRequest converts s into a gluttonpb.RunScriptRequest that executes
// all of the step's resource operations sequentially inside the sandbox in a
// single /runscript call, scaling burn_cpu by cpuScale, dwell by dwellScale,
// and byte sizes by sizeScale. Returns nil if the step contains no non-ping
// operations after scaling.
func (s Step) ToRunScriptRequest(cpuScale, dwellScale, sizeScale float64, ingestBuf []byte) *gluttonpb.RunScriptRequest {
	steps := make([]*gluttonpb.Step, 0, len(s.Ops))
	for _, o := range s.Ops {
		var req *gluttonpb.Request
		switch o.kind {
		case opPing:
			// The /runscript HTTP round-trip itself exercises router ingress
			// reachability; ping-only steps fall back to Ping.
			continue
		case opFillRAM:
			sz := scaleBytes(o.bytes, sizeScale)
			if sz <= 0 {
				continue
			}
			// Match execOp: OVERWRITE grows the array on first touch and
			// re-randomizes it in place on subsequent laps without doubling
			// guest heap.
			req = &gluttonpb.Request{
				Kind: &gluttonpb.Request_WriteRam{
					WriteRam: &gluttonpb.WriteRAMRequest{
						Key:       o.key,
						Size:      strconv.FormatInt(sz, 10),
						WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE,
					},
				},
			}
		case opChurnRAM:
			sz := scaleBytes(o.bytes, sizeScale)
			if sz <= 0 {
				continue
			}
			req = &gluttonpb.Request{
				Kind: &gluttonpb.Request_WriteRam{
					WriteRam: &gluttonpb.WriteRAMRequest{
						Key:       o.key,
						Size:      strconv.FormatInt(sz, 10),
						WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE,
					},
				},
			}
		case opWalkRAM:
			req = &gluttonpb.Request{
				Kind: &gluttonpb.Request_ReadRam{
					ReadRam: &gluttonpb.ReadRAMRequest{
						Key: o.key,
					},
				},
			}
		case opWriteDisk:
			sz := scaleBytes(o.bytes, sizeScale)
			if sz <= 0 {
				continue
			}
			req = &gluttonpb.Request{
				Kind: &gluttonpb.Request_WriteDisk{
					WriteDisk: &gluttonpb.WriteDiskRequest{
						Key:       o.key,
						Size:      int32(sz),
						WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
					},
				},
			}
		case opReadDiskDigest:
			req = &gluttonpb.Request{
				Kind: &gluttonpb.Request_ReadDisk{
					ReadDisk: &gluttonpb.ReadDiskRequest{
						Key:      o.key,
						ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY,
					},
				},
			}
		case opReadDiskData:
			req = &gluttonpb.Request{
				Kind: &gluttonpb.Request_ReadDisk{
					ReadDisk: &gluttonpb.ReadDiskRequest{
						Key:      o.key,
						ReadMode: gluttonpb.ReadMode_READ_MODE_DATA,
					},
				},
			}
		case opIngest:
			sz := scaleBytes(o.bytes, sizeScale)
			if sz <= 0 {
				continue
			}
			payload := ingestBuf
			if int64(len(payload)) < sz {
				payload = make([]byte, sz)
			} else {
				payload = payload[:sz]
			}
			req = &gluttonpb.Request{
				Kind: &gluttonpb.Request_Ingest{
					Ingest: &gluttonpb.IngestRequest{
						Key:     o.key,
						Payload: payload,
					},
				},
			}
		case opBurnCPU:
			if o.millis <= 0 || cpuScale <= 0 {
				continue
			}
			scaledMS := int64(math.Round(float64(o.millis) * cpuScale))
			if scaledMS < 1 {
				scaledMS = 1
			}
			par := min(max(o.parallel, 1), maxScaledCPUParallelism)
			req = &gluttonpb.Request{
				Kind: &gluttonpb.Request_BurnCpu{
					BurnCpu: &gluttonpb.BurnCPURequest{
						DurationMs:  scaledMS,
						Parallelism: par,
					},
				},
			}
		case opDwell:
			if o.millis <= 0 || dwellScale <= 0 {
				continue
			}
			scaledMS := int64(math.Round(float64(o.millis) * dwellScale))
			if scaledMS <= 0 {
				continue
			}
			req = &gluttonpb.Request{
				Kind: &gluttonpb.Request_Sleep{
					Sleep: &gluttonpb.SleepRequest{
						DurationMs: scaledMS,
					},
				},
			}
		}
		if req != nil {
			steps = append(steps, &gluttonpb.Step{
				Kind: &gluttonpb.Step_Operation{
					Operation: &gluttonpb.Operation{
						Requests: []*gluttonpb.Request{req},
					},
				},
			})
		}
	}
	if len(steps) == 0 {
		return nil
	}
	return &gluttonpb.RunScriptRequest{
		Script: &gluttonpb.Block{
			Steps: steps,
		},
	}
}
