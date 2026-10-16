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

package scheduling

import (
	"context"
	"fmt"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// benchFleet is a fleet of n active workers that each report CPU and memory
// capacity and hold between one and fifteen residents, so allocations vary
// across the fleet the way they do under load, while Schedule exercises the
// comparison on every worker.
func benchFleet(n int) fleet {
	f := make(fleet, 0, n)
	for i := range n {
		opts := []func(*ateapipb.Worker){withMaxActors(16), withCapacity(16000, 64<<30)}
		for r := range i%15 + 1 {
			opts = append(opts, assignedFor("demo", fmt.Sprintf("resident-%d", r), resources.CPUMemory(500, 1<<30)))
		}
		f = append(f, worker(fmt.Sprintf("w-%d", i), "gvisor", "node-a", nil, opts...))
	}
	return f
}

// ingested is a WorkerSource over entries derived once, as the cache serves
// them, so Schedule is measured without the per-call derivation fleet does.
type ingested []*workercache.Entry

func (in ingested) Entries() ([]*workercache.Entry, error) { return in, nil }

func ingest(f fleet) ingested {
	entries, _ := f.Entries()
	return entries
}

func BenchmarkSchedule(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("workers=%d", n), func(b *testing.B) {
			s := New(ingest(benchFleet(n)), WithIntn(firstIntn))
			constraints := Constraints{SandboxClass: "gvisor", Limits: resources.CPUMemory(500, 1<<30)}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := s.Schedule(context.Background(), constraints); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// HasRoom reads a bare record, so unlike Schedule it parses the worker's
// quantities; it runs once per bind rather than once per worker per pass.
func BenchmarkHasRoom(b *testing.B) {
	w := benchFleet(1)[0]
	s := New(ingest(fleet{w}))
	constraints := Constraints{SandboxClass: "gvisor", Limits: resources.CPUMemory(500, 1<<30)}
	b.ReportAllocs()
	for b.Loop() {
		if !s.HasRoom(w, constraints) {
			b.Fatal("HasRoom() = false, want true")
		}
	}
}

// benchFleet568 models the 568-worker microVM scale fleet (176 actor slots and
// 176 vCPU / 704 GiB capacity per worker across 568 nodes).
func benchFleet568() fleet {
	const n = 568
	f := make(fleet, 0, n)
	for i := range n {
		opts := []func(*ateapipb.Worker){withMaxActors(176), withCapacity(176000, 704<<30)}
		for r := range i%16 + 1 {
			opts = append(opts, assignedFor("demo", fmt.Sprintf("resident-%d", r), resources.CPUMemory(500, 1<<30)))
		}
		f = append(f, worker(fmt.Sprintf("w-%d", i), "microvm", fmt.Sprintf("node-%d", i), nil, opts...))
	}
	return f
}

func BenchmarkSchedule_568Workers(b *testing.B) {
	rawFleet := benchFleet568()
	ingestedFleet := ingest(rawFleet)
	indexed := newIndexedFleet(rawFleet)
	limits := resources.CPUMemory(500, 1<<30)

	b.Run("ColdScan_IngestedEntries", func(b *testing.B) {
		s := New(ingestedFleet, WithIntn(firstIntn))
		constraints := Constraints{SandboxClass: "microvm", Limits: limits}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.Schedule(context.Background(), constraints); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("PreferredNodes_Hit", func(b *testing.B) {
		s := New(ingestedFleet, WithIntn(firstIntn))
		constraints := Constraints{
			SandboxClass:   "microvm",
			PreferredNodes: []string{"node-284"},
			Limits:         limits,
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.Schedule(context.Background(), constraints); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("PreferredWorker_IndexedHit", func(b *testing.B) {
		s := New(indexed, WithIntn(firstIntn))
		constraints := Constraints{
			SandboxClass:    "microvm",
			PreferredWorker: "w-284",
			Limits:          limits,
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.Schedule(context.Background(), constraints); err != nil {
				b.Fatal(err)
			}
		}
	})
}
