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
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

func limits(pairs ...string) *ateapipb.Resources {
	out := &ateapipb.Resources{}
	for i := 0; i < len(pairs); i += 2 {
		out.Limits = append(out.Limits, &ateapipb.Limits{Name: pairs[i], Quantity: pairs[i+1]})
	}
	return out
}

// recorded is a worker with one free actor slot whose capacity and allocation
// read as given, so a test is about the quantities alone.
func recorded(capacity, allocated *ateapipb.Resources) *ateapipb.Worker {
	return &ateapipb.Worker{Status: &ateapipb.WorkerStatus{
		Capacity:  &ateapipb.WorkerResources{Actors: 1, Resources: capacity},
		Allocated: &ateapipb.WorkerResources{Resources: allocated},
	}}
}

func mustNeed(t *testing.T, want *ateapipb.Resources) workercache.Amounts {
	t.Helper()
	need, err := workercache.ParseAmounts(want)
	if err != nil {
		t.Fatalf("ParseAmounts: %v", err)
	}
	return need
}

// The room check compares what the cache derived against what the actor
// parsed, so it is held to the same answers as parsing everything into
// Quantities, subtracting, and comparing dimension by dimension.
func TestHasRoomAgreesWithQuantities(t *testing.T) {
	tests := []struct {
		name      string
		want      *ateapipb.Resources
		capacity  *ateapipb.Resources
		allocated *ateapipb.Resources
		ok        bool
	}{
		{
			name:     "enough of every dimension",
			want:     limits("cpu", "500m", "memory", "1Gi"),
			capacity: limits("cpu", "4", "memory", "8Gi"),
			ok:       true,
		},
		{
			name:      "exactly what is left",
			want:      limits("cpu", "500m", "memory", "1Gi"),
			capacity:  limits("cpu", "2", "memory", "4Gi"),
			allocated: limits("cpu", "1500m", "memory", "3Gi"),
			ok:        true,
		},
		{
			name:      "short in one dimension once allocation is taken off",
			want:      limits("cpu", "500m", "memory", "1Gi"),
			capacity:  limits("cpu", "2", "memory", "4Gi"),
			allocated: limits("cpu", "1", "memory", "3584Mi"),
			ok:        false,
		},
		{
			name:     "a dimension the worker never reported is none of it",
			want:     limits("cpu", "1", "nvidia.com/gpu", "1"),
			capacity: limits("cpu", "4"),
			ok:       false,
		},
		{
			name:     "asking for none of an absent dimension fits",
			want:     limits("nvidia.com/gpu", "0"),
			capacity: limits("cpu", "4"),
			ok:       true,
		},
		{
			name:      "an overcommitted dimension covers nothing",
			want:      limits("cpu", "1"),
			capacity:  limits("cpu", "2"),
			allocated: limits("cpu", "3"),
			ok:        false,
		},
		{
			name:      "a dimension only the allocation names is overcommitted",
			want:      limits("nvidia.com/gpu", "1"),
			capacity:  limits("cpu", "2"),
			allocated: limits("nvidia.com/gpu", "1"),
			ok:        false,
		},
		{
			name:     "a repeated name sums rather than shadowing",
			want:     limits("cpu", "3"),
			capacity: limits("cpu", "2", "cpu", "1"),
			ok:       true,
		},
		{
			name:      "a repeated name in the allocation sums too",
			want:      limits("cpu", "1"),
			capacity:  limits("cpu", "2"),
			allocated: limits("cpu", "1", "cpu", "500m"),
			ok:        false,
		},
		{
			name:     "an actor asking for nothing fits anywhere",
			want:     nil,
			capacity: limits("cpu", "0"),
			ok:       true,
		},
		{
			name:     "an actor asking for nothing fits a worker reporting nothing",
			want:     nil,
			capacity: nil,
			ok:       true,
		},
		{
			name:     "a worker reporting nothing has none of anything",
			want:     limits("cpu", "1"),
			capacity: nil,
			ok:       false,
		},
		{
			// Fractional binary quantities leave ParseQuantity's int64 fast path.
			name:      "quantities off the int64 fast path compare exactly",
			want:      limits("memory", "1.5Gi"),
			capacity:  limits("memory", "3.5Gi"),
			allocated: limits("memory", "2Gi"),
			ok:        true,
		},
		{
			name:      "quantities off the int64 fast path still come up short",
			want:      limits("memory", "1.5Gi"),
			capacity:  limits("memory", "3.5Gi"),
			allocated: limits("memory", "2049Mi"),
			ok:        false,
		},
		{
			// Nine decimal places under a ten-digit byte count overflow an int64
			// at nano scale, so these stay inf.Dec: the slow comparison path.
			name:      "quantities that stay inf.Dec fit",
			want:      limits("memory", "1.5Gi"),
			capacity:  limits("memory", "12.000000001Gi"),
			allocated: limits("memory", "9.000000001Gi"),
			ok:        true,
		},
		{
			name:      "quantities that stay inf.Dec come up short",
			want:      limits("memory", "1.5Gi"),
			capacity:  limits("memory", "12.000000001Gi"),
			allocated: limits("memory", "10.600000001Gi"),
		},
		{
			name:      "an inf.Dec size against an int64 remainder",
			want:      limits("memory", "1.000000001Gi"),
			capacity:  limits("memory", "4Gi"),
			allocated: limits("memory", "2Gi"),
			ok:        true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := hasRoom(workercache.NewEntry(recorded(tc.capacity, tc.allocated)), mustNeed(t, tc.want))
			if got != tc.ok {
				t.Errorf("hasRoom() = %v, want %v", got, tc.ok)
			}
			if ref := referenceAnswer(t, tc.want, tc.capacity, tc.allocated); got != ref {
				t.Errorf("hasRoom() = %v but the long way through Quantities says %v", got, ref)
			}
		})
	}
}

// referenceAnswer is the question asked the long way, through Quantities. It
// is the specification hasRoom is checked against: capacity less allocated
// must reach the actor's size in every dimension the actor names. A dimension
// the worker never reported reads as zero, since a worker reports everything
// it has, and a dimension gone negative from overcommit covers nothing.
func referenceAnswer(t *testing.T, want, capacity, allocated *ateapipb.Resources) bool {
	t.Helper()
	need, err := resources.ParseQuantities(want)
	if err != nil {
		t.Fatal(err)
	}
	free, err := resources.ParseQuantities(capacity)
	if err != nil {
		t.Fatal(err)
	}
	if free == nil {
		free = resources.Quantities{}
	}
	used, err := resources.ParseQuantities(allocated)
	if err != nil {
		t.Fatal(err)
	}
	free.Sub(used)
	for name, q := range need {
		have := free[name] // absent reads as the zero quantity
		if have.Cmp(q) < 0 {
			return false
		}
	}
	return true
}

func TestHasRoomUnreadableRecords(t *testing.T) {
	t.Run("an unparseable size is an error", func(t *testing.T) {
		if _, err := workercache.ParseAmounts(limits("cpu", "banana")); err == nil {
			t.Error("ParseAmounts() = nil error, want one")
		}
	})

	need := mustNeed(t, limits("cpu", "1"))
	t.Run("an unparseable capacity has no room, even in a dimension not asked for", func(t *testing.T) {
		if hasRoom(workercache.NewEntry(recorded(limits("cpu", "4", "memory", "lots"), nil)), need) {
			t.Error("hasRoom() = true, want false")
		}
	})
	t.Run("an unparseable allocation has no room", func(t *testing.T) {
		if hasRoom(workercache.NewEntry(recorded(limits("cpu", "4"), limits("cpu", "some"))), need) {
			t.Error("hasRoom() = true, want false")
		}
	})
	t.Run("an actor asking for nothing is not blocked by an unreadable worker", func(t *testing.T) {
		if !hasRoom(workercache.NewEntry(recorded(limits("cpu", "banana"), nil)), nil) {
			t.Error("hasRoom() = false, want true")
		}
	})
	t.Run("HasRoom on a bare record answers the same", func(t *testing.T) {
		s := New(nil)
		constraints := Constraints{Limits: limits("cpu", "1")}
		if s.HasRoom(recorded(limits("cpu", "some"), nil), constraints) {
			t.Error("HasRoom() = true for an unreadable capacity, want false")
		}
		if !s.HasRoom(recorded(limits("cpu", "4"), nil), constraints) {
			t.Error("HasRoom() = false for a readable worker with room, want true")
		}
	})
}

// The check is asked of every worker in the fleet on every placement, so with
// the fleet already ingested it must not allocate per worker, whether or not
// the quantities take ParseQuantity's int64 fast path.
func TestHasRoomAllocatesNothingPerWorker(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		want, capacity, allocated *ateapipb.Resources
	}{
		{
			name:      "canonical quantities",
			want:      limits("cpu", "500m", "memory", "256Mi"),
			capacity:  limits("cpu", "16", "memory", "64Gi"),
			allocated: limits("cpu", "8500m", "memory", "9472Mi"),
		},
		{
			name:      "fractional binary quantities",
			want:      limits("memory", "1.5Gi"),
			capacity:  limits("memory", "63.5Gi"),
			allocated: limits("memory", "10.5Gi"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := workercache.NewEntry(recorded(tc.capacity, tc.allocated))
			need := mustNeed(t, tc.want)
			if !hasRoom(entry, need) {
				t.Fatal("hasRoom() = false, want true")
			}
			allocs := testing.AllocsPerRun(100, func() {
				if !hasRoom(entry, need) {
					t.Fatal("hasRoom() = false, want true")
				}
			})
			if allocs != 0 {
				t.Errorf("hasRoom() allocates %v times per worker, want 0", allocs)
			}
		})
	}
}

// An Entry is shared by every pass and every goroutine scheduling at once, so
// comparing against it must never write into it, whichever form either side
// takes. Quantity.Cmp rewrites its receiver into inf.Dec when the other side
// is one; this checks that rewrite lands on a copy.
func TestHasRoomLeavesTheEntryUntouched(t *testing.T) {
	for _, tc := range []struct {
		name      string
		want      string
		capacity  string
		allocated string
	}{
		{name: "int64 remainder against an int64 size", want: "1.5Gi", capacity: "3.5Gi", allocated: "1.5Gi"},
		{name: "int64 remainder against an inf.Dec size", want: "1.000000001Gi", capacity: "4Gi", allocated: "1Gi"},
		{name: "inf.Dec remainder against an int64 size", want: "1.5Gi", capacity: "12.000000001Gi", allocated: "9.000000001Gi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := workercache.NewEntry(recorded(limits("memory", tc.capacity), limits("memory", tc.allocated)))
			before := entry.Room.Free.Get("memory").DeepCopy()
			need := mustNeed(t, limits("memory", tc.want))
			for i := range 3 {
				if !hasRoom(entry, need) {
					t.Fatalf("round %d: hasRoom() = false, want true", i)
				}
			}
			after := entry.Room.Free.Get("memory")
			if after.Cmp(before) != 0 || after.String() != before.String() {
				t.Errorf("free memory drifted from %v to %v", before.String(), after.String())
			}
		})
	}
}
