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

package workercache_test

import (
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/resource"
)

func limits(pairs ...string) *ateapipb.Resources {
	out := &ateapipb.Resources{}
	for i := 0; i < len(pairs); i += 2 {
		out.Limits = append(out.Limits, &ateapipb.Limits{Name: pairs[i], Quantity: pairs[i+1]})
	}
	return out
}

func recorded(capacity, allocated *ateapipb.Resources) *ateapipb.Worker {
	return &ateapipb.Worker{Status: &ateapipb.WorkerStatus{
		Capacity:  &ateapipb.WorkerResources{Actors: 1, Resources: capacity},
		Allocated: &ateapipb.WorkerResources{Resources: allocated},
	}}
}

// amounts renders what RoomOf derived in a form a table can state.
func amounts(a workercache.Amounts) []string {
	out := make([]string, 0, len(a))
	for _, x := range a {
		out = append(out, x.Name+"="+x.Quantity.String())
	}
	return out
}

// sameAmounts compares by value, since the stored form may print differently
// from how the table spells the same quantity.
func sameAmounts(t *testing.T, got workercache.Amounts, want []string) bool {
	t.Helper()
	if len(got) != len(want) {
		return false
	}
	for i, w := range want {
		name, quantity, ok := strings.Cut(w, "=")
		if !ok {
			t.Fatalf("malformed want %q", w)
		}
		if got[i].Name != name || got[i].Quantity.Cmp(resource.MustParse(quantity)) != 0 {
			return false
		}
	}
	return true
}

func TestNewEntryDerivesRoom(t *testing.T) {
	tests := []struct {
		name            string
		capacity        *ateapipb.Resources
		allocated       *ateapipb.Resources
		wantFree        []string
		wantUtilization float64
		wantUnreadable  bool
	}{
		{
			name:            "capacity less allocated, sorted by name",
			capacity:        limits("memory", "8Gi", "cpu", "4"),
			allocated:       limits("cpu", "1", "memory", "2Gi"),
			wantFree:        []string{"cpu=3", "memory=6Gi"},
			wantUtilization: 0.25,
		},
		{
			name:            "utilization is the hottest dimension",
			capacity:        limits("cpu", "4", "memory", "8Gi"),
			allocated:       limits("cpu", "1", "memory", "6Gi"),
			wantFree:        []string{"cpu=3", "memory=2Gi"},
			wantUtilization: 0.75,
		},
		{
			name:            "repeated names sum",
			capacity:        limits("cpu", "2", "cpu", "1"),
			allocated:       limits("cpu", "500m", "cpu", "500m"),
			wantFree:        []string{"cpu=2"},
			wantUtilization: 1.0 / 3,
		},
		{
			name:            "a dimension only the allocation names goes negative",
			capacity:        limits("cpu", "2"),
			allocated:       limits("nvidia.com/gpu", "1"),
			wantFree:        []string{"cpu=2", "nvidia.com/gpu=-1"},
			wantUtilization: 0,
		},
		{
			name:            "no capacity at all is fully utilized",
			wantFree:        nil,
			wantUtilization: 1,
		},
		{
			name:            "zero capacity is fully utilized",
			capacity:        limits("cpu", "0"),
			wantFree:        []string{"cpu=0"},
			wantUtilization: 1,
		},
		{
			name:            "an unparseable capacity is unreadable",
			capacity:        limits("cpu", "4", "memory", "lots"),
			wantUtilization: 1,
			wantUnreadable:  true,
		},
		{
			name:            "an unparseable allocation is unreadable",
			capacity:        limits("cpu", "4"),
			allocated:       limits("cpu", "some"),
			wantUtilization: 1,
			wantUnreadable:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := workercache.NewEntry(recorded(tc.capacity, tc.allocated)).Room
			if !sameAmounts(t, got.Free, tc.wantFree) {
				t.Errorf("Free = %v, want %v", amounts(got.Free), tc.wantFree)
			}
			if got.Utilization != tc.wantUtilization {
				t.Errorf("Utilization = %v, want %v", got.Utilization, tc.wantUtilization)
			}
			if got.Unreadable != tc.wantUnreadable {
				t.Errorf("Unreadable = %v, want %v", got.Unreadable, tc.wantUnreadable)
			}
		})
	}
}

func TestParseAmounts(t *testing.T) {
	t.Run("sorted by name with repeated names summed", func(t *testing.T) {
		got, err := workercache.ParseAmounts(limits("memory", "1Gi", "cpu", "500m", "cpu", "500m"))
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"cpu=1", "memory=1Gi"}; !sameAmounts(t, got, want) {
			t.Errorf("ParseAmounts() = %v, want %v", amounts(got), want)
		}
	})
	t.Run("nothing asked for is nil", func(t *testing.T) {
		got, err := workercache.ParseAmounts(nil)
		if err != nil || got != nil {
			t.Errorf("ParseAmounts(nil) = %v, %v; want nil, nil", got, err)
		}
	})
	t.Run("a malformed quantity is an error", func(t *testing.T) {
		if _, err := workercache.ParseAmounts(limits("cpu", "banana")); err == nil {
			t.Error("ParseAmounts() = nil error, want one")
		}
	})
	t.Run("an absent name reads as zero", func(t *testing.T) {
		got, err := workercache.ParseAmounts(limits("cpu", "1"))
		if err != nil {
			t.Fatal(err)
		}
		if have := got.Get("memory"); !have.IsZero() {
			t.Errorf("Get(memory) = %v, want zero", have.String())
		}
	})
}

// Fractional binary quantities parse onto inf.Dec, where every comparison
// against them allocates. The cache stores them in int64 form when their
// value allows, so a pass over the fleet compares without allocating.
func TestAmountsTakeInt64Form(t *testing.T) {
	a, err := workercache.ParseAmounts(limits("memory", "3.5Gi"))
	if err != nil {
		t.Fatal(err)
	}
	// Comparing two int64 forms allocates nothing; any inf.Dec involved would.
	got, other := a.Get("memory"), a.Get("memory")
	if allocs := testing.AllocsPerRun(100, func() { _ = got.Cmp(other) }); allocs != 0 {
		t.Errorf("Cmp between stored forms allocates %v times, want 0", allocs)
	}
	// want is inf.Dec, and Cmp rewrites its receiver to match, so it is the
	// receiver here rather than the stored form.
	if want := resource.MustParse("3.5Gi"); want.Cmp(got) != 0 {
		t.Fatalf("Get(memory) = %v, want %v", got.String(), want.String())
	}
}

func BenchmarkNewEntry(b *testing.B) {
	w := recorded(limits("cpu", "16", "memory", "64Gi"), limits("cpu", "8500m", "memory", "9472Mi"))
	b.ReportAllocs()
	for b.Loop() {
		_ = workercache.NewEntry(w)
	}
}
