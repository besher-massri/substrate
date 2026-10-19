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

package workercache

import (
	"cmp"
	"slices"

	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Entry is a cached Worker and what the cache derived from its record when it
// ingested it. Scheduling reads the derived figures for every Worker on every
// placement, so they are computed once per change rather than once per read.
// An Entry is immutable once published; a change to the Worker publishes a
// new one.
type Entry struct {
	Worker *ateapipb.Worker
	Room   Room
}

// Room is what a Worker has left for one more actor, derived from its recorded
// capacity and allocation.
type Room struct {
	// Free is capacity less allocated in every compute dimension either
	// record names. A dimension the Worker never reported is absent, which
	// reads as none of it: a Worker reports everything it has.
	Free Amounts
	// Utilization is the highest allocated/capacity ratio across the
	// dimensions with positive capacity, or 1 when there are none, so a Worker
	// reporting no compute capacity is never the less loaded of two.
	Utilization float64
	// Unreadable marks a record with a quantity that would not parse. Free is
	// nil then. The Worker's true occupancy is unknown, and the only answer
	// that cannot overcommit it is that it has no room.
	Unreadable bool
}

// Amount is a named quantity.
type Amount struct {
	Name     string
	Quantity resource.Quantity
}

// Amounts is a set of named quantities sorted by name. Each is backed by a
// plain int64 where its value allows, so comparing two Amounts allocates
// nothing and shares nothing. Sizes have two or three dimensions, so a scan
// beats a map.
type Amounts []Amount

// ParseAmounts reads the wire form, summing repeated names. It errors on a
// quantity it cannot parse rather than skipping it, so a malformed limit
// cannot silently become unconstrained.
func ParseAmounts(r *ateapipb.Resources) (Amounts, error) {
	quantities, err := resources.ParseQuantities(r)
	if err != nil {
		return nil, err
	}
	return amountsOf(quantities), nil
}

func amountsOf(quantities resources.Quantities) Amounts {
	if len(quantities) == 0 {
		return nil
	}
	out := make(Amounts, 0, len(quantities))
	for name, q := range quantities {
		out = append(out, Amount{Name: name, Quantity: int64Form(q)})
	}
	slices.SortFunc(out, func(a, b Amount) int { return cmp.Compare(a.Name, b.Name) })
	return out
}

// Get returns the quantity under name, or the zero quantity when absent.
func (a Amounts) Get(name string) resource.Quantity {
	for i := range a {
		if a[i].Name == name {
			return a[i].Quantity
		}
	}
	return resource.Quantity{}
}

// NewEntry pairs w with what scheduling reads of it.
func NewEntry(w *ateapipb.Worker) *Entry {
	return &Entry{Worker: w, Room: roomOf(w)}
}

// roomOf derives w's Room from its recorded capacity and allocation.
func roomOf(w *ateapipb.Worker) Room {
	capacity, err := resources.ParseQuantities(w.GetStatus().GetCapacity().GetResources())
	if err != nil {
		return Room{Unreadable: true, Utilization: 1}
	}
	allocated, err := resources.ParseQuantities(w.GetStatus().GetAllocated().GetResources())
	if err != nil {
		return Room{Unreadable: true, Utilization: 1}
	}
	utilization := utilizationOf(capacity, allocated)
	// capacity is this call's own map, so it can become the difference.
	free := capacity
	if free == nil {
		free = resources.Quantities{}
	}
	free.Sub(allocated)
	return Room{Free: amountsOf(free), Utilization: utilization}
}

// utilizationOf is the highest allocated/capacity ratio across the dimensions
// with positive capacity, or 1 when there are none.
func utilizationOf(capacity, allocated resources.Quantities) float64 {
	var (
		maxRatio    float64
		hasPositive bool
	)
	for name, capVal := range capacity {
		if capFloat := capVal.AsApproximateFloat64(); capFloat > 0 {
			hasPositive = true
			if usedVal, ok := allocated[name]; ok {
				maxRatio = max(maxRatio, usedVal.AsApproximateFloat64()/capFloat)
			}
		}
	}
	if !hasPositive {
		return 1
	}
	return maxRatio
}

// int64Form returns q backed by a plain int64 at whole, milli, micro, or nano
// scale when its value fits one, else q unchanged. ParseQuantity leaves values
// such as "1.5Gi" on its inf.Dec path, where Cmp converts the other operand to
// inf.Dec and allocates for it. Two int64 forms compare without allocating,
// and the form is a value, so a comparison shares nothing with the Entry.
func int64Form(q resource.Quantity) resource.Quantity {
	for _, scale := range []resource.Scale{0, resource.Milli, resource.Micro, resource.Nano} {
		// ScaledValue rounds up and silently overflows, so accept the result
		// only if it compares equal. Cmp against an inf.Dec rewrites its
		// receiver into one, hence the throwaway copy.
		candidate := *resource.NewScaledQuantity(q.ScaledValue(scale), scale)
		if compared := candidate; compared.Cmp(q) == 0 {
			return candidate
		}
	}
	return q
}
