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

package snapshottracker

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// writeTestLocalSnapshotFile creates a dense file of sizeBytes inside the local
// snapshot directory for (actorUID, snapUUID) so localSnapshotSize reports
// sizeBytes allocated on disk.
func writeTestLocalSnapshotFile(t *testing.T, actorUID, snapUUID string, sizeBytes int64) {
	t.Helper()
	dir := ateletpath.LocalSnapshotDir(actorUID, snapUUID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "pages.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 64<<10)
	for i := range buf {
		buf[i] = 0x5a
	}
	for rem := sizeBytes; rem > 0; {
		chunk := min(rem, int64(len(buf)))
		if _, err := f.Write(buf[:chunk]); err != nil {
			t.Fatal(err)
		}
		rem -= chunk
	}
}

// commitDurableIdleActor creates the actor directory, writes a local snapshot
// file of sizeBytes, and records the snapshot as both LatestDurable and
// LatestLocal at commitTime so the actor is idle and already durable.
func commitDurableIdleActor(t *testing.T, tr *Tracker, actorUID, snapUUID string, commitTime time.Time, sizeBytes int64) {
	t.Helper()
	if err := os.MkdirAll(ateletpath.ActorPath(actorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestLocalSnapshotFile(t, actorUID, snapUUID, sizeBytes)
	local := testRef(snapUUID, 1, commitTime)
	durable := local
	durable.UploadTime = commitTime.Add(time.Second)
	tr.CommitDurable(actorUID, durable)
	tr.CommitLocal(actorUID, local)
}

func TestMemoryPressureBelowHighWatermarkDoesNothing(t *testing.T) {
	useTempNodeDirs(t)
	store := newFakeSnapshotStore()
	// 60% usage is below the 65% default high-water mark.
	tr := newStoreTracker(t, store,
		WithMemoryPressureWatermarks(0.65, 0.50),
		WithMemoryPressureInterval(5*time.Second),
		WithDiskUsageFunc(func(string) (uint64, uint64, error) {
			return 600 << 20, 1000 << 20, nil
		}),
	)
	tr.Start()

	t0 := tr.clock.Now()
	for i := 1; i <= 3; i++ {
		actorUID := fmt.Sprintf("actor-%d", i)
		snapUUID := fmt.Sprintf("snap-%d", i)
		commitDurableIdleActor(t, tr, actorUID, snapUUID, t0.Add(time.Duration(i)*time.Minute), 1<<20)
	}

	if got := tr.evictUnderMemoryPressureOnce(); got != 0 {
		t.Fatalf("evictUnderMemoryPressureOnce() = %d below high watermark, want 0", got)
	}
	for i := 1; i <= 3; i++ {
		actorUID := fmt.Sprintf("actor-%d", i)
		snapUUID := fmt.Sprintf("snap-%d", i)
		if got := uuidOf(tr.State(actorUID).GetLatestLocal()); got != snapUUID {
			t.Errorf("State(%q).LatestLocal = %q, want %q kept", actorUID, got, snapUUID)
		}
	}
}

func TestMemoryPressureAboveHighWatermarkEvictsOldestDurableIdleFirstDownToLowWatermark(t *testing.T) {
	useTempNodeDirs(t)
	store := newFakeSnapshotStore()
	// 4 actors * 2 MiB each on a 10 MiB filesystem currently at 7 MiB (70% >= 65%
	// high-water mark). Target low-water mark is 50% (5 MiB), so 2 MiB must be
	// freed -> exactly the 1 oldest 2 MiB snapshot if 2 MiB each, or with 1 MiB
	// each -> exactly the 2 oldest actors (actor-1 at t0 and actor-2 at t0+1m).
	tr := newStoreTracker(t, store,
		WithMemoryPressureWatermarks(0.65, 0.50),
		WithMemoryPressureInterval(5*time.Second),
		WithDiskUsageFunc(func(string) (uint64, uint64, error) {
			return 7 << 20, 10 << 20, nil
		}),
	)
	tr.Start()

	t0 := tr.clock.Now()
	for i := 1; i <= 4; i++ {
		actorUID := fmt.Sprintf("actor-%d", i)
		snapUUID := fmt.Sprintf("snap-%d", i)
		commitDurableIdleActor(t, tr, actorUID, snapUUID, t0.Add(time.Duration(i-1)*time.Minute), 1<<20)
	}

	// Step the fake clock by the 5s pressure check interval so runMemoryPressureEvictor triggers.
	stepTo(tr, t0.Add(5*time.Second))

	eventually(t, "the 2 oldest durable idle actors are detached", func() bool {
		return tr.State("actor-1").GetLatestLocal() == nil && tr.State("actor-2").GetLatestLocal() == nil
	})

	for _, actorUID := range []string{"actor-1", "actor-2"} {
		st := tr.State(actorUID)
		if st.GetLatestLocal() != nil {
			t.Errorf("State(%q).LatestLocal = %v, want nil after LRU pressure eviction", actorUID, st.GetLatestLocal())
		}
		if st.GetLatestDurable() == nil {
			t.Errorf("State(%q).LatestDurable = nil, want durable snapshot retained", actorUID)
		}
		if _, err := os.Stat(ateletpath.LocalCheckpointsDir(actorUID)); !os.IsNotExist(err) {
			t.Errorf("LocalCheckpointsDir(%q) stat err = %v, want not exist", actorUID, err)
		}
	}

	// The 2 newest actors (actor-3 at t0+2m, actor-4 at t0+3m) must remain in local RAM.
	for i := 3; i <= 4; i++ {
		actorUID := fmt.Sprintf("actor-%d", i)
		snapUUID := fmt.Sprintf("snap-%d", i)
		if got := uuidOf(tr.State(actorUID).GetLatestLocal()); got != snapUUID {
			t.Errorf("State(%q).LatestLocal = %q, want %q retained in local cache", actorUID, got, snapUUID)
		}
	}
	if got := store.uploads(); len(got) != 0 {
		t.Errorf("uploads = %v, want none for already-durable snapshots", got)
	}
}

func TestMemoryPressureSkipsStartedAndHeldActorsAndPrioritizesDurableFirst(t *testing.T) {
	useTempNodeDirs(t)
	store := newFakeSnapshotStore()
	tr := newStoreTracker(t, store,
		WithMemoryPressureWatermarks(0.55, 0.50),
		WithMemoryPressureInterval(5*time.Second),
		WithDiskUsageFunc(func(string) (uint64, uint64, error) {
			return 6 << 20, 10 << 20, nil
		}),
	)
	tr.Start()

	t0 := tr.clock.Now()

	// 1. Started actor (oldest, t0) - must NOT be evicted.
	commitDurableIdleActor(t, tr, "actor-started", "snap-started", t0, 1<<20)
	endStart, err := tr.BeginStart(t.Context(), "actor-started")
	if err != nil {
		t.Fatalf("BeginStart: %v", err)
	}
	endStart()

	// 2. Held actor (t0+1m, checkpoint in progress) - must NOT be evicted.
	commitDurableIdleActor(t, tr, "actor-held", "snap-held", t0.Add(time.Minute), 1<<20)
	_, endCheckpoint, err := tr.BeginCheckpoint(t.Context(), "actor-held", testRef("snap-held-2", 2, t0.Add(time.Minute)).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	defer endCheckpoint()

	// 3. Undurable actor (t0+2m, LatestDurable == nil) - prioritized after already-durable actors.
	if err := os.MkdirAll(ateletpath.ActorPath("actor-undurable"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestLocalSnapshotFile(t, "actor-undurable", "snap-undurable", 1<<20)
	tr.CommitLocal("actor-undurable", testRef("snap-undurable", 1, t0.Add(2*time.Minute)))

	// 4. Eligible durable idle actor (t0+3m) - MUST be evicted first despite newer CommitTime than actor-undurable.
	commitDurableIdleActor(t, tr, "actor-eligible", "snap-eligible", t0.Add(3*time.Minute), 1<<20)

	// 6 MiB used - 5 MiB target = 1 MiB to free -> only 1 actor (already-durable actor-eligible) is detached.
	if got := tr.evictUnderMemoryPressureOnce(); got != 1 {
		t.Fatalf("evictUnderMemoryPressureOnce() = %d, want 1 (only actor-eligible)", got)
	}
	eventually(t, "actor-eligible is detached", func() bool {
		return tr.State("actor-eligible").GetLatestLocal() == nil
	})

	for _, tc := range []struct {
		actorUID string
		snapUUID string
	}{
		{"actor-started", "snap-started"},
		{"actor-held", "snap-held"},
		{"actor-undurable", "snap-undurable"},
	} {
		if got := uuidOf(tr.State(tc.actorUID).GetLatestLocal()); got != tc.snapUUID {
			t.Errorf("State(%q).LatestLocal = %q, want %q untouched", tc.actorUID, got, tc.snapUUID)
		}
	}
}

func TestMemoryPressureDetachesAndUploadsUndurableDeferredCheckpointsInLRUOrder(t *testing.T) {
	useTempNodeDirs(t)
	store := newFakeSnapshotStore()
	tr := newStoreTracker(t, store,
		WithUploadDelay(48*time.Hour),
		WithDetachDelay(72*time.Hour),
		WithMemoryPressureWatermarks(0.55, 0.40),
		WithMemoryPressureInterval(5*time.Second),
		WithDiskUsageFunc(func(string) (uint64, uint64, error) {
			return 6 << 20, 10 << 20, nil
		}),
	)
	tr.Start()

	t0 := tr.clock.Now()
	for i := 1; i <= 3; i++ {
		actorUID := fmt.Sprintf("cold-actor-%d", i)
		snapUUID := fmt.Sprintf("snap-gen2-%d", i)
		if err := os.MkdirAll(ateletpath.ActorPath(actorUID), 0o700); err != nil {
			t.Fatal(err)
		}
		writeTestLocalSnapshotFile(t, actorUID, snapUUID, 1<<20)
		tr.CommitLocal(actorUID, testRef(snapUUID, 2, t0.Add(time.Duration(i)*time.Minute)))
	}

	// 6 MiB used - 4 MiB target = 2 MiB to free -> 2 oldest cold actors (cold-actor-1, cold-actor-2) scheduled for detach.
	if got := tr.evictUnderMemoryPressureOnce(); got != 2 {
		t.Fatalf("evictUnderMemoryPressureOnce() = %d, want 2", got)
	}
	eventually(t, "cold-actor-1 and cold-actor-2 are uploaded and detached", func() bool {
		return tr.State("cold-actor-1").GetLatestLocal() == nil && tr.State("cold-actor-2").GetLatestLocal() == nil
	})
	for i, actorUID := range []string{"cold-actor-1", "cold-actor-2"} {
		st := tr.State(actorUID)
		if st.GetLatestLocal() != nil {
			t.Errorf("State(%q).LatestLocal = %v, want nil after detach", actorUID, st.GetLatestLocal())
		}
		wantUUID := fmt.Sprintf("snap-gen2-%d", i+1)
		if got := uuidOf(st.GetLatestDurable()); got != wantUUID {
			t.Errorf("State(%q).LatestDurable = %q, want %q uploaded before local prune", actorUID, got, wantUUID)
		}
		if _, err := os.Stat(ateletpath.LocalCheckpointsDir(actorUID)); !os.IsNotExist(err) {
			t.Errorf("LocalCheckpointsDir(%q) stat err = %v, want not exist", actorUID, err)
		}
	}
	if got := uuidOf(tr.State("cold-actor-3").GetLatestLocal()); got != "snap-gen2-3" {
		t.Errorf("State(cold-actor-3).LatestLocal = %q, want snap-gen2-3 kept", got)
	}
}

func TestMemoryPressureAccountsForInFlightDetaches(t *testing.T) {
	useTempNodeDirs(t)
	store := newFakeSnapshotStore()
	// 6 MiB used on 10 MiB filesystem (high=0.55, low=0.40 -> 2 MiB to free).
	// Disable background detach workers (WithDetachWorkers(0)) so scheduled
	// detaches remain pending in the queue with DetachAt <= now.
	tr := newStoreTracker(t, store,
		WithDetachWorkers(0),
		WithMemoryPressureWatermarks(0.55, 0.40),
		WithMemoryPressureInterval(2*time.Second),
		WithDiskUsageFunc(func(string) (uint64, uint64, error) {
			return 6 << 20, 10 << 20, nil
		}),
	)
	tr.Start()

	t0 := tr.clock.Now()
	for i := 1; i <= 4; i++ {
		actorUID := fmt.Sprintf("actor-%d", i)
		snapUUID := fmt.Sprintf("snap-%d", i)
		commitDurableIdleActor(t, tr, actorUID, snapUUID, t0.Add(time.Duration(i)*time.Minute), 1<<20)
	}

	// First pressure sweep schedules 2 actors (2 MiB) for immediate detach.
	if got := tr.evictUnderMemoryPressureOnce(); got != 2 {
		t.Fatalf("first evictUnderMemoryPressureOnce() = %d, want 2", got)
	}
	// Second pressure sweep while those 2 actors (2 MiB) are still pending
	// detach must account for their pending 2 MiB and evict 0 additional actors.
	if got := tr.evictUnderMemoryPressureOnce(); got != 0 {
		t.Fatalf("second evictUnderMemoryPressureOnce() with 2 MiB already pending = %d, want 0", got)
	}
}

func TestDefaultDiskUsage(t *testing.T) {
	used, total, err := DefaultDiskUsage(t.TempDir())
	if err != nil {
		t.Fatalf("DefaultDiskUsage: %v", err)
	}
	if total == 0 {
		t.Fatal("DefaultDiskUsage total = 0, want > 0")
	}
	if used > total {
		t.Errorf("DefaultDiskUsage used = %d > total = %d", used, total)
	}
}
