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
	"cmp"
	"log/slog"
	"slices"
	"time"

	"go.ate.dev/substrate/internal/nodepath"
	"golang.org/x/sys/unix"
)

const (
	// DefaultMemoryPressureHighWatermark is the fraction of the local snapshot
	// filesystem capacity at or above which atelet evicts the oldest
	// already-durable idle local snapshots in LRU order. Below this watermark,
	// zero pressure evictions occur and the normal detach timer governs.
	DefaultMemoryPressureHighWatermark = 0.65
	// DefaultMemoryPressureLowWatermark is the target fraction of the local
	// snapshot filesystem capacity to which a pressure-eviction sweep trims
	// already-durable idle local snapshots once usage reaches the high
	// watermark.
	DefaultMemoryPressureLowWatermark = 0.50
	// DefaultMemoryPressureInterval is how often atelet checks the local
	// snapshot filesystem usage for memory pressure.
	DefaultMemoryPressureInterval = 5 * time.Second
	// defaultEstimatedSnapshotBytes is the fallback size estimate (64 MiB) used
	// when a local snapshot directory has zero allocated bytes on disk (for
	// example in unit tests that record metadata without writing sentry files).
	defaultEstimatedSnapshotBytes = 64 << 20
)

// DiskUsageFunc returns the used and total capacity in bytes of the filesystem
// backing path.
type DiskUsageFunc func(path string) (usedBytes, totalBytes uint64, err error)

// DefaultDiskUsage queries the filesystem backing path via unix.Statfs and
// returns its used and total capacity in bytes.
func DefaultDiskUsage(path string) (usedBytes, totalBytes uint64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bsize := uint64(st.Bsize)
	totalBytes = st.Blocks * bsize
	availBytes := st.Bavail * bsize
	if availBytes > totalBytes {
		availBytes = totalBytes
	}
	return totalBytes - availBytes, totalBytes, nil
}

// WithMemoryPressureWatermarks configures the high and low capacity watermarks
// (as fractions in [0, 1]) for LRU eviction of already-durable idle local
// snapshots. Setting high <= 0 disables memory-pressure eviction.
func WithMemoryPressureWatermarks(high, low float64) Option {
	return func(t *Tracker) {
		t.pressureHighWatermark = high
		t.pressureLowWatermark = low
	}
}

// WithMemoryPressureInterval sets how often the tracker checks local snapshot
// filesystem usage for memory pressure. Setting d <= 0 disables
// memory-pressure eviction.
func WithMemoryPressureInterval(d time.Duration) Option {
	return func(t *Tracker) {
		t.pressureInterval = d
	}
}

// WithDiskUsageFunc overrides the function used to query local snapshot
// filesystem usage.
func WithDiskUsageFunc(fn DiskUsageFunc) Option {
	return func(t *Tracker) {
		t.diskUsage = fn
	}
}

// evictionCandidate represents an idle actor whose LatestLocal snapshot is
// already durable in object storage and can be detached immediately to free
// local tmpfs/disk space.
type evictionCandidate struct {
	actorUID   string
	snapUUID   string
	commitTime time.Time
}

// runMemoryPressureEvictor periodically checks the filesystem usage of
// nodepath.SnapshotsDir and triggers LRU eviction of already-durable idle
// local snapshots whenever usage reaches pressureHighWatermark.
func (t *Tracker) runMemoryPressureEvictor() {
	ticker := t.clock.NewTicker(t.pressureInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C():
			t.evictUnderMemoryPressureOnce()
		}
	}
}

// evictUnderMemoryPressureOnce inspects the filesystem backing
// nodepath.SnapshotsDir and, if usage is at or above pressureHighWatermark,
// schedules immediate background detaches of the oldest already-durable idle
// local snapshots (ordered by LatestLocal.CommitTime ascending) until enough
// bytes are queued for eviction to bring usage down to pressureLowWatermark.
// It returns the number of actors queued for immediate detach.
func (t *Tracker) evictUnderMemoryPressureOnce() int {
	if t.store == nil || t.diskUsage == nil || t.pressureHighWatermark <= 0 {
		return 0
	}
	usedBytes, totalBytes, err := t.diskUsage(nodepath.SnapshotsDir)
	if err != nil || totalBytes == 0 {
		return 0
	}
	usedRatio := float64(usedBytes) / float64(totalBytes)
	if usedRatio < t.pressureHighWatermark {
		return 0
	}
	targetBytes := uint64(float64(totalBytes) * t.pressureLowWatermark)
	if usedBytes <= targetBytes {
		return 0
	}
	bytesToFree := int64(usedBytes - targetBytes)

	actorUIDs := t.collectKnownActorUIDs()
	if len(actorUIDs) == 0 {
		return 0
	}

	now := t.clock.Now()
	candidates := make([]evictionCandidate, 0, len(actorUIDs))
	for _, actorUID := range actorUIDs {
		if !t.locks.Lock(t.ctx, actorUID) {
			return 0
		}
		a := t.findLocked(actorUID)
		if isEligibleForPressureEviction(a, now) {
			candidates = append(candidates, evictionCandidate{
				actorUID:   actorUID,
				snapUUID:   a.LatestLocal.uuid(),
				commitTime: a.LatestLocal.CommitTime,
			})
		}
		t.unlock(actorUID)
	}
	if len(candidates) == 0 {
		return 0
	}

	slices.SortFunc(candidates, func(a, b evictionCandidate) int {
		if c := a.commitTime.Compare(b.commitTime); c != 0 {
			return c
		}
		return cmp.Compare(a.actorUID, b.actorUID)
	})

	var freedBytes int64
	evictedCount := 0
	for _, cand := range candidates {
		if freedBytes >= bytesToFree {
			break
		}
		snapBytes, err := localSnapshotSize(cand.actorUID, cand.snapUUID)
		if err != nil || snapBytes <= 0 {
			snapBytes = defaultEstimatedSnapshotBytes
		}
		if !t.locks.Lock(t.ctx, cand.actorUID) {
			break
		}
		a := t.findLocked(cand.actorUID)
		if isEligibleForPressureEviction(a, now) && a.LatestLocal.uuid() == cand.snapUUID {
			a.DetachAt = now
			t.saveLocked(a)
			t.requeueLocked(a)
			freedBytes += snapBytes
			evictedCount++
		}
		t.unlock(cand.actorUID)
	}

	if evictedCount > 0 {
		slog.InfoContext(t.ctx, "Scheduled LRU local snapshot detaches under memory pressure",
			slog.Float64("usedRatio", usedRatio),
			slog.Float64("highWatermark", t.pressureHighWatermark),
			slog.Float64("lowWatermark", t.pressureLowWatermark),
			slog.Int("evictedActors", evictedCount),
			slog.Int64("estimatedFreedBytes", freedBytes),
			slog.Int64("targetBytesToFree", bytesToFree),
		)
	}
	return evictedCount
}

// isEligibleForPressureEviction reports whether actor a is idle, unheld, has
// no operation in progress, has a local snapshot that is already durable in
// object storage, and is not already due for immediate detach.
func isEligibleForPressureEviction(a *actorSnapshots, now time.Time) bool {
	if a == nil || a.Started || a.hold != holdNone || a.op != nil {
		return false
	}
	if a.LatestLocal == nil || a.LatestDurable == nil || !sameUUID(a.LatestLocal, a.LatestDurable) {
		return false
	}
	due, ok := a.detachDue()
	if !ok {
		return false
	}
	return due.After(now)
}

// collectKnownActorUIDs returns a deduplicated list of actor UIDs currently
// tracked in memory or persisted on disk under nodepath.ActorsDir.
func (t *Tracker) collectKnownActorUIDs() []string {
	seen := make(map[string]struct{})
	t.mu.Lock()
	for uid := range t.actors {
		seen[uid] = struct{}{}
	}
	t.mu.Unlock()

	if onDisk, err := savedActorUIDs(); err == nil {
		for _, uid := range onDisk {
			seen[uid] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for uid := range seen {
		out = append(out, uid)
	}
	return out
}
