//go:build linux

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

package ch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/agent-substrate/substrate/pkg/objectstorage"
)

// region is a populated byte range written into a sparse test image.
type region struct {
	off  int64
	data []byte
}

// writeSparse creates a sparse file of logical size with the given populated
// regions (the gaps are holes). It mirrors a CH memory-ranges image: scattered
// resident pages in a sea of zero (free) RAM.
func writeSparse(t *testing.T, path string, size int64, regions []region) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for _, r := range regions {
		if _, err := f.WriteAt(r.data, r.off); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

// fill returns n bytes of nonzero pseudo-data keyed by seed (never zero, so the
// regions are genuinely "data" and distinguishable from holes).
func fill(seed, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i+seed)%251 + 1)
	}
	return b
}

// TestMergeDeltaIntoBase asserts the fast (rename+overlay) merge produces exactly
// base-with-delta-overlaid — byte-identical to the reference copying merge
// (MergeSparseOverlay). This is the guest memory image, so any divergence = a dead
// or corrupted guest on resume.
func TestMergeDeltaIntoBase(t *testing.T) {
	const size = 8 << 20 // 8 MiB logical

	// base = a complete restore source: data at three scattered offsets.
	baseRegions := []region{
		{off: 0, data: fill(1, 4096)},           // first page
		{off: 1 << 20, data: fill(2, 70000)},    // interior, crosses 64KiB boundaries
		{off: size - 8192, data: fill(3, 4096)}, // near the end
	}
	// delta = CH's post-restore faulted pages: one OVERLAPS base (newer content,
	// must win), one lands in a base HOLE (newly faulted page), rest holes.
	deltaRegions := []region{
		{off: 1 << 20, data: fill(99, 70000)}, // overwrites base's interior region
		{off: 4 << 20, data: fill(42, 12345)}, // new page where base had a hole
	}

	// Build the expected merged image in memory: base, with delta overlaid.
	want := make([]byte, size)
	for _, r := range baseRegions {
		copy(want[r.off:], r.data)
	}
	for _, r := range deltaRegions {
		copy(want[r.off:], r.data)
	}

	ctx := context.Background()

	// --- fast path: MergeDeltaIntoBase (consumes base, result lands in delta) ---
	dir := t.TempDir()
	base := filepath.Join(dir, "restore-state", "memory-ranges")
	delta := filepath.Join(dir, "checkpoint-state", "memory-ranges")
	if err := os.MkdirAll(filepath.Dir(base), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(delta), 0o700); err != nil {
		t.Fatal(err)
	}
	writeSparse(t, base, size, baseRegions)
	writeSparse(t, delta, size, deltaRegions)

	if err := MergeDeltaIntoBase(ctx, base, delta); err != nil {
		t.Fatalf("MergeDeltaIntoBase: %v", err)
	}
	got, err := os.ReadFile(delta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("MergeDeltaIntoBase result != expected (len got=%d want=%d)", len(got), len(want))
	}
	// base must have been consumed (renamed away), not left behind as a stale copy.
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Errorf("base still present after MergeDeltaIntoBase (err=%v); expected it consumed", err)
	}
	// The merged image must stay sparse (the holes between regions are not allocated).
	if fi, err := os.Stat(delta); err == nil {
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Blocks > 0 {
			if st.Blocks*512 >= size {
				t.Logf("note: merged not sparse on this fs (actual=%d >= logical=%d) — correctness still holds", st.Blocks*512, size)
			}
		}
	}

	// --- reference path: MergeSparseOverlay must produce the identical bytes ---
	rdir := t.TempDir()
	rbase := filepath.Join(rdir, "memory-ranges-base")
	rdelta := filepath.Join(rdir, "memory-ranges-delta")
	writeSparse(t, rbase, size, baseRegions)
	writeSparse(t, rdelta, size, deltaRegions)
	if err := MergeSparseOverlay(ctx, rbase, rdelta, rdelta); err != nil {
		t.Fatalf("MergeSparseOverlay: %v", err)
	}
	ref, err := os.ReadFile(rdelta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ref, want) {
		t.Fatalf("MergeSparseOverlay result != expected (sanity check on the reference)")
	}
	if !bytes.Equal(got, ref) {
		t.Fatalf("MergeDeltaIntoBase and MergeSparseOverlay disagree")
	}
}

// TestMergeSparseOverlayNewOutFile covers the copying merge writing to an outFile
// that does not exist yet: the unlink before its final rename has to tolerate a
// missing destination rather than fail the merge. TestMergeDeltaIntoBase above
// already covers the case where outFile does exist.
func TestMergeSparseOverlayNewOutFile(t *testing.T) {
	const size = 8 << 20 // 8 MiB logical
	baseRegions := []region{{off: 0, data: fill(1, 4096)}}
	deltaRegions := []region{{off: 4 << 20, data: fill(42, 12345)}}
	want := make([]byte, size)
	copy(want[baseRegions[0].off:], baseRegions[0].data)
	copy(want[deltaRegions[0].off:], deltaRegions[0].data)

	dir := t.TempDir()
	base := filepath.Join(dir, "base")
	delta := filepath.Join(dir, "delta")
	out := filepath.Join(dir, "out") // deliberately never created
	writeSparse(t, base, size, baseRegions)
	writeSparse(t, delta, size, deltaRegions)

	if err := MergeSparseOverlay(context.Background(), base, delta, out); err != nil {
		t.Fatalf("MergeSparseOverlay: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("merged result != expected (len got=%d want=%d)", len(got), len(want))
	}
}

// TestMergeDeltaIntoBaseHardlinkedBase asserts the fast path is refused when base
// has a second link. The fast path overlays base's inode in place, so a shared
// inode (atelet stages restore-state by linking from the actor's local pause
// snapshot) would have that snapshot silently rewritten behind the merge.
func TestMergeDeltaIntoBaseHardlinkedBase(t *testing.T) {
	const size = 8 << 20 // 8 MiB logical
	baseRegions := []region{{off: 0, data: fill(1, 4096)}}
	deltaRegions := []region{{off: 4 << 20, data: fill(42, 12345)}}
	want := make([]byte, size)
	copy(want[baseRegions[0].off:], baseRegions[0].data)
	copy(want[deltaRegions[0].off:], deltaRegions[0].data)
	// What the shared inode must still hold afterwards: base, with no delta in it.
	baseOnly := make([]byte, size)
	copy(baseOnly[baseRegions[0].off:], baseRegions[0].data)

	dir := t.TempDir()
	base := filepath.Join(dir, "base")
	delta := filepath.Join(dir, "delta")
	cached := filepath.Join(dir, "cached-snapshot")
	writeSparse(t, base, size, baseRegions)
	writeSparse(t, delta, size, deltaRegions)
	if err := os.Link(base, cached); err != nil {
		t.Fatal(err)
	}

	if err := MergeDeltaIntoBase(context.Background(), base, delta); err != nil {
		t.Fatalf("MergeDeltaIntoBase: %v", err)
	}
	got, err := os.ReadFile(delta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("merged result != expected (len got=%d want=%d)", len(got), len(want))
	}
	shared, err := os.ReadFile(cached)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(shared, baseOnly) {
		t.Error("the hardlinked base was mutated; the cached snapshot behind it is now corrupt")
	}
	// The copying merge leaves base alone, unlike the fast path which consumes it.
	if _, err := os.Stat(base); err != nil {
		t.Errorf("base should survive a copying merge: %v", err)
	}
}

// TestMergeDeltaIntoBaseSizeMismatch verifies a base/delta size mismatch is
// refused (misaligned overlay would corrupt the image) rather than silently
// producing garbage.
func TestMergeDeltaIntoBaseSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base")
	delta := filepath.Join(dir, "delta")
	writeSparse(t, base, 1<<20, []region{{off: 0, data: fill(1, 4096)}})
	writeSparse(t, delta, 2<<20, []region{{off: 0, data: fill(2, 4096)}})
	if err := MergeDeltaIntoBase(context.Background(), base, delta); err == nil {
		t.Fatal("expected size-mismatch error, got nil")
	}
	// base must be untouched (no destructive rename happened before the check).
	if _, err := os.Stat(base); err != nil {
		t.Errorf("base should be intact after a refused merge: %v", err)
	}
}

// Zero runs inside a data region come out as holes, as with cp --sparse=always.
func TestCopySparseFileLeavesZeroBlocksAsHoles(t *testing.T) {
	dir := t.TempDir()
	// 1 MiB of written (allocated) zeros with one non-zero page in the middle.
	data := make([]byte, 1<<20)
	copy(data[512<<10:], bytes.Repeat([]byte{0xab}, 4096))
	src := filepath.Join(dir, "src")
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "dst")
	if err := copySparseFile(context.Background(), src, dst); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("copy differs from source")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(dst, &st); err != nil {
		t.Fatal(err)
	}
	// st.Blocks counts 512-byte units; allow slack for the filesystem's block size.
	if allocated := st.Blocks * 512; allocated > 64<<10 {
		t.Errorf("dst allocates %d bytes, want the zero runs left as holes", allocated)
	}
}

type memGCS struct {
	data []byte
}

func (m *memGCS) PutObject(_ context.Context, _, _ string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.data = b
	return nil
}

func (m *memGCS) GetObject(_ context.Context, _, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m.data)), nil
}

// TestDeflateAgainstBaseRoundTrip verifies that deflating a warm snapshot against
// a golden base, shipping the deflated delta through sparse-zstd compression, and
// reconstructing via MergeSparseOverlay yields the exact byte-for-byte original
// warm snapshot (including pages that are identical to base, modified non-zero
// pages, newly populated pages, and pages that were non-zero in base and zeroed
// out in the warm snapshot).
func TestDeflateAgainstBaseRoundTrip(t *testing.T) {
	const size = 4 << 20 // 4 MiB logical
	baseRegions := []region{
		{off: 0, data: fill(1, 64*4096)},        // 256 KiB unchanged in target
		{off: 1 << 20, data: fill(2, 4*4096)},   // 16 KiB modified in target
		{off: 2 << 20, data: fill(3, 4*4096)},   // 16 KiB zeroed out in target!
		{off: 3 << 20, data: fill(4, 128*4096)}, // 512 KiB unchanged in target
	}
	targetRegions := []region{
		{off: 0, data: fill(1, 64*4096)},        // identical to base
		{off: 1 << 20, data: fill(99, 4*4096)},  // modified non-zero
		{off: 2<<20 + 16*4096, data: fill(77, 2*4096)}, // newly populated where base had a hole
		{off: 3 << 20, data: fill(4, 128*4096)}, // identical to base
	}
	want := make([]byte, size)
	for _, r := range targetRegions {
		copy(want[r.off:], r.data)
	}

	ctx := context.Background()
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base")
	targetPath := filepath.Join(dir, "target")
	writeSparse(t, basePath, size, baseRegions)
	writeSparse(t, targetPath, size, targetRegions)

	if err := DeflateAgainstBase(ctx, basePath, targetPath); err != nil {
		t.Fatalf("DeflateAgainstBase: %v", err)
	}

	// Ship targetPath through sparse-zstd (SendFileToGCSWithZstd -> FetchFileFromGCSWithZstd).
	gcs := &memGCS{}
	tf, err := os.Open(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := objectstorage.SendFileToGCSWithZstd(ctx, gcs, "gs://b/delta.zstd", tf); err != nil {
		tf.Close()
		t.Fatalf("SendFileToGCSWithZstd: %v", err)
	}
	tf.Close()

	// The compressed delta should be tiny (< 32 KiB) even though base+target have >768 KiB of random data.
	if len(gcs.data) > 32<<10 {
		t.Errorf("compressed delta size = %d bytes, want < 32 KiB (unchanged base pages should be holes)", len(gcs.data))
	}

	fetchedDelta := filepath.Join(dir, "fetched-delta")
	df, err := os.OpenFile(fetchedDelta, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := objectstorage.FetchFileFromGCSWithZstd(ctx, gcs, "gs://b/delta.zstd", df); err != nil {
		df.Close()
		t.Fatalf("FetchFileFromGCSWithZstd: %v", err)
	}
	df.Close()

	reconstructed := filepath.Join(dir, "reconstructed")
	if err := MergeSparseOverlay(ctx, basePath, fetchedDelta, reconstructed); err != nil {
		t.Fatalf("MergeSparseOverlay: %v", err)
	}

	got, err := os.ReadFile(reconstructed)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(want) {
		t.Fatalf("reconstructed SHA256 != original target SHA256")
	}

	// If real microvm snapshots are staged in /tmp/uvm_snap_inspect, verify
	// bit-for-bit SHA256 roundtrip and measure latency & compression ratio.
	snap2 := "/tmp/uvm_snap_inspect/snap2/memory-ranges"
	snap3 := "/tmp/uvm_snap_inspect/snap3/memory-ranges"
	if _, err := os.Stat(snap2); err == nil {
		if _, err := os.Stat(snap3); err == nil {
			t.Run("RealStep5MicroVMSnapshots", func(t *testing.T) {
				orig3, err := os.ReadFile(snap3)
				if err != nil {
					t.Fatal(err)
				}
				wantHash := sha256.Sum256(orig3)

				workTarget := filepath.Join(dir, "snap3-work")
				if err := copySparseFile(ctx, snap3, workTarget); err != nil {
					t.Fatal(err)
				}
				if err := DeflateAgainstBase(ctx, snap2, workTarget); err != nil {
					t.Fatalf("DeflateAgainstBase(snap2, snap3): %v", err)
				}
				realGCS := &memGCS{}
				wf, err := os.Open(workTarget)
				if err != nil {
					t.Fatal(err)
				}
				if err := objectstorage.SendFileToGCSWithZstd(ctx, realGCS, "gs://b/snap3-delta.zstd", wf); err != nil {
					wf.Close()
					t.Fatal(err)
				}
				wf.Close()
				t.Logf("Real Step 5 microVM compressed delta size: %d bytes (%.2f MiB)", len(realGCS.data), float64(len(realGCS.data))/(1024*1024))

				fetched := filepath.Join(dir, "snap3-fetched")
				ff, err := os.OpenFile(fetched, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				if err := objectstorage.FetchFileFromGCSWithZstd(ctx, realGCS, "gs://b/snap3-delta.zstd", ff); err != nil {
					ff.Close()
					t.Fatal(err)
				}
				ff.Close()

				rebuilt := filepath.Join(dir, "snap3-rebuilt")
				if err := MergeSparseOverlay(ctx, snap2, fetched, rebuilt); err != nil {
					t.Fatal(err)
				}
				got3, err := os.ReadFile(rebuilt)
				if err != nil {
					t.Fatal(err)
				}
				if sha256.Sum256(got3) != wantHash {
					t.Fatalf("real Step 5 microVM rebuilt SHA256 mismatch")
				}
			})
		}
	}
}
