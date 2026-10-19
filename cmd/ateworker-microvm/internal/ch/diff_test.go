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
	"os"
	"path/filepath"
	"testing"
)

func TestDeflateAgainstBaseAndMergeSparseOverlayRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	const logicalSize = 16 << 20 // 16 MiB logical, 4096 pages
	const pageSize = 4096

	// Populate ~50% of logical pages as the golden base (8 MiB resident), and
	// mutate ~7% of resident pages (~576 KiB dirty) in the warm checkpoint.
	baseData := make([]byte, logicalSize)
	targetData := make([]byte, logicalSize)

	var baseRegions, targetRegions []region
	var expectedPunched, expectedKept int64

	for page := 0; page < logicalSize/pageSize; page++ {
		off := int64(page * pageSize)
		if page%2 == 0 {
			pBase := fill(page+1, pageSize)
			copy(baseData[off:], pBase)
			baseRegions = append(baseRegions, region{off: off, data: pBase})

			if page%28 == 0 {
				// ~7% of resident pages dirtied during warm execution.
				pTarget := fill(page+1000, pageSize)
				copy(targetData[off:], pTarget)
				targetRegions = append(targetRegions, region{off: off, data: pTarget})
				expectedKept += pageSize
			} else {
				copy(targetData[off:], pBase)
				targetRegions = append(targetRegions, region{off: off, data: pBase})
				expectedPunched += pageSize
			}
		} else if page%57 == 0 {
			// Newly faulted page where base had a hole.
			pTarget := fill(page+2000, pageSize)
			copy(targetData[off:], pTarget)
			targetRegions = append(targetRegions, region{off: off, data: pTarget})
			expectedKept += pageSize
		}
	}

	wantHash := sha256.Sum256(targetData)

	baseFile := filepath.Join(dir, "base-memory-ranges")
	targetFile := filepath.Join(dir, "target-memory-ranges")
	writeSparse(t, baseFile, logicalSize, baseRegions)
	writeSparse(t, targetFile, logicalSize, targetRegions)

	punched, kept, err := DeflateAgainstBase(ctx, baseFile, targetFile)
	if err != nil {
		t.Fatalf("DeflateAgainstBase: %v", err)
	}
	if punched != expectedPunched || kept != expectedKept {
		t.Fatalf("DeflateAgainstBase punched=%d kept=%d, want punched=%d kept=%d", punched, kept, expectedPunched, expectedKept)
	}

	outFile := filepath.Join(dir, "reconstructed-memory-ranges")
	if err := MergeSparseOverlay(ctx, baseFile, targetFile, outFile); err != nil {
		t.Fatalf("MergeSparseOverlay: %v", err)
	}

	gotBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("ReadFile(outFile): %v", err)
	}
	gotHash := sha256.Sum256(gotBytes)
	if gotHash != wantHash {
		t.Fatalf("round-trip SHA256 mismatch: got %x, want %x", gotHash, wantHash)
	}
}

func TestCreateDeltaAgainstBaseAndRestoreFromDeltaRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	const logicalSize = 16 << 20 // 16 MiB logical
	const pageSize = 4096

	baseData := make([]byte, logicalSize)
	targetData := make([]byte, logicalSize)

	var baseRegions, targetRegions []region
	var expectedKept int64

	for page := 0; page < logicalSize/pageSize; page++ {
		off := int64(page * pageSize)
		if page%2 == 0 {
			pBase := fill(page+1, pageSize)
			copy(baseData[off:], pBase)
			baseRegions = append(baseRegions, region{off: off, data: pBase})

			if page%28 == 0 {
				pTarget := fill(page+500, pageSize)
				copy(targetData[off:], pTarget)
				targetRegions = append(targetRegions, region{off: off, data: pTarget})
				expectedKept += pageSize
			} else {
				copy(targetData[off:], pBase)
				targetRegions = append(targetRegions, region{off: off, data: pBase})
			}
		} else if page%61 == 0 {
			pTarget := fill(page+900, pageSize)
			copy(targetData[off:], pTarget)
			targetRegions = append(targetRegions, region{off: off, data: pTarget})
			expectedKept += pageSize
		}
	}

	wantHash := sha256.Sum256(targetData)

	baseFile := filepath.Join(dir, "base")
	targetFile := filepath.Join(dir, "target")
	metaFile := filepath.Join(dir, "memory-ranges.diff-meta")
	dataFile := filepath.Join(dir, "memory-ranges.diff-data")
	outFile := filepath.Join(dir, "reconstructed")

	writeSparse(t, baseFile, logicalSize, baseRegions)
	writeSparse(t, targetFile, logicalSize, targetRegions)

	kept, err := CreateDeltaAgainstBase(ctx, baseFile, targetFile, metaFile, dataFile)
	if err != nil {
		t.Fatalf("CreateDeltaAgainstBase: %v", err)
	}
	if kept != expectedKept {
		t.Fatalf("CreateDeltaAgainstBase kept=%d, want %d", kept, expectedKept)
	}
	dataFi, err := os.Stat(dataFile)
	if err != nil {
		t.Fatalf("Stat(dataFile): %v", err)
	}
	if dataFi.Size() != expectedKept {
		t.Fatalf("dataFile size=%d, want %d", dataFi.Size(), expectedKept)
	}

	if err := RestoreFromDelta(ctx, baseFile, metaFile, dataFile, outFile); err != nil {
		t.Fatalf("RestoreFromDelta: %v", err)
	}

	gotBytes, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("ReadFile(outFile): %v", err)
	}
	gotHash := sha256.Sum256(gotBytes)
	if gotHash != wantHash {
		t.Fatalf("RestoreFromDelta SHA256 mismatch: got %x, want %x", gotHash, wantHash)
	}
}

func TestOverlaySparseRegionsRecordedAndRestoreSparseRegions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	const size = 8 << 20
	baseRegions := []region{
		{off: 0, data: fill(1, 64*1024)},
		{off: 2 << 20, data: fill(2, 128*1024)},
	}
	deltaRegions := []region{
		{off: 4096, data: fill(50, 8192)},      // inside first base region
		{off: 4 << 20, data: fill(60, 16*1024)}, // in a base hole
	}

	baseFile := filepath.Join(dir, "base")
	slotFile := filepath.Join(dir, "slot")
	deltaFile := filepath.Join(dir, "delta")

	writeSparse(t, baseFile, size, baseRegions)
	if err := CopySparseFileFast(ctx, baseFile, slotFile); err != nil {
		t.Fatalf("CopySparseFileFast: %v", err)
	}
	writeSparse(t, deltaFile, size, deltaRegions)

	baseBytes, err := os.ReadFile(baseFile)
	if err != nil {
		t.Fatal(err)
	}
	wantOverlaid := append([]byte(nil), baseBytes...)
	for _, r := range deltaRegions {
		copy(wantOverlaid[r.off:], r.data)
	}

	extents, overlaidBytes, err := OverlaySparseRegionsRecorded(ctx, deltaFile, slotFile)
	if err != nil {
		t.Fatalf("OverlaySparseRegionsRecorded: %v", err)
	}
	if overlaidBytes != 8192+16*1024 {
		t.Fatalf("overlaidBytes = %d, want %d", overlaidBytes, 8192+16*1024)
	}
	gotOverlaid, err := os.ReadFile(slotFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotOverlaid, wantOverlaid) {
		t.Fatal("slot contents after OverlaySparseRegionsRecorded do not match expected")
	}

	if err := RestoreSparseRegions(ctx, baseFile, slotFile, extents); err != nil {
		t.Fatalf("RestoreSparseRegions: %v", err)
	}
	gotRestored, err := os.ReadFile(slotFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotRestored, baseBytes) {
		t.Fatal("slot contents after RestoreSparseRegions do not match original base")
	}
}

func setup167MBTestFiles(b *testing.B) (dir, baseFile, targetFile, deltaSparseFile, metaFile, dataFile string) {
	b.Helper()
	dir = b.TempDir()
	const residentSize = 167 << 20 // 167 MiB resident guest RAM
	const pageSize = 4096

	baseFile = filepath.Join(dir, "golden-base")
	targetFile = filepath.Join(dir, "target-full")
	deltaSparseFile = filepath.Join(dir, "delta-sparse")
	metaFile = filepath.Join(dir, "memory-ranges.diff-meta")
	dataFile = filepath.Join(dir, "memory-ranges.diff-data")

	// Construct a 167 MiB base image and a target image with ~7.15 MiB (~4.3%) dirty pages.
	chunk := fill(7, 1<<20)
	bf, err := os.Create(baseFile)
	if err != nil {
		b.Fatal(err)
	}
	tf, err := os.Create(targetFile)
	if err != nil {
		b.Fatal(err)
	}
	for written := 0; written < residentSize; written += len(chunk) {
		n := len(chunk)
		if residentSize-written < n {
			n = residentSize - written
		}
		if _, err := bf.Write(chunk[:n]); err != nil {
			b.Fatal(err)
		}
		if _, err := tf.Write(chunk[:n]); err != nil {
			b.Fatal(err)
		}
	}
	bf.Close()

	dirtyPage := fill(99, pageSize)
	totalPages := residentSize / pageSize
	// Dirty 1 in every 23 pages (~7.26 MiB total across 167 MiB).
	for p := 0; p < totalPages; p += 23 {
		if _, err := tf.WriteAt(dirtyPage, int64(p*pageSize)); err != nil {
			b.Fatal(err)
		}
	}
	tf.Close()

	if err := CopySparseFileFast(context.Background(), targetFile, deltaSparseFile); err != nil {
		b.Fatal(err)
	}
	if _, _, err := DeflateAgainstBase(context.Background(), baseFile, deltaSparseFile); err != nil {
		b.Fatal(err)
	}
	if _, err := CreateDeltaAgainstBase(context.Background(), baseFile, targetFile, metaFile, dataFile); err != nil {
		b.Fatal(err)
	}
	return dir, baseFile, targetFile, deltaSparseFile, metaFile, dataFile
}

func BenchmarkDeflateAgainstBase_167MB(b *testing.B) {
	ctx := context.Background()
	dir, baseFile, targetFile, _, _, _ := setup167MBTestFiles(b)
	workFile := filepath.Join(dir, "work-deflate")
	b.SetBytes(167 << 20)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if err := CopySparseFileFast(ctx, targetFile, workFile); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, _, err := DeflateAgainstBase(ctx, baseFile, workFile); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCreateDeltaAgainstBase_167MB(b *testing.B) {
	ctx := context.Background()
	dir, baseFile, targetFile, _, _, _ := setup167MBTestFiles(b)
	metaOut := filepath.Join(dir, "bench.diff-meta")
	dataOut := filepath.Join(dir, "bench.diff-data")
	b.SetBytes(167 << 20)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := CreateDeltaAgainstBase(ctx, baseFile, targetFile, metaOut, dataOut); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMergeSparseOverlay_167MB(b *testing.B) {
	ctx := context.Background()
	dir, baseFile, _, deltaSparseFile, _, _ := setup167MBTestFiles(b)
	outFile := filepath.Join(dir, "merged-out")
	b.SetBytes(167 << 20)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := MergeSparseOverlay(ctx, baseFile, deltaSparseFile, outFile); err != nil {
			b.Fatal(err)
		}
	}
}
