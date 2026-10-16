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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The fixtures are cloud-hypervisor v53.0 snapshots of one 384 MiB guest
// (paths in config.json neutralized):
//
//   - memfd-*: the guest booted with memfd RAM (memory.size set, no zones);
//   - filebacked-*: the same guest after a restore that mapped its RAM from a
//     file (UseFileBackedMemory), snapshotted by cloud-hypervisor itself, which
//     wrote no memory-ranges and an empty memory_ranges table.
const fixtureDir = "testdata/filebacked"

const fixtureRAM = 402653184 // 384 MiB

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// snapshotDir copies a fixture pair into a fresh snapshot directory.
func snapshotDir(t *testing.T, prefix string) string {
	t.Helper()
	dir := t.TempDir()
	for _, f := range []string{"config.json", "state.json"} {
		if err := os.WriteFile(filepath.Join(dir, f), readFixture(t, prefix+"-"+f), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// guestRAMFile creates a (sparse) guest RAM file of size bytes.
func guestRAMFile(t *testing.T, size int64) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "slot-1")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	return p
}

func readDir(t *testing.T, dir string) (cfg, state []byte) {
	t.Helper()
	var err error
	if cfg, err = os.ReadFile(filepath.Join(dir, "config.json")); err != nil {
		t.Fatal(err)
	}
	if state, err = os.ReadFile(filepath.Join(dir, "state.json")); err != nil {
		t.Fatal(err)
	}
	return cfg, state
}

func memorySectionOf(t *testing.T, cfg []byte) json.RawMessage {
	t.Helper()
	o, err := parseObject(cfg)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := o.get("memory")
	if !ok {
		t.Fatal("no memory section")
	}
	return m
}

// mmField returns the raw value of field in the memory manager's state.
func mmField(t *testing.T, state []byte, field string) json.RawMessage {
	t.Helper()
	s, err := parseMemoryManagerState(state)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := s.state.get(field)
	if !ok {
		t.Fatalf("memory manager state has no %s", field)
	}
	return v
}

// The memory-manager state is a JSON string inside state.json, so its quotes
// are escaped there.
const (
	memfdRangesInFile = `\"memory_ranges\":{\"data\":[{\"gpa\":0,\"length\":402653184}]}`
	emptyRangesInFile = `\"memory_ranges\":{\"data\":[]}`
)

func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Errorf("temp file %s left in %s", e.Name(), dir)
		}
	}
}

func TestUseFileBackedMemoryOnCHSnapshot(t *testing.T) {
	dir := snapshotDir(t, "memfd")
	origCfg, origState := readDir(t, dir)
	slot := guestRAMFile(t, fixtureRAM)

	orig, err := UseFileBackedMemory(dir, slot)
	if err != nil {
		t.Fatal(err)
	}
	if want := memorySectionOf(t, origCfg); !bytes.Equal(orig, want) {
		t.Errorf("returned memory section %s, want the original %s", orig, want)
	}
	cfg, state := readDir(t, dir)

	// The memory section is exactly what cloud-hypervisor itself writes for
	// that zone (its snapshot of the guest restored this way), and nothing
	// else in config.json changed.
	chZone := memorySectionOf(t, readFixture(t, "filebacked-config.json"))
	wantMem := bytes.Replace(chZone, []byte("/run/ateom-golden-slots/base1-0a1b2c3d/slots-42/slot-1"), []byte(slot), 1)
	if got := memorySectionOf(t, cfg); !bytes.Equal(got, wantMem) {
		t.Errorf("memory section\n got %s\nwant %s", got, wantMem)
	}
	wantCfg := bytes.Replace(origCfg, memorySectionOf(t, origCfg), wantMem, 1)
	if !bytes.Equal(cfg, wantCfg) {
		t.Errorf("config.json changed beyond its memory section:\n got %s\nwant %s", cfg, wantCfg)
	}

	// state.json: only the memory manager's memory_ranges, now empty.
	if strings.Count(string(origState), memfdRangesInFile) != 1 {
		t.Fatal("fixture does not hold the memfd memory_ranges table once")
	}
	wantState := strings.Replace(string(origState), memfdRangesInFile, emptyRangesInFile, 1)
	if string(state) != wantState {
		t.Error("state.json changed beyond the memory manager's memory_ranges")
	}
	assertNoTempFiles(t, dir)
}

func TestCanonicalizeFileBackedSnapshotOnCHSnapshot(t *testing.T) {
	memfdCfg, memfdState := readFixture(t, "memfd-config.json"), readFixture(t, "memfd-state.json")
	origMemory := memorySectionOf(t, memfdCfg)

	for _, tc := range []struct {
		name       string
		origMemory json.RawMessage
	}{
		{"saved memory section", origMemory},
		{"derived memory section", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := snapshotDir(t, "filebacked")
			fbCfg, fbState := readDir(t, dir)
			if err := CanonicalizeFileBackedSnapshot(dir, tc.origMemory); err != nil {
				t.Fatal(err)
			}
			cfg, state := readDir(t, dir)

			// The memory sections are the original ones, exactly.
			if got := memorySectionOf(t, cfg); !bytes.Equal(got, origMemory) {
				t.Errorf("memory section\n got %s\nwant %s", got, origMemory)
			}
			if got, want := mmField(t, state, "memory_ranges"), mmField(t, memfdState, "memory_ranges"); !bytes.Equal(got, want) {
				t.Errorf("memory_ranges %s, want the memfd snapshot's %s", got, want)
			}
			// Nothing else changed.
			wantCfg := bytes.Replace(fbCfg, memorySectionOf(t, fbCfg), origMemory, 1)
			if !bytes.Equal(cfg, wantCfg) {
				t.Errorf("config.json changed beyond its memory section:\n got %s\nwant %s", cfg, wantCfg)
			}
			wantState := strings.Replace(string(fbState), emptyRangesInFile, memfdRangesInFile, 1)
			if string(state) != wantState {
				t.Error("state.json changed beyond the memory manager's memory_ranges")
			}
			assertNoTempFiles(t, dir)

			// A canonical snapshot is left alone.
			if err := CanonicalizeFileBackedSnapshot(dir, tc.origMemory); err != nil {
				t.Fatalf("second canonicalization: %v", err)
			}
			cfg2, state2 := readDir(t, dir)
			if !bytes.Equal(cfg2, cfg) || !bytes.Equal(state2, state) {
				t.Error("canonicalizing a canonical snapshot changed it")
			}
		})
	}
}

// UseFileBackedMemory then CanonicalizeFileBackedSnapshot on the same
// directory (what a preserved restore directory goes through) gives back the
// original files, byte for byte.
func TestFileBackedRoundTrip(t *testing.T) {
	dir := snapshotDir(t, "memfd")
	origCfg, origState := readDir(t, dir)
	orig, err := UseFileBackedMemory(dir, guestRAMFile(t, fixtureRAM))
	if err != nil {
		t.Fatal(err)
	}
	if err := CanonicalizeFileBackedSnapshot(dir, orig); err != nil {
		t.Fatal(err)
	}
	cfg, state := readDir(t, dir)
	if !bytes.Equal(cfg, origCfg) {
		t.Errorf("config.json not restored:\n got %s\nwant %s", cfg, origCfg)
	}
	if !bytes.Equal(state, origState) {
		t.Error("state.json not restored byte for byte")
	}
}

// mutateState rewrites the memory manager's state in a state.json through a
// generic decode that keeps numbers exact.
func mutateState(t *testing.T, state []byte, fn func(mm map[string]any)) []byte {
	t.Helper()
	var top map[string]any
	dec := json.NewDecoder(bytes.NewReader(state))
	dec.UseNumber()
	if err := dec.Decode(&top); err != nil {
		t.Fatal(err)
	}
	data := top["snapshots"].(map[string]any)[memoryManagerSnapshotID].(map[string]any)["snapshot_data"].(map[string]any)
	var mm map[string]any
	dec = json.NewDecoder(strings.NewReader(data["state"].(string)))
	dec.UseNumber()
	if err := dec.Decode(&mm); err != nil {
		t.Fatal(err)
	}
	fn(mm)
	b, err := json.Marshal(mm)
	if err != nil {
		t.Fatal(err)
	}
	data["state"] = string(b)
	out, err := json.Marshal(top)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func mutateMemory(t *testing.T, cfg []byte, fn func(mem map[string]any)) []byte {
	t.Helper()
	var top map[string]any
	dec := json.NewDecoder(bytes.NewReader(cfg))
	dec.UseNumber()
	if err := dec.Decode(&top); err != nil {
		t.Fatal(err)
	}
	fn(top["memory"].(map[string]any))
	out, err := json.Marshal(top)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func firstMapping(mm map[string]any) map[string]any {
	return mm["guest_ram_mappings"].([]any)[0].(map[string]any)
}

func TestUseFileBackedMemoryRejectsUnsupportedLayouts(t *testing.T) {
	memfdCfg, memfdState := readFixture(t, "memfd-config.json"), readFixture(t, "memfd-state.json")
	for _, tc := range []struct {
		name     string
		cfg      func() []byte
		state    func() []byte
		slotSize int64
		slotDir  bool
	}{
		{name: "user-defined zones", cfg: func() []byte {
			return mutateMemory(t, memfdCfg, func(m map[string]any) {
				m["size"] = json.Number("0")
				m["zones"] = []any{
					map[string]any{"id": "mem0", "size": json.Number("201326592"), "shared": true},
					map[string]any{"id": "mem1", "size": json.Number("201326592"), "shared": true},
				}
			})
		}},
		{name: "memory hotplug", cfg: func() []byte {
			return mutateMemory(t, memfdCfg, func(m map[string]any) { m["hotplug_size"] = json.Number("1073741824") })
		}},
		{name: "hugepages", cfg: func() []byte {
			return mutateMemory(t, memfdCfg, func(m map[string]any) { m["hugepages"] = true })
		}},
		{name: "private memory", cfg: func() []byte {
			return mutateMemory(t, memfdCfg, func(m map[string]any) { m["shared"] = false })
		}},
		{name: "size differs from the mappings", cfg: func() []byte {
			return mutateMemory(t, memfdCfg, func(m map[string]any) { m["size"] = json.Number("536870912") })
		}},
		{name: "virtio-mem mapping", state: func() []byte {
			return mutateState(t, memfdState, func(mm map[string]any) { firstMapping(mm)["virtio_mem"] = true })
		}},
		{name: "other zone", state: func() []byte {
			return mutateState(t, memfdState, func(mm map[string]any) { firstMapping(mm)["zone_id"] = "mem1" })
		}},
		{name: "file offset mismatch", state: func() []byte {
			return mutateState(t, memfdState, func(mm map[string]any) { firstMapping(mm)["file_offset"] = json.Number("4096") })
		}},
		{name: "range without a mapping", state: func() []byte {
			return mutateState(t, memfdState, func(mm map[string]any) {
				r := mm["memory_ranges"].(map[string]any)
				r["data"] = append(r["data"].([]any), map[string]any{"gpa": json.Number("4294967296"), "length": json.Number("4096")})
			})
		}},
		{name: "no ranges", state: func() []byte {
			return mutateState(t, memfdState, func(mm map[string]any) {
				mm["memory_ranges"].(map[string]any)["data"] = []any{}
			})
		}},
		{name: "unknown mapping field", state: func() []byte {
			return mutateState(t, memfdState, func(mm map[string]any) { firstMapping(mm)["file"] = "/somewhere" })
		}},
		{name: "no memory manager", state: func() []byte {
			var top map[string]any
			if err := json.Unmarshal(memfdState, &top); err != nil {
				t.Fatal(err)
			}
			delete(top["snapshots"].(map[string]any), memoryManagerSnapshotID)
			b, _ := json.Marshal(top)
			return b
		}},
		{name: "file too small", slotSize: fixtureRAM - 4096},
		{name: "file is a directory", slotDir: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg, state := memfdCfg, memfdState
			if tc.cfg != nil {
				cfg = tc.cfg()
			}
			if tc.state != nil {
				state = tc.state()
			}
			for f, b := range map[string][]byte{"config.json": cfg, "state.json": state} {
				if err := os.WriteFile(filepath.Join(dir, f), b, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			size := int64(fixtureRAM)
			if tc.slotSize != 0 {
				size = tc.slotSize
			}
			slot := guestRAMFile(t, size)
			if tc.slotDir {
				slot = t.TempDir()
			}
			_, err := UseFileBackedMemory(dir, slot)
			if !errors.Is(err, ErrUnsupportedLayout) {
				t.Fatalf("UseFileBackedMemory = %v, want an ErrUnsupportedLayout", err)
			}
			gotCfg, gotState := readDir(t, dir)
			if !bytes.Equal(gotCfg, cfg) || !bytes.Equal(gotState, state) {
				t.Error("a rejected snapshot was modified")
			}
			assertNoTempFiles(t, dir)
		})
	}
}

// Two RAM regions (as on x86_64 above the 32-bit hole): accepted when
// memory-ranges lays them out at their file offsets, rejected otherwise, and
// the canonical table comes back in the original order.
func TestFileBackedTwoRegions(t *testing.T) {
	const low, high = 3 << 30, 1 << 30
	memfdCfg, memfdState := readFixture(t, "memfd-config.json"), readFixture(t, "memfd-state.json")
	cfg := mutateMemory(t, memfdCfg, func(m map[string]any) { m["size"] = json.Number("4294967296") })
	twoRegions := func(rangesInFileOrder bool) []byte {
		return mutateState(t, memfdState, func(mm map[string]any) {
			mm["guest_ram_mappings"] = []any{
				map[string]any{"slot": json.Number("0"), "gpa": json.Number("0"), "size": json.Number("3221225472"), "zone_id": "mem0", "virtio_mem": false, "file_offset": json.Number("0")},
				map[string]any{"slot": json.Number("1"), "gpa": json.Number("4294967296"), "size": json.Number("1073741824"), "zone_id": "mem0", "virtio_mem": false, "file_offset": json.Number("3221225472")},
			}
			ranges := []any{
				map[string]any{"gpa": json.Number("0"), "length": json.Number("3221225472")},
				map[string]any{"gpa": json.Number("4294967296"), "length": json.Number("1073741824")},
			}
			if !rangesInFileOrder {
				ranges[0], ranges[1] = ranges[1], ranges[0]
			}
			mm["memory_ranges"] = map[string]any{"data": ranges}
		})
	}
	write := func(state []byte) string {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "config.json"), cfg, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "state.json"), state, 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	dir := write(twoRegions(true))
	_, origState := readDir(t, dir)
	orig, err := UseFileBackedMemory(dir, guestRAMFile(t, low+high))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(mmField(t, readFileOrFatal(t, filepath.Join(dir, "state.json")), "memory_ranges")); got != `{"data":[]}` {
		t.Errorf("memory_ranges = %s, want empty", got)
	}
	if err := CanonicalizeFileBackedSnapshot(dir, orig); err != nil {
		t.Fatal(err)
	}
	if _, state := readDir(t, dir); !bytes.Equal(state, origState) {
		t.Error("two-region state.json not restored byte for byte")
	}

	// memory-ranges would hold the high region first, at offset 0, but the
	// zone file has it at 3 GiB.
	dir = write(twoRegions(false))
	if _, err := UseFileBackedMemory(dir, guestRAMFile(t, low+high)); !errors.Is(err, ErrUnsupportedLayout) {
		t.Errorf("UseFileBackedMemory with ranges out of file order = %v, want ErrUnsupportedLayout", err)
	}
}

func readFileOrFatal(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// u64 values above 2^53 survive both rewrites exactly: the fixture's
// arch_mem_regions holds u64::MAX, which a float64 round trip would turn into
// 18446744073709551616.
func TestFileBackedKeepsU64Precision(t *testing.T) {
	const u64max = "18446744073709551615"
	dir := snapshotDir(t, "memfd")
	if _, state := readDir(t, dir); !strings.Contains(string(state), u64max) {
		t.Fatal("fixture lost its u64::MAX")
	}
	orig, err := UseFileBackedMemory(dir, guestRAMFile(t, fixtureRAM))
	if err != nil {
		t.Fatal(err)
	}
	if _, state := readDir(t, dir); !strings.Contains(string(mmField(t, state, "arch_mem_regions")), u64max) {
		t.Error("u64::MAX changed by UseFileBackedMemory")
	}
	if err := CanonicalizeFileBackedSnapshot(dir, orig); err != nil {
		t.Fatal(err)
	}
	if _, state := readDir(t, dir); !strings.Contains(string(mmField(t, state, "arch_mem_regions")), u64max) {
		t.Error("u64::MAX changed by CanonicalizeFileBackedSnapshot")
	}

	// Values are compared by their exact text, not as float64s.
	if jsonEqual(json.RawMessage(`{"a":18446744073709551615}`), json.RawMessage(`{"a":18446744073709551614}`)) {
		t.Error("jsonEqual conflated two u64s that differ only below float64 precision")
	}
	if !jsonEqual(json.RawMessage(`{"a":1,"b":[2]}`), json.RawMessage(`{"b":[2],"a":1}`)) {
		t.Error("jsonEqual depends on key order")
	}
	if v, err := parseUint(json.RawMessage(u64max)); err != nil || v != 1<<64-1 {
		t.Errorf("parseUint(%s) = %d, %v", u64max, v, err)
	}
}

func TestFileBackedWritesAtomically(t *testing.T) {
	t.Run("replaces files, never writes through a shared inode", func(t *testing.T) {
		dir := snapshotDir(t, "memfd")
		origCfg, origState := readDir(t, dir)
		other := t.TempDir()
		for _, f := range []string{"config.json", "state.json"} {
			if err := os.Link(filepath.Join(dir, f), filepath.Join(other, f)); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Chmod(filepath.Join(dir, "config.json"), 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := UseFileBackedMemory(dir, guestRAMFile(t, fixtureRAM)); err != nil {
			t.Fatal(err)
		}
		linkCfg, linkState := readDir(t, other)
		if !bytes.Equal(linkCfg, origCfg) || !bytes.Equal(linkState, origState) {
			t.Error("a file sharing the snapshot's inode was modified")
		}
		if fi, err := os.Stat(filepath.Join(dir, "config.json")); err != nil || fi.Mode().Perm() != 0o640 {
			t.Errorf("config.json mode not kept (%v, %v)", fi.Mode(), err)
		}
		assertNoTempFiles(t, dir)
	})
	t.Run("a failed write leaves the snapshot as it was", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root ignores directory permissions")
		}
		dir := snapshotDir(t, "memfd")
		origCfg, origState := readDir(t, dir)
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(dir, 0o700)
		_, err := UseFileBackedMemory(dir, guestRAMFile(t, fixtureRAM))
		if err == nil || errors.Is(err, ErrUnsupportedLayout) {
			t.Fatalf("UseFileBackedMemory in a read-only dir = %v, want a write error", err)
		}
		cfg, state := readDir(t, dir)
		if !bytes.Equal(cfg, origCfg) || !bytes.Equal(state, origState) {
			t.Error("a failed rewrite modified the snapshot")
		}
		assertNoTempFiles(t, dir)
	})
}

func TestCanonicalizeFileBackedSnapshotRejects(t *testing.T) {
	memfdCfg := readFixture(t, "memfd-config.json")
	origMemory := memorySectionOf(t, memfdCfg)
	t.Run("original memory of another size", func(t *testing.T) {
		dir := snapshotDir(t, "filebacked")
		other := json.RawMessage(strings.Replace(string(origMemory), "402653184", "536870912", 1))
		if err := CanonicalizeFileBackedSnapshot(dir, other); err == nil {
			t.Error("accepted an original memory section of another size")
		}
	})
	t.Run("memfd config with another memory section", func(t *testing.T) {
		dir := snapshotDir(t, "memfd")
		other := json.RawMessage(strings.Replace(string(origMemory), `"thp":true`, `"thp":false`, 1))
		if err := CanonicalizeFileBackedSnapshot(dir, other); err == nil {
			t.Error("accepted a memfd snapshot whose memory section is not the original")
		}
	})
	t.Run("table matching neither form", func(t *testing.T) {
		dir := snapshotDir(t, "filebacked")
		_, state := readDir(t, dir)
		state = mutateState(t, state, func(mm map[string]any) {
			mm["memory_ranges"] = map[string]any{"data": []any{map[string]any{"gpa": json.Number("0"), "length": json.Number("4096")}}}
		})
		if err := os.WriteFile(filepath.Join(dir, "state.json"), state, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := CanonicalizeFileBackedSnapshot(dir, origMemory); err == nil {
			t.Error("accepted a memory_ranges table matching neither form")
		}
	})
}
