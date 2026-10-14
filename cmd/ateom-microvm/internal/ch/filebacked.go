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

// File-backed guest RAM, for a restore that copies nothing.
//
// A snapshot of a VM whose guest RAM is a memfd (config.json memory.size != 0,
// no zones) carries that RAM in memory-ranges, and a Copy-mode vm.restore
// copies all of it into a fresh memfd. When a file already holds the RAM
// image in the guest's own layout (memory-ranges' layout for one RAM region
// at GPA 0, which is what a golden slot holds), the restored guest can map
// that file instead:
//
//   - UseFileBackedMemory rewrites the snapshot so its memory is one zone
//     backed by the file (memory.size = 0, memory.zones = [{"mem0", file}])
//     and empties the memory-manager's memory_ranges table. cloud-hypervisor
//     then maps each guest RAM mapping onto the file at its file_offset, and
//     Copy mode copies nothing (fill_saved_regions returns at once on an empty
//     table). The guest writes straight into the file from then on.
//   - A VM restored that way snapshots no RAM: cloud-hypervisor skips regions
//     backed by a named, MAP_SHARED file (it assumes the user keeps the file)
//     and writes no memory-ranges when nothing is left. The caller builds
//     memory-ranges from the file itself, then calls
//     CanonicalizeFileBackedSnapshot, which puts the original memory sections
//     back. The result is the standard memfd snapshot format, so any restore
//     path, with or without this feature, can use it.
//
// Only the memory sections change: config.json's "memory" object and the
// memory_ranges field inside the memory-manager's (JSON-encoded) state in
// state.json. Every other byte is passed through as it was, and both files
// are replaced atomically (temp file + rename), never written in place.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	snapshotConfigFile = "config.json"
	snapshotStateFile  = "state.json"
	// memoryManagerSnapshotID is cloud-hypervisor's MEMORY_MANAGER_SNAPSHOT_ID,
	// the memory manager's key in the VM snapshot tree.
	memoryManagerSnapshotID = "memory-manager"
	// defaultMemoryZone is cloud-hypervisor's DEFAULT_MEMORY_ZONE: the id of the
	// one zone it builds from memory.size when no zones are given.
	defaultMemoryZone = "mem0"
)

// ErrUnsupportedLayout is wrapped by UseFileBackedMemory errors about the
// snapshot's guest-RAM layout (more than one zone, virtio-mem, hotplug, a
// layout that is not memory-ranges' own, a file that is too small, ...).
// The snapshot was left untouched: restore it the usual way.
var ErrUnsupportedLayout = errors.New("snapshot guest memory cannot be file-backed")

func layoutError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnsupportedLayout, fmt.Sprintf(format, args...))
}

// UseFileBackedMemory rewrites the memfd snapshot in snapshotDir so that
// vm.restore maps guest RAM from file (which must hold the RAM image laid out
// as the snapshot's memory-ranges is) instead of copying memory-ranges into
// a memfd. Restore it with the Copy memory mode (never OnDemand); memory-ranges
// is then not read at all. It returns the original config.json "memory"
// section, which CanonicalizeFileBackedSnapshot puts back later.
//
// It proceeds only when guest RAM is the single default zone with no
// virtio-mem and no hotplug, every guest RAM mapping has its memory_ranges
// entry, each entry's offset in memory-ranges equals its mapping's
// file_offset, and file covers every mapping. Otherwise it returns an error
// wrapping ErrUnsupportedLayout and changes nothing. On any other error the
// snapshot is put back as it was, unless the error says otherwise.
func UseFileBackedMemory(snapshotDir, file string) (origMemory json.RawMessage, err error) {
	cfgPath := filepath.Join(snapshotDir, snapshotConfigFile)
	statePath := filepath.Join(snapshotDir, snapshotStateFile)
	cfgBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		return nil, err
	}

	cfg, err := parseObject(cfgBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", cfgPath, err)
	}
	memRaw, ok := cfg.get("memory")
	if !ok {
		return nil, layoutError("config.json has no memory section")
	}
	mem, err := parseObject(memRaw)
	if err != nil {
		return nil, layoutError("config.json memory section: %v", err)
	}
	memSize, err := memfdMemorySize(mem)
	if err != nil {
		return nil, err
	}

	state, err := parseMemoryManagerState(stateBytes)
	if err != nil {
		return nil, err
	}
	mappings, err := state.mappings()
	if err != nil {
		return nil, layoutError("%v", err)
	}
	ranges, err := state.ranges()
	if err != nil {
		return nil, layoutError("%v", err)
	}
	end, err := validateMemfdLayout(mappings, ranges, memSize)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(file)
	if err != nil {
		return nil, fmt.Errorf("stat guest memory file: %w", err)
	}
	if !fi.Mode().IsRegular() {
		return nil, layoutError("guest memory file %q is not a regular file", file)
	}
	if uint64(fi.Size()) < end {
		return nil, layoutError("guest memory file %q holds %d bytes, guest RAM needs %d", file, fi.Size(), end)
	}

	// config.json: one zone backed by file, of the old size.
	zone, err := fileBackedZone(mem, file)
	if err != nil {
		return nil, err
	}
	newMem := mem.clone()
	newMem.set("size", json.RawMessage("0"))
	newMem.setBefore("zones", zone, "thp")
	newMemRaw, err := newMem.marshal()
	if err != nil {
		return nil, err
	}
	cfg.set("memory", newMemRaw)
	newCfg, err := cfg.marshal()
	if err != nil {
		return nil, err
	}
	// state.json: nothing to copy at restore.
	newState, err := state.withRanges(nil)
	if err != nil {
		return nil, err
	}

	// config.json first: should the second write fail, a file-backed config
	// with the full memory_ranges table still restores the right RAM (it
	// copies the file onto itself). The reverse order could leave a memfd
	// config with an empty table, i.e. a guest with zeroed RAM.
	if err := writeFileAtomic(cfgPath, newCfg); err != nil {
		return nil, err
	}
	if err := writeFileAtomic(statePath, newState); err != nil {
		if rerr := writeFileAtomic(cfgPath, cfgBytes); rerr != nil {
			return nil, fmt.Errorf("writing %s: %w (and putting config.json back failed: %v; it still maps %q)", statePath, err, rerr, file)
		}
		return nil, fmt.Errorf("writing %s: %w", statePath, err)
	}
	return append(json.RawMessage(nil), memRaw...), nil
}

// CanonicalizeFileBackedSnapshot turns a snapshot of a VM whose guest RAM was
// file-backed by UseFileBackedMemory back into the standard memfd format: it
// puts origMemory (the config.json "memory" section UseFileBackedMemory
// returned) back into config.json, and sets the memory-manager's
// memory_ranges table to one {gpa, length} entry per guest RAM mapping, in
// file_offset order, which is what cloud-hypervisor writes for memfd-backed
// RAM. The caller provides the matching memory-ranges file (for example the
// guest RAM file itself, or a delta of it against a golden base).
//
// A nil origMemory is derived from the zone instead (its size, without the
// zones). It is also safe on a snapshot that is already canonical, e.g. to
// put back a restore directory that UseFileBackedMemory rewrote.
func CanonicalizeFileBackedSnapshot(dir string, origMemory json.RawMessage) error {
	cfgPath := filepath.Join(dir, snapshotConfigFile)
	statePath := filepath.Join(dir, snapshotStateFile)
	cfgBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		return err
	}
	cfg, err := parseObject(cfgBytes)
	if err != nil {
		return fmt.Errorf("parsing %s: %w", cfgPath, err)
	}
	memRaw, ok := cfg.get("memory")
	if !ok {
		return fmt.Errorf("%s has no memory section", cfgPath)
	}
	mem, err := parseObject(memRaw)
	if err != nil {
		return fmt.Errorf("%s memory section: %w", cfgPath, err)
	}

	var canonicalMem json.RawMessage
	if zoneSize, fileBacked, err := fileBackedZoneSize(mem); err != nil {
		return err
	} else if fileBacked {
		if canonicalMem, err = canonicalMemorySection(origMemory, mem, zoneSize); err != nil {
			return err
		}
	} else {
		// Already the memfd form: leave config.json alone, but it must be the
		// section the caller expects.
		if origMemory != nil && !jsonEqual(memRaw, origMemory) {
			return fmt.Errorf("%s memory section is neither file-backed nor the original one", cfgPath)
		}
		if _, err := memfdMemorySize(mem); err != nil {
			return err
		}
	}

	state, err := parseMemoryManagerState(stateBytes)
	if err != nil {
		return err
	}
	mappings, err := state.mappings()
	if err != nil {
		return err
	}
	ranges, err := state.ranges()
	if err != nil {
		return err
	}
	want, err := memfdRanges(mappings)
	if err != nil {
		return err
	}
	var newState []byte
	switch {
	case len(ranges) == 0:
		if newState, err = state.withRanges(want); err != nil {
			return err
		}
	case !equalRanges(ranges, want):
		return fmt.Errorf("%s memory_ranges %v match neither a file-backed snapshot (empty) nor its guest RAM mappings %v", statePath, ranges, want)
	}

	// state.json first, for the reason UseFileBackedMemory writes config.json
	// first: never a memfd config with an empty memory_ranges table.
	if newState != nil {
		if err := writeFileAtomic(statePath, newState); err != nil {
			return err
		}
	}
	if canonicalMem != nil {
		cfg.set("memory", canonicalMem)
		newCfg, err := cfg.marshal()
		if err != nil {
			return err
		}
		if err := writeFileAtomic(cfgPath, newCfg); err != nil {
			return err
		}
	}
	return nil
}

// memfdMemorySize checks that a config.json memory section describes the
// memfd-backed default zone and nothing else, and returns its size.
func memfdMemorySize(mem jsonObject) (uint64, error) {
	sizeRaw, ok := mem.get("size")
	if !ok {
		return 0, layoutError("memory.size is missing")
	}
	size, err := parseUint(sizeRaw)
	if err != nil {
		return 0, layoutError("memory.size: %v", err)
	}
	if size == 0 {
		return 0, layoutError("memory.size is 0 (guest RAM is already in user-defined zones)")
	}
	for _, key := range []string{"zones", "hotplug_size", "hotplugged_size", "hugepage_size"} {
		if v, ok := mem.get(key); ok && !isNull(v) {
			return 0, layoutError("memory.%s is set (%s)", key, v)
		}
	}
	if b, err := optBool(mem, "shared"); err != nil || !b {
		return 0, layoutError("memory.shared is not true")
	}
	if b, err := optBool(mem, "hugepages"); err != nil || b {
		return 0, layoutError("memory.hugepages is set")
	}
	return size, nil
}

// fileBackedZone is the zone UseFileBackedMemory gives a memfd memory
// section: the default zone, its size, backed by file and shared, with the
// section's reserve and mergeable settings. Fields cloud-hypervisor defaults
// to none or false when absent are left out.
func fileBackedZone(mem jsonObject, file string) (json.RawMessage, error) {
	fileRaw, err := marshalNoEscape(file)
	if err != nil {
		return nil, err
	}
	sizeRaw, _ := mem.get("size")
	zone := jsonObject{
		{"id", json.RawMessage(`"` + defaultMemoryZone + `"`)},
		{"size", sizeRaw},
		{"file", fileRaw},
		{"shared", json.RawMessage("true")},
		{"hugepages", json.RawMessage("false")},
		{"prefault", json.RawMessage("false")},
	}
	for _, key := range []string{"reserve", "mergeable"} {
		b, err := optBool(mem, key)
		if err != nil {
			return nil, layoutError("memory.%s: %v", key, err)
		}
		zone = append(zone, jsonField{key, json.RawMessage(strconv.FormatBool(b))})
	}
	z, err := zone.marshal()
	if err != nil {
		return nil, err
	}
	return json.RawMessage("[" + string(z) + "]"), nil
}

// fileBackedZoneSize reports whether a config.json memory section is the
// file-backed form UseFileBackedMemory writes (size 0, the default zone
// alone, backed by a file), and that zone's size.
func fileBackedZoneSize(mem jsonObject) (size uint64, fileBacked bool, err error) {
	sizeRaw, ok := mem.get("size")
	if !ok {
		return 0, false, fmt.Errorf("memory.size is missing")
	}
	if s, err := parseUint(sizeRaw); err != nil {
		return 0, false, fmt.Errorf("memory.size: %w", err)
	} else if s != 0 {
		return 0, false, nil
	}
	zonesRaw, ok := mem.get("zones")
	if !ok || isNull(zonesRaw) {
		return 0, false, fmt.Errorf("memory.size is 0 but there are no zones")
	}
	var zones []struct {
		ID   string  `json:"id"`
		Size uint64  `json:"size"`
		File *string `json:"file"`
	}
	if err := json.Unmarshal(zonesRaw, &zones); err != nil {
		return 0, false, fmt.Errorf("memory.zones: %w", err)
	}
	if len(zones) != 1 || zones[0].ID != defaultMemoryZone || zones[0].File == nil || *zones[0].File == "" || zones[0].Size == 0 {
		return 0, false, fmt.Errorf("memory.zones is not one file-backed %q zone: %s", defaultMemoryZone, zonesRaw)
	}
	return zones[0].Size, true, nil
}

// canonicalMemorySection is the memfd memory section to put back: origMemory
// when given (checked against the zone's size), else the file-backed section
// with the zone's size and no zones.
func canonicalMemorySection(origMemory json.RawMessage, fileBacked jsonObject, zoneSize uint64) (json.RawMessage, error) {
	if origMemory == nil {
		m := fileBacked.clone()
		m.set("size", json.RawMessage(strconv.FormatUint(zoneSize, 10)))
		m.remove("zones")
		return m.marshal()
	}
	orig, err := parseObject(origMemory)
	if err != nil {
		return nil, fmt.Errorf("original memory section: %w", err)
	}
	size, err := memfdMemorySize(orig)
	if err != nil {
		return nil, fmt.Errorf("original memory section: %w", err)
	}
	if size != zoneSize {
		return nil, fmt.Errorf("original memory size %d differs from the file-backed zone's %d", size, zoneSize)
	}
	return append(json.RawMessage(nil), origMemory...), nil
}

// validateMemfdLayout checks that ranges (a memfd snapshot's memory_ranges)
// lay guest RAM out in memory-ranges exactly as mappings lay it out in a zone
// file, and returns the end of the last mapping in that file.
func validateMemfdLayout(mappings []guestRAMMapping, ranges []memoryRange, memSize uint64) (uint64, error) {
	if len(mappings) == 0 {
		return 0, layoutError("no guest RAM mappings")
	}
	var total, end uint64
	for _, m := range mappings {
		if m.ZoneID != defaultMemoryZone {
			return 0, layoutError("guest RAM mapping in zone %q", m.ZoneID)
		}
		if m.VirtioMem {
			return 0, layoutError("virtio-mem guest RAM mapping at gpa %#x", m.GPA)
		}
		if m.Size == 0 {
			return 0, layoutError("empty guest RAM mapping at gpa %#x", m.GPA)
		}
		if total+m.Size < total || m.FileOffset+m.Size < m.FileOffset {
			return 0, layoutError("guest RAM mapping at gpa %#x overflows", m.GPA)
		}
		total += m.Size
		end = max(end, m.FileOffset+m.Size)
	}
	if total != memSize {
		return 0, layoutError("guest RAM mappings total %d bytes, memory.size is %d", total, memSize)
	}
	if len(ranges) != len(mappings) {
		return 0, layoutError("%d memory_ranges entries for %d guest RAM mappings", len(ranges), len(mappings))
	}
	used := make([]bool, len(mappings))
	var cursor uint64
	for _, r := range ranges {
		i := -1
		for j, m := range mappings {
			if !used[j] && m.GPA == r.GPA && m.Size == r.Length {
				i = j
				break
			}
		}
		if i < 0 {
			return 0, layoutError("memory_ranges entry {gpa %#x, length %d} matches no guest RAM mapping", r.GPA, r.Length)
		}
		used[i] = true
		if mappings[i].FileOffset != cursor {
			return 0, layoutError("guest RAM mapping at gpa %#x has file_offset %d, but memory-ranges holds it at %d", r.GPA, mappings[i].FileOffset, cursor)
		}
		cursor += r.Length
	}
	return end, nil
}

// memfdRanges is the memory_ranges table cloud-hypervisor writes for
// memfd-backed RAM laid out as mappings are: one entry per mapping, in
// file_offset order, contiguous from offset 0.
func memfdRanges(mappings []guestRAMMapping) ([]memoryRange, error) {
	ms := append([]guestRAMMapping(nil), mappings...)
	sort.SliceStable(ms, func(i, j int) bool { return ms[i].FileOffset < ms[j].FileOffset })
	var out []memoryRange
	var cursor uint64
	for _, m := range ms {
		if m.VirtioMem {
			return nil, fmt.Errorf("virtio-mem guest RAM mapping at gpa %#x", m.GPA)
		}
		if m.ZoneID != defaultMemoryZone {
			return nil, fmt.Errorf("guest RAM mapping in zone %q", m.ZoneID)
		}
		if m.FileOffset != cursor {
			return nil, fmt.Errorf("guest RAM mapping at gpa %#x has file_offset %d, want %d", m.GPA, m.FileOffset, cursor)
		}
		out = append(out, memoryRange{GPA: m.GPA, Length: m.Size})
		cursor += m.Size
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no guest RAM mappings")
	}
	return out, nil
}

func equalRanges(a, b []memoryRange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// guestRAMMapping is cloud-hypervisor's GuestRamMapping.
type guestRAMMapping struct {
	Slot       uint32 `json:"slot"`
	GPA        uint64 `json:"gpa"`
	Size       uint64 `json:"size"`
	ZoneID     string `json:"zone_id"`
	VirtioMem  bool   `json:"virtio_mem"`
	FileOffset uint64 `json:"file_offset"`
}

// memoryRange is cloud-hypervisor's MemoryRange.
type memoryRange struct {
	GPA    uint64 `json:"gpa"`
	Length uint64 `json:"length"`
}

// memoryManagerState is a state.json, opened down to the memory manager's
// state (a JSON string inside it).
type memoryManagerState struct {
	top, snapshots, mm, data, state jsonObject
}

func parseMemoryManagerState(b []byte) (*memoryManagerState, error) {
	var s memoryManagerState
	var err error
	if s.top, err = parseObject(b); err != nil {
		return nil, layoutError("state.json: %v", err)
	}
	if s.snapshots, err = s.top.object("snapshots"); err != nil {
		return nil, layoutError("state.json: %v", err)
	}
	if s.mm, err = s.snapshots.object(memoryManagerSnapshotID); err != nil {
		return nil, layoutError("state.json: %v", err)
	}
	if s.data, err = s.mm.object("snapshot_data"); err != nil {
		return nil, layoutError("state.json %s: %v", memoryManagerSnapshotID, err)
	}
	stateRaw, ok := s.data.get("state")
	if !ok {
		return nil, layoutError("state.json %s has no state", memoryManagerSnapshotID)
	}
	var stateStr string
	if err := json.Unmarshal(stateRaw, &stateStr); err != nil {
		return nil, layoutError("state.json %s state: %v", memoryManagerSnapshotID, err)
	}
	if s.state, err = parseObject([]byte(stateStr)); err != nil {
		return nil, layoutError("state.json %s state: %v", memoryManagerSnapshotID, err)
	}
	return &s, nil
}

func (s *memoryManagerState) mappings() ([]guestRAMMapping, error) {
	raw, ok := s.state.get("guest_ram_mappings")
	if !ok {
		return nil, fmt.Errorf("memory manager state has no guest_ram_mappings")
	}
	var ms []guestRAMMapping
	if err := decodeStrict(raw, &ms); err != nil {
		return nil, fmt.Errorf("guest_ram_mappings: %w", err)
	}
	return ms, nil
}

func (s *memoryManagerState) ranges() ([]memoryRange, error) {
	raw, ok := s.state.get("memory_ranges")
	if !ok {
		return nil, fmt.Errorf("memory manager state has no memory_ranges")
	}
	var t struct {
		Data []memoryRange `json:"data"`
	}
	if err := decodeStrict(raw, &t); err != nil {
		return nil, fmt.Errorf("memory_ranges: %w", err)
	}
	return t.Data, nil
}

// withRanges returns the state.json with the memory manager's memory_ranges
// set to ranges, and every other byte as it was. The table is written the way
// cloud-hypervisor writes it.
func (s *memoryManagerState) withRanges(ranges []memoryRange) ([]byte, error) {
	var t bytes.Buffer
	t.WriteString(`{"data":[`)
	for i, r := range ranges {
		if i > 0 {
			t.WriteByte(',')
		}
		fmt.Fprintf(&t, `{"gpa":%d,"length":%d}`, r.GPA, r.Length)
	}
	t.WriteString(`]}`)
	state := s.state.clone()
	state.set("memory_ranges", t.Bytes())
	stateJSON, err := state.marshal()
	if err != nil {
		return nil, err
	}
	stateStr, err := marshalNoEscape(string(stateJSON))
	if err != nil {
		return nil, err
	}
	data := s.data.clone()
	data.set("state", stateStr)
	dataRaw, err := data.marshal()
	if err != nil {
		return nil, err
	}
	mm := s.mm.clone()
	mm.set("snapshot_data", dataRaw)
	mmRaw, err := mm.marshal()
	if err != nil {
		return nil, err
	}
	snaps := s.snapshots.clone()
	snaps.set(memoryManagerSnapshotID, mmRaw)
	snapsRaw, err := snaps.marshal()
	if err != nil {
		return nil, err
	}
	top := s.top.clone()
	top.set("snapshots", snapsRaw)
	return top.marshal()
}

// jsonObject is a JSON object as its fields in order, each value kept as its
// exact bytes, so re-encoding it changes nothing that was not set.
type jsonObject []jsonField

type jsonField struct {
	key string
	val json.RawMessage
}

// parseObject parses one JSON object (and nothing after it).
func parseObject(b []byte) (jsonObject, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("not a JSON object")
	}
	obj := jsonObject{}
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected token %v", tok)
		}
		if seen[key] {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		seen[key] = true
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, err
		}
		obj = append(obj, jsonField{key, val})
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("data after the JSON object")
	}
	return obj, nil
}

func (o jsonObject) get(key string) (json.RawMessage, bool) {
	for _, f := range o {
		if f.key == key {
			return f.val, true
		}
	}
	return nil, false
}

// object returns the non-null object at key.
func (o jsonObject) object(key string) (jsonObject, error) {
	raw, ok := o.get(key)
	if !ok || isNull(raw) {
		return nil, fmt.Errorf("no %q object", key)
	}
	v, err := parseObject(raw)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", key, err)
	}
	return v, nil
}

func (o jsonObject) clone() jsonObject { return append(jsonObject(nil), o...) }

// set replaces key's value in place, or appends key.
func (o *jsonObject) set(key string, val json.RawMessage) {
	for i := range *o {
		if (*o)[i].key == key {
			(*o)[i].val = val
			return
		}
	}
	*o = append(*o, jsonField{key, val})
}

// setBefore replaces key's value in place, or inserts key before the field
// before (appending it when there is no such field).
func (o *jsonObject) setBefore(key string, val json.RawMessage, before string) {
	if _, ok := o.get(key); ok {
		o.set(key, val)
		return
	}
	for i := range *o {
		if (*o)[i].key == before {
			*o = append((*o)[:i], append(jsonObject{{key, val}}, (*o)[i:]...)...)
			return
		}
	}
	*o = append(*o, jsonField{key, val})
}

func (o *jsonObject) remove(key string) {
	for i := range *o {
		if (*o)[i].key == key {
			*o = append((*o)[:i], (*o)[i+1:]...)
			return
		}
	}
}

// marshal encodes the object compactly, values as they are.
func (o jsonObject) marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, f := range o {
		if i > 0 {
			buf.WriteByte(',')
		}
		k, err := marshalNoEscape(f.key)
		if err != nil {
			return nil, err
		}
		buf.Write(k)
		buf.WriteByte(':')
		buf.Write(f.val)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// marshalNoEscape is json.Marshal without HTML escaping (cloud-hypervisor does
// not escape <, > or &, so neither do we).
func marshalNoEscape(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// decodeStrict decodes raw into v exactly: integers into integer fields
// without a float detour, and no fields v does not know.
func decodeStrict(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func isNull(raw json.RawMessage) bool { return string(bytes.TrimSpace(raw)) == "null" }

func parseUint(raw json.RawMessage) (uint64, error) {
	return strconv.ParseUint(string(bytes.TrimSpace(raw)), 10, 64)
}

// optBool reads an optional boolean field (absent or null is false).
func optBool(o jsonObject, key string) (bool, error) {
	raw, ok := o.get(key)
	if !ok || isNull(raw) {
		return false, nil
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return false, err
	}
	return b, nil
}

// jsonEqual reports whether two JSON documents are equal, numbers compared by
// their exact text.
func jsonEqual(a, b json.RawMessage) bool {
	var va, vb any
	da := json.NewDecoder(bytes.NewReader(a))
	da.UseNumber()
	db := json.NewDecoder(bytes.NewReader(b))
	db.UseNumber()
	if da.Decode(&va) != nil || db.Decode(&vb) != nil {
		return false
	}
	ja, errA := json.Marshal(va)
	jb, errB := json.Marshal(vb)
	return errA == nil && errB == nil && bytes.Equal(ja, jb)
}

// writeFileAtomic replaces path with data through a temp file in the same
// directory and a rename, keeping path's mode. Never O_TRUNC: path may share
// its inode with another snapshot.
func writeFileAtomic(path string, data []byte) error {
	mode := os.FileMode(0o600)
	if fi, err := os.Stat(path); err == nil {
		mode = fi.Mode().Perm()
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op once the rename succeeds
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
