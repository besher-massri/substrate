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

// Opt-in zero-copy restore (envZeroCopyRestore). A warm restore overlays its
// golden delta onto a pre-staged golden slot on the tmpfs (see
// acquireGoldenSlot), so the slot already holds the guest's RAM image, laid
// out the way memory-ranges is. Instead of having vm.restore copy that image
// into a fresh memfd, ateom rewrites the snapshot so cloud-hypervisor maps the
// slot itself as guest RAM (ch.UseFileBackedMemory) and restores in Copy mode
// with nothing to copy. The slot leaves the pool with the actor:
//
//   - restore: claim the slot (the pool stages a replacement in the
//     background), put the restore directory back once vm.restore has read it,
//     and record the slot on the running actor.
//   - checkpoint: cloud-hypervisor writes no memory-ranges for RAM backed by a
//     named shared file, so ateom writes it from the slot (its delta against
//     the golden base, or a copy when the snapshot is not deflated) and puts
//     the snapshot back into the standard format
//     (ch.CanonicalizeFileBackedSnapshot): what atelet stores and ships
//     restores anywhere, with or without this knob.
//   - teardown: delete the slot once the VMM and virtiofsd are gone.
//
// A restore that fails after the rewrite deletes the slot (the guest may have
// written to it), and the actor's next attempt copies guest RAM as before.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
)

// Why a restore with zero-copy on still copied guest RAM ("Actor restore
// phases" zero_copy_fallback).
const (
	zeroCopyNoGoldenDelta   = "no_golden_delta"         // not a warm golden-delta restore
	zeroCopyMergedOnDisk    = "merged_on_disk"          // the image was rebuilt on the node disk, not in a slot
	zeroCopyDiskSlot        = "disk_slot"               // the slot is on the node disk, not the tmpfs
	zeroCopyNoIdleSlot      = "no_idle_slot"            // the pool was empty, the slot was staged for this restore
	zeroCopyPreviousFailure = "previous_attempt_failed" // this actor's last zero-copy attempt failed
	zeroCopyLayout          = "layout"                  // the snapshot's guest RAM layout cannot be file-backed
	zeroCopyRewriteFailed   = "rewrite_failed"          // the snapshot could not be rewritten (it was left as it was)
)

// zeroCopyBackoff holds the actors whose last zero-copy restore failed after
// the snapshot was rewritten: their next attempt copies guest RAM instead, so
// a failure peculiar to zero-copy cannot fail every retry.
var zeroCopyBackoff sync.Map // actor UID -> struct{}

// zeroCopyRestore is a restore whose guest RAM is a claimed golden slot.
type zeroCopyRestore struct {
	restoreDir string
	// slot is the slot file (guest RAM); slotDev and slotIno identify it.
	slot             string
	slotDev, slotIno uint64
	// slotBase is the pool's base file the slot was staged from.
	slotBase string
	// origMemory is the snapshot's config.json memory section before the
	// rewrite, which every checkpoint of the actor puts back.
	origMemory json.RawMessage
	// reverted says restoreDir is in the standard format again.
	reverted bool
}

// startZeroCopyRestore claims lease's slot and rewrites restoreDir so that
// vm.restore maps the slot as guest RAM. When it does not, it says why
// (fallback) and the restore copies guest RAM as before. An error means
// restoreDir could not be left in a state any restore can use.
func startZeroCopyRestore(ctx context.Context, actorUID, restoreDir string, lease *goldenSlotLease) (zc *zeroCopyRestore, fallback string, err error) {
	switch {
	case lease == nil:
		return nil, zeroCopyNoGoldenDelta, nil
	case lease.slot == nil:
		return nil, zeroCopyMergedOnDisk, nil
	case !lease.pool.inTmpfs:
		return nil, zeroCopyDiskSlot, nil
	case !lease.fromIdle:
		// Never hold a restore back to stage a slot it can keep.
		return nil, zeroCopyNoIdleSlot, nil
	}
	if _, failed := zeroCopyBackoff.LoadAndDelete(actorUID); failed {
		return nil, zeroCopyPreviousFailure, nil
	}
	slot := lease.slot.path
	var st unix.Stat_t
	if err := unix.Stat(slot, &st); err != nil {
		return nil, zeroCopyRewriteFailed, nil
	}
	orig, err := ch.UseFileBackedMemory(restoreDir, slot)
	if err != nil {
		if errors.Is(err, ch.ErrUnsupportedLayout) {
			slog.InfoContext(ctx, "Snapshot guest memory cannot be mapped from the golden slot; copying it",
				slog.String("id", actorUID), slog.Any("reason", err))
			return nil, zeroCopyLayout, nil
		}
		// UseFileBackedMemory puts the snapshot back when it fails; check
		// that it is a standard one before restoring it the usual way.
		if cerr := ch.CanonicalizeFileBackedSnapshot(restoreDir, nil); cerr != nil {
			return nil, "", fmt.Errorf("while mapping the golden slot as guest RAM: %w (and the snapshot is not restorable as it is: %v)", err, cerr)
		}
		slog.WarnContext(ctx, "Could not map the golden slot as guest RAM; copying it",
			slog.String("id", actorUID), slog.Any("err", err))
		return nil, zeroCopyRewriteFailed, nil
	}
	if lease.claimForVM() == "" {
		// Unreachable (the lease has a slot and is not disposed of yet), but
		// never let the VM map a slot the pool may hand out again.
		if err := ch.CanonicalizeFileBackedSnapshot(restoreDir, orig); err != nil {
			return nil, "", fmt.Errorf("while putting the snapshot back: %w", err)
		}
		return nil, zeroCopyRewriteFailed, nil
	}
	return &zeroCopyRestore{
		restoreDir: restoreDir,
		slot:       slot,
		slotDev:    uint64(st.Dev),
		slotIno:    st.Ino,
		slotBase:   lease.pool.baseFile,
		origMemory: orig,
	}, "", nil
}

// revert puts restoreDir back into the standard format (cloud-hypervisor has
// read it by then, or never will).
func (z *zeroCopyRestore) revert() error {
	if z.reverted {
		return nil
	}
	if err := ch.CanonicalizeFileBackedSnapshot(z.restoreDir, z.origMemory); err != nil {
		return err
	}
	z.reverted = true
	return nil
}

// abandon undoes a zero-copy restore that failed after the rewrite, once the
// VMM and virtiofsd that may map the slot are gone: restoreDir goes back to
// the standard format so a retry can use it, the slot is deleted (the guest
// may have written to it), and the actor's next attempt copies guest RAM.
func (z *zeroCopyRestore) abandon(ctx context.Context, actorUID string) {
	if err := z.revert(); err != nil {
		slog.ErrorContext(ctx, "Could not put the restore directory back after a failed zero-copy restore",
			slog.String("id", actorUID), slog.String("dir", z.restoreDir), slog.Any("err", err))
	}
	removeMemSlot(ctx, actorUID, z.slot)
	zeroCopyBackoff.Store(actorUID, struct{}{})
}

// removeMemSlot deletes a golden slot that held an actor's guest RAM.
func removeMemSlot(ctx context.Context, actorUID, slot string) {
	if err := os.Remove(slot); err != nil && !os.IsNotExist(err) {
		slog.WarnContext(ctx, "Could not delete the golden slot that held the actor's guest RAM",
			slog.String("id", actorUID), slog.String("slot", slot), slog.Any("err", err))
	}
}

// discardMemSlot deletes the golden slot holding ra's guest RAM, if any. Call
// it only once the actor's VMM and virtiofsd have exited.
func (s *AteomService) discardMemSlot(ctx context.Context, actorUID string, ra *runningActor) {
	s.actorsMu.Lock()
	slot := ra.memSlot
	ra.memSlot = ""
	s.actorsMu.Unlock()
	if slot != "" {
		removeMemSlot(ctx, actorUID, slot)
	}
}

// checkSlotSnapshot checks a snapshot of a VM whose guest RAM is the golden
// slot: cloud-hypervisor must have written no memory-ranges (it never copies
// RAM backed by a named shared file), and the slot must still be the file
// the VM maps, or what it holds is not this guest's RAM.
func checkSlotSnapshot(checkpointDir string, ra *runningActor) error {
	if _, err := os.Lstat(filepath.Join(checkpointDir, "memory-ranges")); err == nil {
		return fmt.Errorf("cloud-hypervisor wrote memory-ranges for a guest whose RAM is the golden slot %s", ra.memSlot)
	} else if !os.IsNotExist(err) {
		return err
	}
	var st unix.Stat_t
	if err := unix.Stat(ra.memSlot, &st); err != nil {
		return fmt.Errorf("while checking the golden slot holding guest RAM: %w", err)
	}
	if uint64(st.Dev) != ra.memSlotDev || st.Ino != ra.memSlotIno {
		return fmt.Errorf("golden slot %s is no longer the file the guest's RAM is mapped from", ra.memSlot)
	}
	return nil
}

// copySlotMemory writes memory-ranges as a sparse copy of the slot (what
// cloud-hypervisor would have dumped), for a checkpoint that is not deflated
// against a golden base.
func copySlotMemory(ctx context.Context, actorUID string, ra *runningActor, checkpointMem string) error {
	t := time.Now()
	if err := ch.CopySparseFileFast(ctx, ra.memSlot, checkpointMem); err != nil {
		_ = os.Remove(checkpointMem)
		return fmt.Errorf("while copying guest memory from the golden slot: %w", err)
	}
	slog.InfoContext(ctx, "Copied guest memory from the golden slot", slog.String("id", actorUID),
		slog.String("checkpoint_mode", "slot_copy"), slog.Duration("checkpoint_delta_from_slot", time.Since(t)))
	return nil
}
