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

// The paths ateworker-microvm derives from the ActorDirs atelet sends. The
// directory under root_dir is ateworker-microvm's own: atelet does not know it.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

const (
	// goldenDeltaMarkerFile marks a checkpoint directory whose memory-ranges
	// file has been deflated against the node-cached golden base image.
	goldenDeltaMarkerFile = "golden-delta"

	// memoryRangesDiffMetaFile and memoryRangesDiffDataFile hold compact ADIF
	// v1 differential snapshot metadata and payload when compact delta format
	// is enabled.
	memoryRangesDiffMetaFile = "memory-ranges.diff-meta"
	memoryRangesDiffDataFile = "memory-ranges.diff-data"
)

// microvmEnv reads ATEWORKER_MICROVM_<suffix>, falling back to the legacy
// ATEOM_MICROVM_<suffix> environment variable for backward compatibility.
func microvmEnv(suffix string) string {
	if v := strings.TrimSpace(os.Getenv("ATEWORKER_MICROVM_" + suffix)); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("ATEOM_MICROVM_" + suffix))
}

func microvmEnvBool(suffix string) bool {
	switch strings.ToLower(microvmEnv(suffix)) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

func diffSnapshotEnabled() bool {
	return microvmEnvBool("DIFF_SNAPSHOT")
}

func compactDeltaFormatEnabled() bool {
	return strings.EqualFold(microvmEnv("DELTA_FORMAT"), "compact")
}

// ociBundlePath is the container's OCI bundle.
func ociBundlePath(actorDirs *ateworkerpb.ActorDirs, containerName string) string {
	return filepath.Join(actorDirs.GetOciBundleDir(), containerName)
}

// rootfsUpperDir is the host directory backing the actor's rootfs overlay
// uppers: one subdirectory per container (see kata.UpperWorkDirs). Local to
// this binary: atelet never touches it, so it is not one of the ActorDirs.
func rootfsUpperDir(actorDirs *ateworkerpb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "rootfs-upper")
}

// deltaRestoreStagingDir is a per-actor directory used to stage reconstructed
// differential snapshots when the source restore_dir is preserved.
func deltaRestoreStagingDir(actorUID string) string {
	return filepath.Join(nodepath.ActorsDir, actorUID, "delta-restore-staging")
}

// goldenBaseMemoryRangesPath returns the node-local path where the frozen
// golden base memory-ranges file for baseID is cached.
func goldenBaseMemoryRangesPath(baseID string) string {
	ateRoot := filepath.Dir(filepath.Clean(nodepath.ActorsDir))
	return filepath.Join(ateRoot, "golden-base-cache", baseID, "memory-ranges")
}

// goldenBaseMemoryRangesPathForDir resolves the golden base memory-ranges path
// relative to snapshotDir when snapshotDir sits under <ateRoot>/actors/<uid>/...,
// falling back to goldenBaseMemoryRangesPath(baseID).
func goldenBaseMemoryRangesPathForDir(snapshotDir, baseID string) string {
	clean := filepath.Clean(snapshotDir)
	actorDir := filepath.Dir(clean)
	actorsDir := filepath.Dir(actorDir)
	if filepath.Base(actorsDir) == "actors" {
		ateRoot := filepath.Dir(actorsDir)
		candidate := filepath.Join(ateRoot, "golden-base-cache", baseID, "memory-ranges")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return goldenBaseMemoryRangesPath(baseID)
}

// ensureGoldenBaseCached atomically populates goldenBaseMemoryRangesPath(baseID)
// from srcMemPath if it is not already present on this node.
func ensureGoldenBaseCached(ctx context.Context, baseID, srcMemPath string) error {
	if baseID == "" || srcMemPath == "" {
		return nil
	}
	dstPath := goldenBaseMemoryRangesPath(baseID)
	if fi, err := os.Stat(dstPath); err == nil && fi.Mode().IsRegular() && fi.Size() > 0 {
		if tmpfsSlotsEnabled() {
			maybePrewarmGoldenSlots(dstPath)
		}
		return nil
	}
	srcFi, err := os.Stat(srcMemPath)
	if err != nil || !srcFi.Mode().IsRegular() || srcFi.Size() == 0 {
		return err
	}
	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0o700); err != nil {
		return err
	}
	tmpPath := fmt.Sprintf("%s.tmp.%d", dstPath, time.Now().UnixNano())
	_ = os.Remove(tmpPath)
	if err := os.Link(srcMemPath, tmpPath); err != nil {
		if cerr := ch.CopySparseFileFast(ctx, srcMemPath, tmpPath); cerr != nil {
			_ = os.Remove(tmpPath)
			return cerr
		}
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if tmpfsSlotsEnabled() {
		maybePrewarmGoldenSlots(dstPath)
	}
	return nil
}
