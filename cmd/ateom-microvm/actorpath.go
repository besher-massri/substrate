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

// The paths ateom-microvm derives from the ActorDirs atelet sends. The
// directory under root_dir is ateom-microvm's own: atelet does not know it.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// goldenDeltaFile marks a checkpoint/restore directory whose memory-ranges has
// been deflated against the node-cached golden base (<baseID>/memory-ranges).
// Its content is the GCS object URI of the golden memory-ranges.zstd (when known)
// or "1".
const goldenDeltaFile = "golden-delta"

// ociBundlePath is the container's OCI bundle.
func ociBundlePath(actorDirs *ateompb.ActorDirs, containerName string) string {
	return filepath.Join(actorDirs.GetOciBundleDir(), containerName)
}

// rootfsUpperDir is the host directory backing the actor's rootfs overlay
// uppers: one subdirectory per container (see kata.UpperWorkDirs). Local to
// this binary: atelet never touches it, so it is not one of the ActorDirs.
func rootfsUpperDir(actorDirs *ateompb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "rootfs-upper")
}

// goldenBaseCacheDir returns the node-local directory caching the immutable
// golden base memory-ranges for baseID (<ateRoot>/golden-base-cache/<baseID>),
// derived from snapshotDir (<ateRoot>/actors/<actorUID>/{checkpoint-state,restore-state}).
func goldenBaseCacheDir(snapshotDir, baseID string) (string, bool) {
	if os.Getenv("ATEOM_MICROVM_DISABLE_GOLDEN_DELTA") == "true" {
		return "", false
	}
	if baseID == "" || baseID == "." || strings.Contains(baseID, "/") || !filepath.IsLocal(baseID) {
		return "", false
	}
	ateRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Clean(snapshotDir))))
	if ateRoot == "" || ateRoot == "." || ateRoot == "/" {
		return "", false
	}
	return filepath.Join(ateRoot, "golden-base-cache", baseID), true
}

// ensureGoldenBaseCached caches a full golden memory-ranges at
// <ateRoot>/golden-base-cache/<baseID>/memory-ranges if not already present.
func ensureGoldenBaseCached(ctx context.Context, snapshotDir, baseID, memRangesPath string) {
	cacheDir, ok := goldenBaseCacheDir(snapshotDir, baseID)
	if !ok {
		return
	}
	cacheFile := filepath.Join(cacheDir, "memory-ranges")
	if _, err := os.Stat(cacheFile); err == nil {
		return
	}
	if _, err := os.Stat(memRangesPath); err != nil {
		return
	}
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		slog.WarnContext(ctx, "Failed to create golden base cache dir", slog.String("dir", cacheDir), slog.Any("err", err))
		return
	}
	tmp := fmt.Sprintf("%s.tmp.%d", cacheFile, time.Now().UnixNano())
	_ = os.Remove(tmp)
	if err := os.Link(memRangesPath, tmp); err != nil {
		// Fall back to sparse copy if across filesystems.
		if cerr := ch.MergeSparseOverlay(ctx, memRangesPath, memRangesPath, tmp); cerr != nil {
			slog.WarnContext(ctx, "Failed to stage golden base memory-ranges cache", slog.String("base_id", baseID), slog.Any("err", cerr))
			return
		}
	}
	if err := os.Rename(tmp, cacheFile); err != nil {
		_ = os.Remove(tmp)
		return
	}
	slog.InfoContext(ctx, "Cached golden base memory-ranges on node", slog.String("base_id", baseID), slog.String("path", cacheFile))
}
