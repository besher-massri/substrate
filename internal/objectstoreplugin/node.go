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

// Package objectstoreplugin implements the object-store plugin API on GCS or S3
// object storage, using the plugin's own credentials.
//
// Snapshot files are stored as zstd-compressed objects named <file>.zstd
// under the snapshot URI, except the manifest, which is stored as is. This is
// the layout atelet has always written, so snapshots taken before the plugin
// existed stay readable.
package objectstoreplugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/objectstorage"
	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This plugin stores each file as <name>.zstd, except manifestFile, which it
// stores as-is so that snapshots written before atelet used this plugin stay
// readable.
const manifestFile = "manifest.json"

// NodePlugin serves NodeProvider on an object storage client.
type NodePlugin struct {
	objectstorev1.UnimplementedNodeProviderServer

	gcsClient objectstorage.ObjectStorage
	uploader  *objectstorage.Uploader
	// root confines every local path a caller names.
	root string

	cacheFlight singleflight.Group
	actorLocks  sync.Map
}

// NewNodePlugin returns a NodePlugin that reads and writes local files only
// below root. All upload requests share uploadMemory bytes of reservations for
// compression and backend buffers.
func NewNodePlugin(gcsClient objectstorage.ObjectStorage, root string, uploadMemory int64) (*NodePlugin, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("root %q is not an absolute path", root)
	}
	uploader, err := objectstorage.NewUploader(gcsClient, uploadMemory)
	if err != nil {
		return nil, err
	}
	return &NodePlugin{gcsClient: gcsClient, uploader: uploader, root: filepath.Clean(root)}, nil
}

// FetchSnapshot downloads the requested snapshot files into write_path.
func (p *NodePlugin) FetchSnapshot(ctx context.Context, req *objectstorev1.FetchSnapshotRequest) (*objectstorev1.FetchSnapshotResponse, error) {
	uri, dstDir, err := p.validate(req.GetSnapshotUri(), req.GetWritePath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := p.openDir(dstDir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening restore directory: %w", err))
	}
	defer root.Close()

	absDstDir := filepath.Join(p.root, dstDir)
	cacheTag := isCacheableTagSnapshot(req.GetSnapshotUri())
	actorPrefix, snapKey, cacheActor := parseActorSnapshotCachePath(req.GetSnapshotUri())

	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range req.GetFiles() {
		fileName := fileName
		g.Go(func() error {
			if cacheTag {
				if err := p.fetchCachedTagFile(gCtx, req.GetSnapshotUri(), uri, absDstDir, fileName); err == nil {
					return nil
				}
			} else if cacheActor {
				if err := p.fetchCachedActorFile(gCtx, actorPrefix, snapKey, uri, absDstDir, fileName); err == nil {
					return nil
				}
			}
			if fileName == manifestFile {
				return p.fetchManifest(gCtx, uri, root)
			}
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
			}
			if dir := filepath.Dir(fileName); dir != "." {
				if err := root.MkdirAll(dir, 0o700); err != nil {
					return fmt.Errorf("while creating %s in restore directory: %w", dir, err)
				}
			}
			local, err := root.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				return fmt.Errorf("while opening %s in restore directory: %w", fileName, err)
			}
			fetchErr := objectstorage.FetchFileFromGCSWithZstd(gCtx, p.gcsClient, objectURI, local)
			closeErr := local.Close()
			if err := errors.Join(fetchErr, closeErr); err != nil {
				return fmt.Errorf("while downloading %s from GCS: %w", fileName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, toStatus(err)
	}
	if cacheTag {
		p.linkGoldenBaseFromFetchedTag(req.GetSnapshotUri(), uri, absDstDir)
	}
	if slices.Contains(req.GetFiles(), "golden-delta") || slices.Contains(req.GetFiles(), "memory-ranges.delta") {
		p.ensureGoldenBaseFromDelta(ctx, req.GetSnapshotUri(), uri, absDstDir)
	}
	return &objectstorev1.FetchSnapshotResponse{}, nil
}

// isCacheableTagSnapshot returns true if the snapshot URI refers to an
// immutable golden/tag snapshot (which is shared across many actors).
func isCacheableTagSnapshot(rawURI string) bool {
	return strings.Contains(rawURI, "/tags/") || strings.Contains(rawURI, "/atespaces/ate-golden-actors/")
}

// parseActorSnapshotCachePath extracts the actor prefix and snapshot key for
// per-actor snapshot URIs when ATE_CACHE_ACTOR_SNAPSHOTS is enabled.
func parseActorSnapshotCachePath(rawURI string) (actorPrefix, snapKey string, ok bool) {
	if v := os.Getenv("ATE_CACHE_ACTOR_SNAPSHOTS"); v != "true" && v != "1" {
		return "", "", false
	}
	if isCacheableTagSnapshot(rawURI) {
		return "", "", false
	}
	idx := strings.Index(rawURI, "/snapshots/")
	if idx <= 0 {
		return "", "", false
	}
	actorPrefix = rawURI[:idx]
	rest := strings.Trim(rawURI[idx+len("/snapshots/"):], "/")
	if rest == "" || strings.Contains(rest, "/") {
		return "", "", false
	}
	return actorPrefix, rest, true
}

func (p *NodePlugin) actorLock(actorHash string) *sync.Mutex {
	v, _ := p.actorLocks.LoadOrStore(actorHash, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (p *NodePlugin) actorSnapshotDir(actorPrefix, snapKey string) (actorDir, snapDir string) {
	sum := sha256.Sum256([]byte(actorPrefix))
	actorHash := hex.EncodeToString(sum[:16])
	actorDir = filepath.Join(p.root, "actor-snapshot-cache", actorHash)
	snapDir = filepath.Join(actorDir, snapKey)
	return actorDir, snapDir
}

// pruneOtherActorSnapshots removes all cached snapshot subdirectories under
// actorDir except keepSnapKey so at most 1 snapshot per actor is retained.
func pruneOtherActorSnapshots(actorDir, keepSnapKey string) {
	entries, err := os.ReadDir(actorDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.Name() == keepSnapKey {
			continue
		}
		_ = os.RemoveAll(filepath.Join(actorDir, e.Name()))
	}
}

// cacheActorSnapshotOnUpload populates the node-local actor snapshot cache via
// hardlinks (or copies if across filesystems) during UploadSnapshot and evicts
// any previous snapshot generation for the same actor.
func (p *NodePlugin) cacheActorSnapshotOnUpload(actorPrefix, snapKey, absSrcDir string, files []string) {
	actorDir, snapDir := p.actorSnapshotDir(actorPrefix, snapKey)
	mu := p.actorLock(filepath.Base(actorDir))
	mu.Lock()
	defer mu.Unlock()

	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		return
	}
	for _, fileName := range files {
		srcPath := filepath.Join(absSrcDir, fileName)
		dstPath := filepath.Join(snapDir, fileName)
		if dir := filepath.Dir(dstPath); dir != snapDir {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				continue
			}
		}
		_ = os.Remove(dstPath)
		if err := os.Link(srcPath, dstPath); err != nil {
			_ = copyFileContents(srcPath, dstPath)
		}
	}
	pruneOtherActorSnapshots(actorDir, snapKey)
}

// fetchCachedActorFile serves an actor snapshot file from the node-local
// actor-snapshot-cache if present, or downloads it from GCS into the cache
// (evicting any older snapshot for the same actor) and links/copies it into
// absDstDir.
func (p *NodePlugin) fetchCachedActorFile(ctx context.Context, actorPrefix, snapKey string, uri resources.SnapshotURI, absDstDir, fileName string) error {
	actorDir, snapDir := p.actorSnapshotDir(actorPrefix, snapKey)
	cachePath := filepath.Join(snapDir, fileName)
	flightKey := cachePath

	_, err, _ := p.cacheFlight.Do(flightKey, func() (any, error) {
		mu := p.actorLock(filepath.Base(actorDir))
		mu.Lock()
		defer mu.Unlock()

		if info, statErr := os.Stat(cachePath); statErr == nil && info.Mode().IsRegular() {
			pruneOtherActorSnapshots(actorDir, snapKey)
			return nil, nil
		}
		if err := os.MkdirAll(filepath.Dir(cachePath), 0o700); err != nil {
			return nil, err
		}
		tmpFile, err := os.CreateTemp(filepath.Dir(cachePath), ".tmp-*")
		if err != nil {
			return nil, err
		}
		tmpPath := tmpFile.Name()
		defer os.Remove(tmpPath)

		if fileName == manifestFile {
			if err := tmpFile.Close(); err != nil {
				return nil, err
			}
			manifestURI, err := uri.ObjectURI(manifestFile)
			if err != nil {
				return nil, err
			}
			manifest, err := objectstorage.FetchFromGCS(ctx, p.gcsClient, manifestURI)
			if err != nil {
				return nil, err
			}
			if err := os.WriteFile(tmpPath, manifest, 0o600); err != nil {
				return nil, err
			}
		} else {
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				_ = tmpFile.Close()
				return nil, err
			}
			fetchErr := objectstorage.FetchFileFromGCSWithZstd(ctx, p.gcsClient, objectURI, tmpFile)
			closeErr := tmpFile.Close()
			if err := errors.Join(fetchErr, closeErr); err != nil {
				return nil, err
			}
		}
		if err := os.Chmod(tmpPath, 0o600); err != nil {
			return nil, err
		}
		if err := os.Rename(tmpPath, cachePath); err != nil {
			return nil, err
		}
		pruneOtherActorSnapshots(actorDir, snapKey)
		return nil, nil
	})
	if err != nil {
		return err
	}

	dstPath := filepath.Join(absDstDir, fileName)
	if dir := filepath.Dir(dstPath); dir != absDstDir {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	if fileName != manifestFile && fileName != "config.json" {
		_ = os.Remove(dstPath)
		if err := os.Link(cachePath, dstPath); err == nil {
			return nil
		}
	}
	return copyFileContents(cachePath, dstPath)
}

func (p *NodePlugin) linkGoldenBaseCache(baseID, srcMemPath, sourceObjectURI string) {
	if baseID == "" || strings.Contains(baseID, "/") || strings.Contains(baseID, "..") {
		return
	}
	baseDir := filepath.Join(p.root, "golden-base-cache", baseID)
	baseMem := filepath.Join(baseDir, "memory-ranges")
	if info, err := os.Stat(baseMem); err == nil && info.Size() > 0 {
		return
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return
	}
	tmpFile, err := os.CreateTemp(baseDir, ".tmp-link-*")
	if err != nil {
		return
	}
	tmpPath := tmpFile.Name()
	_ = tmpFile.Close()
	_ = os.Remove(tmpPath)
	if err := os.Link(srcMemPath, tmpPath); err != nil {
		return
	}
	if err := os.Rename(tmpPath, baseMem); err != nil {
		_ = os.Remove(tmpPath)
		return
	}
	if sourceObjectURI != "" {
		_ = os.WriteFile(filepath.Join(baseDir, "source-uri"), []byte(sourceObjectURI), 0o644)
	}
}

func (p *NodePlugin) linkGoldenBaseFromFetchedTag(rawURI string, uri resources.SnapshotURI, absDstDir string) {
	sum := sha256.Sum256([]byte(rawURI))
	cacheDir := filepath.Join(p.root, "golden-snapshot-cache", hex.EncodeToString(sum[:16]))
	idBytes, err := os.ReadFile(filepath.Join(cacheDir, "base-id"))
	if err != nil {
		idBytes, err = os.ReadFile(filepath.Join(absDstDir, "base-id"))
	}
	if err != nil {
		return
	}
	baseID := strings.TrimSpace(string(idBytes))
	memPath := filepath.Join(cacheDir, "memory-ranges")
	if _, err := os.Stat(memPath); err != nil {
		memPath = filepath.Join(absDstDir, "memory-ranges")
	}
	if _, err := os.Stat(memPath); err == nil {
		objURI, _ := uri.ObjectURI("memory-ranges.zstd")
		p.linkGoldenBaseCache(baseID, memPath, objURI)
	}
}

// ensureGoldenBaseFromDelta populates /var/lib/ate/golden-base-cache/<baseID>/memory-ranges
// when a resume downloads a differential checkpoint (golden-delta + base-id, or
// memory-ranges.delta + base-snapshot.id) on a node that did not yet have the
// golden base memory-ranges cached locally.
func (p *NodePlugin) ensureGoldenBaseFromDelta(ctx context.Context, rawURI string, uri resources.SnapshotURI, absDstDir string) {
	idBytes, err := os.ReadFile(filepath.Join(absDstDir, "base-id"))
	if err != nil {
		idBytes, err = os.ReadFile(filepath.Join(absDstDir, "base-snapshot.id"))
	}
	if err != nil {
		return
	}
	baseID := strings.TrimSpace(string(idBytes))
	if baseID == "" || strings.Contains(baseID, "/") || strings.Contains(baseID, "..") {
		return
	}
	goldenBaseDir := filepath.Join(p.root, "golden-base-cache", baseID)
	goldenMemPath := filepath.Join(goldenBaseDir, "memory-ranges")
	if info, err := os.Stat(goldenMemPath); err == nil && info.Size() > 0 {
		return
	}
	var explicitObjURI string
	if deltaBytes, derr := os.ReadFile(filepath.Join(absDstDir, "golden-delta")); derr == nil {
		s := strings.TrimSpace(string(deltaBytes))
		if strings.HasPrefix(s, "gs://") || strings.HasPrefix(s, "s3://") {
			explicitObjURI = s
		}
	}

	_, _, _ = p.cacheFlight.Do("golden-base:"+baseID, func() (any, error) {
		if info, err := os.Stat(goldenMemPath); err == nil && info.Size() > 0 {
			return nil, nil
		}
		memObjURI := explicitObjURI
		if memObjURI == "" {
			atespaceIdx := strings.Index(rawURI, "/atespaces/")
			if atespaceIdx < 0 {
				return nil, fmt.Errorf("snapshot URI %s missing /atespaces/", rawURI)
			}
			rest := rawURI[atespaceIdx+len("/atespaces/"):]
			slashIdx := strings.Index(rest, "/")
			if slashIdx <= 0 {
				return nil, fmt.Errorf("snapshot URI %s missing atespace name", rawURI)
			}
			atespace := rest[:slashIdx]
			bucketAndRoot := rawURI[:atespaceIdx]
			tagsPrefix := strings.TrimPrefix(bucketAndRoot, "gs://")
			slashBucket := strings.Index(tagsPrefix, "/")
			if slashBucket <= 0 {
				return nil, fmt.Errorf("snapshot URI %s missing bucket", rawURI)
			}
			bucket := tagsPrefix[:slashBucket]
			rootPrefix := tagsPrefix[slashBucket+1:]
			tagSearchPrefix := fmt.Sprintf("%s/atespaces/%s/tags/", rootPrefix, atespace)

			lister, ok := p.gcsClient.(interface {
				List(ctx context.Context, bucket, prefix string) ([]string, error)
			})
			if !ok {
				return nil, fmt.Errorf("object storage client does not support List")
			}
			objs, err := lister.List(ctx, bucket, tagSearchPrefix)
			if err != nil {
				return nil, err
			}
			for _, obj := range objs {
				if strings.HasSuffix(obj, "/memory-ranges.zstd") {
					memObjURI = fmt.Sprintf("gs://%s/%s", bucket, obj)
					break
				}
			}
		}
		if memObjURI == "" {
			return nil, fmt.Errorf("no golden tag memory-ranges.zstd found for base %s", baseID)
		}
		if err := os.MkdirAll(goldenBaseDir, 0o755); err != nil {
			return nil, err
		}
		tmpFile, err := os.CreateTemp(goldenBaseDir, ".tmp-golden-*")
		if err != nil {
			return nil, err
		}
		tmpPath := tmpFile.Name()
		defer os.Remove(tmpPath)

		fetchErr := objectstorage.FetchFileFromGCSWithZstd(ctx, p.gcsClient, memObjURI, tmpFile)
		closeErr := tmpFile.Close()
		if err := errors.Join(fetchErr, closeErr); err != nil {
			return nil, err
		}
		_ = os.Chmod(tmpPath, 0o644)
		_ = os.WriteFile(filepath.Join(goldenBaseDir, "source-uri"), []byte(memObjURI), 0o644)
		return nil, os.Rename(tmpPath, goldenMemPath)
	})
}

func (p *NodePlugin) fetchCachedTagFile(ctx context.Context, rawURI string, uri resources.SnapshotURI, absDstDir, fileName string) error {
	sum := sha256.Sum256([]byte(rawURI))
	cacheDir := filepath.Join(p.root, "golden-snapshot-cache", hex.EncodeToString(sum[:16]))
	cachePath := filepath.Join(cacheDir, fileName)

	_, err, _ := p.cacheFlight.Do(cachePath, func() (any, error) {
		if info, statErr := os.Stat(cachePath); statErr == nil && info.Mode().IsRegular() {
			return nil, nil
		}
		if err := os.MkdirAll(filepath.Dir(cachePath), 0o700); err != nil {
			return nil, err
		}
		tmpFile, err := os.CreateTemp(filepath.Dir(cachePath), ".tmp-*")
		if err != nil {
			return nil, err
		}
		tmpPath := tmpFile.Name()
		defer os.Remove(tmpPath)

		if fileName == manifestFile {
			if err := tmpFile.Close(); err != nil {
				return nil, err
			}
			manifestURI, err := uri.ObjectURI(manifestFile)
			if err != nil {
				return nil, err
			}
			manifest, err := objectstorage.FetchFromGCS(ctx, p.gcsClient, manifestURI)
			if err != nil {
				return nil, err
			}
			if err := os.WriteFile(tmpPath, manifest, 0o600); err != nil {
				return nil, err
			}
		} else {
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				_ = tmpFile.Close()
				return nil, err
			}
			fetchErr := objectstorage.FetchFileFromGCSWithZstd(ctx, p.gcsClient, objectURI, tmpFile)
			closeErr := tmpFile.Close()
			if err := errors.Join(fetchErr, closeErr); err != nil {
				return nil, err
			}
		}
		if err := os.Chmod(tmpPath, 0o600); err != nil {
			return nil, err
		}
		return nil, os.Rename(tmpPath, cachePath)
	})
	if err != nil {
		return err
	}

	dstPath := filepath.Join(absDstDir, fileName)
	if dir := filepath.Dir(dstPath); dir != absDstDir {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	// Hardlink memory-ranges from the golden-snapshot-cache so sparse holes and
	// page cache are preserved across cold restores without copying 384 MiB.
	if fileName == "memory-ranges" {
		_ = os.Remove(dstPath)
		if err := os.Link(cachePath, dstPath); err == nil {
			return nil
		}
	}
	// Copy small metadata files (e.g. config.json) into the per-actor restore
	// directory so in-place socket path rewrites do not mutate the shared cache.
	return copyFileContents(cachePath, dstPath)
}

func copyFileContents(srcPath, dstPath string) error {
	in, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	return errors.Join(copyErr, closeErr)
}

// UploadSnapshot uploads the requested files from local_path into the
// snapshot.
func (p *NodePlugin) UploadSnapshot(ctx context.Context, req *objectstorev1.UploadSnapshotRequest) (*objectstorev1.UploadSnapshotResponse, error) {
	uri, srcDir, err := p.validate(req.GetSnapshotUri(), req.GetLocalPath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := p.openDir(srcDir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening snapshot directory: %w", err))
	}
	defer root.Close()

	upload := func(ctx context.Context, fileName string) error {
		local, err := root.Open(fileName)
		if err != nil {
			return fmt.Errorf("while opening %s in snapshot directory: %w", fileName, err)
		}
		defer local.Close()
		info, err := local.Stat()
		if err != nil {
			return fmt.Errorf("while inspecting %s in snapshot directory: %w", fileName, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("snapshot file %s is not a regular file", fileName)
		}
		if fileName == manifestFile {
			return p.uploadManifest(ctx, uri, local)
		}
		objectURI, err := uri.ObjectURI(fileName + ".zstd")
		if err != nil {
			return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
		}
		if err := p.uploader.SendFile(ctx, objectURI, local); err != nil {
			return fmt.Errorf("while uploading %s to GCS: %w", fileName, err)
		}
		return nil
	}
	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range req.GetFiles() {
		if fileName != manifestFile {
			g.Go(func() error { return upload(gCtx, fileName) })
		}
	}
	if err := g.Wait(); err != nil {
		return nil, toStatus(err)
	}
	// A manifest commits the snapshot only after every requested data file has
	// succeeded. Use ctx: errgroup cancels gCtx when Wait returns.
	if slices.Contains(req.GetFiles(), manifestFile) {
		if err := upload(ctx, manifestFile); err != nil {
			return nil, toStatus(err)
		}
	}
	if idBytes, berr := root.ReadFile("base-id"); berr == nil {
		baseID := strings.TrimSpace(string(idBytes))
		if _, derr := root.Stat("golden-delta"); errors.Is(derr, os.ErrNotExist) {
			memPath := filepath.Join(p.root, srcDir, "memory-ranges")
			if _, merr := os.Stat(memPath); merr == nil {
				objURI, _ := uri.ObjectURI("memory-ranges.zstd")
				p.linkGoldenBaseCache(baseID, memPath, objURI)
			}
		}
	}
	if actorPrefix, snapKey, ok := parseActorSnapshotCachePath(req.GetSnapshotUri()); ok {
		p.cacheActorSnapshotOnUpload(actorPrefix, snapKey, filepath.Join(p.root, srcDir), req.GetFiles())
	}
	return &objectstorev1.UploadSnapshotResponse{}, nil
}

// fetchManifest downloads the uncompressed manifest into root.
func (p *NodePlugin) fetchManifest(ctx context.Context, uri resources.SnapshotURI, root *os.Root) error {
	manifestURI, err := uri.ObjectURI(manifestFile)
	if err != nil {
		return err
	}
	manifest, err := objectstorage.FetchFromGCS(ctx, p.gcsClient, manifestURI)
	if err != nil {
		return fmt.Errorf("while fetching snapshot manifest: %w", err)
	}
	if err := root.WriteFile(manifestFile, manifest, 0o600); err != nil {
		return fmt.Errorf("while writing %s in restore directory: %w", manifestFile, err)
	}
	return nil
}

// uploadManifest uploads the manifest from local uncompressed.
func (p *NodePlugin) uploadManifest(ctx context.Context, uri resources.SnapshotURI, local *os.File) error {
	manifestURI, err := uri.ObjectURI(manifestFile)
	if err != nil {
		return fmt.Errorf("while addressing snapshot manifest in GCS: %w", err)
	}
	if err := p.uploader.Send(ctx, manifestURI, local); err != nil {
		return fmt.Errorf("while uploading snapshot manifest: %w", err)
	}
	return nil
}

// validate checks a request's snapshot URI, local directory and file names,
// returning the parsed URI and the local directory relative to p.root.
func (p *NodePlugin) validate(snapshotURI, dir string, files []string) (resources.SnapshotURI, string, error) {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return resources.SnapshotURI{}, "", status.Error(codes.InvalidArgument, err.Error())
	}
	if !filepath.IsAbs(dir) {
		return resources.SnapshotURI{}, "", status.Errorf(codes.InvalidArgument, "local path %q is not absolute", dir)
	}
	rel, err := filepath.Rel(p.root, dir)
	if err != nil || !filepath.IsLocal(rel) {
		return resources.SnapshotURI{}, "", status.Errorf(codes.InvalidArgument, "local path %q is outside %q", dir, p.root)
	}
	if len(files) == 0 {
		return resources.SnapshotURI{}, "", status.Error(codes.InvalidArgument, "no files requested")
	}
	if err := resources.ValidateSnapshotFileNames(files); err != nil {
		return resources.SnapshotURI{}, "", status.Error(codes.InvalidArgument, err.Error())
	}
	return uri, rel, nil
}

// openDir opens rel, a directory below p.root, through an os.Root at p.root,
// so a symlink anywhere on the path cannot lead outside p.root.
func (p *NodePlugin) openDir(rel string) (*os.Root, error) {
	base, err := os.OpenRoot(p.root)
	if err != nil {
		return nil, err
	}
	defer base.Close()
	return base.OpenRoot(rel)
}
