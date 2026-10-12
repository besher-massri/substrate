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

// Package objectstoreplugin implements the snapshot plugin API on GCS or S3
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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	objectstoresnapshotv1 "github.com/agent-substrate/substrate/pkg/proto/objectstoresnapshotpb/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This plugin stores each file as <name>.zstd, except manifestFile, which it
// stores as-is so that snapshots written before atelet used this plugin stay
// readable.
const (
	manifestFile    = "manifest.json"
	baseIDFile      = "base-id"
	memoryRangesFile = "memory-ranges"
	goldenDeltaFile = "golden-delta"
)

// NodePlugin serves NodeProvider on an object storage client.
type NodePlugin struct {
	objectstoresnapshotv1.UnimplementedNodeProviderServer

	gcsClient objectstorage.ObjectStorage
	// root confines every local path a caller names.
	root string

	cacheMu   sync.Mutex
	keyLocks  map[string]*sync.Mutex
}

// NewNodePlugin returns a NodePlugin that reads and writes local files only
// below root.
func NewNodePlugin(gcsClient objectstorage.ObjectStorage, root string) (*NodePlugin, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("root %q is not an absolute path", root)
	}
	return &NodePlugin{
		gcsClient: gcsClient,
		root:      filepath.Clean(root),
		keyLocks:  make(map[string]*sync.Mutex),
	}, nil
}

func (p *NodePlugin) keyLock(key string) *sync.Mutex {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	mu, ok := p.keyLocks[key]
	if !ok {
		mu = &sync.Mutex{}
		p.keyLocks[key] = mu
	}
	return mu
}

func isCacheableTagURI(uri resources.SnapshotURI) bool {
	if os.Getenv("ATE_DISABLE_GOLDEN_SNAPSHOT_CACHE") == "true" {
		return false
	}
	return uri.OwnedBy(resources.TagSnapshotOwner(uri.Atespace(), uri.Name()))
}

func isCacheableActorURI(uri resources.SnapshotURI) bool {
	if os.Getenv("ATE_CACHE_ACTOR_SNAPSHOTS") != "true" {
		return false
	}
	return !isCacheableTagURI(uri)
}

func validLocalID(id string) bool {
	return id != "" && id != "." && !strings.Contains(id, "/") && filepath.IsLocal(id)
}

// FetchSnapshot downloads the requested snapshot files into write_path.
func (p *NodePlugin) FetchSnapshot(ctx context.Context, req *objectstoresnapshotv1.FetchSnapshotRequest) (*objectstoresnapshotv1.FetchSnapshotResponse, error) {
	uri, dstDir, err := p.validate(req.GetSnapshotUri(), req.GetWritePath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := p.openDir(dstDir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening restore directory: %w", err))
	}
	defer root.Close()

	if isCacheableTagURI(uri) {
		if err := p.fetchCachedTagSnapshot(ctx, uri, dstDir, root, req.GetFiles()); err != nil {
			return nil, toStatus(err)
		}
		return &objectstoresnapshotv1.FetchSnapshotResponse{}, nil
	}

	if isCacheableActorURI(uri) {
		if hit, err := p.tryFetchCachedActorSnapshot(ctx, uri, dstDir, root, req.GetFiles()); err != nil {
			return nil, toStatus(err)
		} else if hit {
			return &objectstoresnapshotv1.FetchSnapshotResponse{}, nil
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range req.GetFiles() {
		fileName := fileName
		g.Go(func() error {
			if fileName == manifestFile {
				return p.fetchManifest(gCtx, uri, root)
			}
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
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
	if err := p.ensureGoldenBaseFromDelta(ctx, root); err != nil {
		return nil, toStatus(err)
	}
	if isCacheableActorURI(uri) {
		p.cacheActorSnapshot(uri, dstDir, req.GetFiles())
	}
	return &objectstoresnapshotv1.FetchSnapshotResponse{}, nil
}

func (p *NodePlugin) actorSnapshotCachePaths(uri resources.SnapshotURI) (actorHash, actorCacheRoot, snapKey, snapCacheDir string, ok bool) {
	actorDirKey := path.Dir(uri.String())
	snapKey = path.Base(uri.String())
	if !validLocalID(snapKey) {
		return "", "", "", "", false
	}
	actorHash = fmt.Sprintf("%x", sha256.Sum256([]byte(actorDirKey)))[:24]
	actorCacheRoot = filepath.Join(p.root, "actor-snapshot-cache", actorHash)
	snapCacheDir = filepath.Join(actorCacheRoot, snapKey)
	return actorHash, actorCacheRoot, snapKey, snapCacheDir, true
}

func (p *NodePlugin) tryFetchCachedActorSnapshot(ctx context.Context, uri resources.SnapshotURI, dstDir string, root *os.Root, files []string) (bool, error) {
	actorHash, _, _, snapCacheDir, ok := p.actorSnapshotCachePaths(uri)
	if !ok {
		return false, nil
	}
	mu := p.keyLock("actor:" + actorHash)
	mu.Lock()
	for _, f := range files {
		if fi, err := os.Stat(filepath.Join(snapCacheDir, f)); err != nil || !fi.Mode().IsRegular() {
			mu.Unlock()
			return false, nil
		}
	}
	for _, fileName := range files {
		cachedPath := filepath.Join(snapCacheDir, fileName)
		if fi, err := root.Lstat(fileName); err == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				mu.Unlock()
				return false, fmt.Errorf("restore file %s is a symlink", fileName)
			}
			_ = root.Remove(fileName)
		}
		if err := os.Link(cachedPath, filepath.Join(p.root, dstDir, fileName)); err == nil {
			continue
		}
		src, err := os.Open(cachedPath)
		if err != nil {
			mu.Unlock()
			return false, err
		}
		dst, err := root.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			src.Close()
			mu.Unlock()
			return false, fmt.Errorf("while opening %s in restore directory: %w", fileName, err)
		}
		_, copyErr := io.Copy(dst, src)
		closeErr := dst.Close()
		src.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			mu.Unlock()
			return false, err
		}
	}
	mu.Unlock()

	if err := p.ensureGoldenBaseFromDelta(ctx, root); err != nil {
		return true, err
	}
	return true, nil
}

func (p *NodePlugin) cacheActorSnapshot(uri resources.SnapshotURI, srcDir string, files []string) {
	actorHash, actorCacheRoot, snapKey, snapCacheDir, ok := p.actorSnapshotCachePaths(uri)
	if !ok {
		return
	}
	mu := p.keyLock("actor:" + actorHash)
	mu.Lock()
	defer mu.Unlock()

	if err := os.MkdirAll(snapCacheDir, 0o700); err != nil {
		return
	}
	for _, fileName := range files {
		srcFile := filepath.Join(p.root, srcDir, fileName)
		dstFile := filepath.Join(snapCacheDir, fileName)
		if srcFi, err := os.Stat(srcFile); err == nil {
			if dstFi, err := os.Stat(dstFile); err == nil && os.SameFile(srcFi, dstFi) {
				continue
			}
		}
		tmpFile := fmt.Sprintf("%s.tmp.%d", dstFile, time.Now().UnixNano())
		_ = os.Remove(tmpFile)
		if err := os.Link(srcFile, tmpFile); err != nil {
			src, oerr := os.Open(srcFile)
			if oerr != nil {
				continue
			}
			dst, cerr := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if cerr != nil {
				src.Close()
				continue
			}
			_, copyErr := io.Copy(dst, src)
			closeErr := dst.Close()
			src.Close()
			if errors.Join(copyErr, closeErr) != nil {
				_ = os.Remove(tmpFile)
				continue
			}
		}
		if err := os.Rename(tmpFile, dstFile); err != nil {
			_ = os.Remove(tmpFile)
		}
	}
	if entries, err := os.ReadDir(actorCacheRoot); err == nil {
		for _, entry := range entries {
			if entry.Name() != snapKey {
				_ = os.RemoveAll(filepath.Join(actorCacheRoot, entry.Name()))
			}
		}
	}
}

func (p *NodePlugin) fetchCachedTagSnapshot(ctx context.Context, uri resources.SnapshotURI, dstDir string, root *os.Root, files []string) error {
	cacheHash := fmt.Sprintf("%x", sha256.Sum256([]byte(uri.String())))
	cacheDir := filepath.Join(p.root, "golden-snapshot-cache", cacheHash)

	mu := p.keyLock("tag:" + cacheHash)
	mu.Lock()
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		mu.Unlock()
		return fmt.Errorf("while creating golden snapshot cache directory: %w", err)
	}
	var missing []string
	for _, f := range files {
		if _, err := os.Stat(filepath.Join(cacheDir, f)); err != nil {
			missing = append(missing, f)
		}
	}
	if len(missing) > 0 {
		g, gCtx := errgroup.WithContext(ctx)
		for _, fileName := range missing {
			fileName := fileName
			g.Go(func() error {
				dstFile := filepath.Join(cacheDir, fileName)
				tmpFile := fmt.Sprintf("%s.tmp.%d", dstFile, time.Now().UnixNano())
				defer os.Remove(tmpFile)
				if fileName == manifestFile {
					manifestURI, err := uri.ObjectURI(manifestFile)
					if err != nil {
						return err
					}
					b, err := objectstorage.FetchFromGCS(gCtx, p.gcsClient, manifestURI)
					if err != nil {
						return fmt.Errorf("while fetching snapshot manifest: %w", err)
					}
					if err := os.WriteFile(tmpFile, b, 0o600); err != nil {
						return err
					}
					return os.Rename(tmpFile, dstFile)
				}
				objectURI, err := uri.ObjectURI(fileName + ".zstd")
				if err != nil {
					return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
				}
				local, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
				if err != nil {
					return err
				}
				fetchErr := objectstorage.FetchFileFromGCSWithZstd(gCtx, p.gcsClient, objectURI, local)
				closeErr := local.Close()
				if err := errors.Join(fetchErr, closeErr); err != nil {
					return fmt.Errorf("while downloading %s from GCS: %w", fileName, err)
				}
				return os.Rename(tmpFile, dstFile)
			})
		}
		if err := g.Wait(); err != nil {
			mu.Unlock()
			return err
		}
	}
	// If this cached tag snapshot holds base-id and memory-ranges, also link it
	// under golden-base-cache/<baseID> so ateom-microvm can deflate/reconstruct
	// warm deltas on this node.
	if b, err := os.ReadFile(filepath.Join(cacheDir, baseIDFile)); err == nil {
		baseID := strings.TrimSpace(string(b))
		memPath := filepath.Join(cacheDir, memoryRangesFile)
		if validLocalID(baseID) {
			if _, merr := os.Stat(memPath); merr == nil {
				objURI, _ := uri.ObjectURI(memoryRangesFile + ".zstd")
				p.linkGoldenBaseCache(baseID, memPath, objURI)
			}
		}
	}
	mu.Unlock()

	for _, fileName := range files {
		cachedPath := filepath.Join(cacheDir, fileName)
		if fileName == memoryRangesFile {
			if fi, err := root.Lstat(fileName); err == nil {
				if fi.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("restore file %s is a symlink", fileName)
				}
				_ = root.Remove(fileName)
			}
			if err := os.Link(cachedPath, filepath.Join(p.root, dstDir, fileName)); err == nil {
				continue
			}
		}
		src, err := os.Open(cachedPath)
		if err != nil {
			return err
		}
		dst, err := root.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			src.Close()
			return fmt.Errorf("while opening %s in restore directory: %w", fileName, err)
		}
		_, copyErr := io.Copy(dst, src)
		closeErr := dst.Close()
		src.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return err
		}
	}
	return nil
}

func (p *NodePlugin) linkGoldenBaseCache(baseID, srcMemPath, sourceObjectURI string) {
	if !validLocalID(baseID) {
		return
	}
	baseDir := filepath.Join(p.root, "golden-base-cache", baseID)
	baseMem := filepath.Join(baseDir, memoryRangesFile)
	if _, err := os.Stat(baseMem); err == nil {
		return
	}
	if err := os.MkdirAll(baseDir, 0o700); err != nil {
		return
	}
	tmp := fmt.Sprintf("%s.tmp.%d", baseMem, time.Now().UnixNano())
	_ = os.Remove(tmp)
	if err := os.Link(srcMemPath, tmp); err != nil {
		return
	}
	if err := os.Rename(tmp, baseMem); err != nil {
		_ = os.Remove(tmp)
		return
	}
	if sourceObjectURI != "" {
		_ = os.WriteFile(filepath.Join(baseDir, "source-uri"), []byte(sourceObjectURI), 0o600)
	}
}

func (p *NodePlugin) ensureGoldenBaseFromDelta(ctx context.Context, root *os.Root) error {
	deltaBytes, err := root.ReadFile(goldenDeltaFile)
	if err != nil {
		return nil // Not a golden-delta snapshot
	}
	baseBytes, err := root.ReadFile(baseIDFile)
	if err != nil {
		return nil
	}
	baseID := strings.TrimSpace(string(baseBytes))
	if !validLocalID(baseID) {
		return nil
	}
	baseDir := filepath.Join(p.root, "golden-base-cache", baseID)
	baseMem := filepath.Join(baseDir, memoryRangesFile)
	if _, err := os.Stat(baseMem); err == nil {
		return nil
	}
	sourceObjectURI := strings.TrimSpace(string(deltaBytes))
	if !strings.HasPrefix(sourceObjectURI, "gs://") && !strings.HasPrefix(sourceObjectURI, "s3://") {
		return nil
	}
	mu := p.keyLock("base:" + baseID)
	mu.Lock()
	defer mu.Unlock()
	if _, err := os.Stat(baseMem); err == nil {
		return nil
	}
	if err := os.MkdirAll(baseDir, 0o700); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", baseMem, time.Now().UnixNano())
	defer os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	fetchErr := objectstorage.FetchFileFromGCSWithZstd(ctx, p.gcsClient, sourceObjectURI, f)
	closeErr := f.Close()
	if err := errors.Join(fetchErr, closeErr); err != nil {
		return fmt.Errorf("while fetching golden base %s from %s: %w", baseID, sourceObjectURI, err)
	}
	_ = os.WriteFile(filepath.Join(baseDir, "source-uri"), []byte(sourceObjectURI), 0o600)
	return os.Rename(tmp, baseMem)
}

// UploadSnapshot uploads the requested files from local_path into the
// snapshot.
func (p *NodePlugin) UploadSnapshot(ctx context.Context, req *objectstoresnapshotv1.UploadSnapshotRequest) (*objectstoresnapshotv1.UploadSnapshotResponse, error) {
	uri, srcDir, err := p.validate(req.GetSnapshotUri(), req.GetLocalPath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := p.openDir(srcDir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening snapshot directory: %w", err))
	}
	defer root.Close()

	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range req.GetFiles() {
		g.Go(func() error {
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
				return p.uploadManifest(gCtx, uri, local)
			}

			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
			}
			if err := objectstorage.SendFileToGCSWithZstd(gCtx, p.gcsClient, objectURI, local); err != nil {
				return fmt.Errorf("while uploading %s to GCS: %w", fileName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, toStatus(err)
	}
	if b, err := root.ReadFile(baseIDFile); err == nil {
		baseID := strings.TrimSpace(string(b))
		if validLocalID(baseID) && uri.OwnedBy(resources.ActorSnapshotOwner(uri.Atespace(), baseID)) {
			if _, derr := root.Stat(goldenDeltaFile); errors.Is(derr, os.ErrNotExist) {
				memPath := filepath.Join(p.root, srcDir, memoryRangesFile)
				if _, merr := os.Stat(memPath); merr == nil {
					objURI, _ := uri.ObjectURI(memoryRangesFile + ".zstd")
					p.linkGoldenBaseCache(baseID, memPath, objURI)
				}
			}
		}
	}
	if isCacheableActorURI(uri) {
		p.cacheActorSnapshot(uri, srcDir, req.GetFiles())
	}
	return &objectstoresnapshotv1.UploadSnapshotResponse{}, nil
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
	manifest, err := io.ReadAll(local)
	if err != nil {
		return fmt.Errorf("while reading %s in snapshot directory: %w", manifestFile, err)
	}
	manifestURI, err := uri.ObjectURI(manifestFile)
	if err != nil {
		return fmt.Errorf("while addressing snapshot manifest in GCS: %w", err)
	}
	if err := objectstorage.SendBytesToGCS(ctx, p.gcsClient, manifestURI, manifest); err != nil {
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
