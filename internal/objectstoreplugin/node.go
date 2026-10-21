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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/objectstorage"
	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This plugin stores each file as <name>.zstd, except manifestFile, which it
// stores as-is so that snapshots written before atelet used this plugin stay
// readable.
const (
	manifestFile     = "manifest.json"
	baseIDFile       = "base-id"
	memoryRangesFile = "memory-ranges"
	goldenDeltaFile  = "golden-delta"
)

// NodePlugin serves NodeProvider on an object storage client.
type NodePlugin struct {
	objectstorev1.UnimplementedNodeProviderServer

	// creds supplies the client each transfer runs with.
	creds    Credentials
	uploader *objectstorage.Uploader
	// root confines every local path a caller names.
	root string

	cacheMu  sync.Mutex
	keyLocks map[string]*sync.Mutex
}

// NewNodePlugin returns a NodePlugin that reads and writes local files only
// below root. All upload requests share uploadMemory bytes of reservations for
// compression and backend buffers. client is the pod's own storage client,
// GCS or S3 as the environment selected. It sizes that budget and serves
// every transfer unless WithCredentials supplies a per-actor one.
func NewNodePlugin(client objectstorage.ObjectStorage, root string, uploadMemory int64, opts ...NodeOption) (*NodePlugin, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("root %q is not an absolute path", root)
	}
	uploader, err := objectstorage.NewUploader(client, uploadMemory)
	if err != nil {
		return nil, err
	}
	cleanRoot := filepath.Clean(root)
	if os.Getenv("ATE_CACHE_ACTOR_SNAPSHOTS") == "true" && os.Getenv("ATE_RETAIN_NODE_CACHE_ACROSS_STEPS") != "true" {
		ts := time.Now().UnixNano()
		for _, name := range []string{"actor-snapshot-cache", "golden-base-cache", "golden-snapshot-cache"} {
			src := filepath.Join(cleanRoot, name)
			if _, err := os.Stat(src); err == nil {
				dst := filepath.Join(cleanRoot, fmt.Sprintf(".cache-trash-%d-%s", ts, name))
				_ = os.Rename(src, dst)
			}
		}
		go purgeCacheTrashAsync(cleanRoot)
	}
	p := &NodePlugin{
		creds:    Ambient(client),
		uploader: uploader,
		root:     cleanRoot,
		keyLocks: make(map[string]*sync.Mutex),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// purgeCacheTrashAsync deletes renamed snapshot cache directories in the background.
func purgeCacheTrashAsync(cleanRoot string) {
	entries, err := os.ReadDir(cleanRoot)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".cache-trash-") {
			continue
		}
		trashDir := filepath.Join(cleanRoot, entry.Name())
		subEntries, subErr := os.ReadDir(trashDir)
		if subErr == nil && len(subEntries) > 0 {
			ch := make(chan string, len(subEntries))
			for _, se := range subEntries {
				ch <- filepath.Join(trashDir, se.Name())
			}
			close(ch)
			var wg sync.WaitGroup
			workers := min(16, len(subEntries))
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for p := range ch {
						_ = os.RemoveAll(p)
					}
				}()
			}
			wg.Wait()
		}
		_ = os.RemoveAll(trashDir)
	}
}

func (p *NodePlugin) keyLock(key string) *sync.Mutex {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	if p.keyLocks == nil {
		p.keyLocks = make(map[string]*sync.Mutex)
	}
	mu, ok := p.keyLocks[key]
	if !ok {
		mu = &sync.Mutex{}
		p.keyLocks[key] = mu
	}
	return mu
}

func isCacheableTagURI(uri resources.SnapshotURI) bool {
	if os.Getenv("ATE_CACHE_ACTOR_SNAPSHOTS") != "true" || os.Getenv("ATE_DISABLE_GOLDEN_SNAPSHOT_CACHE") == "true" {
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

// NodeOption configures a NodePlugin.
type NodeOption func(*NodePlugin)

// WithCredentials makes transfers run with the client creds supplies for the
// actor JWT each request carries, instead of the ambient client.
func WithCredentials(creds Credentials) NodeOption {
	return func(p *NodePlugin) { p.creds = creds }
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
	tr := p.begin(req.GetActorJwt())

	if isCacheableTagURI(uri) {
		if err := p.fetchCachedTagSnapshot(ctx, tr, uri, dstDir, root, req.GetFiles()); err != nil {
			return nil, toStatus(err)
		}
		return &objectstorev1.FetchSnapshotResponse{}, nil
	}

	if isCacheableActorURI(uri) {
		if hit, err := p.tryFetchCachedActorSnapshot(ctx, tr, uri, dstDir, root, req.GetFiles()); err != nil {
			return nil, toStatus(err)
		} else if hit {
			return &objectstorev1.FetchSnapshotResponse{}, nil
		}
		if os.Getenv("ATE_SKIP_ACTOR_GCS_UPLOAD") == "true" && uri.Atespace() != resources.GoldenActorAtespace {
			if hit, err := p.tryFetchAnyGoldenSnapshot(dstDir, root, req.GetFiles()); err != nil {
				return nil, toStatus(err)
			} else if hit {
				return &objectstorev1.FetchSnapshotResponse{}, nil
			}
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range req.GetFiles() {
		fileName := fileName
		g.Go(func() error {
			if fileName == manifestFile {
				return p.fetchManifest(gCtx, tr, uri, root)
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
			fetchErr := tr.do(gCtx, func(client objectstorage.ObjectStorage, _ *objectstorage.Uploader) error {
				if err := truncate(local); err != nil {
					return err
				}
				return objectstorage.FetchFileFromGCSWithZstd(gCtx, client, objectURI, local)
			})
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
	if isCacheableActorURI(uri) {
		if err := p.ensureGoldenBaseFromDelta(ctx, tr, root); err != nil {
			return nil, toStatus(err)
		}
		p.cacheActorSnapshot(uri, dstDir, req.GetFiles())
	}
	return &objectstorev1.FetchSnapshotResponse{}, nil
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

func (p *NodePlugin) tryFetchCachedActorSnapshot(ctx context.Context, tr *transfer, uri resources.SnapshotURI, dstDir string, root *os.Root, files []string) (bool, error) {
	actorHash, _, _, snapCacheDir, ok := p.actorSnapshotCachePaths(uri)
	if !ok {
		return false, nil
	}
	mu := p.keyLock("actor:" + actorHash)
	mu.Lock()
	_, deltaErr := os.Stat(filepath.Join(snapCacheDir, goldenDeltaFile))
	_, memErr := os.Stat(filepath.Join(snapCacheDir, memoryRangesFile))
	completeCached := errors.Is(deltaErr, os.ErrNotExist) && memErr == nil
	for _, f := range files {
		if f == goldenDeltaFile && completeCached {
			continue
		}
		if fi, err := os.Stat(filepath.Join(snapCacheDir, f)); err != nil || !fi.Mode().IsRegular() {
			mu.Unlock()
			return false, nil
		}
	}
	for _, fileName := range files {
		if fileName == goldenDeltaFile && completeCached {
			_ = root.Remove(fileName)
			continue
		}
		if dir := filepath.Dir(fileName); dir != "." {
			if err := root.MkdirAll(dir, 0o700); err != nil {
				mu.Unlock()
				return false, err
			}
		}
		cachedPath := filepath.Join(snapCacheDir, fileName)
		if fi, err := root.Lstat(fileName); err == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				mu.Unlock()
				return false, fmt.Errorf("restore file %s is a symlink", fileName)
			}
			_ = root.Remove(fileName)
		}
		dstPath := filepath.Join(p.root, dstDir, fileName)
		if fileName == memoryRangesFile && completeCached {
			if err := os.Rename(cachedPath, dstPath); err == nil {
				continue
			}
		}
		if err := os.Link(cachedPath, dstPath); err == nil {
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

	if err := p.ensureGoldenBaseFromDelta(ctx, tr, root); err != nil {
		return true, err
	}
	return true, nil
}

func (p *NodePlugin) tryFetchAnyGoldenSnapshot(dstDir string, root *os.Root, files []string) (bool, error) {
	goldenRoot := filepath.Join(p.root, "golden-snapshot-cache")
	entries, err := os.ReadDir(goldenRoot)
	if err != nil {
		return false, nil
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cacheDir := filepath.Join(goldenRoot, entry.Name())
		allPresent := true
		for _, f := range files {
			if f == goldenDeltaFile {
				continue
			}
			if fi, serr := os.Stat(filepath.Join(cacheDir, f)); serr != nil || !fi.Mode().IsRegular() {
				allPresent = false
				break
			}
		}
		if !allPresent {
			continue
		}
		for _, fileName := range files {
			if fileName == goldenDeltaFile {
				_ = root.Remove(fileName)
				continue
			}
			if dir := filepath.Dir(fileName); dir != "." {
				if err := root.MkdirAll(dir, 0o700); err != nil {
					return false, err
				}
			}
			cachedPath := filepath.Join(cacheDir, fileName)
			if fi, lerr := root.Lstat(fileName); lerr == nil {
				if fi.Mode()&os.ModeSymlink != 0 {
					return false, fmt.Errorf("restore file %s is a symlink", fileName)
				}
				_ = root.Remove(fileName)
			}
			if err := os.Link(cachedPath, filepath.Join(p.root, dstDir, fileName)); err == nil {
				continue
			}
			src, oerr := os.Open(cachedPath)
			if oerr != nil {
				return false, oerr
			}
			dst, derr := root.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if derr != nil {
				src.Close()
				return false, fmt.Errorf("while opening %s in restore directory: %w", fileName, derr)
			}
			_, copyErr := io.Copy(dst, src)
			closeErr := dst.Close()
			src.Close()
			if cerr := errors.Join(copyErr, closeErr); cerr != nil {
				return false, cerr
			}
		}
		return true, nil
	}
	return false, nil
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
	completeMem := filepath.Join(p.root, filepath.Dir(srcDir), "complete-memory-ranges")
	hasCompleteMem := false
	if os.Getenv("ATE_STAGE_COMPLETE_MEMORY_RANGES") == "true" {
		for _, f := range files {
			if f == memoryRangesFile {
				if fi, err := os.Stat(completeMem); err == nil && fi.Mode().IsRegular() {
					hasCompleteMem = true
				}
				break
			}
		}
	}
	for _, fileName := range files {
		if fileName == goldenDeltaFile && hasCompleteMem {
			_ = os.Remove(filepath.Join(snapCacheDir, fileName))
			continue
		}
		if dir := filepath.Dir(fileName); dir != "." {
			_ = os.MkdirAll(filepath.Join(snapCacheDir, dir), 0o700)
		}
		if fileName == memoryRangesFile && hasCompleteMem {
			dstFile := filepath.Join(snapCacheDir, fileName)
			_ = os.Remove(dstFile)
			if err := os.Rename(completeMem, dstFile); err == nil {
				continue
			}
		}
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

func (p *NodePlugin) fetchCachedTagSnapshot(ctx context.Context, tr *transfer, uri resources.SnapshotURI, dstDir string, root *os.Root, files []string) error {
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
				if dir := filepath.Dir(fileName); dir != "." {
					if err := os.MkdirAll(filepath.Join(cacheDir, dir), 0o700); err != nil {
						return err
					}
				}
				dstFile := filepath.Join(cacheDir, fileName)
				tmpFile := fmt.Sprintf("%s.tmp.%d", dstFile, time.Now().UnixNano())
				defer os.Remove(tmpFile)
				if fileName == manifestFile {
					manifestURI, err := uri.ObjectURI(manifestFile)
					if err != nil {
						return err
					}
					var b []byte
					err = tr.do(gCtx, func(client objectstorage.ObjectStorage, _ *objectstorage.Uploader) (err error) {
						b, err = objectstorage.FetchFromGCS(gCtx, client, manifestURI)
						return err
					})
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
				fetchErr := tr.do(gCtx, func(client objectstorage.ObjectStorage, _ *objectstorage.Uploader) error {
					if err := truncate(local); err != nil {
						return err
					}
					return objectstorage.FetchFileFromGCSWithZstd(gCtx, client, objectURI, local)
				})
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
		if dir := filepath.Dir(fileName); dir != "." {
			if err := root.MkdirAll(dir, 0o700); err != nil {
				return err
			}
		}
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

func (p *NodePlugin) ensureGoldenBaseFromDelta(ctx context.Context, tr *transfer, root *os.Root) error {
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
	fetchErr := tr.do(ctx, func(client objectstorage.ObjectStorage, _ *objectstorage.Uploader) error {
		if err := truncate(f); err != nil {
			return err
		}
		return objectstorage.FetchFileFromGCSWithZstd(ctx, client, sourceObjectURI, f)
	})
	closeErr := f.Close()
	if err := errors.Join(fetchErr, closeErr); err != nil {
		return fmt.Errorf("while fetching golden base %s from %s: %w", baseID, sourceObjectURI, err)
	}
	_ = os.WriteFile(filepath.Join(baseDir, "source-uri"), []byte(sourceObjectURI), 0o600)
	return os.Rename(tmp, baseMem)
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

	if isCacheableActorURI(uri) && os.Getenv("ATE_SKIP_ACTOR_GCS_UPLOAD") == "true" && uri.Atespace() != resources.GoldenActorAtespace {
		for _, fileName := range req.GetFiles() {
			local, err := root.Open(fileName)
			if err != nil {
				return nil, toStatus(fmt.Errorf("while opening %s in snapshot directory: %w", fileName, err))
			}
			info, statErr := local.Stat()
			_ = local.Close()
			if statErr != nil {
				return nil, toStatus(fmt.Errorf("while inspecting %s in snapshot directory: %w", fileName, statErr))
			}
			if !info.Mode().IsRegular() {
				return nil, toStatus(fmt.Errorf("snapshot file %s is not a regular file", fileName))
			}
		}
		p.cacheActorSnapshot(uri, srcDir, req.GetFiles())
		_ = os.Remove(filepath.Join(p.root, filepath.Dir(srcDir), "complete-memory-ranges"))
		return &objectstorev1.UploadSnapshotResponse{}, nil
	}

	tr := p.begin(req.GetActorJwt())

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
			return p.uploadManifest(ctx, tr, uri, local)
		}
		objectURI, err := uri.ObjectURI(fileName + ".zstd")
		if err != nil {
			return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
		}
		err = tr.do(ctx, func(_ objectstorage.ObjectStorage, uploader *objectstorage.Uploader) error {
			if err := rewind(local); err != nil {
				return err
			}
			return uploader.SendFile(ctx, objectURI, local)
		})
		if err != nil {
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
	_ = os.Remove(filepath.Join(p.root, filepath.Dir(srcDir), "complete-memory-ranges"))
	return &objectstorev1.UploadSnapshotResponse{}, nil
}

// fetchManifest downloads the uncompressed manifest into root.
func (p *NodePlugin) fetchManifest(ctx context.Context, tr *transfer, uri resources.SnapshotURI, root *os.Root) error {
	manifestURI, err := uri.ObjectURI(manifestFile)
	if err != nil {
		return err
	}
	var manifest []byte
	err = tr.do(ctx, func(client objectstorage.ObjectStorage, _ *objectstorage.Uploader) (err error) {
		manifest, err = objectstorage.FetchFromGCS(ctx, client, manifestURI)
		return err
	})
	if err != nil {
		return fmt.Errorf("while fetching snapshot manifest: %w", err)
	}
	if err := root.WriteFile(manifestFile, manifest, 0o600); err != nil {
		return fmt.Errorf("while writing %s in restore directory: %w", manifestFile, err)
	}
	return nil
}

// uploadManifest uploads the manifest from local uncompressed.
func (p *NodePlugin) uploadManifest(ctx context.Context, tr *transfer, uri resources.SnapshotURI, local *os.File) error {
	manifestURI, err := uri.ObjectURI(manifestFile)
	if err != nil {
		return fmt.Errorf("while addressing snapshot manifest in GCS: %w", err)
	}
	err = tr.do(ctx, func(_ objectstorage.ObjectStorage, uploader *objectstorage.Uploader) error {
		if err := rewind(local); err != nil {
			return err
		}
		return uploader.Send(ctx, manifestURI, local)
	})
	if err != nil {
		return fmt.Errorf("while uploading snapshot manifest: %w", err)
	}
	return nil
}

// rewind puts an upload's source back at its start. A request storage
// refuses may already have read the file, and the retry must send all of it
// rather than what is left.
func rewind(f *os.File) error {
	_, err := f.Seek(0, io.SeekStart)
	return err
}

// truncate empties a download's destination, so a retry does not append to
// whatever a refused attempt wrote.
func truncate(f *os.File) error {
	if err := f.Truncate(0); err != nil {
		return err
	}
	return rewind(f)
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
