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

package objectstoreplugin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/objectstorage"
	objectstoresnapshotv1 "github.com/agent-substrate/substrate/pkg/proto/objectstoresnapshotpb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	testBucket = "bucket"
	testPrefix = "root/atespaces/team-a/actors/uid1/snapshots/snap1"
	testURI    = "gs://" + testBucket + "/" + testPrefix
)

// memObjects is an in-memory backend implementing both objectstorage.ObjectStorage
// and objectstore.Store, keyed by "bucket/object".
type memObjects struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemObjects() *memObjects { return &memObjects{m: map[string][]byte{}} }

func (s *memObjects) PutObject(_ context.Context, bucket, object string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[bucket+"/"+object] = b
	return nil
}

func (s *memObjects) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.m[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", objectstorage.ErrObjectNotFound, bucket, object)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *memObjects) List(_ context.Context, bucket, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.m {
		if name, ok := strings.CutPrefix(k, bucket+"/"); ok && strings.HasPrefix(name, prefix) {
			out = append(out, name)
		}
	}
	return out, nil
}

func (s *memObjects) Delete(_ context.Context, bucket, object string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, bucket+"/"+object)
	return nil
}

func (s *memObjects) Copy(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[dstBucket+"/"+dstObject] = s.m[srcBucket+"/"+srcObject]
	return nil
}

func (s *memObjects) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// serve starts the plugins on a real Unix socket, as the sidecar does, and
// returns a connection to it.
func serve(t *testing.T, backend *memObjects, root string) *grpc.ClientConn {
	t.Helper()
	// Unix socket paths are length-limited; t.TempDir can exceed that on macOS.
	sockDir, err := os.MkdirTemp("", "snapplug")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	lis, err := Listen(filepath.Join(sockDir, "plugin.sock"))
	if err != nil {
		t.Fatal(err)
	}
	node, err := NewNodePlugin(backend, root)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	objectstoresnapshotv1.RegisterNodeProviderServer(srv, node)
	objectstoresnapshotv1.RegisterControlProviderServer(srv, NewControlPlugin(backend))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := Dial(filepath.Join(sockDir, "plugin.sock"), ReadyWait)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestWaitReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := WaitReady(ctx, serve(t, newMemObjects(), t.TempDir())); err != nil {
		t.Errorf("WaitReady against a serving plugin = %v", err)
	}

	// A socket nobody listens on must fail when ctx ends, not hang: WaitReady
	// waits for the plugin as long as ctx allows.
	conn, err := Dial(filepath.Join(t.TempDir(), "missing.sock"), ReadyWait)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	short, cancelShort := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelShort()
	err = WaitReady(short, conn)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("WaitReady against a missing socket = %v, want %s", err, codes.DeadlineExceeded)
	}
}

func TestUploadFetchRoundTrip(t *testing.T) {
	ctx := context.Background()
	backend := newMemObjects()
	root := t.TempDir()
	client := objectstoresnapshotv1.NewNodeProviderClient(serve(t, backend, root))

	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")
	for _, d := range []string{src, dst} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string][]byte{
		"memory.img": bytes.Repeat([]byte("abcdefgh"), 1<<16),
		"state.bin":  []byte("vm state"),
		manifestFile: []byte(`{"snapshotFiles":["memory.img","state.bin"]}`),
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(src, name), content, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := client.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{
		SnapshotUri: testURI, LocalPath: src, Files: []string{"memory.img", "state.bin"},
	}); err != nil {
		t.Fatalf("UploadSnapshot(data) = %v", err)
	}
	if _, err := client.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{
		SnapshotUri: testURI, LocalPath: src, Files: []string{manifestFile},
	}); err != nil {
		t.Fatalf("UploadSnapshot(manifest) = %v", err)
	}

	// The stored layout is the one atelet has always written.
	want := []string{
		testBucket + "/" + testPrefix + "/manifest.json",
		testBucket + "/" + testPrefix + "/memory.img.zstd",
		testBucket + "/" + testPrefix + "/state.bin.zstd",
	}
	if got := backend.keys(); !equal(got, want) {
		t.Fatalf("stored objects = %v, want %v", got, want)
	}
	if got := backend.m[want[0]]; !bytes.Equal(got, files[manifestFile]) {
		t.Errorf("stored manifest = %q, want it uncompressed", got)
	}

	if _, err := client.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{
		SnapshotUri: testURI, WritePath: dst, Files: []string{manifestFile, "memory.img", "state.bin"},
	}); err != nil {
		t.Fatalf("FetchSnapshot = %v", err)
	}
	for name, content := range files {
		got, err := os.ReadFile(filepath.Join(dst, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("fetched %s differs from the uploaded file", name)
		}
	}
}

func TestFetchMissingIsNotFound(t *testing.T) {
	root := t.TempDir()
	client := objectstoresnapshotv1.NewNodeProviderClient(serve(t, newMemObjects(), root))
	for _, file := range []string{manifestFile, "memory.img"} {
		_, err := client.FetchSnapshot(context.Background(), &objectstoresnapshotv1.FetchSnapshotRequest{
			SnapshotUri: testURI, WritePath: root, Files: []string{file},
		})
		if status.Code(err) != codes.NotFound {
			t.Errorf("FetchSnapshot(%s) = %v, want NotFound", file, err)
		}
	}
}

func TestNodeRequestValidation(t *testing.T) {
	root := t.TempDir()
	client := objectstoresnapshotv1.NewNodeProviderClient(serve(t, newMemObjects(), root))
	for _, tc := range []struct {
		name  string
		uri   string
		dir   string
		files []string
	}{
		{"bad uri", "gs://bucket/not-a-snapshot", root, []string{"a"}},
		{"relative dir", testURI, "relative/dir", []string{"a"}},
		{"dir outside root", testURI, filepath.Dir(root), []string{"a"}},
		{"dir escapes root", testURI, root + "/../elsewhere", []string{"a"}},
		{"sibling sharing root's prefix", testURI, root + "-other", []string{"a"}},
		{"no files", testURI, root, nil},
		{"empty file", testURI, root, []string{""}},
		{"nested file", testURI, root, []string{"sub/a"}},
		{"parent file", testURI, root, []string{".."}},
		{"duplicate file", testURI, root, []string{"a", "b", "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, fetchErr := client.FetchSnapshot(context.Background(), &objectstoresnapshotv1.FetchSnapshotRequest{
				SnapshotUri: tc.uri, WritePath: tc.dir, Files: tc.files,
			})
			_, uploadErr := client.UploadSnapshot(context.Background(), &objectstoresnapshotv1.UploadSnapshotRequest{
				SnapshotUri: tc.uri, LocalPath: tc.dir, Files: tc.files,
			})
			for _, err := range []error{fetchErr, uploadErr} {
				if status.Code(err) != codes.InvalidArgument {
					t.Errorf("got %v, want InvalidArgument", err)
				}
			}
		})
	}
}

func TestUploadRejectsSymlinkOutsideDir(t *testing.T) {
	for _, file := range []string{manifestFile, "checkpoint.img"} {
		t.Run(file, func(t *testing.T) {
			parent := t.TempDir()
			backend := newMemObjects()
			client := objectstoresnapshotv1.NewNodeProviderClient(serve(t, backend, parent))
			checkpointDir := filepath.Join(parent, "checkpoint-state")
			if err := os.Mkdir(checkpointDir, 0o700); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(parent, "outside")
			if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(checkpointDir, file)); err != nil {
				t.Fatal(err)
			}

			_, err := client.UploadSnapshot(context.Background(), &objectstoresnapshotv1.UploadSnapshotRequest{
				SnapshotUri: testURI, LocalPath: checkpointDir, Files: []string{file},
			})
			if err == nil {
				t.Fatal("UploadSnapshot followed a symlink outside the snapshot directory")
			}
			if got := backend.keys(); len(got) != 0 {
				t.Fatalf("uploaded objects = %v, want none", got)
			}
		})
	}
}

func TestFetchRejectsSymlinkOutsideDir(t *testing.T) {
	for _, file := range []string{manifestFile, "checkpoint.img"} {
		t.Run(file, func(t *testing.T) {
			ctx := context.Background()
			parent := t.TempDir()
			backend := newMemObjects()
			client := objectstoresnapshotv1.NewNodeProviderClient(serve(t, backend, parent))
			src := filepath.Join(parent, "src")
			restoreDir := filepath.Join(parent, "restore-state")
			for _, d := range []string{src, restoreDir} {
				if err := os.Mkdir(d, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(src, file), []byte("replacement"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := client.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{
				SnapshotUri: testURI, LocalPath: src, Files: []string{file},
			}); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(parent, "outside")
			if err := os.WriteFile(outside, []byte("keep me"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(restoreDir, file)); err != nil {
				t.Fatal(err)
			}

			_, err := client.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{
				SnapshotUri: testURI, WritePath: restoreDir, Files: []string{file},
			})
			if err == nil {
				t.Fatal("FetchSnapshot followed a symlink outside the restore directory")
			}
			if got, err := os.ReadFile(outside); err != nil || string(got) != "keep me" {
				t.Fatalf("outside file = %q, %v; want unchanged", got, err)
			}
		})
	}
}

// A directory below --root that is a symlink to elsewhere passes the lexical
// check, so only opening it through os.Root keeps the transfer inside --root.
func TestRejectsDirSymlinkOutsideRoot(t *testing.T) {
	ctx := context.Background()
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	outside := filepath.Join(parent, "outside")
	for _, d := range []string{root, outside} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(outside, "checkpoint.img"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	backend := newMemObjects()
	client := objectstoresnapshotv1.NewNodeProviderClient(serve(t, backend, root))

	if _, err := client.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{
		SnapshotUri: testURI, LocalPath: link, Files: []string{"checkpoint.img"},
	}); err == nil {
		t.Error("UploadSnapshot read through a directory symlink outside --root")
	}
	if got := backend.keys(); len(got) != 0 {
		t.Errorf("uploaded objects = %v, want none", got)
	}

	backend.m[testBucket+"/"+testPrefix+"/"+manifestFile] = []byte("m")
	if _, err := client.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{
		SnapshotUri: testURI, WritePath: link, Files: []string{manifestFile},
	}); err == nil {
		t.Error("FetchSnapshot succeeded through a directory symlink outside --root")
	}
	if _, err := os.Stat(filepath.Join(outside, manifestFile)); !os.IsNotExist(err) {
		t.Errorf("FetchSnapshot created a file outside --root: %v", err)
	}
}

func TestCleanupSnapshot(t *testing.T) {
	ctx := context.Background()
	backend := newMemObjects()
	client := objectstoresnapshotv1.NewControlProviderClient(serve(t, backend, t.TempDir()))
	// uid10 shares a string prefix with uid1, so it survives only if cleanup
	// stops at a path-segment boundary.
	keep := testBucket + "/root/atespaces/team-a/actors/uid10/snapshots/snap1/manifest.json"
	backend.m[testBucket+"/"+testPrefix+"/manifest.json"] = []byte("m")
	backend.m[testBucket+"/"+testPrefix+"/memory.img.zstd"] = []byte("d")
	backend.m[keep] = []byte("other actor")

	// An owner prefix collects every snapshot below it, and nothing beside it.
	owner := "gs://" + testBucket + "/root/atespaces/team-a/actors/uid1"
	if _, err := client.CleanupSnapshot(ctx, &objectstoresnapshotv1.CleanupSnapshotRequest{SnapshotUri: owner}); err != nil {
		t.Fatalf("CleanupSnapshot = %v", err)
	}
	if got := backend.keys(); !equal(got, []string{keep}) {
		t.Errorf("objects left = %v, want only %s", got, keep)
	}
	// Cleaning up again is a no-op.
	if _, err := client.CleanupSnapshot(ctx, &objectstoresnapshotv1.CleanupSnapshotRequest{SnapshotUri: owner}); err != nil {
		t.Errorf("repeated CleanupSnapshot = %v", err)
	}
	// A bare bucket is never a valid prefix.
	for _, uri := range []string{"gs://" + testBucket, "gs://" + testBucket + "/", "not a uri?x=1"} {
		_, err := client.CleanupSnapshot(ctx, &objectstoresnapshotv1.CleanupSnapshotRequest{SnapshotUri: uri})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("CleanupSnapshot(%q) = %v, want InvalidArgument", uri, err)
		}
	}
}

func TestCopySnapshot(t *testing.T) {
	ctx := context.Background()
	backend := newMemObjects()
	client := objectstoresnapshotv1.NewControlProviderClient(serve(t, backend, t.TempDir()))
	backend.m[testBucket+"/"+testPrefix+"/manifest.json"] = []byte("m")
	tag := "gs://" + testBucket + "/root/atespaces/team-a/tags/tag1"

	if _, err := client.CopySnapshot(ctx, &objectstoresnapshotv1.CopySnapshotRequest{SrcUri: testURI, DstUri: tag}); err != nil {
		t.Fatalf("CopySnapshot = %v", err)
	}
	if got := backend.m[testBucket+"/root/atespaces/team-a/tags/tag1/manifest.json"]; string(got) != "m" {
		t.Errorf("copied manifest = %q, want %q", got, "m")
	}

	empty := "gs://" + testBucket + "/root/atespaces/team-a/actors/uid9/snapshots/none"
	_, err := client.CopySnapshot(ctx, &objectstoresnapshotv1.CopySnapshotRequest{SrcUri: empty, DstUri: tag})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CopySnapshot(empty source) = %v, want FailedPrecondition", err)
	}
}

func TestFetchTagSnapshotUsesNodeCache(t *testing.T) {
	ctx := context.Background()
	backend := newMemObjects()
	root := t.TempDir()
	conn := serve(t, backend, root)
	nodeClient := objectstoresnapshotv1.NewNodeProviderClient(conn)
	ctrlClient := objectstoresnapshotv1.NewControlProviderClient(conn)

	src := filepath.Join(root, "src")
	dst1 := filepath.Join(root, "dst1")
	dst2 := filepath.Join(root, "dst2")
	for _, d := range []string{src, dst1, dst2} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	wantMem := bytes.Repeat([]byte("golden-page-"), 4096)
	if err := os.WriteFile(filepath.Join(src, baseIDFile), []byte("uid1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, memoryRangesFile), wantMem, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeClient.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{
		SnapshotUri: testURI, LocalPath: src, Files: []string{baseIDFile, memoryRangesFile},
	}); err != nil {
		t.Fatalf("UploadSnapshot = %v", err)
	}
	tagURI := "gs://" + testBucket + "/root/atespaces/ate-golden/tags/tag1"
	if _, err := ctrlClient.CopySnapshot(ctx, &objectstoresnapshotv1.CopySnapshotRequest{SrcUri: testURI, DstUri: tagURI}); err != nil {
		t.Fatalf("CopySnapshot = %v", err)
	}

	// First fetch from the tag populates the node-local cache.
	if _, err := nodeClient.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{
		SnapshotUri: tagURI, WritePath: dst1, Files: []string{baseIDFile, memoryRangesFile},
	}); err != nil {
		t.Fatalf("first FetchSnapshot(tag) = %v", err)
	}

	// Delete all objects from backend: a second fetch of the same tag must hit the node cache.
	for _, k := range backend.keys() {
		parts := strings.SplitN(k, "/", 2)
		_ = backend.Delete(ctx, parts[0], parts[1])
	}
	if _, err := nodeClient.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{
		SnapshotUri: tagURI, WritePath: dst2, Files: []string{baseIDFile, memoryRangesFile},
	}); err != nil {
		t.Fatalf("second FetchSnapshot(tag) after backend wipe = %v (expected node cache hit)", err)
	}
	got, err := os.ReadFile(filepath.Join(dst2, memoryRangesFile))
	if err != nil || !bytes.Equal(got, wantMem) {
		t.Fatalf("cached memory-ranges mismatch")
	}
	if _, err := os.Stat(filepath.Join(root, "golden-base-cache", "uid1", memoryRangesFile)); err != nil {
		t.Errorf("golden-base-cache not populated: %v", err)
	}
}

func TestFetchActorSnapshotUsesNodeCacheAndEvictsOldGenerations(t *testing.T) {
	t.Setenv("ATE_CACHE_ACTOR_SNAPSHOTS", "true")
	ctx := context.Background()
	backend := newMemObjects()
	root := t.TempDir()
	nodeClient := objectstoresnapshotv1.NewNodeProviderClient(serve(t, backend, root))

	src1 := filepath.Join(root, "src1")
	src2 := filepath.Join(root, "src2")
	dst1 := filepath.Join(root, "dst1")
	dst2 := filepath.Join(root, "dst2")
	for _, d := range []string{src1, src2, dst1, dst2} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	snap1URI := "gs://" + testBucket + "/root/atespaces/team-a/actors/uid1/snapshots/snap1"
	snap2URI := "gs://" + testBucket + "/root/atespaces/team-a/actors/uid1/snapshots/snap2"

	mem1 := bytes.Repeat([]byte("actor-gen1-"), 1024)
	manifest1 := []byte(`{"snapshotFiles":["memory-ranges"]}`)
	if err := os.WriteFile(filepath.Join(src1, memoryRangesFile), mem1, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src1, manifestFile), manifest1, 0o600); err != nil {
		t.Fatal(err)
	}

	// Upload in two calls (data files then manifest), matching atelet's sequence.
	if _, err := nodeClient.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{
		SnapshotUri: snap1URI, LocalPath: src1, Files: []string{memoryRangesFile},
	}); err != nil {
		t.Fatalf("UploadSnapshot(snap1 data) = %v", err)
	}
	if _, err := nodeClient.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{
		SnapshotUri: snap1URI, LocalPath: src1, Files: []string{manifestFile},
	}); err != nil {
		t.Fatalf("UploadSnapshot(snap1 manifest) = %v", err)
	}

	// Wipe backend: FetchSnapshot for snap1 must still succeed from local actor-snapshot-cache.
	for _, k := range backend.keys() {
		parts := strings.SplitN(k, "/", 2)
		_ = backend.Delete(ctx, parts[0], parts[1])
	}

	if _, err := nodeClient.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{
		SnapshotUri: snap1URI, WritePath: dst1, Files: []string{manifestFile},
	}); err != nil {
		t.Fatalf("FetchSnapshot(snap1 manifest) after backend wipe = %v", err)
	}
	if _, err := nodeClient.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{
		SnapshotUri: snap1URI, WritePath: dst1, Files: []string{memoryRangesFile},
	}); err != nil {
		t.Fatalf("FetchSnapshot(snap1 data) after backend wipe = %v", err)
	}
	got1, err := os.ReadFile(filepath.Join(dst1, memoryRangesFile))
	if err != nil || !bytes.Equal(got1, mem1) {
		t.Fatalf("cached snap1 memory-ranges mismatch")
	}

	// Upload snap2 for the same actor: snap1 must be evicted from actor-snapshot-cache.
	mem2 := bytes.Repeat([]byte("actor-gen2-"), 1024)
	if err := os.WriteFile(filepath.Join(src2, memoryRangesFile), mem2, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src2, manifestFile), manifest1, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeClient.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{
		SnapshotUri: snap2URI, LocalPath: src2, Files: []string{memoryRangesFile},
	}); err != nil {
		t.Fatalf("UploadSnapshot(snap2 data) = %v", err)
	}
	if _, err := nodeClient.UploadSnapshot(ctx, &objectstoresnapshotv1.UploadSnapshotRequest{
		SnapshotUri: snap2URI, LocalPath: src2, Files: []string{manifestFile},
	}); err != nil {
		t.Fatalf("UploadSnapshot(snap2 manifest) = %v", err)
	}

	// Wipe backend again: snap1 should now be evicted (NotFound), while snap2 hits the cache.
	for _, k := range backend.keys() {
		parts := strings.SplitN(k, "/", 2)
		_ = backend.Delete(ctx, parts[0], parts[1])
	}
	if _, err := nodeClient.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{
		SnapshotUri: snap1URI, WritePath: dst2, Files: []string{memoryRangesFile},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("FetchSnapshot(evicted snap1) = %v, want NotFound", err)
	}
	if _, err := nodeClient.FetchSnapshot(ctx, &objectstoresnapshotv1.FetchSnapshotRequest{
		SnapshotUri: snap2URI, WritePath: dst2, Files: []string{memoryRangesFile},
	}); err != nil {
		t.Fatalf("FetchSnapshot(snap2) after backend wipe = %v", err)
	}
	got2, err := os.ReadFile(filepath.Join(dst2, memoryRangesFile))
	if err != nil || !bytes.Equal(got2, mem2) {
		t.Fatalf("cached snap2 memory-ranges mismatch")
	}
}

func equal(a, b []string) bool {
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
