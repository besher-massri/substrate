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

package kata

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// startFakeAgent serves grpc.AgentService/ReseedRandomDev with handle and returns
// a client connected to it.
func startFakeAgent(t *testing.T, handle func(*agentpb.ReseedRandomDevRequest) error) *AgentClient {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "agent.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv, err := ttrpc.NewServer()
	if err != nil {
		t.Fatalf("ttrpc.NewServer: %v", err)
	}
	srv.Register("grpc.AgentService", map[string]ttrpc.Method{
		"ReseedRandomDev": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.ReseedRandomDevRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			if err := handle(&req); err != nil {
				return nil, err
			}
			return &emptypb.Empty{}, nil
		},
	})
	go func() { _ = srv.Serve(context.Background(), l) }()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ac := &AgentClient{conn: conn, client: ttrpc.NewClient(conn)}
	t.Cleanup(func() { _ = ac.Close() })
	return ac
}

// The restore path fails when this call fails, so a wrong service or method name
// here would fail every micro-VM restore.
func TestReseedRandomDevSendsData(t *testing.T) {
	got := make(chan []byte, 1)
	ac := startFakeAgent(t, func(req *agentpb.ReseedRandomDevRequest) error {
		got <- req.GetData()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	want := []byte("per-restore nonce")
	if err := ac.ReseedRandomDev(ctx, want); err != nil {
		t.Fatalf("ReseedRandomDev: %v", err)
	}
	select {
	case data := <-got:
		if !bytes.Equal(data, want) {
			t.Errorf("agent got data %q, want %q", data, want)
		}
	default:
		t.Fatal("agent never received ReseedRandomDev")
	}
}

func TestReseedRandomDevReturnsAgentError(t *testing.T) {
	ac := startFakeAgent(t, func(*agentpb.ReseedRandomDevRequest) error {
		return errors.New("reseed refused")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ac.ReseedRandomDev(ctx, []byte("nonce")); err == nil {
		t.Fatal("ReseedRandomDev returned nil for an agent error, want the error")
	}
}

// A per-attempt context deadline cancels the ttrpc request stream, not the
// underlying vsock connection, so the next reseed attempt can reuse the same
// AgentClient without redialing.
func TestReseedRandomDevSucceedsOnSameClientAfterStreamTimeout(t *testing.T) {
	firstBlocked := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls int
	ac := startFakeAgent(t, func(req *agentpb.ReseedRandomDevRequest) error {
		calls++
		if calls == 1 {
			close(firstBlocked)
			<-releaseFirst
		}
		return nil
	})
	t.Cleanup(func() {
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
	})

	shortCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := ac.ReseedRandomDev(shortCtx, []byte("attempt-1"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first ReseedRandomDev = %v, want DeadlineExceeded", err)
	}
	<-firstBlocked
	close(releaseFirst)

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer retryCancel()
	if err := ac.ReseedRandomDev(retryCtx, []byte("attempt-2")); err != nil {
		t.Fatalf("second ReseedRandomDev on same client: %v", err)
	}
}

