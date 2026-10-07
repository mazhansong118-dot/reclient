// Copyright 2026 reclient contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package depsscannerclient

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/bazelbuild/reclient/api/scandeps"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// No OS process is spawned. The mock acknowledges cancellation with an exit
// result, allowing the same contract tests to run on Windows without an RBE.
type codexDemoExecutor struct {
	child     context.Context
	earlyExit bool
}

func (e *codexDemoExecutor) ExecuteInBackground(ctx context.Context, _ *command.Command, _ outerr.OutErr, ch chan *command.Result) error {
	e.child = ctx
	go func() {
		if !e.earlyExit {
			<-ctx.Done()
		}
		select {
		case ch <- command.NewResultFromExitCode(0):
		case <-time.After(time.Second):
		}
	}()
	return nil
}

func TestCodexDemoConnectionFailureCancelsChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	executor := &codexDemoExecutor{}
	connect := func(context.Context, string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		return nil, nil, status.Error(codes.Unavailable, "TEST_INJECTED: handshake failure")
	}
	client, err := New(ctx, executor, t.TempDir(), 1, false, "", "exec://codex-demo-mock", "127.0.0.1:0", time.Second, connect)
	if client != nil || err == nil {
		t.Fatalf("expected startup failure, got client=%v error=%v", client, err)
	}
	if executor.child == nil {
		t.Fatal("mock executor did not receive the child context")
	}
	select {
	case <-executor.child.Done():
	default:
		t.Fatal("startup returned an error while its child context remained live")
	}
}

func TestCodexDemoEarlyExitPreservesExitCode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	executor := &codexDemoExecutor{earlyExit: true}
	connect := func(ctx context.Context, _ string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	_, err := New(ctx, executor, t.TempDir(), 1, false, "", "exec://codex-demo-mock", "127.0.0.1:0", 5*time.Second, connect)
	if err == nil {
		t.Fatal("child exited before readiness but startup reported success")
	}
	if strings.Contains(err.Error(), "%!w") || !strings.Contains(err.Error(), "exit_code=0") {
		t.Fatalf("early-exit diagnostic must retain exit code without wrapping nil: %v", err)
	}
}

func TestCodexDemoNilConnectionIsNotReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connect := func(context.Context, string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		return nil, &pb.CapabilitiesResponse{}, nil
	}
	client, err := New(ctx, nil, "", 1, false, "", "127.0.0.1:1", "127.0.0.1:0", time.Second, connect)
	if client != nil || err == nil {
		t.Fatalf("nil connection must not become a ready client: client=%v error=%v", client, err)
	}
}
