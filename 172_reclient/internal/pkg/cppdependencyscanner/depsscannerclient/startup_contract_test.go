package depsscannerclient

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/bazelbuild/reclient/api/scandeps"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// contractExecutor does not spawn an OS process. It models a child whose exit
// result must be consumed after cancellation. Cleanup never signals a real task.
type contractExecutor struct {
	ctx     context.Context
	exited  chan struct{}
	release chan struct{}
}

func TestStartupContractCleanupTimeoutBlocksRetry(t *testing.T) {
	e := &stubExecutor{} // Started, but never supplies an exit acknowledgement.
	connect := func(context.Context, string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		return nil, nil, status.Error(codes.Unavailable, "injected transient failure")
	}
	_, err := New(context.Background(), e, t.TempDir(), 1, false, "", "exec://test-only-no-process", "unix:///tmp/reproxy-contract.sock", time.Second, connect)
	if !errors.Is(err, ErrStartupCleanup) || CanRetryStartup(err) {
		t.Fatalf("unconfirmed cleanup must block retry: %v", err)
	}
	path := strings.TrimPrefix(e.cmd.Args[2], "unix://")
	if _, statErr := os.Stat(filepath.Dir(path)); statErr != nil {
		t.Errorf("unconfirmed child's private endpoint must be preserved: %v", statErr)
	}
	// This executor never started an OS process; only remove its own empty test dir.
	if err := os.Remove(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	select {
	case e.ch <- command.NewResultFromExitCode(1):
	default:
		t.Fatal("late exit result must not block executor after cleanup timeout")
	}
}

type acknowledgedExecutor struct {
	ctx     context.Context
	result  chan *command.Result
	started chan struct{}
}

func (e *acknowledgedExecutor) ExecuteInBackground(ctx context.Context, _ *command.Command, _ outerr.OutErr, ch chan *command.Result) error {
	e.ctx, e.result = ctx, ch
	close(e.started)
	return nil
}

func TestStartupContractWaitsForExitAcknowledgement(t *testing.T) {
	e := &acknowledgedExecutor{started: make(chan struct{})}
	returned := make(chan error, 1)
	connect := func(context.Context, string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		return nil, nil, status.Error(codes.Unavailable, "retry only after reap")
	}
	go func() {
		_, err := New(context.Background(), e, "", 1, false, "", "exec://test-only-no-process", "unix:///tmp/reproxy-contract.sock", time.Second, connect)
		returned <- err
	}()
	<-e.started
	select {
	case <-e.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("child context was not canceled")
	}
	select {
	case err := <-returned:
		t.Fatalf("returned before acknowledgement: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	e.result <- command.NewResultFromExitCode(1)
	if err := <-returned; !CanRetryStartup(err) {
		t.Fatalf("confirmed exit should permit transient retry: %v", err)
	}
}

func TestStartupContractParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &contractExecutor{exited: make(chan struct{}), release: make(chan struct{})}
	defer close(e.release)
	defer cancel()
	connect := func(ctx context.Context, _ string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		cancel()
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	_, err := New(ctx, e, "", 1, false, "", "exec://test-only-no-process", "unix:///tmp/reproxy-contract.sock", time.Second, connect)
	if !errors.Is(err, context.Canceled) || CanRetryStartup(err) {
		t.Fatalf("canceled caller must not be retried: %v", err)
	}
	select {
	case <-e.exited:
	case <-time.After(time.Second):
		t.Fatal("canceled parent's child not reaped")
	}
}

type resultContractExecutor struct{ result *command.Result }

func (e *resultContractExecutor) ExecuteInBackground(_ context.Context, _ *command.Command, _ outerr.OutErr, ch chan *command.Result) error {
	ch <- e.result
	close(ch)
	return nil
}

func TestStartupContractExitResults(t *testing.T) {
	for _, code := range []int{0, 1, -1} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			connect := func(ctx context.Context, _ string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
				<-ctx.Done()
				return nil, nil, ctx.Err()
			}
			_, err := New(context.Background(), &resultContractExecutor{result: command.NewResultFromExitCode(code)}, "", 1, false, "", "exec://test-only-no-process", "unix:///tmp/reproxy-contract.sock", time.Second, connect)
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("exit_code=%d", code)) || CanRetryStartup(err) || errors.Is(err, ErrStartupCleanup) {
				t.Fatalf("unexpected early-exit classification: %v", err)
			}
		})
	}
	err := startupExitError("test", nil)
	if strings.Contains(err.Error(), "%!w") {
		t.Fatal(err)
	}
	sentinel := errors.New("wrapped result")
	if !errors.Is(startupExitError("test", &command.Result{Err: sentinel}), sentinel) {
		t.Fatal("lost wrapped exit error")
	}
}

func TestStartupContractPrivateEndpointsAndClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	legacy := filepath.Join(t.TempDir(), "depscan.sock")
	if err := os.WriteFile(legacy, []byte("foreign-owned"), 0600); err != nil {
		t.Fatal(err)
	}
	proxy := "unix://" + strings.Replace(legacy, "depscan", "reproxy", 1)
	var clients []*DepsScannerClient
	var executors []*stubExecutor
	for i := 0; i < 2; i++ {
		e := &stubExecutor{}
		s := &testService{stubClient: &stubClient{}}
		c, err := New(ctx, e, "", 1, false, "", "exec://test-only-no-process", proxy, time.Second, s.connect)
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
		executors = append(executors, e)
	}
	if clients[0].address == clients[1].address {
		t.Fatal("independent clients share an endpoint")
	}
	for i, c := range clients {
		dir := c.ownedSocketDir
		if dir == "" {
			t.Fatal("missing private directory")
		}
		if st, err := os.Stat(dir); err != nil || st.Mode().Perm() != 0700 {
			t.Fatalf("private dir permissions: %v %v", st, err)
		}
		executors[i].ch <- command.NewResultFromExitCode(0)
		c.Close()
		c.Close()
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("owned directory not released: %v", err)
		}
	}
	if b, err := os.ReadFile(legacy); err != nil || string(b) != "foreign-owned" {
		t.Fatalf("foreign endpoint changed: %v", err)
	}
}

func TestStartupContractPermanentConnectionErrorNoRetry(t *testing.T) {
	for _, code := range []codes.Code{codes.PermissionDenied, codes.InvalidArgument, codes.Unavailable, codes.DeadlineExceeded} {
		connect := func(context.Context, string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
			return nil, nil, status.Error(code, "injected")
		}
		_, err := New(context.Background(), nil, "", 1, false, "", "127.0.0.1:1", "127.0.0.1:2", time.Second, connect)
		want := code == codes.Unavailable || code == codes.DeadlineExceeded
		if CanRetryStartup(err) != want {
			t.Fatalf("code %v retry=%v want=%v", code, CanRetryStartup(err), want)
		}
	}
}

func TestStartupContractCanceledReadyNeverSucceeds(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		connect := func(context.Context, string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
			cancel()
			return &stubClient{}, &pb.CapabilitiesResponse{}, nil
		}
		c, err := New(ctx, nil, "", 0, false, "", "127.0.0.1:1", "127.0.0.1:0", time.Second, connect)
		cancel()
		if c != nil || !errors.Is(err, context.Canceled) || CanRetryStartup(err) {
			t.Fatalf("canceled readiness accepted: c=%v err=%v", c, err)
		}
	}
}

func TestStartupContractNilClientIsFailure(t *testing.T) {
	connect := func(context.Context, string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		return nil, nil, nil
	}
	c, err := New(context.Background(), nil, "", 0, false, "", "127.0.0.1:1", "127.0.0.1:0", time.Second, connect)
	if c != nil || err == nil || CanRetryStartup(err) {
		t.Fatalf("invalid ready response accepted: %v %v", c, err)
	}
}

func (e *contractExecutor) ExecuteInBackground(ctx context.Context, _ *command.Command, _ outerr.OutErr, result chan *command.Result) error {
	e.ctx = ctx
	go func() {
		<-ctx.Done()
		select {
		case result <- &command.Result{ExitCode: 0}:
		case <-e.release:
		}
		close(e.exited)
	}()
	return nil
}

func TestStartupContractConnectErrorCancelsOwnedService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &contractExecutor{exited: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		cancel()
		close(e.release)
		select {
		case <-e.exited:
		case <-time.After(time.Second):
			t.Error("test executor goroutine did not exit")
		}
	})
	connectErr := errors.New("injected connection failure")
	connect := func(context.Context, string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		return nil, nil, connectErr
	}
	_, err := New(ctx, e, t.TempDir(), 1, false, "", "exec://test-only-no-process", "unix://"+filepath.Join(t.TempDir(), "rp.sock"), time.Second, connect)
	if !errors.Is(err, connectErr) {
		t.Fatalf("New error = %v, want wrapped connect failure", err)
	}
	if e.ctx.Err() == nil {
		t.Fatal("New returned after connect failure with owned service context still live: caller retry can overlap the old child")
	}
}

type earlyExitContractExecutor struct{}

func (*earlyExitContractExecutor) ExecuteInBackground(ctx context.Context, _ *command.Command, _ outerr.OutErr, result chan *command.Result) error {
	go func() {
		select {
		case result <- &command.Result{ExitCode: 0}:
		case <-ctx.Done():
		}
	}()
	return nil
}

func TestStartupContractEarlyExitIncludesExitCode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connect := func(ctx context.Context, _ string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	_, err := New(ctx, &earlyExitContractExecutor{}, t.TempDir(), 1, false, "", "exec://test-only-no-process", "unix://"+filepath.Join(t.TempDir(), "rp.sock"), time.Second, connect)
	if err == nil {
		t.Fatal("exit before readiness must remain an error even when exit code is zero")
	}
	if strings.Contains(err.Error(), "%!w") || !strings.Contains(err.Error(), "exit_code=0") {
		t.Fatalf("startup error loses process result: %v", err)
	}
}
