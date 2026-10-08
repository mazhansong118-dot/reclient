//go:build linux

package depsscannerclient

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/bazelbuild/reclient/api/scandeps"
	"github.com/bazelbuild/reclient/internal/pkg/subprocess"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestLifecycleHelperProcess is also the real child executable in these tests.
// It only listens on the test-owned private Unix socket, not an RBE endpoint.
func TestLifecycleHelperProcess(t *testing.T) {
	if os.Getenv("RBE_LIFECYCLE_TEST_CHILD") != "1" {
		return
	}
	var addr, pidFile string
	for i, arg := range os.Args {
		if i+1 >= len(os.Args) {
			break
		}
		switch arg {
		case "--server_address":
			addr = strings.TrimPrefix(os.Args[i+1], "unix://")
		case "--pid_file":
			pidFile = os.Args[i+1]
		}
	}
	l, err := net.Listen("unix", addr)
	if err != nil {
		os.Exit(21)
	}
	defer l.Close()
	if err = os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		os.Exit(22)
	}
	for {
		c, err := l.Accept()
		if err != nil {
			os.Exit(23)
		}
		c.SetReadDeadline(time.Now().Add(time.Second))
		var b [1]byte
		n, _ := c.Read(b[:])
		c.Close()
		if n == 1 && b[0] == 'q' {
			return
		}
	}
}

type realChildExecutor struct{ pidFile string }

func (e *realChildExecutor) ExecuteInBackground(ctx context.Context, cmd *command.Command, oe outerr.OutErr, ch chan *command.Result) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	addr := cmd.Args[2]
	cmd.Args = []string{self, "-test.run=^TestLifecycleHelperProcess$", "--", "--server_address", addr, "--pid_file", e.pidFile}
	cmd.InputSpec.EnvironmentVariables = map[string]string{"RBE_LIFECYCLE_TEST_CHILD": "1", "GOMAXPROCS": "2"}
	return (subprocess.SystemExecutor{}).ExecuteInBackground(ctx, cmd, oe, ch)
}

func waitChildReady(ctx context.Context, pidFile string) (int, error) {
	for {
		if b, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(string(b)); err == nil {
				return pid, nil
			}
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

type socketShutdownClient struct {
	*stubClient
	address string
}

func (c *socketShutdownClient) Shutdown(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*pb.StatusResponse, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", strings.TrimPrefix(c.address, "unix://"))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetWriteDeadline(time.Now().Add(time.Second))
	_, err = conn.Write([]byte("q"))
	return &pb.StatusResponse{}, err
}

func TestLifecycleRealChildFailureReapedBeforeRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var prevAddress string
	for attempt := 0; attempt < 2; attempt++ {
		e := &realChildExecutor{pidFile: filepath.Join(t.TempDir(), "pid")}
		var pid int
		var address string
		connect := func(ctx context.Context, a string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
			var err error
			pid, err = waitChildReady(ctx, e.pidFile)
			address = a
			if err != nil {
				return nil, nil, err
			}
			return nil, nil, status.Error(codes.Unavailable, "injected after real child bound its private socket")
		}
		_, err := New(ctx, e, t.TempDir(), 1, false, "", "exec://test-real-child", "unix:///tmp/reproxy-shared-test.sock", 5*time.Second, connect)
		if !CanRetryStartup(err) {
			t.Fatalf("expected reaped transient failure, got %v", err)
		}
		if pid <= 0 {
			t.Fatal("child never became ready")
		}
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !os.IsNotExist(err) {
			t.Fatalf("child %d not reaped before return: %v", pid, err)
		}
		if _, err := os.Stat(filepath.Dir(strings.TrimPrefix(address, "unix://"))); !os.IsNotExist(err) {
			t.Fatalf("owned socket directory remains: %v", err)
		}
		if prevAddress == address {
			t.Fatal("retry reused previous attempt's address")
		}
		prevAddress = address
	}
}

func TestLifecycleRealChildNormalShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	e := &realChildExecutor{pidFile: filepath.Join(t.TempDir(), "pid")}
	var pid int
	connect := func(ctx context.Context, a string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
		var err error
		pid, err = waitChildReady(ctx, e.pidFile)
		if err != nil {
			return nil, nil, err
		}
		return &socketShutdownClient{stubClient: &stubClient{}, address: a}, &pb.CapabilitiesResponse{}, nil
	}
	c, err := New(ctx, e, t.TempDir(), 1, false, "", "exec://test-real-child", "unix:///tmp/reproxy-shared-test.sock", 5*time.Second, connect)
	if err != nil {
		t.Fatal(err)
	}
	dir := c.ownedSocketDir
	c.Close()
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !os.IsNotExist(err) {
		t.Fatalf("child not reaped: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("socket directory remains: %v", err)
	}
}

func TestLifecycleRealChildrenIndependent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir, err := os.MkdirTemp("", "rbe-foreign-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(dir)
	legacy := filepath.Join(dir, "depscan.sock")
	foreign, err := net.Listen("unix", legacy)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	proxy := "unix://" + filepath.Join(dir, "reproxy.sock")
	var clients []*DepsScannerClient
	var pids []int
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		e := &realChildExecutor{pidFile: filepath.Join(t.TempDir(), "pid")}
		var pid int
		connect := func(ctx context.Context, a string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error) {
			var err error
			pid, err = waitChildReady(ctx, e.pidFile)
			if err != nil {
				return nil, nil, err
			}
			return &socketShutdownClient{stubClient: &stubClient{}, address: a}, &pb.CapabilitiesResponse{}, nil
		}
		c, err := New(ctx, e, t.TempDir(), 1, false, "", "exec://test-real-child", proxy, 5*time.Second, connect)
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
		pids = append(pids, pid)
	}
	if clients[0].address == clients[1].address {
		t.Fatal("shared endpoint")
	}
	clients[0].Close()
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pids[1])); err != nil {
		t.Fatalf("closing first client affected second child: %v", err)
	}
	if _, err := os.Stat(legacy); err != nil {
		t.Fatalf("foreign listener was unlinked: %v", err)
	}
	clients[1].Close()
	for _, pid := range pids {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !os.IsNotExist(err) {
			t.Fatalf("child %d remains: %v", pid, err)
		}
	}
}
