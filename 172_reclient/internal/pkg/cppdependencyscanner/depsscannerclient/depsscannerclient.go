// Copyright 2023 Google LLC
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

// Package depsscannerclient implements the cppdependencyscanner.DepsScanner with gRPC
package depsscannerclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bazelbuild/reclient/internal/pkg/diagnostics"
	"github.com/bazelbuild/reclient/internal/pkg/ipc"

	pb "github.com/bazelbuild/reclient/api/scandeps"

	log "github.com/golang/glog"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// Executor can run commands and retrieve their outputs.
type executor interface {
	ExecuteInBackground(ctx context.Context, cmd *command.Command, oe outerr.OutErr, ch chan *command.Result) error
}

// Implements the outerr.OutErr interface to log stdout and stderr from a background process.
// Note that the dependency scanner service has its own logging and this output is expected to be
// empty except when a failure occurs.
type depsScannerLogger struct{}

// Log stdout lines from dependency scanner service to reproxy.INFO
func (l *depsScannerLogger) WriteOut(s []byte) {
	log.Infof("depsscannerclient stdout: %s", string(s[:]))
}

// Log stderr lines from dependency scanner service to reproxy.ERROR
func (l *depsScannerLogger) WriteErr(s []byte) {
	log.Errorf("depsscannerclient stderr: %s", string(s[:]))
}

const (
	// gRPC C++ library does not support named pipes on Windows.
	// Named pipes are supported on Linux and Mac but for consistency we will use TCP sockets for all.
	localhost = "127.0.0.1"
	// 60 seconds to try and shut down gracefully, 10 seconds to hard kill
	shutdownTimeout        = 60 * time.Second
	startupShutdownTimeout = time.Second
)

// DepsScannerClient wraps the dependency scanner gRPC client.
type DepsScannerClient struct {
	ctx              context.Context
	terminate        context.CancelFunc
	address          string
	executable       string
	cacheDir         string
	cacheFileMaxMb   int
	useDepsCache     bool
	logDir           string
	client           pb.CPPDepsScannerClient
	executor         executor
	oe               outerr.OutErr
	ch               chan *command.Result
	serviceStopped   bool
	ownedSocketDir   string
	serviceRestarted time.Time
	m                sync.Mutex
	capabilities     *pb.CapabilitiesResponse
	capabilitiesMu   sync.RWMutex
}

var (
	backoff     = retry.ExponentialBackoff(1*time.Second, 10*time.Second, retry.Attempts(10))
	shouldRetry = func(err error) bool {
		if err == context.DeadlineExceeded {
			return true
		}
		s, ok := status.FromError(err)
		if !ok {
			return false
		}
		switch s.Code() {
		case codes.Canceled, codes.Unknown, codes.DeadlineExceeded, codes.Unavailable, codes.ResourceExhausted:
			return true
		default:
			return false
		}
	}
)

type connectFn func(ctx context.Context, address string) (pb.CPPDepsScannerClient, *pb.CapabilitiesResponse, error)

// ErrStartupCleanup means that exit of an owned child could not be confirmed.
// Starting a replacement in this state can overlap the original process.
var ErrStartupCleanup = errors.New("dependency scanner startup cleanup incomplete")

type startupError struct {
	err       error
	retryable bool
}

func (e *startupError) Error() string { return e.err.Error() }
func (e *startupError) Unwrap() error { return e.err }

// CanRetryStartup only permits transient connection failures whose owned child
// has been reaped (or where no child was owned). Other failures are not retried.
func CanRetryStartup(err error) bool {
	var e *startupError
	return errors.As(err, &e) && e.retryable && !errors.Is(err, ErrStartupCleanup)
}

// New creates new DepsScannerClient.
func New(ctx context.Context, executor executor, cacheDir string, cacheFileMaxMb int, useDepsCache bool, logDir string, depsScannerAddress, proxyServerAddress string, connTimeout time.Duration, connect connectFn) (client *DepsScannerClient, err error) {
	var addr string
	defer func() {
		if err == nil {
			return
		}
		log.Infof("Found errors in scandeps startup, running post-cleanup diagnostics; a removed owned endpoint is expected...")
		cCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		in := &diagnostics.DiagnosticInputs{
			UDSAddr: addr,
		}
		diagnostics.Run(cCtx, in)
	}()

	log.Infof("Connecting to remote dependency scanner: %v", depsScannerAddress)
	addr = depsScannerAddress
	client = &DepsScannerClient{
		address:        depsScannerAddress,
		executor:       executor,
		cacheDir:       cacheDir,
		logDir:         logDir,
		cacheFileMaxMb: cacheFileMaxMb,
		useDepsCache:   useDepsCache,
		// TODO (b/260707840): context shouldn't be a member variable. Pass in as function variable elsewhere and remote this.
		ctx: ctx,
	}
	// Capture the allocated client: named return client becomes nil on failure.
	ds := client
	startupTime := time.Now()
	started, retryable := false, false
	defer func() {
		if err == nil {
			return // Ownership transfers to the caller only after readiness.
		}
		if started {
			if cleanupErr := ds.stopService(startupShutdownTimeout); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("%w: %v", ErrStartupCleanup, cleanupErr))
				// Preserve the private endpoint for diagnosis; never unlink a live child.
				err = &startupError{err: err}
				log.Errorf("scandeps_startup phase=cleanup_unconfirmed retry_allowed=false address=%q elapsed_ms=%d error=%v", ds.address, time.Since(startupTime).Milliseconds(), cleanupErr)
				return
			}
		}
		if ds.terminate != nil {
			ds.terminate()
		}
		if cleanupErr := ds.releaseOwnedSocket(); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: %v", ErrStartupCleanup, cleanupErr))
		}
		err = &startupError{err: err, retryable: retryable}
		log.Warningf("scandeps_startup phase=failed child_started=%t exit_confirmed=%t retry_allowed=%t address=%q elapsed_ms=%d error=%v", started, ds.serviceStopped, CanRetryStartup(err), ds.address, time.Since(startupTime).Milliseconds(), err)
	}()

	if strings.HasPrefix(depsScannerAddress, "exec://") {
		executable := depsScannerAddress[7:]
		addr, ds.ownedSocketDir, err = buildOwnedAddress(proxyServerAddress)
		if err != nil {
			return nil, fmt.Errorf("Failed to build address for dependency scanner: %w", err)
		}
		client.address = addr
		if err = client.startService(ctx, executable); err != nil {
			return nil, fmt.Errorf("Failed to start dependency scanner: %w", err)
		}
		started = true
	}

	type connectResponse struct {
		client       pb.CPPDepsScannerClient
		capabilities *pb.CapabilitiesResponse
		err          error
	}
	// A single buffered response avoids blocking the connector after cancellation.
	// Do not close this channel without a result: a closed channel looks like a
	// successful zero-value response in the readiness select below.
	connectCh := make(chan connectResponse, 1)
	ctx, cancel := context.WithTimeout(ctx, connTimeout)
	defer cancel()
	go func() {
		client, capabilities, err := connect(ctx, addr)
		select {
		case connectCh <- connectResponse{
			client:       client,
			capabilities: capabilities,
			err:          err,
		}:
		case <-ctx.Done():
		}
	}()
	select {
	case <-ctx.Done():
		retryable = errors.Is(ctx.Err(), context.DeadlineExceeded)
		return nil, fmt.Errorf("Failed to connect to dependency scanner service after %v seconds: %w", connTimeout.Seconds(), ctx.Err())
	case result := <-client.ch:
		// The exit result has already been consumed; cleanup must not wait twice.
		client.serviceStopped = result != nil
		return nil, startupExitError(client.executable, result)
	case c := <-connectCh:
		if ctx.Err() != nil {
			retryable = errors.Is(ctx.Err(), context.DeadlineExceeded)
			return nil, fmt.Errorf("dependency scanner startup canceled before readiness: %w", ctx.Err())
		}
		if c.err != nil {
			retryable = errors.Is(c.err, context.DeadlineExceeded) || shouldRetry(c.err)
			return nil, fmt.Errorf("Failed to connect to dependency scanner service: %w", c.err)
		}
		if c.client == nil {
			return nil, errors.New("dependency scanner connector returned no client")
		}
		select {
		case result := <-client.ch:
			client.serviceStopped = result != nil
			return nil, startupExitError(client.executable, result)
		default:
		}
		log.Infof("Connected to dependency scanner service on %v", client.address)
		client.client = c.client
		client.updateCapabilities(c.capabilities)
		log.Infof("scandeps_startup phase=ready address=%q elapsed_ms=%d", ds.address, time.Since(startupTime).Milliseconds())
		return client, nil
	}
}

func startupExitError(executable string, result *command.Result) error {
	if result == nil {
		return fmt.Errorf("%s terminated during startup: process result unavailable", executable)
	}
	if result.Err != nil {
		return fmt.Errorf("%s terminated during startup (exit_code=%d): %w", executable, result.ExitCode, result.Err)
	}
	return fmt.Errorf("%s terminated during startup (exit_code=%d): process exited before readiness", executable, result.ExitCode)
}

// Each constructor attempt owns a private directory, not a shared derived socket
// name. In-place restarts retain this address only after stopService confirms exit.
func buildOwnedAddress(proxyAddress string) (address, dir string, err error) {
	if ipc.GrpcCxxSupportsUDS && (strings.HasPrefix(proxyAddress, "unix://") || strings.HasPrefix(proxyAddress, "pipe://")) {
		dir, err = os.MkdirTemp("", "reclient-ds-")
		if err != nil {
			return "", "", err
		}
		path := filepath.Join(dir, "ds.sock")
		if len(path) < 108 {
			if runtime.GOOS == "windows" {
				return "unix:" + path, dir, nil
			}
			return "unix://" + path, dir, nil
		}
		if err := os.Remove(dir); err != nil {
			return "", "", err
		}
		proxyAddress = localhost + ":0"
	}
	address, err = buildAddress(proxyAddress, findOpenPort)
	return address, "", err
}

func (ds *DepsScannerClient) releaseOwnedSocket() error {
	if ds.ownedSocketDir == "" {
		return nil
	}
	// Never recursively remove a directory, or touch externally supplied endpoints.
	if err := os.Remove(filepath.Join(ds.ownedSocketDir, "ds.sock")); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(ds.ownedSocketDir); err != nil && !os.IsNotExist(err) {
		return err
	}
	ds.ownedSocketDir = ""
	return nil
}

// buildAddress generates an address for the depsscanner process to listen on.
// The path is constructed as:
//
//	a) If platform is UNIX & using UDS for rewrapper <-> reproxy
//	   comms (e.g., /tmp/reproxy.sock), socket filename would be
//	   unix:///tmp/ds_depsscan.sock.
//	b) If platform is Windows & using named pipe for rewrapper <-> reproxy
//	   comms (e.g., pipe://\\.pipe\reproxy.pipe), socket filename would be
//	   unix:<tmp-dir>\ds_<randnum>.sock (e.g., unix:c:\src\temp/ds_123.sock).
//
// If the socket path in (a) or (b) exceeds 108 characters, TCP ports will be used.
func buildAddress(proxyServerAddress string, openPortFunc func() (int, error)) (string, error) {
	address := proxyServerAddress
	if ipc.GrpcCxxSupportsUDS {
		var socketFilePath string
		switch {
		case strings.HasPrefix(address, "unix://"):
			if strings.Contains(address, "reproxy") {
				return strings.Replace(address, "reproxy", "depscan", -1), nil
			}
			dir, sockFileName := filepath.Split(address[len("unix://"):])
			socketFilePath = filepath.Join(dir, "ds_"+sockFileName)

		case strings.HasPrefix(address, "pipe://"):
			sockFileName := fmt.Sprintf("ds_%v.sock", rand.Intn(1000))
			socketFilePath = filepath.Join(os.TempDir(), sockFileName)
		}

		sfLen := len(socketFilePath)
		if sfLen > 0 && sfLen <= 108 {
			if runtime.GOOS == "windows" {
				// On Windows, for grpc C++, unix:// style socket file paths
				// dont seem to work (i.e., the dependency scanner server in C++
				// is not able to listen on that path).
				return fmt.Sprintf("unix:%s", socketFilePath), nil
			}
			return fmt.Sprintf("unix://%s", socketFilePath), nil
		}
		if sfLen > 0 {
			log.Warningf("buildAddress(%v), constructed socket file path exceeds 108 characters (%v). Falling back to using TCP ports.", proxyServerAddress, socketFilePath)
		}
	}
	if strings.HasPrefix(address, "unix://") || strings.HasPrefix(address, "pipe://") {
		address = fmt.Sprintf("%s:0", localhost)
	}
	base, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("failed to find base address: %w", err)
	}
	port, err := openPortFunc()
	if err != nil {
		return "", fmt.Errorf("failed to find open port: %w", err)
	}
	return fmt.Sprintf("%s:%d", base, port), nil
}

// findOpenPort finds an open port by resolving 127.0.0.1:0 which the kernel resolves
// to an unused free port. It then opens a tcp server and client on that port and sends 1
// byte before closing to ensure the kernel sets the port to TIME-WAIT to prevent non
// subprocesses from listening on this port for 60s.
// Inspired by https://github.com/Yelp/ephemeral-port-reserve
func findOpenPort() (int, error) {
	// Listen on port 0 because by convention kernels will resolve it to an unsused free port
	// see https://linux.die.net/man/7/ip
	// and https://learn.microsoft.com/en-us/windows/win32/api/winsock/nf-winsock-bind
	// Other systems may have different behaviour
	// (https://daniel.haxx.se/blog/2014/10/25/pretending-port-zero-is-a-normal-one/)
	// in that case a uds socket should be used since only windows does not support them.
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP(localhost)})
	if err != nil {
		return 0, err
	}
	// Ignore error as l will be already closed unless there is an error before l.Close() below.
	defer l.Close()
	// Wait a maximum of 1 second for the byte to be sent on the acquired port
	l.SetDeadline(time.Now().Add(time.Second))
	resolvedAddr, ok := l.Addr().(*net.TCPAddr)
	if !ok || resolvedAddr == nil {
		return 0, fmt.Errorf("Failed to resolve %s:0 to an open port", localhost)
	}
	lAddr := resolvedAddr.String()
	port := resolvedAddr.Port
	if port == 0 {
		return 0, fmt.Errorf("Kernel did not resolve %s:0 to an open port", localhost)
	}
	errCh := make(chan error, 1)
	go func() {
		var err error
		defer close(errCh)
		defer func() {
			errCh <- err
		}()
		// Accept 1 connection from the socket.
		var ac net.Conn
		ac, err = l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				err = nil
			}
			return
		}
		// Ignore error as ac will be already closed unless there is an
		// error before ac.Close() below.
		defer ac.Close()
		// Read from the connection until it's closed by the remote peer.
		if _, err = io.ReadAll(ac); err != nil {
			return
		}
		// Close the socket.
		if err = ac.Close(); err != nil {
			return
		}
	}()
	// Dial the socket that we just opened.
	c, err := net.Dial("tcp", lAddr)
	if err != nil {
		return 0, err
	}
	// Ignore error as c will be already closed unless there is an error before c.Close() below.
	defer c.Close()
	// Write some arbitrary bytes to it.
	// If the port is not written to then the kernel will close and free the port immediately.
	if _, err := c.Write([]byte("x")); err != nil {
		return 0, err
	}
	// Close the connection to the socket now that we're done.
	if err := c.Close(); err != nil {
		return 0, err
	}
	// Close the listener now that we know we're done sending anything to it.
	if err := l.Close(); err != nil {
		return 0, err
	}
	if err := <-errCh; err != nil {
		return 0, err
	}
	// The port should be in TIME-WAIT state now
	return port, nil
}

// Close implements DepsScanner.Close.
// It cleanly disconnects from the remote service and releases resource associated with
// DepsScannerClient.
func (ds *DepsScannerClient) Close() {
	ds.m.Lock()
	defer ds.m.Unlock()
	if ds.client == nil {
		return
	}
	if err := ds.stopService(shutdownTimeout); err != nil {
		log.Errorf("%v", err)
		return
	}
	if err := ds.releaseOwnedSocket(); err != nil {
		log.Errorf("removing owned scanner socket: %v", err)
	}
	ds.client = nil
}

// ProcessInputs implements DepsScanner.ProcessInputs by sending a ProcessInputs gRPC to the
// connected server and returns the result.
// Returns list of dependencies, boolean indicating whether deps cache was used, and
// error if there was one.
func (ds *DepsScannerClient) ProcessInputs(ctx context.Context, execID string, compileCommand []string, filename, directory string, cmdEnv []string) (dependencies []string, usedCache bool, err error) {
	log.V(3).Infof("%v: Started remote input processing for %v", execID, filename)
	resCh := make(chan *pb.CPPProcessInputsResponse)
	errCh := make(chan error)
	go func() {
		resp, err := ds.client.ProcessInputs(
			ctx,
			&pb.CPPProcessInputsRequest{
				ExecId:    execID,
				Command:   compileCommand,
				Directory: directory,
				CmdEnv:    cmdEnv,
				Filename:  filename,
			})
		log.V(3).Infof("ProcessInputs complete: %v", resp)
		if err != nil {
			errCh <- err
		} else {
			resCh <- resp
		}
	}()

	select {
	case <-ctx.Done():
		// Only restart service if context timed out.
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, false, fmt.Errorf("failed to get response from scandeps: %w",
				ctx.Err())
		}
		// Timeout processing inputs.  Could be the service is offline.  Restart it if possible.
		log.Infof("restartService B: %v", ctx.Err())
		err := ds.restartService(ds.ctx, ds.executable)
		if err == nil {
			// Successfully restarted service; bubble up DeadlineExceeded to trigger a retry
			return nil, false, fmt.Errorf("failed to get response from scandeps: %w",
				ctx.Err())
		}
		// else unable to restart the service, or reproxy is not responsible for the service
		return nil, false, fmt.Errorf("failed to get response from scandeps; additionally failed to restart service: %w", err)

	case err := <-errCh:
		if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
			// Unavailable means a disconnect has occurred.
			select {
			// If the context was cancelled it means that scandeps was terminated by a propagated Ctrl-C
			// and reproxy is currently shutting down.
			case <-ctx.Done():
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return nil, false, fmt.Errorf("failed to get response from scandeps: %w",
						ctx.Err())
				}
			default:
				if restartErr := ds.restartService(ds.ctx, ds.executable); restartErr != nil {
					return nil, false, fmt.Errorf("communication with service lost; failed to restart service: %w", restartErr)
				}
				return nil, false, errors.New("communication with service lost; service restarted")
			}
		}
		// else something unexpected has gone wrong.
		return nil, false, fmt.Errorf("An unexpected error occurred communicating with the service: %w", err)
	case resp := <-resCh:
		if resp.Error != "" {
			return nil, false, fmt.Errorf("input processing failed: %v", resp.Error)
		}
		absDeps := make([]string, 0, len(resp.Dependencies))

		for _, p := range resp.Dependencies {
			if p == "" {
				continue
			}
			if !filepath.IsAbs(p) {
				p = filepath.Join(directory, p)
			}
			absDeps = append(absDeps, p)
		}
		return absDeps, resp.UsedCache, nil
	}
}

func (ds *DepsScannerClient) updateCapabilities(capabilities *pb.CapabilitiesResponse) {
	ds.capabilitiesMu.Lock()
	defer ds.capabilitiesMu.Unlock()
	ds.capabilities = capabilities
}

// Capabilities implements DepsScanner.Capabilities.
func (ds *DepsScannerClient) Capabilities() *pb.CapabilitiesResponse {
	ds.capabilitiesMu.RLock()
	defer ds.capabilitiesMu.RUnlock()
	return ds.capabilities
}

func (ds *DepsScannerClient) verifyService(ctx context.Context) error {
	retries := 10
	timeout := 10 * time.Second
	for i := 0; i < retries; i++ {
		sctx, cancel := context.WithTimeout(ds.ctx, timeout)
		defer cancel()

		capabilities, err := ds.client.Capabilities(sctx, &emptypb.Empty{})

		select {
		case <-ctx.Done():
			// timeout, retry
			continue
		default:
			// success?
			if err == nil {
				ds.updateCapabilities(capabilities)
				return nil
			} // else
			// Status call may return an error before the 10 seconds timeout expires if it isn't
			// ready to accept connections yet.  Typically it will error instantly with no delay.
			// In that case we want a delay (up to timeout) to give it more time to be ready.
			time.Sleep(timeout)
		}
	}
	// Still haven't connected; give up
	return fmt.Errorf("Unable to connect to server after %v seconds: %w", retries*(int)(timeout.Seconds()), context.DeadlineExceeded)
}

func (ds *DepsScannerClient) restartService(ctx context.Context, executable string) error {
	if executable == "" {
		// Not responsible for service
		return fmt.Errorf("Service is not managed by reproxy")
	}

	t := time.Now()
	ds.m.Lock()
	defer ds.m.Unlock()
	if t.Before(ds.serviceRestarted) {
		// service has been restarted since this thread was paused
		return nil
	}
	if err := ds.stopService(shutdownTimeout); err != nil {
		log.Errorf("%v", err)
		return fmt.Errorf("Unable to shutdown service: %w", err)
	}
	if err := ds.startService(ctx, executable); err != nil {
		log.Errorf("Failed to start dependency scanner: %v", err)
		return fmt.Errorf("Unable to start service: %w", err)
	}

	err := ds.verifyService(ctx)
	if err == nil {
		ds.serviceRestarted = time.Now()
	}
	return err
}

func removeUDSFile(addr string) {
	udsPath := addr
	if !strings.HasPrefix(addr, "unix") {
		return
	}
	if strings.HasPrefix(addr, "unix://") {
		udsPath = strings.TrimPrefix(addr, "unix://")
	} else if strings.HasPrefix(addr, "unix:") {
		udsPath = strings.TrimPrefix(addr, "unix:")
	}
	if err := os.Remove(udsPath); err != nil {
		log.Warningf("Failed to remove UDS socket file at %v: %v", udsPath, err)
		return
	}
	log.Infof("Successfully removed UDS socket file at %v", udsPath)
}

func (ds *DepsScannerClient) startService(ctx context.Context, executable string) (err error) {
	ctx, ds.terminate = context.WithCancel(ctx)
	defer func() {
		if err != nil {
			ds.terminate() // ExecuteInBackground returns an error only before launch.
		}
	}()
	ds.executable = executable
	ds.serviceStopped = false

	cmdArgs := []string{ds.executable, "--server_address", ds.address}
	cmdArgs = append(cmdArgs, "--cache_dir", ds.cacheDir)
	cmdArgs = append(cmdArgs, "--deps_cache_max_mb", strconv.FormatInt(int64(ds.cacheFileMaxMb), 10))
	if ds.useDepsCache {
		cmdArgs = append(cmdArgs, "--enable_deps_cache")
	} else {
		cmdArgs = append(cmdArgs, "--noenable_deps_cache")
	}

	envVars := make(map[string]string)
	for _, e := range os.Environ() {
		// Debugging parameters `experimental_segfault` and `experimental_deadlock` can be used
		// by setting their corresponding `FLAGS_*` environment variables.
		key, val, err := findKeyVal(e, os.LookupEnv)
		if err != nil {
			return err
		}
		switch key {
		case "FLAGS_experimental_segfault":
			cmdArgs = append(cmdArgs, "--experimental_segfault", val)
		case "FLAGS_experimental_deadlock":
			cmdArgs = append(cmdArgs, "--experimental_deadlock", val)
		default:
			envVars[key] = val
		}
	}
	if ds.logDir != "" {
		log.Infof("Setting GLOG_log_dir=\"%v\"", ds.logDir)
		envVars["GLOG_log_dir"] = ds.logDir
	}

	if ds.ownedSocketDir != "" {
		// This private endpoint is only reused after the prior child has exited.
		removeUDSFile(ds.address)
	}

	log.Infof("Starting service: %v", cmdArgs)
	cmd := &command.Command{Args: cmdArgs}
	cmd.InputSpec = &command.InputSpec{
		EnvironmentVariables: envVars,
	}

	ds.oe = &depsScannerLogger{}
	// A late exit must not block the executor if bounded cleanup has timed out.
	ds.ch = make(chan *command.Result, 1)

	return ds.executor.ExecuteInBackground(ctx, cmd, ds.oe, ds.ch)
}

func findKeyVal(envStr string, lookupEnvFunc func(string) (string, bool)) (string, string, error) {
	envParts := strings.Split(envStr, "=")
	if len(envParts) == 2 {
		return envParts[0], envParts[1], nil
	}
	for i := 1; i < len(envParts)+1; i++ {
		pkey := strings.Join(envParts[:i], "=")
		pval := strings.Join(envParts[i:], "=")
		if val, ok := lookupEnvFunc(pkey); ok && val == pval {
			return pkey, pval, nil
		}
	}
	return "", "", fmt.Errorf("Got %s in env vars list but could not find any matching env var", strings.Join(envParts, "="))
}

// stopService attempts to stop the dependency scanner service started by reproxy.
// If process cannot be stopped within `timeout` it will be killed forcefully.
// If process is still running after a second `timeout` passed, an error will be returned.
// Note the total time before returning may be up to `2*timeout`.
func (ds *DepsScannerClient) stopService(timeout time.Duration) error {
	if ds.executable == "" || ds.serviceStopped {
		// Dependency scanner service wasn't started by reclient; nothing to do
		return nil
	}

	// Dependency scanner service was started by reproxy; we must stop it
	// Parent cancellation must not eliminate the independent cleanup deadline.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if ds.client != nil {
		statusResponse, err := ds.client.Shutdown(ctx, &emptypb.Empty{})
		if err != nil {
			// This could mean the service has crashed, it could be frozen, or may have taken
			// too long to respond. Log the error and wait to see if it shuts itself down.
			log.Errorf("Error sending shutdown command to %v: %v", ds.executable, err)
		} else {
			log.Infof("Shutdown response from %s (v%s)", statusResponse.GetName(), statusResponse.GetVersion())
			if statusResponse.GetUptime() != nil {
				log.Infof("> Uptime: %d seconds", statusResponse.GetUptime().GetSeconds())
			}
			log.Infof("> Completed: %d", statusResponse.GetCompletedActions())
			log.Infof("> Still in Progress: %d", statusResponse.GetRunningActions())
		}
	} else {
		// We started but were never able to connect to the service; must force kill the service.
		// This is a noop if the service crashed unexpectedly.
		ds.terminate()
	}

	for i := 0; i < 2; i++ {
		select {
		case done := <-ds.ch:
			if done == nil {
				return fmt.Errorf("%s shutdown result unavailable", ds.executable)
			}
			ds.serviceStopped = true
			log.Infof("scandeps_lifecycle phase=exit_confirmed address=%q exit_code=%d", ds.address, done.ExitCode)
			if done.IsOk() {
				log.Infof("%v successfully stopped", ds.executable)
			} else {
				log.Warningf("%v stopped with error code %v", ds.executable, done.ExitCode)
			}
			// service has been successfully stopped
			return nil
		case <-time.After(timeout):
			log.Warningf("Still waiting for shutdown after %.0f seconds", timeout.Seconds())
			if i == 0 {
				log.Info("Sending termination signal to %v", ds.executable)
				// We've waited hardkill seconds for the shutdown to complete.
				// Assume that it's stuck and perform a hard kill.
				// This cancel is tied to the command context and will kill the subprocess and all
				// active ProcessInputs requests
				ds.terminate()
			}
		}
	}
	return fmt.Errorf("could not shutdown %v after %.0f seconds. Giving up: %w", ds.executable, 2*(timeout.Seconds()), context.DeadlineExceeded)
}
