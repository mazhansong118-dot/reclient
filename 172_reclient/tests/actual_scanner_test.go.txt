//go:build linux

package depsscannerclient

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/bazelbuild/reclient/api/scandeps"
	"github.com/bazelbuild/reclient/internal/pkg/ipc"
	"github.com/bazelbuild/reclient/internal/pkg/subprocess"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type actualChild struct {
	pid int
	ticks string
	address string
	done chan struct{}
}

type actualExecutor struct {
	children []*actualChild
	finished chan struct{}
}

// Keep the production executor unchanged; only relay its exit notification and
// inspect the exact direct child launched with this test's private endpoint.
func (e *actualExecutor) ExecuteInBackground(ctx context.Context, cmd *command.Command, oe outerr.OutErr, ch chan *command.Result) error {
	result := make(chan *command.Result, 1)
	if err := (subprocess.SystemExecutor{}).ExecuteInBackground(ctx, cmd, oe, result); err != nil { return err }
	c := &actualChild{address: cmd.Args[2], done: make(chan struct{})}
	e.children = append(e.children, c)
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name()); if err != nil { continue }
		known:=false
		for _, previous:=range e.children {if previous!=c && previous.pid==pid {known=true}}
		if known {continue}
		p := filepath.Join("/proc", entry.Name())
		b, err := os.ReadFile(p+"/stat"); if err != nil { continue }
		fields := strings.Fields(string(b)[strings.LastIndexByte(string(b), ')')+1:])
		if len(fields)<20 || fields[1] != strconv.Itoa(os.Getpid()) { continue }
		args, _ := os.ReadFile(p+"/cmdline")
		parts := strings.Split(string(args), "\x00")
		if len(parts)>2 && parts[0]==cmd.Args[0] && parts[1]=="--server_address" && parts[2]==c.address {
			c.pid, c.ticks = pid, fields[19]; break
		}
	}
	go func() {
		r := <-result
		close(c.done)
		select { case ch <- r: case <-e.finished: }
	}()
	return nil
}

func actualAlive(c *actualChild) bool {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", c.pid)); if err != nil { return false }
	f := strings.Fields(string(b)[strings.LastIndexByte(string(b), ')')+1:])
	return len(f)>19 && f[19]==c.ticks && f[0]!="Z"
}

func actualInodes(c *actualChild) []string {
	owned := map[string]bool{}
	paths, _ := filepath.Glob(fmt.Sprintf("/proc/%d/fd/*", c.pid))
	for _, p := range paths {
		s, _ := os.Readlink(p)
		if strings.HasPrefix(s,"socket:[") { owned[strings.TrimSuffix(strings.TrimPrefix(s,"socket:["),"]")]=true }
	}
	b, _ := os.ReadFile("/proc/net/unix")
	var inodes []string
	for _, line := range strings.Split(string(b),"\n") {
		f:=strings.Fields(line)
		if len(f)>7 && owned[f[6]] && filepath.Clean(f[7])==filepath.Clean(strings.TrimPrefix(c.address,"unix://")) { inodes=append(inodes,f[6]) }
	}
	return inodes
}

func actualPathInodes(address string) []string {
	b,_:=os.ReadFile("/proc/net/unix")
	var found []string
	for _,line:=range strings.Split(string(b),"\n") {
		f:=strings.Fields(line)
		if len(f)>7 && filepath.Clean(f[7])==filepath.Clean(strings.TrimPrefix(address,"unix://")) {found=append(found,f[6])}
	}
	return found
}

func TestActualScannerStartupFailureAndRetry(t *testing.T) {
	binary := os.Getenv("TEST_REAL_SCANNER")
	if binary=="" { t.Skip("opt-in actual scanner integration") }
	dir, err := os.MkdirTemp("", "rbe-actual-"); if err!=nil {t.Fatal(err)}
	defer os.RemoveAll(dir) // A newly created test-owned directory, never a source/out path.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	e := &actualExecutor{finished:make(chan struct{})}
	defer func(){
		cancel()
		close(e.finished)
		for _, c := range e.children {
			select { case <-c.done: case <-time.After(5*time.Second): t.Errorf("test-owned child not reaped pid=%d startticks=%s",c.pid,c.ticks) }
			t.Logf("OWNED_CLEANUP pid=%d startticks=%s alive=%t",c.pid,c.ticks,actualAlive(c))
		}
	}()
	var connections []*grpc.ClientConn
	defer func(){for _, c:=range connections {c.Close()}}()
	connect := func(ctx context.Context, address string)(pb.CPPDepsScannerClient,*pb.CapabilitiesResponse,error){
		conn,err:=ipc.DialContext(ctx,address); if err!=nil{return nil,nil,err}
		connections=append(connections,conn)
		client:=pb.NewCPPDepsScannerClient(conn)
		for {
			caps,err:=client.Capabilities(ctx,&emptypb.Empty{})
			if err==nil {return client,caps,nil}
			select {case <-ctx.Done(): return nil,nil,ctx.Err(); case <-time.After(20*time.Millisecond):}
		}
	}
	firstReady:=false
	failAfterReady:=func(ctx context.Context,a string)(pb.CPPDepsScannerClient,*pb.CapabilitiesResponse,error){
		_,_,err:=connect(ctx,a); if err!=nil{return nil,nil,err}
		firstReady=true
		return nil,nil,status.Error(codes.Unavailable,"TEST_INJECTED connection failure after actual Capabilities succeeded")
	}
	proxy:="unix://"+filepath.Join(dir,"reproxy.sock")
	_,err=New(ctx,e,dir,1,false,"","exec://"+binary,proxy,10*time.Second,failAfterReady)
	if err==nil || !strings.Contains(err.Error(),"TEST_INJECTED") || !firstReady { t.Fatalf("injection prerequisite not reached: %v",err) }
	first:=e.children[0]
	if first.pid==0 {t.Fatal("unable to identify owned scanner")}
	before:=actualAlive(first)
	t.Logf("AFTER_FIRST_FAILURE pid=%d startticks=%s alive=%t address=%s inodes=%v",first.pid,first.ticks,before,first.address,actualInodes(first))
	if before {t.Error("REGRESSION: actual scanner remains alive after startup connection failure")}
	second,err:=New(ctx,e,dir,1,false,"","exec://"+binary,proxy,10*time.Second,connect)
	if err!=nil {t.Fatalf("second actual scanner startup failed: %v",err)}
	defer second.Close()
	last:=e.children[1]
	if last.pid==0 {t.Fatal("unable to identify second scanner")}
	if last.pid==first.pid {t.Fatal("invalid test identity: scanner PIDs must differ")}
	overlap:=actualAlive(first)&&actualAlive(last)
	same:=first.address==last.address
	t.Logf("SECOND_READY first_pid=%d first_alive=%t second_pid=%d startticks=%s second_alive=%t same_address=%t first_inodes=%v second_inodes=%v",first.pid,actualAlive(first),last.pid,last.ticks,actualAlive(last),same,actualInodes(first),actualInodes(last))
	t.Logf("SOCKET_PATH_INODES first=%v second=%v",actualPathInodes(first.address),actualPathInodes(last.address))
	if overlap {t.Error("REGRESSION: two actual scanners overlap after retry")}
	if same {t.Error("REGRESSION: retry reused the same socket pathname")}
	second.Close()
	if actualAlive(last) {t.Error("normal close did not reap actual scanner")}
}
