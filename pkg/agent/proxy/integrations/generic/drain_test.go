package generic

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	syncMock "go.keploy.io/server/v3/pkg/agent/proxy/syncMock"
	proxyutil "go.keploy.io/server/v3/pkg/agent/proxy/util"
	"go.keploy.io/server/v3/pkg/models"
	"go.keploy.io/server/v3/pkg/neterr"
	"go.uber.org/zap"
)

// testDrainIdle keeps these quick; the drain's logic does not depend on
// the size of the window, only on whether the survivor moved bytes during
// one. The production value is pinned separately, in the util package.
const testDrainIdle = 400 * time.Millisecond

// splitReadConn fails on Read while Write keeps working — what Go's
// tls.Conn does, since it keeps c.in.err and c.out.err separate. Recording
// reaches encodeGeneric with SafeConns over *tls.Conn on every
// TLS-intercepted path, so one direction failing while the other stays
// healthy is the real case, not a contrivance.
type splitReadConn struct {
	net.Conn
	readErr error
}

func (c *splitReadConn) Read(p []byte) (int, error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	return c.Conn.Read(p)
}

// Delegated because embedding net.Conn as an INTERFACE promotes only
// net.Conn's method set — a fake that silently swallows CloseWrite
// certifies nothing about a path whose whole subject is the FIN.
func (c *splitReadConn) CloseWrite() error { return proxyutil.CloseWriteIfPossible(c.Conn) }

// TestEncodeGeneric_DrainsTheSurvivorInBothDirections is the generic-path
// twin of the relay's drain test. What it loses when it goes wrong is
// worse than the relay's: the user's bytes AND the mocks the tee captured
// from them.
//
// Scope it honestly, though. encodeGeneric is reached only via
// Generic.recordLegacy, which needs session.V2 == nil, and
// shouldRecordViaSupervisor routes every parser implementing
// IntegrationsV2 to the supervisor, and every in-tree parser reports
// IsV2() == true. With the KEPLOY_NEW_RELAY rollback knob removed there is
// no way to reach this legacy path from the dispatcher at all; it is kept
// covered because the code is still reachable by direct construction.
//
// Both orientations are driven on purpose. The survivor is whichever
// direction did NOT report, so the drain has to poll that direction's byte
// counter; code that assumes the first result came from the first copy
// goroutine watches the counter of the direction that just died, which
// never advances again — the first tick reads "stalled" and the drain
// collapses into the fixed wall clock it replaced. That is invisible in
// one orientation and total data loss in the other.
func TestEncodeGeneric_DrainsTheSurvivorInBothDirections(t *testing.T) {
	for _, tc := range []struct {
		name      string
		breakDest bool
	}{
		{"client read half fails, response survives", false},
		{"upstream read half fails, request survives", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientApp, srcProxy := prodPair(t, true)
			destSvc, dstProxy := prodPair(t, false)

			clientConn, destConn := srcProxy, dstProxy
			var sender, receiver net.Conn
			if tc.breakDest {
				destConn = &splitReadConn{Conn: dstProxy, readErr: io.ErrUnexpectedEOF}
				sender, receiver = clientApp, destSvc
			} else {
				clientConn = &splitReadConn{Conn: srcProxy, readErr: io.ErrUnexpectedEOF}
				sender, receiver = destSvc, clientApp
			}

			// Paced so the transfer is still ARRIVING when the other
			// direction breaks, and so it outlasts a whole idle window —
			// a wall clock truncates this, an idle timer does not.
			const chunks, chunkSize = 12, 16 * 1024
			gap := testDrainIdle / 3
			payload := bytes.Repeat([]byte("G"), chunks*chunkSize)
			go func() {
				for i := 0; i < chunks; i++ {
					if _, err := sender.Write(payload[i*chunkSize : (i+1)*chunkSize]); err != nil {
						return
					}
					time.Sleep(gap)
				}
			}()

			got := make(chan int, 1)
			go func() {
				buf := make([]byte, len(payload))
				n, _ := io.ReadFull(receiver, buf)
				got <- n
			}()

			mocks := make(chan *models.Mock, 64)
			returned := make(chan struct{})
			go func() {
				_ = encodeGenericWithDrain(context.Background(), zap.NewNop(), nil,
					clientConn, destConn, mocks, models.OutgoingOptions{}, testDrainIdle)
				close(returned)
			}()

			select {
			case <-returned:
			case <-time.After(30 * time.Second):
				t.Fatal("encodeGeneric never returned")
			}
			// Model the caller: handleConnection closes the real sockets the
			// moment this returns. Without that close the test proves nothing,
			// because the copy goroutines would keep delivering on their own.
			//
			// It must be the REAL socket. SafeConn.Close is a no-op — that is
			// the whole reason keploy cannot wake a stranded io.Copy — so
			// closing the wrapper here would model nothing and quietly turn
			// this into a test that passes against a truncating build.
			closeUnderlying(t, srcProxy)
			closeUnderlying(t, dstProxy)

			select {
			case n := <-got:
				if n < len(payload) {
					t.Fatalf("the surviving direction delivered %d of %d bytes over %v. On this "+
						"path an early return or a fixed grace costs the user's bytes AND the "+
						"mocks the tee captured from them.",
						n, len(payload), time.Duration(chunks)*gap)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("reader never completed")
			}
		})
	}
}

// TestCaptureTeeWriterCountsWhatItDelivered is the positive control for
// the counter the drain polls. A tee that always reported zero would make
// every survivor look stalled from the first tick, silently restoring the
// wall clock — and no forwarding test would notice, because forwarding
// still works.
func TestCaptureTeeWriterCountsWhatItDelivered(t *testing.T) {
	var sink bytes.Buffer
	ch := make(chan []byte, 8)
	tee := &captureTeeWriter{dest: &sink, ch: ch, closeOnce: &sync.Once{}}

	if got := tee.delivered(); got != 0 {
		t.Fatalf("a fresh tee reports %d bytes delivered, want 0", got)
	}
	if _, err := tee.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := tee.Write([]byte(" world")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got, want := tee.delivered(), int64(len("hello world")); got != want {
		t.Fatalf("tee reports %d bytes delivered, want %d. The drain treats an unmoving "+
			"count as a stalled survivor, so a counter that does not advance abandons a "+
			"direction that is still delivering.", got, want)
	}
	if sink.String() != "hello world" {
		t.Fatalf("destination got %q", sink.String())
	}
}

// closeUnderlying closes the real socket inside a SafeConn, which is what
// handleConnection's deferred close reaches. SafeConn's own Close is a
// no-op, so calling it here would assert nothing.
func closeUnderlying(t *testing.T, c net.Conn) {
	t.Helper()
	sc, ok := c.(*proxyutil.SafeConn)
	if !ok {
		t.Fatalf("expected a *proxyutil.SafeConn, got %T — production wraps both conns, and a "+
			"raw conn here would make this test pass for the wrong reason", c)
	}
	_ = sc.Unwrap().Close()
}

// TestEncodeGeneric_ProductionEntryPointDrainsTheSurvivor drives
// encodeGeneric itself, on the production window, through the survivor
// path.
//
// The tests above call encodeGenericWithDrain so they can run at 400ms,
// which means the one line that wires proxyutil.RelayDrainIdle into the
// drain is executed but never checked. Passing a window too small to cover
// a real transfer abandons the survivor immediately — the original bug —
// and nothing else here would notice.
func TestEncodeGeneric_ProductionEntryPointDrainsTheSurvivor(t *testing.T) {
	clientApp, srcProxy := prodPair(t, true)
	destSvc, dstProxy := prodPair(t, false)

	broken := &splitReadConn{Conn: srcProxy, readErr: io.ErrUnexpectedEOF}

	const chunks, chunkSize = 10, 16 * 1024
	payload := bytes.Repeat([]byte("E"), chunks*chunkSize)
	go func() {
		for i := 0; i < chunks; i++ {
			if _, err := destSvc.Write(payload[i*chunkSize : (i+1)*chunkSize]); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		// Ends the survivor's copy so this returns without sitting out the
		// production idle window.
		if tc, ok := destSvc.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()

	got := make(chan int, 1)
	go func() {
		buf := make([]byte, len(payload))
		n, _ := io.ReadFull(clientApp, buf)
		got <- n
	}()

	mocks := make(chan *models.Mock, 64)
	returned := make(chan struct{})
	go func() {
		_ = encodeGeneric(context.Background(), zap.NewNop(), nil, broken, dstProxy,
			mocks, models.OutgoingOptions{})
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(60 * time.Second):
		t.Fatal("encodeGeneric never returned")
	}
	closeUnderlying(t, srcProxy)
	closeUnderlying(t, dstProxy)

	select {
	case n := <-got:
		if n < len(payload) {
			t.Fatalf("the surviving direction delivered %d of %d bytes through encodeGeneric "+
				"on the production window. The drain is only as good as the window wired in "+
				"at that call site.", n, len(payload))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reader never completed")
	}
}

// TestEncodeGeneric_CleanThenResetReturnsPromptly guards generic's copy of
// the early return: when both copy results are already in hand there is
// nothing to drain, and waiting anyway costs a full idle window — 30s in
// production — on the case the code itself calls "very common".
func TestEncodeGeneric_CleanThenResetReturnsPromptly(t *testing.T) {
	clientApp, srcProxy := prodPair(t, true)
	destSvc, dstProxy := prodPair(t, false)

	go func() {
		_, _ = clientApp.Write([]byte("hello"))
		if tc, ok := clientApp.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()
	go func() {
		time.Sleep(20 * time.Millisecond)
		if tc, ok := destSvc.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = destSvc.Close()
	}()

	mocks := make(chan *models.Mock, 64)
	start := time.Now()
	returned := make(chan struct{})
	go func() {
		_ = encodeGenericWithDrain(context.Background(), zap.NewNop(), nil, srcProxy, dstProxy,
			mocks, models.OutgoingOptions{}, testDrainIdle)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("encodeGenericWithDrain never returned")
	}
	if elapsed := time.Since(start); elapsed > testDrainIdle*3/4 {
		t.Fatalf("returned after %v, most of one %v window, with both copy results already "+
			"available. In production that dead time is %v per clean-then-reset connection.",
			elapsed, testDrainIdle, proxyutil.RelayDrainIdle)
	}
}

// TestEncodeGeneric_PassesAResetOnAndEndsOnIt is
// proxyutil's TestRelayRawPassthrough_PassesAResetOnAndEndsOnIt on generic's
// own capturing copy loop: a peer that resets is gone both ways, so the
// other peer is reset too and the exchange ends on that event, with no
// survivor drain. Before the fix an eager client (it speaks, then closes
// with the reply unread, which is a reset) held the connection for a whole
// idle window, 30s in production, while its destination waited for the
// next request. No clock decides anything: the drain and the waiting peer's
// read are both events.
func TestEncodeGeneric_PassesAResetOnAndEndsOnIt(t *testing.T) {
	for _, destResets := range []bool{false, true} {
		name := "the client resets"
		if destResets {
			name = "the destination resets"
		}
		t.Run(name, func(t *testing.T) {
			clientApp, srcProxy := prodPair(t, true)
			destSvc, dstProxy := prodPair(t, false)

			drained := make(chan struct{})
			returned := make(chan struct{})
			go func() {
				_ = encodeGenericDrained(context.Background(), zap.NewNop(), nil, srcProxy, dstProxy,
					make(chan *models.Mock, 64), models.OutgoingOptions{},
					func(finished <-chan copyOutcome, _ func() int64) {
						close(drained)
						<-finished // the test's close of the sockets ends it
					})
				close(returned)
			}()

			req := []byte("EHLO eager\r\n")
			if _, err := clientApp.Write(req); err != nil {
				t.Fatalf("write: %v", err)
			}
			if _, err := io.ReadFull(destSvc, make([]byte, len(req))); err != nil {
				t.Fatalf("the request did not reach the destination: %v", err)
			}
			gone, waiting := clientApp, destSvc
			if destResets {
				gone, waiting = destSvc, clientApp
			}
			if err := gone.(*net.TCPConn).SetLinger(0); err != nil {
				t.Fatalf("linger: %v", err)
			}
			_ = gone.Close()

			select {
			case <-returned:
			case <-drained:
				t.Fatal("generic waited on a survivor after a peer reset: the survivor writes to " +
					"the peer that is gone, and the other peer, never told, waits for the next " +
					"request for a whole idle window, " + proxyutil.RelayDrainIdle.String() +
					" in production")
			}
			// The caller closes the real sockets once encode returns.
			closeUnderlying(t, srcProxy)
			closeUnderlying(t, dstProxy)
			if _, err := waiting.Read(make([]byte, 1)); !neterr.IsConnReset(err) {
				t.Fatalf("the waiting peer read %v; want a reset. A FIN, or nothing, tells it "+
					"the other peer ended cleanly or is still there, when it reset", err)
			}
		})
	}
}

// resetWriteConn fails every Write with a reset once armed, while Read keeps
// working: the proxy's write to a destination that has reset, whose last
// reply is still to be read.
type resetWriteConn struct {
	net.Conn
	armed atomic.Bool
}

func (c *resetWriteConn) Write(p []byte) (int, error) {
	if c.armed.Load() {
		return 0, &net.OpError{Op: "write", Net: "tcp",
			Err: &os.SyscallError{Syscall: "write", Err: syscall.ECONNRESET}}
	}
	return c.Conn.Write(p)
}

// See splitReadConn: an embedded net.Conn interface does not promote it.
func (c *resetWriteConn) CloseWrite() error { return proxyutil.CloseWriteIfPossible(c.Conn) }

// TestEncodeGeneric_AResetWriteStillDrainsTheSurvivor is proxyutil's
// TestRelayRawPassthrough_AResetWriteStillDrainsTheSurvivor on generic's own
// capturing copy loop, whose two directions are two copies of the rule, so
// both are driven. A write that fails with a reset says the DESTINATION of
// that write is gone, not the copy's source, so generic passes no reset on
// and drains the other direction: it reads that gone peer, which may still
// hold its last bytes (a server's 413 before it resets an upload it will not
// read). Telling the two apart is why each copy reads its source's error
// (proxyutil.ReadRecorder), not io.Copy's: a copy that took io.Copy's error
// would end generic at once on the failed write, and the caller's close
// would drop those bytes (and, from the destination, the mock of the
// exchange). No clock decides anything: the drain starting is an event.
func TestEncodeGeneric_AResetWriteStillDrainsTheSurvivor(t *testing.T) {
	for _, clientGone := range []bool{false, true} {
		name := "the write to the destination fails"
		if clientGone {
			name = "the write to the client fails"
		}
		t.Run(name, func(t *testing.T) {
			clientApp, srcProxy := prodPair(t, true)
			destSvc, dstProxy := prodPair(t, false)
			// goner is the peer that is gone for writes; other is the one whose
			// bytes are written to it.
			var clientConn, destConn net.Conn = srcProxy, dstProxy
			gone, goner, other := &resetWriteConn{Conn: dstProxy}, destSvc, clientApp
			destConn = gone
			if clientGone {
				gone, goner, other = &resetWriteConn{Conn: srcProxy}, clientApp, destSvc
				clientConn, destConn = gone, dstProxy
			}

			out := make(chan *models.Mock, 4)
			mgr := syncMock.New(zap.NewNop())
			mgr.SetOutputChannel(out)
			ctx := syncMock.NewContext(context.WithValue(context.Background(), models.ClientConnectionIDKey, "c1"), mgr)
			// The mock creator starts once encode has returned, with the
			// whole exchange buffered, as on a loaded host: what it makes of
			// it cannot depend on when it was scheduled.
			ctx, startRecorder := holdRecorderStart(t, ctx)

			drained := make(chan struct{})
			returned := make(chan struct{})
			req := []byte("PUT /upload\r\n")
			go func() {
				_ = encodeGenericDrained(ctx, zap.NewNop(), req, clientConn, destConn,
					make(chan *models.Mock, 64), models.OutgoingOptions{},
					func(finished <-chan copyOutcome, _ func() int64) {
						close(drained)
						<-finished // the test's close of the sockets ends it
					})
				close(returned)
			}()
			if _, err := io.ReadFull(destSvc, make([]byte, len(req))); err != nil {
				t.Fatalf("the request did not reach the destination: %v", err)
			}

			// Writes to the gone peer now fail with a reset, and the other
			// peer's next bytes are written there.
			gone.armed.Store(true)
			if _, err := other.Write([]byte("more")); err != nil {
				t.Fatalf("write: %v", err)
			}
			select {
			case <-drained:
			case <-returned:
				t.Fatal("generic ended at once on a failed write, as if the copy's source had reset. " +
					"The survivor still reads the gone peer and may hold its last bytes; ending now " +
					"lets the caller's close drop them")
			}
			// The gone peer's last bytes, sent before it went, still reach the
			// other peer through the survivor.
			last := []byte("413 too large\r\n")
			if _, err := goner.Write(last); err != nil {
				t.Fatalf("write: %v", err)
			}
			got := make([]byte, len(last))
			if _, err := io.ReadFull(other, got); err != nil || !bytes.Equal(got, last) {
				t.Fatalf("the survivor delivered %q, %v; want the gone peer's last bytes %q", got, err, last)
			}

			// The caller closes the real sockets once encode returns; here that
			// close is what ends the drain, and the capture flushes the exchange.
			closeUnderlying(t, srcProxy)
			closeUnderlying(t, dstProxy)
			<-returned
			startRecorder()
			if clientGone {
				// The reply that failed to reach the client was not captured, so
				// the exchange has no reply and no mock; delivery is the check.
				return
			}
			select {
			case m := <-out:
				if len(m.Spec.GenericRequests) == 0 || m.Spec.GenericRequests[0].Message[0].Data != string(req) {
					t.Fatalf("the mock's request is %+v; want %q", m.Spec.GenericRequests, req)
				}
				if len(m.Spec.GenericResponses) == 0 || m.Spec.GenericResponses[0].Message[0].Data != string(last) {
					t.Fatalf("the mock's reply is %+v; want the destination's %q, which arrived after "+
						"the failed write", m.Spec.GenericResponses, last)
				}
			case <-time.After(30 * time.Second): // a hang guard, far above any run
				t.Fatal("no mock was captured for the exchange")
			}
		})
	}
}

// holdRecorderStart returns ctx with a captured-request-time source whose
// first call, the mock creator's first act (createGenericMocksAsync), waits
// for start: the creator is scheduled after its connection's chunks are
// buffered, as a loaded host can schedule it. A test that ends before it
// calls start still releases the creator, at its cleanup, so that no
// goroutine is left blocked behind it.
func holdRecorderStart(t *testing.T, ctx context.Context) (context.Context, func()) {
	release := make(chan struct{})
	var once sync.Once
	at := func() time.Time {
		once.Do(func() { <-release })
		return time.Time{} // models.CapturedReqTime then reads the clock
	}
	var startOnce sync.Once
	start := func() { startOnce.Do(func() { close(release) }) }
	t.Cleanup(start)
	return context.WithValue(ctx, models.CapturedReqTimeKey, at), start
}

// TestEncodeGeneric_AFirstReplyBufferedBeforeTheRecorderStartsKeepsItsMock:
// encode wrote the initial request and started the mock creator, and the
// whole exchange was relayed before the creator first ran (a loaded host).
// The initial request reached the creator on clientCapChan, so with the
// reply on destCapChan both channels held data, and a select picks among
// ready channels at random: half the time it took the reply first, as a
// reply with no request, then dropped it when the request came, and the
// connection's only exchange had no mock. The initial request is known
// before either copy starts, so the creator now has it before it reads
// either channel. Each round is a fresh connection; a creator that can
// still take the reply first fails one of them with odds 1 - 2^-16, and
// none passes by luck of timing, since the order is fixed by the hold.
func TestEncodeGeneric_AFirstReplyBufferedBeforeTheRecorderStartsKeepsItsMock(t *testing.T) {
	for round := 0; round < 16; round++ {
		clientApp, srcProxy := prodPair(t, true)
		destSvc, dstProxy := prodPair(t, false)
		out := make(chan *models.Mock, 4)
		mgr := syncMock.New(zap.NewNop())
		mgr.SetOutputChannel(out)
		ctx := syncMock.NewContext(context.WithValue(context.Background(), models.ClientConnectionIDKey, "c1"), mgr)
		ctx, startRecorder := holdRecorderStart(t, ctx)

		req, reply := []byte("ping\r\n"), []byte("pong\r\n")
		returned := make(chan struct{})
		go func() {
			_ = encodeGeneric(ctx, zap.NewNop(), req, srcProxy, dstProxy,
				make(chan *models.Mock, 64), models.OutgoingOptions{})
			close(returned)
		}()
		if _, err := io.ReadFull(destSvc, make([]byte, len(req))); err != nil {
			t.Fatalf("round %d: the request did not reach the destination: %v", round, err)
		}
		if _, err := destSvc.Write(reply); err != nil {
			t.Fatalf("round %d: write: %v", round, err)
		}
		got := make([]byte, len(reply))
		if _, err := io.ReadFull(clientApp, got); err != nil || !bytes.Equal(got, reply) {
			t.Fatalf("round %d: the client read %q, %v; want %q", round, got, err, reply)
		}
		// Both peers end cleanly, so encode returns with both capture
		// channels closed and the reply buffered; then the creator starts.
		if err := clientApp.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatalf("round %d: half-close: %v", round, err)
		}
		if err := destSvc.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatalf("round %d: half-close: %v", round, err)
		}
		<-returned
		startRecorder()
		select {
		case m := <-out:
			if len(m.Spec.GenericRequests) != 1 || m.Spec.GenericRequests[0].Message[0].Data != string(req) ||
				len(m.Spec.GenericResponses) != 1 || m.Spec.GenericResponses[0].Message[0].Data != string(reply) {
				t.Fatalf("round %d: the mock is %+v -> %+v; want %q -> %q", round,
					m.Spec.GenericRequests, m.Spec.GenericResponses, req, reply)
			}
		case <-time.After(30 * time.Second): // a hang guard, far above any run
			t.Fatalf("round %d: no mock was captured for the connection's only exchange: the "+
				"creator took the buffered reply before the initial request and dropped it", round)
		}
	}
}
