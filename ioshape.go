// Package ioshape provides utilities for bandwidth shaping of I/O operations.
package ioshape

import (
	"bytes"
	"context"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/goinsane/xcontext"
)

// Reader implements io.Reader with bandwidth rate limit. Reader uses leaky bucket algorithm.
type Reader struct {
	ctx        xcontext.CancelableContext
	wg         sync.WaitGroup
	rd         io.Reader
	rate       int64
	period     time.Duration
	leakSize   int
	bucketSize int
	buf        *bytes.Buffer
	bufErr     error
	bufCh      chan struct{}
	bufMu      sync.Mutex
	rrCh       chan *readRequest
}

// NewReader creates new Reader given rate and period.
// rate is bandwidth limit in bytes per second, period is leak period that used by leaky bucket algorithm.
func NewReader(rd io.Reader, rate int64, period time.Duration) *Reader {
	l := &Reader{
		ctx:    xcontext.WithCancelable2(context.Background()),
		rd:     rd,
		rate:   rate,
		period: period,
		buf:    bytes.NewBuffer(nil),
		bufCh:  make(chan struct{}, 1),
		rrCh:   make(chan *readRequest),
	}
	x := big.NewInt(l.rate)
	x.Mul(x, big.NewInt(l.period.Nanoseconds()))
	x.Quo(x, big.NewInt(1e9))
	l.leakSize = int(x.Int64())
	l.bucketSize = 1 * int(x.Int64())
	l.wg.Add(1)
	go l.readLoop()
	l.wg.Add(1)
	go l.writeLoop()
	return l
}

func (r *Reader) readLoop() {
	defer r.wg.Done()
	b := make([]byte, 32*1024)
	for r.ctx.Err() == nil {
		r.bufMu.Lock()
		nn := r.bucketSize - r.buf.Len()
		r.bufMu.Unlock()
		if nn <= 0 {
			select {
			case <-r.ctx.Done():
				return
			case <-r.bufCh:
			}
			continue
		}
		if nn > len(b) {
			nn = len(b)
		}
		n, e := r.rd.Read(b)
		r.bufMu.Lock()
		r.buf.Write(b[:n])
		if e != nil {
			r.bufErr = e
			r.bufMu.Unlock()
			break
		}
		r.bufMu.Unlock()
	}
}

func (r *Reader) writeLoop() {
	defer r.wg.Done()
	tkr := time.NewTicker(r.period)
	defer tkr.Stop()
	for r.ctx.Err() == nil {
		select {
		case <-r.ctx.Done():
			return
		case <-tkr.C:
		}
		nr := r.leakSize
		for r.ctx.Err() == nil && nr > 0 {
			var rr *readRequest
			select {
			case <-r.ctx.Done():
				return
			case rr = <-r.rrCh:
			}
			nn := len(rr.B)
			if nn > nr {
				nn = nr
			}
			r.bufMu.Lock()
			rr.N, _ = r.buf.Read(rr.B[:nn])
			if rr.N <= 0 {
				rr.E = r.bufErr
			} else {
				rr.E = nil
			}
			r.bufMu.Unlock()
			nr -= rr.N
			close(rr.C)
			select {
			case r.bufCh <- struct{}{}:
			default:
			}
		}
	}
}

// Stop stops bandwidth shaping operations. Stop should be called after the operation was ended.
func (r *Reader) Stop() {
	r.ctx.Cancel()
}

// Read implements (io.Reader).Read. When Stop called, Read drains bucket and returns EOF.
func (r *Reader) Read(b []byte) (n int, err error) {
	c := make(chan struct{})
	rr := &readRequest{
		C: c,
		B: b,
	}
	select {
	case <-r.ctx.Done():
		r.wg.Wait()
		r.bufMu.Lock()
		defer r.bufMu.Unlock()
		return r.buf.Read(b)
	case r.rrCh <- rr:
		<-c
		return rr.N, rr.E
	}
}

type readRequest struct {
	C chan<- struct{}
	B []byte
	N int
	E error
}

// Copy uses io.Copy with Reader on src argument.
func Copy(dst io.Writer, src io.Reader, rate int64, period time.Duration) (written int64, err error) {
	rd := NewReader(src, rate, period)
	defer rd.Stop()
	return io.Copy(dst, rd)
}

// CopyN uses io.CopyN with Reader on src argument.
func CopyN(dst io.Writer, src io.Reader, n int64, rate int64, period time.Duration) (written int64, err error) {
	rd := NewReader(src, rate, period)
	defer rd.Stop()
	return io.CopyN(dst, rd, n)
}
