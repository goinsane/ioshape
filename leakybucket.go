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

// LeakyBucket implements leaky bucket algorithm as io.Reader.
type LeakyBucket struct {
	ctx        xcontext.CancelableContext
	wg         sync.WaitGroup
	src        io.Reader
	period     time.Duration
	leakSize   int64
	bucketSize int64
	buf        *bytes.Buffer
	bufErr     error
	bufCh      chan struct{}
	bufMu      sync.Mutex
	rrCh       chan *leakyBucketReadRequest
}

// NewLeakyBucket creates new LeakyBucket the given period, leak and bucket size.
func NewLeakyBucket(src io.Reader, period time.Duration, leakSize, bucketSize int64) *LeakyBucket {
	l := &LeakyBucket{
		ctx:        xcontext.WithCancelable2(context.Background()),
		src:        src,
		period:     period,
		leakSize:   leakSize,
		bucketSize: bucketSize,
		buf:        bytes.NewBuffer(nil),
		bufCh:      make(chan struct{}, 1),
		rrCh:       make(chan *leakyBucketReadRequest),
	}
	l.wg.Add(1)
	go l.readLoop()
	l.wg.Add(1)
	go l.writeLoop()
	return l
}

func (a *LeakyBucket) readLoop() {
	defer a.wg.Done()
	b := make([]byte, BufferSize)
	for a.ctx.Err() == nil {
		a.bufMu.Lock()
		nn := a.bucketSize - int64(a.buf.Len())
		a.bufMu.Unlock()
		if nn <= 0 {
			select {
			case <-a.ctx.Done():
				return
			case <-a.bufCh:
			}
			continue
		}
		if l := int64(len(b)); nn > l {
			nn = l
		}
		n, e := a.src.Read(b)
		a.bufMu.Lock()
		a.buf.Write(b[:n])
		if e != nil {
			a.bufErr = e
			a.bufMu.Unlock()
			break
		}
		a.bufMu.Unlock()
	}
}

func (a *LeakyBucket) writeLoop() {
	defer a.wg.Done()
	tkr := time.NewTicker(a.period)
	defer tkr.Stop()
	for a.ctx.Err() == nil {
		select {
		case <-a.ctx.Done():
			return
		case <-tkr.C:
		}
		nr := a.leakSize
		for a.ctx.Err() == nil && nr > 0 {
			var rr *leakyBucketReadRequest
			select {
			case <-a.ctx.Done():
				return
			case rr = <-a.rrCh:
			}
			nn := int64(len(rr.B))
			if nn > nr {
				nn = nr
			}
			a.bufMu.Lock()
			rr.N, _ = a.buf.Read(rr.B[:nn])
			if rr.N <= 0 {
				rr.E = a.bufErr
			} else {
				rr.E = nil
			}
			a.bufMu.Unlock()
			nr -= int64(rr.N)
			close(rr.C)
			select {
			case a.bufCh <- struct{}{}:
			default:
			}
		}
	}
}

// Read implements (io.Reader).Read. When Stop called, Read drains bucket and returns EOF.
func (a *LeakyBucket) Read(b []byte) (n int, err error) {
	c := make(chan struct{})
	rr := &leakyBucketReadRequest{
		C: c,
		B: b,
	}
	select {
	case <-a.ctx.Done():
		a.wg.Wait()
		a.bufMu.Lock()
		defer a.bufMu.Unlock()
		return a.buf.Read(b)
	case a.rrCh <- rr:
		<-c
		return rr.N, rr.E
	}
}

// Stop stops bandwidth shaping operations. Stop should be called after the operation was ended.
func (a *LeakyBucket) Stop() {
	a.ctx.Cancel()
}

type leakyBucketReadRequest struct {
	C chan<- struct{}
	B []byte
	N int
	E error
}

// CalculateLeakyBucketLeakSize calculates leak size by the given period and rate. rate is in bytes per second.
func CalculateLeakyBucketLeakSize(period time.Duration, rate int64) int64 {
	x := big.NewInt(rate)
	x.Mul(x, big.NewInt(period.Nanoseconds()))
	x.Quo(x, big.NewInt(1e9))
	return x.Int64()
}
