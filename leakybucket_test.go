package ioshape_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/goinsane/ioshape"
)

func TestLeakyBucket(t *testing.T) {
	period := 250 * time.Millisecond
	leakSize := int64(256 * 1024)
	bucketSize := 4 * leakSize
	nLeak := 20
	dur := time.Duration(nLeak) * period

	b := make([]byte, int64(nLeak)*leakSize)
	r := ioshape.NewLeakyBucket(bytes.NewReader(b), period, leakSize, bucketSize)

	beginning := time.Now()
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(b)) {
		t.Fatal("size mismatch")
	}
	d := time.Now().Sub(beginning)
	t.Logf("duration: %v", d)
	if !(dur+period <= d && d <= dur+period*2) {
		t.Fatal("duration mismatch")
	}
	r.Stop()
}
