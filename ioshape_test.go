package ioshape_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/goinsane/ioshape"
)

func TestCopy(t *testing.T) {
	period := 250 * time.Millisecond
	beginning := time.Now()
	b := make([]byte, 10*1024*1024)
	n, err := ioshape.Copy(io.Discard, bytes.NewReader(b), 1*1024*1024, period)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(b)) {
		t.Fatal("size mismatch")
	}
	d := time.Now().Sub(beginning)
	t.Logf("duration: %v", d)
	if !(10*time.Second+period <= d && d <= 10*time.Second+period*2) {
		t.Fatal("duration mismatch")
	}
}

func TestReader(t *testing.T) {
	period := 250 * time.Millisecond
	beginning := time.Now()
	b := make([]byte, 10*1024*1024)
	r := ioshape.NewReader(bytes.NewReader(b), 1*1024*1024, period)
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(b)) {
		t.Fatal("size mismatch")
	}
	d := time.Now().Sub(beginning)
	t.Logf("duration: %v", d)
	if !(10*time.Second+period <= d && d <= 10*time.Second+period*2) {
		t.Fatal("duration mismatch")
	}
	r.Stop()
}

func TestReader_Stop(t *testing.T) {
	period := 250 * time.Millisecond
	beginning := time.Now()
	b := make([]byte, 10*1024*1024)
	r := ioshape.NewReader(bytes.NewReader(b), 1*1024*1024, period)
	go func() {
		time.Sleep(5 * time.Second)
		r.Stop()
	}()
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("size: %d", n)
	d := time.Now().Sub(beginning)
	t.Logf("duration: %v", d)
	if !(5*time.Second <= d && d <= 5*time.Second+period) {
		t.Fatal("duration mismatch")
	}
}
