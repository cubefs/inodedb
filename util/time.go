package util

import (
	"io"
	"time"
)

type (
	TimeReader struct {
		R  io.Reader
		dt time.Duration
	}
	TimeWriter struct {
		W  io.Writer
		dt time.Duration
	}
)

func (tr *TimeReader) Read(p []byte) (n int, err error) {
	start := time.Now()
	n, err = tr.R.Read(p)
	if err != nil && err != io.EOF {
		return n, err
	}
	tr.dt += time.Since(start)
	return n, err
}

func (tr *TimeReader) GetCost() time.Duration {
	return tr.dt
}

func (tw *TimeWriter) Write(p []byte) (n int, err error) {
	start := time.Now()
	n, err = tw.W.Write(p)
	if err != nil && err != io.EOF {
		return n, err
	}
	tw.dt += time.Since(start)
	return n, err
}

func (tw *TimeWriter) GetCost() time.Duration {
	return tw.dt
}
