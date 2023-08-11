// Copyright 2023 The Cuber Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package limiter

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type (
	Limiter interface {
		AcquireRead() error
		ReleaseRead()
		AcquireWrite() error
		ReleaseWrite()
		Reader(ctx context.Context, r io.Reader) LimitReader
		Writer(ctx context.Context, w io.Writer) LimitWriter
		SetReadConcurrency(value uint32)
		SetWriteConcurrency(value uint32)
		SetReadMBPS(mbps int)
		SetWriteMBPS(mbps int)
		GetConfig() *LimitConfig
		Status() Status
	}
	LimitReader interface {
		WaitN(n int) error
		io.Reader
	}
	LimitWriter interface {
		WaitN(n int) error
		io.Writer
	}
	CountLimit interface {
		Running() int
		Acquire() error
		Release()
		SetLimit(limit uint32)
	}
	LimitConfig struct {
		ReadConcurrency  int
		WriteConcurrency int
		ReadMBPS         int
		WriteMBPS        int
	}
	Status struct {
		Config       LimitConfig
		ReadRunning  int
		WriteRunning int
		ReadWait     int
		WriteWait    int
	}
	// reader limited reader
	reader struct {
		ctx        context.Context
		rate       *rate.Limiter
		underlying io.Reader
	}
	// writer limited writer
	writer struct {
		ctx        context.Context
		rate       *rate.Limiter
		underlying io.Writer
	}
	noopLimitReader struct {
		underlying io.Reader
	}
	noopLimitWriter struct {
		underlying io.Writer
	}
	limiter struct {
		config          LimitConfig
		readCountLimit  CountLimit
		writeCountLimit CountLimit
		rateReader      *rate.Limiter
		rateWriter      *rate.Limiter
	}
)

func (r *reader) Read(p []byte) (n int, err error) {
	if err = r.rate.WaitN(r.ctx, len(p)); err != nil {
		return 0, err
	}
	n, err = r.underlying.Read(p)
	return
}

func (r *reader) WaitN(n int) error {
	return r.rate.WaitN(r.ctx, n)
}

func (w *writer) Write(p []byte) (n int, err error) {
	if err = w.rate.WaitN(w.ctx, len(p)); err != nil {
		return 0, err
	}
	n, err = w.underlying.Write(p)
	return
}

func (w *writer) WaitN(n int) error {
	return w.rate.WaitN(w.ctx, n)
}

func (nr *noopLimitReader) Read(p []byte) (n int, err error) {
	return nr.underlying.Read(p)
}

func (nr *noopLimitReader) WaitN(n int) error {
	return nil
}

func (nw *noopLimitWriter) Write(p []byte) (n int, err error) {
	return nw.underlying.Write(p)
}

func (nw *noopLimitWriter) WaitN(n int) error {
	return nil
}

func NewLimiter(cfg LimitConfig) Limiter {
	mb := 1 << 20
	limiter := &limiter{}
	if cfg.ReadConcurrency > 0 {
		limiter.readCountLimit = NewCountLimit(cfg.ReadConcurrency)
	}
	if cfg.WriteConcurrency > 0 {
		limiter.writeCountLimit = NewCountLimit(cfg.WriteConcurrency)
	}
	if cfg.ReadMBPS > 0 {
		limiter.rateReader = rate.NewLimiter(rate.Limit(cfg.ReadMBPS*mb), cfg.ReadMBPS*mb)
	}
	if cfg.WriteMBPS > 0 {
		limiter.rateWriter = rate.NewLimiter(rate.Limit(cfg.WriteMBPS*mb), cfg.WriteMBPS*mb)
	}
	limiter.config = cfg

	return limiter
}

func (lim *limiter) AcquireRead() error {
	if lim.readCountLimit != nil {
		return lim.readCountLimit.Acquire()
	}
	return nil
}

func (lim *limiter) AcquireWrite() error {
	if lim.writeCountLimit != nil {
		return lim.writeCountLimit.Acquire()
	}
	return nil
}

func (lim *limiter) ReleaseRead() {
	if lim.readCountLimit != nil {
		lim.readCountLimit.Release()
	}
}

func (lim *limiter) ReleaseWrite() {
	if lim.writeCountLimit != nil {
		lim.writeCountLimit.Release()
	}
}

func (lim *limiter) Reader(ctx context.Context, r io.Reader) LimitReader {
	if lim.rateReader != nil {
		return &reader{
			ctx:        ctx,
			rate:       lim.rateReader,
			underlying: r,
		}
	}
	return &noopLimitReader{underlying: r}
}

func (lim *limiter) Writer(ctx context.Context, w io.Writer) LimitWriter {
	if lim.rateWriter != nil {
		return &writer{
			ctx:        ctx,
			rate:       lim.rateWriter,
			underlying: w,
		}
	}
	return &noopLimitWriter{underlying: w}
}

func (lim *limiter) SetReadConcurrency(value uint32) {
	if lim.readCountLimit == nil {
		lim.readCountLimit = NewCountLimit(int(value))
	} else {
		lim.readCountLimit.SetLimit(value)
	}
	lim.config.ReadConcurrency = int(value)
}

func (lim *limiter) SetWriteConcurrency(value uint32) {
	if lim.writeCountLimit == nil {
		lim.writeCountLimit = NewCountLimit(int(value))
	} else {
		lim.writeCountLimit.SetLimit(value)
	}
	lim.config.WriteConcurrency = int(value)
}

func (lim *limiter) SetReadMBPS(mbps int) {
	mb := 1 << 20
	if lim.rateReader == nil {
		lim.rateReader = rate.NewLimiter(rate.Limit(mbps*mb), mbps*mb)
	} else {
		lim.rateReader.SetLimit(rate.Limit(mbps * mb))
		lim.rateReader.SetBurst(mbps * mb)
	}
	lim.config.ReadMBPS = mbps
}

func (lim *limiter) SetWriteMBPS(mbps int) {
	mb := 1 << 20
	if lim.rateWriter == nil {
		lim.rateWriter = rate.NewLimiter(rate.Limit(mbps*mb), mbps*mb)
	} else {
		lim.rateWriter.SetLimit(rate.Limit(mbps * mb))
		lim.rateWriter.SetBurst(mbps * mb)
	}
	lim.config.WriteMBPS = mbps
}

func (lim *limiter) GetConfig() *LimitConfig {
	return &lim.config
}

func (lim *limiter) Status() Status {
	st := Status{
		Config: lim.config,
	}

	if lim.readCountLimit == nil {
		st.ReadRunning = 0
	} else {
		st.ReadRunning = lim.readCountLimit.Running()
	}
	if lim.writeCountLimit == nil {
		st.WriteRunning = 0
	} else {
		st.WriteRunning = lim.writeCountLimit.Running()
	}
	if lim.rateReader == nil {
		st.ReadWait = 0
	} else {
		st.ReadWait = rateWait(lim.rateReader)
	}
	if lim.rateWriter == nil {
		st.WriteWait = 0
	} else {
		st.WriteWait = rateWait(lim.rateWriter)
	}

	return st
}

func rateWait(r *rate.Limiter) int {
	if r == nil {
		return 0
	}
	now := time.Now()
	reserve := r.ReserveN(now, int(r.Limit())/2)
	duration := reserve.DelayFrom(now)
	reserve.Cancel()
	return int(duration.Milliseconds())
}

const minusOne = ^uint32(0)

type countLimit struct {
	limit   uint32
	current uint32
}

// NewCountLimit returns limiter with concurrent n
func NewCountLimit(n int) CountLimit {
	return &countLimit{limit: uint32(n)}
}

func (l *countLimit) Running() int {
	return int(atomic.LoadUint32(&l.current))
}

func (l *countLimit) Acquire() error {
	if atomic.AddUint32(&l.current, 1) > l.limit {
		atomic.AddUint32(&l.current, minusOne)
		return errors.New("limit exceeded")
	}
	return nil
}

func (l *countLimit) Release() {
	atomic.AddUint32(&l.current, minusOne)
}

func (l *countLimit) SetLimit(limit uint32) {
	atomic.StoreUint32(&l.limit, limit)
}
