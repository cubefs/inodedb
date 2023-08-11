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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type (
	limitReader struct {
		size int
		read int
	}
	limitWriter struct{}
)

func (r *limitReader) Read(p []byte) (n int, err error) {
	if r.read >= r.size {
		return 0, io.EOF
	}

	b := make([]byte, 1<<20)
	n = copy(p, b)
	r.read += n
	return
}

func (w *limitWriter) Write(p []byte) (n int, err error) {
	b := make([]byte, 1<<20)
	n = 0
	for len(p) > 0 {
		nn := copy(p, b)
		n += nn
		p = p[nn:]
	}
	return
}

func TestLimiter(t *testing.T) {
	cfg := LimitConfig{
		ReadConcurrency:  1,
		WriteConcurrency: 1,
		ReadMBPS:         1,
		WriteMBPS:        1,
	}
	l := NewLimiter(cfg)
	{
		err := l.AcquireRead()
		require.NoError(t, err)
		err = l.AcquireRead()
		require.Equal(t, errors.New("limit exceeded"), err)
		go func() {
			l.SetReadConcurrency(2)
		}()
		time.Sleep(1 * time.Second)
		err = l.AcquireRead()
		require.NoError(t, err)
		l.ReleaseRead()
		l.ReleaseRead()
		require.Equal(t, 0, l.Status().ReadRunning)
	}
	{
		// ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		ctx := context.TODO()
		now := time.Now()
		var wg sync.WaitGroup
		worker := 2
		wg.Add(worker)
		var n int64 = 0
		for i := 0; i < worker; i++ {
			go func() {
				rbuff := &limitReader{size: 1 << 20}
				r := l.Reader(ctx, rbuff)
				b := make([]byte, 1<<20)
				r.Read(b)
				atomic.AddInt64(&n, 1)
				wg.Done()
			}()
		}
		wg.Wait()
		fmt.Println(float64(n)/time.Since(now).Seconds(), "mbps")
	}
	{
		// ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		ctx := context.TODO()
		now := time.Now()
		var wg sync.WaitGroup
		worker := 2
		wg.Add(worker)
		var n int64 = 0
		for i := 0; i < worker; i++ {
			go func() {
				wbuff := &limitWriter{}
				w := l.Writer(ctx, wbuff)
				b := make([]byte, 1<<20)
				w.Write(b)
				atomic.AddInt64(&n, 1)
				wg.Done()
			}()
		}
		wg.Wait()
		fmt.Println(float64(n)/time.Since(now).Seconds(), "mbps")
	}
}
