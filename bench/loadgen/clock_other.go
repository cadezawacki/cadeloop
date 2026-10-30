//go:build !windows

package main

import (
	"syscall"
	"time"
)

// Outside Windows, Go's monotonic clock has nanosecond resolution, so it
// timestamps latencies directly (see clock_windows.go for why Windows
// needs QueryPerformanceCounter instead).
var origin = time.Now()

// now is nanoseconds since the program started, on the monotonic clock.
func now() int64 { return int64(time.Since(origin)) }

// sleepUntil sleeps, never spins, for the reason given in clock_windows.go.
func sleepUntil(t int64) {
	if d := t - now(); d > 0 {
		time.Sleep(time.Duration(d))
	}
}

// cpuTimes is this process's user and kernel CPU time, in seconds.
func cpuTimes() (user, kernel float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime), tv(ru.Stime)
}
