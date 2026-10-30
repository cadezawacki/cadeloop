//go:build windows

package main

import (
	"syscall"
	"time"
	"unsafe"
)

// Latency timestamps come from QueryPerformanceCounter. Go's own monotonic
// clock on Windows reads the kernel's interrupt time, which only advances
// at the system timer interrupt (0.5-15.6 ms): echo round trips of ~50 us
// measured as exactly 0 or a whole tick.
var (
	procQPC  = syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceCounter")
	procQPF  = syscall.NewLazyDLL("kernel32.dll").NewProc("QueryPerformanceFrequency")
	qpcFreq  int64
	qpcValid bool
)

func init() {
	var f int64
	if r, _, _ := procQPF.Call(uintptr(unsafe.Pointer(&f))); r != 0 && f > 0 {
		qpcFreq, qpcValid = f, true
	}
}

// now is nanoseconds on the QPC timeline (arbitrary origin).
func now() int64 {
	if !qpcValid {
		return time.Now().UnixNano()
	}
	var c int64
	procQPC.Call(uintptr(unsafe.Pointer(&c)))
	return c/qpcFreq*1e9 + c%qpcFreq*1e9/qpcFreq
}

// sleepUntil sleeps, never spins: with hundreds of paced connections a
// spin-wait burns the cores the server under test shares with us. Go's
// Windows sleeps use high-resolution timers, so a send leaves at most
// about the platform's 0.5 ms timer floor late -- and since open-loop
// latency runs from the intended time, that lateness is charged to the
// measurement, never hidden.
func sleepUntil(t int64) {
	if d := t - now(); d > 0 {
		time.Sleep(time.Duration(d))
	}
}

// cpuTimes is this process's user and kernel CPU time, in seconds.
func cpuTimes() (user, kernel float64) {
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0, 0
	}
	var c, e, k, u syscall.Filetime
	if err := syscall.GetProcessTimes(h, &c, &e, &k, &u); err != nil {
		return 0, 0
	}
	ft := func(f syscall.Filetime) float64 {
		return float64(uint64(f.HighDateTime)<<32|uint64(f.LowDateTime)) / 1e7
	}
	return ft(u), ft(k)
}
