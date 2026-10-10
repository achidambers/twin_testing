package ratelimit

import (
	"sort"
	"sync"
	"time"
)

// Clock is the time source used by limiters.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// systemClock derives the current time from the monotonic clock, so wall
// clock adjustments (NTP steps, manual changes) cannot refill or drain buckets.
type systemClock struct {
	base time.Time
}

var sysClock = systemClock{base: time.Now()}

// SystemClock returns the default, monotonic clock.
func SystemClock() Clock { return sysClock }

func (c systemClock) Now() time.Time { return c.base.Add(time.Since(c.base)) }

func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// ManualClock is a Clock that only moves when told to. It is intended for
// deterministic tests of code that uses a limiter.
type ManualClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []manualWaiter
}

type manualWaiter struct {
	at time.Time
	ch chan time.Time
}

// NewManualClock returns a ManualClock set to t.
func NewManualClock(t time.Time) *ManualClock {
	return &ManualClock{now: t}
}

// Now returns the clock's current time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that receives once the clock has been advanced by d.
func (c *ManualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, manualWaiter{at: c.now.Add(d), ch: ch})
	return ch
}

// Advance moves the clock forward by d and fires any timers that are due.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLocked(c.now.Add(d))
}

// Set moves the clock to t, which may be in the past.
func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLocked(t)
}

// PendingTimers returns the number of After channels that have not fired.
func (c *ManualClock) PendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

func (c *ManualClock) setLocked(t time.Time) {
	c.now = t
	sort.SliceStable(c.waiters, func(i, j int) bool { return c.waiters[i].at.Before(c.waiters[j].at) })
	fired := 0
	for _, w := range c.waiters {
		if w.at.After(t) {
			break
		}
		w.ch <- t
		fired++
	}
	c.waiters = c.waiters[fired:]
}
