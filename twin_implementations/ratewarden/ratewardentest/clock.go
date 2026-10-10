// Package ratewardentest provides a manually driven clock for testing code
// that uses ratewarden limiters.
package ratewardentest

import (
	"sync"
	"time"
)

// Clock is a ratewarden.Clock whose time only moves when Advance or Set is
// called. It is safe for concurrent use.
type Clock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

// NewClock returns a Clock reading t.
func NewClock(t time.Time) *Clock { return &Clock{now: t} }

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that receives once the clock has been advanced by
// at least d.
func (c *Clock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, waiter{c.now.Add(d), ch})
	return ch
}

// Advance moves the clock forward by d and fires any due After channels.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLocked(c.now.Add(d))
}

// Set moves the clock to t, which may be earlier than now.
func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLocked(t)
}

func (c *Clock) setLocked(t time.Time) {
	c.now = t
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.at.After(t) {
			w.ch <- t
		} else {
			kept = append(kept, w)
		}
	}
	c.waiters = kept
}

// Waiters reports how many After channels are pending. Tests use it to know
// when a goroutine has started waiting.
func (c *Clock) Waiters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}
