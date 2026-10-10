package ratewarden_test

import (
	"context"
	"fmt"
	"time"

	"ratewarden"
	"ratewarden/ratewardentest"
)

func ExampleNewTokenBucket() {
	clk := ratewardentest.NewClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	// 2 requests per second on average, bursts of up to 3.
	l, _ := ratewarden.NewTokenBucket(2, time.Second, 3, ratewarden.WithClock(clk))
	defer l.Close()

	for i := 0; i < 4; i++ {
		d := l.Allow("user-42")
		fmt.Printf("allowed=%v remaining=%d retry_after=%v\n", d.Allowed, d.Remaining, d.RetryAfter)
	}
	clk.Advance(500 * time.Millisecond)
	fmt.Println("after 500ms:", l.Allow("user-42").Allowed)
	// Output:
	// allowed=true remaining=2 retry_after=0s
	// allowed=true remaining=1 retry_after=0s
	// allowed=true remaining=0 retry_after=0s
	// allowed=false remaining=0 retry_after=500ms
	// after 500ms: true
}

func ExampleNewSlidingWindow() {
	clk := ratewardentest.NewClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	l, _ := ratewarden.NewSlidingWindow(10, time.Minute, ratewarden.WithClock(clk))
	defer l.Close()

	l.AllowN("k", 10)
	clk.Advance(90 * time.Second) // half of the previous window still counts
	fmt.Println("remaining:", l.Peek("k").Remaining)
	// Output:
	// remaining: 5
}

func ExampleLimiter_Wait() {
	l, _ := ratewarden.NewTokenBucket(100, time.Second, 1)
	defer l.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if err := l.Wait(ctx, "outbound-api"); err != nil {
			fmt.Println(err)
			return
		}
	}
	fmt.Println("sent 3 requests, paced at 100/s")
	// Output:
	// sent 3 requests, paced at 100/s
}
