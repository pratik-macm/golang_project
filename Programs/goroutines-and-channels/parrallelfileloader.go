package goroutinesandchannels

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"
)

/*
Task 1: Parallel file "loader" (WaitGroup + Mutex)
	You have 20 symbols: RELIANCE", TCS", INFY", .... Write loadChart(symbol string) []float64. It should sleep a random 100–500ms (fake I/O) and return 5 random prices.

	Load all 20 at the same time.
	Store the results in a map[string][]float64.
	Print the total time taken. It should be about 500ms", not about 6s.
	Concepts: sync.WaitGroup", sync.Mutex", closure capture.
	Hint: Concurrent writes to a map crash the program. Protect the map.

*/

var SymbolStore map[string][]float32

func ReadPrice(symbol string, wg *sync.WaitGroup) []float32 {

	time.Sleep(time.Millisecond * 498)
	var stkprices []float32

	for i := 0; i < 5; i++ {
		stkprices = append(stkprices, rand.Float32()*100)
	}

	wg.Done()
	return stkprices
}

func ReturnTokenPrice() {

	var wg sync.WaitGroup
	var mu sync.Mutex
	SymbolStore = make(map[string][]float32)

	symbols := []string{
		"RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
		"WIPRO", "BHARTIARTL", "ITC", "SBIN", "LT",
		"AXISBANK", "KOTAKBANK", "TATAMOTORS", "MARUTI", "SUNPHARMA",
		"TITAN", "ADANIENT", "ASIANPAINT", "HCLTECH", "ULTRACEMCO",
	}

	startTime := time.Now()

	for _, sym := range symbols {
		wg.Add(1)
		go func(symbol string) {
			price := ReadPrice(symbol, &wg)
			mu.Lock()
			SymbolStore[symbol] = price
			mu.Unlock()
		}(sym)
	}
	wg.Wait()
	
	fmt.Printf("Symbol loaded " + strconv.Itoa(len(symbols)) + " in time " + time.Since(startTime).String()+"\n")
}
