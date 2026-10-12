package goroutinesandchannels

import (
	"math/rand/v2"
	"sync"
	"time"
)

/*

Task 2: Parallel file "loader" without mutex
	You have 20 symbols: RELIANCE, TCS, INFY, .... Write loadChart(symbol string) []float64. It should sleep a random 100–500ms (fake I/O) and return 5 random prices.
	Load all 20 at the same time.
	Store the results in a map[string][]float64.
	Print the total time taken. It should be about 500ms, not about 6s.

*/

func StoreTokenPrice() {

	const worker = 3
	var wg sync.WaitGroup

	SymbolStore = make(map[string][]float32)
	var workerChn = make(chan (string), worker)
	var resultChn = make(chan (map[string][]float32), worker)

	symbols := []string{
		"RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
		"WIPRO", "BHARTIARTL", "ITC", "SBIN", "LT",
		"AXISBANK", "KOTAKBANK", "TATAMOTORS", "MARUTI", "SUNPHARMA",
		"TITAN", "ADANIENT", "ASIANPAINT", "HCLTECH", "ULTRACEMCO",
	}

	for i := 0; i < worker; i++ {
		wg.Add(1)
		go DoJob(&wg, workerChn, resultChn)
	}

	for _, sym := range symbols {
		workerChn <- sym
	}
	close(workerChn)

	go func() {
		for result := range resultChn {
			for sym, prices := range result {
				SymbolStore[sym] = prices
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultChn)
	}()
}

func DoJob(wg *sync.WaitGroup, workerCh <-chan string, result chan<- map[string][]float32) {

	for symbol := range workerCh {
		stkprices := make(map[string][]float32)
		prices := ReadStkPrice(symbol)
		stkprices[symbol] = prices
		result <- stkprices
	}
	wg.Done()
}

func ReadStkPrice(symbol string) []float32 {

	time.Sleep(time.Millisecond * 498)
	var stkprices []float32

	for i := 0; i < 5; i++ {
		stkprices = append(stkprices, rand.Float32()*100)
	}

	return stkprices
}
