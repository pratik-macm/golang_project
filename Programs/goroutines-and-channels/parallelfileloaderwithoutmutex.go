package goroutinesandchannels

import (
	"fmt"
	"math/rand/v2"
	"strconv"
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

type Result struct {
	Symbol string
	Prices []float32
}

func StoreTokenPrice() {

	const worker = 20
	var wg sync.WaitGroup

	SymbolStore = make(map[string][]float32)
	var workerChn = make(chan (string), 20)
	var resultChn = make(chan (Result), 20)

	symbols := []string{
		"RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
		"WIPRO", "BHARTIARTL", "ITC", "SBIN", "LT",
		"AXISBANK", "KOTAKBANK", "TATAMOTORS", "MARUTI", "SUNPHARMA",
		"TITAN", "ADANIENT", "ASIANPAINT", "HCLTECH", "ULTRACEMCO",
	}
	startTime := time.Now()
	for i := 0; i < worker; i++ {
		wg.Add(1)
		go DoJob(&wg, workerChn, resultChn, i)
	}

	go func() {
		for _, sym := range symbols {
			workerChn <- sym
		}
		close(workerChn)
	}()

	go func() {
		for result := range resultChn {
			SymbolStore[result.Symbol] = result.Prices

		}
	}()

	wg.Wait()
	close(resultChn)
	fmt.Printf("Symbol loaded " + strconv.Itoa(len(symbols)) + " in time " + time.Since(startTime).String() + "\n")

}

func DoJob(wg *sync.WaitGroup, workerCh <-chan string, result chan<- Result, workerId int) {

	for symbol := range workerCh {

		prices := ReadStkPrice(symbol)
		result <- Result{
			Symbol: symbol,
			Prices: prices,
		}

		fmt.Println(" WorkerId ", workerId, " symbol ", result)
	}
	wg.Done()
}

func ReadStkPrice(symbol string) []float32 {

	// time.Sleep(time.Millisecond * 498)
	var stkprices []float32

	for i := 0; i < 5; i++ {
		stkprices = append(stkprices, rand.Float32()*100)
	}

	return stkprices
}
