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

	const worker = 2
	var wg sync.WaitGroup
	var cwg sync.WaitGroup

	SymbolStore = make(map[string][]float32)
	var workerChn = make(chan (string), 20)
	var resultChn = make(chan (Result), 2)

	symbols := []string{
		"RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
		"WIPRO", "BHARTIARTL", "ITC", "SBIN", "LT",
		"AXISBANK", "KOTAKBANK", "TATAMOTORS", "MARUTI", "SUNPHARMA",
		"TITAN", "ADANIENT", "ASIANPAINT", "HCLTECH", "ULTRACEMCO",
	}
	startTime := time.Now()
	for i := 1; i <= worker; i++ {
		wg.Add(1)
		go DoJob(&wg, workerChn, resultChn, i)
	}

	go func() {
		for _, sym := range symbols {
			workerChn <- sym
		}
		close(workerChn)
	}()

	cwg.Add(1)
	go func() {
		//this is consumer suppose it does io opearion for 2s
		time.Sleep(time.Second * 2)
		for result := range resultChn {
			SymbolStore[result.Symbol] = result.Prices
		}
		defer cwg.Done()
	}()

	go func() {
		wg.Wait()
		close(resultChn)
	}()
	cwg.Wait()
	fmt.Printf("Symbol loaded " + strconv.Itoa(len(symbols)) + " in time " + time.Since(startTime).String() + "\n")
	fmt.Println(SymbolStore)
}

func DoJob(wg *sync.WaitGroup, workerCh <-chan string, result chan<- Result, workerId int) {

	for symbol := range workerCh {

		prices := ReadStkPrice(symbol)
		result <- Result{
			Symbol: symbol,
			Prices: prices,
		}

		fmt.Println(" WorkerId ", workerId, " symbol ", symbol, " Prices", prices)
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
