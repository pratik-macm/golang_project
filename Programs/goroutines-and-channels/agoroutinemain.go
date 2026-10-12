package goroutinesandchannels

func EnableGoroutine() {
	// RunGoroutine()
	// RunWorkers()
	// WorkerPoolBuffered()
	// WorkerPoolUnBuffered()
	// ProblemMain2()
	// ProblemMain4()
	ReturnTokenPrice()
}

func RunGoroutine() {
	go func() {
		SayHello("Pratik Gaikwad")
	}()
}
