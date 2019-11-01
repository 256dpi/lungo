package dbkit

// Semaphore manages access to a resource using a set of tokens.
type Semaphore struct {
	tokens chan struct{}
}

// NewSemaphore will create and return a new semaphore.
func NewSemaphore(capacity int) *Semaphore {
	// prepare tokens
	tokens := make(chan struct{}, capacity)
	for i := 0; i < capacity; i++ {
		tokens <- struct{}{}
	}

	return &Semaphore{
		tokens: tokens,
	}
}

// Acquire will acquire a token from the semaphore. If the function returns
// true the token must be released back to the semaphore exactly once.
func (s *Semaphore) Acquire(timeout <-chan struct{}) bool {
	select {
	case <-s.tokens:
		return true
	case <-timeout:
		return false
	}
}

// Release will release a token to the semaphore. It is critical that this is
// only done once per token.
func (s *Semaphore) Release() {
	select {
	case s.tokens <- struct{}{}:
	default:
		panic("semaphore full")
	}
}
