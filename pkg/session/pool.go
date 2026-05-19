package session

import "sync"

var sessionPool = sync.Pool{
	New: func() any { return &Session{} },
}

// Acquire 取一个 Session。
func Acquire() *Session {
	s := sessionPool.Get().(*Session)
	return s
}

// Release 归还 Session（会自动 Reset）。
func Release(s *Session) {
	if s == nil {
		return
	}
	s.Reset()
	sessionPool.Put(s)
}
