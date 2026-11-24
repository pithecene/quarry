package iox

import (
	"errors"
	"testing"
)

type spyCloser struct{ closed bool }

func (s *spyCloser) Close() error { s.closed = true; return errors.New("ignored") }

func TestDiscardClose(t *testing.T) {
	t.Parallel()

	s := &spyCloser{}
	DiscardClose(s)
	if !s.closed {
		t.Fatal("Close was not called")
	}
}

func TestCloseFunc(t *testing.T) {
	t.Parallel()

	s := &spyCloser{}
	fn := CloseFunc(s)
	if s.closed {
		t.Fatal("Close called before invoking returned func")
	}
	fn()
	if !s.closed {
		t.Fatal("Close was not called")
	}
}
