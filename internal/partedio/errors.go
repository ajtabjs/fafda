package partedio

import (
	"errors"
)

var (
	ErrClosed  = errors.New("is closed")
	ErrNoParts = errors.New("no parts provided")
)
