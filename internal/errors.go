package internal

import (
	"errors"
	"os"
)

var (
	ErrIsDir                = &os.PathError{Err: errors.New("is a directory")}
	ErrIsNotDir             = &os.PathError{Err: errors.New("is not a directory")}
	ErrNotFound             = &os.PathError{Err: errors.New("source or destination does not exist")}
	ErrNotEmpty             = &os.PathError{Err: errors.New("directory not empty")}
	ErrInvalidSeek          = &os.PathError{Err: errors.New("invalid seek offset")}
	ErrNotSupported         = &os.PathError{Err: errors.New("fs doesn't support this operation")}
	ErrAlreadyExist         = &os.PathError{Err: errors.New("destination already exist")}
	ErrInvalidOperation     = &os.PathError{Err: errors.New("invalid operation - hint: trying to move directory into itself")}
	ErrInvalidRootOperation = &os.PathError{Err: errors.New("invalid operation - stop fucking with root directory")}
)
