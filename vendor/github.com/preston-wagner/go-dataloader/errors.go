package dataloader

import (
	"errors"
	"fmt"
)

var ErrMissingResponse = errors.New("no data or explicit error was returned for the given key")

type GetterPanicError struct {
	recovered any
}

func (gpe GetterPanicError) Error() string {
	return fmt.Sprintf("panic in getter: %v", gpe.recovered)
}
