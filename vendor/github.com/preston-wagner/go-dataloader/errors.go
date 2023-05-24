package dataloader

import (
	"errors"
	"fmt"
)

var ErrMissingResponse = errors.New("pending task timed out")

type GetterPanicError struct {
	recovered any
}

func (gpe GetterPanicError) Error() string {
	return fmt.Sprintf("panic in getter: %v", gpe.recovered)
}
