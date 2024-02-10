package dbbatch

import (
	"context"
	"errors"
)

type CallbackFn = func(ctx context.Context) error

type Batch struct {
	callbacks []CallbackFn
}

func (b *Batch) Add(cb CallbackFn) {
	b.callbacks = append(b.callbacks, cb)
}

func (b *Batch) Callbacks() []CallbackFn {
	return b.callbacks
}

func (b *Batch) RunSequential(ctx context.Context) (err error) {
	for _, cb := range b.Callbacks() {
		cbErr := cb(ctx)
		err = errors.Join(err, cbErr)
	}

	return err
}
