package dbbatch

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	ctx := context.Background()

	a := 0
	cb1 := func(ctx context.Context) error {
		a += 100

		return nil
	}
	cb2 := func(ctx context.Context) error {
		a += 1000

		return nil
	}
	cb3 := func(ctx context.Context) error {
		a += 10000

		return errors.New("some error")
	}

	b := &Batch{}

	b.Add(cb1)
	assert.Len(t, b.Callbacks(), 1)

	b.Add(cb2)
	assert.Len(t, b.Callbacks(), 2)
	assert.Equal(t, 0, a)
	err := b.RunSequential(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1100, a)

	b.Add(cb3)
	assert.Len(t, b.Callbacks(), 3)
	err = b.RunSequential(ctx)
	assert.Error(t, err)
	assert.Equal(t, 12200, a)
}
