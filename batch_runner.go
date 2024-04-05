package dbbatch

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const maxAllowedIterations = 10_000_000

type batchItem struct {
	i           int
	cb          CallbackFn
	batchResult any
	roundTrip   chan struct{}
	result      chan error
	isWaiting   bool
	isFinished  bool
}
type Request struct {
	Query string
	Args  []any
}

type batchRunner struct {
	requests    []Request
	currentItem *batchItem
	sema        chan struct{} // cap = 1
	batchSender BatchRequestsSender
}

var _ batchRunnerMachine = &batchRunner{}

func newBatchRunner(batchSender BatchRequestsSender) *batchRunner {
	return &batchRunner{
		requests:    []Request{},
		currentItem: nil,
		sema:        make(chan struct{}, 1),
		batchSender: batchSender,
	}
}

func (br *batchRunner) run(ctx context.Context, b *Batch) (err error) {
	if b == nil {
		return errors.New("batch must be not nil")
	}

	items := make([]batchItem, 0, len(b.Callbacks()))
	for i, cb := range b.Callbacks() {
		items = append(items, batchItem{
			i:          i,
			cb:         cb,
			roundTrip:  make(chan struct{}),
			result:     make(chan error),
			isFinished: false,
		})
	}

	// run goroutines
	for i := range items {
		br.currentItem = &items[i]

		br.sema <- struct{}{}

		go func() {
			cbErr := br.currentItem.cb(ctx)
			br.currentItem.result <- cbErr
		}()

		resultErr := br.waitForCurrentItemFinishedOrLocked()
		err = errors.Join(err, resultErr)
	}

	// do batches while all goroutines not done
	var (
		res          any
		closeFn      func() error
		sendBatchErr error
		iteration    = 0
	)
	for len(br.requests) > 0 {
		res, closeFn, sendBatchErr = br.batchSender.SendBatchRequests(ctx, br.requests)
		if sendBatchErr != nil {
			return fmt.Errorf("batchSender.sendBatch: %w", sendBatchErr)
		}
		br.requests = br.requests[:0]

		for i := range items {
			br.currentItem = &items[i]

			if br.currentItem.isFinished {
				continue
			}

			br.sema <- struct{}{}
			br.currentItem.batchResult = res
			br.currentItem.roundTrip <- struct{}{}

			resultErr := br.waitForCurrentItemFinishedOrLocked()
			err = errors.Join(err, resultErr)
		}

		if closeErr := closeFn(); closeErr != nil {
			return fmt.Errorf("close batch results: %w", closeErr)
		}

		iteration++
		if iteration >= maxAllowedIterations {
			return fmt.Errorf("max allowed iterations %d reached", iteration)
		}
	}
	// got all results

	return err
}

// Wait for created item goroutine finished or locked by db query/exec
func (br *batchRunner) waitForCurrentItemFinishedOrLocked() (err error) {
	select {
	case err = <-br.currentItem.result:
		close(br.currentItem.roundTrip)
		br.currentItem.isFinished = true
	case br.sema <- struct{}{}:
	case <-time.After(120 * time.Second):
		panic("possible deadlock in waiting for finished batch callbacks")
	}

	<-br.sema

	return err
}

// Queue Only for using in the driver implementation code!
func (br *batchRunner) Queue(request Request) any {
	if !br.currentItem.isWaiting {
		br.currentItem.isWaiting = true
		br.requests = append(br.requests, request)

		return nil
	}

	res := br.currentItem.batchResult // if we read this sema, then batchSender.sema already locked
	br.currentItem.isWaiting = false

	return res
}

func (br *batchRunner) roundTrip() {
	// important to save currentItem pointer before releasing batchSender.sema
	currentItem := br.currentItem
	<-br.sema
	<-currentItem.roundTrip
}
