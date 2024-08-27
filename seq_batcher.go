package dbbatch

import "context"

type SeqBatcher struct{}

func NewSeqBatcher() *SeqBatcher {
	return &SeqBatcher{}
}

func (sb *SeqBatcher) SendBatch(ctx context.Context, b *Batch) (err error) {
	return b.RunSequential(ctx)
}
