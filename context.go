package dbbatch

import (
	"context"
)

type contextKeyBatchConnType struct{}

var contextKeyBatchConn = contextKeyBatchConnType{}

func BatchConnFromContext(ctx context.Context) *BatchConn {
	iTx := ctx.Value(contextKeyBatchConn)
	if iTx == nil {
		return nil
	}

	b, ok := iTx.(*BatchConn)
	if !ok {
		return nil
	}

	return b
}

func SetBatchConnToContext(ctx context.Context, b *BatchConn) context.Context {
	return context.WithValue(ctx, contextKeyBatchConn, b)
}
