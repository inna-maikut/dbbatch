package dbbatch

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestBatchRunner_OneStep(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchSenderMock := NewMockBatchRequestsSender(ctrl)

	result1 := struct{ name string }{name: "result 1"}

	batchSenderMock.EXPECT().SendBatchRequests(gomock.Any(), []Request{
		{Query: "first", Args: []interface{}{1, 2}},
		{Query: "second", Args: []interface{}{3, 4}},
	}).Return(result1, func() error {
		return nil
	}, nil)

	br := newBatchRunner(batchSenderMock)

	a := 0

	b := &Batch{}
	b.Add(func(ctx context.Context) error {
		a += 1

		res := br.Queue(Request{Query: "first", Args: []interface{}{1, 2}})
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(Request{Query: "first", Args: []interface{}{1, 2}})
		assert.Equal(t, result1, res)

		return nil
	})
	b.Add(func(ctx context.Context) error {
		a += 100

		res := br.Queue(Request{Query: "second", Args: []interface{}{3, 4}})
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(Request{Query: "second", Args: []interface{}{3, 4}})
		assert.Equal(t, result1, res)

		return nil
	})

	err := br.run(ctx, b)
	assert.NoError(t, err)
	assert.Equal(t, 101, a)
}

func TestBatchRunner_MultiStep(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchSenderMock := NewMockBatchRequestsSender(ctrl)

	result1 := struct{ name string }{name: "result 1"}
	result2 := struct{ name string }{name: "result 2"}

	request1 := Request{Query: "first", Args: []interface{}{1, 2}}
	request2 := Request{Query: "second", Args: []interface{}{3, 4}}
	request3 := Request{Query: "third", Args: []interface{}{5, 6}}

	batchSenderMock.EXPECT().SendBatchRequests(gomock.Any(), []Request{
		request1,
		request2,
	}).Return(result1, func() error {
		return nil
	}, nil)
	batchSenderMock.EXPECT().SendBatchRequests(gomock.Any(), []Request{
		request3,
	}).Return(result2, func() error {
		return nil
	}, nil)

	br := newBatchRunner(batchSenderMock)

	a := 0

	b := &Batch{}
	b.Add(func(ctx context.Context) error {
		a += 1

		res := br.Queue(request1)
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(request1)
		assert.Equal(t, result1, res)

		res = br.Queue(request3)
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(request3)
		assert.Equal(t, result2, res)

		return nil
	})
	b.Add(func(ctx context.Context) error {
		a += 100

		res := br.Queue(request2)
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(request2)
		assert.Equal(t, result1, res)

		return nil
	})

	err := br.run(ctx, b)
	assert.NoError(t, err)
	assert.Equal(t, 101, a)
}

func TestBatchRunner_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchSenderMock := NewMockBatchRequestsSender(ctrl)

	result1 := struct{ name string }{name: "result 1"}
	result2 := struct{ name string }{name: "result 2"}

	request1 := Request{Query: "first", Args: []interface{}{1, 2}}
	request2 := Request{Query: "second", Args: []interface{}{3, 4}}
	request3 := Request{Query: "third", Args: []interface{}{5, 6}}

	batchSenderMock.EXPECT().SendBatchRequests(gomock.Any(), []Request{
		request1,
		request2,
	}).Return(result1, func() error {
		return nil
	}, nil)
	batchSenderMock.EXPECT().SendBatchRequests(gomock.Any(), []Request{
		request3,
	}).Return(result2, func() error {
		return nil
	}, nil)

	br := newBatchRunner(batchSenderMock)

	a := 0

	b := &Batch{}
	b.Add(func(ctx context.Context) error {
		a += 1

		res := br.Queue(request1)
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(request1)
		assert.Equal(t, result1, res)

		res = br.Queue(request3)
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(request3)
		assert.Equal(t, result2, res)

		return nil
	})
	b.Add(func(ctx context.Context) error {
		a += 100

		res := br.Queue(request2)
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(request2)
		assert.Equal(t, result1, res)

		return errors.New("some error")
	})

	err := br.run(ctx, b)
	assert.EqualError(t, err, "some error")
	assert.Equal(t, 101, a)
}

func TestBatchRunner_SendBatchErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchSenderMock := NewMockBatchRequestsSender(ctrl)

	request1 := Request{Query: "first", Args: []interface{}{1, 2}}

	batchSenderMock.EXPECT().SendBatchRequests(gomock.Any(), []Request{
		request1,
	}).Return(nil, func() error {
		return nil
	}, errors.New("some error"))

	br := newBatchRunner(batchSenderMock)

	a := 0

	b := &Batch{}
	b.Add(func(ctx context.Context) error {
		a += 1

		res := br.Queue(request1)
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(request1)

		return nil
	})

	err := br.run(ctx, b)
	assert.EqualError(t, err, "batchSender.sendBatch: some error")
	assert.Equal(t, 1, a)
}

func TestBatchRunner_CloseBatchResultsErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchSenderMock := NewMockBatchRequestsSender(ctrl)

	result1 := struct{ name string }{name: "result 1"}
	request1 := Request{Query: "first", Args: []interface{}{1, 2}}

	batchSenderMock.EXPECT().SendBatchRequests(gomock.Any(), []Request{
		request1,
	}).Return(result1, func() error {
		return errors.New("some error")
	}, nil)

	br := newBatchRunner(batchSenderMock)

	a := 0

	b := &Batch{}
	b.Add(func(ctx context.Context) error {
		a += 1

		res := br.Queue(request1)
		assert.Nil(t, res)

		br.roundTrip()

		res = br.Queue(request1)

		return nil
	})

	err := br.run(ctx, b)
	assert.EqualError(t, err, "close batch results: some error")
	assert.Equal(t, 1, a)
}
