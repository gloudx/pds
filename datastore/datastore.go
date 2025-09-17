package datastore

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	bds "github.com/ipfs/go-ds-badger4"
)

// Datastore ...
type Datastore interface {

	// Встраивание методов базового хранилища данных и его возможностей.
	ds.Datastore
	ds.BatchingFeature
	ds.TxnFeature
	ds.GCFeature
	ds.PersistentFeature
	ds.TTL

	// Iterator ...
	Iterator(ctx context.Context, prefix ds.Key) (<-chan KeyValue, error)

	// Merge ...
	Merge(ctx context.Context, other Datastore) error

	// Clear ...
	Clear(ctx context.Context) error
}

// KeyValue - это простая структура для хранения пары ключ-значение.
type KeyValue struct {
	Key   ds.Key
	Value []byte
}

var _ ds.Datastore = (*datastorage)(nil)
var _ ds.PersistentDatastore = (*datastorage)(nil)
var _ ds.TxnDatastore = (*datastorage)(nil)
var _ ds.TTLDatastore = (*datastorage)(nil)
var _ ds.GCDatastore = (*datastorage)(nil)
var _ ds.Batching = (*datastorage)(nil)

type datastorage struct {
	*bds.Datastore
}

type Options struct {
	bds.Options

	Prefix string
}

// NewDatastorage ...
func NewDatastorage(path string, opts Options) (Datastore, error) {

	badgerDS, err := bds.NewDatastore(path, &opts.Options)
	if err != nil {
		return nil, err
	}
	return &datastorage{Datastore: badgerDS}, nil
}

// Iterator ...
func (s *datastorage) Iterator(ctx context.Context, prefix ds.Key) (<-chan KeyValue, error) {
	result, err := s.Datastore.Query(ctx, query.Query{Prefix: prefix.String()})
	if err != nil {
		return nil, err
	}
	ch := make(chan KeyValue)
	go func() {
		defer close(ch)
		defer result.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-result.Next():
				if !ok {
					return
				}
				if res.Error != nil {
					return
				}
				ch <- KeyValue{Key: ds.NewKey(res.Key), Value: res.Value}
			}
		}
	}()
	return ch, nil
}

// Merge ...
func (s *datastorage) Merge(ctx context.Context, other Datastore) error {
	batch, err := s.Batch(ctx)
	if err != nil {
		return err
	}
	it, err := other.Iterator(ctx, ds.NewKey("/"))
	if err != nil {
		return err
	}
	for kv := range it {
		if err := batch.Put(ctx, kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return batch.Commit(ctx)
}

// Clear ...
func (s *datastorage) Clear(ctx context.Context) error {
	q, err := s.Query(ctx, query.Query{})
	if err != nil {
		return err
	}
	defer q.Close()
	for res := range q.Next() {
		if res.Error != nil {
			return res.Error
		}
		if err := s.Delete(ctx, ds.NewKey(res.Key)); err != nil {
			return err
		}
	}
	return nil
}
