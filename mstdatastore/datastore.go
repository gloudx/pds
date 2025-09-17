package mstdatastore

import (
	"context"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

// === go-datastore СОВМЕСТИМОСТЬ ===

func (cmd *MstDatastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	return cmd.storage.Put(ctx, key, value)
}

func (cmd *MstDatastore) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	return cmd.storage.Get(ctx, key)
}

func (cmd *MstDatastore) Delete(ctx context.Context, key datastore.Key) error {
	return cmd.storage.Delete(ctx, key)
}

func (cmd *MstDatastore) Has(ctx context.Context, key datastore.Key) (bool, error) {
	return cmd.storage.Has(ctx, key)
}

func (cmd *MstDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	return cmd.storage.Query(ctx, q)
}

func (cmd *MstDatastore) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	return cmd.storage.GetSize(ctx, key)
}

func (cmd *MstDatastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return cmd.storage.Sync(ctx, prefix)
}

func (cmd *MstDatastore) Close() error {
	return cmd.storage.Close()
}

func (cmd *MstDatastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return cmd.storage.Batch(ctx)
}
