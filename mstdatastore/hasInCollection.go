package mstdatastore

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore"
)

// HasInCollection проверяет существование элемента в коллекции
func (cmd *MstDatastore) HasInCollection(ctx context.Context, collectionName string, key datastore.Key) (bool, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	if _, exists := cmd.collections[collectionName]; !exists {
		return false, fmt.Errorf("collection %s not found", collectionName)
	}

	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, key.String()))
	_, err := cmd.storage.Get(ctx, dataKey)
	if err == datastore.ErrNotFound {
		return false, nil
	}
	return err == nil, err
}
