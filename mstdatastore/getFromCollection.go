package mstdatastore

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore"
)

// GetFromCollection получает элемент из коллекции
func (cmd *MstDatastore) GetFromCollection(ctx context.Context, collectionName string, key datastore.Key) ([]byte, error) {
	cmd.mu.RLock()
	defer cmd.mu.RUnlock()

	if _, exists := cmd.collections[collectionName]; !exists {
		return nil, fmt.Errorf("collection %s not found", collectionName)
	}

	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, key.String()))
	return cmd.storage.Get(ctx, dataKey)
}
