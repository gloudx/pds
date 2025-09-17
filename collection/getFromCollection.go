package collection

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore"
)

// GetFromCollection получает элемент из коллекции
func (cm *CollectionManager) GetFromCollection(ctx context.Context, collectionName string, key datastore.Key) ([]byte, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if _, exists := cm.collections[collectionName]; !exists {
		return nil, fmt.Errorf("collection %s not found", collectionName)
	}
	dataKey := datastore.NewKey(fmt.Sprintf("%s%s%s%s", collectionPrefix, collectionName, collectionDataKey, key.String()))
	return cm.storage.Get(ctx, dataKey)
}
